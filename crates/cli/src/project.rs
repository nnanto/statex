//! A statex app project: `statex.toml` + a Rust crate building a component.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use statex_runtime::sqlite::{apply_migrations, open_db};
use statex_runtime::{ActorIdentity, HttpPolicy, Limits, Manifest, Migration, Runtime, Ty};

#[derive(Debug, Deserialize)]
pub struct StatexToml {
    pub app: AppSection,
    #[serde(default)]
    pub http: HttpPolicy,
    #[serde(default)]
    pub limits: Limits,
    /// Non-Rust toolchains: how to build the component.
    #[serde(default)]
    pub build: Option<BuildSection>,
    /// Other apps whose actors this app calls (see `statex calls sync`).
    #[serde(default)]
    pub calls: CallsSection,
}

/// `[calls]`: apps this app calls; `statex calls sync` generates their client interfaces.
#[derive(Debug, Default, Deserialize)]
pub struct CallsSection {
    #[serde(default)]
    pub apps: Vec<String>,
    /// Source directories of callee apps outside a workspace, relative to the
    /// project root: `counter = "../counter"`.
    #[serde(default)]
    pub paths: BTreeMap<String, PathBuf>,
}

/// `[build]`: a custom build, e.g. componentize-py for Python actors.
#[derive(Debug, Deserialize)]
pub struct BuildSection {
    /// Shell command run in the project root.
    pub command: String,
    /// Component path produced by `command`, relative to the project root.
    pub wasm: PathBuf,
    /// Paths watched by `statex dev` (default: the whole project, minus build outputs).
    #[serde(default)]
    pub watch: Vec<PathBuf>,
}

#[derive(Debug, Deserialize)]
pub struct AppSection {
    pub name: String,
}

pub struct Project {
    pub root: PathBuf,
    pub cfg: StatexToml,
}

impl Project {
    /// Finds `statex.toml` in `start` or its parents.
    pub fn find(start: &Path) -> Result<Project> {
        let start = start.canonicalize().with_context(|| format!("{}", start.display()))?;
        for dir in start.ancestors() {
            let f = dir.join("statex.toml");
            if f.exists() {
                let cfg: StatexToml =
                    toml::from_str(&std::fs::read_to_string(&f)?).with_context(|| format!("parse {}", f.display()))?;
                statex_runtime::validate_app_name(&cfg.app.name).with_context(|| format!("app name in {}", f.display()))?;
                cfg.limits.validate().with_context(|| format!("guest limits in {}", f.display()))?;
                return Ok(Project { root: dir.to_path_buf(), cfg });
            }
        }
        bail!("no statex.toml found in {} or its parents (create a project with `statex new <name>`)", start.display())
    }

    pub fn app(&self) -> &str {
        &self.cfg.app.name
    }

    /// Actor types and migrations read from source (`wit/` and `migrations/`),
    /// without building the component.
    pub fn source_surface(&self) -> Result<(Vec<statex_runtime::ActorType>, BTreeMap<String, Vec<Migration>>)> {
        source_surface(&self.root)
    }

    /// Builds the component and returns its path: `[build] command` when set,
    /// otherwise `cargo build --release --target wasm32-wasip2`.
    pub fn build(&self, quiet: bool) -> Result<PathBuf> {
        if let Some(b) = &self.cfg.build {
            let st = Command::new("sh")
                .args(["-c", &b.command])
                .current_dir(&self.root)
                .status()
                .with_context(|| format!("run build command {:?}", b.command))?;
            if !st.success() {
                bail!("build command failed: {}", b.command);
            }
            return Ok(self.root.join(&b.wasm));
        }
        let mut cmd = Command::new("cargo");
        cmd.args(["build", "--release", "--target", "wasm32-wasip2"]).current_dir(&self.root);
        if quiet {
            cmd.arg("--quiet");
        }
        let st = cmd.status().context("run cargo (is Rust installed?)")?;
        if !st.success() {
            bail!("cargo build failed (hint: `rustup target add wasm32-wasip2`)");
        }
        self.wasm_path()
    }

    pub fn wasm_path(&self) -> Result<PathBuf> {
        if let Some(b) = &self.cfg.build {
            return Ok(self.root.join(&b.wasm));
        }
        let out = Command::new("cargo")
            .args(["metadata", "--format-version", "1", "--no-deps"])
            .current_dir(&self.root)
            .output()
            .context("cargo metadata")?;
        if !out.status.success() {
            bail!("cargo metadata failed: {}", String::from_utf8_lossy(&out.stderr));
        }
        let meta: serde_json::Value = serde_json::from_slice(&out.stdout)?;
        let target = PathBuf::from(meta["target_directory"].as_str().unwrap_or("target"));
        let manifest_path = self.root.join("Cargo.toml").canonicalize()?;
        let pkg = meta["packages"]
            .as_array()
            .and_then(|p| {
                p.iter().find(|p| p["manifest_path"].as_str().map(PathBuf::from).as_deref() == Some(&manifest_path))
            })
            .or_else(|| meta["packages"].get(0))
            .context("no package in Cargo.toml")?;
        let name = pkg["name"].as_str().unwrap_or_default().replace('-', "_");
        Ok(target.join("wasm32-wasip2/release").join(format!("{name}.wasm")))
    }

    /// Reads `migrations/<type>/*.sql`, sorted by file name.
    pub fn migrations(&self) -> Result<BTreeMap<String, Vec<Migration>>> {
        read_migrations(&self.root)
    }

    pub fn manifest(&self, wasm: &[u8]) -> Result<Manifest> {
        Manifest::build(wasm, self.app(), self.migrations()?, self.cfg.http.clone(), self.cfg.limits.clone())
    }

    /// Paths whose changes trigger a rebuild in `statex dev`.
    pub fn watched(&self) -> Vec<PathBuf> {
        match &self.cfg.build {
            Some(b) if !b.watch.is_empty() => {
                let mut v: Vec<_> = b.watch.iter().map(|p| self.root.join(p)).collect();
                v.extend(["migrations", "statex.toml"].iter().map(|p| self.root.join(p)));
                v
            }
            Some(b) => {
                let skip = |n: &str| n.starts_with('.') || n == "target" || n == "__pycache__" || n == "node_modules";
                let out = self.root.join(&b.wasm);
                std::fs::read_dir(&self.root)
                    .into_iter()
                    .flatten()
                    .flatten()
                    .map(|e| e.path())
                    .filter(|p| *p != out && !p.file_name().is_some_and(|n| skip(&n.to_string_lossy())))
                    .collect()
            }
            None => ["src", "wit", "migrations", "statex.toml", "Cargo.toml"].iter().map(|p| self.root.join(p)).collect(),
        }
    }
}

/// Adds `apps` to `[calls] apps` in `<root>/statex.toml`, keeping the rest of
/// the file as is. Returns whether the file changed.
pub fn add_calls(root: &Path, apps: &[String]) -> Result<bool> {
    let path = root.join("statex.toml");
    let text = std::fs::read_to_string(&path)?;
    let cfg: StatexToml = toml::from_str(&text)?;
    let mut list = cfg.calls.apps.clone();
    for a in apps {
        if !list.contains(a) {
            list.push(a.clone());
        }
    }
    if list == cfg.calls.apps {
        return Ok(false);
    }
    let rendered = format!("apps = [{}]", list.iter().map(|a| format!("{a:?}")).collect::<Vec<_>>().join(", "));
    let new = match text.lines().position(|l| l.trim() == "[calls]") {
        None => format!(
            "{}\n\n[calls]\n# Apps whose actors this app calls; `statex calls sync` generates their clients.\n{rendered}\n",
            text.trim_end()
        ),
        Some(i) => {
            let lines: Vec<&str> = text.lines().collect();
            let end = lines[i + 1..].iter().position(|l| l.trim_start().starts_with('[')).map_or(lines.len(), |p| i + 1 + p);
            let mut out: Vec<String> = lines[..=i].iter().map(|l| l.to_string()).collect();
            let mut j = i + 1;
            let mut done = false;
            while j < end {
                let l = lines[j];
                if !done && l.trim_start().starts_with("apps") && l.contains('=') {
                    // Skip a multi-line array.
                    let mut k = j;
                    while !lines[k].contains(']') && k + 1 < end {
                        k += 1;
                    }
                    out.push(rendered.clone());
                    done = true;
                    j = k + 1;
                    continue;
                }
                out.push(l.to_string());
                j += 1;
            }
            if !done {
                out.insert(i + 1, rendered.clone());
            }
            out.extend(lines[end..].iter().map(|l| l.to_string()));
            out.join("\n") + "\n"
        }
    };
    let check: StatexToml = toml::from_str(&new).context("update statex.toml")?;
    anyhow::ensure!(check.calls.apps == list, "could not update [calls] in statex.toml; edit it by hand");
    std::fs::write(&path, new)?;
    Ok(true)
}

/// Reads `<root>/migrations/<type>/*.sql`, sorted by file name.
pub fn read_migrations(root: &Path) -> Result<BTreeMap<String, Vec<Migration>>> {
    let mut out = BTreeMap::new();
    let dir = root.join("migrations");
    if !dir.exists() {
        return Ok(out);
    }
    for e in std::fs::read_dir(&dir)? {
        let e = e?;
        if !e.file_type()?.is_dir() {
            continue;
        }
        let ty = e.file_name().to_string_lossy().to_string();
        let mut v = Vec::new();
        for f in std::fs::read_dir(e.path())? {
            let p = f?.path();
            if p.extension().is_some_and(|x| x == "sql") {
                v.push(Migration {
                    name: p.file_name().unwrap().to_string_lossy().into(),
                    sql: std::fs::read_to_string(&p)?,
                });
            }
        }
        v.sort_by(|a, b| a.name.cmp(&b.name));
        out.insert(ty, v);
    }
    Ok(out)
}

/// Actor types (from `<root>/wit`) and migrations of an app's source tree,
/// with the same consistency checks a deploy applies.
pub fn source_surface(root: &Path) -> Result<(Vec<statex_runtime::ActorType>, BTreeMap<String, Vec<Migration>>)> {
    let types = statex_runtime::inspect_wit(&root.join("wit"), None)?.types;
    let migrations = read_migrations(root)?;
    for t in migrations.keys() {
        if !types.iter().any(|x| &x.name == t) {
            bail!("migrations/{t}/ does not match any exported actor type");
        }
    }
    Ok((types, migrations))
}

/// Full verification: imports, exports, linking, instantiation and migrations.
pub fn verify(wasm: &[u8], manifest: &Manifest) -> Result<()> {
    let rt = Runtime::shared()?;
    let code = rt.load(wasm, manifest.clone())?;
    for t in &manifest.types {
        let dir = tempfile::tempdir()?;
        let conn = open_db(&dir.path().join("verify.db"))?;
        conn.execute_batch("BEGIN")?;
        let migrations = manifest.migrations.get(&t.name).cloned().unwrap_or_default();
        apply_migrations(&conn, &migrations).with_context(|| format!("migrations of actor type {}", t.name))?;
        let id = ActorIdentity { app: manifest.app.clone(), actor_type: t.name.clone(), key: "verify".into(), epoch: 0 };
        code.instantiate(id, statex_runtime::database::sqlite_handle(Arc::new(Mutex::new(conn))))
            .with_context(|| format!("instantiate for {}", t.name))?;
    }
    Ok(())
}

pub use statex_runtime::fmt_ty;

pub fn print_summary(manifest: &Manifest, wasm_len: usize) {
    let size = if wasm_len > 0 { format!("{} KB, ", wasm_len.div_ceil(1024)) } else { String::new() };
    println!("app {}  ({size}sha256 {})", manifest.app, &manifest.sha256[..12]);
    for t in &manifest.types {
        let n = manifest.migrations.get(&t.name).map_or(0, |m| m.len());
        println!("  actor type {}  ({} migration{})", t.name, n, if n == 1 { "" } else { "s" });
        for m in &t.methods {
            let params = m.params.iter().map(|p| format!("{}: {}", p.name, fmt_ty(&p.ty))).collect::<Vec<_>>().join(", ");
            let ret = m.result.as_ref().map(|r| format!(" -> {}", fmt_ty(r))).unwrap_or_default();
            println!("    {}({params}){ret}", m.name);
        }
    }
    for c in &manifest.calls {
        println!("  calls {} {}  ({} method{})", c.app, c.actor_type, c.methods.len(), if c.methods.len() == 1 { "" } else { "s" });
    }
}

/// A plausible example JSON value for a type (used in hints).
pub fn sample(t: &Ty) -> serde_json::Value {
    use serde_json::json;
    match t {
        Ty::Bool => json!(true),
        Ty::F32 | Ty::F64 => json!(1.5),
        Ty::Char => json!("a"),
        Ty::String => json!("hello"),
        Ty::List { .. } | Ty::Flags { .. } => json!([]),
        Ty::Option { .. } => json!(null),
        Ty::Tuple { items } => items.iter().map(sample).collect(),
        Ty::Record { fields, .. } => fields.iter().map(|f| (f.name.clone(), sample(&f.ty))).collect(),
        Ty::Enum { cases, .. } => json!(cases.first()),
        Ty::Variant { cases, .. } => json!({ "tag": cases.first().map(|c| c.name.clone()) }),
        Ty::Result { ok, .. } => json!({ "ok": ok.as_deref().map(sample) }),
        _ => json!(1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guest_limits_parse_and_validate_before_building() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("statex.toml");
        std::fs::write(&path, "[app]\nname = \"budgeted\"\n[limits]\ntimeout_ms = 20\nmemory_mb = 8\nfuel = 1000\nrps = 2\nburst = 3\nmax_concurrent = 1\n").unwrap();
        let project = Project::find(dir.path()).unwrap();
        assert_eq!(project.cfg.limits.fuel, Some(1000));
        assert_eq!(project.cfg.limits.rps, Some(2));
        assert_eq!(project.cfg.limits.burst, Some(3));
        assert_eq!(project.cfg.limits.max_concurrent, Some(1));
        for limits in ["rps = 0", "fuel = 0", "memory_mb = 0", "timeout_ms = 0", "max_concurrent = 0", "burst = 3", "cpu_ms = 10"] {
            std::fs::write(&path, format!("[app]\nname = \"budgeted\"\n[limits]\n{limits}\n")).unwrap();
            assert!(Project::find(dir.path()).is_err(), "{limits} must not be silently accepted");
        }
    }

    #[test]
    fn repository_manifests_use_canonical_app_and_callee_names() {
        fn walk(dir: &Path, count: &mut usize) {
            if !dir.exists() {
                return;
            }
            let manifest = dir.join("statex.toml");
            if manifest.is_file() {
                let cfg: StatexToml = toml::from_str(&std::fs::read_to_string(&manifest).unwrap()).unwrap();
                for app in std::iter::once(&cfg.app.name).chain(&cfg.calls.apps).chain(cfg.calls.paths.keys()) {
                    statex_runtime::validate_app_name(app)
                        .unwrap_or_else(|e| panic!("{}: {e}", manifest.display()));
                    *count += 1;
                }
                return;
            }
            for entry in std::fs::read_dir(dir).unwrap() {
                let entry = entry.unwrap();
                let name = entry.file_name();
                if entry.file_type().unwrap().is_dir()
                    && !["target", "node_modules", "__pycache__"].contains(&name.to_str().unwrap_or(""))
                    && !name.to_string_lossy().starts_with('.')
                {
                    walk(&entry.path(), count);
                }
            }
        }
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut count = 0;
        walk(&root.join("apps"), &mut count);
        walk(&root.join("examples"), &mut count);
        assert!(count > 0, "repository app-name audit found no manifests");
    }
}
