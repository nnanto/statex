//! A statex app project: `statex.toml` + a Rust crate building a component.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use statex_runtime::{apply_migrations, open_db, ActorIdentity, HttpPolicy, Limits, Manifest, Migration, Runtime, Ty};

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
    /// Team that owns the app name in the cluster (see `statex deploy`).
    /// Defaults to the namespace of a `team/app` name.
    #[serde(default)]
    pub owner: Option<String>,
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
                return Ok(Project { root: dir.to_path_buf(), cfg });
            }
        }
        bail!("no statex.toml found in {} or its parents (create a project with `statex new <name>`)", start.display())
    }

    pub fn app(&self) -> &str {
        &self.cfg.app.name
    }

    /// The deploy owner from statex.toml, else the app's namespace.
    pub fn owner(&self) -> Option<String> {
        self.cfg.app.owner.clone().or_else(|| self.app().split_once('/').map(|(ns, _)| ns.to_string()))
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
        code.instantiate(id, Arc::new(Mutex::new(conn))).with_context(|| format!("instantiate for {}", t.name))?;
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
