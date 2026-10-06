//! Optional workspace support. `statex-workspace.toml` configures app
//! discovery and shared dependencies; manifest names are independent of paths.
//! Apps can link the shared host WIT and guest SDKs
//! instead of copying them, Rust apps share one Cargo workspace, and CI can
//! check every app and regenerate the registry in one pass.

use std::path::{Component, Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::Deserialize;

use crate::project::Project;

pub const FILE: &str = "statex-workspace.toml";

/// `statex-workspace.toml`. Paths are relative to the workspace root.
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorkspaceToml {
    /// Directory recursively searched for projects, with no required nesting.
    pub apps: PathBuf,
    /// Directory containing `statex-host.wit`.
    pub host_wit: PathBuf,
    /// The `statex-guest` Rust crate.
    pub guest_sdk: PathBuf,
    /// Directory containing the Python guest helper `statex.py`.
    pub python_guest: PathBuf,
    /// Output of `statex registry build`.
    pub registry: PathBuf,
}

impl Default for WorkspaceToml {
    fn default() -> Self {
        Self {
            apps: "apps".into(),
            host_wit: "wit".into(),
            guest_sdk: "crates/guest".into(),
            python_guest: "sdk/python-guest".into(),
            registry: "registry".into(),
        }
    }
}

pub struct Workspace {
    pub root: PathBuf,
    pub cfg: WorkspaceToml,
}

impl Workspace {
    /// Finds `statex-workspace.toml` in `start` or its parents.
    pub fn find(start: &Path) -> Result<Option<Workspace>> {
        let start = start.canonicalize().with_context(|| format!("{}", start.display()))?;
        for dir in start.ancestors() {
            let f = dir.join(FILE);
            if f.exists() {
                let cfg: WorkspaceToml =
                    toml::from_str(&std::fs::read_to_string(&f)?).with_context(|| format!("parse {}", f.display()))?;
                return Ok(Some(Workspace { root: dir.to_path_buf(), cfg }));
            }
        }
        Ok(None)
    }

    pub fn require(start: &Path) -> Result<Workspace> {
        Self::find(start)?.with_context(|| format!("no {FILE} found in {} or its parents", start.display()))
    }

    pub fn path(&self, p: &Path) -> PathBuf {
        self.root.join(p)
    }

    /// How `from` (inside the workspace) should refer to configured path `p`:
    /// relative when `p` is workspace-relative, absolute when configured absolute.
    pub fn reach(&self, from: &Path, p: &Path) -> Result<PathBuf> {
        if p.is_absolute() {
            return Ok(p.to_path_buf());
        }
        relative(from, &self.path(p))
    }

    pub fn apps_dir(&self) -> PathBuf {
        self.path(&self.cfg.apps)
    }

    /// Default scaffold directory for an app, not an identity lookup.
    pub fn app_dir(&self, app: &str) -> PathBuf {
        app.split('/').fold(self.apps_dir(), |p, s| p.join(s))
    }

    /// Path relative to the workspace root, with `/` separators.
    pub fn rel(&self, p: &Path) -> String {
        let root = self.root.canonicalize().unwrap_or_else(|_| self.root.clone());
        let p = p.canonicalize().unwrap_or_else(|_| p.to_path_buf());
        p.strip_prefix(&root).unwrap_or(&p).components().map(|c| c.as_os_str().to_string_lossy()).collect::<Vec<_>>().join("/")
    }

    /// Every project under the apps directory, sorted by path.
    pub fn projects(&self) -> Result<Vec<Project>> {
        fn walk(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
            if dir.join("statex.toml").exists() {
                out.push(dir.to_path_buf());
                return Ok(());
            }
            for e in std::fs::read_dir(dir)? {
                let e = e?;
                let name = e.file_name().to_string_lossy().into_owned();
                if e.file_type()?.is_dir() && !(name.starts_with('.') || ["target", "node_modules", "__pycache__"].contains(&name.as_str())) {
                    walk(&e.path(), out)?;
                }
            }
            Ok(())
        }
        let mut dirs = Vec::new();
        if self.apps_dir().exists() {
            walk(&self.apps_dir(), &mut dirs)?;
        }
        dirs.sort();
        let projects: Vec<_> = dirs.iter().map(|d| Project::find(d)).collect::<Result<_>>()?;
        let mut seen = std::collections::BTreeMap::new();
        for p in &projects {
            statex_runtime::validate_app_name(p.app())?;
            if let Some(prev) = seen.insert(p.app(), &p.root) {
                bail!("app name {:?} is used by both {} and {}", p.app(), prev.display(), p.root.display());
            }
        }
        Ok(projects)
    }

}

/// Creates `statex-workspace.toml` and the apps directory in `root`. `statex_dir` is the statex checkout (or vendored copy) that
/// provides the host WIT and guest SDKs.
pub fn init(root: &Path, statex_dir: &Path) -> Result<Vec<String>> {
    let root = &root.canonicalize()?;
    let f = root.join(FILE);
    if f.exists() {
        bail!("{} already exists", f.display());
    }
    let rel = |sub: &str| -> Result<String> {
        let p = statex_dir.join(sub);
        if !p.exists() {
            bail!("{sub} not found under {}; pass --statex <checkout>", statex_dir.display());
        }
        // A checkout outside the repo (not vendored/submoduled) is machine-specific; keep it absolute.
        let abs = p.canonicalize()?;
        if !abs.starts_with(root) {
            return Ok(slash(&abs));
        }
        Ok(slash(&relative(root, &p).with_context(|| format!("{sub} not found under {}; pass --statex <checkout>", statex_dir.display()))?))
    };
    let mut created = vec![];
    std::fs::write(
        &f,
        format!(
            "# Optional statex workspace: discover apps recursively; names come from statex.toml.\n\
             # Relative paths are resolved from this file.\n\
             apps = \"apps\"\n\
             host_wit = {:?}\n\
             guest_sdk = {:?}\n\
             python_guest = {:?}\n\
             registry = \"registry\"\n",
            rel("wit")?,
            rel("crates/guest")?,
            rel("sdk/python-guest")?
        ),
    )?;
    created.push(FILE.to_string());
    std::fs::create_dir_all(root.join("apps"))?;
    created.push("apps/".into());
    Ok(created)
}

/// Lexical relative path from directory `from` to `to`. Symlinks are not
/// resolved, so links inside the workspace stay workspace-relative.
pub fn relative(from: &Path, to: &Path) -> Result<PathBuf> {
    if !to.exists() {
        bail!("{} does not exist", to.display());
    }
    let (from, to) = (normalize(&std::path::absolute(from)?), normalize(&std::path::absolute(to)?));
    let (a, b): (Vec<Component>, Vec<Component>) = (from.components().collect(), to.components().collect());
    let common = a.iter().zip(&b).take_while(|(x, y)| x == y).count();
    if common == 0 {
        bail!("{} and {} share no common root", from.display(), to.display());
    }
    let mut out = PathBuf::new();
    for _ in common..a.len() {
        out.push("..");
    }
    for c in &b[common..] {
        out.push(c.as_os_str());
    }
    Ok(out)
}

/// Removes `.` and resolves `..` lexically.
fn normalize(p: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            c => out.push(c.as_os_str()),
        }
    }
    out
}

/// Path as a string with `/` separators (for TOML and shell commands).
pub fn slash(p: &Path) -> String {
    if p.is_absolute() {
        return p.to_string_lossy().replace('\\', "/");
    }
    p.components().map(|c| c.as_os_str().to_string_lossy()).collect::<Vec<_>>().join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_paths() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join("apps/payments/shop")).unwrap();
        std::fs::create_dir_all(d.path().join("wit")).unwrap();
        assert_eq!(relative(&d.path().join("apps/payments/shop"), &d.path().join("wit")).unwrap(), PathBuf::from("../../../wit"));
    }

    #[test]
    fn discovery_rejects_duplicate_manifest_names() {
        let d = tempfile::tempdir_in(".").unwrap();
        let ws = Workspace { root: d.path().to_path_buf(), cfg: WorkspaceToml::default() };
        for path in ["a", "deep/b"] {
            let root = ws.apps_dir().join(path);
            std::fs::create_dir_all(&root).unwrap();
            std::fs::write(root.join("statex.toml"), "[app]\nname = \"shop\"\n").unwrap();
        }
        assert!(ws.projects().err().unwrap().to_string().contains("used by both"));
    }
}
