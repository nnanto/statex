//! `statex check`: pre-merge checks for one app or every app of a workspace.
//!
//! - the app name is valid and, in a workspace, equals the app's path;
//! - WIT and migrations are consistent (read from source, no build needed);
//! - with `--against <git ref>`, nothing breaks compared to that version
//!   (same rules as `statex deploy`, see `statex_runtime::compat`).

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};
use statex_runtime::{breaking_changes, validate_app_name, Surface};

use crate::project::{source_surface, Project, StatexToml};
use crate::workspace::{slash, Workspace};

pub struct Report {
    pub app: String,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

pub fn check(p: &Project, ws: Option<&Workspace>, against: Option<&str>, allow_breaking: bool) -> Report {
    let mut r = Report { app: p.app().to_string(), errors: vec![], warnings: vec![] };
    if let Err(e) = validate_app_name(p.app()) {
        r.errors.push(format!("{e:#}"));
    }
    if let Some(ws) = ws {
        match ws.expected_name(&p.root) {
            None => r.errors.push(format!(
                "app is outside the workspace apps directory {}; apps live at {}/<team>/<app>",
                ws.rel(&ws.apps_dir()),
                ws.rel(&ws.apps_dir())
            )),
            Some(exp) if exp.matches('/').count() != 1 => r.errors.push(format!(
                "app at {} must live at {}/<team>/<app>",
                ws.rel(&p.root),
                ws.rel(&ws.apps_dir())
            )),
            Some(exp) if exp != p.app() => r.errors.push(format!(
                "app name {:?} does not match its path {}; set `name = {exp:?}` in statex.toml",
                p.app(),
                ws.rel(&p.root)
            )),
            Some(_) => {}
        }
    }
    let new = match source_surface(&p.root) {
        Ok(s) => s,
        Err(e) => {
            r.errors.push(format!("{e:#}"));
            return r;
        }
    };
    check_calls(p, ws, &mut r);
    if let Some(rev) = against {
        match old_version(&p.root, rev) {
            Ok(None) => r.warnings.push(format!("new app (not present at {rev})")),
            Ok(Some((old_name, old_root, _tmp))) => {
                let mut breaking = Vec::new();
                if old_name != p.app() {
                    breaking.push(format!("app was renamed from {old_name:?} to {:?}; clients and existing actors use the old name", p.app()));
                }
                match source_surface(&old_root) {
                    Ok(old) => breaking.extend(breaking_changes(
                        Surface { types: &old.0, migrations: &old.1 },
                        Surface { types: &new.0, migrations: &new.1 },
                    )),
                    Err(e) => r.warnings.push(format!("cannot read the version at {rev}, skipping compatibility: {e:#}")),
                }
                let label = |c: String| format!("breaking change vs {rev}: {c}");
                if allow_breaking {
                    r.warnings.extend(breaking.into_iter().map(label));
                } else {
                    r.errors.extend(breaking.into_iter().map(label));
                }
            }
            Err(e) => r.errors.push(format!("{e:#}")),
        }
    }
    r
}

fn git(dir: &Path, args: &[&str]) -> Result<Vec<u8>> {
    let out = Command::new("git").arg("-C").arg(dir).args(args).output().context("run git")?;
    if !out.status.success() {
        bail!("git {}: {}", args.join(" "), String::from_utf8_lossy(&out.stderr).trim());
    }
    Ok(out.stdout)
}

/// Materializes the app's `statex.toml`, `wit/` and `migrations/` as of git
/// revision `rev` into a temporary directory. `wit/deps` is taken from the
/// working tree: host interfaces are not part of the app's contract.
/// Returns `None` when the app did not exist at `rev`.
fn old_version(root: &Path, rev: &str) -> Result<Option<(String, PathBuf, tempfile::TempDir)>> {
    let top = PathBuf::from(String::from_utf8(git(root, &["rev-parse", "--show-toplevel"])?)?.trim());
    let top = top.canonicalize()?;
    let rel = slash(root.canonicalize()?.strip_prefix(&top).context("project is outside the git repository")?);
    let prefix = if rel.is_empty() { String::new() } else { format!("{rel}/") };
    git(&top, &["rev-parse", "--verify", "--quiet", &format!("{rev}^{{commit}}")])
        .with_context(|| format!("unknown git revision {rev:?}"))?;
    let listing = String::from_utf8(git(&top, &["ls-tree", "-r", "--full-tree", rev, "--", &format!("{prefix}statex.toml"), &format!("{prefix}wit"), &format!("{prefix}migrations")])?)?;
    let tmp = tempfile::tempdir()?;
    let mut name = None;
    for line in listing.lines() {
        let Some((meta, path)) = line.split_once('\t') else { continue };
        let mode = meta.split_whitespace().next().unwrap_or("");
        let Some(sub) = path.strip_prefix(&prefix) else { continue };
        if mode == "120000" || sub.starts_with("wit/deps/") {
            continue;
        }
        let data = git(&top, &["show", &format!("{rev}:{path}")])?;
        if sub == "statex.toml" {
            let cfg: StatexToml = toml::from_str(&String::from_utf8_lossy(&data)).with_context(|| format!("parse {path} at {rev}"))?;
            name = Some(cfg.app.name);
        }
        let dst = tmp.path().join(sub);
        std::fs::create_dir_all(dst.parent().unwrap())?;
        std::fs::write(dst, data)?;
    }
    let Some(name) = name else { return Ok(None) };
    let deps = root.join("wit/deps");
    if deps.exists() {
        copy_dir(&deps, &tmp.path().join("wit/deps"))?;
    }
    let dir = tmp.path().to_path_buf();
    Ok(Some((name, dir, tmp)))
}

/// Recursive copy that follows symlinks.
fn copy_dir(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for e in std::fs::read_dir(src)? {
        let e = e?;
        let p = e.path();
        if std::fs::metadata(&p)?.is_dir() {
            copy_dir(&p, &dst.join(e.file_name()))?;
        } else {
            std::fs::copy(&p, dst.join(e.file_name()))?;
        }
    }
    Ok(())
}

/// Client interfaces: every imported one is listed under `[calls]`, generated
/// files are up to date, and calls match the callee's source where available.
fn check_calls(p: &Project, ws: Option<&Workspace>, r: &mut Report) {
    let listed = match crate::calls::listed(p) {
        Ok(l) => l,
        Err(e) => return r.errors.push(format!("{e:#}")),
    };
    let calls = match statex_runtime::inspect_wit(&p.root.join("wit"), None) {
        Ok(i) => i.calls,
        Err(e) => return r.errors.push(format!("{e:#}")),
    };
    for c in &calls {
        if !listed.contains(&c.app) {
            r.errors.push(format!("{} is imported but app {} is not listed under [calls] apps in statex.toml", c.import, c.app));
            continue;
        }
        if let Some(Ok(types)) = crate::calls::local_types(p, ws, &c.app) {
            for m in statex_runtime::call_mismatches_in(c, &c.app, &types) {
                r.errors.push(format!("{}: {m} (run `statex calls sync`)", c.import));
            }
        }
    }
    match crate::calls::local_plan(p, ws) {
        Ok((want, remote)) => {
            let have = crate::calls::on_disk(p);
            for d in crate::calls::diff(&want, &have) {
                r.errors.push(format!("client interfaces: {d} (run `statex calls sync`)"));
            }
            for a in remote {
                r.warnings.push(format!("app {a} is not in this workspace; its client interface was not checked against its source"));
            }
        }
        Err(e) => r.errors.push(format!("{e:#}")),
    }
}
