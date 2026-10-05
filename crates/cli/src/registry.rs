//! `statex registry build`: a generated, browsable index of every app in a
//! workspace. The source of truth stays next to each app's code; the
//! registry is a derived view that CI regenerates (and `--check` verifies).
//!
//! ```text
//! <registry>/index.json          every app: name, path, actor types, method signatures, calls
//! <registry>/<team>/<app>.wit    copy of the app's wit/app.wit
//! ```

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use serde_json::json;
use statex_runtime::{fmt_ty, sha256_hex};

use crate::workspace::Workspace;

/// Expected registry files, keyed by path relative to the registry directory.
pub fn generate(ws: &Workspace) -> Result<BTreeMap<PathBuf, String>> {
    let mut files = BTreeMap::new();
    let mut apps = Vec::new();
    let mut seen = BTreeMap::new();
    for p in ws.projects()? {
        let path = ws.rel(&p.root);
        if let Some(prev) = seen.insert(p.app().to_string(), path.clone()) {
            bail!("app name {:?} is used by both {prev} and {path}", p.app());
        }
        let (types, migrations) = p.source_surface().with_context(|| format!("read {path}"))?;
        let wit_src = std::fs::read_to_string(p.root.join("wit/app.wit")).with_context(|| format!("read {path}/wit/app.wit"))?;
        let wit_file = format!("{}.wit", p.app());
        let lang = if p.root.join("Cargo.toml").exists() {
            "rust"
        } else if p.root.join("app.py").exists() {
            "python"
        } else {
            "other"
        };
        let calls = statex_runtime::inspect_wit(&p.root.join("wit"), None).with_context(|| format!("read {path}/wit"))?.calls;
        let mut entry = json!({
            "app": p.app(),
            "path": path,
            "lang": lang,
            "wit": wit_file,
            "wit_sha256": sha256_hex(wit_src.as_bytes()),
            "types": types.iter().map(|t| json!({
                "name": t.name,
                "docs": t.docs,
                "migrations": migrations.get(&t.name).map_or(0, |m| m.len()),
                "methods": t.methods.iter().map(|m| {
                    let params = m.params.iter().map(|p| format!("{}: {}", p.name, fmt_ty(&p.ty))).collect::<Vec<_>>().join(", ");
                    let ret = m.result.as_ref().map(|r| format!(" -> {}", fmt_ty(r))).unwrap_or_default();
                    format!("{}({params}){ret}", m.name)
                }).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        });
        if !calls.is_empty() {
            // Actor types this app calls, as `app type`; reverse lookups find an app's callers.
            entry["calls"] = json!(calls.iter().map(|c| json!({ "app": c.app, "type": c.actor_type })).collect::<Vec<_>>());
        }
        apps.push(entry);
        files.insert(PathBuf::from(&wit_file), wit_src);
    }
    let index = json!({
        "generated_by": "statex registry build; do not edit by hand",
        "apps": apps,
    });
    files.insert(PathBuf::from("index.json"), serde_json::to_string_pretty(&index)? + "\n");
    Ok(files)
}

/// Files under the registry directory, relative to it.
fn existing(dir: &std::path::Path) -> Result<BTreeMap<PathBuf, String>> {
    fn walk(base: &std::path::Path, dir: &std::path::Path, out: &mut BTreeMap<PathBuf, String>) -> Result<()> {
        for e in std::fs::read_dir(dir)? {
            let p = e?.path();
            if p.is_dir() {
                walk(base, &p, out)?;
            } else {
                out.insert(p.strip_prefix(base)?.to_path_buf(), std::fs::read_to_string(&p).unwrap_or_default());
            }
        }
        Ok(())
    }
    let mut out = BTreeMap::new();
    if dir.exists() {
        walk(dir, dir, &mut out)?;
    }
    Ok(out)
}

/// Writes the registry, removing stale files. Returns the number of apps.
pub fn build(ws: &Workspace) -> Result<usize> {
    let dir = ws.path(&ws.cfg.registry);
    let want = generate(ws)?;
    for stale in existing(&dir)?.keys().filter(|k| !want.contains_key(*k)) {
        std::fs::remove_file(dir.join(stale))?;
    }
    for (rel, content) in &want {
        let p = dir.join(rel);
        std::fs::create_dir_all(p.parent().unwrap())?;
        std::fs::write(&p, content)?;
    }
    // drop now-empty team directories
    if let Ok(rd) = std::fs::read_dir(&dir) {
        for e in rd.flatten() {
            if e.path().is_dir() && std::fs::read_dir(e.path()).is_ok_and(|mut d| d.next().is_none()) {
                let _ = std::fs::remove_dir(e.path());
            }
        }
    }
    Ok(want.len() - 1)
}

/// Lists differences between the registry on disk and what `build` would write.
pub fn diff(ws: &Workspace) -> Result<Vec<String>> {
    let want = generate(ws)?;
    let have = existing(&ws.path(&ws.cfg.registry))?;
    let mut out = Vec::new();
    for (k, v) in &want {
        match have.get(k) {
            None => out.push(format!("missing {}", k.display())),
            Some(h) if h != v => out.push(format!("outdated {}", k.display())),
            _ => {}
        }
    }
    out.extend(have.keys().filter(|k| !want.contains_key(*k)).map(|k| format!("stale {}", k.display())));
    Ok(out)
}
