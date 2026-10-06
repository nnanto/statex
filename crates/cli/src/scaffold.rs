//! `statex new` and `statex add-actor`.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use statex_runtime::{validate_app_name, validate_name};

use crate::workspace::{relative, slash, Workspace};

use crate::project::Project;

/// Explicit SDK location; never embed the machine where the CLI was built.
pub fn default_sdk_path() -> Option<PathBuf> {
    std::env::var_os("STATEX_GUEST_PATH").map(PathBuf::from)
}

fn actor_name(actor: &str) -> Result<()> {
    validate_name("actor type", actor)?;
    if !crate::calls::is_wit_id(actor) {
        bail!("actor type {actor:?} cannot be named in WIT: use lowercase words separated by single dashes, each word starting with a letter");
    }
    Ok(())
}

pub const HOST_WIT: &str = include_str!("../../../wit/statex-host.wit");
const PY_GUEST: &str = include_str!("../../../sdk/python-guest/statex.py");
const PY_TESTING: &str = include_str!("../../../sdk/python-guest/statex_testing.py");
const COMPONENTIZE_PY: &str = "componentize-py==0.25.1";

fn pascal(s: &str) -> String {
    s.split(['-', '_']).filter(|p| !p.is_empty()).map(|p| p[..1].to_uppercase() + &p[1..]).collect()
}

fn on_path(bin: &str) -> bool {
    std::env::var_os("PATH")
        .map(|p| std::env::split_paths(&p).any(|d| d.join(bin).is_file()))
        .unwrap_or(false)
}

/// A Python project built with componentize-py. Inside a workspace the host
/// WIT and `statex.py` are linked from the workspace instead of copied.
pub fn new_python_project(name: &str, dir: &Path, actor: &str, ws: Option<&Workspace>) -> Result<()> {
    validate_app_name(name)?;
    actor_name(actor)?;
    if dir.exists() && std::fs::read_dir(dir)?.next().is_some() {
        bail!("{} already exists and is not empty", dir.display());
    }
    std::fs::create_dir_all(dir)?;
    let slug = name.replace('/', "-");
    let w = |rel: &str, content: String| -> Result<()> {
        let p = dir.join(rel);
        std::fs::create_dir_all(p.parent().unwrap())?;
        std::fs::write(&p, content).with_context(|| format!("write {}", p.display()))
    };
    // componentize-py needs Python >= 3.10; uvx fetches a suitable one if needed.
    let tool = if on_path("componentize-py") {
        "componentize-py".to_string()
    } else {
        format!("uvx --python 3.12 --from {COMPONENTIZE_PY} componentize-py")
    };
    let python_path = match ws {
        Some(ws) => format!(" -p . -p {}", slash(&ws.reach(dir, &ws.cfg.python_guest)?)),
        None => String::new(),
    };
    w(
        "statex.toml",
        format!(
            r#"# statex app manifest
[app]
name = "{name}"

# Hosts the http-client interface may call: "api.example.com", "*.example.com" or "*".
[http]
allow = []

[limits]
timeout_ms = 5000
memory_mb = 64
# Optional per-invocation fuel and per-app/node rate/concurrency budgets:
# fuel = 10000000
# rps = 100
# burst = 100
# max_concurrent = 16

# Python actors are compiled to a WebAssembly component with componentize-py
# (pip install {COMPONENTIZE_PY}, or let uvx fetch it).
[build]
command = "{tool} -d wit -w app componentize{python_path} app -o app.wasm"
wasm = "app.wasm"
"#
        ),
    )?;
    w(
        "wit/app.wit",
        format!(
            r#"package local:{slug}@0.1.0;

/// Each exported interface is an actor type; each function is a method.
/// Every key (e.g. "alice") is an independent actor with its own SQLite database.
interface {actor} {{
  /// Adds `by` and returns the new value.
  increment: func(by: s64) -> s64;
  /// Returns the current value.
  get: func() -> s64;
}}

world app {{
  import statex:host/context@0.1.0;
  import statex:host/sql@0.1.0;
  import statex:host/http-client@0.1.0;
  import statex:host/log@0.1.0;
  import statex:host/alarms@0.1.0;
  import statex:host/actors@0.1.0;

  export {actor};
}}
"#,
            slug = crate::calls::wid(&slug),
            actor = crate::calls::wid(actor),
        ),
    )?;
    ensure_host_wit(dir, ws)?;
    w(
        &format!("migrations/{actor}/0001_init.sql"),
        format!(
            "-- Applied once per actor, inside a transaction, the first time the actor is used.\n\
             CREATE TABLE {t} (id INTEGER PRIMARY KEY CHECK (id = 0), value INTEGER NOT NULL);\n\
             INSERT INTO {t} (id, value) VALUES (0, 0);\n",
            t = snake(actor)
        ),
    )?;
    let mut extra_paths = vec![".statex/bindings".to_string()];
    match ws {
        Some(ws) => extra_paths.push(slash(&ws.reach(dir, &ws.cfg.python_guest)?)),
        None => {
            w("statex.py", PY_GUEST.to_string())?;
            w("statex_testing.py", PY_TESTING.to_string())?;
        }
    }
    w(
        "pyproject.toml",
        format!(
            "# Lets editors and type checkers see the typed `wit_world` bindings, which\n\
             # `statex build`, `statex test` and `statex dev` generate into .statex/bindings.\n\
             [tool.pyright]\nextraPaths = [{}]\n\n[tool.mypy]\nmypy_path = \"{}\"\n",
            extra_paths.iter().map(|p| format!("{p:?}")).collect::<Vec<_>>().join(", "),
            extra_paths.join(":")
        ),
    )?;
    w(
        "conftest.py",
        "# Resets the statex mock host (actors, stubs, logs) before every test.\npytest_plugins = [\"statex_testing\"]\n".into(),
    )?;
    w(
        "test_app.py",
        format!(
            r#"# Unit tests against the mock host; run them with `statex test`.
# Each (actor type, key) gets its own in-memory database with migrations applied,
# and each `call` is one transaction, like on a node.
from statex_testing import call

from app import {cls}


def test_increments_per_key():
    assert call("{actor}", "alice", {cls}().increment, 2) == 2
    assert call("{actor}", "alice", {cls}().increment, 3) == 5
    assert call("{actor}", "bob", {cls}().get) == 0
"#,
            cls = pascal(actor)
        ),
    )?;
    w(
        "app.py",
        format!(
            r#"# Each exported WIT interface is implemented by a class with the same
# (PascalCase) name. Every call runs in one transaction: returning commits,
# raising rolls back. For result<T, E> methods, `raise statex.Err(e)` returns err(e).
from wit_world import exports

import statex


class {cls}(exports.{cls}):
    def increment(self, by: int) -> int:
        statex.execute("UPDATE {t} SET value = value + ?1 WHERE id = 0", by)
        return self.get()

    def get(self) -> int:
        return statex.query_scalar("SELECT value FROM {t} WHERE id = 0") or 0
"#,
            cls = pascal(actor),
            t = snake(actor)
        ),
    )?;
    w(".gitignore", "/app.wasm
/.statex
__pycache__/
/wit_world/
".into())?;
    Ok(())
}

fn snake(s: &str) -> String {
    s.replace('-', "_")
}

fn export_package(slug: &str) -> String {
    if crate::calls::rid(slug) != snake(slug) {
        format!("app-{slug}")
    } else {
        slug.to_string()
    }
}

/// A Rust project. Inside a workspace it joins the apps' Cargo workspace and
/// depends on the workspace's statex-guest crate by relative path.
pub fn new_project(name: &str, dir: &Path, actor: &str, sdk: Option<PathBuf>, ws: Option<&Workspace>) -> Result<()> {
    validate_app_name(name)?;
    actor_name(actor)?;
    if dir.exists() && std::fs::read_dir(dir)?.next().is_some() {
        bail!("{} already exists and is not empty", dir.display());
    }
    let slug = name.replace('/', "-");
    let package = export_package(&slug);
    let sdk = match ws {
        Some(ws) if sdk.is_none() => ws.path(&ws.cfg.guest_sdk),
        _ => sdk.or_else(default_sdk_path).context("standalone Rust scaffolding needs --sdk <statex-guest path> or STATEX_GUEST_PATH; no published SDK version is assumed")?,
    };
    let sdk = sdk.canonicalize().with_context(|| format!("locate statex-guest at {}", sdk.display()))?;
    let sdk_manifest: toml::Value = toml::from_str(&std::fs::read_to_string(sdk.join("Cargo.toml")).context("read SDK Cargo.toml")?)?;
    if sdk_manifest["package"]["name"].as_str() != Some("statex-guest") {
        bail!("{} is not the statex-guest crate", sdk.display());
    }
    std::fs::create_dir_all(dir)?;
    let sdk = relative(dir, &sdk)?;
    let shared_cargo = ws.is_some_and(|ws| {
        dir.canonicalize().is_ok_and(|dir| {
            ws.apps_dir().canonicalize().is_ok_and(|apps| dir != apps && dir.starts_with(apps))
        })
    });
    // A standalone project is its own Cargo workspace; workspace apps share one.
    let standalone = if shared_cargo {
        ""
    } else {
        "\n[profile.release]\nopt-level = \"s\"\nlto = true\nstrip = true\n\n[workspace]\n"
    };
    let w = |rel: &str, content: String| -> Result<()> {
        let p = dir.join(rel);
        std::fs::create_dir_all(p.parent().unwrap())?;
        std::fs::write(&p, content).with_context(|| format!("write {}", p.display()))
    };
    w(
        "Cargo.toml",
        format!(
            r#"[package]
name = "{slug}"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies]
statex-guest = {{ path = {sdk} }}
wit-bindgen = "0.62"
{standalone}"#,
            sdk = format!("{:?}", slash(&sdk))
        ),
    )?;
    w(
        "statex.toml",
        format!(
            r#"# statex app manifest
[app]
name = "{name}"

# Hosts the http-client interface may call: "api.example.com", "*.example.com" or "*".
[http]
allow = []

[limits]
timeout_ms = 5000
memory_mb = 64
# Optional per-invocation fuel and per-app/node rate/concurrency budgets:
# fuel = 10000000
# rps = 100
# burst = 100
# max_concurrent = 16
"#
        ),
    )?;
    w(
        "wit/app.wit",
        format!(
            r#"package local:{slug}@0.1.0;

/// Each exported interface is an actor type; each function is a method.
/// Every key (e.g. "alice") is an independent actor with its own SQLite database.
interface {actor} {{
  /// Adds `by` and returns the new value.
  increment: func(by: s64) -> s64;
  /// Returns the current value.
  get: func() -> s64;
}}

world app {{
  export {actor};
}}
"#,
            slug = crate::calls::wid(&package),
            actor = crate::calls::wid(actor),
        ),
    )?;
    ensure_host_wit(dir, ws)?;
    w(
        &format!("migrations/{actor}/0001_init.sql"),
        format!(
            "-- Applied once per actor, inside a transaction, the first time the actor is used.\n\
             CREATE TABLE {t} (id INTEGER PRIMARY KEY CHECK (id = 0), value INTEGER NOT NULL);\n\
             INSERT INTO {t} (id, value) VALUES (0, 0);\n",
            t = snake(actor)
        ),
    )?;
    w(
        "src/lib.rs",
        format!(
            r#"use statex_guest::{{params, sql}};

wit_bindgen::generate!({{ path: "wit", world: "app", additional_derives: [PartialEq] }});

use exports::local::{pkg}::{m} as {m};

struct App;

// Each call runs inside its own transaction against this actor's database.
impl {m}::Guest for App {{
    fn increment(by: i64) -> i64 {{
        sql::execute("UPDATE {t} SET value = value + ?1 WHERE id = 0", params![by]).unwrap();
        Self::get()
    }}

    fn get() -> i64 {{
        sql::query_scalar("SELECT value FROM {t} WHERE id = 0", &[]).unwrap().unwrap_or(0)
    }}
}}

export!(App);

#[cfg(test)]
mod tests {{
    use super::*;
    use {m}::Guest as _;
    use statex_guest::testing::call;

    #[test]
    fn increments_per_key() {{
        assert_eq!(call("{actor}", "alice", || App::increment(2)), 2);
        assert_eq!(call("{actor}", "alice", || App::increment(3)), 5);
        assert_eq!(call("{actor}", "bob", App::get), 0);
    }}
}}
"#,
            pkg = crate::calls::rid(&package),
            m = crate::calls::rid(actor),
            t = snake(actor),
        ),
    )?;
    match ws.filter(|_| shared_cargo) {
        Some(ws) => {
            w(".gitignore", "/.statex\n".into())?;
            add_cargo_member(ws, dir)?;
        }
        None => w(".gitignore", "/target\n/.statex\n".into())?,
    }
    Ok(())
}

/// Adds `dir` to the apps' Cargo workspace (`<apps>/Cargo.toml`), creating it
/// on first use.
fn add_cargo_member(ws: &Workspace, dir: &Path) -> Result<()> {
    let apps = ws.apps_dir();
    let manifest = apps.join("Cargo.toml");
    if !manifest.exists() {
        std::fs::write(
            &manifest,
            "# Cargo workspace shared by every Rust statex app (maintained by `statex new`).\n\
             [workspace]\nresolver = \"2\"\nmembers = []\n\n\
             [profile.release]\nopt-level = \"s\"\nlto = true\nstrip = true\n",
        )?;
        std::fs::write(apps.join(".gitignore"), "/target\n")?;
    }
    let text = std::fs::read_to_string(&manifest)?;
    let parsed: toml::Value = toml::from_str(&text).with_context(|| format!("parse {}", manifest.display()))?;
    let mut members: Vec<String> = parsed
        .get("workspace")
        .and_then(|w| w.get("members"))
        .and_then(|m| m.as_array())
        .context("apps Cargo.toml has no [workspace] members array")?
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    let member = slash(&relative(&apps, dir)?);
    if members.contains(&member) {
        return Ok(());
    }
    members.push(member);
    members.sort();
    let start = text.find("members").context("members not found")?;
    let end = start + text[start..].find(']').context("unterminated members array")? + 1;
    let list: String = members.iter().map(|m| format!("    {m:?},\n")).collect();
    let new = format!("{}members = [\n{list}]{}", &text[..start], &text[end..]);
    std::fs::write(&manifest, new)?;
    Ok(())
}

/// Makes `<dir>/wit/deps/statex-host` available: linked to the workspace's
/// host WIT inside a workspace, else a copy of the one this CLI was built with.
/// Returns whether it was created.
pub fn ensure_host_wit(dir: &Path, ws: Option<&Workspace>) -> Result<bool> {
    let link = dir.join("wit/deps/statex-host");
    if link.exists() {
        return Ok(false);
    }
    match ws {
        Some(ws) => link_dir(&ws.path(&ws.cfg.host_wit), &ws.reach(link.parent().unwrap(), &ws.cfg.host_wit)?, &link)?,
        None => {
            std::fs::create_dir_all(&link)?;
            std::fs::write(link.join("statex-host.wit"), HOST_WIT)?;
        }
    }
    Ok(true)
}

/// Symlinks directory `target` at `link` via `rel` (relative when the target
/// is in the repo, so it stays relocatable); copies it where symlinks are unavailable.
#[cfg_attr(unix, allow(unused_variables))]
fn link_dir(target: &Path, rel: &Path, link: &Path) -> Result<()> {
    std::fs::create_dir_all(link.parent().unwrap())?;
    #[cfg(unix)]
    std::os::unix::fs::symlink(rel, link).with_context(|| format!("symlink {}", link.display()))?;
    #[cfg(not(unix))]
    {
        std::fs::create_dir_all(link)?;
        for e in std::fs::read_dir(target)? {
            let e = e?;
            std::fs::copy(e.path(), link.join(e.file_name()))?;
        }
    }
    Ok(())
}

/// Adds a new actor type: WIT interface + world export, a migration and a Rust stub.
pub fn add_actor(project: &Project, actor: &str) -> Result<()> {
    actor_name(actor)?;
    let wit_path = project.root.join("wit/app.wit");
    let wit = std::fs::read_to_string(&wit_path).context("read wit/app.wit")?;
    let actor_wit = crate::calls::wid(actor);
    if wit.contains(&format!("interface {actor_wit} ")) || wit.contains(&format!("interface {actor_wit}{{")) {
        bail!("wit/app.wit already defines interface {actor}");
    }
    let (ns, pkg) = wit
        .lines()
        .find_map(|l| l.trim().strip_prefix("package "))
        .and_then(|p| {
            let p = p.trim_end_matches(';');
            let p = p.split('@').next()?;
            let (ns, pkg) = p.split_once(':')?;
            Some((ns.trim_start_matches('%').to_string(), pkg.trim_start_matches('%').to_string()))
        })
        .context("wit/app.wit has no `package ns:name;` line")?;
    let world_at = wit.find("\nworld ").context("wit/app.wit has no world")? + 1;
    let close = world_at + wit[world_at..].find('}').context("unterminated world")?;
    let mut out = String::new();
    out.push_str(&wit[..world_at]);
    out.push_str(&format!(
        "/// TODO: describe the `{actor}` actor type.\ninterface {} {{\n  /// Stores a value.\n  set: func(value: string);\n  /// Returns the stored value, if any.\n  get: func() -> option<string>;\n}}\n\n",
        crate::calls::wid(actor)
    ));
    out.push_str(&wit[world_at..close]);
    out.push_str(&format!("  export {};\n", crate::calls::wid(actor)));
    out.push_str(&wit[close..]);
    std::fs::write(&wit_path, out)?;

    let mig = project.root.join(format!("migrations/{actor}/0001_init.sql"));
    std::fs::create_dir_all(mig.parent().unwrap())?;
    std::fs::write(&mig, "CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT NOT NULL);\n")?;

    let py = project.root.join("app.py");
    if py.exists() {
        let mut src = std::fs::read_to_string(&py)?;
        src.push_str(&format!(
            r#"

class {cls}(exports.{cls}):
    def set(self, value: str) -> None:
        statex.execute(
            "INSERT INTO kv (k, v) VALUES ('value', ?1) ON CONFLICT(k) DO UPDATE SET v = excluded.v", value
        )

    def get(self) -> Optional[str]:
        return statex.query_scalar("SELECT v FROM kv WHERE k = 'value'")
"#,
            cls = pascal(actor)
        ));
        if !src.contains("import Optional") {
            // After the leading comment block, before the first import.
            let at = src.find("\nfrom ").or_else(|| src.find("\nimport ")).map_or(0, |i| i + 1);
            src.insert_str(at, "from typing import Optional\n");
        }
        std::fs::write(&py, src)?;
        return Ok(());
    }
    let lib = project.root.join("src/lib.rs");
    let src = std::fs::read_to_string(&lib)?;
    let stub = format!(
        r#"impl exports::{ns}::{pkg}::{m}::Guest for App {{
    fn set(value: String) {{
        statex_guest::sql::execute(
            "INSERT INTO kv (k, v) VALUES ('value', ?1) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            statex_guest::params![value],
        )
        .unwrap();
    }}

    fn get() -> Option<String> {{
        statex_guest::sql::query_scalar("SELECT v FROM kv WHERE k = 'value'", &[]).unwrap()
    }}
}}

"#,
        ns = crate::calls::rid(&ns),
        pkg = crate::calls::rid(&pkg),
        m = crate::calls::rid(actor)
    );
    let new_src = match src.find("export!(") {
        Some(i) => format!("{}{stub}{}", &src[..i], &src[i..]),
        None => format!("{src}\n{stub}"),
    };
    std::fs::write(&lib, new_src)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sdk() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../guest").canonicalize().unwrap()
    }

    #[test]
    fn standalone_rust_uses_explicit_sdk_and_independent_directory() {
        let d = tempfile::tempdir_in(".").unwrap();
        let root = d.path().join("unrelated-directory");
        new_project("shop", &root, "counter", Some(sdk()), None).unwrap();
        let p = Project::find(&root).unwrap();
        assert_eq!(p.app(), "shop");
        assert_eq!(p.source_surface().unwrap().0[0].name, "counter");
        let cargo: toml::Value = toml::from_str(&std::fs::read_to_string(root.join("Cargo.toml")).unwrap()).unwrap();
        assert!(cargo.get("workspace").is_some());
        let dep = cargo["dependencies"]["statex-guest"]["path"].as_str().unwrap();
        assert_eq!(root.join(dep).canonicalize().unwrap(), sdk());
        assert!(!Path::new(dep).is_absolute());
        assert!(check::check(&p, None, None, false).errors.is_empty());
    }

    #[test]
    fn workspace_names_are_independent_of_depth_and_directory() {
        let d = tempfile::tempdir_in(".").unwrap();
        let ws = Workspace {
            root: d.path().canonicalize().unwrap(),
            cfg: crate::workspace::WorkspaceToml {
                guest_sdk: sdk(),
                host_wit: Path::new(env!("CARGO_MANIFEST_DIR")).join("../../wit").canonicalize().unwrap(),
                ..Default::default()
            },
        };
        std::fs::create_dir_all(ws.apps_dir()).unwrap();
        for (name, path) in [("shop", "deep/arbitrary/source"), ("payments/ledger", "one-folder")] {
            let root = ws.apps_dir().join(path);
            new_project(name, &root, "counter", None, Some(&ws)).unwrap();
            let p = Project::find(&root).unwrap();
            assert_eq!(p.app(), name);
            assert!(check::check(&p, Some(&ws), None, false).errors.is_empty());
            let cargo: toml::Value = toml::from_str(&std::fs::read_to_string(root.join("Cargo.toml")).unwrap()).unwrap();
            assert!(cargo.get("workspace").is_none());
            assert!(p.wasm_path().unwrap().starts_with(ws.apps_dir().join("target")));
        }
        assert_eq!(ws.projects().unwrap().len(), 2);
        let caller = Project::find(&ws.apps_dir().join("one-folder")).unwrap();
        assert_eq!(crate::calls::callee_root(&caller, Some(&ws), "shop").unwrap().unwrap(), ws.apps_dir().join("deep/arbitrary/source"));
        assert!(crate::registry::generate(&ws).unwrap().contains_key(Path::new("shop.wit")));
    }

    #[test]
    fn outside_discovery_root_can_use_workspace_dependencies() {
        let d = tempfile::tempdir_in(".").unwrap();
        let ws = Workspace {
            root: d.path().canonicalize().unwrap(),
            cfg: crate::workspace::WorkspaceToml {
                guest_sdk: sdk(),
                host_wit: Path::new(env!("CARGO_MANIFEST_DIR")).join("../../wit").canonicalize().unwrap(),
                ..Default::default()
            },
        };
        let root = ws.root.join("custom/source");
        new_project("payments/shop", &root, "counter", None, Some(&ws)).unwrap();
        let p = Project::find(&root).unwrap();
        assert!(check::check(&p, Some(&ws), None, false).errors.is_empty());
        let cargo: toml::Value = toml::from_str(&std::fs::read_to_string(root.join("Cargo.toml")).unwrap()).unwrap();
        assert!(cargo.get("workspace").is_some());
        assert!(ws.projects().unwrap().is_empty());
    }

    #[test]
    fn keyword_names_produce_valid_wit_and_rust_identifiers() {
        let d = tempfile::tempdir_in(".").unwrap();
        let root = d.path().join("keyword");
        new_project("type", &root, "match", Some(sdk()), None).unwrap();
        let p = Project::find(&root).unwrap();
        assert_eq!(p.source_surface().unwrap().0[0].name, "match");
        assert!(std::fs::read_to_string(root.join("src/lib.rs")).unwrap().contains("exports::local::app_type::match_"));
        add_actor(&p, "interface").unwrap();
        assert_eq!(p.source_surface().unwrap().0.len(), 2);
    }

    #[test]
    fn standalone_python_copies_helpers_and_keeps_name() {
        let d = tempfile::tempdir_in(".").unwrap();
        let root = d.path().join("python-code");
        new_python_project("shop", &root, "counter", None).unwrap();
        let p = Project::find(&root).unwrap();
        assert_eq!(p.app(), "shop");
        assert!(root.join("statex.py").is_file());
        assert!(root.join("statex_testing.py").is_file());
        assert!(std::fs::read_to_string(root.join("wit/app.wit")).unwrap().contains("import statex:host/actors@0.1.0;"));
        assert!(!std::fs::symlink_metadata(root.join("wit/deps/statex-host")).unwrap().file_type().is_symlink());
        assert!(check::check(&p, None, None, false).errors.is_empty());
    }

    #[test]
    fn bad_sdk_and_wit_names_do_not_create_partial_projects() {
        let d = tempfile::tempdir_in(".").unwrap();
        let root = d.path().join("app");
        assert!(new_project("shop", &root, "counter", Some(d.path().join("missing-sdk")), None).is_err());
        assert!(!root.exists());
        assert!(new_project("a--b", &root, "counter", Some(sdk()), None).is_err());
        assert!(!root.exists());
    }

    use crate::check;
}
