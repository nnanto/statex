//! `statex new` and `statex add-actor`.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use statex_runtime::{validate_app_name, validate_name};

use crate::workspace::{relative, slash, Workspace};

use crate::project::Project;

/// Location of the guest SDK crate, for path dependencies in new projects.
pub fn default_sdk_path() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("STATEX_GUEST_PATH") {
        return Some(PathBuf::from(p));
    }
    let p = Path::new(env!("CARGO_MANIFEST_DIR")).join("../guest");
    p.canonicalize().ok()
}

const HOST_WIT: &str = include_str!("../../../wit/statex-host.wit");
const PY_GUEST: &str = include_str!("../../../sdk/python-guest/statex.py");
const COMPONENTIZE_PY: &str = "componentize-py==0.25.1";

fn pascal(s: &str) -> String {
    s.split(['-', '_']).filter(|p| !p.is_empty()).map(|p| p[..1].to_uppercase() + &p[1..]).collect()
}

/// statex.toml `owner` hint: namespaced apps default to their namespace.
fn owner_line(name: &str) -> String {
    match name.split_once('/') {
        Some((ns, _)) => format!("# Owner defaults to the namespace ({ns:?}); deploys by other owners are refused.\n# owner = {ns:?}\n"),
        None => "# Team that owns this app name in the cluster; deploys by other owners are refused.\n# owner = \"my-team\"\n".into(),
    }
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
    let owner_line = owner_line(name);
    validate_name("actor type", actor)?;
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
{owner_line}
# Hosts the http-client interface may call: "api.example.com", "*.example.com" or "*".
[http]
allow = []

[limits]
timeout_ms = 5000
memory_mb = 64

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

  export {actor};
}}
"#
        ),
    )?;
    match ws {
        Some(ws) => {
            let link = dir.join("wit/deps/statex-host");
            link_dir(&ws.path(&ws.cfg.host_wit), &ws.reach(link.parent().unwrap(), &ws.cfg.host_wit)?, &link)?
        }
        None => w("wit/deps/statex-host/statex-host.wit", HOST_WIT.to_string())?,
    }
    w(
        &format!("migrations/{actor}/0001_init.sql"),
        format!(
            "-- Applied once per actor, inside a transaction, the first time the actor is used.\n\
             CREATE TABLE {t} (id INTEGER PRIMARY KEY CHECK (id = 0), value INTEGER NOT NULL);\n\
             INSERT INTO {t} (id, value) VALUES (0, 0);\n",
            t = snake(actor)
        ),
    )?;
    if ws.is_none() {
        w("statex.py", PY_GUEST.to_string())?;
    }
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

/// A Rust project. Inside a workspace it joins the apps' Cargo workspace and
/// depends on the workspace's statex-guest crate by relative path.
pub fn new_project(name: &str, dir: &Path, actor: &str, sdk: Option<PathBuf>, ws: Option<&Workspace>) -> Result<()> {
    validate_app_name(name)?;
    let owner_line = owner_line(name);
    validate_name("actor type", actor)?;
    if dir.exists() && std::fs::read_dir(dir)?.next().is_some() {
        bail!("{} already exists and is not empty", dir.display());
    }
    std::fs::create_dir_all(dir)?;
    let slug = name.replace('/', "-");
    let sdk = match ws {
        Some(ws) => ws.reach(dir, &ws.cfg.guest_sdk).context("locate the workspace's statex-guest crate (guest_sdk)")?,
        None => sdk.or_else(default_sdk_path).context("cannot locate the statex-guest crate; pass --sdk <path>")?,
    };
    // A standalone project is its own Cargo workspace; workspace apps share one.
    let standalone = if ws.is_some() {
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
statex-guest = {{ path = "{sdk}" }}
wit-bindgen = "0.62"
{standalone}"#,
            sdk = slash(&sdk)
        ),
    )?;
    w(
        "statex.toml",
        format!(
            r#"# statex app manifest
[app]
name = "{name}"
{owner_line}
# Hosts the http-client interface may call: "api.example.com", "*.example.com" or "*".
[http]
allow = []

[limits]
timeout_ms = 5000
memory_mb = 64
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
"#
        ),
    )?;
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
            pkg = snake(&slug),
            m = snake(actor),
            t = snake(actor),
        ),
    )?;
    match ws {
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
    validate_name("actor type", actor)?;
    let wit_path = project.root.join("wit/app.wit");
    let wit = std::fs::read_to_string(&wit_path).context("read wit/app.wit")?;
    if wit.contains(&format!("interface {actor} ")) || wit.contains(&format!("interface {actor}{{")) {
        bail!("wit/app.wit already defines interface {actor}");
    }
    let (ns, pkg) = wit
        .lines()
        .find_map(|l| l.trim().strip_prefix("package "))
        .and_then(|p| {
            let p = p.trim_end_matches(';');
            let p = p.split('@').next()?;
            let (ns, pkg) = p.split_once(':')?;
            Some((ns.to_string(), pkg.to_string()))
        })
        .context("wit/app.wit has no `package ns:name;` line")?;
    let world_at = wit.find("\nworld ").context("wit/app.wit has no world")? + 1;
    let close = world_at + wit[world_at..].find('}').context("unterminated world")?;
    let mut out = String::new();
    out.push_str(&wit[..world_at]);
    out.push_str(&format!(
        "/// TODO: describe the `{actor}` actor type.\ninterface {actor} {{\n  /// Stores a value.\n  set: func(value: string);\n  /// Returns the stored value, if any.\n  get: func() -> option<string>;\n}}\n\n"
    ));
    out.push_str(&wit[world_at..close]);
    out.push_str(&format!("  export {actor};\n"));
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
        ns = snake(&ns),
        pkg = snake(&pkg),
        m = snake(actor)
    );
    let new_src = match src.find("export!(") {
        Some(i) => format!("{}{stub}{}", &src[..i], &src[i..]),
        None => format!("{src}\n{stub}"),
    };
    std::fs::write(&lib, new_src)?;
    Ok(())
}
