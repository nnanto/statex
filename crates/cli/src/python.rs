//! Python (componentize-py) projects: typed bindings for editors and tests,
//! and `statex test`.

use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};

use crate::project::Project;

/// Where the generated `wit_world` package lives, relative to the project root.
pub const BINDINGS: &str = ".statex/bindings";

/// The project's componentize-py build command, if it is a Python project.
fn build_command(p: &Project) -> Option<&str> {
    let b = p.cfg.build.as_ref()?;
    b.command.contains("componentize-py").then_some(b.command.as_str())
}

pub fn is_python(p: &Project) -> bool {
    build_command(p).is_some()
}

/// The build command with its `componentize ...` subcommand replaced by
/// `bindings <dir>`, so it uses the same tool, WIT dir and world.
fn bindings_command(build: &str) -> Result<String> {
    let at = build
        .find(" componentize ")
        .context("[build] command has no `componentize` subcommand; cannot derive the bindings command")?;
    Ok(format!("{} bindings {BINDINGS}", &build[..at]))
}

/// `-p <dir>` paths of the build command (e.g. the workspace's sdk/python-guest).
pub fn python_paths(p: &Project) -> Vec<PathBuf> {
    let Some(cmd) = build_command(p) else { return vec![] };
    let words: Vec<&str> = cmd.split_whitespace().collect();
    words
        .windows(2)
        .filter(|w| w[0] == "-p" || w[0] == "--python-path")
        .map(|w| p.root.join(w[1]))
        .collect()
}

fn hash_dir(dir: &Path, h: &mut impl Hasher) -> Result<()> {
    let mut entries: Vec<_> = std::fs::read_dir(dir)?.collect::<std::io::Result<_>>()?;
    entries.sort_by_key(|e| e.file_name());
    for e in entries {
        let path = e.path();
        e.file_name().hash(h);
        // `metadata` follows symlinks (wit/deps/statex-host links to the workspace).
        if std::fs::metadata(&path)?.is_dir() {
            hash_dir(&path, h)?;
        } else {
            std::fs::read(&path)?.hash(h);
        }
    }
    Ok(())
}

/// Regenerates `.statex/bindings` when the WIT changed. Returns whether it ran.
pub fn bindings(p: &Project, force: bool) -> Result<bool> {
    let Some(build) = build_command(p) else { return Ok(false) };
    let mut h = std::collections::hash_map::DefaultHasher::new();
    build.hash(&mut h);
    hash_dir(&p.root.join("wit"), &mut h)?;
    let stamp = format!("{:016x}", h.finish());
    let out = p.root.join(BINDINGS);
    let stamp_file = out.join(".wit-hash");
    if !force && std::fs::read_to_string(&stamp_file).is_ok_and(|s| s == stamp) {
        return Ok(false);
    }
    if out.exists() {
        std::fs::remove_dir_all(&out)?;
    }
    let cmd = bindings_command(build)?;
    let o = Command::new("sh")
        .args(["-c", &cmd])
        .current_dir(&p.root)
        .output()
        .with_context(|| format!("run `{cmd}`"))?;
    if !o.status.success() {
        bail!("`{cmd}` failed:\n{}", String::from_utf8_lossy(&o.stderr).trim());
    }
    std::fs::write(stamp_file, stamp)?;
    Ok(true)
}

/// Runs the project's tests: pytest against the mock host for Python, `cargo test` for Rust.
pub fn test(p: &Project, args: &[String]) -> Result<()> {
    let status = if let Some(build) = build_command(p) {
        bindings(p, false)?;
        let mut path = vec![p.root.join(BINDINGS), p.root.clone()];
        path.extend(python_paths(p));
        if let Some(old) = std::env::var_os("PYTHONPATH") {
            path.extend(std::env::split_paths(&old));
        }
        // The bindings need Python >= 3.11; use the same uvx-managed Python as the build.
        let mut cmd = if build.trim_start().starts_with("uvx ") {
            let mut c = Command::new("uvx");
            c.args(["--python", "3.12", "pytest", "-o", "cache_dir=.statex/pytest-cache"]);
            c
        } else {
            let mut c = Command::new("python3");
            c.args(["-m", "pytest", "-o", "cache_dir=.statex/pytest-cache"]);
            c
        };
        cmd.args(args)
            .current_dir(&p.root)
            .env("PYTHONPATH", std::env::join_paths(path)?)
            .env("STATEX_PROJECT", &p.root)
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .status()
            .context("run pytest")?
    } else {
        Command::new("cargo").arg("test").args(args).current_dir(&p.root).status().context("run cargo test")?
    };
    if !status.success() {
        bail!("tests failed");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_bindings_command() {
        let b = "uvx --python 3.12 --from componentize-py==0.25.1 componentize-py -d wit -w app componentize -p . -p ../sdk app -o app.wasm";
        assert_eq!(
            bindings_command(b).unwrap(),
            "uvx --python 3.12 --from componentize-py==0.25.1 componentize-py -d wit -w app bindings .statex/bindings"
        );
        assert!(bindings_command("cargo build").is_err());
    }
}
