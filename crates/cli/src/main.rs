//! `statex`: developer CLI and node binary.

mod calls;
mod check;
mod codegen;
mod project;
mod python;
mod registry;
mod scaffold;
mod workspace;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use serde_json::{json, Value as J};
use statex_node::{deploy, NodeConfig};
use statex_runtime::Manifest;

use project::Project;
use workspace::Workspace;

const DEFAULT_URL: &str = "http://127.0.0.1:9876";

#[derive(Parser)]
#[command(name = "statex", version, about = "Stateful WebAssembly actors: build, run and call them")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Create a standalone app project; opt into shared tooling with --workspace.
    New {
        /// App name: `app`, or `namespace/app`, independent of its source path.
        name: String,
        /// Directory (defaults to ./<name>, or <apps>/<name> with --workspace).
        #[arg(long)]
        dir: Option<PathBuf>,
        /// Use statex-workspace.toml for shared SDKs, WIT and default app directory.
        #[arg(long)]
        workspace: bool,
        /// Name of the first actor type.
        #[arg(long, default_value = "counter")]
        actor: String,
        /// Guest language: rust or python (componentize-py).
        #[arg(long, default_value = "rust", value_parser = ["rust", "python"])]
        lang: String,
        /// Path to statex-guest (or STATEX_GUEST_PATH); required for standalone Rust.
        #[arg(long, env = "STATEX_GUEST_PATH")]
        sdk: Option<PathBuf>,
    },
    /// Add an actor type (WIT interface, migration and Rust stub) to the current project.
    AddActor { name: String },
    /// Build using [build] command, or cargo build --release --target wasm32-wasip2.
    Build {
        /// Discover callee source using statex-workspace.toml.
        #[arg(long)]
        workspace: bool,
    },
    /// Run the unit tests against the mock host: pytest for Python projects
    /// (after refreshing the typed bindings in .statex/bindings), cargo test for Rust.
    Test {
        /// Discover callee source using statex-workspace.toml.
        #[arg(long)]
        workspace: bool,
        /// Extra arguments for pytest / cargo test.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Build and verify: WIT exports, allowed imports, migrations and instantiation.
    Verify {
        /// Discover callee source using statex-workspace.toml.
        #[arg(long)]
        workspace: bool,
        /// Verify an existing component instead of building the project.
        #[arg(long)]
        wasm: Option<PathBuf>,
    },
    /// Run a local single-node dev server that rebuilds and redeploys on change.
    Dev {
        /// Discover and deploy local workspace callees.
        #[arg(long)]
        workspace: bool,
        #[arg(long, default_value_t = 9876)]
        port: u16,
        /// Wipe local dev state (all actors) before starting.
        #[arg(long)]
        clean: bool,
    },
    /// Build, verify and deploy the project to a store (all nodes pick it up).
    Deploy {
        /// Discover callee source using statex-workspace.toml.
        #[arg(long)]
        workspace: bool,
        #[arg(long, env = "STATEX_STORE")]
        store: String,
        /// Deploy an existing component instead of building. Uses the project's
        /// statex.toml and migrations, or --manifest outside a project.
        #[arg(long)]
        wasm: Option<PathBuf>,
        #[arg(long, requires = "wasm")]
        manifest: Option<PathBuf>,
        /// Deploy even if it breaks the deployed version's clients or migrations.
        #[arg(long)]
        allow_breaking: bool,
        /// Deploy even if apps this app calls are not deployed or do not match
        /// its client interfaces (those calls fail until they do).
        #[arg(long)]
        allow_unresolved_calls: bool,
    },
    /// Pre-merge checks: valid app name, consistent WIT and migrations,
    /// consistent, and (with --against) nothing breaks.
    Check {
        /// Git revision to compare against, e.g. `origin/main`.
        #[arg(long)]
        against: Option<String>,
        /// Check every app of the workspace.
        #[arg(long)]
        all: bool,
        /// Report breaking changes as warnings instead of errors.
        #[arg(long)]
        allow_breaking: bool,
    },
    /// Optional workspace setup for shared SDKs and app discovery.
    Workspace {
        #[command(subcommand)]
        cmd: WorkspaceCmd,
    },
    /// Workspace registry: a generated index of every app and its WIT.
    Registry {
        #[command(subcommand)]
        cmd: RegistryCmd,
    },
    /// Run a cluster node.
    Node {
        #[arg(long, env = "STATEX_STORE")]
        store: String,
        #[arg(long, env = "STATEX_LISTEN", default_value = "0.0.0.0:9876")]
        listen: SocketAddr,
        /// Address for node-to-node traffic (defaults to the public listener).
        #[arg(long, env = "STATEX_INTERNAL_LISTEN")]
        internal_listen: Option<SocketAddr>,
        /// URL peers use to reach this node's internal listener.
        #[arg(long, env = "STATEX_ADVERTISE")]
        advertise: Option<String>,
        #[arg(long, env = "STATEX_NODE_ID")]
        node_id: Option<String>,
        #[arg(long, env = "STATEX_DATA_DIR", default_value = "./statex-data")]
        data_dir: PathBuf,
        /// Node lease TTL in seconds (failover time is about this long).
        #[arg(long, default_value_t = 10)]
        lease_ttl: u64,
        /// Seconds of inactivity after which an actor is released.
        #[arg(long, default_value_t = 300)]
        idle_timeout: u64,
    },
    /// Call a method: statex call counter alice increment '{"by": 2}'
    Call {
        #[arg(name = "type")]
        ty: String,
        key: String,
        method: String,
        /// JSON arguments: an object of named params or an array of positional ones.
        args: Option<String>,
        /// Named argument (value parsed as JSON, else used as a string); repeatable.
        #[arg(short = 'a', long = "arg", value_name = "NAME=VALUE")]
        arg: Vec<String>,
        #[command(flatten)]
        target: Target,
    },
    /// Explicitly create an actor (fails with 409 if it exists).
    Create {
        #[arg(name = "type")]
        ty: String,
        key: String,
        #[command(flatten)]
        target: Target,
    },
    /// Delete an actor and all of its data.
    Delete {
        #[arg(name = "type")]
        ty: String,
        key: String,
        #[command(flatten)]
        target: Target,
    },
    /// List actors of an app.
    Actors {
        #[arg(long = "type")]
        ty: Option<String>,
        #[arg(long, default_value_t = 100)]
        limit: usize,
        #[command(flatten)]
        target: Target,
    },
    /// Show the schema (actor types and methods) of a deployed app.
    Schema {
        #[command(flatten)]
        target: Target,
    },
    /// Check that an object store supports the conditional writes statex relies on.
    Diagnose {
        #[arg(long, env = "STATEX_STORE")]
        store: String,
    },
    /// Typed calls to other actors: client interfaces of the apps listed under
    /// `[calls] apps` in statex.toml.
    Calls {
        #[command(subcommand)]
        cmd: CallsCmd,
    },
    /// Generate a typed client SDK.
    Codegen {
        #[command(subcommand)]
        lang: Lang,
    },
}

#[derive(Subcommand)]
enum WorkspaceCmd {
    /// Configure optional app discovery and shared SDKs in the current directory.
    Init {
        /// statex checkout or vendored copy providing wit/, crates/guest and
        /// sdk/python-guest (required; no build-machine path assumptions).
        #[arg(long, required = true)]
        statex: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum CallsCmd {
    /// Add apps to `[calls] apps` and generate their client interfaces.
    Add {
        /// Discover callee source using statex-workspace.toml.
        #[arg(long)]
        workspace: bool,
        /// Callee apps, e.g. `ledger` or `payments/ledger` (self-calls work too).
        #[arg(required = true)]
        apps: Vec<String>,
        /// Read callee schemas not found in the workspace from a running node.
        #[arg(long)]
        from_url: Option<String>,
    },
    /// (Re)generate wit/deps/<ns>--<app>/client.wit for every `[calls]` app,
    /// plus src/statex_calls.rs in Rust projects.
    Sync {
        /// Discover callee source using statex-workspace.toml.
        #[arg(long)]
        workspace: bool,
        /// Read callee schemas not found in the workspace from a running node.
        #[arg(long)]
        from_url: Option<String>,
    },
}

#[derive(Subcommand)]
enum RegistryCmd {
    /// Regenerate the registry directory.
    Build {
        /// Fail if the registry is out of date instead of writing it (for CI).
        #[arg(long)]
        check: bool,
    },
}

#[derive(Subcommand)]
enum Lang {
    /// Generate a standalone Python module (stdlib only, Python 3.9+).
    Python {
        /// Output file (defaults to <app>_client.py).
        #[arg(short, long)]
        out: Option<PathBuf>,
        /// Read the schema from a running node instead of building the local project.
        #[arg(long)]
        from_url: Option<String>,
        /// App name (with --from-url).
        #[arg(long)]
        app: Option<String>,
        /// Default base URL baked into the client.
        #[arg(long, default_value = DEFAULT_URL)]
        url: String,
    },
}

#[derive(clap::Args)]
struct Target {
    /// Any node of the cluster.
    #[arg(long, env = "STATEX_URL", default_value = DEFAULT_URL)]
    url: String,
    /// App name (defaults to the app of the current project).
    #[arg(long, env = "STATEX_APP")]
    app: Option<String>,
}

impl Target {
    fn app(&self) -> Result<String> {
        if let Some(a) = &self.app {
            return Ok(a.clone());
        }
        Project::find(Path::new("."))
            .map(|p| p.app().to_string())
            .context("no --app given and not inside a statex project")
    }
}

#[tokio::main]
async fn main() {
    let filter = tracing_subscriber::EnvFilter::try_from_env("STATEX_LOG")
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,wasmtime=warn,cranelift=warn"));
    tracing_subscriber::fmt().with_env_filter(filter).with_target(false).init();
    if let Err(e) = run(Cli::parse()).await {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    match cli.cmd {
        Cmd::New { name, dir, workspace, actor, lang, sdk } => {
            statex_runtime::validate_app_name(&name)?;
            let ws = workspace.then(|| Workspace::require(Path::new("."))).transpose()?;
            let dir = new_directory(&name, dir, ws.as_ref());
            if let Some(ws) = &ws {
                println!("workspace {}: linking shared host WIT and SDKs", ws.root.display());
            }
            if lang == "python" {
                scaffold::new_python_project(&name, &dir, &actor, ws.as_ref())?;
                // Best effort: typed bindings for editors right away.
                if let Err(e) = Project::find(&dir).and_then(|p| python::bindings(&p, true)) {
                    eprintln!("warning: could not generate {}: {e:#}", python::BINDINGS);
                }
                println!("created {}\n\nnext:\n  cd {}\n  statex test       # unit tests against the mock host\n  statex verify     # builds with componentize-py and checks the component\n  statex dev        # local server with hot reload\n  statex call {actor} alice increment '{{\"by\": 1}}'", dir.display(), dir.display());
            } else {
                scaffold::new_project(&name, &dir, &actor, sdk, ws.as_ref())?;
                println!("created {}\n\nnext:\n  cd {}\n  cargo test        # unit tests against the mock host\n  statex dev        # local server with hot reload\n  statex call {actor} alice increment '{{\"by\": 1}}'", dir.display(), dir.display());
            }
        }
        Cmd::AddActor { name } => {
            let p = Project::find(Path::new("."))?;
            scaffold::add_actor(&p, &name)?;
            let code = if p.root.join("app.py").exists() { "app.py" } else { "src/lib.rs" };
            println!("added actor type {name}: edit wit/app.wit, migrations/{name}/ and {code}");
        }
        Cmd::Test { args, workspace } => {
            let p = Project::find(Path::new("."))?;
            sync_local(&p, workspace)?;
            python::test(&p, &args)?;
        }
        Cmd::Build { workspace } => {
            let p = Project::find(Path::new("."))?;
            sync_local(&p, workspace)?;
            let wasm = p.build(false)?;
            println!("built {}", wasm.display());
        }
        Cmd::Verify { wasm, workspace } => {
            let p = Project::find(Path::new("."))?;
            let path = match wasm {
                Some(w) => w,
                None => {
                    sync_local(&p, workspace)?;
                    p.build(true)?
                }
            };
            let bytes = std::fs::read(&path).with_context(|| format!("read {}", path.display()))?;
            let manifest = p.manifest(&bytes)?;
            project::verify(&bytes, &manifest)?;
            project::print_summary(&manifest, bytes.len());
            println!("ok: component verified");
        }
        Cmd::Dev { port, clean, workspace } => dev(port, clean, workspace).await?,
        Cmd::Deploy { store, wasm, manifest, allow_breaking, allow_unresolved_calls, workspace } => {
            let (bytes, manifest) = match (wasm, manifest) {
                (Some(w), Some(m)) => {
                    let bytes = std::fs::read(&w)?;
                    let m: Manifest = serde_json::from_slice(&std::fs::read(&m)?)?;
                    let m = Manifest::build(&bytes, &m.app, m.migrations, m.http, m.limits)?;
                    (bytes, m)
                }
                (w, _) => {
                    let p = Project::find(Path::new("."))?;
                    if w.is_none() {
                        sync_local(&p, workspace)?;
                    }
                    let path = match w {
                        Some(w) => w,
                        None => p.build(true)?,
                    };
                    let bytes = std::fs::read(&path).with_context(|| format!("read {}", path.display()))?;
                    let m = p.manifest(&bytes)?;
                    (bytes, m)
                }
            };
            project::verify(&bytes, &manifest)?;
            let store = statex_store::open(&store)?;
            let opts = deploy::DeployOptions { allow_breaking, allow_unresolved_calls };
            let cur = deploy::deploy(&store, &bytes, &manifest, &opts).await?;
            println!(
                "deployed {} version {} (id {}); nodes pick it up within seconds",
                cur.app, cur.version, cur.id
            );
        }
        Cmd::Check { against, all, allow_breaking } => {
            let ws = workspace_if_requested(Path::new("."), all)?;
            let projects = if all {
                ws.as_ref().context("--all needs a workspace (statex-workspace.toml)")?.projects()?
            } else {
                vec![Project::find(Path::new("."))?]
            };
            let mut failed = 0;
            for p in &projects {
                let r = check::check(p, ws.as_ref(), against.as_deref(), allow_breaking);
                println!("{} {}", if r.errors.is_empty() { "ok  " } else { "FAIL" }, r.app);
                for w in &r.warnings {
                    println!("       warning: {w}");
                }
                for e in &r.errors {
                    println!("       error: {e}");
                }
                failed += usize::from(!r.errors.is_empty());
            }
            if failed > 0 {
                bail!("{failed} of {} app(s) failed checks", projects.len());
            }
        }
        Cmd::Workspace { cmd: WorkspaceCmd::Init { statex } } => {
            let statex = statex.context("workspace init needs --statex <checkout> providing WIT and guest SDKs")?;
            for f in workspace::init(Path::new("."), &statex)? {
                println!("created {f}");
            }
            println!("\nnext:\n  statex new <app> --workspace [--lang python]\n  statex check --all --against origin/main\n  statex registry build");
        }
        Cmd::Registry { cmd: RegistryCmd::Build { check } } => {
            let ws = Workspace::require(Path::new("."))?;
            let dir = ws.rel(&ws.path(&ws.cfg.registry));
            if check {
                let diff = registry::diff(&ws)?;
                if !diff.is_empty() {
                    bail!("registry {dir}/ is out of date (run `statex registry build`):\n  {}", diff.join("\n  "));
                }
                println!("registry {dir}/ is up to date");
            } else {
                let n = registry::build(&ws)?;
                println!("wrote {dir}/ ({n} app{})", if n == 1 { "" } else { "s" });
            }
        }
        Cmd::Node { store, listen, internal_listen, advertise, node_id, data_dir, lease_ttl, idle_timeout } => {
            let node_id = node_id.unwrap_or_else(|| {
                let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "node".into());
                format!("{host}-{}", listen.port())
            });
            let mut cfg = NodeConfig::new(node_id, statex_store::open(&store)?, data_dir);
            cfg.listen = listen;
            cfg.internal_listen = internal_listen;
            cfg.advertise = advertise;
            cfg.lease_ttl = Duration::from_secs(lease_ttl);
            cfg.idle_timeout = Duration::from_secs(idle_timeout);
            cfg.exit_on_fence = true;
            let mut h = statex_node::start(cfg).await?;
            tracing::info!(url = %h.url(), internal = %h.internal_addr, "node ready");
            tokio::select! {
                _ = shutdown_signal() => {
                    tracing::info!("shutting down: handing off actors");
                    h.shutdown().await;
                }
                _ = h.wait() => bail!("node stopped (lease lost)"),
            }
        }
        Cmd::Call { ty, key, method, args, arg, target } => {
            let mut body = match args {
                Some(a) => serde_json::from_str(&a).context("args must be JSON")?,
                None => J::Null,
            };
            if !arg.is_empty() {
                if body.is_null() {
                    body = json!({});
                }
                let obj = body.as_object_mut().context("cannot combine positional JSON args with --arg")?;
                for a in arg {
                    let (k, v) = a.split_once('=').context("--arg must be NAME=VALUE")?;
                    obj.insert(k.into(), serde_json::from_str(v).unwrap_or_else(|_| J::String(v.into())));
                }
            }
            let path = format!("actors/{}/{}/{method}", enc(&ty), enc(&key));
            let r = request(&target, reqwest::Method::POST, &path, Some(body)).await?;
            println!("{}", serde_json::to_string_pretty(&r["result"])?);
        }
        Cmd::Create { ty, key, target } => {
            request(&target, reqwest::Method::POST, &format!("actors/{}/{}/_create", enc(&ty), enc(&key)), Some(json!({}))).await?;
            println!("created {ty}/{key}");
        }
        Cmd::Delete { ty, key, target } => {
            request(&target, reqwest::Method::DELETE, &format!("actors/{}/{}", enc(&ty), enc(&key)), None).await?;
            println!("deleted {ty}/{key}");
        }
        Cmd::Actors { ty, limit, target } => {
            let mut path = format!("actors?limit={limit}");
            if let Some(t) = ty {
                path.push_str(&format!("&type={}", enc(&t)));
            }
            let r = request(&target, reqwest::Method::GET, &path, None).await?;
            for c in r["actors"].as_array().into_iter().flatten() {
                println!(
                    "{:<16} {:<24} {:<8} epoch {:<4} node {}",
                    c["type"].as_str().unwrap_or(""),
                    c["key"].as_str().unwrap_or(""),
                    c["state"].as_str().unwrap_or(""),
                    c["epoch"],
                    c["node"].as_str().unwrap_or("-")
                );
            }
        }
        Cmd::Schema { target } => {
            let m = schema(&target.url, &target.app()?).await?;
            project::print_summary(&m, 0);
        }
        Cmd::Diagnose { store } => {
            let s = statex_store::open(&store)?;
            statex_store::conformance_test(&*s).await?;
            println!("ok: {store} supports conditional create/replace and ranged reads");
        }
        Cmd::Calls { cmd } => {
            let p = Project::find(Path::new("."))?;
            let (from_url, workspace) = match cmd {
                CallsCmd::Add { apps, from_url, workspace } => {
                    for a in &apps {
                        statex_runtime::validate_app_name(a)?;
                    }
                    if project::add_calls(&p.root, &apps)? {
                        println!("updated statex.toml [calls] apps");
                    }
                    (from_url, workspace)
                }
                CallsCmd::Sync { from_url, workspace } => (from_url, workspace),
            };
            let ws = workspace_if_requested(&p.root, workspace)?;
            let p = Project::find(&p.root)?;
            calls_sync(&p, ws.as_ref(), from_url.as_deref()).await?;
        }
        Cmd::Codegen { lang: Lang::Python { out, from_url, app, url } } => {
            let manifest = match from_url {
                Some(u) => {
                    let app = match app {
                        Some(a) => a,
                        None => Project::find(Path::new("."))?.app().to_string(),
                    };
                    schema(&u, &app).await?
                }
                None => {
                    let p = Project::find(Path::new("."))?;
                    let bytes = std::fs::read(p.build(true)?)?;
                    p.manifest(&bytes)?
                }
            };
            let code = codegen::python(&manifest, &url)?;
            let out = out.unwrap_or_else(|| PathBuf::from(format!("{}_client.py", manifest.app.replace(['-', '/'], "_"))));
            std::fs::write(&out, code)?;
            println!("wrote {}", out.display());
        }
    }
    Ok(())
}

fn new_directory(name: &str, dir: Option<PathBuf>, ws: Option<&Workspace>) -> PathBuf {
    dir.unwrap_or_else(|| ws.map_or_else(|| PathBuf::from(name), |ws| ws.app_dir(name)))
}

/// Regenerates client interfaces whose callee source is local; prints changes.
fn workspace_if_requested(start: &Path, requested: bool) -> Result<Option<Workspace>> {
    requested.then(|| Workspace::require(start)).transpose()
}

fn sync_local(p: &Project, workspace: bool) -> Result<()> {
    let ws = workspace_if_requested(&p.root, workspace)?;
    for f in calls::sync_local(p, ws.as_ref())? {
        println!("updated {f}");
    }
    // Typed `wit_world` bindings for editors and `statex test`; the build itself does not need them.
    if let Err(e) = python::bindings(p, false) {
        eprintln!("warning: could not refresh {}: {e:#}", python::BINDINGS);
    }
    Ok(())
}

async fn calls_sync(p: &Project, ws: Option<&Workspace>, from_url: Option<&str>) -> Result<()> {
    let mut callees = Vec::new();
    for app in calls::listed(p)? {
        let types = match calls::local_types(p, ws, &app) {
            Some(t) => t?,
            None => match from_url {
                Some(u) => schema(u, &app).await.with_context(|| format!("read schema of app {app}"))?.types,
                None => bail!(
                    "app {app} has no locally available source; configure [calls] paths or pass --from-url <node url> to read its schema from a running cluster"
                ),
            },
        };
        callees.push(calls::Callee { app, types });
    }
    let want = calls::render(p, &callees)?;
    let changed = calls::write(p, ws, &want)?;
    for f in &changed {
        println!("wrote {f}");
    }
    if callees.is_empty() {
        println!("no apps under [calls] in statex.toml (add some with `statex calls add <app>`)");
        return Ok(());
    }
    if changed.is_empty() {
        println!("client interfaces are up to date");
    }
    let world = std::fs::read_to_string(p.root.join("wit/app.wit")).unwrap_or_default();
    let missing: Vec<_> = calls::imports(&callees).into_iter().filter(|i| !world.contains(i.as_str())).collect();
    if !missing.is_empty() {
        println!("\nimport the actor types you call in your world (wit/app.wit):");
        for i in &missing {
            println!("    {i}");
        }
    }
    if calls::is_rust(p) {
        let lib = std::fs::read_to_string(p.root.join("src/lib.rs")).unwrap_or_default();
        if !lib.contains("mod statex_calls") || !lib.contains("statex_guest::actors") {
            println!("\nin src/lib.rs, declare the generated module and reuse its bindings:");
            println!("    mod statex_calls;\n");
            println!("    wit_bindgen::generate!({{\n        path: \"wit\",\n        world: \"app\",\n        with: {{");
            for w in calls::with_entries(&callees) {
                println!("            {w}");
            }
            println!("        }},\n    }});");
            if let Some((c, t)) = callees.iter().find_map(|c| c.types.first().map(|t| (c, t))) {
                let path = calls::module_path(&c.app, &t.name);
                if let Some(m) = t.methods.first() {
                    println!(
                        "\nthen call e.g. `{path}::{}(\"some-key\", ..)`; in `cargo test`, install a stub with `{path}::stub(..)`.",
                        m.name.replace('-', "_")
                    );
                }
            }
        }
    }
    if python::is_python(p) {
        if missing.is_empty() {
            python::bindings(p, false)?;
        }
        if let Some((c, t)) = callees.iter().find_map(|c| c.types.first().map(|t| (c, t))) {
            let module = t.name.replace('-', "_");
            if let Some(m) = t.methods.first() {
                println!(
                    "
in app.py: `from wit_world.imports import {module}`, then `{module}.{}(\"some-key\", ..)` (app {}).\n\
                     A failed call raises statex.Err(call_error); in tests, answer calls with statex_testing.stub({module}, ..).",
                    m.name.replace('-', "_"),
                    c.app
                );
            }
        }
    }
    Ok(())
}

/// Resolves on Ctrl-C or SIGTERM.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

fn enc(s: &str) -> String {
    let mut o = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
            o.push(b as char);
        } else {
            o.push_str(&format!("%{b:02X}"));
        }
    }
    o
}

/// URL path of an app: `namespace/app` keeps its slash.
fn app_path(app: &str) -> String {
    app.split('/').map(enc).collect::<Vec<_>>().join("/")
}

async fn request(target: &Target, method: reqwest::Method, path: &str, body: Option<J>) -> Result<J> {
    let url = format!("{}/v1/apps/{}/{path}", target.url.trim_end_matches('/'), app_path(&target.app()?));
    let mut req = reqwest::Client::new().request(method, &url);
    if let Some(b) = body {
        req = req.json(&b);
    }
    let resp = req.send().await.with_context(|| format!("request {url}"))?;
    let status = resp.status();
    let body: J = resp.json().await.unwrap_or(J::Null);
    if !status.is_success() {
        let e = &body["error"];
        let mut msg = format!("{} {}: {}", status.as_u16(), e["code"].as_str().unwrap_or("error"), e["message"].as_str().unwrap_or(""));
        if !e["detail"].is_null() {
            msg.push_str(&format!("\ndetail: {}", serde_json::to_string_pretty(&e["detail"])?));
        }
        bail!(msg);
    }
    Ok(body)
}

async fn schema(url: &str, app: &str) -> Result<Manifest> {
    let url = format!("{}/v1/apps/{}/schema", url.trim_end_matches('/'), app_path(app));
    let resp = reqwest::get(&url).await.with_context(|| format!("GET {url}"))?;
    if !resp.status().is_success() {
        bail!("GET {url}: {}", resp.status());
    }
    Ok(resp.json().await?)
}

fn newest_mtime(paths: &[PathBuf]) -> SystemTime {
    fn walk(p: &Path, best: &mut SystemTime) {
        if let Ok(m) = std::fs::metadata(p) {
            if let Ok(t) = m.modified() {
                *best = (*best).max(t);
            }
            if m.is_dir() {
                for e in std::fs::read_dir(p).into_iter().flatten().flatten() {
                    walk(&e.path(), best);
                }
            }
        }
    }
    let mut best = SystemTime::UNIX_EPOCH;
    for p in paths {
        walk(p, &mut best);
    }
    best
}

/// Builds, verifies and deploys the project to the dev store.
async fn dev_deploy(p: &Project, store: &statex_store::DynStore, workspace: bool) -> Result<Manifest> {
    let root = p.root.clone();
    let (bytes, manifest) = tokio::task::spawn_blocking(move || -> Result<_> {
        let p = Project::find(&root)?;
        sync_local(&p, workspace)?;
        let bytes = std::fs::read(p.build(true)?)?;
        let manifest = p.manifest(&bytes)?;
        project::verify(&bytes, &manifest)?;
        Ok((bytes, manifest))
    })
    .await??;
    // Local dev state is disposable: never block a reload on compatibility
    // or on callees that are missing (`statex check` and real deploys enforce both).
    let opts = deploy::DeployOptions { allow_breaking: true, allow_unresolved_calls: true };
    for problem in deploy::unresolved_calls(store, &manifest).await? {
        eprintln!("warning: {problem}; calls to it fail until it is deployed to the dev store and matches");
    }
    let stale = stale_migrations(store, &manifest).await?;
    deploy::deploy(store, &bytes, &manifest, &opts).await?;
    if !stale.is_empty() {
        eprintln!("warning: migrations changed after dev actors may have applied them:");
        for s in &stale {
            eprintln!("  - {s}");
        }
        eprintln!("  Existing dev actors keep their old schema. Run `statex dev --clean` to reset local state.");
    }
    Ok(manifest)
}

/// Migration problems (edited, removed, or inserted before applied ones) of
/// `manifest` against every version previously deployed to the dev store.
/// Comparing with all versions, not just the last, keeps warning until the
/// state is cleaned. API changes are ignored: they are routine in dev.
async fn stale_migrations(store: &statex_store::DynStore, manifest: &Manifest) -> Result<std::collections::BTreeSet<String>> {
    let mut out = std::collections::BTreeSet::new();
    for prev in deploy::history(store, &manifest.app).await? {
        let old = statex_runtime::Surface { types: &manifest.types, migrations: &prev.migrations };
        out.extend(statex_runtime::breaking_changes(old, manifest.into()));
    }
    Ok(out)
}

async fn dev(port: u16, clean: bool, workspace: bool) -> Result<()> {
    let p = Project::find(Path::new("."))?;
    let dev_dir = p.root.join(".statex/dev");
    if clean && dev_dir.exists() {
        std::fs::remove_dir_all(&dev_dir)?;
    }

    let store = statex_store::open(dev_dir.join("bucket").to_str().unwrap())?;
    // Callees with local source run in the dev node too (deployed once, at startup).
    let ws = workspace_if_requested(&p.root, workspace)?;
    for app in calls::listed(&p)? {
        if let Some(dir) = calls::callee_root(&p, ws.as_ref(), &app)?.filter(|_| app != p.app()) {
            println!("building callee {app} ...");
            dev_deploy(&Project::find(&dir)?, &store, workspace).await.with_context(|| format!("deploy callee {app}"))?;
        }
    }
    println!("building {} ...", p.app());
    let manifest = dev_deploy(&p, &store, workspace).await?;

    let mut cfg = NodeConfig::new("dev", store.clone(), dev_dir.join("node"));
    cfg.listen = SocketAddr::from(([127, 0, 0, 1], port));
    cfg.lease_ttl = Duration::from_secs(3);
    let mut h = statex_node::start(cfg).await?;
    let url = h.url();
    project::print_summary(&manifest, 0);
    if let Some(t) = manifest.types.first() {
        if let Some(m) = t.methods.first() {
            let args: serde_json::Map<String, J> = m.params.iter().map(|p| (p.name.clone(), project::sample(&p.ty))).collect();
            println!(
                "\nstatex dev listening on {url}\n  curl -s -X POST {url}/v1/apps/{app}/actors/{ty}/alice/{m} -d '{args}'\n  statex call {ty} alice {m} '{args}'\n  statex codegen python\n\nwatching for changes (Ctrl-C to stop)",
                app = manifest.app,
                ty = t.name,
                m = m.name,
                args = J::Object(args)
            );
        }
    }

    let watched = p.watched();
    let mut last = newest_mtime(&watched);
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    loop {
        tokio::select! {
            _ = shutdown_signal() => {
                println!("\nstopping");
                h.shutdown().await;
                return Ok(());
            }
            _ = h.wait() => bail!("dev node stopped unexpectedly"),
            _ = tick.tick() => {
                let now = newest_mtime(&watched);
                if now <= last {
                    continue;
                }
                last = now;
                println!("change detected, rebuilding ...");
                match dev_deploy(&p, &store, workspace).await {
                    Ok(m) => {
                        h.node.refresh_apps().await?;
                        println!("reloaded {} ({})", m.app, &m.sha256[..12]);
                    }
                    Err(e) => eprintln!("build failed; still serving the previous version:\n{e:#}"),
                }
                last = last.max(newest_mtime(&watched));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_standalone_unless_workspace_is_explicit() {
        let cli = Cli::try_parse_from(["statex", "new", "shop", "--sdk", "guest"]).unwrap();
        assert!(matches!(cli.cmd, Cmd::New { workspace: false, .. }));
        let cli = Cli::try_parse_from(["statex", "new", "shop", "--workspace", "--dir", "custom"]).unwrap();
        assert!(matches!(cli.cmd, Cmd::New { workspace: true, .. }));
        let ws = Workspace { root: PathBuf::from("repo"), cfg: workspace::WorkspaceToml::default() };
        assert_eq!(new_directory("shop", None, None), PathBuf::from("shop"));
        assert_eq!(new_directory("shop", None, Some(&ws)), PathBuf::from("repo/apps/shop"));
        assert_eq!(new_directory("payments/shop", Some("custom".into()), Some(&ws)), PathBuf::from("custom"));
    }

    #[test]
    fn app_urls_keep_optional_namespace() {
        assert_eq!(app_path("shop"), "shop");
        assert_eq!(app_path("payments/shop"), "payments/shop");
    }

    #[test]
    fn project_commands_do_not_implicitly_read_workspace_configuration() {
        let d = tempfile::tempdir_in(".").unwrap();
        std::fs::write(d.path().join(workspace::FILE), "invalid [toml").unwrap();
        assert!(workspace_if_requested(d.path(), false).unwrap().is_none());
        assert!(workspace_if_requested(d.path(), true).is_err());
        assert!(matches!(Cli::try_parse_from(["statex", "build"]).unwrap().cmd, Cmd::Build { workspace: false }));
        assert!(matches!(
            Cli::try_parse_from(["statex", "calls", "sync"]).unwrap().cmd,
            Cmd::Calls { cmd: CallsCmd::Sync { workspace: false, .. } }
        ));
        assert!(matches!(
            Cli::try_parse_from(["statex", "dev", "--workspace"]).unwrap().cmd,
            Cmd::Dev { workspace: true, .. }
        ));
    }
}
