//! WebAssembly runtime for statex actors: compiles app components, links the
//! `statex:host` capabilities and invokes exported methods dynamically from
//! JSON using the app [`manifest`].

pub mod alarm;
pub mod calls;
pub mod compat;
pub mod host;
pub mod json;
pub mod manifest;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use rusqlite::Connection;
use serde_json::Value as J;
use wasmtime::component::{Component, ComponentExportIndex, HasSelf, InstancePre, Linker, Val};
use wasmtime::{Config, Engine, Store, StoreLimitsBuilder};
use wasmtime_wasi::WasiCtxBuilder;

pub use calls::{ActorCaller, ActorRef, CallFailure, CallReply, CallRequest};
pub use host::{forbidden, ActorIdentity, HostState};
pub use compat::{breaking_changes, call_mismatches, call_mismatches_in, fmt_ty, Surface};
pub use manifest::{
    app_of_package, client_package, inspect, inspect_wit, inspect_wit_calls, validate_app_name, validate_name, ActorType, CallImport,
    Case, Field, HttpPolicy, Limits, Manifest, Method, Migration, Param, Ty,
};

/// Engine epoch tick used for call timeouts.
const TICK: Duration = Duration::from_millis(10);

/// Shared engine + linker. Cheap to clone.
#[derive(Clone)]
pub struct Runtime {
    engine: Engine,
    linker: Arc<Linker<HostState>>,
}

impl Runtime {
    pub fn new() -> Result<Self> {
        let mut cfg = Config::new();
        cfg.epoch_interruption(true);
        let engine = Engine::new(&cfg)?;
        let mut linker = Linker::<HostState>::new(&engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker)?;
        host::Imports::add_to_linker::<HostState, HasSelf<HostState>>(&mut linker, |s| s)?;
        let weak = engine.weak();
        std::thread::Builder::new().name("statex-epoch".into()).spawn(move || loop {
            std::thread::sleep(TICK);
            match weak.upgrade() {
                Some(e) => e.increment_epoch(),
                None => break,
            }
        })?;
        Ok(Self { engine, linker: Arc::new(linker) })
    }

    /// Process-wide default runtime.
    pub fn shared() -> Result<Self> {
        static RT: OnceLock<Runtime> = OnceLock::new();
        if let Some(r) = RT.get() {
            return Ok(r.clone());
        }
        let r = Runtime::new()?;
        Ok(RT.get_or_init(|| r).clone())
    }

    /// Compiles and links an app. Fails if the component imports anything the
    /// host does not provide or does not export what the manifest describes.
    pub fn load(&self, wasm: &[u8], manifest: Manifest) -> Result<Arc<AppCode>> {
        let component = Component::new(&self.engine, wasm).map_err(anyhow::Error::from).context("compile component")?;
        let linker = if manifest.calls.is_empty() {
            self.linker.clone()
        } else {
            let mut l = (*self.linker).clone();
            calls::link(&mut l, &manifest.calls).context("link client interfaces")?;
            Arc::new(l)
        };
        let pre = linker
            .instantiate_pre(&component)
            .map_err(anyhow::Error::from)
            .context("link component (does it import something the host does not provide?)")?;
        let mut exports = HashMap::new();
        let mut alarm_exports = HashMap::new();
        for t in &manifest.types {
            let iface = component
                .get_export_index(None, &t.export)
                .ok_or_else(|| anyhow!("component does not export {}", t.export))?;
            for m in &t.methods {
                let f = component
                    .get_export_index(Some(&iface), &m.name)
                    .ok_or_else(|| anyhow!("component does not export {}.{}", t.export, m.name))?;
                exports.insert((t.name.clone(), m.name.clone()), f);
            }
            if let Some(m) = &t.alarm {
                let f = component
                    .get_export_index(Some(&iface), &m.name)
                    .ok_or_else(|| anyhow!("component does not export {}.{}", t.export, m.name))?;
                alarm_exports.insert(t.name.clone(), f);
            }
        }
        Ok(Arc::new(AppCode { manifest, pre, exports, alarm_exports, engine: self.engine.clone() }))
    }
}

/// A compiled, linked app version.
pub struct AppCode {
    pub manifest: Manifest,
    pre: InstancePre<HostState>,
    exports: HashMap<(String, String), ComponentExportIndex>,
    /// Alarm handler per actor type.
    alarm_exports: HashMap<String, ComponentExportIndex>,
    engine: Engine,
}

impl AppCode {
    /// Instantiates the component for one actor, without actor-to-actor calls
    /// (they fail with `call-error::unavailable`).
    pub fn instantiate(self: &Arc<Self>, identity: ActorIdentity, db: Arc<Mutex<Connection>>) -> Result<ActorInstance> {
        self.instantiate_with(identity, db, None)
    }

    /// Instantiates the component for one actor; `caller` routes the calls it
    /// makes to other actors.
    pub fn instantiate_with(
        self: &Arc<Self>,
        identity: ActorIdentity,
        db: Arc<Mutex<Connection>>,
        caller: Option<Arc<dyn ActorCaller>>,
    ) -> Result<ActorInstance> {
        let limits = &self.manifest.limits;
        let has_alarm = self.alarm_exports.contains_key(&identity.actor_type);
        let state = HostState {
            wasi: WasiCtxBuilder::new().inherit_stdout().inherit_stderr().build(),
            table: Default::default(),
            limits: StoreLimitsBuilder::new().memory_size((limits.memory_mb as usize) << 20).build(),
            identity,
            db,
            http: self.manifest.http.clone(),
            http_timeout: Duration::from_millis(limits.timeout_ms),
            caller,
            chain: Vec::new(),
            deadline: None,
            has_alarm,
        };
        let mut store = Store::new(&self.engine, state);
        store.limiter(|s| &mut s.limits);
        store.set_epoch_deadline(u64::MAX / 2);
        let instance = self.pre.instantiate(&mut store).map_err(anyhow::Error::from).context("instantiate component")?;
        Ok(ActorInstance { store, instance, app: self.clone() })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CallError {
    /// Unknown actor type or method.
    NotFound(String),
    /// Arguments do not match the method signature.
    BadArgs(String),
    /// The guest trapped (panic, timeout, out of memory). Discard the instance
    /// and roll back the transaction.
    Trap(String),
}

impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallError::NotFound(s) => write!(f, "not found: {s}"),
            CallError::BadArgs(s) => write!(f, "bad arguments: {s}"),
            CallError::Trap(s) => write!(f, "trap: {s}"),
        }
    }
}

impl std::error::Error for CallError {}

/// Successful method return.
#[derive(Debug, Clone, PartialEq)]
pub struct CallOutput {
    /// JSON result. For `result<T, E>` methods: the unwrapped ok or err payload.
    pub value: J,
    /// True when a `result<T, E>` method returned `err`.
    pub is_err: bool,
}

/// A live component instance bound to one actor.
pub struct ActorInstance {
    store: Store<HostState>,
    instance: wasmtime::component::Instance,
    app: Arc<AppCode>,
}

impl ActorInstance {
    pub fn app(&self) -> &Arc<AppCode> {
        &self.app
    }

    pub fn identity(&self) -> &ActorIdentity {
        &self.store.data().identity
    }

    pub fn set_epoch(&mut self, epoch: u64) {
        self.store.data_mut().identity.epoch = epoch;
    }

    /// Invokes `actor_type.method(args)`. The caller owns the transaction.
    pub fn call(&mut self, actor_type: &str, method: &str, args: &J) -> Result<CallOutput, CallError> {
        self.call_with(actor_type, method, args, &[])
    }

    /// Like [`call`](Self::call), for a call made by another actor: `chain`
    /// holds the actors executing further up the call chain (outermost first).
    pub fn call_with(&mut self, actor_type: &str, method: &str, args: &J, chain: &[ActorRef]) -> Result<CallOutput, CallError> {
        let app = self.app.clone();
        let m = resolve_method(&app.manifest, actor_type, method)?;
        let params = json::args_to_vals(m, args).map_err(CallError::BadArgs)?;
        let idx = *app.exports.get(&(actor_type.to_string(), method.to_string())).expect("indexed");
        self.invoke(actor_type, m, idx, &params, chain)
    }

    /// Whether `actor_type` exports an alarm handler.
    pub fn has_alarm_handler(&self, actor_type: &str) -> bool {
        self.app.alarm_exports.contains_key(actor_type)
    }

    /// Runs the alarm handler of `actor_type`; `retry` counts its earlier
    /// failed attempts. The caller owns the transaction.
    pub fn call_alarm(&mut self, actor_type: &str, retry: u32) -> Result<CallOutput, CallError> {
        let app = self.app.clone();
        let (Some(m), Some(idx)) = (
            app.manifest.actor_type(actor_type).and_then(|t| t.alarm.as_ref()),
            app.alarm_exports.get(actor_type).copied(),
        ) else {
            return Err(CallError::NotFound(format!("actor type {actor_type} has no alarm handler")));
        };
        let params: Vec<Val> = if m.params.is_empty() { vec![] } else { vec![Val::U32(retry)] };
        self.invoke(actor_type, m, idx, &params, &[])
    }

    fn invoke(
        &mut self,
        actor_type: &str,
        m: &Method,
        idx: ComponentExportIndex,
        params: &[Val],
        chain: &[ActorRef],
    ) -> Result<CallOutput, CallError> {
        let app = self.app.clone();
        let method = &m.name;
        let func = self
            .instance
            .get_func(&mut self.store, idx)
            .ok_or_else(|| CallError::NotFound(format!("{actor_type}.{method}")))?;
        let mut results = vec![json::placeholder(); usize::from(m.result.is_some())];
        let ticks = (app.manifest.limits.timeout_ms / TICK.as_millis() as u64).max(1);
        {
            let s = self.store.data_mut();
            s.chain = chain.to_vec();
            s.chain.push(ActorRef {
                app: s.identity.app.clone(),
                actor_type: actor_type.to_string(),
                key: s.identity.key.clone(),
            });
            s.deadline = Some(Instant::now() + Duration::from_millis(app.manifest.limits.timeout_ms));
        }
        self.store.set_epoch_deadline(ticks);
        let r = func.call(&mut self.store, params, &mut results);
        self.store.set_epoch_deadline(u64::MAX / 2);
        {
            let s = self.store.data_mut();
            s.chain.clear();
            s.deadline = None;
        }
        if let Err(e) = r {
            let msg = match e.downcast_ref::<wasmtime::Trap>() {
                Some(wasmtime::Trap::Interrupt) => format!("timed out after {} ms", app.manifest.limits.timeout_ms),
                _ => format!("{e:?}"),
            };
            return Err(CallError::Trap(msg));
        }
        Ok(match (&m.result, results.first()) {
            (Some(Ty::Result { ok, err }), Some(Val::Result(r))) => {
                let (t, p, is_err) = match r {
                    Ok(p) => (ok, p, false),
                    Err(p) => (err, p, true),
                };
                let value = match (t, p) {
                    (Some(t), Some(p)) => json::val_to_json(t, p),
                    _ => J::Null,
                };
                CallOutput { value, is_err }
            }
            (Some(t), Some(v)) => CallOutput { value: json::val_to_json(t, v), is_err: false },
            _ => CallOutput { value: J::Null, is_err: false },
        })
    }
}

pub fn resolve_method<'a>(m: &'a Manifest, actor_type: &str, method: &str) -> Result<&'a Method, CallError> {
    let t = m.actor_type(actor_type).ok_or_else(|| {
        CallError::NotFound(format!(
            "app {} has no actor type {actor_type:?} (types: {})",
            m.app,
            m.types.iter().map(|t| t.name.as_str()).collect::<Vec<_>>().join(", ")
        ))
    })?;
    t.method(method).ok_or_else(|| {
        CallError::NotFound(format!(
            "actor type {actor_type} has no method {method:?} (methods: {})",
            t.methods.iter().map(|m| m.name.as_str()).collect::<Vec<_>>().join(", ")
        ))
    })
}

/// Opens an actor database with the settings the replicator relies on.
pub fn open_db(path: &std::path::Path) -> Result<Connection> {
    let c = Connection::open(path)?;
    c.pragma_update(None, "journal_mode", "WAL")?;
    c.pragma_update(None, "wal_autocheckpoint", 0)?;
    c.pragma_update(None, "synchronous", "FULL")?;
    c.busy_timeout(Duration::from_secs(5))?;
    Ok(c)
}

/// Applies pending migrations inside the caller's transaction. Returns the
/// names applied.
pub fn apply_migrations(conn: &Connection, migrations: &[Migration]) -> Result<Vec<String>> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS _statex_migrations(name TEXT PRIMARY KEY, applied_at INTEGER)")?;
    let mut applied = Vec::new();
    for m in migrations {
        let done: i64 =
            conn.query_row("SELECT count(*) FROM _statex_migrations WHERE name = ?1", [&m.name], |r| r.get(0))?;
        if done > 0 {
            continue;
        }
        conn.execute_batch(&m.sql).with_context(|| format!("migration {}", m.name))?;
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
        conn.execute("INSERT INTO _statex_migrations(name, applied_at) VALUES(?1, ?2)", rusqlite::params![m.name, now])?;
        applied.push(m.name.clone());
    }
    Ok(applied)
}

pub fn sha256_hex(data: &[u8]) -> String {
    use sha2::Digest;
    hex::encode(sha2::Sha256::digest(data))
}
