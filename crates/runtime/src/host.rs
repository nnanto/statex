//! Host implementations of the `statex:host` interfaces.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rusqlite::types::Value as RV;
use rusqlite::Connection;
use wasmtime::component::ResourceTable;
use wasmtime::StoreLimits;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use crate::calls::{ActorCaller, ActorRef};
use crate::manifest::HttpPolicy;

wasmtime::component::bindgen!({
    path: "../../wit",
    world: "imports",
});

use statex::host::{actors, alarms, context, http_client, log, sql, spawn};

/// Identity of the executing actor, exposed through `statex:host/context`.
#[derive(Debug, Clone)]
pub struct ActorIdentity {
    pub app: String,
    pub actor_type: String,
    pub key: String,
    pub epoch: u64,
}

/// Per-actor store data.
pub struct HostState {
    pub(crate) wasi: WasiCtx,
    pub(crate) table: ResourceTable,
    pub(crate) limits: StoreLimits,
    pub identity: ActorIdentity,
    pub db: Arc<Mutex<Connection>>,
    pub(crate) http: HttpPolicy,
    pub(crate) http_timeout: Duration,
    /// Routes actor-to-actor calls; `None` where calls are unavailable.
    pub(crate) caller: Option<Arc<dyn ActorCaller>>,
    /// Call chain of the running method, ending with this actor.
    pub(crate) chain: Vec<ActorRef>,
    /// When the running method times out.
    pub(crate) deadline: Option<Instant>,
    /// Whether the actor type exports an alarm handler.
    pub(crate) has_alarm: bool,
    pub(crate) stateless: bool,
    pub(crate) spawn_methods: Vec<(String, String, crate::manifest::Method)>,
}

impl actors::Host for HostState {}

impl spawn::Host for HostState {
    fn send(&mut self, app: String, actor_type: String, key: String, method: String, args_json: String) -> Result<String, String> {
        if self.stateless {
            return Err("stateless actors have no transaction for a durable spawn".into());
        }
        if key.is_empty() || key.len() > 512 {
            return Err("spawn target key must be 1..=512 bytes".into());
        }
        if args_json.len() > crate::spawn::MAX_ARGS_BYTES {
            return Err(format!("spawn arguments exceed {} bytes", crate::spawn::MAX_ARGS_BYTES));
        }
        let signature = self.spawn_methods.iter()
            .find(|(a, t, m)| a == &app && t == &actor_type && m.name == method)
            .map(|(_, _, m)| m)
            .ok_or_else(|| format!("spawn target {app}/{actor_type}.{method} is not exported by this app or declared in its client imports"))?;
        let args: serde_json::Value = serde_json::from_str(&args_json).map_err(|e| format!("invalid spawn arguments: {e}"))?;
        crate::json::args_to_vals(signature, &args)?;
        crate::spawn::enqueue(&self.db.lock().unwrap(), self.identity.epoch, &app, &actor_type, &key, &method, &args_json)
    }
}

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView { ctx: &mut self.wasi, table: &mut self.table }
    }
}

impl context::Host for HostState {
    fn app(&mut self) -> String {
        self.identity.app.clone()
    }
    fn actor_type(&mut self) -> String {
        self.identity.actor_type.clone()
    }
    fn key(&mut self) -> String {
        self.identity.key.clone()
    }
    fn epoch(&mut self) -> u64 {
        self.identity.epoch
    }
}

impl alarms::Host for HostState {
    fn set(&mut self, at_ms: u64) -> Result<(), String> {
        if self.stateless {
            return Err("stateless actors cannot schedule alarms".into());
        }
        if !self.has_alarm {
            return Err(format!(
                "actor type {} has no alarm handler; export `alarm: func(retry-count: u32);` in its interface",
                self.identity.actor_type
            ));
        }
        let db = self.db.lock().unwrap();
        crate::alarm::set(&db, at_ms, self.identity.epoch).map_err(|e| e.to_string())
    }

    fn get(&mut self) -> Option<u64> {
        let db = self.db.lock().unwrap();
        crate::alarm::read(&db).ok().flatten().map(|a| a.at_ms)
    }

    fn clear(&mut self) {
        let db = self.db.lock().unwrap();
        if let Err(e) = crate::alarm::clear(&db) {
            tracing::warn!("clear alarm: {e}");
        }
    }
}

/// Statements a guest may not run: the host owns transactions and the file.
pub fn forbidden(stmt: &str) -> Option<&'static str> {
    let s = stmt.trim_start().to_ascii_uppercase();
    for kw in [
        "BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE", "ATTACH", "DETACH", "VACUUM", "PRAGMA",
    ] {
        if s.starts_with(kw)
            && s[kw.len()..].chars().next().is_none_or(|c| !c.is_ascii_alphanumeric() && c != '_')
        {
            return Some(kw);
        }
    }
    None
}

fn to_rv(v: &sql::Value) -> RV {
    match v {
        sql::Value::Null => RV::Null,
        sql::Value::Integer(i) => RV::Integer(*i),
        sql::Value::Real(f) => RV::Real(*f),
        sql::Value::Text(t) => RV::Text(t.clone()),
        sql::Value::Blob(b) => RV::Blob(b.clone()),
    }
}

fn from_rv(v: RV) -> sql::Value {
    match v {
        RV::Null => sql::Value::Null,
        RV::Integer(i) => sql::Value::Integer(i),
        RV::Real(f) => sql::Value::Real(f),
        RV::Text(t) => sql::Value::Text(t),
        RV::Blob(b) => sql::Value::Blob(b),
    }
}

impl sql::Host for HostState {
    fn execute(&mut self, stmt: String, params: Vec<sql::Value>) -> Result<u64, String> {
        if self.stateless {
            return Err("stateless actors have no SQL storage".into());
        }
        if let Some(kw) = forbidden(&stmt) {
            return Err(format!("{kw} is not allowed: the host manages transactions"));
        }
        let p: Vec<RV> = params.iter().map(to_rv).collect();
        let db = self.db.lock().unwrap();
        db.execute(&stmt, rusqlite::params_from_iter(p.iter())).map(|n| n as u64).map_err(|e| e.to_string())
    }

    fn query(&mut self, stmt: String, params: Vec<sql::Value>) -> Result<sql::Rows, String> {
        if self.stateless {
            return Err("stateless actors have no SQL storage".into());
        }
        if let Some(kw) = forbidden(&stmt) {
            return Err(format!("{kw} is not allowed: the host manages transactions"));
        }
        let p: Vec<RV> = params.iter().map(to_rv).collect();
        let db = self.db.lock().unwrap();
        let run = || -> rusqlite::Result<sql::Rows> {
            let mut st = db.prepare(&stmt)?;
            let columns: Vec<String> = st.column_names().iter().map(|s| s.to_string()).collect();
            let n = columns.len();
            let rows = st
                .query_map(rusqlite::params_from_iter(p.iter()), |r| {
                    (0..n).map(|i| r.get::<_, RV>(i).map(from_rv)).collect::<rusqlite::Result<Vec<_>>>()
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(sql::Rows { columns, rows })
        };
        run().map_err(|e| e.to_string())
    }
}

/// Whether `host` matches an allowlist entry (`*`, `*.example.com`, `example.com`).
pub fn host_allowed(policy: &HttpPolicy, host: &str) -> bool {
    let host = host.to_ascii_lowercase();
    policy.allow.iter().any(|a| {
        let a = a.to_ascii_lowercase();
        a == "*" || a == host || a.strip_prefix("*.").is_some_and(|d| host.ends_with(&format!(".{d}")))
    })
}

const MAX_BODY: u64 = 10 * 1024 * 1024;

impl http_client::Host for HostState {
    fn send(&mut self, req: http_client::Request) -> Result<http_client::Response, String> {
        let url = url_host(&req.url).ok_or_else(|| format!("invalid url {:?}", req.url))?;
        if !host_allowed(&self.http, &url) {
            return Err(format!(
                "host {url:?} is not allowed; add it to [http] allow in statex.toml"
            ));
        }
        let agent = ureq::AgentBuilder::new().timeout(self.http_timeout).build();
        let mut r = agent.request(&req.method, &req.url);
        for (k, v) in &req.headers {
            r = r.set(k, v);
        }
        let resp = match req.body {
            Some(b) => r.send_bytes(&b),
            None => r.call(),
        };
        let resp = match resp {
            Ok(r) => r,
            Err(ureq::Error::Status(_, r)) => r,
            Err(e) => return Err(e.to_string()),
        };
        let status = resp.status();
        let headers = resp
            .headers_names()
            .into_iter()
            .filter_map(|n| resp.header(&n).map(|v| (n.clone(), v.to_string())))
            .collect();
        let mut body = Vec::new();
        std::io::Read::read_to_end(&mut std::io::Read::take(resp.into_reader(), MAX_BODY), &mut body)
            .map_err(|e| e.to_string())?;
        Ok(http_client::Response { status, headers, body })
    }
}

fn url_host(url: &str) -> Option<String> {
    let rest = url.strip_prefix("https://").or_else(|| url.strip_prefix("http://"))?;
    let authority = rest.split(['/', '?', '#']).next()?;
    let authority = authority.rsplit('@').next()?;
    let host = if authority.starts_with('[') {
        authority.split(']').next()?.trim_start_matches('[')
    } else {
        authority.split(':').next()?
    };
    (!host.is_empty()).then(|| host.to_string())
}

impl log::Host for HostState {
    fn log(&mut self, level: log::Level, msg: String) {
        let id = &self.identity;
        let actor = format!("{}/{}/{}", id.app, id.actor_type, id.key);
        match level {
            log::Level::Trace => tracing::trace!(target: "actor", %actor, "{msg}"),
            log::Level::Debug => tracing::debug!(target: "actor", %actor, "{msg}"),
            log::Level::Info => tracing::info!(target: "actor", %actor, "{msg}"),
            log::Level::Warn => tracing::warn!(target: "actor", %actor, "{msg}"),
            log::Level::Error => tracing::error!(target: "actor", %actor, "{msg}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allowlist() {
        let p = HttpPolicy { allow: vec!["api.example.com".into(), "*.corp.test".into()] };
        assert!(host_allowed(&p, "api.example.com"));
        assert!(host_allowed(&p, "a.corp.test"));
        assert!(!host_allowed(&p, "corp.test.evil.com"));
        assert!(!host_allowed(&p, "example.com"));
        assert_eq!(url_host("https://u:p@a.corp.test:8443/x?y").as_deref(), Some("a.corp.test"));
    }

    #[test]
    fn alarm_set_requires_a_handler() {
        let mut s = HostState {
            wasi: wasmtime_wasi::WasiCtxBuilder::new().build(),
            table: Default::default(),
            limits: Default::default(),
            identity: ActorIdentity { app: "a".into(), actor_type: "t".into(), key: "k".into(), epoch: 2 },
            db: Arc::new(Mutex::new(Connection::open_in_memory().unwrap())),
            http: HttpPolicy { allow: vec![] },
            http_timeout: Duration::from_secs(1),
            caller: None,
            chain: vec![],
            deadline: None,
            has_alarm: false,
            stateless: false,
            spawn_methods: Vec::new(),
        };
        let e = alarms::Host::set(&mut s, 10).unwrap_err();
        assert!(e.contains("no alarm handler"), "{e}");
        assert_eq!(alarms::Host::get(&mut s), None);
        s.has_alarm = true;
        alarms::Host::set(&mut s, 10).unwrap();
        assert_eq!(alarms::Host::get(&mut s), Some(10));
        alarms::Host::clear(&mut s);
        assert_eq!(alarms::Host::get(&mut s), None);
    }

    #[test]
    fn forbidden_statements() {
        assert_eq!(forbidden("  begin immediate"), Some("BEGIN"));
        assert_eq!(forbidden("PRAGMA journal_mode=delete"), Some("PRAGMA"));
        assert_eq!(forbidden("SELECT 1"), None);
        assert_eq!(forbidden("ENDPOINTS"), None);
    }
}
