//! Host implementations of the `statex:host` interfaces.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use rusqlite::types::Value as RV;
use rusqlite::Connection;
use wasmtime::component::ResourceTable;
use wasmtime::StoreLimits;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use crate::manifest::HttpPolicy;

wasmtime::component::bindgen!({
    path: "../../wit",
    world: "imports",
});

use statex::host::{context, http_client, log, sql};

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
        if let Some(kw) = forbidden(&stmt) {
            return Err(format!("{kw} is not allowed: the host manages transactions"));
        }
        let p: Vec<RV> = params.iter().map(to_rv).collect();
        let db = self.db.lock().unwrap();
        db.execute(&stmt, rusqlite::params_from_iter(p.iter())).map(|n| n as u64).map_err(|e| e.to_string())
    }

    fn query(&mut self, stmt: String, params: Vec<sql::Value>) -> Result<sql::Rows, String> {
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
    fn forbidden_statements() {
        assert_eq!(forbidden("  begin immediate"), Some("BEGIN"));
        assert_eq!(forbidden("PRAGMA journal_mode=delete"), Some("PRAGMA"));
        assert_eq!(forbidden("SELECT 1"), None);
        assert_eq!(forbidden("ENDPOINTS"), None);
    }
}
