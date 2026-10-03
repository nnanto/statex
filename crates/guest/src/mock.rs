//! Native mock host used by `cargo test` (see [`crate::testing`]).

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;

use rusqlite::types::Value as RV;
use rusqlite::Connection;

use crate::{http, log::Level, sql::Rows, sql::Value, Error, Result};

pub(crate) type HttpHandler = Box<dyn FnMut(&http::Request) -> Result<http::Response, String>>;

pub(crate) struct MockActor {
    pub conn: Connection,
    pub epoch: u64,
}

pub(crate) struct State {
    pub app: String,
    pub actors: HashMap<(String, String), MockActor>,
    pub current: Option<(String, String)>,
    pub http: Option<HttpHandler>,
    pub migrations_dir: Option<PathBuf>,
    pub logs: Vec<(Level, String)>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            app: "test".into(),
            actors: HashMap::new(),
            current: None,
            http: None,
            migrations_dir: std::env::var_os("CARGO_MANIFEST_DIR")
                .map(|d| PathBuf::from(d).join("migrations")),
            logs: Vec::new(),
        }
    }
}

thread_local! {
    pub(crate) static STATE: RefCell<State> = RefCell::new(State::default());
}

fn current() -> (String, String) {
    STATE.with(|s| {
        s.borrow().current.clone().expect(
            "statex host function called outside an actor. \
             Wrap the call in statex_guest::testing::call(\"type\", \"key\", || ...)",
        )
    })
}

pub fn app() -> String {
    STATE.with(|s| s.borrow().app.clone())
}
pub fn actor_type() -> String {
    current().0
}
pub fn key() -> String {
    current().1
}
pub fn epoch() -> u64 {
    let c = current();
    STATE.with(|s| s.borrow().actors.get(&c).map(|c| c.epoch).unwrap_or(1))
}

/// Statements a guest may not run: the host owns transactions and the file.
pub(crate) fn forbidden(stmt: &str) -> Option<&'static str> {
    let s = stmt.trim_start().to_ascii_uppercase();
    for kw in ["BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE", "ATTACH", "DETACH", "VACUUM", "PRAGMA"] {
        if s.starts_with(kw) && s[kw.len()..].chars().next().map_or(true, |c| !c.is_ascii_alphanumeric() && c != '_') {
            return Some(kw);
        }
    }
    None
}

fn to_rv(v: &Value) -> RV {
    match v {
        Value::Null => RV::Null,
        Value::Integer(i) => RV::Integer(*i),
        Value::Real(f) => RV::Real(*f),
        Value::Text(t) => RV::Text(t.clone()),
        Value::Blob(b) => RV::Blob(b.clone()),
    }
}

fn from_rv(v: RV) -> Value {
    match v {
        RV::Null => Value::Null,
        RV::Integer(i) => Value::Integer(i),
        RV::Real(f) => Value::Real(f),
        RV::Text(t) => Value::Text(t),
        RV::Blob(b) => Value::Blob(b),
    }
}

fn with_conn<T>(f: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> Result<T> {
    let c = current();
    STATE.with(|s| {
        let s = s.borrow();
        let actor = s.actors.get(&c).expect("actor not initialized");
        f(&actor.conn).map_err(|e| Error(e.to_string()))
    })
}

pub fn sql_execute(stmt: &str, params: &[Value]) -> Result<u64> {
    if let Some(kw) = forbidden(stmt) {
        return Err(Error(format!("{kw} is not allowed: the host manages transactions")));
    }
    let p: Vec<RV> = params.iter().map(to_rv).collect();
    with_conn(|c| c.execute(stmt, rusqlite::params_from_iter(p.iter())).map(|n| n as u64))
}

pub fn sql_query(stmt: &str, params: &[Value]) -> Result<Rows> {
    if let Some(kw) = forbidden(stmt) {
        return Err(Error(format!("{kw} is not allowed: the host manages transactions")));
    }
    let p: Vec<RV> = params.iter().map(to_rv).collect();
    with_conn(|c| {
        let mut st = c.prepare(stmt)?;
        let columns: Vec<String> = st.column_names().iter().map(|s| s.to_string()).collect();
        let n = columns.len();
        let rows = st
            .query_map(rusqlite::params_from_iter(p.iter()), |r| {
                (0..n).map(|i| r.get::<_, RV>(i).map(from_rv)).collect::<rusqlite::Result<Vec<_>>>()
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(Rows { columns, rows })
    })
}

pub fn http_send(req: http::Request) -> Result<http::Response> {
    let handler = STATE.with(|s| s.borrow_mut().http.take());
    let Some(mut h) = handler else {
        return Err(Error(format!(
            "no HTTP mock installed for {} {} (use statex_guest::testing::mock_http)",
            req.method, req.url
        )));
    };
    let r = h(&req).map_err(Error);
    STATE.with(|s| s.borrow_mut().http = Some(h));
    r
}

pub fn log(level: Level, msg: &str) {
    eprintln!("[{level:?}] {msg}");
    STATE.with(|s| s.borrow_mut().logs.push((level, msg.to_string())));
}
