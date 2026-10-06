//! Native mock host used by `cargo test` (see [`crate::testing`]).

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;

use rusqlite::types::Value as RV;
use rusqlite::Connection;

use crate::{http, log::Level, sql::Rows, sql::Value, Error, Result};

pub(crate) type HttpHandler = Box<dyn FnMut(&http::Request) -> Result<http::Response, String>>;
pub(crate) type SpawnHandler =
    std::rc::Rc<dyn Fn(&str, &[serde_json::Value]) -> Result<serde_json::Value, crate::CallError>>;

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
    pub jobs: Vec<MockJob>,
    pub clock_ms: u64,
    pub next_job: u64,
    pub spawn_handlers: HashMap<(String, String, String), SpawnHandler>,
}

pub(crate) struct MockJob {
    pub id: String,
    pub source: (String, String),
    pub source_app: String,
    pub app: String,
    pub actor_type: String,
    pub key: String,
    pub method: String,
    pub args: serde_json::Value,
    pub due_ms: u64,
    pub status: String,
    pub attempts: u32,
    pub result: Option<serde_json::Value>,
    pub error: Option<serde_json::Value>,
    pub callback: Option<Box<dyn FnOnce() -> Result<serde_json::Value, crate::CallError>>>,
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
            jobs: Vec::new(),
            clock_ms: 0,
            next_job: 0,
            spawn_handlers: HashMap::new(),
        }
    }
}

pub fn actors_spawn(
    app: &str,
    actor_type: &str,
    key: &str,
    method: &str,
    args: &str,
    delay_ms: u64,
) -> Result<String, crate::CallError> {
    let source = current();
    let args: serde_json::Value =
        serde_json::from_str(args).map_err(|e| crate::CallError::Rejected(e.to_string()))?;
    if !args.is_array() {
        return Err(crate::CallError::Rejected(
            "spawn arguments must be a positional array".into(),
        ));
    }
    STATE.with(|s| {
        let mut s = s.borrow_mut();
        s.next_job += 1;
        let id = format!("mock-job-{}", s.next_job);
        let due_ms = s.clock_ms.saturating_add(delay_ms);
        let source_app = s.app.clone();
        s.jobs.push(MockJob {
            id: id.clone(),
            source,
            source_app,
            app: app.into(),
            actor_type: actor_type.into(),
            key: key.into(),
            method: method.into(),
            args,
            due_ms,
            status: "pending".into(),
            attempts: 0,
            result: None,
            error: None,
            callback: None,
        });
        Ok(id)
    })
}

pub fn actors_job(id: &str) -> Result<Option<String>> {
    let source = current();
    STATE.with(|s| {
        Ok(s.borrow()
            .jobs
            .iter()
            .find(|j| j.id == id && j.source == source && j.source_app == s.borrow().app)
            .map(|j| {
                serde_json::json!({
                    "id": j.id, "target": {"app": j.app, "type": j.actor_type, "key": j.key},
                    "method": j.method, "args": j.args,
                    "status": j.status, "result": j.result, "error": j.error,
                    "not_before_ms": j.due_ms, "next_attempt_ms": j.due_ms,
                    "attempts": j.attempts,
                    "context": {
                        "request_id": format!("mock-source-{}", j.id), "parent_request_id": null,
                        "caller": {"kind": "embedded"}, "principal": null,
                        "deadline_unix_ms": u64::MAX, "attributes": {},
                    },
                })
                .to_string()
            }))
    })
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
    for kw in [
        "BEGIN",
        "COMMIT",
        "END",
        "ROLLBACK",
        "SAVEPOINT",
        "RELEASE",
        "ATTACH",
        "DETACH",
        "VACUUM",
        "PRAGMA",
    ] {
        if s.starts_with(kw)
            && s[kw.len()..]
                .chars()
                .next()
                .map_or(true, |c| !c.is_ascii_alphanumeric() && c != '_')
        {
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
        return Err(Error(format!(
            "{kw} is not allowed: the host manages transactions"
        )));
    }
    let p: Vec<RV> = params.iter().map(to_rv).collect();
    with_conn(|c| {
        c.execute(stmt, rusqlite::params_from_iter(p.iter()))
            .map(|n| n as u64)
    })
}

pub fn sql_query(stmt: &str, params: &[Value]) -> Result<Rows> {
    if let Some(kw) = forbidden(stmt) {
        return Err(Error(format!(
            "{kw} is not allowed: the host manages transactions"
        )));
    }
    let p: Vec<RV> = params.iter().map(to_rv).collect();
    with_conn(|c| {
        let mut st = c.prepare(stmt)?;
        let columns: Vec<String> = st.column_names().iter().map(|s| s.to_string()).collect();
        let n = columns.len();
        let rows = st
            .query_map(rusqlite::params_from_iter(p.iter()), |r| {
                (0..n)
                    .map(|i| r.get::<_, RV>(i).map(from_rv))
                    .collect::<rusqlite::Result<Vec<_>>>()
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

const ALARM_TABLE: &str = "CREATE TABLE IF NOT EXISTS _statex_alarm(
    id INTEGER PRIMARY KEY CHECK (id = 0),
    at_ms INTEGER,
    retry INTEGER NOT NULL DEFAULT 0,
    epoch INTEGER NOT NULL DEFAULT 0,
    seq INTEGER NOT NULL DEFAULT 0)";

pub(crate) fn read_alarm(c: &Connection) -> rusqlite::Result<Option<u64>> {
    c.execute_batch(ALARM_TABLE)?;
    c.query_row("SELECT at_ms FROM _statex_alarm WHERE id = 0", [], |r| {
        r.get::<_, Option<i64>>(0)
    })
    .or_else(|e| {
        if e == rusqlite::Error::QueryReturnedNoRows {
            Ok(None)
        } else {
            Err(e)
        }
    })
    .map(|v| v.map(|v| v as u64))
}

pub fn alarm_set(at_ms: u64) -> Result<()> {
    let epoch = epoch();
    with_conn(|c| {
        c.execute_batch(ALARM_TABLE)?;
        c.execute(
            "INSERT INTO _statex_alarm(id, at_ms, retry, epoch, seq) VALUES(0, ?1, 0, ?2, 1)
             ON CONFLICT(id) DO UPDATE SET at_ms = ?1, retry = 0, epoch = ?2, seq = seq + 1",
            rusqlite::params![i64::try_from(at_ms).unwrap_or(i64::MAX), epoch as i64],
        )
        .map(|_| ())
    })
}

pub fn alarm_get() -> Option<u64> {
    with_conn(read_alarm).ok().flatten()
}

pub fn alarm_clear() {
    let _ = with_conn(|c| {
        c.execute_batch(ALARM_TABLE)?;
        c.execute(
            "UPDATE _statex_alarm SET at_ms = NULL, retry = 0 WHERE id = 0",
            [],
        )
        .map(|_| ())
    });
}
