//! Real host bindings (wasm32).

use crate::{http, log::Level, sql::Rows, sql::Value, Error, Result};

wit_bindgen::generate!({
    path: "wit",
    world: "imports",
});

use statex::host::{alarms as a, context as c, http_client as h, log as l, sql as s};

pub fn app() -> String {
    c::app()
}
pub fn actor_type() -> String {
    c::actor_type()
}
pub fn key() -> String {
    c::key()
}
pub fn epoch() -> u64 {
    c::epoch()
}

fn to_wit(v: &Value) -> s::Value {
    match v {
        Value::Null => s::Value::Null,
        Value::Integer(i) => s::Value::Integer(*i),
        Value::Real(f) => s::Value::Real(*f),
        Value::Text(t) => s::Value::Text(t.clone()),
        Value::Blob(b) => s::Value::Blob(b.clone()),
    }
}

fn from_wit(v: s::Value) -> Value {
    match v {
        s::Value::Null => Value::Null,
        s::Value::Integer(i) => Value::Integer(i),
        s::Value::Real(f) => Value::Real(f),
        s::Value::Text(t) => Value::Text(t),
        s::Value::Blob(b) => Value::Blob(b),
    }
}

pub fn sql_execute(stmt: &str, params: &[Value]) -> Result<u64> {
    let p: Vec<_> = params.iter().map(to_wit).collect();
    s::execute(stmt, &p).map_err(Error)
}

pub fn sql_query(stmt: &str, params: &[Value]) -> Result<Rows> {
    let p: Vec<_> = params.iter().map(to_wit).collect();
    let r = s::query(stmt, &p).map_err(Error)?;
    Ok(Rows {
        columns: r.columns,
        rows: r.rows.into_iter().map(|row| row.into_iter().map(from_wit).collect()).collect(),
    })
}

pub fn http_send(req: http::Request) -> Result<http::Response> {
    let r = h::send(&h::Request {
        method: req.method,
        url: req.url,
        headers: req.headers,
        body: req.body,
    })
    .map_err(Error)?;
    Ok(http::Response { status: r.status, headers: r.headers, body: r.body })
}

pub fn log(level: Level, msg: &str) {
    let lv = match level {
        Level::Trace => l::Level::Trace,
        Level::Debug => l::Level::Debug,
        Level::Info => l::Level::Info,
        Level::Warn => l::Level::Warn,
        Level::Error => l::Level::Error,
    };
    l::log(lv, msg)
}

pub fn alarm_set(at_ms: u64) -> Result<()> {
    a::set(at_ms).map_err(Error)
}

pub fn alarm_get() -> Option<u64> {
    a::get()
}

pub fn alarm_clear() {
    a::clear()
}
