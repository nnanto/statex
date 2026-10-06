//! # statex-guest
//!
//! Rust SDK for writing statex actors. An actor type is an interface exported by
//! your component's WIT world; this crate gives your implementation access to
//! the host capabilities (`statex:host`): the actor's SQL database (SQLite by default), its
//! identity, its alarm, outbound HTTP and logging.
//!
//! On `wasm32` the functions call the real host. On native targets they run
//! against an in-process mock host (an in-memory SQLite per actor), so plain
//! `cargo test` exercises your actor logic; see [`testing`].

use std::fmt;

/// JSON values used by generated scheduling clients; callers need no direct dependency.
pub use serde_json;

#[cfg(target_arch = "wasm32")]
mod wasm;
#[cfg(target_arch = "wasm32")]
use wasm as backend;

#[cfg(not(target_arch = "wasm32"))]
mod mock;
#[cfg(not(target_arch = "wasm32"))]
use adapters as backend;

#[cfg(not(target_arch = "wasm32"))]
pub mod adapters;

#[cfg(not(target_arch = "wasm32"))]
pub mod testing;

/// Error returned by host calls.
#[derive(Debug, Clone, PartialEq)]
pub struct Error(pub String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error(s)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Identity of the executing actor.
pub mod context {
    use super::backend;

    /// The app name this component was deployed as.
    pub fn app() -> String {
        backend::app()
    }
    /// The actor type, i.e. the exported interface name (`counter`).
    pub fn actor_type() -> String {
        backend::actor_type()
    }
    /// The actor key (`alice`).
    pub fn key() -> String {
        backend::key()
    }
    /// The ownership epoch of the current activation.
    pub fn epoch() -> u64 {
        backend::epoch()
    }
}

/// The actor's private SQL database (SQLite by default).
///
/// Each method call runs in one host-managed transaction: a successful return
/// commits; trapping (panicking) or a method-level `result::err` rolls back.
/// Do not issue `BEGIN`/`COMMIT` yourself. The configured state backend owns
/// the SQL dialect and migration support. The default backend creates schema
/// from the SQL files in `migrations/<actor-type>/`.
pub mod sql {
    use super::{backend, Error, Result};

    #[derive(Debug, Clone, PartialEq)]
    pub enum Value {
        Null,
        Integer(i64),
        Real(f64),
        Text(String),
        Blob(Vec<u8>),
    }

    macro_rules! from_int {
        ($($t:ty),*) => {$(
            impl From<$t> for Value { fn from(v: $t) -> Self { Value::Integer(v as i64) } }
        )*};
    }
    from_int!(i8, i16, i32, i64, u8, u16, u32, u64, isize, usize);
    impl From<bool> for Value {
        fn from(v: bool) -> Self {
            Value::Integer(v as i64)
        }
    }
    impl From<f64> for Value {
        fn from(v: f64) -> Self {
            Value::Real(v)
        }
    }
    impl From<f32> for Value {
        fn from(v: f32) -> Self {
            Value::Real(v as f64)
        }
    }
    impl From<&str> for Value {
        fn from(v: &str) -> Self {
            Value::Text(v.to_string())
        }
    }
    impl From<String> for Value {
        fn from(v: String) -> Self {
            Value::Text(v)
        }
    }
    impl From<&String> for Value {
        fn from(v: &String) -> Self {
            Value::Text(v.clone())
        }
    }
    impl From<Vec<u8>> for Value {
        fn from(v: Vec<u8>) -> Self {
            Value::Blob(v)
        }
    }
    impl From<&[u8]> for Value {
        fn from(v: &[u8]) -> Self {
            Value::Blob(v.to_vec())
        }
    }
    impl<T: Into<Value>> From<Option<T>> for Value {
        fn from(v: Option<T>) -> Self {
            v.map(Into::into).unwrap_or(Value::Null)
        }
    }

    /// Converts a SQL value into a Rust value.
    pub trait FromValue: Sized {
        fn from_value(v: &Value) -> Result<Self>;
    }

    fn type_err(want: &str, v: &Value) -> Error {
        Error(format!("expected {want}, got {v:?}"))
    }

    macro_rules! int_from_value {
        ($($t:ty),*) => {$(
            impl FromValue for $t {
                fn from_value(v: &Value) -> Result<Self> {
                    match v {
                        Value::Integer(i) => <$t>::try_from(*i).map_err(|e| Error(e.to_string())),
                        _ => Err(type_err("integer", v)),
                    }
                }
            }
        )*};
    }
    int_from_value!(i8, i16, i32, i64, u8, u16, u32, u64, isize, usize);

    impl FromValue for bool {
        fn from_value(v: &Value) -> Result<Self> {
            i64::from_value(v).map(|i| i != 0)
        }
    }
    impl FromValue for f64 {
        fn from_value(v: &Value) -> Result<Self> {
            match v {
                Value::Real(f) => Ok(*f),
                Value::Integer(i) => Ok(*i as f64),
                _ => Err(type_err("real", v)),
            }
        }
    }
    impl FromValue for String {
        fn from_value(v: &Value) -> Result<Self> {
            match v {
                Value::Text(s) => Ok(s.clone()),
                _ => Err(type_err("text", v)),
            }
        }
    }
    impl FromValue for Vec<u8> {
        fn from_value(v: &Value) -> Result<Self> {
            match v {
                Value::Blob(b) => Ok(b.clone()),
                Value::Text(s) => Ok(s.as_bytes().to_vec()),
                _ => Err(type_err("blob", v)),
            }
        }
    }
    impl<T: FromValue> FromValue for Option<T> {
        fn from_value(v: &Value) -> Result<Self> {
            match v {
                Value::Null => Ok(None),
                v => T::from_value(v).map(Some),
            }
        }
    }
    impl FromValue for Value {
        fn from_value(v: &Value) -> Result<Self> {
            Ok(v.clone())
        }
    }

    /// Converts a SQL row into a Rust value (implemented for tuples).
    pub trait FromRow: Sized {
        fn from_row(row: &[Value]) -> Result<Self>;
    }

    macro_rules! tuple_from_row {
        ($($n:tt $t:ident),+) => {
            impl<$($t: FromValue),+> FromRow for ($($t,)+) {
                fn from_row(row: &[Value]) -> Result<Self> {
                    Ok(($($t::from_value(row.get($n).ok_or_else(|| Error("missing column".into()))?)?,)+))
                }
            }
        };
    }
    tuple_from_row!(0 A);
    tuple_from_row!(0 A, 1 B);
    tuple_from_row!(0 A, 1 B, 2 C);
    tuple_from_row!(0 A, 1 B, 2 C, 3 D);
    tuple_from_row!(0 A, 1 B, 2 C, 3 D, 4 E);
    tuple_from_row!(0 A, 1 B, 2 C, 3 D, 4 E, 5 F);

    #[derive(Debug, Clone, PartialEq, Default)]
    pub struct Rows {
        pub columns: Vec<String>,
        pub rows: Vec<Vec<Value>>,
    }

    impl Rows {
        /// Column index by name.
        pub fn column(&self, name: &str) -> Option<usize> {
            self.columns.iter().position(|c| c == name)
        }
        /// Decodes every row.
        pub fn decode<T: FromRow>(&self) -> Result<Vec<T>> {
            self.rows.iter().map(|r| T::from_row(r)).collect()
        }
    }

    /// Executes a statement; returns the number of changed rows.
    pub fn execute(stmt: &str, params: &[Value]) -> Result<u64> {
        backend::sql_execute(stmt, params)
    }

    /// Runs a query and returns all rows.
    pub fn query(stmt: &str, params: &[Value]) -> Result<Rows> {
        backend::sql_query(stmt, params)
    }

    /// Runs a query and decodes every row.
    pub fn query_as<T: FromRow>(stmt: &str, params: &[Value]) -> Result<Vec<T>> {
        query(stmt, params)?.decode()
    }

    /// Runs a query and decodes the first row, if any.
    pub fn query_row<T: FromRow>(stmt: &str, params: &[Value]) -> Result<Option<T>> {
        let rows = query(stmt, params)?;
        rows.rows.first().map(|r| T::from_row(r)).transpose()
    }

    /// Runs a query and returns the first column of the first row, if any.
    pub fn query_scalar<T: FromValue>(stmt: &str, params: &[Value]) -> Result<Option<T>> {
        let rows = query(stmt, params)?;
        match rows.rows.first().and_then(|r| r.first()) {
            None => Ok(None),
            Some(v) => T::from_value(v).map(Some),
        }
    }
}

/// Builds a `&[sql::Value]` parameter list: `params![key, 1, "x"]`.
#[macro_export]
macro_rules! params {
    () => { &[] as &[$crate::sql::Value] };
    ($($v:expr),+ $(,)?) => { &[$($crate::sql::Value::from($v)),+] as &[$crate::sql::Value] };
}

/// Outbound HTTP. Only hosts allowed in `statex.toml` (`[http] allow`) are
/// reachable. Requests are not part of the actor transaction.
pub mod http {
    use super::{backend, Result};

    #[derive(Debug, Clone, PartialEq, Default)]
    pub struct Request {
        pub method: String,
        pub url: String,
        pub headers: Vec<(String, String)>,
        pub body: Option<Vec<u8>>,
    }

    #[derive(Debug, Clone, PartialEq, Default)]
    pub struct Response {
        pub status: u16,
        pub headers: Vec<(String, String)>,
        pub body: Vec<u8>,
    }

    impl Response {
        pub fn text(&self) -> String {
            String::from_utf8_lossy(&self.body).into_owned()
        }
        pub fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v.as_str())
        }
    }

    impl Request {
        pub fn new(method: &str, url: &str) -> Self {
            Self {
                method: method.into(),
                url: url.into(),
                ..Default::default()
            }
        }
        pub fn get(url: &str) -> Self {
            Self::new("GET", url)
        }
        pub fn post(url: &str) -> Self {
            Self::new("POST", url)
        }
        pub fn header(mut self, k: &str, v: &str) -> Self {
            self.headers.push((k.into(), v.into()));
            self
        }
        pub fn body(mut self, b: impl Into<Vec<u8>>) -> Self {
            self.body = Some(b.into());
            self
        }
        pub fn send(self) -> Result<Response> {
            send(self)
        }
    }

    pub fn send(req: Request) -> Result<Response> {
        backend::http_send(req)
    }
}

/// Calls to other actors.
///
/// Callees are reached through typed client interfaces generated by
/// `statex calls sync` (in Rust: the `statex_calls` module it writes). Every
/// call returns `Result<T, CallError>`; a callee method that itself returns
/// `result<T, E>` yields `Result<Result<T, E>, CallError>`.
///
/// The callee runs in its own transaction, which commits independently of the
/// caller's: if the caller later traps, the callee's changes stay.
///
/// Generated clients also expose `counter::spawn::increment("alice", 2)` and
/// `counter::spawn_after::increment(Duration::from_secs(30), "alice", 2)`.
/// These return a job ID and only enqueue: enqueue rolls back with the caller,
/// while delivery runs later in a separate callee transaction, at least once.
/// Use [`job`] to inspect progress. Native tests explicitly advance delivery
/// with `testing::drain_jobs`; they never sleep or execute the stub inline.
pub mod actors {
    use serde_json::Value;

    #[doc(hidden)]
    #[derive(Debug, Clone, PartialEq)]
    pub struct SpawnedCall { pub id: String }
    #[doc(hidden)]
    #[derive(Debug, Clone, PartialEq)]
    pub struct DelayedCall { pub id: String, pub delay_ms: u64 }
    use std::fmt;
    use std::time::Duration;

    /// Enqueues a call atomically with the caller's transaction. Arguments are
    /// positional JSON values using the statex WIT JSON representation.
    pub fn spawn(
        app: &str,
        actor_type: &str,
        key: &str,
        method: &str,
        args: &[Value],
    ) -> Result<String, CallError> {
        spawn_after(Duration::ZERO, app, actor_type, key, method, args)
    }

    /// Enqueues a call for delivery after `delay`; this does not execute it inline.
    pub fn spawn_after(
        delay: Duration,
        app: &str,
        actor_type: &str,
        key: &str,
        method: &str,
        args: &[Value],
    ) -> Result<String, CallError> {
        let delay_ms = delay_ms(delay)?;
        super::backend::actors_spawn(
            app,
            actor_type,
            key,
            method,
            &serde_json::to_string(args).unwrap(),
            delay_ms,
        )
    }

    #[doc(hidden)]
    pub fn delay_ms(delay: Duration) -> Result<u64, CallError> {
        u64::try_from(delay.as_millis())
            .map_err(|_| CallError::Rejected("delay exceeds u64 milliseconds".into()))
    }

    /// Inspects a job belonging to the current actor. Missing jobs return `None`.
    pub fn job(id: &str) -> super::Result<Option<Value>> {
        super::backend::actors_job(id)?
            .map(|json| serde_json::from_str(&json).map_err(|e| super::Error(e.to_string())))
            .transpose()
    }

    /// Why a call to another actor did not return the callee's result. This
    /// is `statex:host/actors.call-error`; generated client bindings map to it.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum CallError {
        /// A host extension vetoed the call; no callee transaction committed.
        Rejected(String),
        /// The callee app, actor type or method does not exist.
        NotFound(String),
        /// The callee's signature no longer matches the client interface.
        Incompatible(String),
        /// The callee trapped; its transaction was rolled back.
        Trap(String),
        /// Ownership could not be established or confirmed. The outcome of a write is unknown.
        Unavailable(String),
        /// The callee is already executing further up this call chain.
        Cycle(String),
        /// The caller's time budget ran out. The outcome is unknown.
        Timeout,
    }

    impl fmt::Display for CallError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                CallError::NotFound(m) => write!(f, "not found: {m}"),
                CallError::Incompatible(m) => write!(f, "incompatible: {m}"),
                CallError::Trap(m) => write!(f, "callee trapped: {m}"),
                CallError::Rejected(m) => write!(f, "rejected: {m}"),
                CallError::Unavailable(m) => write!(f, "unavailable: {m}"),
                CallError::Cycle(m) => write!(f, "call cycle: {m}"),
                CallError::Timeout => write!(f, "timed out"),
            }
        }
    }

    impl std::error::Error for CallError {}

    impl From<CallError> for super::Error {
        fn from(e: CallError) -> Self {
            super::Error(e.to_string())
        }
    }
}

pub use actors::CallError;

/// The actor's alarm: one scheduled wake-up, stored in its database.
///
/// When the alarm is due, the host calls the actor type's alarm handler, a
/// function named `alarm` in its interface:
///
/// ```wit
/// /// Runs when this actor's alarm fires.
/// alarm: func(retry-count: u32);
/// ```
///
/// The handler runs in its own transaction, activating the actor if it is not
/// resident. Delivery is at-least-once: if the handler panics or returns
/// `err`, it is retried with exponential backoff (`retry-count` counts the
/// failed attempts) and dropped after 6 retries. It is not a public method.
///
/// Setting and clearing are part of the current transaction: they take effect
/// only if the method commits. The handler may set the next alarm.
pub mod alarm {
    use super::{backend, Result};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    /// Schedules the alarm for `at_ms` (Unix time in milliseconds),
    /// replacing any earlier one. A time in the past fires as soon as
    /// possible. Fails if the actor type has no alarm handler.
    pub fn set(at_ms: u64) -> Result<()> {
        backend::alarm_set(at_ms)
    }

    /// Schedules the alarm `delay` from now.
    pub fn set_in(delay: Duration) -> Result<()> {
        set(now_ms().saturating_add(delay.as_millis() as u64))
    }

    /// When the alarm is scheduled (Unix time in milliseconds), if it is.
    pub fn get() -> Option<u64> {
        backend::alarm_get()
    }

    /// Cancels the alarm, if any.
    pub fn clear() {
        backend::alarm_clear()
    }

    /// The current Unix time in milliseconds.
    pub fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

/// Structured logging to the host.
pub mod log {
    use super::backend;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Level {
        Trace,
        Debug,
        Info,
        Warn,
        Error,
    }

    pub fn log(level: Level, msg: &str) {
        backend::log(level, msg)
    }
    pub fn debug(msg: &str) {
        log(Level::Debug, msg)
    }
    pub fn info(msg: &str) {
        log(Level::Info, msg)
    }
    pub fn warn(msg: &str) {
        log(Level::Warn, msg)
    }
    pub fn error(msg: &str) {
        log(Level::Error, msg)
    }
}
