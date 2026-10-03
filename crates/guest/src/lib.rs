//! # statex-guest
//!
//! Rust SDK for writing statex actors. An actor type is an interface exported by
//! your component's WIT world; this crate gives your implementation access to
//! the host capabilities (`statex:host`): the actor's SQLite database, its
//! identity, outbound HTTP and logging.
//!
//! On `wasm32` the functions call the real host. On native targets they run
//! against an in-process mock host (an in-memory SQLite per actor), so plain
//! `cargo test` exercises your actor logic; see [`testing`].

use std::fmt;

#[cfg(target_arch = "wasm32")]
mod wasm;
#[cfg(target_arch = "wasm32")]
use wasm as backend;

#[cfg(not(target_arch = "wasm32"))]
mod mock;
#[cfg(not(target_arch = "wasm32"))]
use mock as backend;

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

/// The actor's private SQLite database.
///
/// Each method call runs in one host-managed transaction: returning commits,
/// trapping (panicking) rolls back. Do not issue `BEGIN`/`COMMIT` yourself.
/// Schema is created by the SQL files in `migrations/<actor-type>/`.
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
            Self { method: method.into(), url: url.into(), ..Default::default() }
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
