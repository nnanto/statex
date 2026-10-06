//! Host implementations of the `statex:host` interfaces.

use std::sync::Arc;
use std::time::{Duration, Instant};

use wasmtime::component::ResourceTable;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use crate::calls::{ActorCaller, ActorRef};
use crate::database::{DatabaseHandle, SqlValue};
use crate::manifest::HttpPolicy;

wasmtime::component::bindgen!({
    path: "../../wit",
    world: "imports",
});

use statex::host::{actors, alarms, context, http_client, log, sql};

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
    pub(crate) limits: crate::limits::MemoryEnvelope,
    pub identity: ActorIdentity,
    pub db: DatabaseHandle,
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
    pub extensions: crate::Extensions,
    pub(crate) http_transport: Arc<dyn crate::HttpTransport>,
    pub(crate) log_sink: Arc<dyn crate::LogSink>,
    /// Failures of capabilities whose WIT signatures cannot return errors.
    pub(crate) capability_error: Option<String>,
    pub(crate) invocation_context: Option<crate::invocation::InvocationContext>,
}

impl HostState {
    /// Present only while guest code executes, including additional imports.
    pub fn invocation_context(&self) -> Option<&crate::invocation::InvocationContext> {
        self.invocation_context.as_ref()
    }
    /// The actor's resource table, shared by WASI and additional host interfaces.
    pub fn resource_table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}

impl actors::Host for HostState {}

impl WasiView for HostState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
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
        if !self.has_alarm {
            return Err(format!(
                "actor type {} has no alarm handler; export `alarm: func(retry-count: u32);` in its interface",
                self.identity.actor_type
            ));
        }
        self.db
            .lock()
            .unwrap()
            .set_alarm(at_ms, 0, self.identity.epoch)
            .map_err(|e| e.to_string())
    }

    fn get(&mut self) -> Option<u64> {
        let result = self.db.lock().unwrap().alarm();
        match result {
            Ok(alarm) => alarm.map(|a| a.at_ms),
            Err(error) => {
                let error = format!("read alarm: {error:#}");
                tracing::error!("{error}");
                self.capability_error.get_or_insert(error);
                None
            }
        }
    }

    fn clear(&mut self) {
        let result = self.db.lock().unwrap().clear_alarm();
        if let Err(error) = result {
            let error = format!("clear alarm: {error:#}");
            tracing::error!("{error}");
            self.capability_error.get_or_insert(error);
        }
    }
}

/// Statements a guest may not run: the host owns transactions and the file.
pub fn forbidden(stmt: &str) -> Option<&'static str> {
    const KEYWORDS: &[&str] = &[
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
    ];
    let bytes = stmt.as_bytes();
    let mut i = 0;
    let mut statement_start = true;
    while i < bytes.len() {
        if bytes[i].is_ascii_whitespace() {
            i += 1;
        } else if bytes[i..].starts_with(b"\xef\xbb\xbf") {
            i += 3;
        } else if bytes[i..].starts_with(b"--") {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' { i += 1; }
        } else if bytes[i..].starts_with(b"/*") {
            i += 2;
            while i < bytes.len() && !bytes[i..].starts_with(b"*/") { i += 1; }
            i = (i + 2).min(bytes.len());
        } else if bytes[i] == b';' {
            statement_start = true;
            i += 1;
        } else if matches!(bytes[i], b'\'' | b'"' | b'`' | b'[') {
            statement_start = false;
            let quote = if bytes[i] == b'[' { b']' } else { bytes[i] };
            i += 1;
            while i < bytes.len() {
                if bytes[i] == quote {
                    i += 1;
                    if i < bytes.len() && bytes[i] == quote { i += 1; } else { break; }
                } else { i += 1; }
            }
        } else {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') { i += 1; }
            if statement_start && i > start {
                for &keyword in KEYWORDS {
                    if bytes[start..i].eq_ignore_ascii_case(keyword.as_bytes()) { return Some(keyword); }
                }
            }
            statement_start = false;
            if i == start { i += 1; }
        }
    }
    None
}

fn to_value(v: &sql::Value) -> SqlValue {
    match v {
        sql::Value::Null => SqlValue::Null,
        sql::Value::Integer(i) => SqlValue::Integer(*i),
        sql::Value::Real(f) => SqlValue::Real(*f),
        sql::Value::Text(t) => SqlValue::Text(t.clone()),
        sql::Value::Blob(b) => SqlValue::Blob(b.clone()),
    }
}

fn from_value(v: SqlValue) -> sql::Value {
    match v {
        SqlValue::Null => sql::Value::Null,
        SqlValue::Integer(i) => sql::Value::Integer(i),
        SqlValue::Real(f) => sql::Value::Real(f),
        SqlValue::Text(t) => sql::Value::Text(t),
        SqlValue::Blob(b) => sql::Value::Blob(b),
    }
}

impl sql::Host for HostState {
    fn execute(&mut self, stmt: String, params: Vec<sql::Value>) -> Result<u64, String> {
        if let Some(kw) = forbidden(&stmt) {
            return Err(format!(
                "{kw} is not allowed: the host manages transactions"
            ));
        }
        let p: Vec<_> = params.iter().map(to_value).collect();
        self.db
            .lock()
            .unwrap()
            .execute(&stmt, &p)
            .map_err(|e| e.to_string())
    }

    fn query(&mut self, stmt: String, params: Vec<sql::Value>) -> Result<sql::Rows, String> {
        if let Some(kw) = forbidden(&stmt) {
            return Err(format!(
                "{kw} is not allowed: the host manages transactions"
            ));
        }
        let p: Vec<_> = params.iter().map(to_value).collect();
        self.db
            .lock()
            .unwrap()
            .query(&stmt, &p)
            .map(|r| sql::Rows {
                columns: r.columns,
                rows: r
                    .rows
                    .into_iter()
                    .map(|row| row.into_iter().map(from_value).collect())
                    .collect(),
            })
            .map_err(|e| e.to_string())
    }
}

/// Whether `host` matches an allowlist entry (`*`, `*.example.com`, `example.com`).
pub fn host_allowed(policy: &HttpPolicy, host: &str) -> bool {
    let host = host.to_ascii_lowercase();
    policy.allow.iter().any(|a| {
        let a = a.to_ascii_lowercase();
        a == "*"
            || a == host
            || a.strip_prefix("*.")
                .is_some_and(|d| host.ends_with(&format!(".{d}")))
    })
}

impl http_client::Host for HostState {
    fn send(&mut self, req: http_client::Request) -> Result<http_client::Response, String> {
        let url = url_host(&req.url).ok_or_else(|| format!("invalid url {:?}", req.url))?;
        if !host_allowed(&self.http, &url) {
            return Err(format!(
                "host {url:?} is not allowed; add it to [http] allow in statex.toml"
            ));
        }
        let timeout = self
            .deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
            .unwrap_or(self.http_timeout)
            .min(self.http_timeout);
        if timeout.is_zero() {
            return Err("HTTP request deadline expired".into());
        }
        self.http_transport.send(req, timeout)
    }
}

fn url_host(url: &str) -> Option<String> {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))?;
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
        self.log_sink.log(&self.identity, level, &msg);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::sync::Mutex;

    #[test]
    fn transaction_controls_cannot_hide_behind_comments_or_statements() {
        for statement in ["-- comment\n COMMIT", "/* comment */ ROLLBACK", "; BEGIN", "SELECT 1; /* c */ PRAGMA journal_mode = off"] {
            assert!(forbidden(statement).is_some(), "{statement}");
        }
        for statement in ["SELECT 'COMMIT; ROLLBACK'", "SELECT \"COMMIT\"", "SELECT 1 -- COMMIT", "SELECT 1 /* COMMIT */"] {
            assert!(forbidden(statement).is_none(), "{statement}");
        }
    }

    #[test]
    fn allowlist() {
        let p = HttpPolicy {
            allow: vec!["api.example.com".into(), "*.corp.test".into()],
        };
        assert!(host_allowed(&p, "api.example.com"));
        assert!(host_allowed(&p, "a.corp.test"));
        assert!(!host_allowed(&p, "corp.test.evil.com"));
        assert!(!host_allowed(&p, "example.com"));
        assert_eq!(
            url_host("https://u:p@a.corp.test:8443/x?y").as_deref(),
            Some("a.corp.test")
        );
    }

    fn host_state() -> HostState {
        HostState {
            wasi: wasmtime_wasi::WasiCtxBuilder::new().build(),
            table: Default::default(),
            limits: Default::default(),
            identity: ActorIdentity {
                app: "a".into(),
                actor_type: "t".into(),
                key: "k".into(),
                epoch: 2,
            },
            db: crate::database::sqlite_handle(Arc::new(Mutex::new(
                Connection::open_in_memory().unwrap(),
            ))),
            http: HttpPolicy { allow: vec![] },
            http_timeout: Duration::from_secs(1),
            caller: None,
            invocation_context: None,
            chain: vec![],
            deadline: None,
            has_alarm: false,
            extensions: Default::default(),
            http_transport: Arc::new(crate::DefaultHttpTransport),
            log_sink: Arc::new(crate::TracingLogSink),
            capability_error: None,
        }
    }

    #[test]
    fn alarm_set_requires_a_handler() {
        let mut s = host_state();
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
    fn additional_host_resources_share_the_wasi_table() {
        let mut state = host_state();
        let resource = state.resource_table().push(42u32).unwrap();
        assert_eq!(*WasiView::ctx(&mut state).table.get(&resource).unwrap(), 42);
        assert_eq!(state.resource_table().delete(resource).unwrap(), 42);
    }

    #[test]
    fn infallible_alarm_imports_record_backend_errors() {
        let mut state = host_state();
        state
            .db
            .lock()
            .unwrap()
            .execute("CREATE TABLE _statex_alarm(id INTEGER PRIMARY KEY)", &[])
            .unwrap();
        assert_eq!(alarms::Host::get(&mut state), None);
        let read_error = state.capability_error.clone().unwrap();
        assert!(read_error.contains("read alarm"), "{read_error}");
        alarms::Host::clear(&mut state);
        assert_eq!(
            state.capability_error,
            Some(read_error),
            "the first failure must remain sticky"
        );
        state.capability_error = None;
        alarms::Host::clear(&mut state);
        assert!(state
            .capability_error
            .take()
            .unwrap()
            .contains("clear alarm"));
    }

    #[derive(Default)]
    struct RecordingTransport {
        requests: Mutex<Vec<(http_client::Request, Duration)>>,
    }

    impl crate::HttpTransport for RecordingTransport {
        fn send(
            &self,
            request: http_client::Request,
            timeout: Duration,
        ) -> Result<http_client::Response, String> {
            self.requests.lock().unwrap().push((request, timeout));
            Ok(http_client::Response {
                status: 201,
                headers: vec![("x-adapter".into(), "custom".into())],
                body: b"ok".to_vec(),
            })
        }
    }

    #[test]
    fn custom_http_transport_cannot_bypass_admission_or_deadlines() {
        let transport = Arc::new(RecordingTransport::default());
        let mut state = host_state();
        state.http_transport = transport.clone();
        state.http.allow = vec!["api.example.com".into()];
        let request = |url: &str| http_client::Request {
            method: "POST".into(),
            url: url.into(),
            headers: vec![("x-request".into(), "test".into())],
            body: Some(vec![1, 2]),
        };
        let error = http_client::Host::send(&mut state, request("https://blocked.example.com/"))
            .unwrap_err();
        assert!(error.contains("not allowed"), "{error}");
        let error =
            http_client::Host::send(&mut state, request("ftp://api.example.com/")).unwrap_err();
        assert!(error.contains("invalid url"), "{error}");
        assert!(transport.requests.lock().unwrap().is_empty());

        let response =
            http_client::Host::send(&mut state, request("https://api.example.com/path")).unwrap();
        assert_eq!(response.status, 201);
        assert_eq!(response.headers, [("x-adapter".into(), "custom".into())]);
        assert_eq!(response.body, b"ok");
        {
            let requests = transport.requests.lock().unwrap();
            assert_eq!(requests[0].0.method, "POST");
            assert_eq!(requests[0].0.url, "https://api.example.com/path");
            assert_eq!(requests[0].0.body, Some(vec![1, 2]));
            assert_eq!(requests[0].1, Duration::from_secs(1));
        }
        state.http_timeout = Duration::from_secs(60);
        state.deadline = Some(Instant::now() + Duration::from_secs(10));
        http_client::Host::send(&mut state, request("https://api.example.com/")).unwrap();
        let budget = transport.requests.lock().unwrap()[1].1;
        assert!(!budget.is_zero() && budget <= Duration::from_secs(10));
        state.http_timeout = Duration::from_secs(1);
        http_client::Host::send(&mut state, request("https://api.example.com/")).unwrap();
        assert_eq!(
            transport.requests.lock().unwrap()[2].1,
            Duration::from_secs(1)
        );

        state.deadline = Some(Instant::now() - Duration::from_secs(1));
        let error =
            http_client::Host::send(&mut state, request("https://api.example.com/")).unwrap_err();
        assert!(error.contains("deadline expired"), "{error}");
        state.deadline = None;
        state.http_timeout = Duration::ZERO;
        assert!(http_client::Host::send(&mut state, request("https://api.example.com/")).is_err());
        assert_eq!(
            transport.requests.lock().unwrap().len(),
            3,
            "expired calls must not reach the adapter"
        );
    }

    #[derive(Default)]
    struct RecordingLog {
        messages: Mutex<Vec<(ActorIdentity, log::Level, String)>>,
    }

    impl crate::LogSink for RecordingLog {
        fn log(&self, identity: &ActorIdentity, level: log::Level, message: &str) {
            self.messages
                .lock()
                .unwrap()
                .push((identity.clone(), level, message.into()));
        }
    }

    #[test]
    fn custom_log_sink_receives_actor_identity_level_and_message() {
        let sink = Arc::new(RecordingLog::default());
        let mut state = host_state();
        state.log_sink = sink.clone();
        log::Host::log(&mut state, log::Level::Warn, "custom message".into());
        let messages = sink.messages.lock().unwrap();
        let [(identity, level, message)] = messages.as_slice() else {
            panic!("one log message expected")
        };
        assert_eq!(
            (
                &identity.app,
                &identity.actor_type,
                &identity.key,
                identity.epoch
            ),
            (
                &state.identity.app,
                &state.identity.actor_type,
                &state.identity.key,
                state.identity.epoch
            )
        );
        assert!(matches!(level, log::Level::Warn));
        assert_eq!(message, "custom message");
    }

    #[test]
    fn forbidden_statements() {
        assert_eq!(forbidden("  begin immediate"), Some("BEGIN"));
        assert_eq!(forbidden("PRAGMA journal_mode=delete"), Some("PRAGMA"));
        assert_eq!(forbidden("SELECT 1"), None);
        assert_eq!(forbidden("ENDPOINTS"), None);
    }
}
