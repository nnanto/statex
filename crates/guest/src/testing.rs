//! Test harness: run actor methods natively against a mock host.
//!
//! ```ignore
//! use statex_guest::testing;
//!
//! #[test]
//! fn increments() {
//!     assert_eq!(testing::call("counter", "alice", || App::increment(2)), 2);
//!     assert_eq!(testing::call("counter", "alice", || App::increment(3)), 5);
//!     assert_eq!(testing::call("counter", "bob", || App::get()), 0);
//! }
//! ```
//!
//! Each `(actor type, key)` gets its own in-memory SQLite database with the
//! migrations from `migrations/<actor-type>/*.sql` (relative to
//! `CARGO_MANIFEST_DIR`) applied, like the default SQLite host does. Each
//! `call` runs in a transaction: a panic rolls it back (and, with
//! [`try_call`], so does returning `Err`).

use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};

use rusqlite::Connection;

use crate::mock::{MockActor, State, STATE};
use crate::{http, log::Level};

/// Applies pending `*.sql` migrations from `dir` (sorted by file name).
pub(crate) fn apply_migrations(conn: &Connection, dir: &Path) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _statex_migrations(name TEXT PRIMARY KEY, applied_at INTEGER)",
    )?;
    let mut files: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x.eq_ignore_ascii_case("sql")))
            .collect(),
        Err(_) => return Ok(()),
    };
    files.sort();
    for f in files {
        let name = f.file_name().unwrap().to_string_lossy().to_string();
        let done: bool = conn.query_row(
            "SELECT count(*) FROM _statex_migrations WHERE name = ?1",
            [&name],
            |r| r.get::<_, i64>(0),
        )? > 0;
        if done {
            continue;
        }
        let sql = std::fs::read_to_string(&f).expect("read migration");
        conn.execute_batch(&sql)?;
        conn.execute("INSERT INTO _statex_migrations(name, applied_at) VALUES(?1, 0)", [&name])?;
    }
    Ok(())
}

fn ensure_actor(s: &mut State, actor_type: &str, key: &str) {
    let id = (actor_type.to_string(), key.to_string());
    if s.actors.contains_key(&id) {
        return;
    }
    let conn = Connection::open_in_memory().expect("open in-memory sqlite");
    if let Some(dir) = &s.migrations_dir {
        apply_migrations(&conn, &dir.join(actor_type))
            .unwrap_or_else(|e| panic!("migration for actor type {actor_type} failed: {e}"));
    }
    s.actors.insert(id, MockActor { conn, epoch: 1 });
}

/// Invokes `f` as actor `(actor_type, key)` inside one transaction, which is
/// committed unless `f` panics. For methods returning `result<T, E>` use
/// [`try_call`], which also rolls back on `Err` like the real host.
pub fn call<R>(actor_type: &str, key: &str, f: impl FnOnce() -> R) -> R {
    run(actor_type, key, f, |_| true)
}

/// Like [`call`], but rolls the transaction back when `f` returns `Err`,
/// exactly as the host does for a method returning `result<T, E>`.
pub fn try_call<T, E>(actor_type: &str, key: &str, f: impl FnOnce() -> Result<T, E>) -> Result<T, E> {
    run(actor_type, key, f, |r| r.is_ok())
}

fn run<R>(actor_type: &str, key: &str, f: impl FnOnce() -> R, commit: impl FnOnce(&R) -> bool) -> R {
    let id = (actor_type.to_string(), key.to_string());
    STATE.with(|s| {
        let mut s = s.borrow_mut();
        assert!(s.current.is_none(), "nested testing::call is not supported");
        ensure_actor(&mut s, actor_type, key);
        s.actors[&id].conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        s.current = Some(id.clone());
    });
    let out = catch_unwind(AssertUnwindSafe(f));
    STATE.with(|s| {
        let mut s = s.borrow_mut();
        s.current = None;
        let conn = &s.actors[&id].conn;
        let ok = match &out {
            Ok(v) => commit(v),
            Err(_) => false,
        };
        conn.execute_batch(if ok { "COMMIT" } else { "ROLLBACK" }).unwrap();
    });
    match out {
        Ok(v) => v,
        Err(p) => resume_unwind(p),
    }
}

/// Simulates a re-activation (e.g. failover): bumps the actor's epoch.
pub fn reactivate(actor_type: &str, key: &str) {
    STATE.with(|s| {
        let mut s = s.borrow_mut();
        ensure_actor(&mut s, actor_type, key);
        s.actors.get_mut(&(actor_type.into(), key.into())).unwrap().epoch += 1;
    });
}

/// Drops all mock actors, HTTP mocks and logs on this thread.
pub fn reset() {
    STATE.with(|s| *s.borrow_mut() = State::default());
}

/// Sets the app name reported by `context::app()`.
pub fn set_app(name: &str) {
    STATE.with(|s| s.borrow_mut().app = name.to_string());
}

/// Overrides the migrations root (default `$CARGO_MANIFEST_DIR/migrations`).
pub fn set_migrations_dir(dir: impl Into<PathBuf>) {
    STATE.with(|s| s.borrow_mut().migrations_dir = Some(dir.into()));
}

/// Installs an HTTP handler used by `statex_guest::http::send`.
pub fn mock_http(
    f: impl FnMut(&http::Request) -> Result<http::Response, String> + 'static,
) {
    STATE.with(|s| s.borrow_mut().http = Some(Box::new(f)));
}

/// When the alarm of actor `(actor_type, key)` is scheduled, if it is. To
/// test the handler, invoke it like any method:
/// `testing::call("t", "k", || App::alarm(0))`.
pub fn alarm(actor_type: &str, key: &str) -> Option<u64> {
    STATE.with(|s| {
        let mut s = s.borrow_mut();
        ensure_actor(&mut s, actor_type, key);
        crate::mock::read_alarm(&s.actors[&(actor_type.into(), key.into())].conn).expect("read alarm")
    })
}

/// Log lines emitted so far on this thread.
pub fn logs() -> Vec<(Level, String)> {
    STATE.with(|s| s.borrow().logs.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{params, sql};

    fn setup() {
        reset();
        static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("statex-guest-test-{}-{n}", std::process::id()));
        std::fs::create_dir_all(dir.join("kv")).unwrap();
        std::fs::write(
            dir.join("kv/0001_init.sql"),
            "CREATE TABLE kv(k TEXT PRIMARY KEY, v INTEGER NOT NULL);",
        )
        .unwrap();
        set_migrations_dir(dir);
    }

    fn put(k: &str, v: i64) {
        sql::execute(
            "INSERT INTO kv(k, v) VALUES(?1, ?2) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            params![k, v],
        )
        .unwrap();
    }

    #[test]
    fn actors_are_isolated_and_transactional() {
        setup();
        call("kv", "a", || put("x", 1));
        call("kv", "b", || put("x", 2));
        let got: Option<i64> =
            call("kv", "a", || sql::query_scalar("SELECT v FROM kv WHERE k='x'", &[]).unwrap());
        assert_eq!(got, Some(1));
        let r = catch_unwind(|| {
            call("kv", "a", || {
                put("x", 99);
                panic!("boom");
            })
        });
        assert!(r.is_err());
        let got: Option<i64> =
            call("kv", "a", || sql::query_scalar("SELECT v FROM kv WHERE k='x'", &[]).unwrap());
        assert_eq!(got, Some(1), "panic must roll back");
        assert_eq!(call("kv", "a", crate::context::key), "a");
        assert!(call("kv", "a", || sql::execute("BEGIN", &[])).is_err());
    }

    #[test]
    fn alarms_are_transactional() {
        setup();
        assert_eq!(alarm("kv", "a"), None);
        call("kv", "a", || crate::alarm::set(1_000).unwrap());
        assert_eq!(alarm("kv", "a"), Some(1_000));
        let r = catch_unwind(|| {
            call("kv", "a", || {
                crate::alarm::clear();
                panic!("boom");
            })
        });
        assert!(r.is_err());
        assert_eq!(call("kv", "a", crate::alarm::get), Some(1_000), "panic must roll back");
        call("kv", "a", crate::alarm::clear);
        assert_eq!(alarm("kv", "a"), None);
        assert_eq!(alarm("kv", "b"), None);
    }

    #[test]
    fn http_mock() {
        setup();
        mock_http(|r| Ok(http::Response { status: 200, headers: vec![], body: r.url.clone().into_bytes() }));
        let resp = call("kv", "a", || http::Request::get("https://x.test/a").send().unwrap());
        assert_eq!(resp.text(), "https://x.test/a");
    }
}
