//! Scoped native capability adapters. SQL, identity and alarms remain managed
//! by the native test host; WASM uses its component's declared WIT imports.

use std::cell::RefCell;
use std::rc::Rc;

use crate::{http, log::Level, Result};

pub(crate) use crate::mock::{
    actor_type, alarm_clear, alarm_get, alarm_set, app, epoch, key, sql_execute, sql_query,
};

/// Native capabilities that may be replaced without replacing transactional
/// storage. Unimplemented methods retain the mock host's behavior.
///
/// Implementations may call SQL/context/alarm APIs inside `testing::call`.
/// HTTP and logging are not rolled back when an actor call fails.
pub trait Capabilities {
    fn http_send(&self, request: http::Request) -> Result<http::Response> {
        crate::mock::http_send(request)
    }

    fn log(&self, level: Level, message: &str) {
        crate::mock::log(level, message)
    }
}

thread_local! {
    static CURRENT: RefCell<Option<Rc<dyn Capabilities>>> = RefCell::new(None);
}

struct Restore(Option<Rc<dyn Capabilities>>);

impl Drop for Restore {
    fn drop(&mut self) {
        CURRENT.with(|current| *current.borrow_mut() = self.0.take());
    }
}

/// Runs `f` with native capabilities on this thread. Nested scopes restore
/// their predecessor, including when `f` panics. Other threads are unaffected.
///
/// This does not start an actor transaction: use `testing::call` or
/// `testing::try_call` inside the scope as usual. Do not recursively invoke
/// the same overridden capability; delegate through the trait's defaults
/// by leaving that method unimplemented.
pub fn with_capabilities<R>(capabilities: Rc<dyn Capabilities>, f: impl FnOnce() -> R) -> R {
    let previous = CURRENT.with(|current| current.replace(Some(capabilities)));
    let _restore = Restore(previous);
    f()
}

fn current() -> Option<Rc<dyn Capabilities>> {
    CURRENT.with(|current| current.borrow().clone())
}

pub(crate) fn http_send(request: http::Request) -> Result<http::Response> {
    match current() {
        Some(adapter) => adapter.http_send(request),
        None => crate::mock::http_send(request),
    }
}

pub(crate) fn log(level: Level, message: &str) {
    match current() {
        Some(adapter) => adapter.log(level, message),
        None => crate::mock::log(level, message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{alarm, context, http, sql, testing, Error};
    use std::panic::{catch_unwind, AssertUnwindSafe};

    struct Adapter(&'static str);

    impl Capabilities for Adapter {
        fn http_send(&self, _: http::Request) -> Result<http::Response> {
            if self.0 == "error" {
                return Err(Error("adapter failure".into()));
            }
            Ok(http::Response {
                status: 200,
                body: format!("{}:{}", self.0, context::key()).into_bytes(),
                ..Default::default()
            })
        }
    }

    fn request() -> Result<http::Response> {
        http::Request::get("https://example.test").send()
    }

    #[test]
    fn scopes_restore_after_errors_panics_and_nested_calls() {
        testing::reset();
        with_capabilities(Rc::new(Adapter("outer")), || {
            assert_eq!(
                testing::call("kv", "a", || request().unwrap().text()),
                "outer:a"
            );
            with_capabilities(Rc::new(Adapter("inner")), || {
                assert_eq!(
                    testing::call("kv", "b", || request().unwrap().text()),
                    "inner:b"
                );
            });
            with_capabilities(Rc::new(Adapter("error")), || {
                assert_eq!(request().unwrap_err(), Error("adapter failure".into()));
            });
            assert!(catch_unwind(AssertUnwindSafe(|| {
                with_capabilities(Rc::new(Adapter("inner")), || panic!("boom"));
            }))
            .is_err());
            assert_eq!(
                testing::call("kv", "a", || request().unwrap().text()),
                "outer:a"
            );
            assert!(std::thread::spawn(request).join().unwrap().is_err());
        });
        assert!(request().is_err());
    }

    #[test]
    fn adapters_preserve_transactions_defaults_and_actor_isolation() {
        testing::reset();
        with_capabilities(Rc::new(Adapter("custom")), || {
            testing::call("kv", "a", || {
                sql::execute("CREATE TABLE t(v INTEGER)", &[]).unwrap();
                sql::execute("INSERT INTO t VALUES(1)", &[]).unwrap();
                alarm::set(10).unwrap();
                crate::log::info("default log");
                request().unwrap();
                assert!(sql::execute("COMMIT", &[]).is_err());
            });
            let result: Result<()> = testing::try_call("kv", "a", || {
                sql::execute("INSERT INTO t VALUES(2)", &[])?;
                alarm::set(20)?;
                with_capabilities(Rc::new(Adapter("error")), request)?;
                Ok(())
            });
            assert!(result.is_err());
            assert_eq!(
                testing::call("kv", "a", || sql::query_scalar::<i64>(
                    "SELECT count(*) FROM t",
                    &[]
                )
                .unwrap()),
                Some(1)
            );
            assert_eq!(testing::alarm("kv", "a"), Some(10));
            assert_eq!(testing::alarm("kv", "b"), None);
            assert!(testing::call("kv", "b", || sql::query("SELECT * FROM t", &[])).is_err());
            assert_eq!(testing::logs().len(), 1);
        });
        struct Defaults;
        impl Capabilities for Defaults {}
        testing::mock_http(|_| {
            Ok(http::Response {
                status: 204,
                ..Default::default()
            })
        });
        with_capabilities(Rc::new(Defaults), || {
            assert_eq!(request().unwrap().status, 204)
        });
    }

    #[test]
    fn custom_logging_is_used_and_panic_still_rolls_back() {
        struct Logging(RefCell<Vec<(Level, String)>>);
        impl Capabilities for Logging {
            fn log(&self, level: Level, message: &str) {
                self.0.borrow_mut().push((level, message.into()));
            }
        }
        testing::reset();
        let logging = Rc::new(Logging(RefCell::new(Vec::new())));
        with_capabilities(logging.clone(), || {
            testing::call("kv", "a", || alarm::set(10).unwrap());
            assert!(catch_unwind(AssertUnwindSafe(|| {
                testing::call("kv", "a", || {
                    alarm::set(20).unwrap();
                    crate::log::warn("before panic");
                    panic!("boom");
                });
            }))
            .is_err());
            assert_eq!(testing::alarm("kv", "a"), Some(10));
            assert!(testing::logs().is_empty());
        });
        assert_eq!(
            *logging.0.borrow(),
            vec![(Level::Warn, "before panic".into())]
        );
        crate::log::info("restored");
        assert_eq!(testing::logs(), vec![(Level::Info, "restored".into())]);
    }
}
