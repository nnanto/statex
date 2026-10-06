//! Runs the example counter component. Builds it (incrementally) first.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde_json::json;
use statex_runtime::*;
use statex_runtime::sqlite::{apply_migrations, open_db};

fn counter_wasm() -> Vec<u8> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/counter");
    let wasm = root.join("target/wasm32-wasip2/release/counter.wasm");
    let st = std::process::Command::new("cargo")
        .args(["build", "--release", "--target", "wasm32-wasip2"])
        .current_dir(&root)
        .status()
        .unwrap();
    assert!(st.success());
    std::fs::read(wasm).unwrap()
}

fn manifest(wasm: &[u8]) -> Manifest {
    let ins = inspect(wasm).unwrap();
    for i in &ins.imports {
        assert!(manifest::import_allowed(i), "unexpected import {i}");
    }
    assert!(ins.imports.iter().any(|i| i.starts_with("statex:host/sql")), "{:?}", ins.imports);
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/counter/migrations");
    let mut migrations = std::collections::BTreeMap::new();
    for t in ["counter", "account"] {
        let mut v = vec![];
        for e in std::fs::read_dir(root.join(t)).unwrap() {
            let p = e.unwrap().path();
            v.push(Migration { name: p.file_name().unwrap().to_string_lossy().into(), sql: std::fs::read_to_string(&p).unwrap() });
        }
        v.sort_by(|a, b| a.name.cmp(&b.name));
        migrations.insert(t.to_string(), v);
    }
    Manifest {
        calls: vec![],
        format: 1,
        app: "counter".into(),
        sha256: sha256_hex(wasm),
        types: ins.types,
        migrations,
        http: Default::default(),
        limits: Default::default(),
    }
}

#[test]
fn invoke_counter_and_account() {
    let wasm = counter_wasm();
    let m = manifest(&wasm);
    let names: Vec<_> = m.types.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names, ["counter", "account"]);
    let rt = Runtime::new().unwrap();
    let app = rt.load(&wasm, m.clone()).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let open = |t: &str| {
        let conn = open_db(&dir.path().join(format!("{t}.db"))).unwrap();
        let applied = apply_migrations(&conn, &m.migrations[t]).unwrap();
        assert_eq!(applied.first().map(String::as_str), Some("0001_init.sql"));
        let id = ActorIdentity { app: "counter".into(), actor_type: t.into(), key: "alice".into(), epoch: 1 };
        app.instantiate(id, database::sqlite_handle(Arc::new(Mutex::new(conn)))).unwrap()
    };
    let mut actor = open("counter");

    assert_eq!(actor.call("counter", "increment", &json!({"by": 2})).unwrap().value, json!(2));
    assert_eq!(actor.call("counter", "increment", &json!([3])).unwrap().value, json!(5));
    assert_eq!(actor.call("counter", "get", &json!(null)).unwrap().value, json!(5));

    let mut acct = open("account");
    // No transaction at this layer (the node owns it), so use an error path that writes nothing.
    let r = acct.call("account", "withdraw", &json!({"amount": 0})).unwrap();
    assert_eq!(r, CallOutput { value: json!({"tag": "invalid-amount"}), is_err: true });
    let r = acct.call("account", "deposit", &json!({"amount": 50, "memo": "pay"})).unwrap();
    assert_eq!(r, CallOutput { value: json!(50), is_err: false });
    let h = acct.call("account", "history", &json!({"limit": 10})).unwrap();
    println!("history: {}", h.value);

    assert!(matches!(actor.call("counter", "nope", &json!(null)), Err(CallError::NotFound(_))));
    assert!(matches!(actor.call("counter", "increment", &json!({"by": "x"})), Err(CallError::BadArgs(_))));
}

#[test]
fn alarm_handler_is_private_and_callable_by_the_host() {
    let wasm = counter_wasm();
    let m = manifest(&wasm);
    let counter = m.actor_type("counter").unwrap();
    assert_eq!(counter.alarm.as_ref().map(|a| a.name.as_str()), Some("alarm"));
    assert!(counter.method("alarm").is_none(), "the handler is not a public method");
    assert!(m.actor_type("account").unwrap().alarm.is_none());

    let rt = Runtime::new().unwrap();
    let app = rt.load(&wasm, m.clone()).unwrap();
    let db = Arc::new(Mutex::new(rusqlite::Connection::open_in_memory().unwrap()));
    apply_migrations(&db.lock().unwrap(), &m.migrations["counter"]).unwrap();
    let id = ActorIdentity { app: "counter".into(), actor_type: "counter".into(), key: "t".into(), epoch: 7 };
    let mut inst = app.instantiate(id, database::sqlite_handle(db.clone())).unwrap();
    assert!(inst.has_alarm_handler("counter"));
    assert!(matches!(inst.call("counter", "alarm", &json!({ "retry-count": 0 })), Err(CallError::NotFound(_))));

    inst.call("counter", "schedule", &json!({ "delay-ms": 1000, "fail-times": 1 })).unwrap();
    let a = alarm::read(&db.lock().unwrap()).unwrap().unwrap();
    assert_eq!((a.epoch, a.seq, a.retry), (7, 1, 0));
    assert_eq!(inst.call("counter", "alarm-at", &json!({})).unwrap().value, json!(a.at_ms));

    // The handler's first attempt fails as planned; the second succeeds.
    assert!(inst.call_alarm("counter", 0).unwrap().is_err);
    assert!(!inst.call_alarm("counter", 1).unwrap().is_err);
    assert_eq!(inst.call("counter", "fired", &json!({})).unwrap().value, json!(1));
    inst.call("counter", "cancel", &json!({})).unwrap();
    assert_eq!(alarm::read(&db.lock().unwrap()).unwrap(), None);
    assert!(matches!(inst.call_alarm("account", 0), Err(CallError::NotFound(_))));
}
