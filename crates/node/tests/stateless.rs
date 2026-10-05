//! Stateless calls never acquire ownership or retain instance/database state.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use serde_json::{json, Value};
use statex_node::{deploy, owner, start, ActorId, InvOp, Invocation, NodeConfig};
use statex_runtime::Manifest;

#[tokio::test(flavor = "multi_thread")]
async fn stateless_calls_are_fresh_local_and_non_durable() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/stateless");
    assert!(std::process::Command::new("cargo")
        .args(["build", "--release", "--target", "wasm32-wasip2"])
        .current_dir(&root).status().unwrap().success());
    let wasm = std::fs::read(root.join("target/wasm32-wasip2/release/stateless.wasm")).unwrap();
    let mut manifest = Manifest::build(
        &wasm, "stateless", BTreeMap::new(), Default::default(), Default::default(),
    ).unwrap();
    manifest.types[0].stateless = true;

    let dir = tempfile::tempdir_in(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target")).unwrap();
    let store = statex_store::open(dir.path().join("bucket").to_str().unwrap()).unwrap();
    let mut invalid = manifest.clone();
    invalid.migrations.insert("worker".into(), vec![statex_runtime::Migration {
        name: "init.sql".into(), sql: "SELECT 1".into(),
    }]);
    assert!(deploy::deploy(&store, &wasm, &invalid, &Default::default()).await.unwrap_err()
        .to_string().contains("cannot have migrations"));
    assert!(store.list("").await.unwrap().is_empty());
    deploy::deploy(&store, &wasm, &manifest, &Default::default()).await.unwrap();
    let mut cfg = NodeConfig::new("a", store.clone(), dir.path().join("a"));
    cfg.wake_tick = Duration::from_secs(60);
    let a = start(cfg).await.unwrap();
    let mut cfg = NodeConfig::new("b", store.clone(), dir.path().join("b"));
    cfg.wake_tick = Duration::from_secs(60);
    let b = start(cfg).await.unwrap();
    let call = |method: &str| Invocation {
        app: "stateless".into(), ty: "worker".into(), key: "same".into(),
        op: InvOp::Call { method: method.into(), args: Value::Null }, chain: vec![],
    };
    let before = store.list("").await.unwrap();
    let mut recursive = call("increment");
    recursive.chain.push(statex_runtime::ActorRef {
        app: "stateless".into(), actor_type: "worker".into(), key: "same".into(),
    });
    assert_eq!(a.node.invoke(recursive, 0).await.status, 508);
    let mut deep = call("increment");
    deep.chain = (0..statex_node::node::MAX_CALL_DEPTH).map(|i| statex_runtime::ActorRef {
        app: "stateless".into(), actor_type: "worker".into(), key: format!("ancestor-{i}"),
    }).collect();
    assert_eq!(a.node.invoke(deep, 0).await.status, 508);
    for n in [&a, &b, &a] {
        let out = n.node.invoke(call("increment"), 0).await;
        assert_eq!((out.status, out.body), (200, json!({ "result": 1 })));
        assert_eq!(n.node.invoke(call("epoch"), 0).await.body, json!({ "result": 0 }));
        assert!(n.node.resident_actors().is_empty());
    }
    for method in ["sql", "schedule", "spawn"] {
        let out = a.node.invoke(call(method), 0).await;
        assert_eq!(out.status, 422, "{}", out.body);
        assert!(out.body["error"]["detail"].as_str().unwrap().contains("stateless"), "{}", out.body);
    }
    assert_eq!(a.node.invoke(call("fail"), 0).await.status, 500);
    assert_eq!(a.node.invoke(call("increment"), 0).await.body, json!({ "result": 1 }));
    for _ in 0..2 {
        let mut inv = call("increment");
        inv.op = InvOp::Deliver { method: "increment".into(), args: Value::Null, delivery_id: "same-delivery".into() };
        assert_eq!(a.node.invoke(inv, 0).await.body, json!({ "result": 1 }));
    }
    for op in [InvOp::Create, InvOp::Delete, InvOp::Alarm] {
        let mut inv = call("increment");
        inv.op = op;
        assert_eq!(a.node.invoke(inv, 0).await.status, 400);
    }
    let id = ActorId { app: "stateless".into(), ty: "worker".into(), key: "same".into() };
    assert!(owner::read(&store, &id).await.unwrap().is_none());
    assert_eq!(store.list("").await.unwrap(), before, "calls must not write store objects");
    assert!(!dir.path().join("a").join(id.local_dir()).exists(), "calls must not create actor databases");
    a.shutdown().await;
    b.shutdown().await;
}
