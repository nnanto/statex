//! The queue is a deployed Wasm application; these tests exercise real
//! stateless consumer calls, sender lock release and fleet recovery.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use serde_json::{json, Value as J};
use statex_node::{deploy, start, InvOp, Invocation, Node, NodeConfig};
use statex_runtime::{Manifest, Migration};
use statex_store::DynStore;

fn component() -> &'static (Vec<u8>, Manifest) {
    static CODE: OnceLock<(Vec<u8>, Manifest)> = OnceLock::new();
    CODE.get_or_init(|| {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/queue");
        assert!(std::process::Command::new("cargo")
            .args(["build", "--quiet", "--release", "--target", "wasm32-wasip2"])
            .current_dir(&root).status().unwrap().success());
        let wasm = std::fs::read(root.join("target/wasm32-wasip2/release/queue.wasm")).unwrap();
        let mut migrations = BTreeMap::new();
        for ty in ["queue", "sink"] {
            let mut files: Vec<_> = std::fs::read_dir(root.join("migrations").join(ty)).unwrap()
                .map(|entry| entry.unwrap().path()).collect();
            files.sort();
            migrations.insert(ty.into(), files.into_iter().map(|path| Migration {
                name: path.file_name().unwrap().to_str().unwrap().into(),
                sql: std::fs::read_to_string(path).unwrap(),
            }).collect());
        }
        let mut manifest = Manifest::build(&wasm, "queue", migrations, Default::default(), Default::default()).unwrap();
        manifest.types.iter_mut().find(|ty| ty.name == "queue").unwrap().group_commit = true;
        manifest.types.iter_mut().find(|ty| ty.name == "worker").unwrap().stateless = true;
        (wasm, manifest)
    })
}

fn cfg(root: &std::path::Path, store: DynStore, name: &str) -> NodeConfig {
    let mut cfg = NodeConfig::new(name, store, root.join(name));
    cfg.lease_ttl = Duration::from_secs(2);
    cfg.wake_tick = Duration::from_millis(30);
    cfg.wake_full_scan = Duration::from_millis(100);
    cfg
}

async fn setup() -> (tempfile::TempDir, DynStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = statex_store::open(dir.path().join("bucket").to_str().unwrap()).unwrap();
    let (wasm, manifest) = component();
    deploy::deploy(&store, wasm, manifest, &Default::default()).await.unwrap();
    (dir, store)
}

async fn call(node: &Node, ty: &str, key: &str, method: &str, args: J) -> J {
    let result = node.invoke(Invocation {
        app: "queue".into(), ty: ty.into(), key: key.into(),
        op: InvOp::Call { method: method.into(), args }, chain: vec![],
    }, 0).await;
    assert_eq!(result.status, 200, "{ty}/{key}.{method}: {result:?}");
    result.body["result"].clone()
}

async fn configure(node: &Node, key: &str, lease_ms: u64) {
    call(node, "queue", key, "configure", json!({ "configuration": {
        "max-batch-size": 4, "batch-timeout-ms": 20,
        "max-retries": 5, "retry-delay-ms": 10, "lease-ms": lease_ms,
        "max-concurrency": 2, "consumer-key": "sample", "dlq-key": null,
    }})).await;
}

async fn wait_drained(node: &Node, key: &str, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(12);
    loop {
        let entries = call(node, "sink", key, "entries", json!([])).await;
        let status = call(node, "queue", key, "info", json!([])).await;
        if entries.as_array().unwrap().len() == expected && status["pending"] == 0 && status["in-flight"] == 0 {
            return;
        }
        assert!(Instant::now() < deadline, "queue did not drain: {status}, sink: {entries}");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn queue_consumers_settle_without_holding_the_queue_lock() {
    let (dir, store) = setup().await;
    let a = start(cfg(dir.path(), store.clone(), "a")).await.unwrap();
    let b = start(cfg(dir.path(), store.clone(), "b")).await.unwrap();
    configure(&a.node, "orders", 3000).await;
    let result = call(&b.node, "queue", "orders", "send-batch", json!({ "messages": [
        { "id": "one", "body": "aGVsbG8=", "delay-ms": 0 },
        { "id": "two", "body": "AAH/", "delay-ms": 0 },
        { "id": "three", "body": "", "delay-ms": 30 },
    ] })).await;
    assert_eq!(result, json!([true, true, true]));
    wait_drained(&b.node, "orders", 3).await;
    assert_eq!(call(&b.node, "queue", "orders", "send", json!({
        "id": "one", "body": "aGVsbG8=", "delay-ms": 0,
    })).await, false);
    // A dispatcher crash can replay an already-settled stateless invocation.
    let replay = json!({
        "queue-key": "orders", "token": "obsolete",
        "messages": [{ "id": "one", "body": "aGVsbG8=", "attempt": 1 }],
    });
    call(&b.node, "worker", "sample", "process", replay.clone()).await;
    call(&a.node, "worker", "sample", "process", replay).await;
    wait_drained(&a.node, "orders", 3).await;
    assert_eq!(call(&a.node, "queue", "orders", "info", json!([])).await["receipts"], 3);
    assert!(store.list("actors/queue/worker/").await.unwrap().is_empty());
    b.shutdown().await;
    a.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn leased_batch_and_spawn_survive_owner_crash() {
    let (dir, store) = setup().await;
    let mut config = cfg(dir.path(), store.clone(), "a");
    // Resident alarm timers still lease the batch, but no outbox dispatcher
    // runs before this node is killed.
    config.wake_tick = Duration::from_secs(60);
    let a = start(config).await.unwrap();
    configure(&a.node, "orders", 500).await;
    assert_eq!(call(&a.node, "queue", "orders", "send", json!({
        "id": "survives", "body": "aGVsbG8=", "delay-ms": 0,
    })).await, true);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let info = call(&a.node, "queue", "orders", "info", json!([])).await;
        if info["in-flight"] == 1 {
            assert_eq!(store.list("outbox/").await.unwrap().len(), 1);
            break;
        }
        assert!(Instant::now() < deadline, "batch was not leased: {info}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    a.kill();
    let b = start(cfg(dir.path(), store.clone(), "b")).await.unwrap();
    wait_drained(&b.node, "orders", 1).await;
    assert_eq!(call(&b.node, "queue", "orders", "send", json!({
        "id": "survives", "body": "aGVsbG8=", "delay-ms": 0,
    })).await, false);
    let deadline = Instant::now() + Duration::from_secs(8);
    while !store.list("outbox/").await.unwrap().is_empty() {
        assert!(Instant::now() < deadline, "obsolete queue dispatch stayed in the outbox");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn old_batch_tokens_cannot_settle_a_recreated_queue() {
    let (dir, store) = setup().await;
    let mut config = cfg(dir.path(), store.clone(), "a");
    config.wake_tick = Duration::from_secs(60);
    let a = start(config).await.unwrap();
    let actor = statex_node::ActorId { app: "queue".into(), ty: "queue".into(), key: "orders".into() };
    let mut tokens = Vec::new();
    for body in ["b2xk", "bmV3"] {
        configure(&a.node, "orders", 30_000).await;
        call(&a.node, "queue", "orders", "send", json!({
            "id": "same-id", "body": body, "delay-ms": 0,
        })).await;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let info = call(&a.node, "queue", "orders", "info", json!([])).await;
            if info["in-flight"] == 1 { break; }
            assert!(Instant::now() < deadline, "batch was not leased: {info}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let (owner, _) = statex_node::owner::read(&store, &actor).await.unwrap().unwrap();
        tokens.push(format!("e{}-1", owner.epoch));
        if tokens.len() == 1 {
            let deleted = a.node.invoke(Invocation {
                app: "queue".into(), ty: "queue".into(), key: "orders".into(),
                op: InvOp::Delete, chain: vec![],
            }, 0).await;
            assert_eq!(deleted.status, 200, "{deleted:?}");
        }
    }
    assert_ne!(tokens[0], tokens[1]);
    call(&a.node, "queue", "orders", "settle", json!({
        "token": tokens[0], "outcomes": [{ "id": "same-id", "ack": true }],
    })).await;
    assert_eq!(call(&a.node, "queue", "orders", "info", json!([])).await["in-flight"], 1);
    call(&a.node, "queue", "orders", "settle", json!({
        "token": tokens[1], "outcomes": [{ "id": "same-id", "ack": true }],
    })).await;
    assert_eq!(call(&a.node, "queue", "orders", "info", json!([])).await["in-flight"], 0);
    a.shutdown().await;
}
