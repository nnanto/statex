use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use statex_node::{deploy, start, InvOp, Invocation, Node, NodeConfig};
use statex_runtime::{Manifest, Migration};
use statex_store::{DynStore, ObjectStore};

struct UploadGate {
    inner: DynStore,
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
    armed: std::sync::atomic::AtomicBool,
    segments: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl statex_store::ObjectStore for UploadGate {
    async fn get(&self, key: &str) -> statex_store::Result<Option<statex_store::Object>> { self.inner.get(key).await }
    async fn get_range(&self, key: &str, start: u64, len: u64) -> statex_store::Result<Option<bytes::Bytes>> {
        self.inner.get_range(key, start, len).await
    }
    async fn put(&self, key: &str, data: bytes::Bytes) -> statex_store::Result<String> {
        if key.starts_with("actors/async-test/source/barrier/") && key.ends_with(".ltx") {
            self.segments.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        if key.starts_with("actors/async-test/source/barrier/") && key.ends_with(".ltx")
            && self.armed.swap(false, std::sync::atomic::Ordering::SeqCst) {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        self.inner.put(key, data).await
    }
    async fn put_if_absent(&self, key: &str, data: bytes::Bytes) -> statex_store::Result<String> {
        self.inner.put_if_absent(key, data).await
    }
    async fn put_if_match(&self, key: &str, data: bytes::Bytes, etag: &str) -> statex_store::Result<String> {
        self.inner.put_if_match(key, data, etag).await
    }
    async fn put_file(&self, key: &str, path: &std::path::Path) -> statex_store::Result<String> {
        self.inner.put_file(key, path).await
    }
    async fn get_to_file(&self, key: &str, path: &std::path::Path) -> statex_store::Result<bool> {
        self.inner.get_to_file(key, path).await
    }
    async fn list(&self, prefix: &str) -> statex_store::Result<Vec<String>> { self.inner.list(prefix).await }
    async fn delete(&self, key: &str) -> statex_store::Result<()> { self.inner.delete(key).await }
    fn describe(&self) -> String { self.inner.describe() }
}

fn component() -> &'static (Vec<u8>, Manifest) {
    static CODE: OnceLock<(Vec<u8>, Manifest)> = OnceLock::new();
    CODE.get_or_init(|| {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/async");
        let status = std::process::Command::new("cargo")
            .args(["build", "--quiet", "--release", "--target", "wasm32-wasip2"])
            .current_dir(&dir).status().unwrap();
        assert!(status.success());
        let wasm = std::fs::read(dir.join("target/wasm32-wasip2/release/async_fixture.wasm")).unwrap();
        let migration = Migration { name: "0001.sql".into(), sql: "CREATE TABLE tally(id INTEGER PRIMARY KEY,value INTEGER);INSERT INTO tally VALUES(0,0);".into() };
        let mut manifest = Manifest::build(&wasm, "async-test",
            BTreeMap::from([("source".into(), vec![migration.clone()]), ("target".into(), vec![migration])]),
            Default::default(), Default::default()).unwrap();
        manifest.types.iter_mut().find(|t| t.name == "worker").unwrap().stateless = true;
        manifest.types.iter_mut().find(|t| t.name == "source").unwrap().group_commit = true;
        manifest.types.iter_mut().find(|t| t.name == "target").unwrap().group_commit = true;
        (wasm, manifest)
    })
}

async fn setup() -> (tempfile::TempDir, DynStore) {
    let dir = tempfile::tempdir().unwrap();
    let store = statex_store::open(dir.path().join("bucket").to_str().unwrap()).unwrap();
    let (wasm, manifest) = component();
    deploy::deploy(&store, wasm, manifest, &Default::default()).await.unwrap();
    (dir, store)
}

fn cfg(dir: &std::path::Path, store: DynStore, name: &str) -> NodeConfig {
    let mut cfg = NodeConfig::new(name, store, dir.join(name));
    cfg.lease_ttl = Duration::from_secs(1);
    cfg.wake_tick = Duration::from_millis(30);
    cfg.wake_full_scan = Duration::from_millis(100);
    cfg
}

async fn call(node: &Node, ty: &str, key: &str, method: &str, args: Value) -> statex_node::Outcome {
    node.invoke(Invocation {
        app: "async-test".into(), ty: ty.into(), key: key.into(),
        op: InvOp::Call { method: method.into(), args }, chain: vec![],
    }, 0).await
}

async fn wait_value(node: &Node, key: &str, expected: i64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let result = call(node, "target", key, "value", json!([])).await;
        if result.status == 200 && result.body["result"] == expected { return; }
        assert!(Instant::now() < deadline, "target never reached {expected}: {result:?}");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn spawn_rolls_back_and_delivers_after_source_failover() {
    let (dir, store) = setup().await;
    let mut config = cfg(dir.path(), store.clone(), "a");
    // Keep dispatch off until the source owner has crashed.
    config.wake_tick = Duration::from_secs(60);
    let a = start(config).await.unwrap();
    let failed = call(&a.node, "source", "sender", "submit", json!(["receiver", true])).await;
    assert_eq!(failed.status, 422, "{failed:?}");
    assert_eq!(call(&a.node, "source", "sender", "value", json!([])).await.body["result"], 0);
    assert!(store.list("outbox/").await.unwrap().is_empty());
    assert_eq!(call(&a.node, "source", "sender", "submit", json!(["receiver", false])).await.status, 200);
    assert_eq!(store.list("outbox/").await.unwrap().len(), 1);
    a.kill();
    let b = start(cfg(dir.path(), store.clone(), "b")).await.unwrap();
    wait_value(&b.node, "receiver", 1).await;
    assert_eq!(call(&b.node, "source", "sender", "value", json!([])).await.body["result"], 1);
    let deadline = Instant::now() + Duration::from_secs(5);
    while !store.list("outbox/").await.unwrap().is_empty() {
        assert!(Instant::now() < deadline, "settled wake hint was not removed");
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn spawn_cannot_dispatch_before_the_shared_segment_is_durable() {
    let (dir, store) = setup().await;
    let gate = Arc::new(UploadGate {
        inner: store, entered: tokio::sync::Semaphore::new(0), release: tokio::sync::Semaphore::new(0),
        armed: std::sync::atomic::AtomicBool::new(true),
        segments: std::sync::atomic::AtomicUsize::new(0),
    });
    let a = start(cfg(dir.path(), gate.clone(), "a")).await.unwrap();
    let node = a.node.clone();
    let submitted = tokio::spawn(async move { call(&node, "source", "barrier", "submit", json!(["receiver", false])).await });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.acquire()).await.unwrap().unwrap().forget();
    assert!(!gate.list("outbox/").await.unwrap().is_empty(), "wake hint must precede segment durability");
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(call(&a.node, "target", "receiver", "value", json!([])).await.body["result"], 0);
    assert!(!submitted.is_finished(), "scheduling response must wait for durability");
    gate.release.add_permits(1);
    assert_eq!(submitted.await.unwrap().status, 200);
    wait_value(&a.node, "receiver", 1).await;
    a.shutdown().await;
}
#[tokio::test(flavor = "multi_thread")]
async fn stateful_delivery_is_deduplicated_across_nodes_and_restore() {
    let (dir, store) = setup().await;
    let a = start(cfg(dir.path(), store.clone(), "a")).await.unwrap();
    let b = start(cfg(dir.path(), store, "b")).await.unwrap();
    let delivery = Invocation {
        app: "async-test".into(), ty: "target".into(), key: "receiver".into(),
        op: InvOp::Deliver { method: "increment".into(), args: json!([]), delivery_id: "same-task".into() },
        chain: vec![],
    };
    assert_eq!(a.node.invoke(delivery.clone(), 0).await.body["result"], 1);
    assert_eq!(b.node.invoke(delivery.clone(), 0).await.body["result"], 1);
    a.kill();
    assert_eq!(b.node.invoke(delivery, 0).await.body["result"], 1);
    assert_eq!(call(&b.node, "target", "receiver", "value", json!([])).await.body["result"], 1);
    b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unsuccessful_deliveries_do_not_create_receipts() {
    let (dir, store) = setup().await;
    let a = start(cfg(dir.path(), store, "a")).await.unwrap();
    let delivery = Invocation {
        app: "async-test".into(), ty: "source".into(), key: "receiver".into(),
        op: InvOp::Deliver {
            method: "submit".into(), args: json!(["target", true]), delivery_id: "failed-task".into(),
        }, chain: vec![],
    };
    for _ in 0..2 {
        let result = a.node.invoke(delivery.clone(), 0).await;
        assert_eq!(result.status, 422, "{result:?}");
    }
    assert_eq!(call(&a.node, "source", "receiver", "value", json!([])).await.body["result"], 0);
    a.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn grouped_deliveries_share_durable_receipts() {
    let (dir, store) = setup().await;
    let a = start(cfg(dir.path(), store, "a")).await.unwrap();
    let mut tasks = Vec::new();
    for _ in 0..24 {
        let node = a.node.clone();
        tasks.push(tokio::spawn(async move {
            node.invoke(Invocation {
                app: "async-test".into(), ty: "target".into(), key: "receiver".into(),
                op: InvOp::Deliver {
                    method: "increment".into(), args: json!([]), delivery_id: "one-task".into(),
                }, chain: vec![],
            }, 0).await
        }));
    }
    for task in tasks {
        let result = task.await.unwrap();
        assert_eq!(result.status, 200, "{result:?}");
        assert_eq!(result.body["result"], 1);
    }
    assert_eq!(call(&a.node, "target", "receiver", "value", json!([])).await.body["result"], 1);
    a.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_calls_through_a_nonowner_still_group_at_the_owner() {
    let (dir, store) = setup().await;
    let gate = Arc::new(UploadGate {
        inner: store, entered: tokio::sync::Semaphore::new(0), release: tokio::sync::Semaphore::new(0),
        armed: std::sync::atomic::AtomicBool::new(false),
        segments: std::sync::atomic::AtomicUsize::new(0),
    });
    let mut config = cfg(dir.path(), gate.clone(), "a");
    config.wake_tick = Duration::from_secs(60);
    let a = start(config).await.unwrap();
    let mut config = cfg(dir.path(), gate.clone(), "b");
    config.wake_tick = Duration::from_secs(60);
    let b = start(config).await.unwrap();
    assert_eq!(call(&a.node, "source", "barrier", "value", json!([])).await.body["result"], 0);
    gate.segments.store(0, std::sync::atomic::Ordering::SeqCst);
    gate.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    let mut tasks = Vec::new();
    for i in 0..24 {
        let node = b.node.clone();
        tasks.push(tokio::spawn(async move {
            call(&node, "source", "barrier", "submit", json!([format!("receiver-{i}"), false])).await
        }));
    }
    tokio::time::timeout(Duration::from_secs(5), gate.entered.acquire()).await.unwrap().unwrap().forget();
    tokio::time::sleep(Duration::from_millis(300)).await;
    gate.release.add_permits(1);
    for task in tasks {
        let result = task.await.unwrap();
        assert_eq!(result.status, 200, "{result:?}");
    }
    let writes = gate.segments.load(std::sync::atomic::Ordering::SeqCst);
    assert!(writes <= 4, "remote ingress serialized sends: {writes} segments for 24 calls");
    assert_eq!(call(&a.node, "source", "barrier", "value", json!([])).await.body["result"], 24);
    b.shutdown().await;
    a.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stateless_instances_are_fresh_and_cannot_persist() {
    let (dir, store) = setup().await;
    let a = start(cfg(dir.path(), store.clone(), "a")).await.unwrap();
    let mut calls = Vec::new();
    for _ in 0..8 {
        let node: Arc<Node> = a.node.clone();
        calls.push(tokio::spawn(async move { call(&node, "worker", "worker", "fresh", json!([])).await }));
    }
    for call in calls {
        assert_eq!(call.await.unwrap().body["result"], 0);
    }
    for method in ["storage", "spawn-call"] {
        let result = call(&a.node, "worker", "worker", method, json!([])).await;
        assert_eq!(result.status, 422, "{result:?}");
        assert!(result.body["error"]["detail"].as_str().unwrap().contains("stateless"));
    }
    assert!(store.list("actors/async-test/worker/").await.unwrap().is_empty());
    a.shutdown().await;
}
