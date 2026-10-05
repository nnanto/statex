//! Multi-node tests on a shared local object store, using examples/counter.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use serde_json::{json, Value as J};
use statex_node::{deploy, owner, start, ActorId, NodeConfig, NodeHandle};
use statex_runtime::{Manifest, Migration};
use statex_store::DynStore;

fn counter() -> &'static (Vec<u8>, Manifest) {
    static C: OnceLock<(Vec<u8>, Manifest)> = OnceLock::new();
    C.get_or_init(|| {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/counter");
        let wasm_path = root.join("target/wasm32-wasip2/release/counter.wasm");
        // Always build (incremental): the example changes along with the host.
        let st = std::process::Command::new("cargo")
            .args(["build", "--release", "--target", "wasm32-wasip2"])
            .current_dir(&root)
            .status()
            .unwrap();
        assert!(st.success());
        let wasm = std::fs::read(&wasm_path).unwrap();
        let mut migrations = BTreeMap::new();
        for t in ["counter", "account"] {
            let mut v: Vec<Migration> = std::fs::read_dir(root.join("migrations").join(t))
                .unwrap()
                .map(|e| {
                    let p = e.unwrap().path();
                    Migration { name: p.file_name().unwrap().to_string_lossy().into(), sql: std::fs::read_to_string(&p).unwrap() }
                })
                .collect();
            v.sort_by(|a, b| a.name.cmp(&b.name));
            migrations.insert(t.to_string(), v);
        }
        let m = Manifest::build(&wasm, "counter", migrations, Default::default(), Default::default()).unwrap();
        (wasm, m)
    })
}

/// examples/caller: `relay` actors that call the counter app and each other.
fn caller() -> &'static (Vec<u8>, Manifest) {
    static C: OnceLock<(Vec<u8>, Manifest)> = OnceLock::new();
    C.get_or_init(|| {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/caller");
        // Always build (incremental): the example changes along with the call machinery.
        let st = std::process::Command::new("cargo")
            .args(["build", "--release", "--target", "wasm32-wasip2"])
            .current_dir(&root)
            .status()
            .unwrap();
        assert!(st.success());
        let wasm = std::fs::read(root.join("target/wasm32-wasip2/release/caller.wasm")).unwrap();
        let sql = std::fs::read_to_string(root.join("migrations/relay/0001_init.sql")).unwrap();
        let migrations = BTreeMap::from([("relay".to_string(), vec![Migration { name: "0001_init.sql".into(), sql }])]);
        let limits = statex_runtime::Limits { timeout_ms: 2000, ..Default::default() };
        let m = Manifest::build(&wasm, "caller", migrations, Default::default(), limits).unwrap();
        (wasm, m)
    })
}

async fn relay(n: &NodeHandle, key: &str, method: &str, args: J) -> (u16, J) {
    post(n, &format!("/v1/apps/caller/actors/relay/{key}/{method}"), args).await
}

struct Fleet {
    _dir: tempfile::TempDir,
    store: DynStore,
    root: PathBuf,
}

impl Fleet {
    async fn new() -> Fleet {
        let _ = tracing_subscriber::fmt().with_env_filter("statex=debug,info").with_test_writer().try_init();
        let dir = tempfile::tempdir().unwrap();
        let store = statex_store::open(dir.path().join("bucket").to_str().unwrap()).unwrap();
        let (wasm, m) = counter();
        deploy::deploy(&store, wasm, m, &deploy::DeployOptions::default()).await.unwrap();
        Fleet { root: dir.path().to_path_buf(), store, _dir: dir }
    }

    fn cfg(&self, name: &str) -> NodeConfig {
        let mut c = NodeConfig::new(name, self.store.clone(), self.root.join(name));
        c.lease_ttl = Duration::from_secs(2);
        c.deploy_poll = Duration::from_millis(200);
        c.wake_tick = Duration::from_millis(100);
        c.wake_full_scan = Duration::from_secs(1);
        c
    }

    async fn node(&self, name: &str) -> NodeHandle {
        start(self.cfg(name)).await.unwrap()
    }
}

async fn post(n: &NodeHandle, path: &str, body: J) -> (u16, J) {
    let r = reqwest::Client::new().post(format!("{}{path}", n.url())).json(&body).send().await.unwrap();
    (r.status().as_u16(), r.json().await.unwrap())
}

async fn inc(n: &NodeHandle, key: &str, by: i64) -> (u16, J) {
    post(n, &format!("/v1/apps/counter/actors/counter/{key}/increment"), json!({ "by": by })).await
}

async fn get(n: &NodeHandle, key: &str) -> i64 {
    let (s, b) = post(n, &format!("/v1/apps/counter/actors/counter/{key}/get"), J::Null).await;
    assert_eq!(s, 200, "{b}");
    b["result"].as_i64().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn api_basics() {
    let f = Fleet::new().await;
    let a = f.node("a").await;
    for i in 1..=3 {
        assert_eq!(inc(&a, "alice", 1).await, (200, json!({ "result": i })));
    }
    assert_eq!(get(&a, "alice").await, 3);
    assert_eq!(get(&a, "bob").await, 0);

    // snake_case method names and positional args work too
    let (s, b) = post(&a, "/v1/apps/counter/actors/account/alice/deposit", json!([100, "salary"])).await;
    assert_eq!((s, &b), (200, &json!({ "result": 100 })));
    let (s, b) = post(&a, "/v1/apps/counter/actors/account/alice/withdraw", json!({ "amount": 500 })).await;
    assert_eq!(s, 422, "{b}");
    assert_eq!(b["error"]["detail"], json!({ "tag": "insufficient-funds", "value": 100 }));
    let (s, b) = post(&a, "/v1/apps/counter/actors/account/alice/history", json!({ "limit": 5 })).await;
    assert_eq!(s, 200);
    // The failed withdraw wrote an entry before returning err: it was rolled back.
    assert_eq!(b["result"], json!([{ "id": 1, "kind": "deposit", "amount": 100, "memo": "salary" }]));

    assert_eq!(post(&a, "/v1/apps/counter/actors/counter/x/nope", J::Null).await.0, 404);
    assert_eq!(post(&a, "/v1/apps/counter/actors/nope/x/get", J::Null).await.0, 404);
    assert_eq!(post(&a, "/v1/apps/nope/actors/counter/x/get", J::Null).await.0, 404);
    assert_eq!(inc(&a, "x", 0).await.0, 200);
    assert_eq!(post(&a, "/v1/apps/counter/actors/counter/x/increment", json!({ "by": "z" })).await.0, 400);

    // explicit create and delete
    assert_eq!(post(&a, "/v1/apps/counter/actors/counter/carol/_create", J::Null).await.0, 201);
    assert_eq!(post(&a, "/v1/apps/counter/actors/counter/carol/_create", J::Null).await.0, 409);
    assert_eq!(post(&a, "/v1/apps/counter/actors/counter/alice/_create", J::Null).await.0, 409);
    let r = reqwest::Client::new().delete(format!("{}/v1/apps/counter/actors/counter/alice", a.url())).send().await.unwrap();
    assert_eq!(r.status(), 200);
    let r = reqwest::Client::new().delete(format!("{}/v1/apps/counter/actors/counter/zed", a.url())).send().await.unwrap();
    assert_eq!(r.status(), 404);
    assert_eq!(get(&a, "alice").await, 0);

    // keys with slashes and dots
    assert_eq!(inc(&a, "team%2F..%2Fx", 7).await.1["result"], 7);

    let actors: J = reqwest::get(format!("{}/v1/apps/counter/actors?type=counter", a.url())).await.unwrap().json().await.unwrap();
    let keys: Vec<&str> = actors["actors"].as_array().unwrap().iter().map(|c| c["key"].as_str().unwrap()).collect();
    assert!(keys.contains(&"team/../x") && keys.contains(&"bob"), "{keys:?}");
    let schema: J = reqwest::get(format!("{}/v1/apps/counter/schema", a.url())).await.unwrap().json().await.unwrap();
    assert_eq!(schema["types"][0]["name"], "counter");
    a.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn forwarding_and_graceful_handoff() {
    let f = Fleet::new().await;
    let a = f.node("a").await;
    let b = f.node("b").await;
    assert_eq!(inc(&a, "alice", 1).await.1["result"], 1);
    // b forwards to a, the owner
    assert_eq!(inc(&b, "alice", 1).await.1["result"], 2);
    assert_eq!(inc(&b, "alice", 1).await.1["result"], 3);
    let id = ActorId { app: "counter".into(), ty: "counter".into(), key: "alice".into() };
    assert_eq!(a.node.resident_actors(), vec![id.clone()]);
    assert!(b.node.resident_actors().is_empty());

    a.shutdown().await;
    // released on shutdown: b takes over immediately with a higher epoch
    assert_eq!(inc(&b, "alice", 1).await.1["result"], 4);
    let (rec, _) = owner::read(&f.store, &id).await.unwrap().unwrap();
    assert_eq!((rec.node.as_str(), rec.epoch), ("b", 2));
    b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn crash_takeover_keeps_acked_writes() {
    let f = Fleet::new().await;
    let a = f.node("a").await;
    let b = f.node("b").await;
    for _ in 0..5 {
        assert_eq!(inc(&a, "alice", 1).await.0, 200);
    }
    a.kill();
    let t = std::time::Instant::now();
    // b first sees a live (but dead) owner, retries until a's lease expires
    assert_eq!(inc(&b, "alice", 1).await, (200, json!({ "result": 6 })));
    assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
    b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_owner_is_fenced() {
    let f = Fleet::new().await;
    let a = f.node("a").await;
    let b = f.node("b").await;
    assert_eq!(inc(&a, "alice", 1).await.0, 200);
    // partition a from the store's point of view
    a.node.lease.pause_renewal(true);
    tokio::time::sleep(Duration::from_millis(2300)).await;
    assert!(a.node.lease.fenced());
    assert_eq!(inc(&a, "alice", 1).await.0, 503);
    assert_eq!(inc(&b, "alice", 1).await.1["result"], 2);
    b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ack_requires_current_ownership() {
    let f = Fleet::new().await;
    let a = f.node("a").await;
    assert_eq!(inc(&a, "alice", 1).await.0, 200);
    // someone else bumps the epoch behind a's back
    let id = ActorId { app: "counter".into(), ty: "counter".into(), key: "alice".into() };
    let (mut rec, etag) = owner::read(&f.store, &id).await.unwrap().unwrap();
    rec.epoch += 1;
    rec.node = "ghost".into();
    f.store.put_if_match(&id.owner_key(), statex_store::to_json_bytes(&rec), &etag).await.unwrap();
    let (s, b) = inc(&a, "alice", 1).await;
    assert_eq!(s, 503, "{b}");
    assert!(a.node.resident_actors().is_empty());
    a.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn compaction_and_restore() {
    let f = Fleet::new().await;
    let mut cfg = f.cfg("a");
    cfg.snapshot_every = 4;
    let a = start(cfg).await.unwrap();
    for _ in 0..10 {
        assert_eq!(inc(&a, "alice", 1).await.0, 200);
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    let id = ActorId { app: "counter".into(), ty: "counter".into(), key: "alice".into() };
    let objs = f.store.list(&id.ltx_prefix()).await.unwrap();
    let snaps = objs.iter().filter(|k| k.contains("snapshot-")).count();
    let segs = objs.iter().filter(|k| k.ends_with(".ltx")).count();
    assert_eq!(snaps, 1, "{objs:?}");
    assert!(segs < 4, "{objs:?}");
    a.shutdown().await;
    let b = f.node("b").await;
    assert_eq!(get(&b, "alice").await, 10);
    b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_eviction_releases_actors() {
    let f = Fleet::new().await;
    let mut cfg = f.cfg("a");
    cfg.idle_timeout = Duration::from_millis(300);
    let a = start(cfg).await.unwrap();
    assert_eq!(inc(&a, "alice", 2).await.0, 200);
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert!(a.node.resident_actors().is_empty());
    let id = ActorId { app: "counter".into(), ty: "counter".into(), key: "alice".into() };
    let (rec, _) = owner::read(&f.store, &id).await.unwrap().unwrap();
    assert_eq!(rec.state, owner::OwnerState::Unowned);
    assert_eq!(get(&a, "alice").await, 2);
    a.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn hot_redeploy_applies_to_resident_actors() {
    let f = Fleet::new().await;
    let a = f.node("a").await;
    assert_eq!(inc(&a, "alice", 1).await.0, 200);
    let (wasm, m) = counter();
    let mut m2 = m.clone();
    m2.migrations.get_mut("counter").unwrap().push(Migration {
        name: "0002_extra.sql".into(),
        sql: "CREATE TABLE extra(x INTEGER);".into(),
    });
    // same binary, new manifest: a new deployment id
    let d1 = a.node.deployment("counter").unwrap();
    let d2 = deploy::deploy(&f.store, wasm, &m2, &deploy::DeployOptions::default()).await.unwrap();
    assert_ne!(d1.id, d2.id);
    assert_eq!(d2.version, d1.version + 1);
    a.node.refresh_apps().await.unwrap();
    // the resident actor switches code and applies 0002 before running the call
    assert_eq!(inc(&a, "alice", 1).await.1["result"], 2);
    a.shutdown().await;
    let b = f.node("b").await;
    assert_eq!(get(&b, "alice").await, 2);
    b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn deploy_compatibility() {
    let f = Fleet::new().await;
    let (wasm, m) = counter();
    let opts = deploy::DeployOptions::default();
    let mut m2 = m.clone();
    m2.migrations.get_mut("counter").unwrap().push(Migration { name: "0002_x.sql".into(), sql: "SELECT 1;".into() });

    // re-deploying identical bits is a no-op
    assert_eq!(deploy::deploy(&f.store, wasm, m, &opts).await.unwrap().version, 1);

    // compatible changes deploy
    assert_eq!(deploy::deploy(&f.store, wasm, &m2, &opts).await.unwrap().version, 2);

    // ...but not breaking applied migrations without --allow-breaking
    let mut m3 = m2.clone();
    m3.migrations.get_mut("counter").unwrap()[0].sql.push_str("\nALTER TABLE counter ADD COLUMN y INTEGER;");
    let e = deploy::deploy(&f.store, wasm, &m3, &opts).await.unwrap_err();
    assert!(e.to_string().contains("migration `counter/0001"), "{e}");
    let forced = deploy::DeployOptions { allow_breaking: true, ..opts.clone() };
    assert_eq!(deploy::deploy(&f.store, wasm, &m3, &forced).await.unwrap().version, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn namespaced_apps_route_and_store_separately() {
    let f = Fleet::new().await;
    let (wasm, m) = counter();
    let ns = Manifest::build(wasm, "payments/counter", m.migrations.clone(), Default::default(), Default::default()).unwrap();
    deploy::deploy(&f.store, wasm, &ns, &deploy::DeployOptions::default()).await.unwrap();
    let a = f.node("a").await;
    let call = |app: &'static str, key: &'static str| {
        let a = &a;
        async move { post(a, &format!("/v1/apps/{app}/actors/counter/{key}/increment"), json!({ "by": 1 })).await }
    };
    assert_eq!(call("payments/counter", "alice").await, (200, json!({ "result": 1 })));
    assert_eq!(call("payments/counter", "alice").await.1["result"], 2);
    // same type and key in a different app is a different actor
    assert_eq!(call("counter", "alice").await.1["result"], 1);
    // keys may contain slashes and reserved words
    assert_eq!(call("payments/counter", "a%2Factors").await.1["result"], 1);
    let r: J = reqwest::get(format!("{}/v1/apps/payments/counter/actors", a.url())).await.unwrap().json().await.unwrap();
    let mut keys: Vec<_> = r["actors"].as_array().unwrap().iter().map(|c| c["key"].as_str().unwrap().to_string()).collect();
    keys.sort();
    assert_eq!(keys, ["a/actors", "alice"]);
    let r = reqwest::get(format!("{}/v1/apps/payments/counter/schema", a.url())).await.unwrap();
    assert_eq!(r.status(), 200);
    let apps: J = reqwest::get(format!("{}/v1/apps", a.url())).await.unwrap().json().await.unwrap();
    assert!(apps["apps"].as_array().unwrap().iter().any(|x| x["app"] == "payments/counter"), "{apps}");
    assert!(f.store.get("deploy/payments.counter/current.json").await.unwrap().is_some());
    assert_eq!(post(&a, "/v1/apps/a/b/c/actors/counter/k/get", J::Null).await.0, 404);
    a.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_calls_across_apps_and_nodes() {
    let f = Fleet::new().await;
    let (wasm, m) = caller();
    assert_eq!(m.calls.len(), 3, "{:?}", m.calls);
    deploy::deploy(&f.store, wasm, m, &deploy::DeployOptions::default()).await.unwrap();
    let a = f.node("a").await;
    let b = f.node("b").await;
    // b owns counter c1; relay r1 on a calls it through a forward.
    assert_eq!(inc(&b, "c1", 1).await.1["result"], 1);
    assert_eq!(relay(&a, "r1", "bump", json!({ "key": "c1", "by": 2 })).await, (200, json!({ "result": 3 })));
    assert_eq!(get(&b, "c1").await, 3);

    // records, options and the callee's own result<_, tx-error> round-trip
    assert_eq!(relay(&a, "r1", "deposit", json!(["acct", 5, "hi"])).await, (200, json!({ "result": 5 })));
    let (s, body) = relay(&a, "r1", "deposit", json!(["acct", 0, null])).await;
    assert_eq!((s, &body["error"]["detail"]), (422, &json!("refused: invalid amount")), "{body}");
    assert_eq!(relay(&b, "r1", "memos", json!(["acct", 10])).await, (200, json!({ "result": ["hi"] })));
    // The refused deposit returned err, so the relay's own write rolled back.
    assert_eq!(relay(&a, "r1", "calls", J::Null).await.1["result"], 3);
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn call_cycles_are_rejected_not_deadlocked() {
    let f = Fleet::new().await;
    let (wasm, m) = caller();
    deploy::deploy(&f.store, wasm, m, &deploy::DeployOptions::default()).await.unwrap();
    let a = f.node("a").await;
    let b = f.node("b").await;
    assert_eq!(relay(&b, "r2", "calls", J::Null).await.0, 200);
    assert_eq!(
        relay(&a, "r1", "ping", json!([["r2", "r3"]])).await,
        (200, json!({ "result": ["r1", "r2", "r3"] }))
    );
    for path in [json!(["r1"]), json!(["r2", "r1"]), json!(["r2", "r3", "r2"])] {
        let t = std::time::Instant::now();
        let (s, body) = relay(&a, "r1", "ping", json!([path])).await;
        assert_eq!(s, 422, "{body}");
        assert!(body["error"]["detail"].as_str().unwrap().starts_with("call cycle:"), "{body}");
        assert!(t.elapsed() < Duration::from_secs(1), "cycle should fail fast, took {:?}", t.elapsed());
    }
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn calls_time_out_within_the_callers_deadline() {
    let f = Fleet::new().await;
    let (wasm, m) = caller();
    deploy::deploy(&f.store, wasm, m, &deploy::DeployOptions::default()).await.unwrap();
    let a = f.node("a").await;
    assert_eq!(relay(&a, "s1", "spin-on", json!(["s2", 10])).await, (200, json!({ "result": null })));
    let t = std::time::Instant::now();
    let (s, body) = relay(&a, "s1", "spin-on", json!(["s2", 10_000])).await;
    assert_eq!((s, &body["error"]["detail"]), (422, &json!("timed out")), "{body}");
    // timeout_ms is 2000; the caller gets its error before its own deadline.
    assert!(t.elapsed() < Duration::from_millis(2000), "{:?}", t.elapsed());
    a.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn deploys_check_calls_both_ways() {
    let dir = tempfile::tempdir().unwrap();
    let store = statex_store::open(dir.path().join("bucket").to_str().unwrap()).unwrap();
    let (wasm, m) = caller();
    let opts = deploy::DeployOptions::default();
    let e = deploy::deploy(&store, wasm, m, &opts).await.unwrap_err().to_string();
    assert!(e.contains("app counter is not deployed"), "{e}");
    let lenient = deploy::DeployOptions { allow_unresolved_calls: true, ..opts.clone() };
    deploy::deploy(&store, wasm, m, &lenient).await.unwrap();

    // Calls to an app that is not deployed fail with not-found.
    let mut cfg = NodeConfig::new("a", store.clone(), dir.path().join("a"));
    cfg.lease_ttl = Duration::from_secs(2);
    cfg.deploy_poll = Duration::from_millis(200);
    let a = start(cfg).await.unwrap();
    let (s, body) = relay(&a, "r", "bump", json!(["c", 1])).await;
    assert_eq!(s, 422, "{body}");
    assert!(body["error"]["detail"].as_str().unwrap().starts_with("not found:"), "{body}");

    let (cw, cm) = counter();
    deploy::deploy(&store, cw, cm, &opts).await.unwrap();
    // Re-checking the caller now passes.
    assert!(deploy::unresolved_calls(&store, m).await.unwrap().is_empty());

    // A counter version that changes a method the caller uses is refused,
    // naming the deployed caller.
    let mut cm2 = cm.clone();
    let acct = cm2.types.iter_mut().find(|t| t.name == "account").unwrap();
    acct.methods.iter_mut().find(|x| x.name == "history").unwrap().params[0].ty = statex_runtime::Ty::U64;
    let e = deploy::deploy(&store, cw, &cm2, &opts).await.unwrap_err().to_string();
    assert!(e.contains("deployed caller caller"), "{e}");
    a.shutdown().await;
}

async fn counter_call(n: &NodeHandle, key: &str, method: &str, args: J) -> J {
    let (s, b) = post(n, &format!("/v1/apps/counter/actors/counter/{key}/{method}"), args).await;
    assert_eq!(s, 200, "{method}: {b}");
    b["result"].clone()
}

async fn wake_hints(f: &Fleet) -> Vec<String> {
    f.store.list("wake/").await.unwrap()
}

/// Polls `cond` every 100 ms until it holds or `within` passes.
async fn eventually<Fut: std::future::Future<Output = bool>>(within: Duration, mut cond: impl FnMut() -> Fut) -> bool {
    let t = std::time::Instant::now();
    while t.elapsed() < within {
        if cond().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread")]
async fn alarms_fire_on_resident_actors() {
    let f = Fleet::new().await;
    let a = f.node("a").await;
    counter_call(&a, "t", "schedule", json!({ "delay-ms": 300, "fail-times": 0 })).await;
    assert!(counter_call(&a, "t", "alarm-at", J::Null).await.is_u64());
    assert_eq!(wake_hints(&f).await.len(), 1);
    // The handler is not a public method.
    assert_eq!(post(&a, "/v1/apps/counter/actors/counter/t/alarm", json!({ "retry-count": 0 })).await.0, 404);
    let t = std::time::Instant::now();
    assert!(eventually(Duration::from_secs(3), || async { counter_call(&a, "t", "fired", J::Null).await == json!(1) }).await);
    assert!(t.elapsed() >= Duration::from_millis(200), "{:?}", t.elapsed());
    assert_eq!(counter_call(&a, "t", "alarm-at", J::Null).await, J::Null);
    assert!(eventually(Duration::from_secs(2), || async { wake_hints(&f).await.is_empty() }).await);

    // Rescheduling replaces the alarm; cancelling prevents it from firing.
    counter_call(&a, "t", "schedule", json!({ "delay-ms": 60_000, "fail-times": 0 })).await;
    counter_call(&a, "t", "schedule", json!({ "delay-ms": 400, "fail-times": 0 })).await;
    counter_call(&a, "t", "cancel", J::Null).await;
    assert!(eventually(Duration::from_secs(2), || async { wake_hints(&f).await.is_empty() }).await);
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(counter_call(&a, "t", "fired", J::Null).await, json!(1));

    // Deleting the actor drops its alarm.
    counter_call(&a, "d", "schedule", json!({ "delay-ms": 60_000, "fail-times": 0 })).await;
    assert_eq!(wake_hints(&f).await.len(), 1);
    let r = reqwest::Client::new().delete(format!("{}/v1/apps/counter/actors/counter/d", a.url())).send().await.unwrap();
    assert_eq!(r.status().as_u16(), 200);
    assert!(wake_hints(&f).await.is_empty());
    a.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_alarm_handlers_are_retried() {
    let f = Fleet::new().await;
    let a = f.node("a").await;
    let t = std::time::Instant::now();
    counter_call(&a, "r", "schedule", json!({ "delay-ms": 0, "fail-times": 1 })).await;
    // The first attempt fails and schedules a retry 2 s later.
    assert!(eventually(Duration::from_secs(1), || async {
        counter_call(&a, "r", "alarm-at", J::Null).await.as_u64().is_some_and(|at| at > statex_node::layout::now_ms() + 1000)
    })
    .await);
    assert_eq!(counter_call(&a, "r", "fired", J::Null).await, json!(0));
    assert!(eventually(Duration::from_secs(5), || async { counter_call(&a, "r", "fired", J::Null).await == json!(1) }).await);
    assert!(t.elapsed() >= Duration::from_millis(1900), "{:?}", t.elapsed());
    assert_eq!(counter_call(&a, "r", "alarm-at", J::Null).await, J::Null);
    a.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn alarms_wake_evicted_actors() {
    let f = Fleet::new().await;
    let mut cfg = f.cfg("a");
    cfg.idle_timeout = Duration::from_millis(300);
    let a = start(cfg).await.unwrap();
    counter_call(&a, "e", "schedule", json!({ "delay-ms": 1500, "fail-times": 0 })).await;
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert!(a.node.resident_actors().is_empty(), "evicted before the alarm is due");
    // The waker activates the actor; the fired alarm's hint is then deleted.
    assert!(eventually(Duration::from_secs(4), || async { wake_hints(&f).await.is_empty() }).await);
    assert_eq!(counter_call(&a, "e", "fired", J::Null).await, json!(1));
    a.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn alarms_survive_owner_crash() {
    let f = Fleet::new().await;
    let a = f.node("a").await;
    let b = f.node("b").await;
    counter_call(&a, "c", "schedule", json!({ "delay-ms": 500, "fail-times": 0 })).await;
    a.kill();
    // b's waker takes over once a's leases expire and activates the actor.
    assert!(eventually(Duration::from_secs(8), || async { wake_hints(&f).await.is_empty() }).await);
    assert_eq!(counter_call(&b, "c", "fired", J::Null).await, json!(1));
    assert!(b.node.resident_actors().iter().any(|id| id.key == "c"));
    b.shutdown().await;
}
