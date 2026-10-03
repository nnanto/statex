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
        if !wasm_path.exists() {
            let st = std::process::Command::new("cargo")
                .args(["build", "--release", "--target", "wasm32-wasip2"])
                .current_dir(&root)
                .status()
                .unwrap();
            assert!(st.success());
        }
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
        deploy::deploy(&store, wasm, m, &deploy::DeployOptions::new("team-a")).await.unwrap();
        Fleet { root: dir.path().to_path_buf(), store, _dir: dir }
    }

    fn cfg(&self, name: &str) -> NodeConfig {
        let mut c = NodeConfig::new(name, self.store.clone(), self.root.join(name));
        c.lease_ttl = Duration::from_secs(2);
        c.deploy_poll = Duration::from_millis(200);
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
    let d2 = deploy::deploy(&f.store, wasm, &m2, &deploy::DeployOptions::new("team-a")).await.unwrap();
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
async fn deploy_ownership_and_compatibility() {
    let f = Fleet::new().await; // `counter` is owned by team-a
    let (wasm, m) = counter();
    let as_b = deploy::DeployOptions::new("team-b");
    let mut m2 = m.clone();
    m2.migrations.get_mut("counter").unwrap().push(Migration { name: "0002_x.sql".into(), sql: "SELECT 1;".into() });

    // another team cannot take the name, not even with an identical binary
    let e = deploy::deploy(&f.store, wasm, m, &as_b).await.unwrap_err();
    assert!(e.to_string().contains("owned by \"team-a\""), "{e}");
    let e = deploy::deploy(&f.store, wasm, &m2, &as_b).await.unwrap_err();
    assert!(e.downcast_ref::<deploy::DeployError>().is_some_and(|e| matches!(e, deploy::DeployError::NotOwner { .. })));

    // the owner can deploy compatible changes
    let a = deploy::DeployOptions::new("team-a");
    assert_eq!(deploy::deploy(&f.store, wasm, &m2, &a).await.unwrap().version, 2);

    // ...but not break applied migrations without --allow-breaking
    let mut m3 = m2.clone();
    m3.migrations.get_mut("counter").unwrap()[0].sql.push_str("\nALTER TABLE counter ADD COLUMN y INTEGER;");
    let e = deploy::deploy(&f.store, wasm, &m3, &a).await.unwrap_err();
    assert!(e.to_string().contains("migration `counter/0001"), "{e}");
    let forced = deploy::DeployOptions { allow_breaking: true, ..a.clone() };
    assert_eq!(deploy::deploy(&f.store, wasm, &m3, &forced).await.unwrap().version, 3);

    // an explicit take-over transfers the app
    let take = deploy::DeployOptions { take_over: true, ..as_b.clone() };
    let cur = deploy::deploy(&f.store, wasm, &m3, &take).await.unwrap();
    assert_eq!((cur.owner.as_str(), cur.version), ("team-b", 4));
    assert!(deploy::deploy(&f.store, wasm, &m3, &a).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn namespaced_apps_route_and_store_separately() {
    let f = Fleet::new().await;
    let (wasm, m) = counter();
    let ns = Manifest::build(wasm, "payments/counter", m.migrations.clone(), Default::default(), Default::default()).unwrap();
    deploy::deploy(&f.store, wasm, &ns, &deploy::DeployOptions::new("payments")).await.unwrap();
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
    assert!(apps["apps"].as_array().unwrap().iter().any(|x| x["app"] == "payments/counter" && x["owner"] == "payments"), "{apps}");
    assert!(f.store.get("deploy/payments.counter/current.json").await.unwrap().is_some());
    assert_eq!(post(&a, "/v1/apps/a/b/c/actors/counter/k/get", J::Null).await.0, 404);
    a.shutdown().await;
}
