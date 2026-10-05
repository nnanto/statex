use super::*;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::OnceLock;

use async_trait::async_trait;
use statex_runtime::{Manifest, Migration};
use statex_store::{ETag, Object, ObjectStore};
use tokio::sync::Semaphore;

struct GatedStore {
    inner: DynStore,
    gate: AtomicBool,
    fail: AtomicBool,
    uploads: AtomicUsize,
    entered: Semaphore,
    proceed: Semaphore,
}

#[async_trait]
impl ObjectStore for GatedStore {
    async fn get(&self, key: &str) -> statex_store::Result<Option<Object>> {
        self.inner.get(key).await
    }
    async fn get_range(
        &self,
        key: &str,
        start: u64,
        len: u64,
    ) -> statex_store::Result<Option<Bytes>> {
        self.inner.get_range(key, start, len).await
    }
    async fn put(&self, key: &str, data: Bytes) -> statex_store::Result<ETag> {
        if key.ends_with(".ltx") {
            self.uploads.fetch_add(1, Ordering::SeqCst);
            if self.gate.load(Ordering::SeqCst) {
                self.entered.add_permits(1);
                self.proceed.acquire().await.unwrap().forget();
                if self.fail.load(Ordering::SeqCst) {
                    return Err(StoreError::Other(anyhow::anyhow!(
                        "injected upload failure"
                    )));
                }
            }
        }
        self.inner.put(key, data).await
    }
    async fn put_if_absent(&self, key: &str, data: Bytes) -> statex_store::Result<ETag> {
        self.inner.put_if_absent(key, data).await
    }
    async fn put_if_match(&self, key: &str, data: Bytes, etag: &str) -> statex_store::Result<ETag> {
        self.inner.put_if_match(key, data, etag).await
    }
    async fn put_file(&self, key: &str, path: &Path) -> statex_store::Result<ETag> {
        self.inner.put_file(key, path).await
    }
    async fn get_to_file(&self, key: &str, path: &Path) -> statex_store::Result<bool> {
        self.inner.get_to_file(key, path).await
    }
    async fn list(&self, prefix: &str) -> statex_store::Result<Vec<String>> {
        self.inner.list(prefix).await
    }
    async fn delete(&self, key: &str) -> statex_store::Result<()> {
        self.inner.delete(key).await
    }
    fn describe(&self) -> String {
        self.inner.describe()
    }
}

fn fixture() -> &'static (Vec<u8>, Manifest) {
    static FIXTURE: OnceLock<(Vec<u8>, Manifest)> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/counter");
        assert!(std::process::Command::new("cargo")
            .args(["build", "--quiet", "--release", "--target", "wasm32-wasip2"])
            .current_dir(&root)
            .status()
            .unwrap()
            .success());
        let wasm = std::fs::read(root.join("target/wasm32-wasip2/release/counter.wasm")).unwrap();
        let mut migrations = BTreeMap::new();
        for ty in ["counter", "account"] {
            let mut files: Vec<_> = std::fs::read_dir(root.join("migrations").join(ty))
                .unwrap()
                .map(|entry| {
                    let path = entry.unwrap().path();
                    Migration {
                        name: path.file_name().unwrap().to_string_lossy().into(),
                        sql: std::fs::read_to_string(&path).unwrap(),
                    }
                })
                .collect();
            files.sort_by(|a, b| a.name.cmp(&b.name));
            if ty == "account" {
                files.push(Migration {
                    name: "9999_test_trap.sql".into(),
                    sql:
                        "CREATE TRIGGER test_trap BEFORE INSERT ON entries WHEN NEW.memo = 'trap' \
                          BEGIN SELECT RAISE(ABORT, 'planned trap'); END;"
                            .into(),
                });
            }
            migrations.insert(ty.to_owned(), files);
        }
        let mut manifest = Manifest::build(
            &wasm,
            "group-test",
            migrations,
            Default::default(),
            Default::default(),
        )
        .unwrap();
        for ty in &mut manifest.types {
            ty.group_commit = true;
        }
        (wasm, manifest)
    })
}

async fn setup() -> (tempfile::TempDir, Arc<GatedStore>, crate::NodeHandle) {
    let dir = tempfile::Builder::new()
        .prefix("statex-group-")
        .tempdir_in(".")
        .unwrap();
    let store = Arc::new(GatedStore {
        inner: statex_store::open(dir.path().join("store").to_str().unwrap()).unwrap(),
        gate: AtomicBool::new(false),
        fail: AtomicBool::new(false),
        uploads: AtomicUsize::new(0),
        entered: Semaphore::new(0),
        proceed: Semaphore::new(0),
    });
    let (wasm, manifest) = fixture();
    let dyn_store: DynStore = store.clone();
    deploy::deploy(
        &dyn_store,
        wasm,
        manifest,
        &deploy::DeployOptions::default(),
    )
    .await
    .unwrap();
    let mut cfg = NodeConfig::new("group-test-node", dyn_store, dir.path().join("node"));
    cfg.snapshot_every = 1000;
    let handle = crate::start(cfg).await.unwrap();
    (dir, store, handle)
}

fn invocation(ty: &str, method: &str, args: J) -> Invocation {
    Invocation {
        app: "group-test".into(),
        ty: ty.into(),
        key: "one".into(),
        op: InvOp::Call {
            method: method.into(),
            args,
        },
        chain: vec![],
    }
}

fn call(node: &Arc<Node>, inv: Invocation) -> tokio::task::JoinHandle<Outcome> {
    let node = node.clone();
    tokio::spawn(async move { node.invoke(inv, 0).await })
}

async fn wait_upload(store: &GatedStore) {
    tokio::time::timeout(Duration::from_secs(10), store.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}

async fn wait_pending(node: &Node, ty: &str, count: usize) {
    let id = ActorId {
        app: "group-test".into(),
        ty: ty.into(),
        key: "one".into(),
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let pending = node
                .groups
                .lock()
                .unwrap()
                .get(&id)
                .map(|q| q.pending.lock().unwrap().len());
            if pending == Some(count) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn group_commit_batches_rolls_back_and_orders_reads() {
    let (_dir, store, handle) = setup().await;
    let node = &handle.node;
    store.gate.store(true, Ordering::SeqCst);
    let first = call(
        node,
        invocation("account", "deposit", json!([100, "first"])),
    );
    wait_upload(&store).await;
    let deposit = call(
        node,
        invocation("account", "deposit", json!([20, "second"])),
    );
    wait_pending(node, "account", 1).await;
    let error = call(node, invocation("account", "withdraw", json!([999])));
    wait_pending(node, "account", 2).await;
    let bad_args = call(
        node,
        invocation("account", "deposit", json!(["wrong", null])),
    );
    wait_pending(node, "account", 3).await;
    let trap = call(node, invocation("account", "deposit", json!([50, "trap"])));
    wait_pending(node, "account", 4).await;
    let after_trap = call(node, invocation("account", "deposit", json!([30, "third"])));
    wait_pending(node, "account", 5).await;
    let read = call(node, invocation("account", "balance", J::Null));
    wait_pending(node, "account", 6).await;
    assert!(!first.is_finished() && !read.is_finished() && !error.is_finished());
    store.proceed.add_permits(1);
    assert_eq!(first.await.unwrap().body["result"], 100);
    wait_upload(&store).await;
    assert!(
        !deposit.is_finished()
            && !read.is_finished()
            && !error.is_finished()
            && !bad_args.is_finished()
    );
    store.proceed.add_permits(1);
    assert_eq!(deposit.await.unwrap().body["result"], 120);
    assert_eq!(error.await.unwrap().status, 422);
    assert_eq!(bad_args.await.unwrap().status, 400);
    assert_eq!(trap.await.unwrap().body["error"]["code"], "trap");
    assert_eq!(after_trap.await.unwrap().body["result"], 150);
    assert_eq!(read.await.unwrap().body["result"], 150);
    assert_eq!(
        store.uploads.load(Ordering::SeqCst),
        2,
        "seven calls should produce two segment uploads"
    );
    store.gate.store(false, Ordering::SeqCst);
    let history = node
        .invoke(invocation("account", "history", json!([10])), 0)
        .await;
    assert_eq!(
        history.body["result"].as_array().unwrap().len(),
        3,
        "failed withdraw and trap must not leave rows"
    );
    handle.shutdown().await;
    let mut cfg = NodeConfig::new("restored", store.clone(), _dir.path().join("restored"));
    cfg.snapshot_every = 1000;
    let restored = crate::start(cfg).await.unwrap();
    assert_eq!(
        restored
            .node
            .invoke(invocation("account", "balance", J::Null), 0)
            .await
            .body["result"],
        150
    );
    restored.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn group_commit_withholds_reads_and_errors_on_upload_failure() {
    let (_dir, store, handle) = setup().await;
    let node = &handle.node;
    store.gate.store(true, Ordering::SeqCst);
    let first = call(node, invocation("account", "deposit", json!([10, null])));
    wait_upload(&store).await;
    let write = call(node, invocation("account", "deposit", json!([20, null])));
    wait_pending(node, "account", 1).await;
    let read = call(node, invocation("account", "balance", J::Null));
    wait_pending(node, "account", 2).await;
    let error = call(node, invocation("account", "withdraw", json!([999])));
    wait_pending(node, "account", 3).await;
    store.proceed.add_permits(1);
    assert_eq!(first.await.unwrap().status, 200);
    wait_upload(&store).await;
    store.fail.store(true, Ordering::SeqCst);
    store.proceed.add_permits(1);
    for outcome in [
        write.await.unwrap(),
        read.await.unwrap(),
        error.await.unwrap(),
    ] {
        assert_eq!(outcome.status, 503);
    }
    store.gate.store(false, Ordering::SeqCst);
    assert_eq!(
        node.invoke(invocation("account", "balance", J::Null), 0)
            .await
            .body["result"],
        10
    );
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn group_commit_bounds_pending_admission() {
    let (_dir, store, handle) = setup().await;
    let node = &handle.node;
    store.gate.store(true, Ordering::SeqCst);
    let first = call(node, invocation("counter", "increment", json!([1])));
    wait_upload(&store).await;
    let mut pending = Vec::new();
    for _ in 0..GROUP_PENDING_LIMIT {
        pending.push(call(node, invocation("counter", "increment", json!([1]))));
    }
    wait_pending(node, "counter", GROUP_PENDING_LIMIT).await;
    assert_eq!(
        node.invoke(invocation("counter", "get", J::Null), 0)
            .await
            .status,
        503
    );
    store.gate.store(false, Ordering::SeqCst);
    store.proceed.add_permits(1);
    assert_eq!(first.await.unwrap().status, 200);
    for task in pending {
        assert_eq!(task.await.unwrap().status, 200);
    }
    assert_eq!(store.uploads.load(Ordering::SeqCst), 3);
    assert_eq!(
        node.invoke(invocation("counter", "get", J::Null), 0)
            .await
            .body["result"],
        129
    );
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn group_commit_concurrent_remote_deliveries_batch_at_owner() {
    let (dir, store, owner) = setup().await;
    assert_eq!(
        owner
            .node
            .invoke(invocation("counter", "get", J::Null), 0)
            .await
            .body["result"],
        0
    );
    let mut cfg = NodeConfig::new(
        "group-test-ingress",
        store.clone(),
        dir.path().join("ingress"),
    );
    cfg.snapshot_every = 1000;
    let ingress = crate::start(cfg).await.unwrap();
    let delivery = |id: String| {
        let mut inv = invocation("counter", "increment", json!([1]));
        inv.op = InvOp::Deliver {
            method: "increment".into(),
            args: json!([1]),
            delivery_id: id,
        };
        inv
    };
    let over_hops = ingress
        .node
        .invoke(delivery("remote-hop-limit".into()), MAX_HOPS)
        .await;
    assert_eq!(over_hops.status, 503);
    assert_eq!(
        over_hops.body["error"]["message"],
        "too many forwarding hops"
    );
    store.uploads.store(0, Ordering::SeqCst);
    store.gate.store(true, Ordering::SeqCst);
    let first = call(&ingress.node, delivery("remote-first".into()));
    wait_upload(&store).await;
    let mut pending = Vec::new();
    for i in 0..16 {
        pending.push(call(&ingress.node, delivery(format!("remote-{i}"))));
    }
    wait_pending(&owner.node, "counter", 16).await;
    assert!(
        ingress.node.groups.lock().unwrap().is_empty(),
        "a remote ingress must not serialize forwards in its own writer queue"
    );
    assert!(!first.is_finished());
    store.proceed.add_permits(1);
    assert_eq!(first.await.unwrap().body["result"], 1);
    wait_upload(&store).await;
    assert!(pending.iter().all(|task| !task.is_finished()));
    store.proceed.add_permits(1);
    for task in pending {
        assert_eq!(task.await.unwrap().status, 200);
    }
    assert_eq!(
        store.uploads.load(Ordering::SeqCst),
        2,
        "seventeen remote deliveries must share two segment uploads"
    );
    store.gate.store(false, Ordering::SeqCst);
    assert_eq!(
        ingress
            .node
            .invoke(invocation("counter", "get", J::Null), 0)
            .await
            .body["result"],
        17
    );
    assert_eq!(
        ingress
            .node
            .invoke(delivery("remote-first".into()), 0)
            .await
            .body["result"],
        1,
        "a duplicate delivery must return its receipt without running again"
    );
    assert_eq!(store.uploads.load(Ordering::SeqCst), 2);
    assert_eq!(
        ingress
            .node
            .invoke(invocation("counter", "get", J::Null), 0)
            .await
            .body["result"],
        17
    );
    ingress.shutdown().await;
    owner.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn group_commit_fences_read_only_batches() {
    let (_dir, store, handle) = setup().await;
    assert_eq!(
        handle
            .node
            .invoke(invocation("counter", "increment", json!([1])), 0)
            .await
            .status,
        200
    );
    let id = ActorId {
        app: "group-test".into(),
        ty: "counter".into(),
        key: "one".into(),
    };
    let dyn_store: DynStore = store;
    let (mut record, etag) = owner::read(&dyn_store, &id).await.unwrap().unwrap();
    record.epoch += 1;
    dyn_store
        .put_if_match(&id.owner_key(), to_json_bytes(&record), &etag)
        .await
        .unwrap();
    assert_eq!(
        handle
            .node
            .invoke(invocation("counter", "get", J::Null), 0)
            .await
            .status,
        503
    );
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn group_commit_migrations_follow_first_successful_call() {
    let (dir, store, handle) = setup().await;
    let id = ActorId {
        app: "group-test".into(),
        ty: "account".into(),
        key: "migration".into(),
    };
    let dyn_store: DynStore = store;
    let code = handle.node.app("group-test").unwrap();
    let actor = actor::activate(
        &dyn_store,
        dir.path(),
        &id,
        1,
        "test".into(),
        true,
        code.clone(),
    )
    .await
    .unwrap();
    let (actor, group) = tokio::task::spawn_blocking(move || {
        let mut actor = actor;
        let group = actor
            .execute_group(
                &code,
                vec![
                    Op::Call {
                        method: "withdraw".into(),
                        args: json!([999]),
                        chain: vec![],
                    },
                    Op::Call {
                        method: "deposit".into(),
                        args: json!([100, null]),
                        chain: vec![],
                    },
                    Op::Call {
                        method: "balance".into(),
                        args: J::Null,
                        chain: vec![],
                    },
                ],
            )
            .unwrap();
        (actor, group)
    })
    .await
    .unwrap();
    assert!(group.outcomes[0].as_ref().unwrap().is_err);
    assert_eq!(group.outcomes[1].as_ref().unwrap().value, 100);
    assert_eq!(group.outcomes[2].as_ref().unwrap().value, 100);
    assert_eq!(
        group.segment.unwrap().txid,
        1,
        "the entire batch has one commit"
    );
    let rows: i64 = actor
        .db
        .lock()
        .unwrap()
        .query_row("SELECT count(*) FROM entries", [], |r| r.get(0))
        .unwrap();
    assert_eq!(rows, 1);
    let code = handle.node.app("group-test").unwrap();
    let actor = tokio::task::spawn_blocking(move || {
        let mut actor = actor;
        let pending_id = statex_runtime::spawn::enqueue(
            &actor.db.lock().unwrap(),
            actor.epoch,
            "app",
            "worker",
            "key",
            "run",
            "[]",
        )
        .unwrap();
        let group = actor
            .execute_group(
                &code,
                vec![Op::Call {
                    method: "balance".into(),
                    args: J::Null,
                    chain: vec![],
                }],
            )
            .unwrap();
        assert!(
            group.outbox.is_empty(),
            "existing tasks must not recreate hints"
        );
        actor
            .db
            .lock()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER spawn_success AFTER INSERT ON entries WHEN NEW.memo = 'spawn'
             BEGIN INSERT INTO _statex_outbox(id,app,actor_type,actor_key,method,args_json)
             VALUES('e1-test-new','app','worker','key','run','[]'); END;
             CREATE TRIGGER spawn_rollback AFTER INSERT ON entries WHEN NEW.kind = 'withdrawal'
             BEGIN INSERT INTO _statex_outbox(id,app,actor_type,actor_key,method,args_json)
             VALUES('e1-test-rollback','app','worker','key','run','[]'); END;",
            )
            .unwrap();
        let group = actor
            .execute_group(
                &code,
                vec![
                    Op::Call {
                        method: "deposit".into(),
                        args: json!([10, "spawn"]),
                        chain: vec![],
                    },
                    Op::Call {
                        method: "withdraw".into(),
                        args: json!([999]),
                        chain: vec![],
                    },
                    Op::Call {
                        method: "balance".into(),
                        args: J::Null,
                        chain: vec![],
                    },
                ],
            )
            .unwrap();
        assert_eq!(
            group.outbox.len(),
            1,
            "only newly committed tasks need hints"
        );
        assert_eq!(group.outbox[0].id, "e1-test-new");
        assert!(group.outcomes[1].as_ref().unwrap().is_err);
        let pending = statex_runtime::spawn::pending(&actor.db.lock().unwrap()).unwrap();
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().any(|task| task.id == pending_id));
        assert!(!pending.iter().any(|task| task.id == "e1-test-rollback"));
        let executed = actor
            .execute(
                &code,
                Op::Call {
                    method: "balance".into(),
                    args: J::Null,
                    chain: vec![],
                },
            )
            .unwrap();
        assert!(
            executed.outbox.is_empty(),
            "reads must not rewrite pending task hints"
        );
        assert!(actor.execute_group(&code, vec![Op::Touch]).is_err());
        actor
    })
    .await
    .unwrap();
    drop(actor);
    handle.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn group_commit_alarm_hint_precedes_upload_and_retry_is_preserved() {
    let (_dir, store, handle) = setup().await;
    let node = &handle.node;
    store.gate.store(true, Ordering::SeqCst);
    let first = call(node, invocation("counter", "increment", json!([1])));
    wait_upload(&store).await;
    let schedule = call(node, invocation("counter", "schedule", json!([60_000, 0])));
    wait_pending(node, "counter", 1).await;
    let cancel = call(node, invocation("counter", "cancel", J::Null));
    wait_pending(node, "counter", 2).await;
    let reschedule = call(node, invocation("counter", "schedule", json!([60_000, 1])));
    wait_pending(node, "counter", 3).await;
    store.proceed.add_permits(1);
    assert_eq!(first.await.unwrap().status, 200);
    wait_upload(&store).await;
    let hints = store.list("wake/").await.unwrap();
    assert_eq!(hints.len(), 1, "only the final alarm needs a wake hint");
    assert!(!schedule.is_finished() && !cancel.is_finished() && !reschedule.is_finished());
    store.gate.store(false, Ordering::SeqCst);
    store.proceed.add_permits(1);
    for reply in [
        schedule.await.unwrap(),
        cancel.await.unwrap(),
        reschedule.await.unwrap(),
    ] {
        assert_eq!(reply.status, 200);
    }
    let id = ActorId {
        app: "group-test".into(),
        ty: "counter".into(),
        key: "one".into(),
    };
    let code = node.app("group-test").unwrap();
    let slot = node.slot(&id);
    let guard = slot.clone().lock_owned().await;
    let fire_at = guard.as_ref().unwrap().alarm.unwrap().at_ms;
    assert_eq!(
        hints[0],
        wake_key(&id, &guard.as_ref().unwrap().alarm.unwrap())
    );
    let fired = node
        .run(
            guard,
            &id,
            code.clone(),
            Op::Alarm { now_ms: fire_at },
            false,
        )
        .await;
    assert_eq!(fired.body["result"]["fired"], true);
    assert!(fired.body["result"]["error"].is_string());
    let guard = slot.lock_owned().await;
    let retry = guard.as_ref().unwrap().alarm.unwrap();
    assert_eq!(retry.retry, 1);
    let fired = node
        .run(
            guard,
            &id,
            code,
            Op::Alarm {
                now_ms: retry.at_ms,
            },
            false,
        )
        .await;
    assert_eq!(fired.body["result"], json!({ "fired": true }));
    assert_eq!(
        node.invoke(invocation("counter", "fired", J::Null), 0)
            .await
            .body["result"],
        1
    );
    handle.shutdown().await;
}
