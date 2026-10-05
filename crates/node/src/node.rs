//! The node: routing, actor lifecycle and the call path
//! (transaction -> segment upload -> ownership check -> ack).

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::Bytes;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as J};
use sha2::Sha256;
use statex_ltx::segment_name;
use statex_runtime::{
    resolve_method, ActorCaller, ActorRef, AppCode, CallError, CallFailure, CallReply, CallRequest, Runtime,
};
use statex_store::{get_json, to_json_bytes, DynStore, StoreError};
use tokio::sync::{oneshot, Mutex as AsyncMutex};

use crate::actor::{self, Actor, Op};
use crate::deploy;
use crate::layout::{app_dir, now_ms, wake_key, ActorId, MAX_KEY_LEN, PEER_AUTH};
use crate::lease::{Lease, NodeRecord};
use crate::owner::{self, Acquire, OwnerRecord, OwnerState};

pub const MAX_HOPS: u32 = 4;
/// Longest chain of nested actor-to-actor calls.
pub const MAX_CALL_DEPTH: usize = 16;
pub const SIGNATURE_HEADER: &str = "x-statex-signature";

#[derive(Clone)]
pub struct NodeConfig {
    pub node_id: String,
    pub store: DynStore,
    pub data_dir: PathBuf,
    pub listen: SocketAddr,
    /// Separate listener for node-to-node calls; defaults to the public one.
    pub internal_listen: Option<SocketAddr>,
    /// URL peers use to reach this node's internal API.
    pub advertise: Option<String>,
    pub lease_ttl: Duration,
    pub idle_timeout: Duration,
    pub deploy_poll: Duration,
    /// Write a compaction snapshot after this many segments.
    pub snapshot_every: u64,
    /// Exit the process with status 3 when fenced (production behaviour).
    pub exit_on_fence: bool,
    /// How often the waker node scans for due alarms of non-resident actors.
    pub wake_tick: Duration,
    /// How often the waker rescans all wake hints, picking up ones whose
    /// earlier attempts failed.
    pub wake_full_scan: Duration,
    /// Concurrent fresh stateless instances permitted on this node.
    pub max_stateless_calls: usize,
}

impl NodeConfig {
    pub fn new(node_id: impl Into<String>, store: DynStore, data_dir: impl Into<PathBuf>) -> Self {
        Self {
            node_id: node_id.into(),
            store,
            data_dir: data_dir.into(),
            listen: "127.0.0.1:0".parse().unwrap(),
            internal_listen: None,
            advertise: None,
            lease_ttl: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(300),
            deploy_poll: Duration::from_secs(2),
            snapshot_every: 64,
            exit_on_fence: false,
            wake_tick: Duration::from_secs(1),
            wake_full_scan: Duration::from_secs(30),
            max_stateless_calls: 64,
        }
    }
}

/// A request to an actor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invocation {
    pub app: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub key: String,
    #[serde(flatten)]
    pub op: InvOp,
    /// For calls made by actors: the actors executing up the call chain,
    /// outermost first. Never set by the public API.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub chain: Vec<ActorRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum InvOp {
    Call { method: String, #[serde(default)] args: J },
    Create,
    Delete,
    /// Fire the actor's alarm if it is due (internal: issued by alarm timers
    /// and the waker, never by the public API). Never creates the actor.
    Alarm,
    /// Internal asynchronous delivery; stateful receivers record a receipt.
    Deliver { method: String, args: J, delivery_id: String },
    /// Internal outbox operations, never exposed by the public API.
    OutboxPoll { id: String },
    OutboxSettle { id: String, attempt: u32, error: Option<String> },
}

/// HTTP status + JSON body, identical whether produced locally or by a peer.
#[derive(Debug, Clone, PartialEq)]
pub struct Outcome {
    pub status: u16,
    pub body: J,
}

impl Outcome {
    pub fn ok(v: J) -> Self {
        Self { status: 200, body: json!({ "result": v }) }
    }
    pub fn err(status: u16, code: &str, message: impl Into<String>) -> Self {
        Self { status, body: json!({ "error": { "code": code, "message": message.into() } }) }
    }
    fn unavailable(m: impl Into<String>) -> Self {
        Self::err(503, "unavailable", m)
    }
}

type Slot = Arc<AsyncMutex<Option<Actor>>>;

const GROUP_PENDING_LIMIT: usize = 128;
const GROUP_BATCH_LIMIT: usize = 64;

#[cfg(test)]
#[path = "stateless_admission_tests.rs"]
mod stateless_admission_tests;

struct PendingCall {
    inv: Invocation,
    hops: u32,
    code: Arc<AppCode>,
    reply: oneshot::Sender<Retry>,
}

#[derive(Default)]
struct GroupQueue {
    pending: Mutex<VecDeque<PendingCall>>,
}

pub struct Node {
    pub cfg: NodeConfig,
    pub store: DynStore,
    pub runtime: Runtime,
    pub lease: Arc<Lease>,
    apps: RwLock<HashMap<String, Arc<AppCode>>>,
    deployed: Mutex<HashMap<String, deploy::Current>>,
    actors: Mutex<HashMap<ActorId, Slot>>,
    groups: Mutex<HashMap<ActorId, Arc<GroupQueue>>>,
    me_weak: Weak<Node>,
    stateless_slots: Arc<tokio::sync::Semaphore>,
    secret: Vec<u8>,
    client: reqwest::Client,
    caller: Arc<dyn ActorCaller>,
    /// Due times of the alarms of resident actors (see `alarms.rs`).
    pub(crate) timers: Mutex<HashMap<ActorId, u64>>,
    pub(crate) timers_changed: tokio::sync::Notify,
}

/// Routes calls made by actors through [`Node::invoke`], exactly like calls
/// arriving over HTTP.
struct NodeCaller {
    node: Weak<Node>,
    rt: tokio::runtime::Handle,
}

impl ActorCaller for NodeCaller {
    fn call(&self, req: CallRequest) -> CallReply {
        let Some(node) = self.node.upgrade() else {
            return CallReply::Failed(CallFailure::Unavailable("node is shutting down".into()));
        };
        let inv = Invocation {
            app: req.target.app,
            ty: req.target.actor_type,
            key: req.target.key,
            op: InvOp::Call { method: req.method, args: req.args },
            chain: req.chain,
        };
        // Runs on the blocking thread executing the caller.
        match self.rt.block_on(tokio::time::timeout(req.timeout, node.invoke(inv, 0))) {
            Err(_) => CallReply::Failed(CallFailure::Timeout),
            Ok(o) => reply_of(o),
        }
    }
}

/// Maps an HTTP-shaped outcome to a call reply.
pub fn reply_of(o: Outcome) -> CallReply {
    let err = &o.body["error"];
    let msg = err["message"].as_str().unwrap_or_default().to_string();
    if (200..300).contains(&o.status) {
        return CallReply::Ok(o.body["result"].clone());
    }
    CallReply::Failed(match err["code"].as_str() {
        Some("method_error") => return CallReply::MethodErr(err["detail"].clone()),
        Some("not_found") => CallFailure::NotFound(msg),
        Some("bad_request") => CallFailure::Incompatible(msg),
        Some("trap") => CallFailure::Trap(msg),
        // The variant already says "cycle"; keep only the path.
        Some("cycle") => CallFailure::Cycle(msg.trim_start_matches("call cycle: ").to_string()),
        _ => CallFailure::Unavailable(format!("{} {msg}", o.status)),
    })
}

#[derive(Serialize, Deserialize)]
struct PeerAuth {
    secret: String,
}

#[derive(Serialize, Deserialize)]
pub struct Forwarded {
    pub invocation: Invocation,
    pub hops: u32,
    pub ts_ms: u64,
}

async fn peer_secret(store: &DynStore) -> Result<Vec<u8>> {
    loop {
        if let Some((a, _)) = get_json::<PeerAuth>(&**store, PEER_AUTH).await? {
            return Ok(hex::decode(a.secret)?);
        }
        let a = PeerAuth { secret: hex::encode(rand::random::<[u8; 32]>()) };
        match store.put_if_absent(PEER_AUTH, to_json_bytes(&a)).await {
            Ok(_) | Err(StoreError::Precondition) => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

pub fn sign(secret: &[u8], body: &[u8]) -> String {
    let mut m = Hmac::<Sha256>::new_from_slice(secret).expect("any key size");
    m.update(body);
    hex::encode(m.finalize().into_bytes())
}

/// Accepts WIT names (`get-balance`) and snake_case (`get_balance`).
pub fn norm(s: &str) -> String {
    if s.starts_with('_') { s.to_string() } else { s.replace('_', "-") }
}

impl Node {
    /// Acquires the lease, starts renewing it, then loads deployments.
    pub(crate) async fn new(cfg: NodeConfig, advertise: String) -> Result<(Arc<Node>, tokio::task::JoinHandle<()>)> {
        anyhow::ensure!(cfg.max_stateless_calls > 0, "max_stateless_calls must be greater than zero");
        let stateless_slots = Arc::new(tokio::sync::Semaphore::new(cfg.max_stateless_calls));
        let store = cfg.store.clone();
        let runtime = Runtime::shared()?;
        let lease = Lease::acquire(store.clone(), &cfg.node_id, &advertise, cfg.lease_ttl).await?;
        let renew = tokio::spawn(lease.clone().run());
        let secret = match peer_secret(&store).await {
            Ok(s) => s,
            Err(e) => {
                renew.abort();
                return Err(e);
            }
        };
        let client = reqwest::Client::builder().timeout(Duration::from_secs(60)).build()?;
        let rt = tokio::runtime::Handle::current();
        let node = Arc::new_cyclic(|me| Node {
            cfg,
            store,
            runtime,
            lease,
            apps: Default::default(),
            deployed: Default::default(),
            actors: Default::default(),
            groups: Default::default(),
            me_weak: me.clone(),
            secret,
            client,
            caller: Arc::new(NodeCaller { node: me.clone(), rt }),
            stateless_slots,
            timers: Default::default(),
            timers_changed: Default::default(),
        });
        if let Err(e) = node.refresh_apps().await {
            renew.abort();
            return Err(e);
        }
        Ok((node, renew))
    }

    pub fn me(&self) -> NodeRecord {
        self.lease.record()
    }

    pub fn app(&self, name: &str) -> Option<Arc<AppCode>> {
        self.apps.read().unwrap().get(name).cloned()
    }

    pub fn deployment(&self, app: &str) -> Option<deploy::Current> {
        self.deployed.lock().unwrap().get(app).cloned()
    }

    pub fn apps(&self) -> Vec<Arc<AppCode>> {
        let mut v: Vec<_> = self.apps.read().unwrap().values().cloned().collect();
        v.sort_by(|a, b| a.manifest.app.cmp(&b.manifest.app));
        v
    }

    pub fn resident_actors(&self) -> Vec<ActorId> {
        let actors = self.actors.lock().unwrap();
        let mut v: Vec<_> = actors
            .iter()
            .filter(|(_, s)| s.try_lock().map(|g| g.is_some()).unwrap_or(true))
            .map(|(k, _)| k.clone())
            .collect();
        v.sort();
        v
    }

    /// Loads new deployments. Resident actors switch on their next call.
    pub async fn refresh_apps(&self) -> Result<()> {
        for cur in deploy::list(&self.store).await? {
            if self.deployed.lock().unwrap().get(&cur.app).is_some_and(|d| d.id == cur.id) {
                continue;
            }
            let (wasm, manifest) = deploy::fetch(&self.store, &cur).await?;
            let rt = self.runtime.clone();
            let code = tokio::task::spawn_blocking(move || rt.load(&wasm, manifest))
                .await?
                .with_context(|| format!("load {}@{}", cur.app, cur.sha256))?;
            tracing::info!(app = cur.app, version = cur.version, sha = &cur.sha256[..12], "loaded deployment");
            self.apps.write().unwrap().insert(cur.app.clone(), code);
            self.deployed.lock().unwrap().insert(cur.app.clone(), cur);
        }
        Ok(())
    }

    fn slot(&self, id: &ActorId) -> Slot {
        self.actors.lock().unwrap().entry(id.clone()).or_default().clone()
    }

    fn drop_slot_if_empty(&self, id: &ActorId) {
        let mut actors = self.actors.lock().unwrap();
        if let Some(s) = actors.get(id) {
            if Arc::strong_count(s) == 1 && s.try_lock().map(|g| g.is_none()).unwrap_or(false) {
                actors.remove(id);
            }
        }
    }

    /// Routes and runs an invocation. Any node accepts any call.
    pub async fn invoke(&self, mut inv: Invocation, hops: u32) -> Outcome {
        inv.ty = norm(&inv.ty);
        if let InvOp::Call { method, .. } | InvOp::Deliver { method, .. } = &mut inv.op {
            *method = norm(method);
        }
        let deadline = Instant::now() + self.cfg.lease_ttl * 2;
        loop {
            match self.try_invoke(&inv, hops).await {
                Retry::Done(o) => return o,
                Retry::Again(why) if Instant::now() < deadline => {
                    tracing::debug!("retrying {}: {why}", inv.key);
                    tokio::time::sleep(Duration::from_millis(250)).await;
                }
                Retry::Again(why) => return Outcome::unavailable(why),
            }
        }
    }

    async fn try_invoke(&self, inv: &Invocation, hops: u32) -> Retry {
        use Retry::*;
        if !self.lease.valid() {
            return Done(Outcome::unavailable("node is fenced or its lease is not current"));
        }
        let Some(code) = self.app(&inv.app) else {
            return Done(Outcome::err(404, "not_found", format!("app {:?} is not deployed", inv.app)));
        };
        if let InvOp::Call { method, .. } | InvOp::Deliver { method, .. } = &inv.op {
            if let Err(e) = resolve_method(&code.manifest, &inv.ty, method) {
                return Done(Outcome::err(404, "not_found", e.to_string()));
            }
        } else if code.manifest.actor_type(&inv.ty).is_none() {
            return Done(Outcome::err(404, "not_found", format!("app {} has no actor type {:?}", inv.app, inv.ty)));
        }
        if inv.key.is_empty() || inv.key.len() > MAX_KEY_LEN {
            return Done(Outcome::err(400, "bad_request", format!("actor key must be 1..={MAX_KEY_LEN} bytes")));
        }
        let id = ActorId { app: inv.app.clone(), ty: inv.ty.clone(), key: inv.key.clone() };
        // The actors on the chain hold their slot locks while waiting for this
        // call, so calling back into one of them would deadlock.
        if inv.chain.iter().any(|a| a.app == id.app && a.actor_type == id.ty && a.key == id.key) {
            let path: Vec<String> = inv.chain.iter().map(|a| a.to_string()).collect();
            return Done(Outcome::err(508, "cycle", format!("call cycle: {} -> {id}", path.join(" -> "))));
        }
        if inv.chain.len() >= MAX_CALL_DEPTH {
            return Done(Outcome::err(508, "cycle", format!("call chain deeper than {MAX_CALL_DEPTH}")));
        }
        if code.manifest.actor_type(&inv.ty).is_some_and(|t| t.stateless) {
            return Done(self.run_stateless(inv, code).await);
        }
        if matches!(inv.op, InvOp::Call { .. } | InvOp::Deliver { .. })
            && code.manifest.actor_type(&inv.ty).is_some_and(|ty| ty.group_commit)
        {
            // Serial forwarding at an ingress would prevent concurrent sends
            // from accumulating in the actual owner's writer queue.
            match owner::remote_owner(&self.store, &id, &self.me()).await {
                Ok(Some(peer)) => {
                    if hops >= MAX_HOPS {
                        return Done(Outcome::unavailable("too many forwarding hops"));
                    }
                    return match self.forward(&peer.advertise, inv, hops + 1).await {
                        Ok(outcome) => Done(outcome),
                        Err(e) => Retry::Again(format!("owner {} unreachable: {e}", peer.node_id)),
                    };
                }
                Ok(None) => {}
                Err(e) => return Retry::Again(format!("read owner for group admission: {e:#}")),
            }
            return self.group_call(&id, inv, hops, code).await;
        }
        self.try_invoke_serial(inv, hops, code, &id, None).await
    }

    async fn group_call(&self, id: &ActorId, inv: &Invocation, hops: u32, code: Arc<AppCode>) -> Retry {
        let (reply, received) = oneshot::channel();
        {
            let mut groups = self.groups.lock().unwrap();
            let start = !groups.contains_key(id);
            let queue = groups.entry(id.clone()).or_default().clone();
            let mut pending = queue.pending.lock().unwrap();
            if pending.len() >= GROUP_PENDING_LIMIT {
                return Retry::Done(Outcome::unavailable("actor pending call limit reached"));
            }
            pending.push_back(PendingCall { inv: inv.clone(), hops, code, reply });
            drop(pending);
            if start {
                let node = self.me_weak.upgrade().expect("live node");
                let id = id.clone();
                tokio::spawn(async move { node.drain_group(id, queue).await });
            }
        }
        received.await.unwrap_or_else(|_| Retry::Done(Outcome::unavailable("actor writer stopped")))
    }

    async fn drain_group(self: Arc<Self>, id: ActorId, queue: Arc<GroupQueue>) {
        loop {
            let first = {
                // Admission and removal use the same lock order, so an idle
                // queue cannot lose a call or elect two writers.
                let mut groups = self.groups.lock().unwrap();
                let mut pending = queue.pending.lock().unwrap();
                match pending.pop_front() {
                    Some(first) => first,
                    None => {
                        groups.remove(&id);
                        return;
                    }
                }
            };
            let outcome = self.try_invoke_serial(&first.inv, first.hops, first.code, &id, Some(&queue)).await;
            let _ = first.reply.send(outcome);
        }
    }

    async fn try_invoke_serial(
        &self,
        inv: &Invocation,
        hops: u32,
        code: Arc<AppCode>,
        id: &ActorId,
        group: Option<&Arc<GroupQueue>>,
    ) -> Retry {
        use Retry::*;
        if !self.lease.valid() {
            return Done(Outcome::unavailable("node is fenced or its lease is not current"));
        }
        let slot = self.slot(&id);
        let mut guard = slot.clone().lock_owned().await;
        let mut created = false;
        if guard.is_none() {
            if matches!(inv.op, InvOp::Delete | InvOp::Alarm | InvOp::OutboxPoll { .. } | InvOp::OutboxSettle { .. }) {
                match owner::read(&self.store, &id).await {
                    Ok(Some((r, _))) if r.state != OwnerState::Deleted => {}
                    Ok(_) => {
                        drop(guard);
                        self.drop_slot_if_empty(&id);
                        // `gone` tells the waker the actor no longer exists.
                        let code = if matches!(inv.op, InvOp::Delete) { "not_found" } else { "gone" };
                        return Done(Outcome::err(404, code, format!("actor {id} does not exist")));
                    }
                    Err(e) => return Again(format!("read owner: {e}")),
                }
            }
            let create_only = matches!(inv.op, InvOp::Create);
            match owner::acquire(&self.store, &id, &self.me(), create_only).await {
                Err(e) => return Again(format!("acquire {id}: {e:#}")),
                Ok(Acquire::Exists) => {
                    drop(guard);
                    self.drop_slot_if_empty(&id);
                    return Done(Outcome::err(409, "conflict", format!("actor {id} already exists")));
                }
                Ok(Acquire::Remote(peer)) => {
                    drop(guard);
                    self.drop_slot_if_empty(&id);
                    if hops >= MAX_HOPS {
                        return Done(Outcome::unavailable("too many forwarding hops"));
                    }
                    return match self.forward(&peer.advertise, inv, hops + 1).await {
                        Ok(o) => Done(o),
                        Err(e) => Again(format!("owner {} unreachable: {e}", peer.node_id)),
                    };
                }
                Ok(Acquire::Acquired { epoch, etag, fresh }) => {
                    match actor::activate(&self.store, &self.cfg.data_dir, &id, epoch, etag, fresh, code.clone()).await {
                        Ok(mut c) => {
                            c.caller = Some(self.caller.clone());
                            tracing::info!(actor = %id, epoch, "activated");
                            self.set_timer(&id, c.alarm.map(|a| a.at_ms));
                            *guard = Some(c);
                            created = true;
                        }
                        Err(e) => return Again(format!("activate {id}: {e:#}")),
                    }
                }
            }
        } else if matches!(inv.op, InvOp::Create) {
            return Done(Outcome::err(409, "conflict", format!("actor {id} already exists")));
        }

        let op = match &inv.op {
            InvOp::Delete => return Done(self.delete(guard, &id).await),
            InvOp::Create => {
                debug_assert!(created);
                Op::Touch
            }
            InvOp::Call { method, args } => {
                Op::Call { method: method.clone(), args: args.clone(), chain: inv.chain.clone() }
            }
            InvOp::Alarm => Op::Alarm { now_ms: now_ms() },
            InvOp::Deliver { method, args, delivery_id } => Op::Deliver {
                method: method.clone(), args: args.clone(), delivery_id: delivery_id.clone(),
            },
            InvOp::OutboxPoll { id } => Op::OutboxPoll { id: id.clone(), now_ms: now_ms() },
            InvOp::OutboxSettle { id, attempt, error } => Op::OutboxSettle {
                id: id.clone(), attempt: *attempt, error: error.clone(), now_ms: now_ms(),
            },
        };
        if let Some(queue) = group {
            let mut replies = Vec::new();
            let mut ops = vec![op];
            {
                let mut pending = queue.pending.lock().unwrap();
                while ops.len() < GROUP_BATCH_LIMIT
                    && pending.front().is_some_and(|p| Arc::ptr_eq(&p.code, &code))
                {
                    let call = pending.pop_front().unwrap();
                    ops.push(match call.inv.op {
                        InvOp::Call { method, args } => Op::Call { method, args, chain: call.inv.chain },
                        InvOp::Deliver { method, args, delivery_id } => Op::Deliver { method, args, delivery_id },
                        _ => unreachable!("only calls and deliveries enter the group writer"),
                    });
                    replies.push(call.reply);
                }
            }
            let mut outcomes = self.run_group(guard, id, code, ops).await.into_iter();
            let first = outcomes.next().expect("first group outcome");
            for (reply, outcome) in replies.into_iter().zip(outcomes) {
                let _ = reply.send(Retry::Done(outcome));
            }
            Done(first)
        } else {
            Done(self.run(guard, &id, code, op, matches!(inv.op, InvOp::Create)).await)
        }
    }

    async fn run_stateless(&self, inv: &Invocation, code: Arc<AppCode>) -> Outcome {
        let (method, args) = match &inv.op {
            InvOp::Call { method, args } | InvOp::Deliver { method, args, .. } => (method, args),
            _ => return Outcome::err(400, "bad_request", "stateless actors do not support create, delete, alarms or outbox operations"),
        };
        let permit = match self.stateless_slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => return Outcome::unavailable("stateless invocation limit reached"),
        };
        let identity = statex_runtime::ActorIdentity {
            app: inv.app.clone(), actor_type: inv.ty.clone(), key: inv.key.clone(), epoch: 0,
        };
        let (method, args, chain, caller) = (method.clone(), args.clone(), inv.chain.clone(), self.caller.clone());
        let res = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            code.call_stateless(identity, &method, &args, &chain, Some(caller))
        }).await;
        match res {
            Err(e) => Outcome::err(500, "internal", format!("execute task: {e}")),
            Ok(Ok(out)) if out.is_err => Outcome {
                status: 422,
                body: json!({ "error": { "code": "method_error", "message": "method returned an error", "detail": out.value } }),
            },
            Ok(Ok(out)) => Outcome::ok(out.value),
            Ok(Err(CallError::NotFound(m))) => Outcome::err(404, "not_found", m),
            Ok(Err(CallError::BadArgs(m))) => Outcome::err(400, "bad_request", m),
            Ok(Err(CallError::Trap(m))) => Outcome::err(500, "trap", m),
        }
    }

    async fn run(
        &self,
        guard: tokio::sync::OwnedMutexGuard<Option<Actor>>,
        id: &ActorId,
        code: Arc<AppCode>,
        op: Op,
        is_create: bool,
    ) -> Outcome {
        let (guard, res) = tokio::task::spawn_blocking(move || {
            let mut guard = guard;
            let res = guard.as_mut().expect("resident").execute(&code, op);
            (guard, res)
        })
        .await
        .expect("execute task panicked");
        self.finish_execution(guard, res, id, is_create, false).await
    }

    async fn run_group(
        &self,
        guard: tokio::sync::OwnedMutexGuard<Option<Actor>>,
        id: &ActorId,
        code: Arc<AppCode>,
        ops: Vec<Op>,
    ) -> Vec<Outcome> {
        let count = ops.len();
        let (guard, res) = tokio::task::spawn_blocking(move || {
            let mut guard = guard;
            let res = guard.as_mut().expect("resident").execute_group(&code, ops);
            (guard, res)
        })
        .await
        .expect("execute group task panicked");
        let (outcomes, executed) = match res {
            Ok(group) => (
                Some(group.outcomes),
                Ok(actor::Executed {
                    outcome: Ok(statex_runtime::CallOutput { value: J::Null, is_err: false }),
                    segment: group.segment,
                    alarm: group.alarm,
                    outbox: group.outbox,
                }),
            ),
            Err(e) => (None, Err(e)),
        };
        let durability = self.finish_execution(guard, executed, id, false, true).await;
        if durability.status != 200 {
            return vec![durability; count];
        }
        outcomes.expect("successful group").into_iter().map(call_outcome).collect()
    }

    async fn finish_execution(
        &self,
        mut guard: tokio::sync::OwnedMutexGuard<Option<Actor>>,
        res: Result<actor::Executed>,
        id: &ActorId,
        is_create: bool,
        fence_reads: bool,
    ) -> Outcome {
        let executed = match res {
            Ok(x) => x,
            Err(e) => {
                *guard = None;
                self.set_timer(id, None);
                return Outcome::err(500, "internal", format!("{e:#}"));
            }
        };
        if let Err(e) = self.write_outbox_hints(id, &executed.outbox).await {
            *guard = None;
            self.set_timer(id, None);
            return Outcome::unavailable(format!("spawn was not made durable ({e}); sender write did not apply"));
        }
        // A new alarm's wake hint is written before the transaction becomes
        // durable, so a durable alarm always has one.
        if let Some(a) = executed.alarm.and_then(|c| c.after) {
            if let Err(e) = self.store.put(&wake_key(id, &a), to_json_bytes(&json!({ "at_ms": a.at_ms }))).await {
                *guard = None;
                self.set_timer(id, None);
                return Outcome::unavailable(format!("write was not made durable ({e}); it did not apply"));
            }
        }
        if fence_reads && executed.segment.is_none() {
            let epoch = guard.as_ref().unwrap().epoch;
            if !matches!(owner::still_owner(&self.store, id, &self.me(), epoch).await, Ok(true))
                || !self.lease.valid()
            {
                *guard = None;
                self.set_timer(id, None);
                return Outcome::unavailable("actor ownership moved; group not acknowledged");
            }
        }
        if let Some(seg) = executed.segment {
            // Actor is not Sync: copy what we need before awaiting.
            let (epoch, txid, snapshot_txid) = {
                let c = guard.as_ref().unwrap();
                (c.epoch, c.txid, c.snapshot_txid)
            };
            let key = format!("{}{}", id.epoch_prefix(epoch), segment_name(seg.txid));
            if let Err(e) = self.store.put(&key, Bytes::from(seg.encode())).await {
                *guard = None;
                self.set_timer(id, None);
                return Outcome::unavailable(format!("write was not made durable ({e}); it may or may not have applied"));
            }
            let owned = matches!(owner::still_owner(&self.store, id, &self.me(), epoch).await, Ok(true))
                && self.lease.valid();
            if !owned {
                tracing::warn!(actor = %id, epoch, "lost ownership; not acknowledging");
                *guard = None;
                self.set_timer(id, None);
                return Outcome::unavailable("actor ownership moved; write not acknowledged");
            }
            if let Some(change) = executed.alarm {
                self.set_timer(id, change.after.map(|a| a.at_ms));
                if let Some(old) = change.before {
                    let (store, key) = (self.store.clone(), wake_key(id, &old));
                    tokio::spawn(async move {
                        if let Err(e) = store.delete(&key).await {
                            tracing::debug!("delete stale wake hint {key}: {e}");
                        }
                    });
                }
            }
            if txid - snapshot_txid >= self.cfg.snapshot_every {
                let store = self.store.clone();
                let id = id.clone();
                tokio::spawn(async move {
                    let (mut guard, res) = tokio::task::spawn_blocking(move || {
                        let r = guard.as_mut().map(|c| c.snapshot().map(|b| (c.epoch, c.txid, b)));
                        (guard, r)
                    })
                    .await
                    .expect("snapshot task");
                    match res {
                        // `guard` stays held until the upload is done, which
                        // keeps the snapshot file unchanged.
                        Some(Ok((epoch, txid, image))) => match actor::compact(&store, &id, epoch, txid, &image).await {
                            Ok(()) => {
                                if let Some(c) = guard.as_mut() {
                                    c.snapshot_txid = txid;
                                }
                                tracing::debug!(actor = %id, txid, "compacted");
                            }
                            Err(e) => tracing::warn!(actor = %id, "compaction failed: {e:#}"),
                        },
                        Some(Err(e)) => {
                            tracing::warn!(actor = %id, "snapshot failed: {e:#}");
                            *guard = None;
                        }
                        None => {}
                    }
                });
            }
        }
        if is_create {
            return Outcome { status: 201, body: json!({ "result": { "created": true } }) };
        }
        call_outcome(executed.outcome)
    }

    async fn delete(&self, mut guard: tokio::sync::OwnedMutexGuard<Option<Actor>>, id: &ActorId) -> Outcome {
        let actor = guard.take().expect("resident");
        if !self.lease.valid() {
            return Outcome::unavailable("lease not current");
        }
        match owner::release(&self.store, id, &self.me(), actor.epoch, &actor.owner_etag, OwnerState::Deleted).await {
            Ok(true) => {}
            Ok(false) => return Outcome::unavailable("actor ownership moved; retry"),
            Err(e) => return Outcome::unavailable(format!("delete failed: {e}")),
        }
        self.set_timer(id, None);
        if let Some(a) = actor.alarm {
            let _ = self.store.delete(&wake_key(id, &a)).await;
        }
        drop(actor);
        let prefix = id.ltx_prefix();
        if let Ok(keys) = self.store.list(&prefix).await {
            for k in keys {
                let _ = self.store.delete(&k).await;
            }
        }
        drop(guard);
        self.drop_slot_if_empty(id);
        tracing::info!(actor = %id, "deleted");
        Outcome::ok(json!({ "deleted": true }))
    }

    async fn forward(&self, base: &str, inv: &Invocation, hops: u32) -> Result<Outcome> {
        let body = serde_json::to_vec(&Forwarded { invocation: inv.clone(), hops, ts_ms: now_ms() })?;
        let sig = sign(&self.secret, &body);
        let resp = self
            .client
            .post(format!("{}/internal/v1/invoke", base.trim_end_matches('/')))
            .header(SIGNATURE_HEADER, sig)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await?;
        let status = resp.status().as_u16();
        let body: J = resp.json().await?;
        Ok(Outcome { status, body })
    }

    /// Verifies and runs a forwarded call.
    pub async fn invoke_forwarded(&self, body: &[u8], signature: Option<&str>) -> Outcome {
        let ok = signature.is_some_and(|s| {
            let mut m = Hmac::<Sha256>::new_from_slice(&self.secret).expect("key");
            m.update(body);
            hex::decode(s).is_ok_and(|s| m.verify_slice(&s).is_ok())
        });
        if !ok {
            return Outcome::err(401, "unauthorized", "invalid peer signature");
        }
        let f: Forwarded = match serde_json::from_slice(body) {
            Ok(f) => f,
            Err(e) => return Outcome::err(400, "bad_request", e.to_string()),
        };
        if now_ms().abs_diff(f.ts_ms) > 60_000 {
            return Outcome::err(401, "unauthorized", "stale peer request");
        }
        self.invoke(f.invocation, f.hops).await
    }

    /// Releases actors idle for longer than the idle timeout.
    pub async fn evict_idle(&self, idle: Duration) {
        let slots: Vec<(ActorId, Slot)> =
            self.actors.lock().unwrap().iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        for (id, slot) in slots {
            let Ok(mut g) = slot.clone().try_lock_owned() else { continue };
            if g.as_ref().is_some_and(|c| c.last_used.elapsed() >= idle) {
                let c = g.take().unwrap();
                // The waker fires its alarm from now on.
                self.set_timer(&id, None);
                match owner::release(&self.store, &id, &self.me(), c.epoch, &c.owner_etag, OwnerState::Unowned).await {
                    Ok(_) => tracing::info!(actor = %id, "released"),
                    Err(e) => tracing::warn!(actor = %id, "release failed: {e}"),
                }
            }
            drop(g);
            drop(slot);
            self.drop_slot_if_empty(&id);
        }
    }

    /// Releases every actor (graceful shutdown).
    pub async fn release_all(&self) {
        self.evict_idle(Duration::ZERO).await;
    }

    /// Drops all resident actors without touching the store (after fencing).
    pub fn drop_all(&self) {
        self.timers.lock().unwrap().clear();
        let slots: Vec<Slot> = self.actors.lock().unwrap().drain().map(|(_, v)| v).collect();
        for s in slots {
            if let Ok(mut g) = s.try_lock() {
                *g = None;
            }
        }
    }

    /// Lists actors of an app from ownership records.
    pub async fn list_actors(&self, app: &str, ty: Option<&str>, limit: usize) -> Result<Vec<OwnerRecord>> {
        let prefix = match ty {
            Some(t) => format!("actors/{}/{}/", app_dir(app), norm(t)),
            None => format!("actors/{}/", app_dir(app)),
        };
        let mut out = Vec::new();
        for k in self.store.list(&prefix).await? {
            if !k.ends_with("/owner.json") || k.matches('/').count() != 4 {
                continue;
            }
            if let Some((r, _)) = get_json::<OwnerRecord>(&*self.store, &k).await? {
                if r.state != OwnerState::Deleted {
                    out.push(r);
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(out)
    }
}

fn call_outcome(outcome: Result<statex_runtime::CallOutput, CallError>) -> Outcome {
    match outcome {
        Ok(out) if out.is_err => Outcome {
            status: 422,
            body: json!({ "error": { "code": "method_error", "message": "method returned an error", "detail": out.value } }),
        },
        Ok(out) => Outcome::ok(out.value),
        Err(CallError::NotFound(m)) => Outcome::err(404, "not_found", m),
        Err(CallError::BadArgs(m)) => Outcome::err(400, "bad_request", m),
        Err(CallError::Trap(m)) => Outcome::err(500, "trap", m),
    }
}

enum Retry {
    Done(Outcome),
    Again(String),
}

#[cfg(test)]
#[path = "group_commit_tests.rs"]
mod group_commit_tests;
