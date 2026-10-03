//! The node: routing, actor lifecycle and the call path
//! (transaction -> segment upload -> ownership check -> ack).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::Bytes;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as J};
use sha2::Sha256;
use statex_ltx::segment_name;
use statex_runtime::{resolve_method, AppCode, CallError, Runtime};
use statex_store::{get_json, to_json_bytes, DynStore, StoreError};
use tokio::sync::Mutex as AsyncMutex;

use crate::actor::{self, Actor, Op};
use crate::deploy;
use crate::layout::{app_dir, now_ms, ActorId, MAX_KEY_LEN, PEER_AUTH};
use crate::lease::{Lease, NodeRecord};
use crate::owner::{self, Acquire, OwnerRecord, OwnerState};

pub const MAX_HOPS: u32 = 4;
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum InvOp {
    Call { method: String, #[serde(default)] args: J },
    Create,
    Delete,
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

pub struct Node {
    pub cfg: NodeConfig,
    pub store: DynStore,
    pub runtime: Runtime,
    pub lease: Arc<Lease>,
    apps: RwLock<HashMap<String, Arc<AppCode>>>,
    deployed: Mutex<HashMap<String, deploy::Current>>,
    actors: Mutex<HashMap<ActorId, Slot>>,
    secret: Vec<u8>,
    client: reqwest::Client,
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
        let node = Arc::new(Node {
            cfg,
            store,
            runtime,
            lease,
            apps: Default::default(),
            deployed: Default::default(),
            actors: Default::default(),
            secret,
            client: reqwest::Client::builder().timeout(Duration::from_secs(60)).build()?,
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
        if let InvOp::Call { method, .. } = &mut inv.op {
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
        if let InvOp::Call { method, .. } = &inv.op {
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
        let slot = self.slot(&id);
        let mut guard = slot.clone().lock_owned().await;
        let mut created = false;
        if guard.is_none() {
            if matches!(inv.op, InvOp::Delete) {
                match owner::read(&self.store, &id).await {
                    Ok(Some((r, _))) if r.state != OwnerState::Deleted => {}
                    Ok(_) => {
                        drop(guard);
                        self.drop_slot_if_empty(&id);
                        return Done(Outcome::err(404, "not_found", format!("actor {id} does not exist")));
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
                        Ok(c) => {
                            tracing::info!(actor = %id, epoch, "activated");
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
            InvOp::Call { method, args } => Op::Call { method: method.clone(), args: args.clone() },
        };
        Done(self.run(guard, &id, code, op, matches!(inv.op, InvOp::Create)).await)
    }

    async fn run(
        &self,
        guard: tokio::sync::OwnedMutexGuard<Option<Actor>>,
        id: &ActorId,
        code: Arc<AppCode>,
        op: Op,
        is_create: bool,
    ) -> Outcome {
        let (mut guard, res) = tokio::task::spawn_blocking(move || {
            let mut guard = guard;
            let res = guard.as_mut().expect("resident").execute(&code, op);
            (guard, res)
        })
        .await
        .expect("execute task panicked");
        let executed = match res {
            Ok(x) => x,
            Err(e) => {
                *guard = None;
                return Outcome::err(500, "internal", format!("{e:#}"));
            }
        };
        if let Some(seg) = executed.segment {
            // Actor is not Sync: copy what we need before awaiting.
            let (epoch, txid, snapshot_txid) = {
                let c = guard.as_ref().unwrap();
                (c.epoch, c.txid, c.snapshot_txid)
            };
            let key = format!("{}{}", id.epoch_prefix(epoch), segment_name(seg.txid));
            if let Err(e) = self.store.put(&key, Bytes::from(seg.encode())).await {
                *guard = None;
                return Outcome::unavailable(format!("write was not made durable ({e}); it may or may not have applied"));
            }
            let owned = self.lease.valid()
                && matches!(owner::still_owner(&self.store, id, &self.me(), epoch).await, Ok(true));
            if !owned {
                tracing::warn!(actor = %id, epoch, "lost ownership; not acknowledging");
                *guard = None;
                return Outcome::unavailable("actor ownership moved; write not acknowledged");
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
                        Some(Ok((epoch, txid, image))) => match actor::compact(&store, &id, epoch, txid, image).await {
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
        match executed.outcome {
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

enum Retry {
    Done(Outcome),
    Again(String),
}
