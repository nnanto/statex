//! The node: routing, actor lifecycle and the call path
//! (transaction -> segment upload -> ownership check -> ack).

use std::collections::HashMap;
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
use statex_runtime::database::{DynDatabaseFactory, SqliteFactory};
use statex_runtime::{
    resolve_method, ActorCaller, ActorRef, AppCode, CallError, CallFailure, CallReply, CallRequest,
    Runtime,
};
use statex_store::{get_json, to_json_bytes, DynStore, StoreError};
use tokio::sync::Mutex as AsyncMutex;

use crate::actor::{self, Actor, Op};
use crate::deploy;
use crate::extensions::{
    observe_lifecycle, run_hook, Caller, HookError, InvocationContext, InvocationExtension,
    InvocationMetadata, InvocationOperation, LifecycleEvent, LifecycleKind,
};
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
    /// Actor database and durable state format. All nodes serving an actor
    /// must use a factory with the same identity. SQLite/WAL is the default.
    pub database_factory: DynDatabaseFactory,
    /// Optional configured runtime (additional host capabilities and adapters).
    /// None uses the process-wide default runtime.
    pub runtime: Option<Runtime>,
    /// Ordered invocation and lifecycle extensions. Empty means no hooks.
    pub extensions: Vec<Arc<dyn InvocationExtension>>,
    /// Maximum wait for each async hook. Critical hooks also share the
    /// invocation deadline; observers use an independent bounded budget.
    pub extension_timeout: Duration,
    /// Optional public HTTP router customization, e.g. trusted authentication
    /// middleware. The peer router is never passed through this callback.
    pub public_router: Option<Arc<dyn Fn(axum::Router) -> axum::Router + Send + Sync>>,
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
            database_factory: Arc::new(SqliteFactory),
            runtime: None,
            extensions: Vec::new(),
            extension_timeout: Duration::from_secs(5),
            public_router: None,
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
    Call {
        method: String,
        #[serde(default)]
        args: J,
    },
    Create,
    Delete,
    /// Fire the actor's alarm if it is due (internal: issued by alarm timers
    /// and the waker, never by the public API). Never creates the actor.
    Alarm,
}

/// HTTP status + JSON body, identical whether produced locally or by a peer.
#[derive(Debug, Clone, PartialEq)]
pub struct Outcome {
    pub status: u16,
    pub body: J,
}

impl Outcome {
    pub fn ok(v: J) -> Self {
        Self {
            status: 200,
            body: json!({ "result": v }),
        }
    }
    pub fn err(status: u16, code: &str, message: impl Into<String>) -> Self {
        Self {
            status,
            body: json!({ "error": { "code": code, "message": message.into() } }),
        }
    }
    fn unavailable(m: impl Into<String>) -> Self {
        Self::err(503, "unavailable", m)
    }

    pub fn rejected(error: HookError) -> Self {
        Self::err(error.status(), error.code(), error.to_string())
    }
}

type Slot = Arc<AsyncMutex<Option<Actor>>>;

pub struct Node {
    self_ref: Weak<Node>,
    pub cfg: NodeConfig,
    pub store: DynStore,
    pub runtime: Runtime,
    pub lease: Arc<Lease>,
    apps: RwLock<HashMap<String, Arc<AppCode>>>,
    deployed: Mutex<HashMap<String, deploy::Current>>,
    actors: Mutex<HashMap<ActorId, Slot>>,
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
            op: InvOp::Call {
                method: req.method,
                args: req.args,
            },
            chain: req.chain,
        };
        // Runs on the blocking thread executing the caller.
        match self.rt.block_on(tokio::time::timeout(
            req.timeout,
            node.invoke_with_metadata(inv, req.context),
        )) {
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
        Some(
            "unauthorized"
            | "forbidden"
            | "extension_invalid"
            | "extension_unavailable"
            | "extension_error"
            | "extension_timeout",
        ) => CallFailure::Rejected(msg),
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
    pub context: InvocationMetadata,
    pub hops: u32,
    pub ts_ms: u64,
}

async fn peer_secret(store: &DynStore) -> Result<Vec<u8>> {
    loop {
        if let Some((a, _)) = get_json::<PeerAuth>(&**store, PEER_AUTH).await? {
            return Ok(hex::decode(a.secret)?);
        }
        let a = PeerAuth {
            secret: hex::encode(rand::random::<[u8; 32]>()),
        };
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
    if s.starts_with('_') {
        s.to_string()
    } else {
        s.replace('_', "-")
    }
}

impl Node {
    /// Acquires the lease, starts renewing it, then loads deployments.
    pub(crate) async fn new(
        cfg: NodeConfig,
        advertise: String,
    ) -> Result<(Arc<Node>, tokio::task::JoinHandle<()>)> {
        let store = cfg.store.clone();
        let runtime = match &cfg.runtime {
            Some(runtime) => runtime.clone(),
            None => Runtime::shared()?,
        };
        let lease = Lease::acquire(store.clone(), &cfg.node_id, &advertise, cfg.lease_ttl).await?;
        let renew = tokio::spawn(lease.clone().run());
        let secret = match peer_secret(&store).await {
            Ok(s) => s,
            Err(e) => {
                renew.abort();
                return Err(e);
            }
        };
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()?;
        let rt = tokio::runtime::Handle::current();
        let node = Arc::new_cyclic(|me| Node {
            self_ref: me.clone(),
            cfg,
            store,
            runtime,
            lease,
            apps: Default::default(),
            deployed: Default::default(),
            actors: Default::default(),
            secret,
            client,
            caller: Arc::new(NodeCaller {
                node: me.clone(),
                rt,
            }),
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
            if self
                .deployed
                .lock()
                .unwrap()
                .get(&cur.app)
                .is_some_and(|d| d.id == cur.id)
            {
                continue;
            }
            let (wasm, manifest) = deploy::fetch(&self.store, &cur).await?;
            let rt = self.runtime.clone();
            let code = tokio::task::spawn_blocking(move || rt.load(&wasm, manifest))
                .await?
                .with_context(|| format!("load {}@{}", cur.app, cur.sha256))?;
            tracing::info!(
                app = cur.app,
                version = cur.version,
                sha = &cur.sha256[..12],
                "loaded deployment"
            );
            self.apps.write().unwrap().insert(cur.app.clone(), code);
            self.deployed.lock().unwrap().insert(cur.app.clone(), cur);
        }
        Ok(())
    }

    fn slot(&self, id: &ActorId) -> Slot {
        self.actors
            .lock()
            .unwrap()
            .entry(id.clone())
            .or_default()
            .clone()
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
    pub async fn invoke(&self, inv: Invocation, hops: u32) -> Outcome {
        let caller = if matches!(inv.op, InvOp::Alarm) {
            Caller::System {
                name: "alarm".into(),
            }
        } else {
            Caller::Embedded
        };
        let context = match self.invocation_context(&inv, caller) {
            Ok(context) => context,
            Err(error) => return Outcome::rejected(error),
        };
        self.invoke_with_context(inv, context, hops).await
    }

    pub fn invocation_context(
        &self,
        inv: &Invocation,
        caller: Caller,
    ) -> std::result::Result<InvocationContext, HookError> {
        let (target, operation) = invocation_parts(inv);
        InvocationContext::new(target, operation, caller, self.cfg.lease_ttl * 2)
    }

    /// Trusted embedding API. Metadata is never taken from public request
    /// bodies or identity headers by the framework.
    pub async fn invoke_with_metadata(
        &self,
        inv: Invocation,
        metadata: InvocationMetadata,
    ) -> Outcome {
        let (target, operation) = invocation_parts(&inv);
        let context = match InvocationContext::from_metadata(target, operation, metadata) {
            Ok(context) => context,
            Err(error) => return Outcome::rejected(error),
        };
        self.invoke_with_context(inv, context, 0).await
    }

    pub async fn invoke_with_context(
        &self,
        mut inv: Invocation,
        mut context: InvocationContext,
        hops: u32,
    ) -> Outcome {
        inv.ty = norm(&inv.ty);
        if let InvOp::Call { method, .. } = &mut inv.op {
            *method = norm(method);
        }
        (context.target, context.operation) = invocation_parts(&inv);
        match self.admit(&mut context).await {
            Ok(()) => self.invoke_routed(&inv, &context, hops, true).await,
            Err(error) => {
                let outcome = Outcome::rejected(error);
                self.completed(context, outcome.clone());
                outcome
            }
        }
    }

    async fn admit(&self, context: &mut InvocationContext) -> std::result::Result<(), HookError> {
        let target = context.target.clone();
        let operation = context.operation.clone();
        let metadata = context.request.clone();
        for extension in &self.cfg.extensions {
            context.check_deadline()?;
            let budget = self.cfg.extension_timeout.min(context.remaining());
            run_hook(
                extension.admit(context),
                extension.name(),
                "admission",
                budget,
            )
            .await?;
            if context.target != target
                || context.operation != operation
                || context.request.caller != metadata.caller
                || context.request.request_id != metadata.request_id
                || context.request.parent_request_id != metadata.parent_request_id
                || context.request.deadline_unix_ms != metadata.deadline_unix_ms
            {
                context.target = target.clone();
                context.operation = operation.clone();
                context.request.caller = metadata.caller.clone();
                context.request.request_id = metadata.request_id.clone();
                context.request.parent_request_id = metadata.parent_request_id.clone();
                context.request.deadline_unix_ms = metadata.deadline_unix_ms;
                return Err(HookError::Internal(format!(
                    "extension {} changed immutable invocation context",
                    extension.name(),
                )));
            }
        }
        context.check_deadline()
    }

    async fn authorize_execution(
        &self,
        context: &InvocationContext,
    ) -> std::result::Result<(), HookError> {
        for extension in &self.cfg.extensions {
            context.check_deadline()?;
            run_hook(
                extension.before_execute(context),
                extension.name(),
                "before-execute",
                self.cfg.extension_timeout.min(context.remaining()),
            )
            .await?;
            if !self.lease.valid() {
                return Err(HookError::Unavailable(
                    "node lease expired during extension".into(),
                ));
            }
        }
        context.check_deadline()
    }

    fn completed(&self, context: InvocationContext, outcome: Outcome) {
        if self.cfg.extensions.is_empty() {
            return;
        }
        let extensions = self.cfg.extensions.clone();
        let budget = self.cfg.extension_timeout;
        tokio::spawn(async move {
            for extension in extensions {
                let _ = run_hook(
                    extension.completed(&context, &outcome),
                    extension.name(),
                    "completed",
                    budget,
                )
                .await;
            }
        });
    }

    async fn lifecycle(&self, id: &ActorId, epoch: u64, kind: LifecycleKind) {
        observe_lifecycle(
            &self.cfg.extensions,
            self.cfg.extension_timeout,
            &LifecycleEvent {
                actor: actor_ref(id),
                epoch,
                node_id: self.cfg.node_id.clone(),
                kind,
            },
        )
        .await;
    }

    async fn discard(&self, guard: &mut Option<Actor>, id: &ActorId) {
        let epoch = guard.take().map(|actor| actor.epoch);
        self.set_timer(id, None);
        if let Some(epoch) = epoch {
            let kind = if self.lease.valid() {
                LifecycleKind::Discarded
            } else {
                LifecycleKind::Fenced
            };
            self.lifecycle(id, epoch, kind).await;
        }
    }

    async fn invoke_routed(
        &self,
        inv: &Invocation,
        context: &InvocationContext,
        hops: u32,
        complete: bool,
    ) -> Outcome {
        let Some(node) = self.self_ref.upgrade() else {
            return Outcome::unavailable("node is shutting down");
        };
        let inv = inv.clone();
        let context = context.clone();
        // Once routing starts, caller cancellation must not abandon a local
        // transaction between commit, capture, and durable upload.
        match tokio::spawn(async move {
            let outcome = node.route_loop(&inv, &context, hops).await;
            if complete {
                node.completed(context, outcome.clone());
            }
            outcome
        })
        .await
        {
            Ok(outcome) => outcome,
            Err(error) => Outcome::err(500, "internal", format!("invocation task failed: {error}")),
        }
    }

    async fn route_loop(
        &self,
        inv: &Invocation,
        context: &InvocationContext,
        hops: u32,
    ) -> Outcome {
        let deadline = Instant::now() + (self.cfg.lease_ttl * 2).min(context.remaining());
        let mut execution_checked = false;
        loop {
            if context.remaining().is_zero() {
                return Outcome::unavailable("routing deadline expired; outcome may be unknown");
            }
            match self
                .try_invoke(inv, context, &mut execution_checked, hops)
                .await
            {
                Retry::Done(o) => return o,
                Retry::Again(why) if Instant::now() < deadline => {
                    tracing::debug!("retrying {}: {why}", inv.key);
                    tokio::time::sleep(Duration::from_millis(250).min(context.remaining())).await;
                }
                Retry::Again(why) => return Outcome::unavailable(why),
            }
        }
    }

    async fn try_invoke(
        &self,
        inv: &Invocation,
        context: &InvocationContext,
        execution_checked: &mut bool,
        hops: u32,
    ) -> Retry {
        use Retry::*;
        if !self.lease.valid() {
            return Done(Outcome::unavailable(
                "node is fenced or its lease is not current",
            ));
        }
        let Some(code) = self.app(&inv.app) else {
            return Done(Outcome::err(
                404,
                "not_found",
                format!("app {:?} is not deployed", inv.app),
            ));
        };
        if let InvOp::Call { method, .. } = &inv.op {
            if let Err(e) = resolve_method(&code.manifest, &inv.ty, method) {
                return Done(Outcome::err(404, "not_found", e.to_string()));
            }
        } else if code.manifest.actor_type(&inv.ty).is_none() {
            return Done(Outcome::err(
                404,
                "not_found",
                format!("app {} has no actor type {:?}", inv.app, inv.ty),
            ));
        }
        if inv.key.is_empty() || inv.key.len() > MAX_KEY_LEN {
            return Done(Outcome::err(
                400,
                "bad_request",
                format!("actor key must be 1..={MAX_KEY_LEN} bytes"),
            ));
        }
        let id = ActorId {
            app: inv.app.clone(),
            ty: inv.ty.clone(),
            key: inv.key.clone(),
        };
        // The actors on the chain hold their slot locks while waiting for this
        // call, so calling back into one of them would deadlock.
        if inv
            .chain
            .iter()
            .any(|a| a.app == id.app && a.actor_type == id.ty && a.key == id.key)
        {
            let path: Vec<String> = inv.chain.iter().map(|a| a.to_string()).collect();
            return Done(Outcome::err(
                508,
                "cycle",
                format!("call cycle: {} -> {id}", path.join(" -> ")),
            ));
        }
        if inv.chain.len() >= MAX_CALL_DEPTH {
            return Done(Outcome::err(
                508,
                "cycle",
                format!("call chain deeper than {MAX_CALL_DEPTH}"),
            ));
        }
        let slot = self.slot(&id);
        let mut guard =
            match tokio::time::timeout(context.remaining(), slot.clone().lock_owned()).await {
                Ok(guard) => guard,
                Err(_) => return Done(Outcome::rejected(HookError::Timeout)),
            };
        let mut created = false;
        if guard.is_none() {
            if matches!(inv.op, InvOp::Delete | InvOp::Alarm) {
                match owner::read(&self.store, &id).await {
                    Ok(Some((r, _))) if r.state != OwnerState::Deleted => {}
                    Ok(_) => {
                        drop(guard);
                        self.drop_slot_if_empty(&id);
                        // `gone` tells the waker the actor no longer exists.
                        let code = if matches!(inv.op, InvOp::Alarm) {
                            "gone"
                        } else {
                            "not_found"
                        };
                        return Done(Outcome::err(
                            404,
                            code,
                            format!("actor {id} does not exist"),
                        ));
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
                    return Done(Outcome::err(
                        409,
                        "conflict",
                        format!("actor {id} already exists"),
                    ));
                }
                Ok(Acquire::Remote(peer)) => {
                    drop(guard);
                    self.drop_slot_if_empty(&id);
                    if hops >= MAX_HOPS {
                        return Done(Outcome::unavailable("too many forwarding hops"));
                    }
                    return match self.forward(&peer.advertise, inv, context, hops + 1).await {
                        Ok(o) => Done(o),
                        Err(e) => Again(format!("owner {} unreachable: {e}", peer.node_id)),
                    };
                }
                Ok(Acquire::Acquired { epoch, etag, fresh }) => {
                    if !*execution_checked {
                        if let Err(error) = self.authorize_execution(context).await {
                            if let Err(release_error) = owner::release(
                                &self.store,
                                &id,
                                &self.me(),
                                epoch,
                                &etag,
                                if fresh {
                                    OwnerState::Deleted
                                } else {
                                    OwnerState::Unowned
                                },
                            )
                            .await
                            {
                                tracing::warn!(actor = %id, %release_error, "release rejected activation failed");
                            }
                            drop(guard);
                            self.drop_slot_if_empty(&id);
                            return Done(Outcome::rejected(error));
                        }
                        *execution_checked = true;
                    }
                    if !self.lease.valid() {
                        return Done(Outcome::unavailable("lease expired before activation"));
                    }
                    match actor::activate(
                        &self.store,
                        &self.cfg.data_dir,
                        &id,
                        epoch,
                        etag,
                        fresh,
                        code.clone(),
                        self.cfg.database_factory.clone(),
                    )
                    .await
                    {
                        Ok(mut c) => {
                            c.caller = Some(self.caller.clone());
                            tracing::info!(actor = %id, epoch, "activated");
                            self.set_timer(&id, c.alarm.map(|a| a.at_ms));
                            *guard = Some(c);
                            created = true;
                            self.lifecycle(&id, epoch, LifecycleKind::Activated).await;
                        }
                        Err(e) => return Again(format!("activate {id}: {e:#}")),
                    }
                }
            }
        } else if matches!(inv.op, InvOp::Create) {
            return Done(Outcome::err(
                409,
                "conflict",
                format!("actor {id} already exists"),
            ));
        }

        if !*execution_checked {
            if let Err(error) = self.authorize_execution(context).await {
                if !self.lease.valid() {
                    self.discard(&mut guard, &id).await;
                }
                return Done(Outcome::rejected(error));
            }
            *execution_checked = true;
        }
        if !self.lease.valid() {
            self.discard(&mut guard, &id).await;
            return Done(Outcome::unavailable("lease expired before execution"));
        }
        if let Err(error) = context.check_deadline() {
            return Done(Outcome::rejected(error));
        }
        let op = match &inv.op {
            InvOp::Delete => return Done(self.delete(guard, &id).await),
            InvOp::Create => {
                debug_assert!(created);
                Op::Touch
            }
            InvOp::Call { method, args } => Op::Call {
                method: method.clone(),
                args: args.clone(),
                chain: inv.chain.clone(),
            },
            InvOp::Alarm => Op::Alarm { now_ms: now_ms() },
        };
        Done(
            self.run(
                guard,
                &id,
                code,
                op,
                matches!(inv.op, InvOp::Create),
                context,
            )
            .await,
        )
    }

    async fn run(
        &self,
        guard: tokio::sync::OwnedMutexGuard<Option<Actor>>,
        id: &ActorId,
        code: Arc<AppCode>,
        op: Op,
        is_create: bool,
        context: &InvocationContext,
    ) -> Outcome {
        let slot = tokio::sync::OwnedMutexGuard::mutex(&guard).clone();
        let extensions = self.cfg.extensions.clone();
        let context = context.clone();
        let lease = self.lease.clone();
        let execution = tokio::task::spawn_blocking(move || {
            let mut guard = guard;
            let res = if lease.valid() {
                Some(guard.as_mut().expect("resident").execute_with_context(
                    &code,
                    op,
                    &context,
                    &extensions,
                ))
            } else {
                None
            };
            (guard, res)
        })
        .await;
        let (mut guard, res) = match execution {
            Ok((guard, Some(res))) => (guard, res),
            Ok((mut guard, None)) => {
                self.discard(&mut guard, id).await;
                return Outcome::unavailable("lease expired before transaction");
            }
            Err(error) => {
                let mut guard = slot.lock_owned().await;
                self.discard(&mut guard, id).await;
                return Outcome::err(
                    500,
                    "internal",
                    format!("actor execution task failed: {error}"),
                );
            }
        };
        let executed = match res {
            Ok(x) => x,
            Err(e) => {
                self.discard(&mut guard, id).await;
                return Outcome::err(500, "internal", format!("{e:#}"));
            }
        };
        if executed.code_changed {
            let epoch = guard.as_ref().expect("resident").epoch;
            self.lifecycle(id, epoch, LifecycleKind::CodeReplaced).await;
        }
        // A new alarm's wake hint is written before the transaction becomes
        // durable, so a durable alarm always has one.
        if let Some(a) = executed.alarm.and_then(|c| c.after) {
            if let Err(e) = self
                .store
                .put(
                    &wake_key(id, &a),
                    to_json_bytes(&json!({ "at_ms": a.at_ms })),
                )
                .await
            {
                self.discard(&mut guard, id).await;
                return Outcome::unavailable(format!(
                    "write was not made durable ({e}); it did not apply"
                ));
            }
        }
        if let Some(seg) = executed.segment {
            // Actor is not Sync: copy what we need before awaiting.
            let (epoch, txid, snapshot_txid) = {
                let c = guard.as_ref().unwrap();
                (c.epoch, c.txid, c.snapshot_txid)
            };
            let key = format!("{}{}", id.epoch_prefix(epoch), segment_name(seg.txid));
            if let Err(e) = self.store.put(&key, Bytes::from(seg.data)).await {
                self.discard(&mut guard, id).await;
                return Outcome::unavailable(format!(
                    "write was not made durable ({e}); it may or may not have applied"
                ));
            }
            if let Some(outcome) = self.check_owner(&mut guard, id).await {
                return outcome;
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
                let node = self.self_ref.clone();
                tokio::spawn(async move {
                    let (mut guard, res) = tokio::task::spawn_blocking(move || {
                        let r = guard
                            .as_mut()
                            .map(|c| c.snapshot().map(|b| (c.epoch, c.txid, b)));
                        (guard, r)
                    })
                    .await
                    .expect("snapshot task");
                    match res {
                        // `guard` stays held until the upload is done, which
                        // keeps the snapshot file unchanged.
                        Some(Ok((epoch, txid, image))) => {
                            match actor::compact(&store, &id, epoch, txid, &image).await {
                                Ok(()) => {
                                    if let Some(c) = guard.as_mut() {
                                        c.snapshot_txid = txid;
                                    }
                                    tracing::debug!(actor = %id, txid, "compacted");
                                }
                                Err(e) => tracing::warn!(actor = %id, "compaction failed: {e:#}"),
                            }
                        }
                        Some(Err(e)) => {
                            tracing::warn!(actor = %id, "snapshot failed: {e:#}");
                            let epoch = guard.take().map(|actor| actor.epoch);
                            if let Some(node) = node.upgrade() {
                                node.set_timer(&id, None);
                                if let Some(epoch) = epoch {
                                    let kind = if node.lease.valid() {
                                        LifecycleKind::Discarded
                                    } else {
                                        LifecycleKind::Fenced
                                    };
                                    node.lifecycle(&id, epoch, kind).await;
                                }
                            }
                        }
                        None => {}
                    }
                });
            }
        } else if let Some(outcome) = self.check_owner(&mut guard, id).await {
            return outcome;
        }
        if is_create && matches!(&executed.outcome, Ok(output) if !output.is_err) {
            return Outcome {
                status: 201,
                body: json!({ "result": { "created": true } }),
            };
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
            Err(CallError::Rejected(error)) => Outcome::rejected(error),
        }
    }

    async fn check_owner(&self, guard: &mut Option<Actor>, id: &ActorId) -> Option<Outcome> {
        let epoch = guard.as_ref().expect("resident").epoch;
        let owned = self.lease.valid()
            && matches!(
                owner::still_owner(&self.store, id, &self.me(), epoch).await,
                Ok(true)
            )
            && self.lease.valid();
        if !owned {
            tracing::warn!(actor = %id, epoch, "lost ownership; not acknowledging");
            self.discard(guard, id).await;
            Some(Outcome::unavailable(
                "actor ownership moved; outcome not acknowledged",
            ))
        } else {
            None
        }
    }

    async fn delete(
        &self,
        mut guard: tokio::sync::OwnedMutexGuard<Option<Actor>>,
        id: &ActorId,
    ) -> Outcome {
        let actor = guard.take().expect("resident");
        let epoch = actor.epoch;
        self.set_timer(id, None);
        if !self.lease.valid() {
            drop(actor);
            self.lifecycle(id, epoch, LifecycleKind::Fenced).await;
            return Outcome::unavailable("lease not current");
        }
        match owner::release(
            &self.store,
            id,
            &self.me(),
            actor.epoch,
            &actor.owner_etag,
            OwnerState::Deleted,
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => {
                drop(actor);
                self.lifecycle(id, epoch, LifecycleKind::Discarded).await;
                return Outcome::unavailable("actor ownership moved; retry");
            }
            Err(e) => {
                drop(actor);
                self.lifecycle(id, epoch, LifecycleKind::Discarded).await;
                return Outcome::unavailable(format!("delete failed: {e}"));
            }
        }
        if let Some(a) = actor.alarm {
            let _ = self.store.delete(&wake_key(id, &a)).await;
        }
        drop(actor);
        self.lifecycle(id, epoch, LifecycleKind::Deleted).await;
        let prefix = id.ltx_prefix();
        if let Ok(keys) = self.store.list(&prefix).await {
            for k in keys {
                let _ = self.store.delete(&k).await;
            }
        }
        drop(guard);
        self.drop_slot_if_empty(id);
        tracing::info!(actor = %id, "deleted");
        if self.lease.valid() {
            Outcome::ok(json!({ "deleted": true }))
        } else {
            Outcome::unavailable("lease expired after deletion; outcome not acknowledged")
        }
    }

    async fn forward(
        &self,
        base: &str,
        inv: &Invocation,
        context: &InvocationContext,
        hops: u32,
    ) -> Result<Outcome> {
        let body = serde_json::to_vec(&Forwarded {
            invocation: inv.clone(),
            context: context.request.clone(),
            hops,
            ts_ms: now_ms(),
        })?;
        let sig = sign(&self.secret, &body);
        let resp = self
            .client
            .post(format!("{}/internal/v1/invoke", base.trim_end_matches('/')))
            .header(SIGNATURE_HEADER, sig)
            .header("content-type", "application/json")
            .body(body)
            .timeout(context.remaining())
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
        let mut inv = f.invocation;
        inv.ty = norm(&inv.ty);
        if let InvOp::Call { method, .. } = &mut inv.op {
            *method = norm(method);
        }
        let (target, operation) = invocation_parts(&inv);
        let context = match InvocationContext::from_metadata(target, operation, f.context) {
            Ok(context) => context,
            Err(error) => return Outcome::rejected(error),
        };
        // Admission already ran at the trusted entry node. The actual owner
        // still runs its own execution and transaction checks.
        self.invoke_routed(&inv, &context, f.hops, false).await
    }

    /// Releases actors idle for longer than the idle timeout.
    pub async fn evict_idle(&self, idle: Duration) {
        self.release_idle(idle, LifecycleKind::Evicted).await;
    }

    async fn release_idle(&self, idle: Duration, kind: LifecycleKind) {
        let slots: Vec<(ActorId, Slot)> = self
            .actors
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (id, slot) in slots {
            let Ok(mut g) = slot.clone().try_lock_owned() else {
                continue;
            };
            if g.as_ref().is_some_and(|c| c.last_used.elapsed() >= idle) {
                let c = g.take().unwrap();
                // The waker fires its alarm from now on.
                self.set_timer(&id, None);
                match owner::release(
                    &self.store,
                    &id,
                    &self.me(),
                    c.epoch,
                    &c.owner_etag,
                    OwnerState::Unowned,
                )
                .await
                {
                    Ok(_) => tracing::info!(actor = %id, "released"),
                    Err(e) => tracing::warn!(actor = %id, "release failed: {e}"),
                }
                let epoch = c.epoch;
                drop(c);
                self.lifecycle(&id, epoch, kind).await;
            }
            drop(g);
            drop(slot);
            self.drop_slot_if_empty(&id);
        }
    }

    /// Releases every actor (graceful shutdown).
    pub async fn release_all(&self) {
        self.release_idle(Duration::ZERO, LifecycleKind::Shutdown)
            .await;
    }

    /// Drops all resident actors without touching the store (after fencing).
    pub fn drop_all(&self) {
        self.timers.lock().unwrap().clear();
        let slots: Vec<(ActorId, Slot)> = self.actors.lock().unwrap().drain().collect();
        let mut events = Vec::new();
        for (id, s) in slots {
            if let Ok(mut g) = s.try_lock() {
                if let Some(actor) = g.take() {
                    events.push(LifecycleEvent {
                        actor: actor_ref(&id),
                        epoch: actor.epoch,
                        node_id: self.cfg.node_id.clone(),
                        kind: LifecycleKind::Fenced,
                    });
                }
            }
        }
        if !events.is_empty() && !self.cfg.extensions.is_empty() {
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    let extensions = self.cfg.extensions.clone();
                    let budget = self.cfg.extension_timeout;
                    handle.spawn(async move {
                        for event in events {
                            observe_lifecycle(&extensions, budget, &event).await;
                        }
                    });
                }
                Err(error) => {
                    tracing::warn!(%error, "cannot deliver fencing lifecycle events without a runtime")
                }
            }
        }
    }

    /// Lists actors of an app from ownership records.
    pub async fn list_actors(
        &self,
        app: &str,
        ty: Option<&str>,
        limit: usize,
    ) -> Result<Vec<OwnerRecord>> {
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

fn actor_ref(id: &ActorId) -> ActorRef {
    ActorRef {
        app: id.app.clone(),
        actor_type: id.ty.clone(),
        key: id.key.clone(),
    }
}

fn invocation_parts(inv: &Invocation) -> (ActorRef, InvocationOperation) {
    let operation = match &inv.op {
        InvOp::Call { method, args } => InvocationOperation::Call {
            method: norm(method),
            args: args.clone(),
        },
        InvOp::Create => InvocationOperation::Create,
        InvOp::Delete => InvocationOperation::Delete,
        InvOp::Alarm => InvocationOperation::Alarm,
    };
    (
        ActorRef {
            app: inv.app.clone(),
            actor_type: norm(&inv.ty),
            key: inv.key.clone(),
        },
        operation,
    )
}
