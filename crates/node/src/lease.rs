//! Node leases. Every node holds `nodes/<id>.json`, renewed with If-Match
//! every TTL/3. A node that cannot renew before its local deadline fences
//! itself: it stops acknowledging writes and drops all actors.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use statex_store::{get_json, to_json_bytes, DynStore, StoreError};
use tokio::sync::watch;

use crate::layout::{node_key, now_ms};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub node_id: String,
    /// Random per process start; distinguishes restarts of the same node id.
    pub session: String,
    /// Base URL peers use to forward calls (internal API).
    pub advertise: String,
    pub expires_at_ms: u64,
    pub started_at_ms: u64,
}

impl NodeRecord {
    pub fn alive(&self) -> bool {
        self.expires_at_ms > now_ms()
    }
}

pub struct Lease {
    store: DynStore,
    record: Mutex<NodeRecord>,
    etag: Mutex<String>,
    ttl: Duration,
    /// Local deadline (unix ms) after which this node must not acknowledge.
    valid_until: AtomicU64,
    fenced: AtomicBool,
    paused: AtomicBool,
    fence_tx: watch::Sender<Option<String>>,
}

impl Lease {
    /// Acquires the lease for `node_id`, waiting out a previous holder.
    pub async fn acquire(store: DynStore, node_id: &str, advertise: &str, ttl: Duration) -> Result<Arc<Lease>> {
        let session = hex::encode(rand::random::<[u8; 8]>());
        let key = node_key(node_id);
        loop {
            let start = now_ms();
            let rec = NodeRecord {
                node_id: node_id.into(),
                session: session.clone(),
                advertise: advertise.into(),
                expires_at_ms: start + ttl.as_millis() as u64,
                started_at_ms: start,
            };
            let res = match get_json::<NodeRecord>(&*store, &key).await? {
                None => store.put_if_absent(&key, to_json_bytes(&rec)).await,
                Some((old, etag)) => {
                    if old.alive() {
                        let wait = old.expires_at_ms.saturating_sub(now_ms()).min(1000);
                        tracing::warn!(node = node_id, "lease held by session {}; waiting {wait} ms", old.session);
                        tokio::time::sleep(Duration::from_millis(wait.max(50))).await;
                        continue;
                    }
                    store.put_if_match(&key, to_json_bytes(&rec), &etag).await
                }
            };
            match res {
                Ok(etag) => {
                    let (fence_tx, _) = watch::channel(None);
                    tracing::info!(node = node_id, session, "acquired node lease");
                    return Ok(Arc::new(Lease {
                        store,
                        valid_until: AtomicU64::new(rec.expires_at_ms),
                        record: Mutex::new(rec),
                        etag: Mutex::new(etag),
                        ttl,
                        fenced: AtomicBool::new(false),
                        paused: AtomicBool::new(false),
                        fence_tx,
                    }));
                }
                Err(StoreError::Precondition) => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    pub fn record(&self) -> NodeRecord {
        self.record.lock().unwrap().clone()
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    fn margin_ms(&self) -> u64 {
        (self.ttl.as_millis() as u64 / 5).max(200)
    }

    /// True while this node may acknowledge writes.
    pub fn valid(&self) -> bool {
        !self.fenced.load(Ordering::SeqCst)
            && now_ms() + self.margin_ms() < self.valid_until.load(Ordering::SeqCst)
    }

    pub fn fenced(&self) -> bool {
        self.fenced.load(Ordering::SeqCst)
    }

    pub fn subscribe(&self) -> watch::Receiver<Option<String>> {
        self.fence_tx.subscribe()
    }

    pub fn fence(&self, reason: &str) {
        if !self.fenced.swap(true, Ordering::SeqCst) {
            tracing::error!(node = self.record().node_id, "SELF-FENCE: {reason}");
            self.fence_tx.send_replace(Some(reason.to_string()));
        }
    }

    /// Test hook: stop renewing (simulates a partition or a stalled process).
    pub fn pause_renewal(&self, paused: bool) {
        self.paused.store(paused, Ordering::SeqCst);
    }

    async fn renew_once(&self) -> Result<(), StoreError> {
        let start = now_ms();
        let mut rec = self.record();
        rec.expires_at_ms = start + self.ttl.as_millis() as u64;
        let etag = self.etag.lock().unwrap().clone();
        let new = self.store.put_if_match(&node_key(&rec.node_id), to_json_bytes(&rec), &etag).await?;
        *self.etag.lock().unwrap() = new;
        self.valid_until.store(rec.expires_at_ms, Ordering::SeqCst);
        *self.record.lock().unwrap() = rec;
        Ok(())
    }

    /// Renewal loop; returns when fenced.
    pub async fn run(self: Arc<Self>) {
        let period = self.ttl / 3;
        loop {
            tokio::time::sleep(if self.valid() { period } else { Duration::from_millis(100) }).await;
            if self.fenced() {
                return;
            }
            if !self.paused.load(Ordering::SeqCst) {
                match self.renew_once().await {
                    Ok(()) => {}
                    Err(StoreError::Precondition) => {
                        self.fence("lease record changed by someone else");
                        return;
                    }
                    Err(e) => tracing::warn!("lease renewal failed: {e}"),
                }
            }
            if now_ms() + self.margin_ms() >= self.valid_until.load(Ordering::SeqCst) {
                self.fence("could not renew lease before it expired");
                return;
            }
        }
    }

    /// Graceful release: marks the lease expired so peers can take over now.
    pub async fn release(&self) {
        self.fenced.store(true, Ordering::SeqCst);
        let mut rec = self.record();
        rec.expires_at_ms = 0;
        let etag = self.etag.lock().unwrap().clone();
        if let Err(e) = self.store.put_if_match(&node_key(&rec.node_id), to_json_bytes(&rec), &etag).await {
            tracing::warn!("lease release failed: {e}");
        }
    }
}

/// Reads another node's lease.
pub async fn read_node(store: &DynStore, node: &str) -> Result<Option<NodeRecord>> {
    Ok(get_json::<NodeRecord>(&**store, &node_key(node)).await?.map(|(r, _)| r))
}
