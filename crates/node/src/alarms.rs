//! Alarm delivery.
//!
//! The alarm row in an actor's database is the truth; this module only makes
//! sure the actor gets invoked (`InvOp::Alarm`) once it is due:
//!
//! - **Timers**: a node keeps an in-memory timer for the alarm of every
//!   resident actor and fires it on time.
//! - **Waker**: one node at a time (holder of the `fleet/waker.json` lease)
//!   scans the wake hints in the object store for due alarms and invokes those
//!   actors through the normal call path, which forwards to a live owner or
//!   activates the actor. This covers actors that are not resident anywhere
//!   (evicted, or their node died).
//!
//! Firing twice is harmless: invocations of one actor are serialized and the
//! actor only runs its handler if its own row says the alarm is due.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use statex_store::{get_json, to_json_bytes};
use tokio::task::JoinSet;

use crate::layout::{now_ms, parse_wake, wake_minute_prefix, ActorId, WakeEntry, WAKER, WAKE_PREFIX};
use crate::node::{InvOp, Invocation, Node};

/// Waker invocations in flight at once.
const WAKE_CONCURRENCY: usize = 16;

#[derive(Debug, Serialize, Deserialize)]
struct WakerLease {
    node: String,
    session: String,
    expires_at_ms: u64,
}

impl Node {
    /// Sets (or with `None` removes) the timer for a resident actor's alarm.
    pub(crate) fn set_timer(&self, id: &ActorId, at_ms: Option<u64>) {
        let mut t = self.timers.lock().unwrap();
        let changed = match at_ms {
            Some(at) => t.insert(id.clone(), at) != Some(at),
            None => t.remove(id).is_some(),
        };
        drop(t);
        if changed {
            self.timers_changed.notify_one();
        }
    }

    /// Fires the alarms of resident actors when they are due.
    pub(crate) async fn run_timers(self: Arc<Self>) {
        loop {
            let next = self.timers.lock().unwrap().values().min().copied();
            match next {
                None => self.timers_changed.notified().await,
                Some(at) => {
                    let wait = Duration::from_millis(at.saturating_sub(now_ms()));
                    if !wait.is_zero() {
                        tokio::select! {
                            _ = tokio::time::sleep(wait) => {}
                            _ = self.timers_changed.notified() => continue,
                        }
                    }
                }
            }
            let now = now_ms();
            let due: Vec<ActorId> = {
                let mut t = self.timers.lock().unwrap();
                let due: Vec<ActorId> = t.iter().filter(|(_, at)| **at <= now).map(|(id, _)| id.clone()).collect();
                for id in &due {
                    t.remove(id);
                }
                due
            };
            for id in due {
                let node = self.clone();
                tokio::spawn(async move {
                    let o = node.invoke(alarm_invocation(&id), 0).await;
                    if o.status != 200 {
                        // The waker retries from the wake hint.
                        tracing::warn!(actor = %id, status = o.status, "alarm invocation failed: {}", o.body);
                    }
                });
            }
        }
    }

    /// Scans the wake hints while this node holds the waker lease.
    pub(crate) async fn run_waker(self: Arc<Self>) {
        let mut cursor: Option<u64> = None;
        let mut last_full = Instant::now();
        loop {
            tokio::time::sleep(self.cfg.wake_tick).await;
            if !self.lease.valid() {
                cursor = None;
                continue;
            }
            match self.hold_waker().await {
                Ok(true) => {}
                Ok(false) => {
                    cursor = None;
                    continue;
                }
                Err(e) => {
                    tracing::warn!("waker lease: {e:#}");
                    cursor = None;
                    continue;
                }
            }
            let now = now_ms();
            let minute = now / 60_000;
            // Normally only the current minute (and any just passed) is
            // listed; a full scan picks up hints left behind by failed
            // attempts and the backlog after taking over the waker role.
            let keys = match cursor {
                Some(c) if last_full.elapsed() < self.cfg.wake_full_scan => {
                    let mut keys = Vec::new();
                    for m in c..=minute {
                        match self.store.list(&wake_minute_prefix(m)).await {
                            Ok(k) => keys.extend(k),
                            Err(e) => tracing::warn!("list wake hints: {e}"),
                        }
                    }
                    keys
                }
                _ => {
                    last_full = Instant::now();
                    match self.store.list(WAKE_PREFIX).await {
                        Ok(k) => k,
                        Err(e) => {
                            tracing::warn!("list wake hints: {e}");
                            continue;
                        }
                    }
                }
            };
            cursor = Some(minute);
            let due: Vec<(String, WakeEntry)> = keys
                .into_iter()
                .filter_map(|k| parse_wake(&k).map(|e| (k, e)))
                .filter(|(_, e)| e.at_ms <= now)
                .collect();
            let mut set = JoinSet::new();
            for (key, entry) in due {
                while set.len() >= WAKE_CONCURRENCY {
                    set.join_next().await;
                }
                let node = self.clone();
                set.spawn(async move { node.wake(key, entry).await });
            }
            while set.join_next().await.is_some() {}
        }
    }

    /// Invokes the actor named by a due wake hint and deletes the hint once
    /// it no longer names the actor's scheduled alarm.
    async fn wake(&self, key: String, e: WakeEntry) {
        // Keep hints of apps this node has not loaded (yet).
        if self.app(&e.id.app).is_none_or(|c| c.manifest.actor_type(&e.id.ty).is_none()) {
            return;
        }
        let o = self.invoke(alarm_invocation(&e.id), 0).await;
        let stale = match o.status {
            200 => o.body["result"]["armed"].as_str() != Some(e.name.as_str()),
            404 => o.body["error"]["code"] == "gone",
            _ => {
                tracing::warn!(actor = %e.id, status = o.status, "alarm invocation failed: {}", o.body);
                false
            }
        };
        if stale {
            if let Err(err) = self.store.delete(&key).await {
                tracing::debug!("delete wake hint {key}: {err}");
            }
        }
    }

    /// Acquires or renews the waker lease. Returns whether this node holds it.
    pub(crate) async fn hold_waker(&self) -> anyhow::Result<bool> {
        let me = self.me();
        let now = now_ms();
        let ttl = self.cfg.lease_ttl.as_millis() as u64;
        let mine = WakerLease { node: me.node_id.clone(), session: me.session.clone(), expires_at_ms: now + ttl };
        match get_json::<WakerLease>(&*self.store, WAKER).await? {
            None => Ok(self.store.put_if_absent(WAKER, to_json_bytes(&mine)).await.is_ok()),
            Some((cur, etag)) => {
                let ours = cur.node == me.node_id && cur.session == me.session;
                if ours && cur.expires_at_ms > now + ttl / 2 {
                    return Ok(true);
                }
                if !ours && cur.expires_at_ms > now {
                    return Ok(false);
                }
                if !ours {
                    tracing::info!(node = me.node_id, "taking over the waker role from {}", cur.node);
                }
                Ok(self.store.put_if_match(WAKER, to_json_bytes(&mine), &etag).await.is_ok())
            }
        }
    }
}

fn alarm_invocation(id: &ActorId) -> Invocation {
    Invocation { app: id.app.clone(), ty: id.ty.clone(), key: id.key.clone(), op: InvOp::Alarm, chain: vec![] }
}
