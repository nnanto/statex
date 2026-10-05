//! Durable asynchronous dispatch. The sender lock is released after claiming
//! a task and before invoking its target, allowing a consumer to call back.

use std::sync::Arc;

use anyhow::Result;
use serde_json::Value as J;
use statex_runtime::spawn::Pending;
use statex_store::to_json_bytes;
use tokio::task::JoinSet;

use crate::layout::{dec, enc, ActorId};
use crate::node::{InvOp, Invocation, Node};

const PREFIX: &str = "outbox/";
const CONCURRENCY: usize = 16;

pub(crate) fn hint_key(source: &ActorId, id: &str) -> String {
    format!("{PREFIX}{}/{}/{}/{}", enc(&source.app), enc(&source.ty), enc(&source.key), enc(id))
}

fn parse_hint(key: &str) -> Option<(ActorId, String)> {
    let mut parts = key.strip_prefix(PREFIX)?.split('/');
    let source = ActorId { app: dec(parts.next()?)?, ty: dec(parts.next()?)?, key: dec(parts.next()?)? };
    let id = dec(parts.next()?)?;
    if parts.next().is_some() { return None; }
    Some((source, id))
}

impl Node {
    pub(crate) async fn write_outbox_hints(&self, source: &ActorId, pending: &[Pending]) -> Result<()> {
        for task in pending {
            self.store.put(&hint_key(source, &task.id), to_json_bytes(&serde_json::json!({}))).await?;
        }
        Ok(())
    }

    pub(crate) async fn run_outbox(self: Arc<Self>) {
        loop {
            tokio::time::sleep(self.cfg.wake_tick).await;
            if !self.lease.valid() { continue; }
            match self.hold_waker().await {
                Ok(true) => {}
                Ok(false) => continue,
                Err(e) => {
                    tracing::warn!("outbox waker lease: {e:#}");
                    continue;
                }
            }
            let keys = match self.store.list(PREFIX).await {
                Ok(keys) => keys,
                Err(e) => {
                    tracing::warn!("list outbox hints: {e}");
                    continue;
                }
            };
            let mut work = JoinSet::new();
            for key in keys {
                let Some((source, id)) = parse_hint(&key) else {
                    tracing::warn!(key, "invalid outbox hint");
                    continue;
                };
                while work.len() >= CONCURRENCY {
                    if let Some(Err(e)) = work.join_next().await {
                        tracing::error!("outbox dispatcher failed: {e}");
                    }
                }
                let node = self.clone();
                work.spawn(async move {
                    if let Err(e) = node.dispatch(&key, source, id).await {
                        tracing::warn!(key, "spawn delivery: {e:#}");
                    }
                });
            }
            while let Some(result) = work.join_next().await {
                if let Err(e) = result {
                    tracing::error!("outbox dispatcher failed: {e}");
                }
            }
        }
    }

    async fn dispatch(&self, key: &str, source: ActorId, id: String) -> Result<()> {
        let invocation = |op| Invocation {
            app: source.app.clone(), ty: source.ty.clone(), key: source.key.clone(), op, chain: Vec::new(),
        };
        let polled = self.invoke(invocation(InvOp::OutboxPoll { id: id.clone() }), 0).await;
        if polled.status == 404 && polled.body["error"]["code"] == "gone" {
            self.store.delete(key).await?;
            return Ok(());
        }
        if polled.status != 200 {
            anyhow::bail!("claim failed: {} {}", polled.status, polled.body);
        }
        if polled.body["result"]["pending"] == false {
            self.store.delete(key).await?;
            return Ok(());
        }
        let task = &polled.body["result"]["task"];
        if task.is_null() { return Ok(()); }
        let task: Pending = serde_json::from_value(task.clone())?;
        let args: J = serde_json::from_str(&task.args_json)?;
        // Source address is part of the id: two senders may have the same
        // epoch and sequence. The tuple encoding is unambiguous.
        let delivery_id = serde_json::to_string(&(&source.app, &source.ty, &source.key, &task.id))?;
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(statex_runtime::spawn::DELIVERY_LEASE_MS / 2),
            self.invoke(Invocation {
                app: task.app, ty: task.actor_type, key: task.key,
                op: InvOp::Deliver { method: task.method, args, delivery_id },
                chain: Vec::new(),
            }, 0),
        ).await;
        let error = match result {
            Ok(outcome) if (200..300).contains(&outcome.status) => None,
            Ok(outcome) => Some(format!("{} {}", outcome.status, outcome.body)),
            Err(_) => Some("spawn delivery timed out; outcome is unknown".into()),
        };
        if let Some(error) = &error {
            tracing::warn!(actor = %source, task = id, attempt = task.attempts, error, "spawn will retry");
        }
        let settled = self.invoke(invocation(InvOp::OutboxSettle { id, attempt: task.attempts, error }), 0).await;
        if settled.status != 200 {
            anyhow::bail!("settlement failed: {} {}", settled.status, settled.body);
        }
        if settled.body["result"]["pending"] == false {
            self.store.delete(key).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hint_roundtrip() {
        let source = ActorId { app: "team/queue".into(), ty: "queue".into(), key: "jobs/a.b".into() };
        let key = hint_key(&source, "e1-2");
        assert_eq!(parse_hint(&key), Some((source, "e1-2".into())));
        assert!(parse_hint(&(key + "/extra")).is_none());
    }
}
