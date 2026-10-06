//! At-least-once delivery from committed source transactions. Discovery
//! markers are permanent: deleting an idle marker could race a new enqueue.

use std::sync::Arc;
use std::time::Duration;

use statex_runtime::invocation::{Caller, InvocationMetadata};
use statex_runtime::outbox::{Job, JobStatus};
use tokio::task::JoinSet;

use crate::layout::{app_dir, dec, enc, now_ms, ActorId};
use crate::node::{InvOp, Invocation, Node, Outcome};

const PREFIX: &str = "outbox/";
const CONCURRENCY: usize = 16;
pub(crate) const MAX_ATTEMPTS: u32 = 8;

pub(crate) fn marker_key(id: &ActorId) -> String {
    format!(
        "{PREFIX}{}/{}/{}",
        app_dir(&id.app),
        enc(&id.ty),
        enc(&id.key)
    )
}

fn parse_marker(key: &str) -> Option<ActorId> {
    let mut parts = key.strip_prefix(PREFIX)?.split('/');
    let id = ActorId {
        app: parts.next()?.replace('.', "/"),
        ty: dec(parts.next()?)?,
        key: dec(parts.next()?)?,
    };
    if parts.next().is_some() || id.app.is_empty() || id.ty.is_empty() || id.key.is_empty() {
        return None;
    }
    Some(id)
}

pub(crate) fn backoff_ms(attempts: u32) -> u64 {
    1_000u64.saturating_mul(1u64 << attempts.saturating_sub(1).min(6))
}

fn recovery_window_ms(lease_ttl: Duration) -> u64 {
    u64::try_from(lease_ttl.as_millis())
        .unwrap_or(u64::MAX)
        .saturating_mul(8)
        .max(60_000)
}

pub(crate) fn claim_job(
    db: &mut dyn statex_runtime::database::Database,
    now_ms: u64,
    retry_at_ms: u64,
) -> anyhow::Result<Option<Job>> {
    let job = db.claim_job(now_ms, retry_at_ms)?;
    match job {
        Some(mut job) if job.attempts > MAX_ATTEMPTS => {
            job.status = JobStatus::Failed;
            job.error = Some(serde_json::json!({
                "code": "attempts_exhausted",
                "message": "outbox delivery attempts exhausted"
            }));
            db.update_job(&job)?;
            Ok(None)
        }
        job => Ok(job),
    }
}

pub(crate) fn complete_job(
    db: &mut dyn statex_runtime::database::Database,
    job: Job,
) -> anyhow::Result<bool> {
    let Some(mut current) = db.job(&job.id)? else {
        return Ok(false);
    };
    if current.status != JobStatus::Running
        || current.attempts != job.attempts
        || current.next_attempt_ms != job.next_attempt_ms
        || job.status == JobStatus::Running
    {
        return Ok(false);
    }
    current.status = job.status;
    current.result = job.result;
    current.error = job.error;
    if current.status == JobStatus::Pending {
        current.next_attempt_ms = now_ms().saturating_add(backoff_ms(current.attempts));
    }
    db.update_job(&current)?;
    Ok(true)
}

fn source_invocation(id: &ActorId, op: InvOp) -> Invocation {
    Invocation {
        app: id.app.clone(),
        ty: id.ty.clone(),
        key: id.key.clone(),
        op,
        chain: vec![],
    }
}

fn retryable(outcome: &Outcome) -> bool {
    outcome.status == 408
        || outcome.status == 429
        || outcome.status >= 500 && outcome.body["error"]["code"] != "trap"
}

impl Node {
    pub(crate) async fn run_outbox(self: Arc<Self>) {
        loop {
            tokio::time::sleep(self.cfg.wake_tick).await;
            if !self.lease.valid() {
                continue;
            }
            let keys = match self.store.list(PREFIX).await {
                Ok(keys) => keys,
                Err(error) => {
                    tracing::warn!(%error, "list outbox markers");
                    continue;
                }
            };
            let mut deliveries = JoinSet::new();
            for id in keys.iter().filter_map(|key| parse_marker(key)) {
                if self.app(&id.app).is_none() {
                    continue;
                }
                while deliveries.len() >= CONCURRENCY {
                    deliveries.join_next().await;
                }
                let node = self.clone();
                deliveries.spawn(async move { node.dispatch_outbox(id).await });
            }
            while deliveries.join_next().await.is_some() {}
        }
    }

    async fn dispatch_outbox(&self, source: ActorId) {
        let now = now_ms();
        // The claim outlives a target execution. A crashed dispatcher leaves a
        // durable Running row that another scanner can reclaim at this time.
        // Budget two lease periods each for claim acknowledgement, target
        // execution, and completion routing/retries, plus two periods of slack.
        let claim_ms = recovery_window_ms(self.cfg.lease_ttl);
        let outcome = self
            .invoke(
                source_invocation(
                    &source,
                    InvOp::OutboxClaim {
                        now_ms: now,
                        retry_at_ms: now.saturating_add(claim_ms),
                    },
                ),
                0,
            )
            .await;
        if outcome.status != 200 {
            if outcome.status != 404 || outcome.body["error"]["code"] != "gone" {
                tracing::warn!(actor = %source, status = outcome.status, body = %outcome.body,
                    "outbox claim failed");
            }
            return;
        }
        if outcome.body["result"].is_null() {
            return;
        }
        let mut job: Job = match serde_json::from_value(outcome.body["result"].clone()) {
            Ok(job) => job,
            Err(error) => {
                tracing::warn!(actor = %source, %error, "invalid outbox claim response");
                return;
            }
        };
        // invoke has durably published the claim and validated fencing; its
        // source slot is no longer held. Self-spawn is therefore safe.
        if !self.lease.valid() {
            return;
        }
        let mut metadata = match InvocationMetadata::new(
            Caller::Actor {
                actor: statex_runtime::ActorRef {
                    app: source.app.clone(),
                    actor_type: source.ty.clone(),
                    key: source.key.clone(),
                },
            },
            self.cfg.lease_ttl * 2,
        ) {
            Ok(metadata) => metadata,
            Err(error) => {
                tracing::warn!(actor = %source, %error, "outbox invocation metadata failed");
                return;
            }
        };
        metadata.parent_request_id = Some(job.context.request_id.clone());
        // Supporting backends must isolate this host-owned metadata from guest
        // SQL and migrations; actor identity is reconstructed independently.
        metadata.principal = job.context.principal.clone();
        metadata.attributes = job.context.attributes.clone();
        let target = Invocation {
            app: job.target.app.clone(),
            ty: job.target.actor_type.clone(),
            key: job.target.key.clone(),
            op: InvOp::Call {
                method: job.method.clone(),
                args: job.args.clone(),
            },
            chain: vec![],
        };
        let result = self.invoke_with_metadata(target, metadata).await;
        if (200..300).contains(&result.status) {
            job.status = JobStatus::Succeeded;
            job.result = Some(result.body["result"].clone());
            job.error = None;
        } else {
            job.status = if retryable(&result) && job.attempts < MAX_ATTEMPTS {
                JobStatus::Pending
            } else {
                JobStatus::Failed
            };
            job.result = None;
            job.error = Some(result.body["error"].clone());
        }
        // Keep the original claim deadline as its compare-and-set token.
        // The source transaction sets a new retry time only after matching it.
        let completion = self
            .invoke(source_invocation(&source, InvOp::OutboxComplete { job }), 0)
            .await;
        if completion.status != 200 {
            tracing::warn!(actor = %source, status = completion.status,
                "outbox completion failed; durable claim will expire");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discovery_marker_roundtrip() {
        let source = ActorId {
            app: "payments/shop".into(),
            ty: "cart".into(),
            key: "ünicode/a%..".into(),
        };
        assert_eq!(parse_marker(&marker_key(&source)), Some(source));
        assert!(parse_marker("outbox/app/type/key/extra").is_none());
        assert!(parse_marker("outbox/app/type/%XX").is_none());
    }

    #[test]
    fn bounded_retry_policy() {
        assert_eq!(backoff_ms(1), 1_000);
        assert_eq!(backoff_ms(u32::MAX), 64_000);
        assert_eq!(recovery_window_ms(Duration::from_secs(1)), 60_000);
        assert_eq!(recovery_window_ms(Duration::from_secs(10)), 80_000);
        assert_eq!(recovery_window_ms(Duration::from_secs(20)), 160_000);
        assert!(retryable(&Outcome::err(503, "unavailable", "retry")));
        assert!(retryable(&Outcome::err(429, "overloaded", "retry")));
        for (status, code) in [
            (500, "trap"),
            (422, "method_error"),
            (403, "forbidden"),
            (404, "not_found"),
        ] {
            assert!(!retryable(&Outcome::err(status, code, "terminal")));
        }
    }

    #[test]
    fn transactional_claim_delay_reclaim_and_stale_completion() {
        use statex_runtime::database::{DatabaseFactory, SqliteFactory};
        use statex_runtime::{invocation::Caller, ActorRef};
        let directory = tempfile::Builder::new()
            .prefix("outbox-test-")
            .tempdir_in(env!("CARGO_MANIFEST_DIR"))
            .unwrap();
        let mut db = SqliteFactory
            .open(&directory.path().join("source.db"))
            .unwrap();
        let job = Job::new(
            ActorRef {
                app: "test".into(),
                actor_type: "counter".into(),
                key: "self".into(),
            },
            "increment".into(),
            serde_json::json!([1]),
            10_000,
            InvocationMetadata::new(Caller::Embedded, std::time::Duration::from_secs(1)).unwrap(),
        )
        .unwrap();
        db.begin().unwrap();
        db.enqueue_job(&job).unwrap();
        db.rollback().unwrap();
        assert!(db.job(&job.id).unwrap().is_none());
        db.begin().unwrap();
        db.enqueue_job(&job).unwrap();
        db.commit().unwrap();
        assert!(db.has_pending_jobs().unwrap());
        assert!(db.capture(1, 1).unwrap().is_some());

        db.begin().unwrap();
        assert!(
            claim_job(&mut *db, job.not_before_ms - 1, job.not_before_ms + 1)
                .unwrap()
                .is_none()
        );
        db.commit().unwrap();
        db.begin().unwrap();
        let first = claim_job(&mut *db, job.not_before_ms, job.not_before_ms + 1)
            .unwrap()
            .unwrap();
        db.commit().unwrap();
        assert_eq!(first.status, JobStatus::Running);
        assert_eq!(first.attempts, 1);
        assert!(
            db.capture(1, 2).unwrap().is_some(),
            "claim must create durable state"
        );
        db.begin().unwrap();
        let mut second = claim_job(&mut *db, first.next_attempt_ms, first.next_attempt_ms + 1)
            .unwrap()
            .unwrap();
        db.commit().unwrap();
        assert_eq!(second.attempts, 2);
        let mut stale = first;
        stale.status = JobStatus::Succeeded;
        db.begin().unwrap();
        assert!(!complete_job(&mut *db, stale).unwrap());
        db.commit().unwrap();
        assert_eq!(db.job(&job.id).unwrap().unwrap().status, JobStatus::Running);
        second.status = JobStatus::Succeeded;
        second.result = Some(serde_json::json!(1));
        second.target.key = "receipt-must-not-rewrite-target".into();
        db.begin().unwrap();
        assert!(complete_job(&mut *db, second).unwrap());
        db.commit().unwrap();
        assert!(!db.has_pending_jobs().unwrap());
        assert_eq!(
            db.job(&job.id).unwrap().unwrap().result,
            Some(serde_json::json!(1))
        );
        assert_eq!(db.job(&job.id).unwrap().unwrap().target, job.target);
        let abandoned = Job::new(
            job.target.clone(),
            job.method.clone(),
            job.args.clone(),
            0,
            job.context.clone(),
        )
        .unwrap();
        db.begin().unwrap();
        db.enqueue_job(&abandoned).unwrap();
        db.commit().unwrap();
        let mut at = abandoned.not_before_ms;
        for attempt in 1..=MAX_ATTEMPTS {
            db.begin().unwrap();
            let claimed = claim_job(&mut *db, at, at + 1).unwrap().unwrap();
            assert_eq!(claimed.attempts, attempt);
            at = claimed.next_attempt_ms;
            db.commit().unwrap();
        }
        db.begin().unwrap();
        assert!(claim_job(&mut *db, at, at + 1).unwrap().is_none());
        db.commit().unwrap();
        let exhausted = db.job(&abandoned.id).unwrap().unwrap();
        assert_eq!(exhausted.status, JobStatus::Failed);
        assert_eq!(exhausted.error.unwrap()["code"], "attempts_exhausted");
        assert!(!db.has_pending_jobs().unwrap());
    }
}
