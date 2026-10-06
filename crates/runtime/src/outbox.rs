//! Transactional, actor-local jobs for at-least-once deferred actor calls.
//! Enqueue, claim and update share the actor transaction and its durable WAL.

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::database::Database;
use crate::invocation::InvocationMetadata;
use crate::{validate_app_name, validate_name, ActorRef};

/// Actor keys use the same byte limit as the node's normal invocation API.
pub const MAX_KEY_LEN: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
}

impl JobStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

/// Durable record, including source invocation metadata. Dispatch must create
/// a fresh deadline and request ID, rather than reusing the source deadline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub target: ActorRef,
    pub method: String,
    pub args: Value,
    pub not_before_ms: u64,
    pub attempts: u32,
    pub next_attempt_ms: u64,
    pub status: JobStatus,
    pub result: Option<Value>,
    pub error: Option<Value>,
    pub context: InvocationMetadata,
}

/// Unix time in milliseconds, limited to the portable backend timestamp range.
pub fn now_ms() -> Result<u64> {
    let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
    validate_timestamp(now)?;
    Ok(now)
}

pub(crate) fn validate_timestamp(timestamp: u64) -> Result<()> {
    ensure!(timestamp <= i64::MAX as u64, "outbox timestamp exceeds supported range");
    Ok(())
}

impl Job {
    pub fn new(
        target: ActorRef,
        method: String,
        args: Value,
        delay_ms: u64,
        context: InvocationMetadata,
    ) -> Result<Self> {
        let not_before_ms = now_ms()?.checked_add(delay_ms)
            .context("outbox delay overflow")?;
        let job = Self {
            id: hex::encode(rand::random::<[u8; 16]>()),
            target,
            method,
            args,
            not_before_ms,
            attempts: 0,
            next_attempt_ms: not_before_ms,
            status: JobStatus::Pending,
            result: None,
            error: None,
            context,
        };
        job.validate()?;
        Ok(job)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(!self.id.is_empty(), "outbox job ID must not be empty");
        validate_app_name(&self.target.app)?;
        validate_name("actor type", &self.target.actor_type)?;
        validate_name("method", &self.method)?;
        ensure!(
            !self.target.key.is_empty() && self.target.key.len() <= MAX_KEY_LEN,
            "actor key must be 1..={MAX_KEY_LEN} bytes"
        );
        ensure!(self.args.is_array(), "outbox arguments must be a positional JSON array");
        validate_timestamp(self.not_before_ms)?;
        validate_timestamp(self.next_attempt_ms)?;
        ensure!(
            self.next_attempt_ms >= self.not_before_ms,
            "outbox retry cannot precede the scheduled time"
        );
        Ok(())
    }
}

/// Enqueues a validated record in the caller's active transaction. A backend
/// must reject this operation if it does not implement durable outbox support.
pub fn enqueue(database: &mut dyn Database, job: &Job) -> Result<()> {
    job.validate()?;
    ensure!(
        job.status == JobStatus::Pending && job.attempts == 0
            && job.result.is_none() && job.error.is_none()
            && job.next_attempt_ms == job.not_before_ms,
        "new outbox jobs must be pending and unattempted"
    );
    database.enqueue_job(job)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::invocation::Caller;
    use std::time::Duration;

    pub(crate) fn sample_job() -> Job {
        Job::new(
            ActorRef { app: "shop".into(), actor_type: "counter".into(), key: "alice".into() },
            "add".into(),
            serde_json::json!([1]),
            0,
            InvocationMetadata::new(Caller::Embedded, Duration::from_secs(1)).unwrap(),
        ).unwrap()
    }

    #[test]
    fn validates_targets_arguments_and_delay() {
        let job = sample_job();
        let roundtrip: Job = serde_json::from_str(&serde_json::to_string(&job).unwrap()).unwrap();
        assert_eq!(job, roundtrip);
        assert_ne!(job.id, sample_job().id);
        for key in ["".to_owned(), "a".repeat(MAX_KEY_LEN + 1), "é".repeat(MAX_KEY_LEN / 2 + 1)] {
            let mut invalid = job.clone();
            invalid.target.key = key.clone();
            assert!(invalid.validate().is_err(), "{key:?}");
        }
        for key in [".", "..", "a/b", "a\\b", "a\0b", "a\nb", "雪/alice"] {
            let mut valid = job.clone();
            valid.target.key = key.into();
            valid.validate().unwrap();
        }
        let mut valid = job.clone();
        valid.target.key = "é".repeat(MAX_KEY_LEN / 2);
        valid.validate().unwrap();
        for app in ["", "Bad", "a/b/c", "actors"] {
            let mut invalid = job.clone();
            invalid.target.app = app.into();
            assert!(invalid.validate().is_err(), "{app:?}");
        }
        let mut invalid = job.clone();
        invalid.target.actor_type = "Bad".into();
        assert!(invalid.validate().is_err());
        invalid = job.clone();
        invalid.method = "".into();
        assert!(invalid.validate().is_err());
        for args in [Value::Null, serde_json::json!({}), serde_json::json!("[]")] {
            invalid = job.clone();
            invalid.args = args;
            assert!(invalid.validate().is_err());
        }
        invalid = job.clone();
        invalid.next_attempt_ms = 0;
        assert!(invalid.validate().is_err());
        assert!(Job::new(job.target, job.method, job.args, u64::MAX, job.context).is_err());
    }
}
