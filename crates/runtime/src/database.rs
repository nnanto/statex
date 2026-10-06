//! Replaceable transactional actor state and persistence.
//!
//! A backend owns all state, including migration bookkeeping and alarms.
//! Changes and snapshots are opaque to the node; only their ordering and
//! durability are managed by the node.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::alarm::Alarm;
use crate::manifest::Migration;
use crate::outbox::Job;

mod sqlite;
pub use sqlite::{sqlite_handle, SqliteDatabase, SqliteFactory};

/// Stable identity of a backend's snapshot and change format. Changing either
/// format requires a new identity/version, not silently interpreting old data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendIdentity {
    pub name: String,
    pub format_version: u32,
}

/// SQL values exposed to guests, independent of any database driver.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SqlRows {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<SqlValue>>,
}

/// An actor's exclusive, transactional database. All mutations (including
/// alarms and migration metadata) must participate in the current transaction.
/// The runtime and node share one handle; the actor slot serializes access.
pub trait Database: Send {
    fn execute(&mut self, statement: &str, params: &[SqlValue]) -> Result<u64>;
    fn query(&mut self, statement: &str, params: &[SqlValue]) -> Result<SqlRows>;
    fn begin(&mut self) -> Result<()>;
    fn commit(&mut self) -> Result<()>;
    fn rollback(&mut self) -> Result<()>;
    /// Apply each migration name once, inside the current transaction.
    fn apply_migrations(&mut self, migrations: &[Migration]) -> Result<Vec<String>>;
    fn alarm(&mut self) -> Result<Option<Alarm>>;
    /// Every installation must increment the alarm's persistent sequence,
    /// including after clear, to keep wake hints uniquely identifiable.
    fn set_alarm(&mut self, at_ms: u64, retry: u32, epoch: u64) -> Result<()>;
    fn clear_alarm(&mut self) -> Result<()>;
    /// Store a pending job inside the active actor transaction. Implementations
    /// must protect records (especially trusted invocation metadata) against
    /// guest SQL and migration edits, including indirect trigger/schema access.
    fn enqueue_job(&mut self, _job: &Job) -> Result<()> {
        anyhow::bail!("durable outbox is not supported by this backend")
    }
    /// Claim one due pending or overdue running job in the active transaction.
    /// Increment attempts, mark running, and set its recovery deadline.
    fn claim_job(&mut self, _now_ms: u64, _retry_at_ms: u64) -> Result<Option<Job>> {
        anyhow::bail!("durable outbox is not supported by this backend")
    }
    /// Persist retry or terminal state inside the active actor transaction.
    /// Reject stale completion: the stored job must be Running at the supplied
    /// attempt count. Claim is the only operation that increments attempts.
    fn update_job(&mut self, _job: &Job) -> Result<()> {
        anyhow::bail!("durable outbox is not supported by this backend")
    }
    /// Inspect a record without creating or mutating outbox state.
    fn job(&mut self, _id: &str) -> Result<Option<Job>> {
        anyhow::bail!("durable outbox is not supported by this backend")
    }
    /// Whether any nonterminal records exist, including future and running
    /// jobs. Callable before commit so the node can publish its durable index.
    /// Backends without outbox support report false to preserve normal calls;
    /// enqueue still fails explicitly until a backend implements the outbox.
    fn has_pending_jobs(&mut self) -> Result<bool> {
        Ok(false)
    }
    /// Capture committed state since the previous capture/checkpoint. Return
    /// None for read-only or rolled-back transactions. Include migration and
    /// alarm and outbox changes. The payload must replay at the supplied epoch and txid.
    /// Failure causes the node to discard the actor without acknowledging.
    fn capture(&mut self, epoch: u64, txid: u64) -> Result<Option<Vec<u8>>>;
    /// Produce a complete snapshot of committed state, reset change capture,
    /// and return its file. The image must stay stable until the next mutable
    /// operation or drop (the node holds the actor slot during upload).
    fn checkpoint(&mut self) -> Result<PathBuf>;
}

pub type DatabaseHandle = Arc<Mutex<Box<dyn Database>>>;
pub type DynDatabaseFactory = Arc<dyn DatabaseFactory>;

/// Creates fresh state or opens an already restored snapshot. A snapshot is
/// one opaque file; backends needing several files must package them into it.
/// Methods run on blocking threads and must not require a Tokio runtime.
pub trait DatabaseFactory: Send + Sync {
    fn identity(&self) -> BackendIdentity;
    /// `path` is absent for fresh state, otherwise it contains this backend's
    /// snapshot. Any backend working files must be inside path's directory.
    fn open(&self, path: &Path) -> Result<Box<dyn Database>>;
    /// Replay one ordered durable change onto a closed snapshot. Validate the
    /// supplied epoch/txid and payload integrity before modifying the image.
    fn replay(&self, path: &Path, epoch: u64, txid: u64, change: &[u8]) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoOutbox;

    impl Database for NoOutbox {
        fn execute(&mut self, _: &str, _: &[SqlValue]) -> Result<u64> { unreachable!() }
        fn query(&mut self, _: &str, _: &[SqlValue]) -> Result<SqlRows> { unreachable!() }
        fn begin(&mut self) -> Result<()> { Ok(()) }
        fn commit(&mut self) -> Result<()> { Ok(()) }
        fn rollback(&mut self) -> Result<()> { Ok(()) }
        fn apply_migrations(&mut self, _: &[Migration]) -> Result<Vec<String>> { unreachable!() }
        fn alarm(&mut self) -> Result<Option<Alarm>> { unreachable!() }
        fn set_alarm(&mut self, _: u64, _: u32, _: u64) -> Result<()> { unreachable!() }
        fn clear_alarm(&mut self) -> Result<()> { unreachable!() }
        fn capture(&mut self, _: u64, _: u64) -> Result<Option<Vec<u8>>> { unreachable!() }
        fn checkpoint(&mut self) -> Result<PathBuf> { unreachable!() }
    }

    #[test]
    fn unsupported_outbox_preserves_normal_transactions_but_rejects_jobs() {
        let mut db = NoOutbox;
        db.begin().unwrap();
        assert!(!db.has_pending_jobs().unwrap());
        db.commit().unwrap();
        let job = Job::new(
            crate::ActorRef { app: "shop".into(), actor_type: "counter".into(), key: "alice".into() },
            "add".into(), serde_json::json!([1]), 0,
            crate::invocation::InvocationMetadata::new(
                crate::invocation::Caller::Embedded, std::time::Duration::from_secs(1),
            ).unwrap(),
        ).unwrap();
        for error in [
            db.enqueue_job(&job).unwrap_err(),
            db.claim_job(100, 200).unwrap_err(),
            db.update_job(&job).unwrap_err(),
            db.job(&job.id).unwrap_err(),
        ] {
            assert!(error.to_string().contains("not supported"), "{error}");
        }
    }
}
