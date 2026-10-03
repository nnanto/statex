//! A resident actor: local SQLite replica, WAL tail, component instance and
//! the replication bookkeeping for its current epoch.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use bytes::Bytes;
use rusqlite::Connection;
use serde_json::Value as J;
use statex_ltx::{apply_segment, plan_restore, segment_name, snapshot_name, LogEntry, Segment, WalTail};
use statex_runtime::{apply_migrations, open_db, AppCode, CallError, CallOutput, ActorIdentity, ActorInstance};
use statex_store::{DynStore, ETag};

use crate::layout::{parse_ltx, ActorId};

pub struct Actor {
    pub id: ActorId,
    pub epoch: u64,
    pub owner_etag: ETag,
    pub dir: PathBuf,
    db_path: PathBuf,
    db: Arc<Mutex<Connection>>,
    wal: WalTail,
    /// Last durable transaction id.
    pub txid: u64,
    /// Transaction id of the newest snapshot in the current epoch.
    pub snapshot_txid: u64,
    instance: Option<ActorInstance>,
    code: Arc<AppCode>,
    /// Migrations of `code` have been applied and committed.
    migrated: bool,
    pub last_used: Instant,
}

/// What to run inside an actor transaction.
pub enum Op {
    Call { method: String, args: J },
    /// Apply pending migrations only (used by `_create`).
    Touch,
}

pub struct Executed {
    pub outcome: Result<CallOutput, CallError>,
    /// Pages committed by this transaction, to be uploaded before acking.
    pub segment: Option<Segment>,
}

/// Restores an actor from the object store into a fresh local replica and
/// writes the activation snapshot for `epoch`.
pub async fn activate(
    store: &DynStore,
    data_dir: &Path,
    id: &ActorId,
    epoch: u64,
    owner_etag: ETag,
    fresh: bool,
    code: Arc<AppCode>,
) -> Result<Actor> {
    let dir = data_dir.join(id.local_dir());
    let db_path = dir.join(format!("e{epoch}.db"));
    {
        let dir = dir.clone();
        tokio::task::spawn_blocking(move || {
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir)
        })
        .await??;
    }
    let mut txid = 0;
    if !fresh {
        let prefix = id.ltx_prefix();
        let entries: Vec<(u64, LogEntry)> =
            store.list(&prefix).await?.iter().filter_map(|k| parse_ltx(k.strip_prefix(&prefix)?)).collect();
        if let Some(plan) = plan_restore(&entries) {
            let ep = id.epoch_prefix(plan.epoch);
            let snap = store
                .get(&format!("{ep}{}", snapshot_name(plan.snapshot_txid)))
                .await?
                .ok_or_else(|| anyhow!("snapshot disappeared during restore"))?;
            tokio::fs::write(&db_path, &snap.data).await?;
            txid = plan.snapshot_txid;
            for t in &plan.segments {
                let obj = store
                    .get(&format!("{ep}{}", segment_name(*t)))
                    .await?
                    .ok_or_else(|| anyhow!("segment {t} disappeared during restore"))?;
                let seg = Segment::decode(&obj.data).with_context(|| format!("decode segment {t}"))?;
                if seg.txid != *t {
                    bail!("segment {t} has txid {}", seg.txid);
                }
                let p = db_path.clone();
                tokio::task::spawn_blocking(move || apply_segment(&p, &seg)).await??;
                txid = *t;
            }
            tracing::info!(actor = %id, epoch, "restored from e{} snapshot {} + {} segments", plan.epoch, plan.snapshot_txid, plan.segments.len());
        }
    }
    let p = db_path.clone();
    let (conn, bytes) = tokio::task::spawn_blocking(move || -> Result<_> {
        let conn = open_db(&p)?;
        let bytes = checkpoint_and_read(&conn, &p)?;
        Ok((conn, bytes))
    })
    .await??;
    store.put(&format!("{}{}", id.epoch_prefix(epoch), snapshot_name(txid)), Bytes::from(bytes)).await?;
    let wal = WalTail::new(&db_path);
    Ok(Actor {
        id: id.clone(),
        epoch,
        owner_etag,
        dir,
        db_path,
        db: Arc::new(Mutex::new(conn)),
        wal,
        txid,
        snapshot_txid: txid,
        instance: None,
        code,
        migrated: false,
        last_used: Instant::now(),
    })
}

fn checkpoint_and_read(conn: &Connection, path: &Path) -> Result<Vec<u8>> {
    let busy: i64 = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0))?;
    if busy != 0 {
        bail!("checkpoint was blocked");
    }
    Ok(std::fs::read(path)?)
}

impl Actor {
    /// Runs one transaction. Blocking; call from `spawn_blocking`.
    pub fn execute(&mut self, code: &Arc<AppCode>, op: Op) -> Result<Executed> {
        if !Arc::ptr_eq(code, &self.code) {
            tracing::info!(actor = %self.id, "switching to new deployment ({})", &code.manifest.sha256[..12]);
            self.instance = None;
            self.code = code.clone();
            self.migrated = false;
        }
        self.db.lock().unwrap().execute_batch("BEGIN IMMEDIATE")?;
        let outcome = self.run_in_tx(op);
        // Only a successful return commits: a trap or a `result` method
        // returning `err` rolls the transaction back.
        let commit = matches!(&outcome, Ok(Ok(o)) if !o.is_err);
        {
            let db = self.db.lock().unwrap();
            if commit {
                if let Err(e) = db.execute_batch("COMMIT") {
                    let _ = db.execute_batch("ROLLBACK");
                    return Err(e).context("commit");
                }
            } else {
                db.execute_batch("ROLLBACK").context("rollback")?;
            }
        }
        let outcome = outcome?;
        if commit {
            self.migrated = true;
        }
        if matches!(outcome, Err(CallError::Trap(_))) {
            self.instance = None;
        }
        let segment = match self.wal.capture()? {
            Some(pages) if commit => {
                self.txid += 1;
                Some(Segment { epoch: self.epoch, txid: self.txid, pages })
            }
            Some(_) => bail!("WAL changed by a rolled back transaction"),
            None => None,
        };
        self.last_used = Instant::now();
        Ok(Executed { outcome, segment })
    }

    fn run_in_tx(&mut self, op: Op) -> Result<Result<CallOutput, CallError>> {
        if !self.migrated {
            let migrations = self.code.manifest.migrations.get(&self.id.ty).cloned().unwrap_or_default();
            let applied = apply_migrations(&self.db.lock().unwrap(), &migrations)?;
            if !applied.is_empty() {
                tracing::info!(actor = %self.id, "applied migrations {applied:?}");
            }
        }
        let (method, args) = match op {
            Op::Touch => return Ok(Ok(CallOutput { value: J::Null, is_err: false })),
            Op::Call { method, args } => (method, args),
        };
        if self.instance.is_none() {
            let identity = ActorIdentity {
                app: self.id.app.clone(),
                actor_type: self.id.ty.clone(),
                key: self.id.key.clone(),
                epoch: self.epoch,
            };
            self.instance = Some(self.code.instantiate(identity, self.db.clone())?);
        }
        Ok(self.instance.as_mut().unwrap().call(&self.id.ty, &method, &args))
    }

    /// Checkpoints and returns a full database image. Blocking.
    pub fn snapshot(&mut self) -> Result<Vec<u8>> {
        let bytes = checkpoint_and_read(&self.db.lock().unwrap(), &self.db_path)?;
        self.wal.reset();
        Ok(bytes)
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        self.instance = None;
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Uploads a compaction snapshot and deletes everything it supersedes.
pub async fn compact(store: &DynStore, id: &ActorId, epoch: u64, txid: u64, image: Vec<u8>) -> Result<()> {
    let ep = id.epoch_prefix(epoch);
    store.put(&format!("{ep}{}", snapshot_name(txid)), Bytes::from(image)).await?;
    gc(store, id, epoch, txid).await
}

/// Deletes objects of older epochs, and objects up to `txid` in `epoch`
/// (except the snapshot at `txid`). Safe once that snapshot is durable.
pub async fn gc(store: &DynStore, id: &ActorId, epoch: u64, txid: u64) -> Result<()> {
    let prefix = id.ltx_prefix();
    for k in store.list(&prefix).await? {
        let Some((e, entry)) = k.strip_prefix(&prefix).and_then(parse_ltx) else { continue };
        let stale = e < epoch
            || (e == epoch
                && match entry {
                    LogEntry::Snapshot(t) => t < txid,
                    LogEntry::Segment(t) => t <= txid,
                });
        if stale {
            store.delete(&k).await?;
        }
    }
    Ok(())
}
