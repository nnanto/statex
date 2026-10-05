//! A resident actor: local SQLite replica, WAL tail, component instance and
//! the replication bookkeeping for its current epoch.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use rusqlite::Connection;
use serde_json::Value as J;
use statex_ltx::{apply_segment, plan_restore, segment_name, snapshot_name, LogEntry, Segment, WalTail};
use statex_runtime::alarm::{self, Alarm};
use statex_runtime::{
    apply_migrations, open_db, ActorCaller, ActorIdentity, ActorInstance, ActorRef, AppCode, CallError, CallOutput,
};
use statex_store::{DynStore, ETag};

use crate::layout::{parse_ltx, wake_name, ActorId};

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
    /// Routes calls this actor makes to other actors.
    pub caller: Option<Arc<dyn ActorCaller>>,
    /// The scheduled alarm as of the last committed transaction.
    pub alarm: Option<Alarm>,
}

/// What to run inside an actor transaction.
pub enum Op {
    /// `chain`: the actors up the call chain when called by another actor.
    Call { method: String, args: J, chain: Vec<ActorRef> },
    /// Apply pending migrations only (used by `_create`).
    Touch,
    /// Fire the alarm if it is due at `now_ms`: run the handler, or schedule
    /// a retry if it fails. Answers `{"fired": bool, "error"?: string,
    /// "armed"?: wake name}`, `armed` naming the alarm still scheduled.
    Alarm { now_ms: u64 },
}

pub struct Executed {
    pub outcome: Result<CallOutput, CallError>,
    /// Pages committed by this transaction, to be uploaded before acking.
    pub segment: Option<Segment>,
    /// Set when the committed transaction changed the scheduled alarm.
    pub alarm: Option<AlarmChange>,
}

/// The alarm before and after a transaction that changed it.
#[derive(Debug, Clone, Copy)]
pub struct AlarmChange {
    pub before: Option<Alarm>,
    pub after: Option<Alarm>,
}

type Outcome = Result<CallOutput, CallError>;

fn ok(value: J) -> Outcome {
    Ok(CallOutput { value, is_err: false })
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
            let snap_key = format!("{ep}{}", snapshot_name(plan.snapshot_txid));
            if !store.get_to_file(&snap_key, &db_path).await? {
                bail!("snapshot disappeared during restore");
            }
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
    let (conn, alarm) = tokio::task::spawn_blocking(move || -> Result<_> {
        let conn = open_db(&p)?;
        checkpoint(&conn)?;
        let alarm = alarm::read(&conn)?;
        Ok((conn, alarm))
    })
    .await??;
    // Nothing else can touch the database yet, so the file is stable while
    // it streams up.
    store.put_file(&format!("{}{}", id.epoch_prefix(epoch), snapshot_name(txid)), &db_path).await?;
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
        caller: None,
        alarm,
    })
}

/// Folds the WAL into the database file and empties it, so the file alone is
/// a complete image. With autocheckpoint off, the file then stays unchanged
/// until the next explicit checkpoint.
fn checkpoint(conn: &Connection) -> Result<()> {
    let busy: i64 = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0))?;
    if busy != 0 {
        bail!("checkpoint was blocked");
    }
    Ok(())
}

impl Actor {
    /// Runs one operation (one committed transaction at most). Blocking; call
    /// from `spawn_blocking`.
    pub fn execute(&mut self, code: &Arc<AppCode>, op: Op) -> Result<Executed> {
        if !Arc::ptr_eq(code, &self.code) {
            tracing::info!(actor = %self.id, "switching to new deployment ({})", &code.manifest.sha256[..12]);
            self.instance = None;
            self.code = code.clone();
            self.migrated = false;
        }
        let before = self.alarm;
        let is_alarm = matches!(op, Op::Alarm { .. });
        let (mut outcome, segment) = match op {
            Op::Touch => self.transaction(|_| Ok(ok(J::Null)))?,
            Op::Call { method, args, chain } => self.transaction(|a| {
                let ty = a.id.ty.clone();
                Ok(a.instance()?.call_with(&ty, &method, &args, &chain))
            })?,
            Op::Alarm { now_ms } => self.fire_alarm(now_ms)?,
        };
        if segment.is_some() {
            self.alarm = alarm::read(&self.db.lock().unwrap())?;
        }
        if is_alarm {
            if let Ok(CallOutput { value: J::Object(m), .. }) = &mut outcome {
                if let Some(a) = &self.alarm {
                    m.insert("armed".into(), J::String(wake_name(a)));
                }
            }
        }
        self.last_used = Instant::now();
        let alarm = (self.alarm != before).then_some(AlarmChange { before, after: self.alarm });
        Ok(Executed { outcome, segment, alarm })
    }

    /// Runs `body` in one transaction, after any pending migrations. Only a
    /// successful return commits: a trap or a `result` method returning `err`
    /// rolls back. Returns the pages committed, if any.
    fn transaction(&mut self, body: impl FnOnce(&mut Self) -> Result<Outcome>) -> Result<(Outcome, Option<Segment>)> {
        self.db.lock().unwrap().execute_batch("BEGIN IMMEDIATE")?;
        let outcome = self.migrate().and_then(|()| body(self));
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
        Ok((outcome, segment))
    }

    fn migrate(&mut self) -> Result<()> {
        if !self.migrated {
            let migrations = self.code.manifest.migrations.get(&self.id.ty).cloned().unwrap_or_default();
            let applied = apply_migrations(&self.db.lock().unwrap(), &migrations)?;
            if !applied.is_empty() {
                tracing::info!(actor = %self.id, "applied migrations {applied:?}");
            }
        }
        Ok(())
    }

    fn instance(&mut self) -> Result<&mut ActorInstance> {
        if self.instance.is_none() {
            let identity = ActorIdentity {
                app: self.id.app.clone(),
                actor_type: self.id.ty.clone(),
                key: self.id.key.clone(),
                epoch: self.epoch,
            };
            self.instance = Some(self.code.instantiate_with(identity, self.db.clone(), self.caller.clone())?);
        }
        Ok(self.instance.as_mut().unwrap())
    }

    /// Fires the alarm if it is due. The handler runs in a transaction that
    /// also consumes the alarm (it may set a new one); if it fails, a second
    /// transaction schedules a retry with backoff, or drops the alarm after
    /// [`alarm::MAX_RETRIES`] retries.
    fn fire_alarm(&mut self, now_ms: u64) -> Result<(Outcome, Option<Segment>)> {
        let Some(due) = self.alarm.filter(|a| a.at_ms <= now_ms) else {
            return Ok((ok(serde_json::json!({ "fired": false })), None));
        };
        let ty = self.id.ty.clone();
        if !self.code.manifest.actor_type(&ty).is_some_and(|t| t.alarm.is_some()) {
            tracing::warn!(actor = %self.id, "alarm fired but the actor type has no alarm handler (removed by a deployment?); dropping it");
            let (o, seg) = self.transaction(|a| {
                alarm::clear(&a.db.lock().unwrap())?;
                Ok(ok(J::Null))
            })?;
            return Ok((o.map(|_| CallOutput { value: serde_json::json!({ "fired": false }), is_err: false }), seg));
        }
        let (outcome, seg) = self.transaction(|a| {
            alarm::clear(&a.db.lock().unwrap())?;
            Ok(a.instance()?.call_alarm(&ty, due.retry))
        })?;
        let error = match outcome {
            Ok(o) if !o.is_err => return Ok((ok(serde_json::json!({ "fired": true })), seg)),
            Ok(o) => format!("returned err: {}", o.value),
            Err(e) => e.to_string(),
        };
        let retry = due.retry + 1;
        let epoch = self.epoch;
        let id = self.id.clone();
        let (o, seg) = self.transaction(|a| {
            let db = a.db.lock().unwrap();
            if retry > alarm::MAX_RETRIES {
                tracing::error!(actor = %id, "alarm handler failed {retry} times, dropping the alarm: {error}");
                alarm::clear(&db)?;
            } else {
                let at = now_ms + alarm::backoff_ms(retry);
                tracing::warn!(actor = %id, "alarm handler failed (attempt {retry}), retrying at {at}: {error}");
                alarm::set_retry(&db, at, retry, epoch)?;
            }
            Ok(ok(J::Null))
        })?;
        Ok((o.map(|_| CallOutput { value: serde_json::json!({ "fired": true, "error": error }), is_err: false }), seg))
    }

    /// Checkpoints and returns the path of the now complete database file.
    /// Blocking. The file stays valid only while the caller keeps exclusive
    /// access to this actor (no `execute`, `snapshot` or drop) — hold the
    /// slot lock until the upload has finished.
    pub fn snapshot(&mut self) -> Result<PathBuf> {
        checkpoint(&self.db.lock().unwrap())?;
        self.wal.reset();
        Ok(self.db_path.clone())
    }
}

impl Drop for Actor {
    fn drop(&mut self) {
        self.instance = None;
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Uploads a compaction snapshot from `image` (see [`Actor::snapshot`]) and
/// deletes everything it supersedes.
pub async fn compact(store: &DynStore, id: &ActorId, epoch: u64, txid: u64, image: &Path) -> Result<()> {
    let ep = id.epoch_prefix(epoch);
    store.put_file(&format!("{ep}{}", snapshot_name(txid)), image).await?;
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
