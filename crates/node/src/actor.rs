//! A resident actor: replaceable state backend, component instance and
//! the replication bookkeeping for its current epoch.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value as J;
use statex_ltx::{plan_restore, segment_name, snapshot_name, LogEntry};
use statex_runtime::alarm::{self, Alarm};
use statex_runtime::database::{BackendIdentity, DatabaseHandle, DynDatabaseFactory};
use statex_runtime::{
    ActorCaller, ActorIdentity, ActorInstance, ActorRef, AppCode, CallError, CallOutput,
};
use statex_store::{get_json, to_json_bytes, DynStore, ETag};

use crate::extensions::{InvocationExtension, TransactionKind};
use crate::layout::{parse_ltx, wake_name, ActorId};
use statex_runtime::invocation::{
    run_hook, Caller, InvocationContext, InvocationOperation, TransactionState,
};

pub struct Actor {
    pub id: ActorId,
    pub epoch: u64,
    pub owner_etag: ETag,
    pub dir: PathBuf,
    db: DatabaseHandle,
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
    Call {
        method: String,
        args: J,
        chain: Vec<ActorRef>,
    },
    /// Apply pending migrations only (used by `_create`).
    Touch,
    /// Fire the alarm if it is due at `now_ms`: run the handler, or schedule
    /// a retry if it fails. Answers `{"fired": bool, "error"?: string,
    /// "armed"?: wake name}`, `armed` naming the alarm still scheduled.
    Alarm { now_ms: u64 },
}

pub struct Executed {
    /// The resident code identity changed, even if migrations or commit failed.
    pub code_changed: bool,
    pub outcome: Result<CallOutput, CallError>,
    /// Opaque committed changes, to be uploaded before acking.
    pub segment: Option<DurableChange>,
    /// Set when the committed transaction changed the scheduled alarm.
    pub alarm: Option<AlarmChange>,
}

pub struct DurableChange {
    pub txid: u64,
    pub data: Vec<u8>,
}

/// The alarm before and after a transaction that changed it.
#[derive(Debug, Clone, Copy)]
pub struct AlarmChange {
    pub before: Option<Alarm>,
    pub after: Option<Alarm>,
}

type Outcome = Result<CallOutput, CallError>;

fn ok(value: J) -> Outcome {
    Ok(CallOutput {
        value,
        is_err: false,
    })
}

/// Restores an actor from the object store into a fresh local replica and
/// writes the activation snapshot for `epoch` using the selected backend.
#[allow(clippy::too_many_arguments)]
pub async fn activate(
    store: &DynStore,
    data_dir: &Path,
    id: &ActorId,
    epoch: u64,
    owner_etag: ETag,
    fresh: bool,
    code: Arc<AppCode>,
    factory: DynDatabaseFactory,
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
        let entries: Vec<(u64, LogEntry)> = store
            .list(&prefix)
            .await?
            .iter()
            .filter_map(|k| parse_ltx(k.strip_prefix(&prefix)?))
            .collect();
        if let Some(plan) = plan_restore(&entries) {
            let ep = id.epoch_prefix(plan.epoch);
            let identity = get_json::<BackendIdentity>(&**store, &format!("{ep}backend.json"))
                .await?
                .map(|(identity, _)| identity)
                .with_context(|| {
                    format!("snapshot epoch {} is missing backend identity", plan.epoch)
                })?;
            if identity != factory.identity() {
                bail!(
                    "incompatible actor state backend: stored {identity:?}, configured {:?}",
                    factory.identity()
                );
            }
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
                let p = db_path.clone();
                let factory = factory.clone();
                let (replay_epoch, replay_txid) = (plan.epoch, *t);
                tokio::task::spawn_blocking(move || {
                    factory.replay(&p, replay_epoch, replay_txid, &obj.data)
                })
                .await??;
                txid = *t;
            }
            tracing::info!(actor = %id, epoch, "restored from e{} snapshot {} + {} segments", plan.epoch, plan.snapshot_txid, plan.segments.len());
        }
    }
    let p = db_path.clone();
    let opener = factory.clone();
    let (db, alarm, image) = tokio::task::spawn_blocking(move || -> Result<_> {
        let mut db = opener.open(&p)?;
        let image = db.checkpoint()?;
        let alarm = db.alarm()?;
        Ok((db, alarm, image))
    })
    .await??;
    // Nothing else can touch the database yet, so the file is stable while
    // it streams up.
    // Publish the format before the snapshot: an epoch without a snapshot is
    // ignored, but a durable snapshot must never have an ambiguous identity.
    store
        .put(
            &format!("{}backend.json", id.epoch_prefix(epoch)),
            to_json_bytes(&factory.identity()),
        )
        .await?;
    store
        .put_file(
            &format!("{}{}", id.epoch_prefix(epoch), snapshot_name(txid)),
            &image,
        )
        .await?;
    Ok(Actor {
        id: id.clone(),
        epoch,
        owner_etag,
        dir,
        db: Arc::new(Mutex::new(db)),
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

impl Actor {
    /// Runs one operation (one committed transaction at most). Blocking; call
    /// from `spawn_blocking`.
    pub fn execute(&mut self, code: &Arc<AppCode>, op: Op) -> Result<Executed> {
        let operation = match &op {
            Op::Call { method, args, .. } => InvocationOperation::Call {
                method: method.clone(),
                args: args.clone(),
            },
            Op::Touch => InvocationOperation::Create,
            Op::Alarm { .. } => InvocationOperation::Alarm,
        };
        let context = InvocationContext::new(
            ActorRef {
                app: self.id.app.clone(),
                actor_type: self.id.ty.clone(),
                key: self.id.key.clone(),
            },
            operation,
            Caller::Embedded,
            std::time::Duration::from_millis(code.manifest.limits.timeout_ms),
        )?;
        self.execute_with_context(code, op, &context, &[])
    }

    pub fn execute_with_context(
        &mut self,
        code: &Arc<AppCode>,
        op: Op,
        context: &InvocationContext,
        extensions: &[Arc<dyn InvocationExtension>],
    ) -> Result<Executed> {
        let code_changed = !Arc::ptr_eq(code, &self.code);
        if code_changed {
            tracing::info!(actor = %self.id, "switching to new deployment ({})", &code.manifest.sha256[..12]);
            self.instance = None;
            self.code = code.clone();
            self.migrated = false;
        }
        let before = self.alarm;
        let is_alarm = matches!(op, Op::Alarm { .. });
        let (mut outcome, segment) = match op {
            Op::Touch => {
                self.transaction(context, extensions, TransactionKind::Invocation, |_| {
                    Ok(ok(J::Null))
                })?
            }
            Op::Call {
                method,
                args,
                chain,
            } => self.transaction(context, extensions, TransactionKind::Invocation, |a| {
                let ty = a.id.ty.clone();
                Ok(a.instance()?
                    .call_with_context(&ty, &method, &args, &chain, context))
            })?,
            Op::Alarm { now_ms } => self.fire_alarm(now_ms, context, extensions)?,
        };
        if segment.is_some() {
            self.alarm = self.db.lock().unwrap().alarm()?;
        }
        if is_alarm {
            if let Ok(CallOutput {
                value: J::Object(m),
                ..
            }) = &mut outcome
            {
                if let Some(a) = &self.alarm {
                    m.insert("armed".into(), J::String(wake_name(a)));
                }
            }
        }
        self.last_used = Instant::now();
        let alarm = (self.alarm != before).then_some(AlarmChange {
            before,
            after: self.alarm,
        });
        Ok(Executed {
            code_changed,
            outcome,
            segment,
            alarm,
        })
    }

    /// Runs `body` in one transaction, after any pending migrations. Only a
    /// successful return commits: a trap or a `result` method returning `err`
    /// rolls back. Returns the pages committed, if any.
    fn transaction(
        &mut self,
        context: &InvocationContext,
        extensions: &[Arc<dyn InvocationExtension>],
        kind: TransactionKind,
        body: impl FnOnce(&mut Self) -> Result<Outcome>,
    ) -> Result<(Outcome, Option<DurableChange>)> {
        self.db.lock().unwrap().begin()?;
        // Deadline failures participate in the same rollback path as vetoes.
        let mut outcome = if let Err(error) = context.check_deadline() {
            Ok(Err(CallError::Rejected(error)))
        } else {
            self.migrate().and_then(|()| body(self))
        };
        if let Ok(Ok(output)) = &outcome {
            if !output.is_err {
                let result = {
                    let mut db = self.db.lock().unwrap();
                    let mut state = TransactionState::new(&mut **db);
                    context.check_deadline().and_then(|()| {
                        for extension in extensions {
                            run_hook(context, || {
                                extension.before_commit(context, kind, &mut state, output)
                            })?;
                        }
                        context.check_deadline()
                    })
                };
                if let Err(error) = result {
                    outcome = Ok(Err(CallError::Rejected(error)));
                }
            }
        }
        let commit = matches!(&outcome, Ok(Ok(o)) if !o.is_err);
        {
            let mut db = self.db.lock().unwrap();
            if commit {
                if let Err(e) = db.commit() {
                    let _ = db.rollback();
                    return Err(e).context("commit");
                }
            } else {
                db.rollback().context("rollback")?;
            }
        }
        let outcome = outcome?;
        if commit {
            self.migrated = true;
        }
        if matches!(outcome, Err(CallError::Trap(_) | CallError::Rejected(_))) {
            self.instance = None;
        }
        let next_txid = self
            .txid
            .checked_add(1)
            .context("actor transaction id overflow")?;
        let segment = match self.db.lock().unwrap().capture(self.epoch, next_txid)? {
            Some(data) if commit => {
                self.txid = next_txid;
                Some(DurableChange {
                    txid: self.txid,
                    data,
                })
            }
            Some(_) => bail!("backend changed by a rolled back transaction"),
            None => None,
        };
        Ok((outcome, segment))
    }

    fn migrate(&mut self) -> Result<()> {
        if !self.migrated {
            let migrations = self
                .code
                .manifest
                .migrations
                .get(&self.id.ty)
                .cloned()
                .unwrap_or_default();
            let applied = self.db.lock().unwrap().apply_migrations(&migrations)?;
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
            self.instance = Some(self.code.instantiate_with(
                identity,
                self.db.clone(),
                self.caller.clone(),
            )?);
        }
        Ok(self.instance.as_mut().unwrap())
    }

    /// Fires the alarm if it is due. The handler runs in a transaction that
    /// also consumes the alarm (it may set a new one); if it fails, a second
    /// transaction schedules a retry with backoff, or drops the alarm after
    /// [`alarm::MAX_RETRIES`] retries.
    fn fire_alarm(
        &mut self,
        now_ms: u64,
        context: &InvocationContext,
        extensions: &[Arc<dyn InvocationExtension>],
    ) -> Result<(Outcome, Option<DurableChange>)> {
        let Some(due) = self.alarm.filter(|a| a.at_ms <= now_ms) else {
            return Ok((ok(serde_json::json!({ "fired": false })), None));
        };
        let ty = self.id.ty.clone();
        if !self
            .code
            .manifest
            .actor_type(&ty)
            .is_some_and(|t| t.alarm.is_some())
        {
            tracing::warn!(actor = %self.id, "alarm fired but the actor type has no alarm handler (removed by a deployment?); dropping it");
            let (o, seg) = self.transaction(
                context,
                extensions,
                TransactionKind::AlarmMaintenance,
                |a| {
                    a.db.lock().unwrap().clear_alarm()?;
                    Ok(ok(J::Null))
                },
            )?;
            return Ok((
                o.map(|_| CallOutput {
                    value: serde_json::json!({ "fired": false }),
                    is_err: false,
                }),
                seg,
            ));
        }
        let (outcome, seg) =
            self.transaction(context, extensions, TransactionKind::Invocation, |a| {
                a.db.lock().unwrap().clear_alarm()?;
                Ok(a.instance()?
                    .call_alarm_with_context(&ty, due.retry, context))
            })?;
        let error = match outcome {
            Ok(o) if !o.is_err => return Ok((ok(serde_json::json!({ "fired": true })), seg)),
            Ok(o) => format!("returned err: {}", o.value),
            Err(e @ CallError::Rejected(_)) => return Ok((Err(e), None)),
            Err(e) => e.to_string(),
        };
        let retry = due.retry + 1;
        let epoch = self.epoch;
        let id = self.id.clone();
        let (o, seg) = self.transaction(context, extensions, TransactionKind::AlarmMaintenance, |a| {
            let mut db = a.db.lock().unwrap();
            if retry > alarm::MAX_RETRIES {
                tracing::error!(actor = %id, "alarm handler failed {retry} times, dropping the alarm: {error}");
                db.clear_alarm()?;
            } else {
                let at = now_ms + alarm::backoff_ms(retry);
                tracing::warn!(actor = %id, "alarm handler failed (attempt {retry}), retrying at {at}: {error}");
                db.set_alarm(at, retry, epoch)?;
            }
            Ok(ok(J::Null))
        })?;
        Ok((
            o.map(|_| CallOutput {
                value: serde_json::json!({ "fired": true, "error": error }),
                is_err: false,
            }),
            seg,
        ))
    }

    /// Checkpoints and returns the path of the now complete database file.
    /// Blocking. The file stays valid only while the caller keeps exclusive
    /// access to this actor (no `execute`, `snapshot` or drop) — hold the
    /// slot lock until the upload has finished.
    pub fn snapshot(&mut self) -> Result<PathBuf> {
        self.db.lock().unwrap().checkpoint()
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
pub async fn compact(
    store: &DynStore,
    id: &ActorId,
    epoch: u64,
    txid: u64,
    image: &Path,
) -> Result<()> {
    let ep = id.epoch_prefix(epoch);
    store
        .put_file(&format!("{ep}{}", snapshot_name(txid)), image)
        .await?;
    gc(store, id, epoch, txid).await
}

/// Deletes objects of older epochs, and objects up to `txid` in `epoch`
/// (except the snapshot at `txid`). Safe once that snapshot is durable.
pub async fn gc(store: &DynStore, id: &ActorId, epoch: u64, txid: u64) -> Result<()> {
    let prefix = id.ltx_prefix();
    for k in store.list(&prefix).await? {
        if let Some(e) = k
            .strip_prefix(&prefix)
            .and_then(|s| s.strip_suffix("/backend.json"))
            .and_then(|s| s.strip_prefix('e'))
            .and_then(|s| s.parse::<u64>().ok())
        {
            if e < epoch {
                store.delete(&k).await?;
            }
            continue;
        }
        let Some((e, entry)) = k.strip_prefix(&prefix).and_then(parse_ltx) else {
            continue;
        };
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{deploy, start, InvOp, Invocation, NodeConfig};
    use serde::{Deserialize, Serialize};
    use statex_runtime::database::{Database, DatabaseFactory, SqlRows, SqlValue, SqliteFactory};
    use statex_runtime::{Manifest, Migration, Runtime};

    fn context() -> InvocationContext {
        InvocationContext::new(
            ActorRef {
                app: "counter".into(),
                actor_type: "counter".into(),
                key: "test".into(),
            },
            InvocationOperation::Create,
            Caller::Embedded,
            std::time::Duration::from_secs(60),
        )
        .unwrap()
    }

    struct CommitHook {
        id: u8,
        events: Arc<Mutex<Vec<(u8, TransactionKind)>>>,
        mode: u8,
    }

    impl InvocationExtension for CommitHook {
        fn before_commit(
            &self,
            _: &InvocationContext,
            kind: TransactionKind,
            state: &mut TransactionState<'_>,
            _: &CallOutput,
        ) -> std::result::Result<(), statex_runtime::invocation::HookError> {
            use statex_runtime::invocation::HookError;
            self.events.lock().unwrap().push((self.id, kind));
            state
                .execute(
                    "UPDATE counter SET value = value + ?1 WHERE id = 0",
                    &[SqlValue::Integer(10)],
                )
                .map_err(|e| HookError::Internal(e.to_string()))?;
            state
                .set_alarm(800, 0, 1)
                .map_err(|e| HookError::Internal(e.to_string()))?;
            assert!(state.execute("/* comment */ COMMIT", &[]).is_err());
            assert!(state.query("-- comment\nROLLBACK", &[]).is_err());
            match self.mode {
                1 => Err(HookError::Denied("commit veto".into())),
                2 => panic!("commit panic"),
                3 => {
                    std::thread::sleep(std::time::Duration::from_millis(250));
                    Ok(())
                }
                _ => Ok(()),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn commit_hooks_rollback_recover_and_run_for_read_only_calls() {
        let dir = workdir();
        let store = statex_store::open(dir.path().join("bucket").to_str().unwrap()).unwrap();
        let (wasm, manifest) = component();
        let code = Runtime::shared()
            .unwrap()
            .load(wasm, manifest.clone())
            .unwrap();
        for mode in [1, 2, 3] {
            let id = ActorId {
                app: "counter".into(),
                ty: "counter".into(),
                key: format!("veto-{mode}"),
            };
            let mut actor = activate(
                &store,
                dir.path(),
                &id,
                1,
                "etag".into(),
                true,
                code.clone(),
                Arc::new(JsonFactory),
            )
            .await
            .unwrap();
            let events = Arc::new(Mutex::new(vec![]));
            let extensions: Vec<Arc<dyn InvocationExtension>> = vec![
                Arc::new(CommitHook {
                    id: 1,
                    events: events.clone(),
                    mode: 0,
                }),
                Arc::new(CommitHook {
                    id: 2,
                    events: events.clone(),
                    mode,
                }),
            ];
            let mut ctx = context();
            if mode == 3 {
                ctx = InvocationContext::new(
                    ctx.target.clone(),
                    ctx.operation.clone(),
                    Caller::Embedded,
                    std::time::Duration::from_millis(200),
                )
                .unwrap();
                // Instantiate before the short deadline to isolate hook expiry.
                actor.instance().unwrap();
            }
            let executed = actor
                .execute_with_context(
                    &code,
                    Op::Call {
                        method: "increment".into(),
                        args: serde_json::json!({"by": 7}),
                        chain: vec![],
                    },
                    &ctx,
                    &extensions,
                )
                .unwrap();
            assert!(matches!(executed.outcome, Err(CallError::Rejected(_))));
            assert!(executed.segment.is_none());
            assert!(executed.alarm.is_none());
            assert!(!executed.code_changed);
            assert_eq!(actor.txid, 0);
            assert!(!actor.migrated);
            assert!(actor.instance.is_none());
            assert_eq!(
                *events.lock().unwrap(),
                vec![
                    (1, TransactionKind::Invocation),
                    (2, TransactionKind::Invocation)
                ]
            );
            assert_eq!(actor.db.lock().unwrap().alarm().unwrap(), None);
            assert_eq!(
                actor
                    .db
                    .lock()
                    .unwrap()
                    .query("SELECT value FROM counter WHERE id = 0", &[])
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Integer(0)]]
            );
            let recovery = actor
                .execute(
                    &code,
                    Op::Call {
                        method: "get".into(),
                        args: J::Null,
                        chain: vec![],
                    },
                )
                .unwrap();
            assert_eq!(recovery.outcome.unwrap().value, 0);
            assert!(actor.migrated);
            let extension: Vec<Arc<dyn InvocationExtension>> = vec![Arc::new(CommitHook {
                id: 3,
                events,
                mode: 0,
            })];
            let read = actor
                .execute_with_context(
                    &code,
                    Op::Call {
                        method: "get".into(),
                        args: J::Null,
                        chain: vec![],
                    },
                    &context(),
                    &extension,
                )
                .unwrap();
            assert_eq!(read.outcome.unwrap().value, 0);
            assert!(
                read.segment.is_some(),
                "hooks on read-only calls may write transactionally"
            );
            assert_eq!(
                actor
                    .db
                    .lock()
                    .unwrap()
                    .query("SELECT value FROM counter WHERE id = 0", &[])
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Integer(10)]]
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_and_alarm_maintenance_veto_preserve_state() {
        let dir = workdir();
        let store = statex_store::open(dir.path().join("bucket").to_str().unwrap()).unwrap();
        let (wasm, manifest) = component();
        let mut manifest = manifest.clone();
        manifest.types.iter_mut().for_each(|ty| ty.alarm = None);
        let code = Runtime::shared().unwrap().load(wasm, manifest).unwrap();
        let id = ActorId {
            app: "counter".into(),
            ty: "counter".into(),
            key: "maintenance".into(),
        };
        let mut actor = activate(
            &store,
            dir.path(),
            &id,
            1,
            "etag".into(),
            true,
            code.clone(),
            Arc::new(JsonFactory),
        )
        .await
        .unwrap();
        let events = Arc::new(Mutex::new(vec![]));
        let extensions: Vec<Arc<dyn InvocationExtension>> = vec![Arc::new(CommitHook {
            id: 1,
            events: events.clone(),
            mode: 1,
        })];
        let create = actor
            .execute_with_context(&code, Op::Touch, &context(), &extensions)
            .unwrap();
        assert!(matches!(create.outcome, Err(CallError::Rejected(_))));
        assert!(!actor.migrated);
        assert_eq!(actor.txid, 0);
        let (outcome, _) = actor
            .transaction(&context(), &[], TransactionKind::Invocation, |a| {
                a.db.lock().unwrap().set_alarm(100, 0, 1)?;
                Ok(ok(J::Null))
            })
            .unwrap();
        outcome.unwrap();
        actor.alarm = actor.db.lock().unwrap().alarm().unwrap();
        let before = actor.alarm;
        let txid = actor.txid;
        let alarm = actor
            .execute_with_context(&code, Op::Alarm { now_ms: 100 }, &context(), &extensions)
            .unwrap();
        assert!(matches!(alarm.outcome, Err(CallError::Rejected(_))));
        assert!(alarm.segment.is_none());
        assert_eq!(actor.txid, txid);
        assert_eq!(actor.alarm, before);
        assert_eq!(actor.db.lock().unwrap().alarm().unwrap(), before);
        assert_eq!(
            events.lock().unwrap().last(),
            Some(&(1, TransactionKind::AlarmMaintenance))
        );
        let alarm = actor.execute(&code, Op::Alarm { now_ms: 100 }).unwrap();
        assert_eq!(
            alarm.outcome.unwrap().value,
            serde_json::json!({"fired": false})
        );
        assert!(actor.alarm.is_none());
        let replacement = Runtime::shared()
            .unwrap()
            .load(wasm, code.manifest.clone())
            .unwrap();
        let replaced = actor
            .execute_with_context(&replacement, Op::Touch, &context(), &extensions)
            .unwrap();
        assert!(
            replaced.code_changed,
            "identity replacement is independent of commit success"
        );
        assert!(matches!(replaced.outcome, Err(CallError::Rejected(_))));
        assert!(!actor.migrated);
        let recovered = actor.execute(&replacement, Op::Touch).unwrap();
        assert!(!recovered.code_changed);
        assert!(actor.migrated);
    }

    struct AlarmVeto {
        kind: TransactionKind,
        events: Arc<Mutex<Vec<TransactionKind>>>,
    }

    impl InvocationExtension for AlarmVeto {
        fn before_commit(
            &self,
            _: &InvocationContext,
            kind: TransactionKind,
            _: &mut TransactionState<'_>,
            _: &CallOutput,
        ) -> std::result::Result<(), statex_runtime::invocation::HookError> {
            self.events.lock().unwrap().push(kind);
            if kind == self.kind {
                Err(statex_runtime::invocation::HookError::Denied(
                    "alarm veto".into(),
                ))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn alarm_handler_and_retry_maintenance_veto_never_ack_success() {
        let dir = workdir();
        let store = statex_store::open(dir.path().join("bucket").to_str().unwrap()).unwrap();
        let (wasm, manifest) = component();
        let code = Runtime::shared()
            .unwrap()
            .load(wasm, manifest.clone())
            .unwrap();
        let id = ActorId {
            app: "counter".into(),
            ty: "counter".into(),
            key: "handler".into(),
        };
        let mut actor = activate(
            &store,
            dir.path(),
            &id,
            1,
            "etag".into(),
            true,
            code.clone(),
            Arc::new(JsonFactory),
        )
        .await
        .unwrap();
        actor
            .execute(
                &code,
                Op::Call {
                    method: "schedule".into(),
                    args: serde_json::json!({"delay-ms": 0, "fail-times": 0}),
                    chain: vec![],
                },
            )
            .unwrap()
            .outcome
            .unwrap();
        let before = actor.alarm;
        let txid = actor.txid;
        let events = Arc::new(Mutex::new(vec![]));
        let veto: Vec<Arc<dyn InvocationExtension>> = vec![Arc::new(AlarmVeto {
            kind: TransactionKind::Invocation,
            events: events.clone(),
        })];
        let result = actor
            .execute_with_context(&code, Op::Alarm { now_ms: u64::MAX }, &context(), &veto)
            .unwrap();
        assert!(matches!(result.outcome, Err(CallError::Rejected(_))));
        assert!(result.segment.is_none());
        assert_eq!(actor.alarm, before);
        assert_eq!(actor.txid, txid);
        assert!(actor.instance.is_none());
        assert_eq!(*events.lock().unwrap(), vec![TransactionKind::Invocation]);
        assert_eq!(
            actor
                .db
                .lock()
                .unwrap()
                .query("SELECT fired FROM alarm_demo WHERE id = 0", &[])
                .unwrap()
                .rows,
            vec![vec![SqlValue::Integer(0)]]
        );

        actor
            .execute(
                &code,
                Op::Call {
                    method: "schedule".into(),
                    args: serde_json::json!({"delay-ms": 0, "fail-times": 1}),
                    chain: vec![],
                },
            )
            .unwrap()
            .outcome
            .unwrap();
        let before = actor.alarm;
        let txid = actor.txid;
        events.lock().unwrap().clear();
        let veto: Vec<Arc<dyn InvocationExtension>> = vec![Arc::new(AlarmVeto {
            kind: TransactionKind::AlarmMaintenance,
            events: events.clone(),
        })];
        let result = actor
            .execute_with_context(
                &code,
                Op::Alarm {
                    now_ms: before.unwrap().at_ms,
                },
                &context(),
                &veto,
            )
            .unwrap();
        assert!(matches!(result.outcome, Err(CallError::Rejected(_))));
        assert!(result.segment.is_none());
        assert_eq!(actor.alarm, before);
        assert_eq!(actor.txid, txid);
        assert_eq!(
            *events.lock().unwrap(),
            vec![TransactionKind::AlarmMaintenance]
        );
        let result = actor
            .execute(
                &code,
                Op::Alarm {
                    now_ms: before.unwrap().at_ms,
                },
            )
            .unwrap();
        assert!(result.outcome.unwrap().value["error"].is_string());
        assert_eq!(actor.alarm.unwrap().retry, 1);
        assert!(result.segment.is_some());
    }

    /// A deliberately non-SQLite backend: a JSON state machine implementing
    /// the SQL operations used by the counter component.
    #[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
    struct JsonState {
        value: i64,
        fail_times: i64,
        fired: i64,
        migrations: Vec<String>,
        alarm: Option<(u64, u32, u64, u64)>,
        seq: u64,
    }

    struct JsonDb {
        path: PathBuf,
        state: JsonState,
        transaction: Option<JsonState>,
        captured: JsonState,
        fail_alarm_read: bool,
        fail_alarm_clear: bool,
    }

    impl JsonDb {
        fn open(path: &Path) -> Result<Self> {
            let state = if path.exists() {
                serde_json::from_slice(&std::fs::read(path)?)?
            } else {
                JsonState::default()
            };
            Ok(Self {
                path: path.into(),
                captured: state.clone(),
                state,
                transaction: None,
                fail_alarm_read: false,
                fail_alarm_clear: false,
            })
        }
    }

    #[derive(Default)]
    struct JsonFactory;

    struct FailingAlarmFactory {
        read: bool,
        clear: bool,
    }

    impl DatabaseFactory for FailingAlarmFactory {
        fn identity(&self) -> BackendIdentity {
            JsonFactory.identity()
        }
        fn open(&self, path: &Path) -> Result<Box<dyn Database>> {
            let mut db = JsonDb::open(path)?;
            db.fail_alarm_read = self.read;
            db.fail_alarm_clear = self.clear;
            Ok(Box::new(db))
        }
        fn replay(&self, path: &Path, epoch: u64, txid: u64, change: &[u8]) -> Result<()> {
            JsonFactory.replay(path, epoch, txid, change)
        }
    }

    struct IncompatibleJsonFactory;

    impl DatabaseFactory for IncompatibleJsonFactory {
        fn identity(&self) -> BackendIdentity {
            BackendIdentity {
                format_version: 2,
                ..JsonFactory.identity()
            }
        }
        fn open(&self, _path: &Path) -> Result<Box<dyn Database>> {
            panic!("an incompatible factory must not interpret the stored snapshot")
        }
        fn replay(&self, _path: &Path, _epoch: u64, _txid: u64, _change: &[u8]) -> Result<()> {
            panic!("an incompatible factory must not interpret stored changes")
        }
    }

    #[derive(Serialize, Deserialize)]
    struct JsonChange {
        epoch: u64,
        txid: u64,
        state: JsonState,
    }

    impl DatabaseFactory for JsonFactory {
        fn identity(&self) -> BackendIdentity {
            BackendIdentity {
                name: "test.json-state".into(),
                format_version: 1,
            }
        }
        fn open(&self, path: &Path) -> Result<Box<dyn Database>> {
            Ok(Box::new(JsonDb::open(path)?))
        }
        fn replay(&self, path: &Path, epoch: u64, txid: u64, change: &[u8]) -> Result<()> {
            let change: JsonChange = serde_json::from_slice(change)?;
            anyhow::ensure!(
                change.epoch == epoch && change.txid == txid,
                "wrong JSON change identity"
            );
            std::fs::write(path, serde_json::to_vec(&change.state)?)?;
            Ok(())
        }
    }

    impl Database for JsonDb {
        fn execute(&mut self, statement: &str, params: &[SqlValue]) -> Result<u64> {
            match statement {
                "UPDATE counter SET value = value + ?1 WHERE id = 0" => {
                    let [SqlValue::Integer(by)] = params else {
                        bail!("integer required")
                    };
                    self.state.value += by;
                }
                "UPDATE alarm_demo SET fail_times = ?1 WHERE id = 0" => {
                    let [SqlValue::Integer(value)] = params else {
                        bail!("integer required")
                    };
                    self.state.fail_times = *value;
                }
                "UPDATE alarm_demo SET fired = fired + 1 WHERE id = 0" => {
                    self.state.fired += 1;
                }
                _ => bail!("unsupported SQL: {statement}"),
            }
            Ok(1)
        }
        fn query(&mut self, statement: &str, _params: &[SqlValue]) -> Result<SqlRows> {
            let value = match statement {
                "SELECT value FROM counter WHERE id = 0" => self.state.value,
                "SELECT fail_times FROM alarm_demo WHERE id = 0" => self.state.fail_times,
                "SELECT fired FROM alarm_demo WHERE id = 0" => self.state.fired,
                _ => bail!("unsupported query"),
            };
            Ok(SqlRows {
                columns: vec!["value".into()],
                rows: vec![vec![SqlValue::Integer(value)]],
            })
        }
        fn begin(&mut self) -> Result<()> {
            anyhow::ensure!(self.transaction.is_none(), "already in transaction");
            self.transaction = Some(self.state.clone());
            Ok(())
        }
        fn commit(&mut self) -> Result<()> {
            anyhow::ensure!(self.transaction.take().is_some(), "no transaction");
            Ok(())
        }
        fn rollback(&mut self) -> Result<()> {
            self.state = self.transaction.take().context("no transaction")?;
            Ok(())
        }
        fn apply_migrations(&mut self, migrations: &[Migration]) -> Result<Vec<String>> {
            let mut applied = Vec::new();
            for migration in migrations {
                if !self.state.migrations.contains(&migration.name) {
                    self.state.migrations.push(migration.name.clone());
                    applied.push(migration.name.clone());
                }
            }
            Ok(applied)
        }
        fn alarm(&mut self) -> Result<Option<Alarm>> {
            if self.fail_alarm_read && self.transaction.is_some() {
                bail!("backend alarm read failed");
            }
            Ok(self.state.alarm.map(|(at_ms, retry, epoch, seq)| Alarm {
                at_ms,
                retry,
                epoch,
                seq,
            }))
        }
        fn set_alarm(&mut self, at_ms: u64, retry: u32, epoch: u64) -> Result<()> {
            self.state.seq += 1;
            self.state.alarm = Some((at_ms, retry, epoch, self.state.seq));
            Ok(())
        }
        fn clear_alarm(&mut self) -> Result<()> {
            if self.fail_alarm_clear && self.transaction.is_some() {
                bail!("backend alarm clear failed");
            }
            self.state.alarm = None;
            Ok(())
        }
        fn capture(&mut self, epoch: u64, txid: u64) -> Result<Option<Vec<u8>>> {
            anyhow::ensure!(self.transaction.is_none(), "active transaction");
            if self.captured == self.state {
                return Ok(None);
            }
            self.captured = self.state.clone();
            Ok(Some(serde_json::to_vec(&JsonChange {
                epoch,
                txid,
                state: self.state.clone(),
            })?))
        }
        fn checkpoint(&mut self) -> Result<PathBuf> {
            anyhow::ensure!(self.transaction.is_none(), "active transaction");
            std::fs::write(&self.path, serde_json::to_vec(&self.state)?)?;
            self.captured = self.state.clone();
            Ok(self.path.clone())
        }
    }

    fn component() -> &'static (Vec<u8>, Manifest) {
        static CODE: std::sync::OnceLock<(Vec<u8>, Manifest)> = std::sync::OnceLock::new();
        CODE.get_or_init(|| {
            let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/counter");
            let status = std::process::Command::new("cargo")
                .args(["build", "--quiet", "--release", "--target", "wasm32-wasip2"])
                .current_dir(&root)
                .status()
                .unwrap();
            assert!(status.success());
            let wasm =
                std::fs::read(root.join("target/wasm32-wasip2/release/counter.wasm")).unwrap();
            let migrations = std::collections::BTreeMap::from([(
                "counter".into(),
                vec![Migration {
                    name: "init".into(),
                    sql: "initialize counter state".into(),
                }],
            )]);
            let manifest = Manifest::build(
                &wasm,
                "counter",
                migrations,
                Default::default(),
                Default::default(),
            )
            .unwrap();
            (wasm, manifest)
        })
    }

    fn workdir() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("state-backend-test-")
            .tempdir_in(env!("CARGO_MANIFEST_DIR"))
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn infallible_alarm_import_errors_trap_and_roll_back_transaction() {
        let dir = workdir();
        let store = statex_store::open(dir.path().join("bucket").to_str().unwrap()).unwrap();
        let (wasm, manifest) = component();
        let code = Runtime::shared()
            .unwrap()
            .load(wasm, manifest.clone())
            .unwrap();
        for (read, clear, method, expected) in [
            (true, false, "alarm-at", "read alarm"),
            (false, true, "cancel", "clear alarm"),
        ] {
            let id = ActorId {
                app: "counter".into(),
                ty: "counter".into(),
                key: method.into(),
            };
            let mut actor = activate(
                &store,
                dir.path(),
                &id,
                1,
                "etag".into(),
                true,
                code.clone(),
                Arc::new(FailingAlarmFactory { read, clear }),
            )
            .await
            .unwrap();
            let (outcome, change) = actor
                .transaction(&context(), &[], TransactionKind::Invocation, |a| {
                    {
                        let mut db = a.db.lock().unwrap();
                        db.execute(
                            "UPDATE counter SET value = value + ?1 WHERE id = 0",
                            &[SqlValue::Integer(7)],
                        )?;
                        db.set_alarm(100, 0, 1)?;
                    }
                    Ok(a.instance()?.call("counter", method, &J::Null))
                })
                .unwrap();
            match outcome {
                Err(CallError::Trap(error)) => assert!(error.contains(expected), "{error}"),
                other => panic!("expected capability failure trap, got {other:?}"),
            }
            assert!(change.is_none());
            assert_eq!(actor.txid, 0);
            assert!(!actor.migrated);
            assert!(
                actor.instance.is_none(),
                "a capability trap must discard the instance"
            );
            let mut db = actor.db.lock().unwrap();
            assert_eq!(db.alarm().unwrap(), None);
            assert_eq!(
                db.query("SELECT value FROM counter WHERE id = 0", &[])
                    .unwrap()
                    .rows,
                vec![vec![SqlValue::Integer(0)]]
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn custom_backend_node_injection_and_restore() {
        let dir = workdir();
        let store = statex_store::open(dir.path().join("bucket").to_str().unwrap()).unwrap();
        let (wasm, manifest) = component();
        deploy::deploy(&store, wasm, manifest, &Default::default())
            .await
            .unwrap();
        let config = |name: &str| {
            let mut cfg = NodeConfig::new(name, store.clone(), dir.path().join(name));
            cfg.database_factory = Arc::new(JsonFactory);
            cfg
        };
        let invocation = |method: &str, args: J| Invocation {
            app: "counter".into(),
            ty: "counter".into(),
            key: "alice".into(),
            op: InvOp::Call {
                method: method.into(),
                args,
            },
            chain: vec![],
        };
        let a = start(config("a")).await.unwrap();
        let result = a
            .node
            .invoke(invocation("increment", serde_json::json!({"by": 7})), 0)
            .await;
        assert_eq!(result.status, 200, "{result:?}");
        assert_eq!(result.body["result"], 7);
        let unsupported = a.node.invoke(invocation("reset", J::Null), 0).await;
        assert_eq!(
            unsupported.status, 500,
            "unsupported SQL must not silently succeed: {unsupported:?}"
        );
        assert_eq!(unsupported.body["error"]["code"], "trap");
        assert_eq!(
            a.node.invoke(invocation("get", J::Null), 0).await.body["result"],
            7
        );
        let result = a
            .node
            .invoke(
                invocation(
                    "schedule",
                    serde_json::json!({"delay-ms": 600_000, "fail-times": 0}),
                ),
                0,
            )
            .await;
        assert_eq!(result.status, 200, "{result:?}");
        let alarm_at =
            a.node.invoke(invocation("alarm-at", J::Null), 0).await.body["result"].clone();
        a.shutdown().await;

        let b = start(config("b")).await.unwrap();
        assert_eq!(
            b.node.invoke(invocation("get", J::Null), 0).await.body["result"],
            7
        );
        assert_eq!(
            b.node.invoke(invocation("alarm-at", J::Null), 0).await.body["result"],
            alarm_at
        );
        assert_eq!(
            b.node
                .invoke(invocation("increment", serde_json::json!({"by": 2})), 0)
                .await
                .body["result"],
            9
        );
        let id = ActorId {
            app: "counter".into(),
            ty: "counter".into(),
            key: "alice".into(),
        };
        let identity =
            get_json::<BackendIdentity>(&*store, &format!("{}backend.json", id.epoch_prefix(2)))
                .await
                .unwrap()
                .unwrap()
                .0;
        assert_eq!(identity, JsonFactory.identity());
        b.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn custom_backend_rolls_back_and_rejects_incompatible_restore() {
        let dir = workdir();
        let store = statex_store::open(dir.path().join("bucket").to_str().unwrap()).unwrap();
        let (wasm, manifest) = component();
        let code = Runtime::shared()
            .unwrap()
            .load(wasm, manifest.clone())
            .unwrap();
        let id = ActorId {
            app: "counter".into(),
            ty: "counter".into(),
            key: "rollback".into(),
        };
        let mut actor = activate(
            &store,
            dir.path(),
            &id,
            1,
            "etag".into(),
            true,
            code.clone(),
            Arc::new(JsonFactory),
        )
        .await
        .unwrap();
        let (outcome, change) = actor
            .transaction(&context(), &[], TransactionKind::Invocation, |a| {
                let mut db = a.db.lock().unwrap();
                db.execute(
                    "UPDATE counter SET value = value + ?1 WHERE id = 0",
                    &[SqlValue::Integer(5)],
                )?;
                db.set_alarm(100, 0, 1)?;
                Ok(Ok(CallOutput {
                    value: J::Null,
                    is_err: true,
                }))
            })
            .unwrap();
        assert!(outcome.unwrap().is_err);
        assert!(change.is_none());
        assert!(!actor.migrated, "rolled-back migrations must be retried");
        assert_eq!(actor.txid, 0);
        assert_eq!(actor.db.lock().unwrap().alarm().unwrap(), None);
        let (outcome, change) = actor
            .transaction(&context(), &[], TransactionKind::Invocation, |a| {
                a.db.lock().unwrap().execute(
                    "UPDATE counter SET value = value + ?1 WHERE id = 0",
                    &[SqlValue::Integer(8)],
                )?;
                Ok(Err(CallError::Trap("test trap".into())))
            })
            .unwrap();
        assert!(matches!(outcome, Err(CallError::Trap(_))));
        assert!(change.is_none());
        let result = actor
            .execute(
                &code,
                Op::Call {
                    method: "get".into(),
                    args: J::Null,
                    chain: vec![],
                },
            )
            .unwrap();
        assert_eq!(result.outcome.unwrap().value, 0);
        let change = result.segment.unwrap();
        store
            .put(
                &format!("{}{}", id.epoch_prefix(1), segment_name(change.txid)),
                change.data.into(),
            )
            .await
            .unwrap();
        drop(actor);
        let restored = activate(
            &store,
            dir.path(),
            &id,
            2,
            "etag".into(),
            false,
            code.clone(),
            Arc::new(SqliteFactory),
        )
        .await;
        assert!(restored
            .err()
            .unwrap()
            .to_string()
            .contains("incompatible actor state backend"));
        let restored = activate(
            &store,
            dir.path(),
            &id,
            2,
            "etag".into(),
            false,
            code.clone(),
            Arc::new(IncompatibleJsonFactory),
        )
        .await;
        assert!(restored
            .err()
            .unwrap()
            .to_string()
            .contains("incompatible actor state backend"));
        let mut restored = activate(
            &store,
            dir.path(),
            &id,
            2,
            "etag".into(),
            false,
            code.clone(),
            Arc::new(JsonFactory),
        )
        .await
        .unwrap();
        assert_eq!(
            restored
                .execute(&code, Op::Touch)
                .unwrap()
                .segment
                .map(|c| c.txid),
            None
        );
        let (outcome, _) = restored
            .transaction(&context(), &[], TransactionKind::Invocation, |a| {
                let mut db = a.db.lock().unwrap();
                db.execute(
                    "UPDATE counter SET value = value + ?1 WHERE id = 0",
                    &[SqlValue::Integer(13)],
                )?;
                db.set_alarm(900, 0, 2)?;
                Ok(ok(J::Null))
            })
            .unwrap();
        outcome.unwrap();
        let image = restored.snapshot().unwrap();
        compact(&store, &id, 2, restored.txid, &image)
            .await
            .unwrap();
        assert!(store
            .get(&format!("{}backend.json", id.epoch_prefix(1)))
            .await
            .unwrap()
            .is_none());
        assert!(store
            .get(&format!("{}backend.json", id.epoch_prefix(2)))
            .await
            .unwrap()
            .is_some());
        drop(restored);
        let mut restored = activate(
            &store,
            dir.path(),
            &id,
            3,
            "etag".into(),
            false,
            code.clone(),
            Arc::new(JsonFactory),
        )
        .await
        .unwrap();
        assert_eq!(
            restored.alarm,
            Some(Alarm {
                at_ms: 900,
                retry: 0,
                epoch: 2,
                seq: 1
            })
        );
        assert_eq!(
            restored
                .execute(
                    &code,
                    Op::Call {
                        method: "get".into(),
                        args: J::Null,
                        chain: vec![],
                    }
                )
                .unwrap()
                .outcome
                .unwrap()
                .value,
            13
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn existing_snapshots_require_backend_identity_for_all_factories() {
        let dir = workdir();
        let store = statex_store::open(dir.path().join("bucket").to_str().unwrap()).unwrap();
        let (wasm, manifest) = component();
        let mut manifest = manifest.clone();
        manifest.migrations.get_mut("counter").unwrap()[0].sql =
            "CREATE TABLE counter(id INTEGER PRIMARY KEY, value INTEGER);
             INSERT INTO counter VALUES(0, 0);"
                .into();
        let code = Runtime::shared().unwrap().load(wasm, manifest).unwrap();
        let id = ActorId {
            app: "counter".into(),
            ty: "counter".into(),
            key: "missing-identity".into(),
        };
        let mut actor = activate(
            &store,
            dir.path(),
            &id,
            1,
            "etag".into(),
            true,
            code.clone(),
            Arc::new(SqliteFactory),
        )
        .await
        .unwrap();
        let result = actor
            .execute(
                &code,
                Op::Call {
                    method: "increment".into(),
                    args: serde_json::json!({"by": 21}),
                    chain: vec![],
                },
            )
            .unwrap();
        assert_eq!(result.outcome.unwrap().value, 21);
        let change = result.segment.unwrap();
        store
            .put(
                &format!("{}{}", id.epoch_prefix(1), segment_name(change.txid)),
                change.data.into(),
            )
            .await
            .unwrap();
        store
            .delete(&format!("{}backend.json", id.epoch_prefix(1)))
            .await
            .unwrap();
        drop(actor);
        for factory in [
            Arc::new(SqliteFactory) as DynDatabaseFactory,
            Arc::new(JsonFactory) as DynDatabaseFactory,
        ] {
            let error = activate(
                &store,
                dir.path(),
                &id,
                2,
                "etag".into(),
                false,
                code.clone(),
                factory,
            )
            .await
            .err()
            .expect("snapshots without format identity must be rejected");
            assert!(
                error.to_string().contains("missing backend identity"),
                "{error:#}"
            );
        }
        assert!(store
            .get(&format!("{}backend.json", id.epoch_prefix(2)))
            .await
            .unwrap()
            .is_none());
        store
            .put(
                &format!("{}backend.json", id.epoch_prefix(1)),
                to_json_bytes(&SqliteFactory.identity()),
            )
            .await
            .unwrap();
        let mut actor = activate(
            &store,
            dir.path(),
            &id,
            2,
            "etag".into(),
            false,
            code.clone(),
            Arc::new(SqliteFactory),
        )
        .await
        .unwrap();
        assert_eq!(
            actor
                .execute(
                    &code,
                    Op::Call {
                        method: "get".into(),
                        args: J::Null,
                        chain: vec![],
                    }
                )
                .unwrap()
                .outcome
                .unwrap()
                .value,
            21
        );
    }
}
