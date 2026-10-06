use super::*;

fn directory() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("sqlite-backend-test-")
        .tempdir_in(env!("CARGO_MANIFEST_DIR"))
        .unwrap()
}

#[test]
fn sqlite_transaction_capture_snapshot_and_replay() {
    let dir = directory();
    let source = dir.path().join("source.db");
    let restored = dir.path().join("restored.db");
    let mut db = SqliteFactory.open(&source).unwrap();
    std::fs::copy(db.checkpoint().unwrap(), &restored).unwrap();
    let migrations = [Migration {
        name: "init".into(),
        sql: "CREATE TABLE values_test(i INTEGER, r REAL, t TEXT, b BLOB, n);".into(),
    }];
    db.begin().unwrap();
    assert_eq!(db.apply_migrations(&migrations).unwrap(), ["init"]);
    db.set_alarm(123, 0, 7).unwrap();
    db.rollback().unwrap();
    assert!(db.capture(7, 1).unwrap().is_none());
    assert_eq!(db.alarm().unwrap(), None);

    db.begin().unwrap();
    assert_eq!(db.apply_migrations(&migrations).unwrap(), ["init"]);
    let values = [
        SqlValue::Integer(-42),
        SqlValue::Real(3.25),
        SqlValue::Text("hello".into()),
        SqlValue::Blob(vec![0, 1, 255]),
        SqlValue::Null,
    ];
    db.execute(
        "INSERT INTO values_test VALUES(?1, ?2, ?3, ?4, ?5)",
        &values,
    )
    .unwrap();
    db.set_alarm(456, 2, 7).unwrap();
    db.commit().unwrap();
    let change = db.capture(7, 1).unwrap().unwrap();
    assert!(db.capture(7, 2).unwrap().is_none());
    SqliteFactory.replay(&restored, 7, 1, &change).unwrap();
    let mut copy = SqliteFactory.open(&restored).unwrap();
    assert_eq!(
        copy.query("SELECT * FROM values_test", &[]).unwrap().rows,
        vec![values.to_vec()]
    );
    assert_eq!(
        copy.alarm().unwrap(),
        Some(alarm::Alarm {
            at_ms: 456,
            retry: 2,
            epoch: 7,
            seq: 1
        })
    );
    copy.begin().unwrap();
    assert!(copy.apply_migrations(&migrations).unwrap().is_empty());
    copy.rollback().unwrap();
    drop(copy);

    db.checkpoint().unwrap();
    std::fs::copy(&source, &restored).unwrap();
    db.begin().unwrap();
    db.clear_alarm().unwrap();
    db.set_alarm(789, 0, 8).unwrap();
    db.commit().unwrap();
    let change = db.capture(8, 2).unwrap().unwrap();
    SqliteFactory.replay(&restored, 8, 2, &change).unwrap();
    let mut copy = SqliteFactory.open(&restored).unwrap();
    assert_eq!(
        copy.alarm().unwrap(),
        Some(alarm::Alarm {
            at_ms: 789,
            retry: 0,
            epoch: 8,
            seq: 2
        })
    );
    assert_eq!(
        copy.query("SELECT * FROM values_test", &[]).unwrap().rows,
        vec![values.to_vec()]
    );
}

#[test]
fn sqlite_replay_rejects_corruption_and_wrong_identity_before_writing() {
    let dir = directory();
    let path = dir.path().join("image.db");
    let mut db = SqliteFactory.open(&path).unwrap();
    db.begin().unwrap();
    db.execute("CREATE TABLE t(v)", &[]).unwrap();
    db.commit().unwrap();
    let change = db.capture(3, 1).unwrap().unwrap();
    let target = dir.path().join("target.db");
    std::fs::write(&target, b"unchanged").unwrap();
    assert!(SqliteFactory
        .replay(&target, 4, 1, &change)
        .unwrap_err()
        .to_string()
        .contains("epoch"));
    assert!(SqliteFactory
        .replay(&target, 3, 2, &change)
        .unwrap_err()
        .to_string()
        .contains("txid"));
    let mut corrupt = change.clone();
    corrupt[40] ^= 1;
    assert!(SqliteFactory
        .replay(&target, 3, 1, &corrupt)
        .unwrap_err()
        .to_string()
        .contains("checksum"));
    let mut invalid = Segment::decode(&change).unwrap();
    let data = invalid.pages.pages.values().next().unwrap().clone();
    invalid.pages.pages.insert(0, data);
    assert!(SqliteFactory
        .replay(&target, 3, 1, &invalid.encode())
        .is_err());
    assert_eq!(std::fs::read(target).unwrap(), b"unchanged");
    db.begin().unwrap();
    assert!(db
        .checkpoint()
        .unwrap_err()
        .to_string()
        .contains("active transaction"));
    db.rollback().unwrap();
}

#[test]
fn sqlite_failed_migrations_and_alarm_changes_roll_back_together() {
    let dir = directory();
    let mut db = SqliteFactory.open(&dir.path().join("db")).unwrap();
    db.begin().unwrap();
    db.set_alarm(100, 0, 1).unwrap();
    let migrations = [
        Migration {
            name: "one".into(),
            sql: "CREATE TABLE first(v)".into(),
        },
        Migration {
            name: "two".into(),
            sql: "not valid SQL".into(),
        },
    ];
    assert!(db.apply_migrations(&migrations).is_err());
    db.rollback().unwrap();
    assert!(db.capture(1, 1).unwrap().is_none());
    assert!(db.query("SELECT * FROM first", &[]).is_err());
    assert_eq!(db.alarm().unwrap(), None);
    db.begin().unwrap();
    assert_eq!(db.apply_migrations(&migrations[..1]).unwrap(), ["one"]);
    db.commit().unwrap();
    assert!(db.capture(1, 1).unwrap().is_some());
}

fn outbox_job() -> Job {
    let mut job = Job::new(
        crate::ActorRef { app: "shop".into(), actor_type: "counter".into(), key: "alice".into() },
        "add".into(),
        serde_json::json!([1]),
        0,
        crate::invocation::InvocationMetadata::new(
            crate::invocation::Caller::Embedded, std::time::Duration::from_secs(1),
        ).unwrap(),
    ).unwrap();
    job.not_before_ms = 100;
    job.next_attempt_ms = 100;
    job
}

#[test]
fn sqlite_outbox_requires_transaction_and_rolls_back_jobs_without_schema_changes() {
    let dir = directory();
    let mut db = SqliteFactory.open(&dir.path().join("db")).unwrap();
    let job = outbox_job();
    assert_eq!(db.job(&job.id).unwrap(), None);
    assert!(!db.has_pending_jobs().unwrap());
    assert!(db.capture(1, 1).unwrap().is_none());
    for error in [
        db.enqueue_job(&job).unwrap_err(),
        db.update_job(&job).unwrap_err(),
        db.claim_job(100, 200).unwrap_err(),
    ] {
        assert!(error.to_string().contains("active transaction"), "{error}");
    }
    assert_eq!(
        db.query("SELECT name FROM sqlite_schema WHERE name = '_statex_outbox'", &[]).unwrap().rows.len(),
        1,
    );
    db.begin().unwrap();
    assert!(db.claim_job(100, 200).unwrap().is_none());
    crate::outbox::enqueue(db.as_mut(), &job).unwrap();
    assert!(db.has_pending_jobs().unwrap());
    assert_eq!(db.job(&job.id).unwrap(), Some(job.clone()));
    db.rollback().unwrap();
    assert_eq!(db.job(&job.id).unwrap(), None);
    assert!(!db.has_pending_jobs().unwrap());
    assert!(db.capture(1, 1).unwrap().is_none());
    assert_eq!(
        db.query("SELECT name FROM sqlite_schema WHERE name = '_statex_outbox'", &[]).unwrap().rows.len(),
        1,
    );
    db.begin().unwrap();
    db.enqueue_job(&job).unwrap();
    assert!(db.enqueue_job(&job).is_err(), "IDs must be unique");
    db.commit().unwrap();
    assert_eq!(db.job(&job.id).unwrap(), Some(job));
    assert!(db.capture(1, 1).unwrap().is_some());
    assert!(db.job("missing").unwrap().is_none());
    assert!(db.capture(1, 2).unwrap().is_none(), "inspection must be read-only");
}

#[test]
fn sqlite_outbox_claim_retry_recovery_and_terminal_inspection() {
    let dir = directory();
    let mut db = SqliteFactory.open(&dir.path().join("db")).unwrap();
    let job = outbox_job();
    db.begin().unwrap();
    db.enqueue_job(&job).unwrap();
    db.commit().unwrap();
    db.begin().unwrap();
    assert!(db.claim_job(99, 200).unwrap().is_none());
    assert!(db.claim_job(100, 100).is_err());
    assert!(db.claim_job(100, u64::MAX).is_err());
    let claimed = db.claim_job(100, 200).unwrap().unwrap();
    assert_eq!(claimed.status, JobStatus::Running);
    assert_eq!(claimed.attempts, 1);
    assert_eq!(claimed.next_attempt_ms, 200);
    assert!(db.claim_job(100, 200).unwrap().is_none());
    db.rollback().unwrap();
    assert_eq!(db.job(&job.id).unwrap(), Some(job.clone()));
    db.begin().unwrap();
    db.claim_job(100, 200).unwrap().unwrap();
    db.commit().unwrap();
    assert!(db.has_pending_jobs().unwrap(), "running jobs require a recovery hint");
    db.begin().unwrap();
    assert!(db.claim_job(199, 300).unwrap().is_none());
    let recovered = db.claim_job(200, 300).unwrap().unwrap();
    assert_eq!(recovered.attempts, 2);
    db.commit().unwrap();
    let mut claimed = recovered;
    claimed.status = JobStatus::Pending;
    claimed.error = Some(serde_json::json!({"code": "unavailable", "message": "temporarily unavailable"}));
    claimed.next_attempt_ms = 400;
    db.begin().unwrap();
    db.update_job(&claimed).unwrap();
    db.commit().unwrap();
    db.begin().unwrap();
    assert!(db.claim_job(399, 500).unwrap().is_none());
    let mut retried = db.claim_job(400, 500).unwrap().unwrap();
    assert_eq!(retried.attempts, 3);
    retried.status = JobStatus::Succeeded;
    retried.result = Some(serde_json::json!({"value": 42}));
    retried.error = None;
    db.update_job(&retried).unwrap();
    assert!(!db.has_pending_jobs().unwrap());
    db.commit().unwrap();
    assert_eq!(db.job(&job.id).unwrap(), Some(retried.clone()));
    db.begin().unwrap();
    assert!(db.claim_job(1000, 2000).unwrap().is_none());
    assert!(db.update_job(&retried).is_err(), "terminal records cannot be overwritten");
    let failed = outbox_job();
    db.enqueue_job(&failed).unwrap();
    let mut failed = db.claim_job(1000, 2000).unwrap().unwrap();
    failed.status = JobStatus::Failed;
    failed.error = Some(serde_json::json!({"err": "target method no longer exists"}));
    db.update_job(&failed).unwrap();
    assert!(db.claim_job(1000, 2000).unwrap().is_none());
    db.commit().unwrap();
    assert_eq!(db.job(&job.id).unwrap(), Some(retried));
    assert_eq!(db.job(&failed.id).unwrap(), Some(failed));
    assert!(!db.has_pending_jobs().unwrap());
}

#[test]
fn sqlite_outbox_wal_replay_preserves_claims_and_completion() {
    let dir = directory();
    let source = dir.path().join("source.db");
    let restored = dir.path().join("restored.db");
    let mut db = SqliteFactory.open(&source).unwrap();
    std::fs::copy(db.checkpoint().unwrap(), &restored).unwrap();
    let job = outbox_job();
    db.begin().unwrap();
    db.enqueue_job(&job).unwrap();
    db.commit().unwrap();
    let change = db.capture(7, 1).unwrap().unwrap();
    SqliteFactory.replay(&restored, 7, 1, &change).unwrap();
    let mut copy = SqliteFactory.open(&restored).unwrap();
    assert_eq!(copy.job(&job.id).unwrap(), Some(job.clone()));
    assert!(copy.has_pending_jobs().unwrap());
    drop(copy);
    db.begin().unwrap();
    let mut claimed = db.claim_job(100, 200).unwrap().unwrap();
    db.commit().unwrap();
    let change = db.capture(7, 2).unwrap().unwrap();
    SqliteFactory.replay(&restored, 7, 2, &change).unwrap();
    let mut copy = SqliteFactory.open(&restored).unwrap();
    assert_eq!(copy.job(&job.id).unwrap(), Some(claimed.clone()));
    copy.begin().unwrap();
    assert!(copy.claim_job(199, 300).unwrap().is_none());
    assert_eq!(copy.claim_job(200, 300).unwrap().unwrap().attempts, 2);
    copy.rollback().unwrap();
    drop(copy);
    claimed.status = JobStatus::Succeeded;
    claimed.result = Some(serde_json::json!(2));
    db.begin().unwrap();
    db.update_job(&claimed).unwrap();
    db.commit().unwrap();
    let change = db.capture(7, 3).unwrap().unwrap();
    SqliteFactory.replay(&restored, 7, 3, &change).unwrap();
    let mut copy = SqliteFactory.open(&restored).unwrap();
    assert_eq!(copy.job(&job.id).unwrap(), Some(claimed));
    assert!(!copy.has_pending_jobs().unwrap());
}

#[test]
fn sqlite_connection_adapter_supports_outbox_and_validation_is_atomic() {
    let conn = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
    let handle = sqlite_handle(conn.clone());
    let mut db = handle.lock().unwrap();
    let mut job = outbox_job();
    db.begin().unwrap();
    job.args = serde_json::json!({});
    assert!(db.enqueue_job(&job).is_err());
    assert!(!outbox_exists(&conn.lock().unwrap()).unwrap());
    job.args = serde_json::json!([]);
    job.next_attempt_ms = u64::MAX;
    assert!(db.enqueue_job(&job).is_err());
    job.next_attempt_ms = job.not_before_ms;
    db.enqueue_job(&job).unwrap();
    db.commit().unwrap();
    assert_eq!(db.job(&job.id).unwrap(), Some(job.clone()));
    let mut missing = job.clone();
    missing.id = "unknown".into();
    db.begin().unwrap();
    assert!(db.update_job(&missing).is_err());
    job.attempts = u32::MAX;
    conn.lock().unwrap().execute("UPDATE _statex_outbox SET record = ?1 WHERE id = ?2",
        rusqlite::params![serde_json::to_string(&job).unwrap(), job.id],
    ).unwrap();
    assert!(db.claim_job(100, 200).is_err());
    db.rollback().unwrap();
    assert_eq!(db.job(&job.id).unwrap().unwrap().attempts, 0);
}

#[test]
fn sqlite_outbox_completion_rejects_stale_attempts_and_preserves_recovered_job() {
    let dir = directory();
    let mut db = SqliteFactory.open(&dir.path().join("db")).unwrap();
    let job = outbox_job();
    db.begin().unwrap();
    db.enqueue_job(&job).unwrap();
    assert!(db.update_job(&job).is_err(), "unclaimed jobs cannot be completed");
    let mut first = db.claim_job(100, 200).unwrap().unwrap();
    db.commit().unwrap();
    db.begin().unwrap();
    let mut second = db.claim_job(200, 300).unwrap().unwrap();
    db.commit().unwrap();
    first.status = JobStatus::Succeeded;
    first.result = Some(serde_json::json!({"stale": true}));
    db.begin().unwrap();
    let error = db.update_job(&first).unwrap_err();
    assert!(error.to_string().contains("stale outbox completion"));
    assert_eq!(db.job(&job.id).unwrap(), Some(second.clone()));
    let mut altered = second.clone();
    altered.status = JobStatus::Succeeded;
    altered.args = serde_json::json!([99]);
    assert!(db.update_job(&altered).is_err());
    second.status = JobStatus::Succeeded;
    second.result = Some(serde_json::json!({"current": true}));
    db.update_job(&second).unwrap();
    assert!(db.update_job(&second).is_err(), "duplicate completion must be rejected");
    db.commit().unwrap();
    assert_eq!(db.job(&job.id).unwrap(), Some(second));
}

#[test]
fn sqlite_guest_sql_cannot_read_forge_or_shadow_host_outbox() {
    let dir = directory();
    let mut db = SqliteFactory.open(&dir.path().join("db")).unwrap();
    db.begin().unwrap();
    for statement in [
        "CREATE TABLE _statex_outbox(record TEXT)",
        "CREATE TEMP TABLE \"_STATEX_OUTBOX\"(record TEXT)",
        "CREATE VIEW _statex_outbox AS SELECT 'forged' AS record",
        "CREATE TEMP VIEW _statex_outbox AS SELECT 'forged' AS record",
    ] {
        assert!(db.execute(statement, &[]).is_err(), "{statement}");
    }
    db.execute("CREATE TABLE guest_data(value TEXT)", &[]).unwrap();
    db.execute("INSERT INTO guest_data VALUES('legitimate')", &[]).unwrap();
    assert!(db.execute("ALTER TABLE guest_data RENAME TO _statex_outbox", &[]).is_err());
    assert_eq!(db.query("SELECT value FROM guest_data", &[]).unwrap().rows,
        vec![vec![SqlValue::Text("legitimate".into())]]);
    let job = outbox_job();
    db.enqueue_job(&job).unwrap();
    for statement in [
        "/* SQL comments do not evade authorization */ UPDATE \"_STATEX_OUTBOX\" SET record='{}'",
        "INSERT INTO main._statex_outbox VALUES('forged', 'pending', 0, '{}')",
        "DELETE FROM _statex_outbox",
        "DROP TABLE _statex_outbox",
        "ALTER TABLE _statex_outbox ADD COLUMN forged TEXT",
        "DROP INDEX _statex_outbox_due",
        "CREATE INDEX guest_index ON _statex_outbox(status)",
        "CREATE TRIGGER forge AFTER INSERT ON _statex_outbox BEGIN UPDATE _statex_outbox SET record='{}'; END",
        "CREATE TEMP TRIGGER forge AFTER INSERT ON main._statex_outbox BEGIN UPDATE _statex_outbox SET record='{}'; END",
        "PRAGMA writable_schema=ON",
    ] {
        assert!(db.execute(statement, &[]).is_err(), "{statement}");
    }
    for statement in [
        "SELECT record FROM _statex_outbox",
        "SELECT * FROM \"_STATEX_OUTBOX\"",
        "SELECT count(*) FROM main._statex_outbox",
        "UPDATE _statex_outbox SET record='{}' RETURNING record",
    ] {
        assert!(db.query(statement, &[]).is_err(), "{statement}");
    }
    db.execute("CREATE VIEW guest_jobs AS SELECT record FROM _statex_outbox", &[]).unwrap();
    assert!(db.query("SELECT * FROM guest_jobs", &[]).is_err());
    db.execute(
        "CREATE TRIGGER forge_indirect AFTER INSERT ON guest_data
         BEGIN UPDATE _statex_outbox SET record='{}'; END", &[],
    ).unwrap();
    assert!(db.execute("INSERT INTO guest_data VALUES('forged')", &[]).is_err());
    db.execute("DROP TRIGGER forge_indirect", &[]).unwrap();
    db.set_alarm(123, 0, 1).unwrap();
    assert!(db.execute(
        "CREATE TRIGGER forge_from_alarm AFTER INSERT ON _statex_alarm
         BEGIN UPDATE _statex_outbox SET record='{}'; END", &[],
    ).is_err());
    assert_eq!(db.job(&job.id).unwrap(), Some(job.clone()), "trusted host API remains readable");
    let mut claimed = db.claim_job(100, 200).unwrap().unwrap();
    claimed.status = JobStatus::Succeeded;
    claimed.result = Some(serde_json::json!("trusted"));
    db.update_job(&claimed).unwrap();
    db.commit().unwrap();
    assert_eq!(db.job(&job.id).unwrap(), Some(claimed));
}

#[test]
fn sqlite_guest_migrations_cannot_modify_trusted_outbox_or_hook_host_writes() {
    let dir = directory();
    let mut db = SqliteFactory.open(&dir.path().join("db")).unwrap();
    let job = outbox_job();
    db.begin().unwrap();
    db.enqueue_job(&job).unwrap();
    db.commit().unwrap();
    for sql in [
        "UPDATE _statex_outbox SET record='{}'",
        "DROP TABLE _statex_outbox",
        "CREATE TABLE guest_forged(record TEXT); ALTER TABLE guest_forged RENAME TO _statex_outbox",
        "CREATE TRIGGER forge_after_migration AFTER INSERT ON _statex_migrations
         BEGIN UPDATE _statex_outbox SET record='{}'; END",
    ] {
        db.begin().unwrap();
        assert!(db.apply_migrations(&[Migration { name: "forge".into(), sql: sql.into() }]).is_err());
        db.rollback().unwrap();
        assert_eq!(db.job(&job.id).unwrap(), Some(job.clone()));
    }
    db.begin().unwrap();
    db.apply_migrations(&[Migration {
        name: "legitimate".into(), sql: "CREATE TABLE legitimate(value TEXT); ALTER TABLE legitimate ADD COLUMN extra TEXT".into(),
    }]).unwrap();
    db.commit().unwrap();
    assert_eq!(db.job(&job.id).unwrap(), Some(job));
}

#[test]
fn sqlite_guest_table_and_column_renames_preserve_migration_compatibility() {
    let dir = directory();
    let mut db = SqliteFactory.open(&dir.path().join("db")).unwrap();
    assert!(db.capture(1, 1).unwrap().is_none(), "eager reservation is checkpointed at open");
    db.begin().unwrap();
    db.apply_migrations(&[
        Migration {
            name: "users".into(),
            sql: "CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT);
                  INSERT INTO users VALUES(1, 'alice')".into(),
        },
        Migration {
            name: "rename".into(),
            sql: "ALTER TABLE users RENAME TO members;
                  ALTER TABLE members RENAME COLUMN name TO display_name".into(),
        },
    ]).unwrap();
    assert_eq!(db.query("SELECT display_name FROM members", &[]).unwrap().rows,
        vec![vec![SqlValue::Text("alice".into())]]);
    db.execute("ALTER TABLE members RENAME TO people", &[]).unwrap();
    db.execute("ALTER TABLE people RENAME COLUMN display_name TO label", &[]).unwrap();
    db.commit().unwrap();
    assert!(db.capture(1, 1).unwrap().is_some());
    db.begin().unwrap();
    assert_eq!(db.query("SELECT label FROM people", &[]).unwrap().rows,
        vec![vec![SqlValue::Text("alice".into())]]);
    assert!(db.job("missing").unwrap().is_none());
    assert!(!db.has_pending_jobs().unwrap());
    db.commit().unwrap();
    assert!(db.capture(1, 2).unwrap().is_none(), "initialized read-only transactions stay read-only");
}

#[test]
fn sqlite_temp_rename_shadow_cannot_forge_host_jobs() {
    let dir = directory();
    let mut db = SqliteFactory.open(&dir.path().join("db")).unwrap();
    let mut forged = outbox_job();
    forged.context.principal = Some(crate::invocation::Principal {
        subject: "forged-admin".into(), claims: Default::default(),
    });
    db.begin().unwrap();
    db.execute(
        "CREATE TEMP TABLE forged_jobs(id TEXT PRIMARY KEY, status TEXT, next_attempt_ms INTEGER, record TEXT)", &[],
    ).unwrap();
    db.execute("INSERT INTO forged_jobs VALUES(?1, 'pending', 0, ?2)", &[
        SqlValue::Text(forged.id.clone()), SqlValue::Text(serde_json::to_string(&forged).unwrap()),
    ]).unwrap();
    db.execute("ALTER TABLE temp.forged_jobs RENAME TO _statex_outbox", &[]).unwrap();
    assert!(db.job(&forged.id).unwrap().is_none(), "host reads only main");
    assert!(!db.has_pending_jobs().unwrap(), "discovery ignores temp shadows");
    assert!(db.query("SELECT record FROM temp._statex_outbox", &[]).is_err());
    assert!(db.query("SELECT record FROM main._statex_outbox", &[]).is_err());
    let trusted = outbox_job();
    db.enqueue_job(&trusted).unwrap();
    assert_eq!(db.job(&trusted.id).unwrap(), Some(trusted.clone()));
    let mut claimed = db.claim_job(100, 200).unwrap().unwrap();
    assert_eq!(claimed.id, trusted.id);
    assert!(claimed.context.principal.is_none());
    claimed.status = JobStatus::Succeeded;
    claimed.result = Some(serde_json::json!("trusted"));
    db.update_job(&claimed).unwrap();
    assert!(!db.has_pending_jobs().unwrap());
    db.commit().unwrap();
    assert_eq!(db.job(&trusted.id).unwrap(), Some(claimed));
    assert!(db.job(&forged.id).unwrap().is_none());
}

#[test]
fn sqlite_adapter_reserves_main_before_renames_and_reports_initialization_errors() {
    let conn = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
    let handle = sqlite_handle(conn.clone());
    let mut db = handle.lock().unwrap();
    assert!(!outbox_exists(&conn.lock().unwrap()).unwrap(), "adapter constructor remains infallible");
    db.begin().unwrap();
    db.execute("CREATE TABLE users(name TEXT)", &[]).unwrap();
    assert!(outbox_exists(&conn.lock().unwrap()).unwrap());
    db.execute("ALTER TABLE users RENAME TO members", &[]).unwrap();
    db.execute("ALTER TABLE members RENAME COLUMN name TO label", &[]).unwrap();
    assert!(db.execute("ALTER TABLE members RENAME TO _statex_outbox", &[]).is_err());
    db.rollback().unwrap();
    assert!(!outbox_exists(&conn.lock().unwrap()).unwrap(), "reservation follows an adapter's active transaction");
    db.query("SELECT 1", &[]).unwrap();
    assert!(outbox_exists(&conn.lock().unwrap()).unwrap(), "next SQL access recreates reservation");
    let malformed = Arc::new(Mutex::new(Connection::open_in_memory().unwrap()));
    malformed.lock().unwrap().execute("CREATE TABLE _statex_outbox(id TEXT)", []).unwrap();
    let handle = sqlite_handle(malformed);
    let error = handle.lock().unwrap().query("SELECT 1", &[]).unwrap_err();
    assert!(error.to_string().contains("status"), "{error}");
}

#[test]
fn sqlite_renamed_metadata_triggers_cannot_acquire_trusted_outbox_access() {
    let dir = directory();
    let mut db = SqliteFactory.open(&dir.path().join("db")).unwrap();
    let job = outbox_job();
    db.begin().unwrap();
    db.enqueue_job(&job).unwrap();
    db.execute(
        "CREATE TABLE forged_alarm(id INTEGER PRIMARY KEY, at_ms INTEGER,
         retry INTEGER, epoch INTEGER, seq INTEGER)", &[],
    ).unwrap();
    db.execute(
        "CREATE TRIGGER forge_alarm AFTER INSERT ON forged_alarm
         BEGIN UPDATE _statex_outbox SET record='{}'; END", &[],
    ).unwrap();
    db.execute("ALTER TABLE forged_alarm RENAME TO _statex_alarm", &[]).unwrap();
    assert!(db.set_alarm(100, 0, 1).is_err(), "host alarm writes cannot fire privileged queue edits");
    db.execute("CREATE TABLE forged_migrations(name TEXT PRIMARY KEY, applied_at INTEGER)", &[]).unwrap();
    db.execute(
        "CREATE TRIGGER forge_migration AFTER INSERT ON forged_migrations
         BEGIN UPDATE _statex_outbox SET record='{}'; END", &[],
    ).unwrap();
    db.execute("ALTER TABLE forged_migrations RENAME TO _statex_migrations", &[]).unwrap();
    assert!(db.apply_migrations(&[Migration { name: "noop".into(), sql: "SELECT 1".into() }]).is_err(),
        "host migration bookkeeping cannot fire privileged queue edits");
    assert_eq!(db.job(&job.id).unwrap(), Some(job));
    db.rollback().unwrap();
}
