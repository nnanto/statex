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
