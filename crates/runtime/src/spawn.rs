//! Transactional outbox storage. Wake hints are installed by the node before
//! the transaction's segment is made durable.

use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

pub const MAX_PENDING: i64 = 256;
pub const MAX_ARGS_BYTES: usize = 1024 * 1024;
pub const DELIVERY_LEASE_MS: u64 = 120_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Pending {
    pub id: String,
    pub app: String,
    pub actor_type: String,
    pub key: String,
    pub method: String,
    pub args_json: String,
    pub attempts: u32,
    pub next_at_ms: u64,
}

fn exists(db: &Connection, table: &str) -> rusqlite::Result<bool> {
    db.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)", [table], |r| r.get(0))
}

fn ensure(db: &Connection) -> rusqlite::Result<()> {
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS _statex_outbox(
            id TEXT PRIMARY KEY, app TEXT NOT NULL, actor_type TEXT NOT NULL,
            actor_key TEXT NOT NULL, method TEXT NOT NULL, args_json TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0, next_at_ms INTEGER NOT NULL DEFAULT 0,
            last_error TEXT);",
    )
}

/// A fresh nonce prevents stale wake hints from rolled-back transactions
/// colliding with later tasks, even within the same ownership epoch.
pub fn enqueue(
    db: &Connection, epoch: u64, app: &str, actor_type: &str, key: &str, method: &str, args_json: &str,
) -> Result<String, String> {
    if args_json.len() > MAX_ARGS_BYTES {
        return Err(format!("spawn arguments exceed {MAX_ARGS_BYTES} bytes"));
    }
    ensure(db).map_err(|e| e.to_string())?;
    let count: i64 = db.query_row("SELECT count(*) FROM _statex_outbox", [], |r| r.get(0)).map_err(|e| e.to_string())?;
    if count >= MAX_PENDING {
        return Err(format!("spawn outbox is full ({MAX_PENDING} pending deliveries)"));
    }
    let id = format!("e{epoch}-{}", hex::encode(rand::random::<[u8; 16]>()));
    db.execute(
        "INSERT INTO _statex_outbox(id,app,actor_type,actor_key,method,args_json) VALUES(?1,?2,?3,?4,?5,?6)",
        rusqlite::params![id, app, actor_type, key, method, args_json],
    ).map_err(|e| e.to_string())?;
    Ok(id)
}

/// Read without creating tables or changing the WAL.
pub fn pending(db: &Connection) -> rusqlite::Result<Vec<Pending>> {
    if !exists(db, "_statex_outbox")? {
        return Ok(Vec::new());
    }
    let mut stmt = db.prepare("SELECT id,app,actor_type,actor_key,method,args_json,attempts,next_at_ms FROM _statex_outbox ORDER BY id")?;
    let rows = stmt.query_map([], |r| Ok(Pending {
        id: r.get(0)?, app: r.get(1)?, actor_type: r.get(2)?, key: r.get(3)?,
        method: r.get(4)?, args_json: r.get(5)?, attempts: r.get(6)?, next_at_ms: r.get(7)?,
    }))?;
    rows.collect()
}

/// Lease before dispatch. A crash leaves the durable task discoverable and
/// eligible again after expiry.
pub fn claim(db: &Connection, id: &str, now_ms: u64) -> rusqlite::Result<Option<Pending>> {
    let Some(mut task) = pending(db)?.into_iter().find(|p| p.id == id && p.next_at_ms <= now_ms) else {
        return Ok(None);
    };
    task.attempts = task.attempts.saturating_add(1);
    task.next_at_ms = now_ms.saturating_add(DELIVERY_LEASE_MS);
    db.execute("UPDATE _statex_outbox SET attempts=?2,next_at_ms=?3 WHERE id=?1",
        rusqlite::params![id, task.attempts, task.next_at_ms])?;
    Ok(Some(task))
}

/// An old attempt cannot settle a task leased by a newer attempt.
pub fn settle(db: &Connection, id: &str, attempt: u32, error: Option<&str>, now_ms: u64) -> rusqlite::Result<()> {
    if !exists(db, "_statex_outbox")? {
        return Ok(());
    }
    match error {
        None => { db.execute("DELETE FROM _statex_outbox WHERE id=?1 AND attempts=?2", rusqlite::params![id, attempt])?; }
        Some(error) => {
            let delay = (1000u64 << attempt.saturating_sub(1).min(6)).min(60_000);
            db.execute("UPDATE _statex_outbox SET next_at_ms=?3,last_error=?4 WHERE id=?1 AND attempts=?2",
                rusqlite::params![id, attempt, now_ms.saturating_add(delay), error])?;
        }
    }
    Ok(())
}

pub fn receipt(db: &Connection, id: &str) -> rusqlite::Result<Option<String>> {
    if !exists(db, "_statex_inbox")? {
        return Ok(None);
    }
    db.query_row("SELECT result_json FROM _statex_inbox WHERE id=?1", [id], |r| r.get(0)).optional()
}

pub fn acknowledge(db: &Connection, id: &str, result_json: &str) -> rusqlite::Result<()> {
    db.execute_batch("CREATE TABLE IF NOT EXISTS _statex_inbox(id TEXT PRIMARY KEY,result_json TEXT NOT NULL)")?;
    db.execute("INSERT INTO _statex_inbox(id,result_json) VALUES(?1,?2)", [id,result_json])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transactional_leasing_and_stale_settlement() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("BEGIN").unwrap();
        enqueue(&db, 1, "app", "worker", "k", "run", "[]").unwrap();
        db.execute_batch("ROLLBACK").unwrap();
        assert!(pending(&db).unwrap().is_empty());
        let id = enqueue(&db, 2, "app", "worker", "k", "run", "[]").unwrap();
        let a = claim(&db, &id, 10).unwrap().unwrap();
        assert!(claim(&db, &id, 11).unwrap().is_none());
        let b = claim(&db, &id, a.next_at_ms).unwrap().unwrap();
        settle(&db, &id, a.attempts, None, 20).unwrap();
        assert_eq!(pending(&db).unwrap().len(), 1);
        settle(&db, &id, b.attempts, Some("failed"), 20).unwrap();
        assert_eq!(pending(&db).unwrap()[0].next_at_ms, 2020);
        settle(&db, &id, b.attempts, None, 30).unwrap();
        assert!(pending(&db).unwrap().is_empty());
    }

    #[test]
    fn rollback_never_reuses_a_hint_id_and_limits_are_explicit() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("BEGIN").unwrap();
        let abandoned = enqueue(&db, 1, "app", "worker", "k", "run", "[]").unwrap();
        db.execute_batch("ROLLBACK").unwrap();
        let current = enqueue(&db, 1, "app", "worker", "k", "run", "[]").unwrap();
        assert_ne!(abandoned, current);
        for _ in 1..MAX_PENDING {
            enqueue(&db, 1, "app", "worker", "k", "run", "[]").unwrap();
        }
        assert!(enqueue(&db, 1, "app", "worker", "k", "run", "[]").unwrap_err().contains("full"));
        assert!(enqueue(&db, 1, "app", "worker", "k", "run", &"x".repeat(MAX_ARGS_BYTES + 1))
            .unwrap_err().contains("exceed"));
    }

    #[test]
    fn receipts_commit_with_effects() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch("BEGIN").unwrap();
        acknowledge(&db, "delivery", "123").unwrap();
        db.execute_batch("ROLLBACK").unwrap();
        assert_eq!(receipt(&db, "delivery").unwrap(), None);
        db.execute_batch("BEGIN").unwrap();
        acknowledge(&db, "delivery", "123").unwrap();
        db.execute_batch("COMMIT").unwrap();
        assert_eq!(receipt(&db, "delivery").unwrap().as_deref(), Some("123"));
    }
}
