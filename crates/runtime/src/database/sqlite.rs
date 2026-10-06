use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use anyhow::{bail, ensure, Result};
use rusqlite::{types::Value, Connection, OptionalExtension};
use statex_ltx::{apply_segment, Segment, WalTail};

use super::{BackendIdentity, Database, DatabaseFactory, DatabaseHandle, SqlRows, SqlValue};
use crate::{alarm, manifest::Migration};
use crate::outbox::{Job, JobStatus};

/// Default SQLite/WAL backend with opaque LTX change payloads.
#[derive(Debug, Default)]
pub struct SqliteFactory;

impl DatabaseFactory for SqliteFactory {
    fn identity(&self) -> BackendIdentity {
        BackendIdentity {
            name: "statex.sqlite-ltx".into(),
            format_version: 1,
        }
    }

    fn open(&self, path: &Path) -> Result<Box<dyn Database>> {
        let conn = crate::sqlite::open_db(path)?;
        let mut db = SqliteDatabase {
            conn: Arc::new(Mutex::new(conn)),
            persistence: Some((path.to_owned(), WalTail::new(path))),
        };
        db.checkpoint()?;
        Ok(Box::new(db))
    }

    fn replay(&self, path: &Path, epoch: u64, txid: u64, change: &[u8]) -> Result<()> {
        let segment = Segment::decode(change)?;
        ensure!(
            segment.epoch == epoch,
            "segment {txid} has epoch {}, expected {epoch}",
            segment.epoch
        );
        ensure!(
            segment.txid == txid,
            "segment {txid} has txid {}",
            segment.txid
        );
        let pages = &segment.pages;
        ensure!(
            pages.page_size.is_power_of_two() && (512..=65536).contains(&pages.page_size),
            "invalid SQLite page size"
        );
        ensure!(
            pages.pages.iter().all(|(number, data)| {
                *number > 0 && *number <= pages.db_pages && data.len() == pages.page_size as usize
            }),
            "invalid SQLite page image"
        );
        apply_segment(path, &segment)
    }
}

/// SQLite implementation. The replaceable database interface exposes no
/// driver types; explicit SQLite embeddings can also share a raw connection.
pub struct SqliteDatabase {
    conn: Arc<Mutex<Connection>>,
    persistence: Option<(PathBuf, WalTail)>,
}

/// Adapts a shared SQLite connection for runtime embeddings that manage their
/// own persistence. Capture/checkpoint are unavailable on this connection
/// adapter; use SqliteFactory when the node should own durable state.
pub fn sqlite_handle(conn: Arc<Mutex<Connection>>) -> DatabaseHandle {
    Arc::new(Mutex::new(Box::new(SqliteDatabase {
        conn,
        persistence: None,
    })))
}

fn to_sqlite(value: &SqlValue) -> Value {
    match value {
        SqlValue::Null => Value::Null,
        SqlValue::Integer(v) => Value::Integer(*v),
        SqlValue::Real(v) => Value::Real(*v),
        SqlValue::Text(v) => Value::Text(v.clone()),
        SqlValue::Blob(v) => Value::Blob(v.clone()),
    }
}

fn from_sqlite(value: Value) -> SqlValue {
    match value {
        Value::Null => SqlValue::Null,
        Value::Integer(v) => SqlValue::Integer(v),
        Value::Real(v) => SqlValue::Real(v),
        Value::Text(v) => SqlValue::Text(v),
        Value::Blob(v) => SqlValue::Blob(v),
    }
}

fn outbox_exists(conn: &Connection) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM main.sqlite_schema WHERE type = 'table' AND name = '_statex_outbox')",
        [], |row| row.get(0),
    )?)
}

fn require_outbox_transaction(conn: &Connection) -> Result<()> {
    ensure!(!conn.is_autocommit(), "outbox mutations require an active transaction");
    Ok(())
}

fn update_outbox_job(conn: &Connection, job: &Job) -> Result<()> {
    let changed = conn.execute(
        "UPDATE main._statex_outbox SET status = ?2, next_attempt_ms = ?3, record = ?4 WHERE id = ?1",
        rusqlite::params![job.id, job.status.as_str(), job.next_attempt_ms as i64, serde_json::to_string(job)?],
    )?;
    ensure!(changed == 1, "outbox job {} does not exist", job.id);
    Ok(())
}

impl Database for SqliteDatabase {
    fn execute(&mut self, statement: &str, params: &[SqlValue]) -> Result<u64> {
        let params: Vec<_> = params.iter().map(to_sqlite).collect();
        let conn = self.conn.lock().unwrap();
        let _authorization = crate::sqlite::GuestSqlGuard::new(&conn)?;
        Ok(conn.execute(statement, rusqlite::params_from_iter(params.iter()))? as u64)
    }

    fn query(&mut self, statement: &str, params: &[SqlValue]) -> Result<SqlRows> {
        let params: Vec<_> = params.iter().map(to_sqlite).collect();
        let conn = self.conn.lock().unwrap();
        let _authorization = crate::sqlite::GuestSqlGuard::new(&conn)?;
        let mut statement = conn.prepare(statement)?;
        let columns: Vec<String> = statement
            .column_names()
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        let rows = statement
            .query_map(rusqlite::params_from_iter(params.iter()), |r| {
                (0..columns.len())
                    .map(|i| r.get::<_, Value>(i).map(from_sqlite))
                    .collect::<rusqlite::Result<Vec<_>>>()
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(SqlRows { columns, rows })
    }

    fn begin(&mut self) -> Result<()> {
        self.conn.lock().unwrap().execute_batch("BEGIN IMMEDIATE")?;
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        self.conn.lock().unwrap().execute_batch("COMMIT")?;
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        self.conn.lock().unwrap().execute_batch("ROLLBACK")?;
        Ok(())
    }

    fn apply_migrations(&mut self, migrations: &[Migration]) -> Result<Vec<String>> {
        crate::sqlite::apply_migrations(&self.conn.lock().unwrap(), migrations)
    }

    fn alarm(&mut self) -> Result<Option<alarm::Alarm>> {
        let conn = self.conn.lock().unwrap();
        let _authorization = crate::sqlite::GuestSqlGuard::new(&conn)?;
        Ok(alarm::read(&conn)?)
    }

    fn set_alarm(&mut self, at_ms: u64, retry: u32, epoch: u64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let _authorization = crate::sqlite::GuestSqlGuard::new(&conn)?;
        Ok(alarm::set_retry(
            &conn,
            at_ms,
            retry,
            epoch,
        )?)
    }

    fn clear_alarm(&mut self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let _authorization = crate::sqlite::GuestSqlGuard::new(&conn)?;
        Ok(alarm::clear(&conn)?)
    }

    fn enqueue_job(&mut self, job: &Job) -> Result<()> {
        job.validate()?;
        ensure!(
            job.status == JobStatus::Pending && job.attempts == 0
                && job.result.is_none() && job.error.is_none()
                && job.next_attempt_ms == job.not_before_ms,
            "new outbox jobs must be pending and unattempted"
        );
        let conn = self.conn.lock().unwrap();
        require_outbox_transaction(&conn)?;
        crate::sqlite::ensure_outbox_schema(&conn)?;
        conn.execute(
            "INSERT INTO main._statex_outbox(id, status, next_attempt_ms, record) VALUES(?1, ?2, ?3, ?4)",
            rusqlite::params![job.id, job.status.as_str(), job.next_attempt_ms as i64, serde_json::to_string(job)?],
        )?;
        Ok(())
    }

    fn claim_job(&mut self, now_ms: u64, retry_at_ms: u64) -> Result<Option<Job>> {
        crate::outbox::validate_timestamp(now_ms)?;
        crate::outbox::validate_timestamp(retry_at_ms)?;
        ensure!(retry_at_ms > now_ms, "outbox recovery deadline must be after claim time");
        let conn = self.conn.lock().unwrap();
        require_outbox_transaction(&conn)?;
        if !outbox_exists(&conn)? { return Ok(None); }
        let record: Option<String> = conn.query_row(
            "SELECT record FROM main._statex_outbox
             WHERE status IN ('pending', 'running') AND next_attempt_ms <= ?1
             ORDER BY next_attempt_ms, id LIMIT 1",
            [now_ms as i64], |row| row.get(0),
        ).optional()?;
        let Some(record) = record else { return Ok(None); };
        let mut job: Job = serde_json::from_str(&record)?;
        job.attempts = job.attempts.checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("outbox attempt count overflow"))?;
        job.status = JobStatus::Running;
        job.next_attempt_ms = retry_at_ms;
        job.validate()?;
        update_outbox_job(&conn, &job)?;
        Ok(Some(job))
    }

    fn update_job(&mut self, job: &Job) -> Result<()> {
        job.validate()?;
        let conn = self.conn.lock().unwrap();
        require_outbox_transaction(&conn)?;
        ensure!(outbox_exists(&conn)?, "outbox job {} does not exist", job.id);
        let record: Option<String> = conn.query_row(
            "SELECT record FROM main._statex_outbox WHERE id = ?1", [&job.id], |row| row.get(0),
        ).optional()?;
        let stored: Job = serde_json::from_str(
            &record.ok_or_else(|| anyhow::anyhow!("outbox job {} does not exist", job.id))?,
        )?;
        ensure!(
            stored.status == JobStatus::Running && stored.attempts == job.attempts,
            "stale outbox completion for job {} at attempt {}", job.id, job.attempts
        );
        ensure!(job.status != JobStatus::Running, "outbox completion must retry or finish the job");
        ensure!(
            stored.target == job.target && stored.method == job.method && stored.args == job.args
                && stored.not_before_ms == job.not_before_ms && stored.context == job.context,
            "outbox completion cannot alter the enqueued call"
        );
        update_outbox_job(&conn, job)
    }

    fn job(&mut self, id: &str) -> Result<Option<Job>> {
        let conn = self.conn.lock().unwrap();
        if !outbox_exists(&conn)? { return Ok(None); }
        let record: Option<String> = conn.query_row(
            "SELECT record FROM main._statex_outbox WHERE id = ?1", [id], |row| row.get(0),
        ).optional()?;
        record.map(|record| serde_json::from_str(&record).map_err(Into::into)).transpose()
    }

    fn has_pending_jobs(&mut self) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        if !outbox_exists(&conn)? { return Ok(false); }
        Ok(conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM main._statex_outbox WHERE status IN ('pending', 'running'))",
            [], |row| row.get(0),
        )?)
    }

    fn capture(&mut self, epoch: u64, txid: u64) -> Result<Option<Vec<u8>>> {
        let Some((_, wal)) = &mut self.persistence else {
            bail!("SQLite connection adapter does not own persistence");
        };
        Ok(wal
            .capture()?
            .map(|pages| Segment { epoch, txid, pages }.encode()))
    }

    fn checkpoint(&mut self) -> Result<PathBuf> {
        let Some((path, wal)) = &mut self.persistence else {
            bail!("SQLite connection adapter does not own persistence");
        };
        let conn = self.conn.lock().unwrap();
        ensure!(
            conn.is_autocommit(),
            "cannot checkpoint an active transaction"
        );
        let busy: i64 = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0))?;
        ensure!(busy == 0, "checkpoint was blocked");
        wal.reset();
        Ok(path.clone())
    }
}

#[cfg(test)]
#[path = "sqlite_tests.rs"]
mod tests;
