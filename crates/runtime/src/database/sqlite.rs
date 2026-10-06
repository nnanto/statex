use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use anyhow::{bail, ensure, Result};
use rusqlite::{types::Value, Connection};
use statex_ltx::{apply_segment, Segment, WalTail};

use super::{BackendIdentity, Database, DatabaseFactory, DatabaseHandle, SqlRows, SqlValue};
use crate::{alarm, manifest::Migration};

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

impl Database for SqliteDatabase {
    fn execute(&mut self, statement: &str, params: &[SqlValue]) -> Result<u64> {
        let params: Vec<_> = params.iter().map(to_sqlite).collect();
        Ok(self
            .conn
            .lock()
            .unwrap()
            .execute(statement, rusqlite::params_from_iter(params.iter()))? as u64)
    }

    fn query(&mut self, statement: &str, params: &[SqlValue]) -> Result<SqlRows> {
        let params: Vec<_> = params.iter().map(to_sqlite).collect();
        let conn = self.conn.lock().unwrap();
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
        Ok(alarm::read(&self.conn.lock().unwrap())?)
    }

    fn set_alarm(&mut self, at_ms: u64, retry: u32, epoch: u64) -> Result<()> {
        Ok(alarm::set_retry(
            &self.conn.lock().unwrap(),
            at_ms,
            retry,
            epoch,
        )?)
    }

    fn clear_alarm(&mut self) -> Result<()> {
        Ok(alarm::clear(&self.conn.lock().unwrap())?)
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
