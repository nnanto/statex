//! Connection settings and migrations for the concrete default SQLite backend.
//! Generic runtime and node code use `database::Database` instead.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::Connection;

use crate::Migration;

/// Opens SQLite with the settings required by its WAL replicator.
pub fn open_db(path: &Path) -> Result<Connection> {
    let connection = Connection::open(path)?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "wal_autocheckpoint", 0)?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.busy_timeout(Duration::from_secs(5))?;
    Ok(connection)
}

/// Applies pending SQL migrations inside the caller's transaction.
pub fn apply_migrations(connection: &Connection, migrations: &[Migration]) -> Result<Vec<String>> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS _statex_migrations(name TEXT PRIMARY KEY, applied_at INTEGER)",
    )?;
    let mut applied = Vec::new();
    for migration in migrations {
        let done: i64 = connection.query_row(
            "SELECT count(*) FROM _statex_migrations WHERE name = ?1",
            [&migration.name],
            |row| row.get(0),
        )?;
        if done > 0 {
            continue;
        }
        connection
            .execute_batch(&migration.sql)
            .with_context(|| format!("migration {}", migration.name))?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;
        connection.execute(
            "INSERT INTO _statex_migrations(name, applied_at) VALUES(?1, ?2)",
            rusqlite::params![migration.name, now],
        )?;
        applied.push(migration.name.clone());
    }
    Ok(applied)
}
