//! Connection settings and migrations for the concrete default SQLite backend.
//! Generic runtime and node code use `database::Database` instead.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::Connection;

use crate::Migration;

fn outbox_name(name: &str) -> bool {
    name.to_ascii_lowercase().starts_with("_statex_outbox")
}

fn guest_authorizer(context: AuthContext<'_>) -> Authorization {
    use AuthAction::*;
    let denied = match context.action {
        CreateIndex { index_name, table_name }
        | CreateTempIndex { index_name, table_name }
        | DropIndex { index_name, table_name }
        | DropTempIndex { index_name, table_name } => outbox_name(index_name) || outbox_name(table_name),
        CreateTrigger { trigger_name, table_name }
        | CreateTempTrigger { trigger_name, table_name }
        | DropTrigger { trigger_name, table_name }
        | DropTempTrigger { trigger_name, table_name } => {
            outbox_name(trigger_name) || table_name.to_ascii_lowercase().starts_with("_statex_")
        }
        CreateTable { table_name }
        | CreateTempTable { table_name }
        | DropTable { table_name }
        | DropTempTable { table_name }
        | Delete { table_name }
        | Insert { table_name }
        | Read { table_name, .. }
        | Update { table_name, .. }
        | AlterTable { table_name, .. }
        | Analyze { table_name }
        | CreateVtable { table_name, .. }
        | DropVtable { table_name, .. } => outbox_name(table_name),
        CreateView { view_name }
        | CreateTempView { view_name }
        | DropView { view_name }
        | DropTempView { view_name } => outbox_name(view_name),
        Reindex { index_name } => outbox_name(index_name),
        Function { .. } => false,
        Pragma { .. } | Attach { .. } | Detach { .. } | Transaction { .. }
        | Savepoint { .. } | Unknown { .. } => true,
        Select | Recursive => false,
        _ => true,
    };
    if denied { Authorization::Deny } else { Authorization::Allow }
}

/// Applies the SQLite parser's access checks only to guest SQL. Host queue
/// operations run outside this guard, under the same exclusive connection lock.
pub(crate) struct GuestSqlGuard<'a>(&'a Connection);

impl<'a> GuestSqlGuard<'a> {
    pub(crate) fn new(connection: &'a Connection) -> Result<Self> {
        // Reserve the main name before allowing ordinary renames. TEMP tables
        // can still be renamed into shadows, so host queue SQL qualifies main.
        ensure_outbox_schema(connection)?;
        connection.authorizer(Some(guest_authorizer));
        Ok(Self(connection))
    }
}

pub(crate) fn ensure_outbox_schema(connection: &Connection) -> Result<()> {
    // SQLite resolves the ON table in the index's explicitly selected schema.
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS main._statex_outbox (
            id TEXT PRIMARY KEY NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('pending', 'running', 'succeeded', 'failed')),
            next_attempt_ms INTEGER NOT NULL,
            record TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS main._statex_outbox_due
            ON _statex_outbox(status, next_attempt_ms);",
    )?;
    Ok(())
}

impl Drop for GuestSqlGuard<'_> {
    fn drop(&mut self) {
        self.0.authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
    }
}

/// Opens SQLite with the settings required by its WAL replicator.
pub fn open_db(path: &Path) -> Result<Connection> {
    let connection = Connection::open(path)?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "wal_autocheckpoint", 0)?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.busy_timeout(Duration::from_secs(5))?;
    ensure_outbox_schema(&connection)?;
    Ok(connection)
}

/// Applies pending SQL migrations inside the caller's transaction.
pub fn apply_migrations(connection: &Connection, migrations: &[Migration]) -> Result<Vec<String>> {
    // Bookkeeping writes share the guard: a guest-created metadata trigger
    // must not acquire host queue access when a migration is recorded.
    let _authorization = GuestSqlGuard::new(connection)?;
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS main._statex_migrations(name TEXT PRIMARY KEY, applied_at INTEGER)",
    )?;
    let mut applied = Vec::new();
    for migration in migrations {
        let done: i64 = connection.query_row(
            "SELECT count(*) FROM main._statex_migrations WHERE name = ?1",
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
            "INSERT INTO main._statex_migrations(name, applied_at) VALUES(?1, ?2)",
            rusqlite::params![migration.name, now],
        )?;
        applied.push(migration.name.clone());
    }
    Ok(applied)
}
