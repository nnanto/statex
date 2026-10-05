//! The alarm row in an actor's database: the authoritative record of its
//! scheduled wake-up.
//!
//! Every installation of an alarm gets a new `(epoch, seq)` identity, so a
//! wake hint written for one installation can be recognised as stale once the
//! alarm fired, was cleared or was replaced.

use rusqlite::{Connection, OptionalExtension};

pub const TABLE: &str = "_statex_alarm";

/// Failed handler attempts after which an alarm is dropped.
pub const MAX_RETRIES: u32 = 6;

/// Backoff before retry `n` (1-based) of a failed alarm handler.
pub fn backoff_ms(n: u32) -> u64 {
    2000u64 << n.saturating_sub(1).min(MAX_RETRIES)
}

/// A scheduled alarm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Alarm {
    pub at_ms: u64,
    /// Failed handler attempts so far.
    pub retry: u32,
    /// Epoch of the activation that installed it.
    pub epoch: u64,
    /// Installation counter, increasing with every `set`.
    pub seq: u64,
}

fn exists(conn: &Connection) -> rusqlite::Result<bool> {
    conn.query_row("SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1", [TABLE], |r| {
        r.get::<_, i64>(0)
    })
    .map(|n| n > 0)
}

fn ensure(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _statex_alarm(
            id INTEGER PRIMARY KEY CHECK (id = 0),
            at_ms INTEGER,
            retry INTEGER NOT NULL DEFAULT 0,
            epoch INTEGER NOT NULL DEFAULT 0,
            seq INTEGER NOT NULL DEFAULT 0)",
    )
}

/// The scheduled alarm, if any. Never writes.
pub fn read(conn: &Connection) -> rusqlite::Result<Option<Alarm>> {
    if !exists(conn)? {
        return Ok(None);
    }
    let row = conn
        .query_row("SELECT at_ms, retry, epoch, seq FROM _statex_alarm WHERE id = 0", [], |r| {
            Ok((r.get::<_, Option<i64>>(0)?, r.get::<_, i64>(1)?, r.get::<_, i64>(2)?, r.get::<_, i64>(3)?))
        })
        .optional()?;
    Ok(row.and_then(|(at, retry, epoch, seq)| {
        at.map(|at| Alarm { at_ms: at as u64, retry: retry as u32, epoch: epoch as u64, seq: seq as u64 })
    }))
}

fn install(conn: &Connection, at_ms: u64, retry: u32, epoch: u64) -> rusqlite::Result<()> {
    ensure(conn)?;
    let at = i64::try_from(at_ms).unwrap_or(i64::MAX);
    conn.execute(
        "INSERT INTO _statex_alarm(id, at_ms, retry, epoch, seq) VALUES(0, ?1, ?2, ?3, 1)
         ON CONFLICT(id) DO UPDATE SET at_ms = ?1, retry = ?2, epoch = ?3, seq = seq + 1",
        rusqlite::params![at, retry, epoch as i64],
    )?;
    Ok(())
}

/// Schedules the alarm at `at_ms`, replacing any earlier one.
pub fn set(conn: &Connection, at_ms: u64, epoch: u64) -> rusqlite::Result<()> {
    install(conn, at_ms, 0, epoch)
}

/// Schedules retry `retry` of a failed handler.
pub fn set_retry(conn: &Connection, at_ms: u64, retry: u32, epoch: u64) -> rusqlite::Result<()> {
    install(conn, at_ms, retry, epoch)
}

/// Cancels the alarm. `seq` is kept so identities never repeat.
pub fn clear(conn: &Connection) -> rusqlite::Result<()> {
    if exists(conn)? {
        conn.execute("UPDATE _statex_alarm SET at_ms = NULL, retry = 0 WHERE id = 0", [])?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_clear_and_identity() {
        let c = Connection::open_in_memory().unwrap();
        assert_eq!(read(&c).unwrap(), None);
        clear(&c).unwrap();
        set(&c, 100, 3).unwrap();
        assert_eq!(read(&c).unwrap(), Some(Alarm { at_ms: 100, retry: 0, epoch: 3, seq: 1 }));
        set_retry(&c, 200, 2, 4).unwrap();
        assert_eq!(read(&c).unwrap(), Some(Alarm { at_ms: 200, retry: 2, epoch: 4, seq: 2 }));
        clear(&c).unwrap();
        assert_eq!(read(&c).unwrap(), None);
        set(&c, 50, 4).unwrap();
        assert_eq!(read(&c).unwrap().unwrap().seq, 3);
        assert_eq!(backoff_ms(1), 2000);
        assert_eq!(backoff_ms(3), 8000);
    }
}
