//! LTX-style page log for SQLite.
//!
//! An actor's database runs in WAL mode with automatic checkpoints disabled.
//! After every committed transaction [`WalTail::capture`] reads the WAL frames
//! appended by that transaction and returns the final image of each changed
//! page. That page set is encoded as a [`Segment`] and uploaded to the object
//! store. Restoring an actor = take a full snapshot of the database file and
//! [`apply_segment`] every later segment in transaction order.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};

const WAL_HEADER: u64 = 32;
const FRAME_HEADER: u64 = 24;
const MAGIC: &[u8; 8] = b"STXLTX01";

/// Changed pages of one (or more) committed transactions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageSet {
    pub page_size: u32,
    /// Database size in pages after the last captured commit.
    pub db_pages: u32,
    pub pages: BTreeMap<u32, Vec<u8>>,
}

/// Incrementally follows a SQLite WAL file.
pub struct WalTail {
    wal_path: PathBuf,
    offset: u64,
    salt: Option<(u32, u32)>,
}

fn be32(b: &[u8]) -> u32 {
    u32::from_be_bytes(b[..4].try_into().unwrap())
}

impl WalTail {
    pub fn new(db_path: &Path) -> Self {
        let mut p = db_path.as_os_str().to_owned();
        p.push("-wal");
        Self { wal_path: PathBuf::from(p), offset: 0, salt: None }
    }

    /// Must be called after `PRAGMA wal_checkpoint(TRUNCATE)`.
    pub fn reset(&mut self) {
        self.offset = 0;
        self.salt = None;
    }

    /// Returns the pages written by transactions committed since the previous
    /// call, or `None` if nothing was committed.
    pub fn capture(&mut self) -> Result<Option<PageSet>> {
        let mut f = match File::open(&self.wal_path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        let len = f.metadata()?.len();
        if len < WAL_HEADER {
            return Ok(None);
        }
        let mut hdr = [0u8; 32];
        f.read_exact(&mut hdr)?;
        let magic = be32(&hdr[0..]);
        ensure!(magic == 0x377f0682 || magic == 0x377f0683, "bad WAL magic {magic:#x}");
        let page_size = be32(&hdr[8..]);
        let salt = (be32(&hdr[16..]), be32(&hdr[20..]));
        if self.salt != Some(salt) || self.offset < WAL_HEADER || self.offset > len {
            // New or restarted WAL: frames start right after the header.
            self.salt = Some(salt);
            self.offset = WAL_HEADER;
        }
        let frame_len = FRAME_HEADER + page_size as u64;
        let mut pos = self.offset;
        let mut pending: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
        let mut committed: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
        let mut db_pages = 0u32;
        let mut committed_end = self.offset;
        f.seek(SeekFrom::Start(pos))?;
        let mut fh = [0u8; 24];
        while pos + frame_len <= len {
            f.read_exact(&mut fh)?;
            if (be32(&fh[8..]), be32(&fh[12..])) != salt {
                break;
            }
            let mut page = vec![0u8; page_size as usize];
            f.read_exact(&mut page)?;
            let pgno = be32(&fh[0..]);
            let commit_size = be32(&fh[4..]);
            pending.insert(pgno, page);
            pos += frame_len;
            if commit_size != 0 {
                committed.append(&mut pending);
                db_pages = commit_size;
                committed_end = pos;
            }
        }
        self.offset = committed_end;
        if committed.is_empty() {
            return Ok(None);
        }
        // Pages beyond the new end of the database were truncated away.
        committed.retain(|p, _| *p <= db_pages);
        Ok(Some(PageSet { page_size, db_pages, pages: committed }))
    }
}

/// One replicated transaction (txid) of an actor under a given epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    pub epoch: u64,
    pub txid: u64,
    pub pages: PageSet,
}

impl Segment {
    pub fn encode(&self) -> Vec<u8> {
        let ps = &self.pages;
        let mut out = Vec::with_capacity(40 + ps.pages.len() * (4 + ps.page_size as usize));
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.extend_from_slice(&self.txid.to_le_bytes());
        out.extend_from_slice(&ps.page_size.to_le_bytes());
        out.extend_from_slice(&ps.db_pages.to_le_bytes());
        out.extend_from_slice(&(ps.pages.len() as u32).to_le_bytes());
        for (pgno, data) in &ps.pages {
            out.extend_from_slice(&pgno.to_le_bytes());
            out.extend_from_slice(data);
        }
        let crc = crc32fast::hash(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        ensure!(buf.len() >= 40 && &buf[..8] == MAGIC, "not an LTX segment");
        let (body, crc) = buf.split_at(buf.len() - 4);
        ensure!(
            crc32fast::hash(body) == u32::from_le_bytes(crc.try_into().unwrap()),
            "LTX segment checksum mismatch"
        );
        let u64_at = |o: usize| u64::from_le_bytes(body[o..o + 8].try_into().unwrap());
        let u32_at = |o: usize| u32::from_le_bytes(body[o..o + 4].try_into().unwrap());
        let epoch = u64_at(8);
        let txid = u64_at(16);
        let page_size = u32_at(24);
        let db_pages = u32_at(28);
        let n = u32_at(32) as usize;
        let mut off = 36;
        let mut pages = BTreeMap::new();
        for _ in 0..n {
            ensure!(off + 4 + page_size as usize <= body.len(), "truncated LTX segment");
            let pgno = u32_at(off);
            pages.insert(pgno, body[off + 4..off + 4 + page_size as usize].to_vec());
            off += 4 + page_size as usize;
        }
        ensure!(off == body.len(), "trailing bytes in LTX segment");
        Ok(Self { epoch, txid, pages: PageSet { page_size, db_pages, pages } })
    }
}

/// Applies a segment to a closed database file (no open connections, no WAL).
pub fn apply_segment(db_file: &Path, seg: &Segment) -> Result<()> {
    let ps = seg.pages.page_size as u64;
    if ps == 0 {
        bail!("invalid page size");
    }
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(db_file)
        .with_context(|| format!("open {}", db_file.display()))?;
    for (pgno, data) in &seg.pages.pages {
        f.seek(SeekFrom::Start((*pgno as u64 - 1) * ps))?;
        f.write_all(data)?;
    }
    f.set_len(seg.pages.db_pages as u64 * ps)?;
    f.sync_all()?;
    Ok(())
}

/// Object name of a segment inside an epoch prefix.
pub fn segment_name(txid: u64) -> String {
    format!("{txid:016}.ltx")
}

/// Object name of a snapshot inside an epoch prefix.
pub fn snapshot_name(txid: u64) -> String {
    format!("snapshot-{txid:016}.db")
}

/// What an object inside `ltx/e<epoch>/` is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogEntry {
    Snapshot(u64),
    Segment(u64),
}

pub fn parse_entry(name: &str) -> Option<LogEntry> {
    if let Some(t) = name.strip_prefix("snapshot-").and_then(|r| r.strip_suffix(".db")) {
        return t.parse().ok().map(LogEntry::Snapshot);
    }
    name.strip_suffix(".ltx")?.parse().ok().map(LogEntry::Segment)
}

/// Restore plan: the snapshot to start from and the segments to replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestorePlan {
    pub epoch: u64,
    pub snapshot_txid: u64,
    pub segments: Vec<u64>,
}

/// Chooses the restore chain from a listing of `(epoch, entry)`.
///
/// The newest epoch that contains a snapshot wins: every activation writes a
/// snapshot of what it restored before serving, so a newer epoch with a
/// snapshot supersedes everything older (fencing guarantees no acknowledged
/// write of an older epoch is missing from it). Inside that epoch, replay the
/// contiguous segments after the newest snapshot.
pub fn plan_restore(entries: &[(u64, LogEntry)]) -> Option<RestorePlan> {
    let mut epochs: Vec<u64> = entries.iter().map(|(e, _)| *e).collect();
    epochs.sort_unstable();
    epochs.dedup();
    for epoch in epochs.into_iter().rev() {
        let snap = entries
            .iter()
            .filter_map(|(e, x)| match x {
                LogEntry::Snapshot(t) if *e == epoch => Some(*t),
                _ => None,
            })
            .max();
        let Some(snap) = snap else { continue };
        let mut segs: Vec<u64> = entries
            .iter()
            .filter_map(|(e, x)| match x {
                LogEntry::Segment(t) if *e == epoch && *t > snap => Some(*t),
                _ => None,
            })
            .collect();
        segs.sort_unstable();
        let mut chain = Vec::new();
        let mut next = snap + 1;
        for t in segs {
            if t != next {
                break;
            }
            chain.push(t);
            next += 1;
        }
        return Some(RestorePlan { epoch, snapshot_txid: snap, segments: chain });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn open(p: &Path) -> Connection {
        let c = Connection::open(p).unwrap();
        c.pragma_update(None, "journal_mode", "WAL").unwrap();
        c.pragma_update(None, "wal_autocheckpoint", 0).unwrap();
        c
    }

    fn dump(p: &Path) -> Vec<(i64, String)> {
        let c = Connection::open(p).unwrap();
        let mut s = c.prepare("SELECT id, v FROM t ORDER BY id").unwrap();
        s.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().map(|r| r.unwrap()).collect()
    }

    #[test]
    fn capture_and_replay_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src.db");
        let conn = open(&src);
        let mut tail = WalTail::new(&src);
        conn.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)").unwrap();
        let mut segs = vec![];
        let mut txid = 0;
        let mut push = |tail: &mut WalTail, segs: &mut Vec<Vec<u8>>| {
            if let Some(p) = tail.capture().unwrap() {
                txid += 1;
                segs.push(Segment { epoch: 1, txid, pages: p }.encode());
            }
        };
        push(&mut tail, &mut segs);
        // A read-only transaction produces nothing.
        conn.query_row("SELECT count(*) FROM t", [], |r| r.get::<_, i64>(0)).unwrap();
        assert!(tail.capture().unwrap().is_none());
        for i in 0..200 {
            conn.execute("INSERT INTO t(id, v) VALUES(?1, ?2)", (i, "x".repeat(100 + i as usize)))
                .unwrap();
            if i % 50 == 0 {
                push(&mut tail, &mut segs);
            }
        }
        push(&mut tail, &mut segs);
        // Checkpoint + truncate, then keep writing: tail must follow the new WAL.
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
        tail.reset();
        conn.execute("DELETE FROM t WHERE id >= 100", []).unwrap();
        push(&mut tail, &mut segs);
        conn.execute_batch("VACUUM").unwrap();
        push(&mut tail, &mut segs);

        let dst = dir.path().join("dst.db");
        for s in &segs {
            apply_segment(&dst, &Segment::decode(s).unwrap()).unwrap();
        }
        drop(conn);
        assert_eq!(dump(&src), dump(&dst));
        assert_eq!(dump(&dst).len(), 100);
    }

    #[test]
    fn restore_plan_picks_newest_snapshot_epoch() {
        use LogEntry::*;
        let entries = vec![
            (1, Snapshot(0)),
            (1, Segment(1)),
            (1, Segment(2)),
            (2, Snapshot(2)),
            (2, Segment(3)),
            (2, Segment(5)), // gap: ignored
            (3, Segment(4)), // epoch without snapshot (crashed during activation)
        ];
        let p = plan_restore(&entries).unwrap();
        assert_eq!(p, RestorePlan { epoch: 2, snapshot_txid: 2, segments: vec![3] });
        assert_eq!(parse_entry("snapshot-0000000000000007.db"), Some(Snapshot(7)));
        assert_eq!(parse_entry(&segment_name(9)), Some(Segment(9)));
    }
}
