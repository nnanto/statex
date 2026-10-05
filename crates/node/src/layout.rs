//! Object-store key layout.
//!
//! ```text
//! nodes/<node>.json                         node lease
//! fleet/peer-auth.json                      HMAC secret for node-to-node calls
//! deploy/<app>/current.json                 active version (CAS)
//! deploy/<app>/<sha>/component.wasm
//! deploy/<app>/<sha>/manifest.json
//! actors/<app>/<type>/<key>/owner.json       ownership record + epoch (CAS)
//! actors/<app>/<type>/<key>/ltx/e<epoch>/snapshot-<txid>.db
//! actors/<app>/<type>/<key>/ltx/e<epoch>/<txid>.ltx
//! fleet/waker.json                          lease of the node scanning wake/
//! wake/<minute>/<app>/<type>/<key>/<at_ms>-<epoch>-<seq>   alarm wake hint
//! ```
//!
//! Wake hints index scheduled alarms by Unix minute (10 digits, so keys sort
//! by time). They are only hints: the alarm row in the actor's database is
//! authoritative, and a hint naming an alarm that is no longer scheduled is
//! deleted when it is found.
//!
//! `<app>` is the app's storage name (see [`app_dir`]): a namespaced app
//! `payments/shop` is stored as `payments.shop`, so every app is exactly one
//! path segment.

use std::fmt;

pub const PEER_AUTH: &str = "fleet/peer-auth.json";
pub const WAKER: &str = "fleet/waker.json";
pub const WAKE_PREFIX: &str = "wake/";
pub const MAX_KEY_LEN: usize = 512;

/// Identity of an actor.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ActorId {
    pub app: String,
    pub ty: String,
    pub key: String,
}

impl fmt::Display for ActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", self.app, self.ty, self.key)
    }
}

/// Encodes an arbitrary key as a single safe path segment. Only ASCII
/// alphanumerics, `-` and `_` pass through; everything else (including `.`
/// and `/`) is percent-encoded.
pub fn enc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

pub fn dec(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let h = std::str::from_utf8(b.get(i + 1..i + 3)?).ok()?;
            out.push(u8::from_str_radix(h, 16).ok()?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// One-segment storage name for an app. App names are `[a-z0-9-]` segments
/// joined by `/`, so mapping `/` to `.` is unambiguous.
pub fn app_dir(app: &str) -> String {
    app.replace('/', ".")
}

impl ActorId {
    pub fn prefix(&self) -> String {
        format!("actors/{}/{}/{}/", app_dir(&self.app), self.ty, enc(&self.key))
    }
    pub fn owner_key(&self) -> String {
        format!("{}owner.json", self.prefix())
    }
    pub fn ltx_prefix(&self) -> String {
        format!("{}ltx/", self.prefix())
    }
    pub fn epoch_prefix(&self, epoch: u64) -> String {
        format!("{}e{epoch:010}/", self.ltx_prefix())
    }
    /// Local directory, relative to the node data dir.
    pub fn local_dir(&self) -> std::path::PathBuf {
        ["actors", &app_dir(&self.app), &self.ty, &enc(&self.key)].iter().collect()
    }
}

/// Parses `e<epoch>/<name>` relative to an ltx prefix.
pub fn parse_ltx(rel: &str) -> Option<(u64, statex_ltx::LogEntry)> {
    let (e, name) = rel.split_once('/')?;
    let epoch = e.strip_prefix('e')?.parse().ok()?;
    Some((epoch, statex_ltx::parse_entry(name)?))
}

pub fn node_key(node: &str) -> String {
    format!("nodes/{node}.json")
}

pub fn deploy_current(app: &str) -> String {
    format!("deploy/{}/current.json", app_dir(app))
}

pub fn deploy_object(app: &str, sha: &str, name: &str) -> String {
    format!("deploy/{}/{sha}/{name}", app_dir(app))
}

/// Identity of one alarm installation, as named in its wake hint.
pub fn wake_name(a: &statex_runtime::alarm::Alarm) -> String {
    format!("{:015}-{:016x}-{:016x}", a.at_ms, a.epoch, a.seq)
}

/// Prefix of the wake hints due in Unix minute `minute`.
pub fn wake_minute_prefix(minute: u64) -> String {
    format!("{WAKE_PREFIX}{minute:010}/")
}

/// Key of the wake hint for alarm `a` of actor `id`.
pub fn wake_key(id: &ActorId, a: &statex_runtime::alarm::Alarm) -> String {
    format!(
        "{}{}/{}/{}/{}",
        wake_minute_prefix(a.at_ms / 60_000),
        app_dir(&id.app),
        id.ty,
        enc(&id.key),
        wake_name(a)
    )
}

/// A parsed wake hint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WakeEntry {
    pub id: ActorId,
    pub at_ms: u64,
    pub epoch: u64,
    /// The alarm installation it names (see [`wake_name`]).
    pub name: String,
}

pub fn parse_wake(key: &str) -> Option<WakeEntry> {
    let rest = key.strip_prefix(WAKE_PREFIX)?;
    let mut parts = rest.split('/');
    let (_minute, app, ty, k, name) = (parts.next()?, parts.next()?, parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() {
        return None;
    }
    let mut f = name.split('-');
    let at_ms = f.next()?.parse().ok()?;
    let epoch = u64::from_str_radix(f.next()?, 16).ok()?;
    u64::from_str_radix(f.next()?, 16).ok()?;
    Some(WakeEntry {
        id: ActorId { app: app.replace('.', "/"), ty: ty.to_string(), key: dec(k)? },
        at_ms,
        epoch,
        name: name.to_string(),
    })
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_encoding() {
        for k in ["alice", "a/b", "..", "ünï code", "x%y", ""] {
            assert_eq!(dec(&enc(k)).as_deref(), Some(k));
            assert!(!enc(k).contains('/') && !enc(k).contains('.'));
        }
        let id = ActorId { app: "payments/shop".into(), ty: "cart".into(), key: "a/b".into() };
        assert_eq!(id.owner_key(), "actors/payments.shop/cart/a%2Fb/owner.json");
        assert_eq!(deploy_current("payments/shop"), "deploy/payments.shop/current.json");
        assert_eq!(parse_ltx("e0000000003/snapshot-0000000000000007.db"), Some((3, statex_ltx::LogEntry::Snapshot(7))));
    }

    #[test]
    fn wake_keys() {
        let id = ActorId { app: "payments/shop".into(), ty: "cart".into(), key: "a/b".into() };
        let a = statex_runtime::alarm::Alarm { at_ms: 120_500, retry: 0, epoch: 3, seq: 10 };
        let k = wake_key(&id, &a);
        assert_eq!(k, "wake/0000000002/payments.shop/cart/a%2Fb/000000000120500-0000000000000003-000000000000000a");
        let e = parse_wake(&k).unwrap();
        assert_eq!((e.id, e.at_ms, e.epoch, e.name), (id, 120_500, 3, wake_name(&a)));
        assert_eq!(parse_wake("wake/0000000002/x"), None);
    }
}
