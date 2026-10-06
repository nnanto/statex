//! Object-store abstraction used for leases, ownership records, deployments
//! and replicated actor data.
//!
//! statex needs four properties from the store:
//! conditional create, conditional overwrite, read-after-write consistency and
//! ranged reads. Listing must also be consistent after a write.

mod azure;
mod local;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

pub use azure::AzureBlobStore;
pub use local::LocalFsStore;

/// Opaque, nonempty version token (HTTP ETag or equivalent). Every successful
/// write must produce a fresh token, even when the bytes are unchanged.
pub type ETag = String;

#[derive(Debug, Clone)]
pub struct Object {
    pub data: Bytes,
    pub etag: ETag,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// A conditional write was cleanly rejected (object exists / etag mismatch).
    #[error("precondition failed")]
    Precondition,
    /// Any other failure. The write may or may not have been applied.
    #[error("store error: {0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T, E = StoreError> = std::result::Result<T, E>;

/// Strongly consistent object storage with atomic per-key conditional writes.
///
/// Successful writes are immediately visible to reads and listings.
/// `Precondition` guarantees that neither content nor version changed; other
/// errors are ambiguous and callers must reconcile by reading the object.
/// See `docs/extensions/object-stores.md` for the full provider contract.
#[async_trait]
pub trait ObjectStore: Send + Sync + 'static {
    async fn get(&self, key: &str) -> Result<Option<Object>>;
    /// Returns up to `len` bytes starting at `start`, clipped at EOF. An
    /// existing object returns empty bytes for an empty or past-EOF range;
    /// a missing object returns `None`, including for an empty range.
    async fn get_range(&self, key: &str, start: u64, len: u64) -> Result<Option<Bytes>>;
    /// Unconditional write.
    async fn put(&self, key: &str, data: Bytes) -> Result<ETag>;
    /// Create only if the object does not exist.
    async fn put_if_absent(&self, key: &str, data: Bytes) -> Result<ETag>;
    /// Overwrite only if the current version matches `etag`.
    async fn put_if_match(&self, key: &str, data: Bytes, etag: &str) -> Result<ETag>;
    /// Unconditional write of a local file's contents, streamed so memory use
    /// stays bounded regardless of file size. The file must not change while
    /// this runs.
    async fn put_file(&self, key: &str, path: &Path) -> Result<ETag>;
    /// Streams an object into a local file (created or truncated), without
    /// buffering it in memory. Returns `false` if the object does not exist,
    /// leaving the destination untouched. A successful read uses one object
    /// version; errors may leave a partially written destination.
    async fn get_to_file(&self, key: &str, path: &Path) -> Result<bool>;
    /// Lists keys with the given prefix, sorted lexicographically.
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    async fn delete(&self, key: &str) -> Result<()>;
    /// Human readable location, for logs.
    fn describe(&self) -> String;
}

pub type DynStore = Arc<dyn ObjectStore>;

type StoreFactory = Arc<dyn Fn(&str) -> anyhow::Result<DynStore> + Send + Sync>;

/// Scheme-to-factory registry. Factories receive the part after `://`, not the
/// scheme, and may capture provider-specific configuration. Plain paths use
/// the `file` factory. Stores themselves remain injectable via `DynStore`.
pub struct StoreRegistry {
    factories: HashMap<String, StoreFactory>,
}

impl Default for StoreRegistry {
    fn default() -> Self {
        let mut registry = Self::empty();
        registry.register("file", local_factory).expect("valid built-in scheme");
        registry.register("local", local_factory).expect("valid built-in scheme");
        registry
            .register("az", |location| {
                let container = location.trim_end_matches('/');
                anyhow::ensure!(
                    !container.is_empty() && !container.contains(['/', '?', '#']),
                    "az:// requires a container name, not an object path"
                );
                Ok(Arc::new(AzureBlobStore::from_env(container)?) as DynStore)
            })
            .expect("valid built-in scheme");
        registry
    }
}

fn local_factory(location: &str) -> anyhow::Result<DynStore> {
    anyhow::ensure!(!location.is_empty(), "local store path must not be empty");
    Ok(Arc::new(LocalFsStore::new(location)?))
}

impl StoreRegistry {
    /// An empty registry for applications that want to allow only explicitly
    /// registered providers. Usually prefer `StoreRegistry::default()`.
    pub fn empty() -> Self {
        Self { factories: HashMap::new() }
    }

    /// Adds a factory. Scheme names are case insensitive; duplicates are
    /// rejected rather than silently replacing an application's provider.
    pub fn register<F>(&mut self, scheme: &str, factory: F) -> anyhow::Result<()>
    where
        F: Fn(&str) -> anyhow::Result<DynStore> + Send + Sync + 'static,
    {
        anyhow::ensure!(valid_scheme(scheme), "invalid object-store scheme {scheme:?}");
        let scheme = scheme.to_ascii_lowercase();
        anyhow::ensure!(
            !self.factories.contains_key(&scheme),
            "object-store scheme {scheme:?} is already registered"
        );
        self.factories.insert(scheme, Arc::new(factory));
        Ok(())
    }

    /// Resolves an explicit `scheme://location` or a plain filesystem path.
    /// Unknown schemes never fall back to a local directory.
    pub fn open(&self, url: &str) -> anyhow::Result<DynStore> {
        let (scheme, location) = match url.split_once("://") {
            Some((scheme, location)) => {
                anyhow::ensure!(valid_scheme(scheme), "invalid object-store URL {url:?}");
                (scheme.to_ascii_lowercase(), location)
            }
            None => {
                if let Some((scheme, _)) =
                    url.split_once(':').filter(|_| !Path::new(url).is_absolute())
                {
                    anyhow::ensure!(
                        !valid_scheme(scheme),
                        "object-store URLs require scheme://location; use ./ for paths containing ':'"
                    );
                }
                ("file".to_string(), url)
            }
        };
        let factory = self
            .factories
            .get(&scheme)
            .ok_or_else(|| anyhow::anyhow!("unsupported object-store scheme {scheme:?}"))?;
        factory(location)
    }
}

fn valid_scheme(scheme: &str) -> bool {
    let mut bytes = scheme.bytes();
    bytes.next().is_some_and(|b| b.is_ascii_alphabetic())
        && bytes.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.'))
}

/// Opens a built-in store: plain paths, `file://` and `local://` select localfs;
/// `az://container` selects Azure explicitly. For extensions use a registry.
pub fn open(url: &str) -> anyhow::Result<DynStore> {
    StoreRegistry::default().open(url)
}

/// Helpers for JSON records.
pub async fn get_json<T: serde::de::DeserializeOwned>(
    store: &dyn ObjectStore,
    key: &str,
) -> Result<Option<(T, ETag)>> {
    match store.get(key).await? {
        None => Ok(None),
        Some(obj) => {
            let v = serde_json::from_slice(&obj.data)
                .map_err(|e| StoreError::Other(anyhow::anyhow!("decode {key}: {e}")))?;
            Ok(Some((v, obj.etag)))
        }
    }
}

pub fn to_json_bytes<T: serde::Serialize>(v: &T) -> Bytes {
    Bytes::from(serde_json::to_vec_pretty(v).expect("serializable"))
}

/// Probes content/version visibility, conditional writes, ranges, sorted prefix
/// listing, file streaming, and missing/delete semantics. Uses a unique object
/// prefix and a scratch directory in the current directory, cleaned up on
/// success or failure. Requires a writable current directory.
///
/// This sequential smoke test does not prove concurrency or crash safety.
pub async fn conformance_test(store: &dyn ObjectStore) -> anyhow::Result<()> {
    let id: u64 = rand::random();
    let prefix = format!("probe/{id:016x}/");
    let key = format!("{prefix}object");
    let a = format!("{prefix}a");
    let z = format!("{prefix}z");
    let outside = format!("probe/{id:016x}-outside");
    let missing = format!("{prefix}missing");
    let scratch = std::path::PathBuf::from(format!(".statex-store-probe-{id:016x}"));
    tokio::fs::create_dir(&scratch).await?;
    let result = async {
        let e1 = store.put_if_absent(&key, Bytes::from_static(b"one")).await?;
        check_object(store, &key, b"one", &e1).await?;
        expect_precondition(
            store.put_if_absent(&key, Bytes::from_static(b"two")).await,
            "conditional create of an existing object",
        )?;
        check_object(store, &key, b"one", &e1).await?;

        let e2 = store.put_if_match(&key, Bytes::from_static(b"0123456789"), &e1).await?;
        anyhow::ensure!(e2 != e1, "conditional update reused a version token");
        check_object(store, &key, b"0123456789", &e2).await?;
        expect_precondition(
            store.put_if_match(&key, Bytes::from_static(b"stale"), &e1).await,
            "stale conditional overwrite",
        )?;
        check_object(store, &key, b"0123456789", &e2).await?;

        let e3 = store.put_if_match(&key, Bytes::from_static(b"0123456789"), &e2).await?;
        anyhow::ensure!(e3 != e1 && e3 != e2, "same-content update reused a version token");
        check_object(store, &key, b"0123456789", &e3).await?;
        let e4 = store.put(&key, Bytes::from_static(b"0123456789")).await?;
        anyhow::ensure!(
            ![&e1, &e2, &e3].contains(&&e4),
            "unconditional write reused a version token"
        );
        check_object(store, &key, b"0123456789", &e4).await?;
        for (start, len, expected) in
            [(3, 4, &b"3456"[..]), (8, u64::MAX, &b"89"[..]), (10, 4, &b""[..]), (0, 0, &b""[..])]
        {
            let actual = store.get_range(&key, start, len).await?;
            anyhow::ensure!(
                actual.as_deref() == Some(expected),
                "ranged read returned wrong bytes at {start} for length {len}"
            );
        }

        store.put(&z, Bytes::new()).await?;
        store.put(&a, Bytes::new()).await?;
        store.put(&outside, Bytes::new()).await?;
        anyhow::ensure!(
            store.list(&prefix).await? == vec![a.clone(), key.clone(), z.clone()],
            "prefix listing is not complete, sorted, unique, or prefix-filtered"
        );

        let src = scratch.join("source");
        let dst = scratch.join("destination");
        let data: Vec<u8> = (0..131_071).map(|i| (i % 251) as u8).collect();
        tokio::fs::write(&src, &data).await?;
        let e5 = store.put_file(&key, &src).await?;
        anyhow::ensure!(![&e1, &e2, &e3, &e4].contains(&&e5), "file upload reused a version token");
        check_object(store, &key, &data, &e5).await?;
        tokio::fs::write(&dst, vec![0; data.len() + 10]).await?;
        anyhow::ensure!(store.get_to_file(&key, &dst).await?, "streamed object disappeared");
        anyhow::ensure!(tokio::fs::read(&dst).await? == data, "streaming roundtrip changed bytes");
        let e6 = store.put_file(&key, &src).await?;
        anyhow::ensure!(
            ![&e1, &e2, &e3, &e4, &e5].contains(&&e6),
            "same-content file upload reused a version token"
        );
        check_object(store, &key, &data, &e6).await?;
        tokio::fs::write(&src, b"").await?;
        let e7 = store.put_file(&key, &src).await?;
        anyhow::ensure!(
            ![&e1, &e2, &e3, &e4, &e5, &e6].contains(&&e7),
            "empty file upload reused a version token"
        );
        check_object(store, &key, b"", &e7).await?;
        anyhow::ensure!(store.get_to_file(&key, &dst).await?, "empty streamed object disappeared");
        anyhow::ensure!(tokio::fs::read(&dst).await?.is_empty(), "empty download did not truncate");

        anyhow::ensure!(store.get(&missing).await?.is_none(), "missing get returned an object");
        for len in [0, 1] {
            anyhow::ensure!(
                store.get_range(&missing, 0, len).await?.is_none(),
                "missing range returned bytes"
            );
        }
        expect_precondition(
            store.put_if_match(&missing, Bytes::new(), &e7).await,
            "conditional overwrite of a missing object",
        )?;
        tokio::fs::write(&dst, b"untouched").await?;
        anyhow::ensure!(!store.get_to_file(&missing, &dst).await?, "missing download succeeded");
        anyhow::ensure!(
            tokio::fs::read(&dst).await? == b"untouched",
            "missing download changed destination"
        );
        let new_dst = scratch.join("missing-destination");
        anyhow::ensure!(
            !store.get_to_file(&missing, &new_dst).await?,
            "missing download succeeded"
        );
        anyhow::ensure!(!new_dst.exists(), "missing download created destination");
        store.delete(&key).await?;
        store.delete(&key).await?;
        anyhow::ensure!(store.get(&key).await?.is_none(), "delete left an object visible");
        anyhow::ensure!(
            store.list(&prefix).await? == vec![a.clone(), z.clone()],
            "list-after-delete still includes the object"
        );
        Ok(())
    }
    .await;
    let mut cleanup = Ok(());
    for k in [&key, &a, &z, &outside, &missing] {
        if let Err(e) = store.delete(k).await {
            if cleanup.is_ok() {
                cleanup = Err(anyhow::anyhow!("probe cleanup failed: {e}"));
            }
        }
    }
    if let Err(e) = tokio::fs::remove_dir_all(&scratch).await {
        if cleanup.is_ok() {
            cleanup = Err(e.into());
        }
    }
    result.and(cleanup)
}

async fn check_object(
    store: &dyn ObjectStore,
    key: &str,
    data: &[u8],
    etag: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(!etag.is_empty(), "write returned an empty version token");
    let object =
        store.get(key).await?.ok_or_else(|| anyhow::anyhow!("read-after-write missed {key}"))?;
    anyhow::ensure!(object.data.as_ref() == data, "read-after-write returned incorrect content");
    anyhow::ensure!(object.etag == etag, "read-after-write returned incorrect version");
    Ok(())
}

fn expect_precondition(result: Result<ETag>, operation: &str) -> anyhow::Result<()> {
    match result {
        Err(StoreError::Precondition) => Ok(()),
        Ok(_) => anyhow::bail!("store accepted {operation}"),
        Err(e) => anyhow::bail!("rejecting {operation} returned ambiguous error: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_store_conformance() {
        let dir = tempfile::tempdir_in(".").unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();
        conformance_test(&store).await.unwrap();
    }

    #[tokio::test]
    async fn local_store_list_and_cas() {
        let dir = tempfile::tempdir_in(".").unwrap();
        let s = LocalFsStore::new(dir.path()).unwrap();
        s.put("a/b/1", Bytes::from_static(b"x")).await.unwrap();
        s.put("a/b/2", Bytes::from_static(b"y")).await.unwrap();
        s.put("a/c", Bytes::from_static(b"z")).await.unwrap();
        assert_eq!(s.list("a/b/").await.unwrap(), vec!["a/b/1", "a/b/2"]);
        assert_eq!(s.list("a/").await.unwrap().len(), 3);
        let o = s.get("a/c").await.unwrap().unwrap();
        // Same content written again still produces a fresh version.
        let e2 = s.put_if_match("a/c", Bytes::from_static(b"z"), &o.etag).await.unwrap();
        assert_ne!(e2, o.etag);
        assert!(matches!(
            s.put_if_match("a/c", Bytes::from_static(b"q"), &o.etag).await,
            Err(StoreError::Precondition)
        ));
        s.delete("a/c").await.unwrap();
        assert!(s.get("a/c").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn local_store_file_roundtrip() {
        let dir = tempfile::tempdir_in(".").unwrap();
        let s = LocalFsStore::new(dir.path().join("store")).unwrap();
        let src = dir.path().join("src.bin");
        let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
        std::fs::write(&src, &data).unwrap();
        s.put_file("snap/x.db", &src).await.unwrap();
        assert_eq!(&s.get("snap/x.db").await.unwrap().unwrap().data[..], &data[..]);
        let dst = dir.path().join("dst.bin");
        assert!(s.get_to_file("snap/x.db", &dst).await.unwrap());
        assert_eq!(std::fs::read(&dst).unwrap(), data);
        assert!(!s.get_to_file("snap/missing.db", &dst).await.unwrap());
    }

    #[test]
    fn registry_resolves_custom_schemes_and_rejects_duplicates() {
        let dir = tempfile::tempdir_in(".").unwrap();
        let store: DynStore = Arc::new(LocalFsStore::new(dir.path()).unwrap());
        let captured = store.clone();
        let mut registry = StoreRegistry::default();
        registry
            .register("custom", move |location| {
                anyhow::ensure!(location == "bucket/path", "unexpected location");
                Ok(captured.clone())
            })
            .unwrap();
        let resolved = registry.open("CUSTOM://bucket/path").unwrap();
        assert!(Arc::ptr_eq(&store, &resolved));
        assert!(registry.register("Custom", |_| anyhow::bail!("not called")).is_err());
        for scheme in ["", "1bad", "bad/scheme", "bad:scheme"] {
            assert!(registry.register(scheme, |_| anyhow::bail!("not called")).is_err());
        }
    }

    #[test]
    fn default_registry_uses_localfs_and_rejects_unknown_schemes() {
        let dir = tempfile::tempdir_in(".").unwrap();
        let root = dir.path().canonicalize().unwrap();
        for url in [
            root.display().to_string(),
            format!("file://{}", root.display()),
            format!("local://{}", root.display()),
        ] {
            assert_eq!(open(&url).unwrap().describe(), format!("file://{}", root.display()));
        }
        for url in ["s3://bucket", "S3://bucket", "gs://bucket", "https://bucket"] {
            let error = open(url).err().unwrap().to_string();
            assert!(error.contains("unsupported object-store scheme"), "{error}");
        }
        for url in ["s3:bucket", "file:/path", "://bucket", "file://", "az://", "az://a/path"] {
            assert!(open(url).is_err(), "{url}");
        }
        assert!(StoreRegistry::empty().open(root.to_str().unwrap()).is_err());
    }

    #[tokio::test]
    async fn local_store_lists_tmp_named_objects_and_preserves_missing_destination() {
        let dir = tempfile::tempdir_in(".").unwrap();
        let store = LocalFsStore::new(dir.path().join("store")).unwrap();
        store.put("part.tmp-real/value", Bytes::from_static(b"x")).await.unwrap();
        std::fs::write(dir.path().join("store/.staging/unpublished"), b"partial").unwrap();
        assert_eq!(store.list("part").await.unwrap(), vec!["part.tmp-real/value"]);
        for prefix in ["../", "/", "a/../../"] {
            assert!(store.list(prefix).await.is_err(), "{prefix}");
        }

        let dst = dir.path().join("destination");
        assert!(!store.get_to_file("missing", &dst).await.unwrap());
        assert!(!dst.exists());
        std::fs::write(&dst, b"untouched").unwrap();
        assert!(!store.get_to_file("missing", &dst).await.unwrap());
        assert_eq!(std::fs::read(&dst).unwrap(), b"untouched");
        assert!(store.get_range("missing", 0, 0).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn local_store_missing_metadata_is_not_an_empty_version() {
        let dir = tempfile::tempdir_in(".").unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();
        store.put("object", Bytes::from_static(b"content")).await.unwrap();
        std::fs::remove_file(dir.path().join(".meta/object.etag")).unwrap();
        assert!(matches!(store.get("object").await, Err(StoreError::Other(_))));
    }

    #[tokio::test]
    async fn local_store_independent_instances_serialize_conditional_writes() {
        let dir = tempfile::tempdir_in(".").unwrap();
        let first = LocalFsStore::new(dir.path()).unwrap();
        let second = LocalFsStore::new(dir.path()).unwrap();
        let (a, b) = tokio::join!(
            first.put_if_absent("race", Bytes::from_static(b"a")),
            second.put_if_absent("race", Bytes::from_static(b"b")),
        );
        assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
        assert!(
            matches!(a, Err(StoreError::Precondition))
                || matches!(b, Err(StoreError::Precondition))
        );
        let object = first.get("race").await.unwrap().unwrap();
        let (a, b) = tokio::join!(
            first.put_if_match("race", Bytes::from_static(b"c"), &object.etag),
            second.put_if_match("race", Bytes::from_static(b"d"), &object.etag),
        );
        assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
        let expected =
            if let Ok(etag) = a { (b"c".as_slice(), etag) } else { (b"d".as_slice(), b.unwrap()) };
        check_object(&first, "race", expected.0, &expected.1).await.unwrap();
    }

    #[derive(Clone, Copy)]
    enum Fault {
        Content,
        Version,
        ReusedVersion,
        MutatingRejection,
        UnsortedList,
        MissingRange,
        MissingDownload,
    }

    struct FaultyStore {
        inner: LocalFsStore,
        fault: Fault,
    }

    #[async_trait]
    impl ObjectStore for FaultyStore {
        async fn get(&self, key: &str) -> Result<Option<Object>> {
            let mut object = self.inner.get(key).await?;
            if let Some(object) = object.as_mut() {
                match self.fault {
                    Fault::Content => object.data = Bytes::from_static(b"wrong"),
                    Fault::Version => object.etag = "wrong".into(),
                    Fault::ReusedVersion => object.etag = "constant".into(),
                    _ => {}
                }
            }
            Ok(object)
        }
        async fn get_range(&self, key: &str, start: u64, len: u64) -> Result<Option<Bytes>> {
            if matches!(self.fault, Fault::MissingRange) && key.ends_with("/missing") {
                return Ok(Some(Bytes::new()));
            }
            self.inner.get_range(key, start, len).await
        }
        async fn put(&self, key: &str, data: Bytes) -> Result<ETag> {
            self.inner.put(key, data).await
        }
        async fn put_if_absent(&self, key: &str, data: Bytes) -> Result<ETag> {
            let etag = self.inner.put_if_absent(key, data).await?;
            Ok(if matches!(self.fault, Fault::ReusedVersion) { "constant".into() } else { etag })
        }
        async fn put_if_match(&self, key: &str, data: Bytes, etag: &str) -> Result<ETag> {
            if matches!(self.fault, Fault::ReusedVersion) {
                self.inner.put(key, data).await?;
                return Ok("constant".into());
            }
            let result = self.inner.put_if_match(key, data.clone(), etag).await;
            if matches!(self.fault, Fault::MutatingRejection)
                && matches!(result, Err(StoreError::Precondition))
            {
                self.inner.put(key, data).await?;
            }
            result
        }
        async fn put_file(&self, key: &str, path: &Path) -> Result<ETag> {
            self.inner.put_file(key, path).await
        }
        async fn get_to_file(&self, key: &str, path: &Path) -> Result<bool> {
            let found = self.inner.get_to_file(key, path).await?;
            if !found && matches!(self.fault, Fault::MissingDownload) {
                tokio::fs::write(path, b"").await.map_err(anyhow::Error::from)?;
            }
            Ok(found)
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>> {
            let mut keys = self.inner.list(prefix).await?;
            if matches!(self.fault, Fault::UnsortedList) {
                keys.reverse();
            }
            Ok(keys)
        }
        async fn delete(&self, key: &str) -> Result<()> {
            self.inner.delete(key).await
        }
        fn describe(&self) -> String {
            self.inner.describe()
        }
    }

    #[tokio::test]
    async fn conformance_rejects_broken_providers_and_cleans_up() {
        for (fault, expected) in [
            (Fault::Content, "incorrect content"),
            (Fault::Version, "incorrect version"),
            (Fault::ReusedVersion, "reused a version"),
            (Fault::MutatingRejection, "incorrect content"),
            (Fault::UnsortedList, "prefix listing"),
            (Fault::MissingRange, "missing range"),
            (Fault::MissingDownload, "changed destination"),
        ] {
            let dir = tempfile::tempdir_in(".").unwrap();
            let store = FaultyStore { inner: LocalFsStore::new(dir.path()).unwrap(), fault };
            let error = conformance_test(&store).await.unwrap_err().to_string();
            assert!(error.contains(expected), "{error}");
            assert!(store.inner.list("").await.unwrap().is_empty());
        }
    }
}
