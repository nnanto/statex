//! Object-store abstraction used for leases, ownership records, deployments
//! and replicated actor data.
//!
//! statex needs four properties from the store:
//! conditional create, conditional overwrite, read-after-write consistency and
//! ranged reads. Listing must also be consistent after a write.

mod azure;
mod local;

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

pub use azure::AzureBlobStore;
pub use local::LocalFsStore;

/// Opaque version token of an object (HTTP ETag or equivalent).
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

#[async_trait]
pub trait ObjectStore: Send + Sync + 'static {
    async fn get(&self, key: &str) -> Result<Option<Object>>;
    async fn get_range(&self, key: &str, start: u64, len: u64) -> Result<Option<Bytes>>;
    /// Unconditional write.
    async fn put(&self, key: &str, data: Bytes) -> Result<ETag>;
    /// Create only if the object does not exist.
    async fn put_if_absent(&self, key: &str, data: Bytes) -> Result<ETag>;
    /// Overwrite only if the current version matches `etag`.
    async fn put_if_match(&self, key: &str, data: Bytes, etag: &str) -> Result<ETag>;
    /// Lists keys with the given prefix, sorted lexicographically.
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    async fn delete(&self, key: &str) -> Result<()>;
    /// Human readable location, for logs.
    fn describe(&self) -> String;
}

pub type DynStore = Arc<dyn ObjectStore>;

/// Opens a store from a URL: `file:///abs/path`, a plain path, or `az://container`.
pub fn open(url: &str) -> anyhow::Result<DynStore> {
    if let Some(container) = url.strip_prefix("az://") {
        let container = container.trim_end_matches('/');
        return Ok(Arc::new(AzureBlobStore::from_env(container)?));
    }
    let path = url.strip_prefix("file://").unwrap_or(url);
    Ok(Arc::new(LocalFsStore::new(path)?))
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

/// Runs the conditional-write conformance test (a 4-write probe) plus a
/// ranged read. Returns an error describing the violated property.
pub async fn conformance_test(store: &dyn ObjectStore) -> anyhow::Result<()> {
    let id: u64 = rand::random();
    let key = format!("probe/{id:016x}");
    let e1 = store
        .put_if_absent(&key, Bytes::from_static(b"one"))
        .await
        .map_err(|e| anyhow::anyhow!("conditional create failed: {e}"))?;
    match store.put_if_absent(&key, Bytes::from_static(b"two")).await {
        Err(StoreError::Precondition) => {}
        Ok(_) => anyhow::bail!("store accepted a conditional create of an existing object"),
        Err(e) => anyhow::bail!("reject-create returned ambiguous error: {e}"),
    }
    let e2 = store
        .put_if_match(&key, Bytes::from_static(b"0123456789"), &e1)
        .await
        .map_err(|e| anyhow::anyhow!("conditional update failed: {e}"))?;
    match store.put_if_match(&key, Bytes::from_static(b"stale"), &e1).await {
        Err(StoreError::Precondition) => {}
        Ok(_) => anyhow::bail!("store accepted a stale conditional overwrite"),
        Err(e) => anyhow::bail!("reject-stale returned ambiguous error: {e}"),
    }
    let _ = e2;
    let r = store.get_range(&key, 3, 4).await?.unwrap_or_default();
    anyhow::ensure!(&r[..] == b"3456", "ranged read returned wrong bytes: {r:?}");
    let listed = store.list("probe/").await?;
    anyhow::ensure!(listed.contains(&key), "list-after-write did not include probe object");
    store.delete(&key).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_store_conformance() {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalFsStore::new(dir.path()).unwrap();
        conformance_test(&store).await.unwrap();
    }

    #[tokio::test]
    async fn local_store_list_and_cas() {
        let dir = tempfile::tempdir().unwrap();
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
}
