//! Local filesystem object store. Safe for multiple processes on one machine
//! (per-object advisory file locks), used for `statex dev` and tests.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::{ETag, Object, ObjectStore, Result, StoreError};

#[derive(Clone)]
pub struct LocalFsStore {
    root: PathBuf,
}

impl LocalFsStore {
    pub fn new(root: impl AsRef<Path>) -> anyhow::Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("objects"))?;
        fs::create_dir_all(root.join(".meta"))?;
        fs::create_dir_all(root.join(".locks"))?;
        let root = root.canonicalize()?;
        Ok(Self { root })
    }

    fn validate(key: &str) -> Result<()> {
        if key.is_empty()
            || key.starts_with('/')
            || key.split('/').any(|s| s.is_empty() || s == "." || s == "..")
        {
            return Err(StoreError::Other(anyhow::anyhow!("invalid object key {key:?}")));
        }
        Ok(())
    }

    fn obj_path(&self, key: &str) -> PathBuf {
        self.root.join("objects").join(key)
    }
    fn meta_path(&self, key: &str) -> PathBuf {
        self.root.join(".meta").join(format!("{key}.etag"))
    }

    fn lock(&self, key: &str) -> anyhow::Result<File> {
        let h = hex::encode(&Sha256::digest(key.as_bytes())[..16]);
        let f = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.root.join(".locks").join(h))?;
        f.lock()?;
        Ok(f)
    }

    fn read_etag(&self, key: &str) -> Option<String> {
        fs::read_to_string(self.meta_path(key)).ok()
    }

    fn write_locked(&self, key: &str, data: &[u8]) -> anyhow::Result<ETag> {
        self.write_locked_with(key, |f| Ok(f.write_all(data)?))
    }

    /// Writes a new version of `key` via a temp file filled by `fill`.
    fn write_locked_with(
        &self,
        key: &str,
        fill: impl FnOnce(&mut File) -> anyhow::Result<()>,
    ) -> anyhow::Result<ETag> {
        let p = self.obj_path(key);
        fs::create_dir_all(p.parent().unwrap())?;
        let tmp = p.with_extension(format!("tmp-{:016x}", rand::random::<u64>()));
        let res = (|| {
            let mut f = File::create(&tmp)?;
            fill(&mut f)?;
            f.sync_all()?;
            Ok(())
        })();
        if let Err(e) = res {
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }
        let etag = format!("\"{:016x}\"", rand::random::<u64>());
        let m = self.meta_path(key);
        fs::create_dir_all(m.parent().unwrap())?;
        fs::write(&m, &etag)?;
        fs::rename(&tmp, &p)?;
        Ok(etag)
    }

    fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(LocalFsStore) -> Result<T> + Send + 'static,
    ) -> impl std::future::Future<Output = Result<T>> {
        let me = self.clone();
        async move {
            tokio::task::spawn_blocking(move || f(me))
                .await
                .map_err(|e| StoreError::Other(e.into()))?
        }
    }
}

fn walk(dir: &Path, base: &Path, out: &mut Vec<String>) -> std::io::Result<()> {
    let rd = match fs::read_dir(dir) {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for ent in rd {
        let ent = ent?;
        let p = ent.path();
        if ent.file_type()?.is_dir() {
            walk(&p, base, out)?;
        } else {
            let rel = p.strip_prefix(base).unwrap().to_string_lossy().replace('\\', "/");
            if !rel.contains(".tmp-") {
                out.push(rel);
            }
        }
    }
    Ok(())
}

#[async_trait]
impl ObjectStore for LocalFsStore {
    async fn get(&self, key: &str) -> Result<Option<Object>> {
        Self::validate(key)?;
        let key = key.to_string();
        self.blocking(move |s| {
            let _l = s.lock(&key)?;
            match fs::read(s.obj_path(&key)) {
                Ok(d) => Ok(Some(Object {
                    data: Bytes::from(d),
                    etag: s.read_etag(&key).unwrap_or_default(),
                })),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(StoreError::Other(e.into())),
            }
        })
        .await
    }

    async fn get_range(&self, key: &str, start: u64, len: u64) -> Result<Option<Bytes>> {
        Self::validate(key)?;
        let key = key.to_string();
        self.blocking(move |s| {
            let mut f = match File::open(s.obj_path(&key)) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(StoreError::Other(e.into())),
            };
            f.seek(SeekFrom::Start(start)).map_err(anyhow::Error::from)?;
            let mut buf = Vec::new();
            f.take(len).read_to_end(&mut buf).map_err(anyhow::Error::from)?;
            Ok(Some(Bytes::from(buf)))
        })
        .await
    }

    async fn put(&self, key: &str, data: Bytes) -> Result<ETag> {
        Self::validate(key)?;
        let key = key.to_string();
        self.blocking(move |s| {
            let _l = s.lock(&key)?;
            Ok(s.write_locked(&key, &data)?)
        })
        .await
    }

    async fn put_file(&self, key: &str, path: &Path) -> Result<ETag> {
        Self::validate(key)?;
        let key = key.to_string();
        let src = path.to_path_buf();
        self.blocking(move |s| {
            let _l = s.lock(&key)?;
            Ok(s.write_locked_with(&key, |f| {
                let mut r = File::open(&src).with_context(|| format!("open {}", src.display()))?;
                std::io::copy(&mut r, f)?;
                Ok(())
            })?)
        })
        .await
    }

    async fn get_to_file(&self, key: &str, path: &Path) -> Result<bool> {
        Self::validate(key)?;
        let key = key.to_string();
        let dst = path.to_path_buf();
        self.blocking(move |s| {
            let _l = s.lock(&key)?;
            let mut r = match File::open(s.obj_path(&key)) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
                Err(e) => return Err(StoreError::Other(e.into())),
            };
            let mut w = File::create(&dst).with_context(|| format!("create {}", dst.display()))?;
            std::io::copy(&mut r, &mut w).map_err(anyhow::Error::from)?;
            w.sync_all().map_err(anyhow::Error::from)?;
            Ok(true)
        })
        .await
    }

    async fn put_if_absent(&self, key: &str, data: Bytes) -> Result<ETag> {
        Self::validate(key)?;
        let key = key.to_string();
        self.blocking(move |s| {
            let _l = s.lock(&key)?;
            if s.obj_path(&key).exists() {
                return Err(StoreError::Precondition);
            }
            Ok(s.write_locked(&key, &data)?)
        })
        .await
    }

    async fn put_if_match(&self, key: &str, data: Bytes, etag: &str) -> Result<ETag> {
        Self::validate(key)?;
        let key = key.to_string();
        let etag = etag.to_string();
        self.blocking(move |s| {
            let _l = s.lock(&key)?;
            if !s.obj_path(&key).exists() || s.read_etag(&key).as_deref() != Some(etag.as_str()) {
                return Err(StoreError::Precondition);
            }
            Ok(s.write_locked(&key, &data)?)
        })
        .await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let prefix = prefix.to_string();
        self.blocking(move |s| {
            let base = s.root.join("objects");
            // Walk only the deepest directory fully contained in the prefix.
            let dir_part = match prefix.rfind('/') {
                Some(i) => &prefix[..i],
                None => "",
            };
            let mut out = Vec::new();
            walk(&base.join(dir_part), &base, &mut out).context("list")?;
            out.retain(|k| k.starts_with(&prefix));
            out.sort();
            Ok(out)
        })
        .await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        Self::validate(key)?;
        let key = key.to_string();
        self.blocking(move |s| {
            let _l = s.lock(&key)?;
            match fs::remove_file(s.obj_path(&key)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(StoreError::Other(e.into())),
            }
            let _ = fs::remove_file(s.meta_path(&key));
            Ok(())
        })
        .await
    }

    fn describe(&self) -> String {
        format!("file://{}", self.root.display())
    }
}
