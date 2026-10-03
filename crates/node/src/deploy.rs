//! App deployments: content-addressed component + manifest, and a CAS'd
//! `current.json` pointer that nodes poll.
//!
//! `current.json` also records the app's owner. The first deploy of an app
//! claims it (create-if-absent), and later deploys by a different owner are
//! refused unless they explicitly take over. Because the claim lives in the
//! same CAS'd record as the version pointer, two teams racing to deploy the
//! same new name cannot both win. Every deploy is also checked for breaking
//! changes against the version it replaces (see `statex_runtime::compat`).

use anyhow::{anyhow, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use statex_runtime::Manifest;
use statex_store::{get_json, to_json_bytes, DynStore, StoreError};

use crate::layout::{app_dir, deploy_current, deploy_object, now_ms};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Current {
    pub app: String,
    /// Deployment id: hash of the component and its manifest.
    pub id: String,
    /// sha256 of the component.
    pub sha256: String,
    /// Increments on every deploy.
    pub version: u64,
    pub deployed_at_ms: u64,
    /// Team or principal that owns the app name. Empty for records written
    /// before ownership existed; the next deploy claims them.
    #[serde(default)]
    pub owner: String,
}

/// How a deploy treats ownership and compatibility.
#[derive(Debug, Clone, Default)]
pub struct DeployOptions {
    /// Who is deploying (a team, e.g. `payments`).
    pub owner: String,
    /// Transfer the app to `owner` if someone else owns it.
    pub take_over: bool,
    /// Deploy even if the new version breaks clients or existing actors.
    pub allow_breaking: bool,
}

impl DeployOptions {
    pub fn new(owner: impl Into<String>) -> Self {
        Self { owner: owner.into(), ..Default::default() }
    }
}

/// Why a deploy was refused.
#[derive(Debug, thiserror::Error)]
pub enum DeployError {
    #[error("app {app:?} is owned by {owner:?}; you are deploying as {you:?}. Pick another app name, or pass --take-over if the app really moved to you")]
    NotOwner { app: String, owner: String, you: String },
    #[error("deploy of {app:?} has breaking changes against the deployed version {version}:\n  - {}\nKeep the old surface (add new methods or types instead), or pass --allow-breaking", changes.join("\n  - "))]
    Breaking { app: String, version: u64, changes: Vec<String> },
}

/// Uploads a version and makes it current. Idempotent for the same binary.
pub async fn deploy(store: &DynStore, wasm: &[u8], manifest: &Manifest, opts: &DeployOptions) -> Result<Current> {
    anyhow::ensure!(!opts.owner.is_empty(), "deploy owner must not be empty");
    let (app, sha) = (&manifest.app, &manifest.sha256);
    anyhow::ensure!(statex_runtime::sha256_hex(wasm) == *sha, "manifest sha256 does not match component");
    let manifest_bytes = to_json_bytes(manifest);
    let id = statex_runtime::sha256_hex(&[sha.as_bytes(), &manifest_bytes[..]].concat())[..32].to_string();
    store.put(&deploy_object(app, &id, "component.wasm"), Bytes::copy_from_slice(wasm)).await?;
    store.put(&deploy_object(app, &id, "manifest.json"), manifest_bytes).await?;
    let key = deploy_current(app);
    loop {
        let old = get_json::<Current>(&**store, &key).await?;
        if let Some((c, _)) = &old {
            if !c.owner.is_empty() && c.owner != opts.owner && !opts.take_over {
                return Err(DeployError::NotOwner { app: app.clone(), owner: c.owner.clone(), you: opts.owner.clone() }.into());
            }
            if c.id == id && c.owner == opts.owner {
                return Ok(c.clone());
            }
            if c.id != id && !opts.allow_breaking {
                let (_, prev) = fetch(store, c).await?;
                let changes = statex_runtime::breaking_changes((&prev).into(), manifest.into());
                if !changes.is_empty() {
                    return Err(DeployError::Breaking { app: app.clone(), version: c.version, changes }.into());
                }
            }
        }
        let cur = Current {
            app: app.clone(),
            id: id.clone(),
            sha256: sha.clone(),
            version: old.as_ref().map_or(1, |(c, _)| c.version + 1),
            deployed_at_ms: now_ms(),
            owner: opts.owner.clone(),
        };
        let res = match &old {
            None => store.put_if_absent(&key, to_json_bytes(&cur)).await,
            Some((_, etag)) => store.put_if_match(&key, to_json_bytes(&cur), etag).await,
        };
        match res {
            Ok(_) => return Ok(cur),
            Err(StoreError::Precondition) => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

pub async fn list(store: &DynStore) -> Result<Vec<Current>> {
    let mut out = Vec::new();
    for k in store.list("deploy/").await? {
        if k.ends_with("/current.json") && k.matches('/').count() == 2 {
            if let Some((c, _)) = get_json::<Current>(&**store, &k).await? {
                out.push(c);
            }
        }
    }
    Ok(out)
}

/// Manifests of every version of `app` ever uploaded to the store, in no particular order.
pub async fn history(store: &DynStore, app: &str) -> Result<Vec<Manifest>> {
    let mut out = Vec::new();
    for k in store.list(&format!("deploy/{}/", app_dir(app))).await? {
        if k.ends_with("/manifest.json") {
            if let Some((m, _)) = get_json::<Manifest>(&**store, &k).await? {
                out.push(m);
            }
        }
    }
    Ok(out)
}

pub async fn fetch(store: &DynStore, cur: &Current) -> Result<(Vec<u8>, Manifest)> {
    let (app, id) = (&cur.app, &cur.id);
    let wasm = store
        .get(&deploy_object(app, id, "component.wasm"))
        .await?
        .ok_or_else(|| anyhow!("missing component for {app}@{id}"))?;
    let (manifest, _) = get_json::<Manifest>(&**store, &deploy_object(app, id, "manifest.json"))
        .await?
        .ok_or_else(|| anyhow!("missing manifest for {app}@{id}"))?;
    anyhow::ensure!(
        statex_runtime::sha256_hex(&wasm.data) == cur.sha256 && manifest.sha256 == cur.sha256,
        "component checksum mismatch for {app}@{id}"
    );
    Ok((wasm.data.to_vec(), manifest))
}
