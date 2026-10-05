//! App deployments: content-addressed component + manifest, and a CAS'd
//! `current.json` pointer that nodes poll.
//!
//! Every deploy is checked for breaking changes against the version it
//! replaces (see `statex_runtime::compat`). Who may deploy an app is decided
//! by write access to `deploy/` in the store, not by statex.

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
}

/// How a deploy treats compatibility.
#[derive(Debug, Clone, Default)]
pub struct DeployOptions {
    /// Deploy even if the new version breaks clients or existing actors.
    pub allow_breaking: bool,
    /// Deploy even if actor types this app calls are not deployed or do not
    /// match its client interfaces.
    pub allow_unresolved_calls: bool,
}

/// Why a deploy was refused.
#[derive(Debug, thiserror::Error)]
pub enum DeployError {
    #[error("deploy of {app:?} has breaking changes against the deployed version {version}:\n  - {}\nKeep the old surface (add new methods or types instead), or pass --allow-breaking", changes.join("\n  - "))]
    Breaking { app: String, version: u64, changes: Vec<String> },
    #[error("deploy of {app:?} calls actor types that are not deployed or do not match its client interfaces:\n  - {}\nDeploy the callees first, or regenerate the client interfaces (`statex calls sync`); pass --allow-unresolved-calls to deploy anyway", problems.join("\n  - "))]
    UnresolvedCalls { app: String, problems: Vec<String> },
}

/// Checks `manifest`'s client interfaces against the deployed callees (or
/// `manifest` itself for calls within the app).
pub async fn unresolved_calls(store: &DynStore, manifest: &Manifest) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let mut callees: std::collections::BTreeMap<&str, Option<Manifest>> = Default::default();
    for c in &manifest.calls {
        if c.app == manifest.app {
            out.extend(statex_runtime::call_mismatches(c, manifest).into_iter().map(|p| format!("{}: {p}", c.import)));
            continue;
        }
        if !callees.contains_key(c.app.as_str()) {
            let m = match get_json::<Current>(&**store, &deploy_current(&c.app)).await? {
                Some((cur, _)) => Some(fetch_manifest(store, &cur).await?),
                None => None,
            };
            callees.insert(&c.app, m);
        }
        match &callees[c.app.as_str()] {
            None => out.push(format!("{}: app {} is not deployed", c.import, c.app)),
            Some(m) => out.extend(statex_runtime::call_mismatches(c, m).into_iter().map(|p| format!("{}: {p}", c.import))),
        }
    }
    Ok(out)
}

/// Deployed apps whose client interfaces of `manifest.app` would no longer
/// match if `manifest` were deployed.
pub async fn broken_callers(store: &DynStore, manifest: &Manifest) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for cur in list(store).await? {
        if cur.app == manifest.app {
            continue;
        }
        let caller = fetch_manifest(store, &cur).await?;
        for c in caller.calls.iter().filter(|c| c.app == manifest.app) {
            for p in statex_runtime::call_mismatches(c, manifest) {
                out.push(format!("deployed caller {} ({}): {p}", cur.app, c.import));
            }
        }
    }
    Ok(out)
}

/// Uploads a version and makes it current. Idempotent for the same binary.
pub async fn deploy(store: &DynStore, wasm: &[u8], manifest: &Manifest, opts: &DeployOptions) -> Result<Current> {
    let (app, sha) = (&manifest.app, &manifest.sha256);
    anyhow::ensure!(statex_runtime::sha256_hex(wasm) == *sha, "manifest sha256 does not match component");
    let manifest_bytes = to_json_bytes(manifest);
    let id = statex_runtime::sha256_hex(&[sha.as_bytes(), &manifest_bytes[..]].concat())[..32].to_string();
    if !opts.allow_unresolved_calls {
        let problems = unresolved_calls(store, manifest).await?;
        if !problems.is_empty() {
            return Err(DeployError::UnresolvedCalls { app: app.clone(), problems }.into());
        }
    }
    store.put(&deploy_object(app, &id, "component.wasm"), Bytes::copy_from_slice(wasm)).await?;
    store.put(&deploy_object(app, &id, "manifest.json"), manifest_bytes).await?;
    let key = deploy_current(app);
    loop {
        let old = get_json::<Current>(&**store, &key).await?;
        if let Some((c, _)) = &old {
            if c.id == id {
                return Ok(c.clone());
            }
            if !opts.allow_breaking {
                let (_, prev) = fetch(store, c).await?;
                let mut changes = statex_runtime::breaking_changes((&prev).into(), manifest.into());
                changes.extend(broken_callers(store, manifest).await?);
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

/// The manifest of a deployment, without downloading its component.
pub async fn fetch_manifest(store: &DynStore, cur: &Current) -> Result<Manifest> {
    let (m, _) = get_json::<Manifest>(&**store, &deploy_object(&cur.app, &cur.id, "manifest.json"))
        .await?
        .ok_or_else(|| anyhow!("missing manifest for {}@{}", cur.app, cur.id))?;
    anyhow::ensure!(m.sha256 == cur.sha256, "manifest checksum mismatch for {}@{}", cur.app, cur.id);
    Ok(m)
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
