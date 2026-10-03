//! Actor ownership records and the acquire protocol.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use statex_store::{get_json, to_json_bytes, DynStore, ETag, StoreError};

use crate::layout::{now_ms, ActorId};
use crate::lease::{read_node, NodeRecord};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OwnerState {
    Owned,
    Unowned,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OwnerRecord {
    pub app: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub key: String,
    pub node: String,
    pub session: String,
    /// Strictly increases on every activation; data is written under `ltx/e<epoch>/`.
    pub epoch: u64,
    pub state: OwnerState,
    pub updated_at_ms: u64,
}

#[derive(Debug)]
pub enum Acquire {
    /// We own the actor at `epoch`. `fresh` means there is no prior data to restore.
    Acquired { epoch: u64, etag: ETag, fresh: bool },
    /// A live node owns it.
    Remote(NodeRecord),
    /// `create_only` was requested but the actor exists.
    Exists,
}

pub async fn read(store: &DynStore, id: &ActorId) -> Result<Option<(OwnerRecord, ETag)>> {
    Ok(get_json::<OwnerRecord>(&**store, &id.owner_key()).await?)
}

/// Takes ownership of an actor if it is unowned or its owner is dead.
pub async fn acquire(store: &DynStore, id: &ActorId, me: &NodeRecord, create_only: bool) -> Result<Acquire> {
    let key = id.owner_key();
    loop {
        let current = read(store, id).await?;
        let (epoch, fresh, res) = match &current {
            None => {
                let rec = record(id, me, 1, OwnerState::Owned);
                (1, true, store.put_if_absent(&key, to_json_bytes(&rec)).await)
            }
            Some((old, etag)) => {
                if create_only && old.state != OwnerState::Deleted {
                    return Ok(Acquire::Exists);
                }
                if old.state == OwnerState::Owned && !(old.node == me.node_id && old.session == me.session) {
                    if let Some(n) = read_node(store, &old.node).await? {
                        if n.session == old.session && n.alive() {
                            return Ok(Acquire::Remote(n));
                        }
                    }
                    tracing::info!(actor = %id, "taking over from dead owner {} (epoch {})", old.node, old.epoch);
                }
                let rec = record(id, me, old.epoch + 1, OwnerState::Owned);
                let fresh = old.state == OwnerState::Deleted;
                (rec.epoch, fresh, store.put_if_match(&key, to_json_bytes(&rec), etag).await)
            }
        };
        match res {
            Ok(etag) => return Ok(Acquire::Acquired { epoch, etag, fresh }),
            Err(StoreError::Precondition) => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

/// Gives up ownership (idle eviction / shutdown) or marks the actor deleted.
/// Returns false if the record was no longer ours. The epoch is kept so it
/// stays monotonic across releases and deletes.
pub async fn release(store: &DynStore, id: &ActorId, me: &NodeRecord, epoch: u64, etag: &str, state: OwnerState) -> Result<bool> {
    let rec = record(id, me, epoch, state);
    match store.put_if_match(&id.owner_key(), to_json_bytes(&rec), etag).await {
        Ok(_) => Ok(true),
        Err(StoreError::Precondition) => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// The ack rule: the record must still name us at this epoch.
pub async fn still_owner(store: &DynStore, id: &ActorId, me: &NodeRecord, epoch: u64) -> Result<bool> {
    Ok(match read(store, id).await? {
        Some((r, _)) => r.state == OwnerState::Owned && r.node == me.node_id && r.session == me.session && r.epoch == epoch,
        None => false,
    })
}

fn record(id: &ActorId, me: &NodeRecord, epoch: u64, state: OwnerState) -> OwnerRecord {
    OwnerRecord {
        app: id.app.clone(),
        ty: id.ty.clone(),
        key: id.key.clone(),
        node: me.node_id.clone(),
        session: me.session.clone(),
        epoch,
        state,
        updated_at_ms: now_ms(),
    }
}
