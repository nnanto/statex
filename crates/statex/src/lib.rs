//! Stateful WebAssembly actors with local defaults and explicit extension
//! boundaries. Start with [`local_config`], or construct [`NodeConfig`] with
//! your own object store, database factory, and host runtime.

use std::path::Path;
use std::sync::Arc;

pub use statex_node::extensions;
pub use statex_node::extensions::{
    InvocationExtension, LifecycleEvent, LifecycleKind, TransactionKind,
};
pub use statex_node::{deploy, start, ActorId, InvOp, Invocation, NodeConfig, NodeHandle, Outcome};
pub use statex_runtime as runtime;
pub use statex_runtime::database::{Database, DatabaseFactory, DatabaseHandle, SqliteFactory};
pub use statex_runtime::invocation::{
    Caller, ExecutionExtension, HookError, InvocationContext, InvocationMetadata,
    InvocationOperation, Principal, TransactionState,
};
pub use statex_store as store;
pub use statex_store::{DynStore, ObjectStore, StoreRegistry};

/// Configuration for a single local node: local filesystem object store,
/// SQLite actor state, standard host runtime, and an ephemeral loopback port.
///
/// Reuse the root and node ID across restarts to restore durable actors.
/// Different nodes must have distinct IDs and replica directories; use
/// `NodeConfig::new` with a shared object store when constructing a fleet.
pub fn local_config(
    root: impl AsRef<Path>,
    node_id: impl Into<String>,
) -> anyhow::Result<NodeConfig> {
    let root = root.as_ref();
    let object_store = Arc::new(store::LocalFsStore::new(root.join("store"))?);
    Ok(NodeConfig::new(
        node_id,
        object_store,
        root.join("replicas"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_defaults_start_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        let first = start(local_config(dir.path(), "local").unwrap())
            .await
            .unwrap();
        assert!(first.addr.ip().is_loopback());
        assert_ne!(first.addr.port(), 0);
        assert!(first.node.lease.valid());
        first.shutdown().await;
        let second = start(local_config(dir.path(), "local").unwrap())
            .await
            .unwrap();
        assert!(second.node.lease.valid());
        second.shutdown().await;
    }
}
