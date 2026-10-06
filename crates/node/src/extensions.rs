//! Ordered admission, execution, transaction, and observation extensions.

use async_trait::async_trait;
use axum::http::{HeaderMap, Method, Uri};
pub use statex_runtime::invocation::{
    Caller, HookError, InvocationContext, InvocationMetadata, InvocationOperation, Principal,
    TransactionState,
};
use statex_runtime::{ActorRef, CallOutput};

use crate::node::Outcome;

pub(crate) async fn run_hook(
    future: impl std::future::Future<Output = Result<(), HookError>>,
    name: &str,
    phase: &str,
    budget: std::time::Duration,
) -> Result<(), HookError> {
    let mut future = Box::pin(future);
    let guarded = std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(result) => result,
            Err(_) => std::task::Poll::Ready(Err(HookError::Internal(format!(
                "extension {name} panicked during {phase}"
            )))),
        }
    });
    let result = tokio::time::timeout(budget, guarded)
        .await
        .unwrap_or(Err(HookError::Timeout));
    if let Err(error) = &result {
        tracing::warn!(extension = name, phase, %error, "invocation extension failed");
    }
    result
}

pub(crate) async fn observe_lifecycle(
    extensions: &[std::sync::Arc<dyn InvocationExtension>],
    budget: std::time::Duration,
    event: &LifecycleEvent,
) {
    for extension in extensions {
        let _ = run_hook(
            extension.lifecycle(event),
            extension.name(),
            "lifecycle",
            budget,
        )
        .await;
    }
}

/// Untrusted HTTP facts, available only to admission on the receiving node.
/// Authentication extensions must validate credentials before setting a
/// principal. These headers are never forwarded to another node.
#[derive(Clone)]
pub struct HttpRequestInfo {
    pub method: Method,
    pub uri: Uri,
    pub headers: HeaderMap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionKind {
    Invocation,
    AlarmMaintenance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleKind {
    Activated,
    CodeReplaced,
    Evicted,
    Deleted,
    Shutdown,
    Fenced,
    Discarded,
}

#[derive(Debug, Clone)]
pub struct LifecycleEvent {
    pub actor: ActorRef,
    pub epoch: u64,
    pub node_id: String,
    pub kind: LifecycleKind,
}

/// Native extension implementations are trusted host code. Default methods
/// are no-ops; registration order determines order at every phase.
#[async_trait]
pub trait InvocationExtension: Send + Sync {
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Once at the entry node, before routing/activation. May authenticate and
    /// enrich trusted metadata; cannot rewrite target, operation, or deadline.
    async fn admit(&self, _context: &mut InvocationContext) -> Result<(), HookError> {
        Ok(())
    }

    /// On the execution owner, before activation, migrations, or a transaction.
    async fn before_execute(&self, _context: &InvocationContext) -> Result<(), HookError> {
        Ok(())
    }

    /// Inside the transaction, after a successful guest result and before
    /// commit. Rejecting rolls back guest, migration, alarm, and hook writes.
    fn before_commit(
        &self,
        _context: &InvocationContext,
        _kind: TransactionKind,
        _state: &mut TransactionState<'_>,
        _output: &CallOutput,
    ) -> Result<(), HookError> {
        Ok(())
    }

    /// Entry-node observation of the immutable final response, not a veto.
    async fn completed(
        &self,
        _context: &InvocationContext,
        _outcome: &Outcome,
    ) -> Result<(), HookError> {
        Ok(())
    }

    /// Best-effort observation after the indicated transition, not a veto.
    async fn lifecycle(&self, _event: &LifecycleEvent) -> Result<(), HookError> {
        Ok(())
    }
}
