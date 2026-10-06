//! Framework-owned invocation context shared by node and runtime extensions.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{ActorRef, CallError, CallOutput, Extensions};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Principal {
    pub subject: String,
    #[serde(default)]
    pub claims: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Caller {
    External,
    Embedded,
    Actor { actor: ActorRef },
    System { name: String },
}

/// Only trusted host code constructs this metadata. Public request JSON and
/// headers are not deserialized into it; peers transport it under their HMAC.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InvocationMetadata {
    pub request_id: String,
    pub parent_request_id: Option<String>,
    pub caller: Caller,
    pub principal: Option<Principal>,
    pub deadline_unix_ms: u64,
    #[serde(default)]
    pub attributes: BTreeMap<String, Value>,
}

fn unix_ms() -> Result<u64, HookError> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH)
        .map_err(|error| HookError::Internal(format!("invalid system clock: {error}")))?;
    u64::try_from(duration.as_millis())
        .map_err(|_| HookError::Internal("system clock exceeds supported range".into()))
}

impl InvocationMetadata {
    pub fn new(caller: Caller, timeout: Duration) -> Result<Self, HookError> {
        let timeout_ms = u64::try_from(timeout.as_millis())
            .map_err(|_| HookError::Internal("invocation timeout exceeds supported range".into()))?;
        Ok(Self {
            request_id: hex::encode(rand::random::<[u8; 16]>()),
            parent_request_id: None,
            caller,
            principal: None,
            deadline_unix_ms: unix_ms()?.checked_add(timeout_ms)
                .ok_or_else(|| HookError::Internal("invocation deadline overflow".into()))?,
            attributes: BTreeMap::new(),
        })
    }

    pub fn child(&self, actor: ActorRef, timeout: Duration) -> Result<Self, HookError> {
        let mut child = Self::new(Caller::Actor { actor }, timeout)?;
        child.parent_request_id = Some(self.request_id.clone());
        child.principal = self.principal.clone();
        child.attributes = self.attributes.clone();
        child.deadline_unix_ms = child.deadline_unix_ms.min(self.deadline_unix_ms);
        Ok(child)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum InvocationOperation {
    Call { method: String, args: Value },
    Create,
    Delete,
    Alarm,
}

/// Typed extension data is node-local and deliberately not forwarded. Use
/// metadata attributes for small serializable, non-secret propagation data.
#[derive(Clone)]
pub struct InvocationContext {
    pub target: ActorRef,
    pub operation: InvocationOperation,
    pub request: InvocationMetadata,
    pub data: Arc<Mutex<Extensions>>,
    deadline: Instant,
}

impl InvocationContext {
    pub fn new(
        target: ActorRef,
        operation: InvocationOperation,
        caller: Caller,
        timeout: Duration,
    ) -> Result<Self, HookError> {
        Self::from_metadata(target, operation, InvocationMetadata::new(caller, timeout)?)
    }

    pub fn from_metadata(
        target: ActorRef,
        operation: InvocationOperation,
        request: InvocationMetadata,
    ) -> Result<Self, HookError> {
        let remaining = request.deadline_unix_ms.checked_sub(unix_ms()?)
            .filter(|remaining| *remaining > 0)
            .ok_or(HookError::Timeout)?;
        Ok(Self {
            target,
            operation,
            request,
            data: Arc::new(Mutex::new(Extensions::default())),
            deadline: Instant::now().checked_add(Duration::from_millis(remaining))
                .ok_or_else(|| HookError::Internal("invocation deadline exceeds supported range".into()))?,
        })
    }

    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    pub fn check_deadline(&self) -> Result<(), HookError> {
        if self.remaining().is_zero() { Err(HookError::Timeout) } else { Ok(()) }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookError {
    Unauthorized(String),
    Denied(String),
    Invalid(String),
    Unavailable(String),
    Internal(String),
    Timeout,
}

impl HookError {
    pub fn status(&self) -> u16 {
        match self {
            Self::Unauthorized(_) => 401,
            Self::Denied(_) => 403,
            Self::Invalid(_) => 400,
            Self::Unavailable(_) => 503,
            Self::Internal(_) => 500,
            Self::Timeout => 504,
        }
    }

    pub fn code(&self) -> &'static str {
        match self {
            Self::Unauthorized(_) => "unauthorized",
            Self::Denied(_) => "forbidden",
            Self::Invalid(_) => "extension_invalid",
            Self::Unavailable(_) => "extension_unavailable",
            Self::Internal(_) => "extension_error",
            Self::Timeout => "extension_timeout",
        }
    }
}

impl std::fmt::Display for HookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unauthorized(message) | Self::Denied(message) | Self::Invalid(message)
            | Self::Unavailable(message) | Self::Internal(message) => f.write_str(message),
            Self::Timeout => f.write_str("invocation extension deadline expired"),
        }
    }
}

impl std::error::Error for HookError {}

/// Runs trusted synchronous code with panic and deadline protection. Native
/// hooks cannot be forcibly interrupted; expiry is checked when they return.
pub fn run_hook(
    context: &InvocationContext,
    hook: impl FnOnce() -> Result<(), HookError>,
) -> Result<(), HookError> {
    context.check_deadline()?;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(hook))
        .unwrap_or_else(|_| Err(HookError::Internal("invocation hook panicked".into())));
    context.check_deadline()?;
    result
}

/// Synchronous hooks for runtime-only embeddings. Native implementations must
/// honor deadlines; epoch interruption cannot interrupt arbitrary native code.
pub trait ExecutionExtension: Send + Sync {
    fn name(&self) -> &'static str { std::any::type_name::<Self>() }
    fn before_guest(&self, _context: &InvocationContext) -> Result<(), HookError> { Ok(()) }
    fn after_guest(
        &self,
        _context: &InvocationContext,
        _outcome: &Result<CallOutput, CallError>,
    ) -> Result<(), HookError> { Ok(()) }
}

/// Restricted transactional state access: extension code cannot commit,
/// checkpoint, capture, or open a second transaction through this handle.
pub struct TransactionState<'a> {
    database: &'a mut dyn crate::database::Database,
}

impl<'a> TransactionState<'a> {
    pub fn new(database: &'a mut dyn crate::database::Database) -> Self { Self { database } }

    pub fn execute(&mut self, sql: &str, params: &[crate::database::SqlValue]) -> anyhow::Result<u64> {
        if let Some(keyword) = crate::forbidden(sql) {
            anyhow::bail!("{keyword} is not allowed: the host manages transactions");
        }

        self.database.execute(sql, params)
    }

    pub fn query(&mut self, sql: &str, params: &[crate::database::SqlValue]) -> anyhow::Result<crate::database::SqlRows> {
        if let Some(keyword) = crate::forbidden(sql) {
            anyhow::bail!("{keyword} is not allowed: the host manages transactions");
        }
        self.database.query(sql, params)
    }

    pub fn alarm(&mut self) -> anyhow::Result<Option<crate::alarm::Alarm>> {
        self.database.alarm()
    }

    pub fn set_alarm(&mut self, at_ms: u64, retry: u32, epoch: u64) -> anyhow::Result<()> {
        self.database.set_alarm(at_ms, retry, epoch)
    }

    pub fn clear_alarm(&mut self) -> anyhow::Result<()> {
        self.database.clear_alarm()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn child_preserves_identity_and_never_extends_parent_deadline() {
        let mut parent = InvocationMetadata::new(Caller::External, Duration::from_secs(1)).unwrap();
        parent.principal = Some(Principal { subject: "alice".into(), claims: BTreeMap::from([("role".into(), Value::from("reader"))]) });
        parent.attributes.insert("trace".into(), Value::from("trace-1"));
        let actor = ActorRef { app: "a".into(), actor_type: "t".into(), key: "k".into() };
        let child = parent.child(actor.clone(), Duration::from_secs(60)).unwrap();
        assert_ne!(child.request_id, parent.request_id);
        assert_eq!(child.parent_request_id.as_ref(), Some(&parent.request_id));
        assert_eq!(child.caller, Caller::Actor { actor });
        assert_eq!(child.principal, parent.principal);
        assert_eq!(child.attributes, parent.attributes);
        assert_eq!(child.deadline_unix_ms, parent.deadline_unix_ms);
        assert!(parent.child(ActorRef { app: "a".into(), actor_type: "t".into(), key: "k".into() },
            Duration::from_millis(5)).unwrap().deadline_unix_ms < parent.deadline_unix_ms);
    }

    #[test]
    fn metadata_cannot_reset_monotonic_deadline() {
        let actor = ActorRef { app: "a".into(), actor_type: "t".into(), key: "k".into() };
        let mut context = InvocationContext::new(actor, InvocationOperation::Create, Caller::Embedded,
            Duration::from_millis(5)).unwrap();
        context.request.deadline_unix_ms = u64::MAX;
        std::thread::sleep(Duration::from_millis(10));
        assert_eq!(context.check_deadline(), Err(HookError::Timeout));
    }
}
