# Invocation and lifecycle extensions

Register native `InvocationExtension` implementations in
`NodeConfig.extensions`. The default is an empty chain: no authentication,
authorization, or policy is imposed. Hooks apply to HTTP and embedded calls,
actor-to-actor calls, explicit creation/deletion, and system alarms.
Implement only the methods you need. Hooks run in registration order.

| Hook | Location | Effect of rejection |
|---|---|---|
| `admit` | Entry node, once before routing retries | No actor activation or transaction |
| `before_execute` | Actual owner, before activation/migrations/transaction | No guest execution or actor-state writes |
| `before_commit` | After a successful result, inside the actor transaction | Roll back guest, migrations, alarms, and hook writes |
| `completed` | Entry node, after the final acknowledgement decision | Observer only; cannot change the response |
| `lifecycle` | Node where the resident-actor transition happened | Observer only; cannot veto the transition |

Admission is not repeated by signed forwarding. The receiving owner still
executes its own policy checks. A successful owner check is reused across
local activation retries; it is not rerun for every routing attempt.
Registration order is deterministic, but requests can run concurrently for
different actors. Extension implementations must be `Send + Sync`.
Owner, transaction, and some lifecycle callbacks run while the actor slot is
held. Do not call back into that actor or create lock cycles from these hooks;
use an outbox or deferred work instead.

## Example: a small ACL extension

Add `async-trait` to your embedding application's dependencies. Until
publication, depend on this checkout's `crates/statex` by path.

```rust
use std::sync::Arc;
use async_trait::async_trait;
use statex::{HookError, InvocationContext, InvocationExtension};

struct Acl;

#[async_trait]
impl InvocationExtension for Acl {
    async fn before_execute(
        &self, context: &InvocationContext,
    ) -> Result<(), HookError> {
        let principal = context.request.principal.as_ref()
            .ok_or_else(|| HookError::Unauthorized("authentication required".into()))?;
        if principal.claims.get("can-invoke").and_then(|v| v.as_bool()) != Some(true) {
            return Err(HookError::Denied("actor access denied".into()));
        }
        Ok(())
    }
}

# async fn example() -> anyhow::Result<()> {
let mut config = statex::local_config("./data", "local")?;
config.extensions.push(Arc::new(Acl));
let node = statex::start(config).await?;
# node.shutdown().await;
# Ok(())
# }
```

The claim is only an example policy, not a framework convention. Your policy
can consult any ACL implementation and inspect target, operation, arguments,
caller, and principal. Decide explicitly how to authorize system alarms,
which have no originating user principal.

## Authentication and context

`InvocationContext.request` contains a request ID, parent request ID,
immediate `Caller`, optional `Principal` with opaque claims, an absolute
deadline, and serializable attributes. Admission can authenticate and enrich
principal/attributes. Target, operation, caller, IDs, and deadline are
immutable; changing them is an extension error.

Authentication can live in `admit`: HTTP admissions can retrieve
`extensions::HttpRequestInfo` from `context.data`, validate credentials using
their chosen provider, then set `context.request.principal`. Headers are
untrusted facts, **not** an authenticated identity. They remain local and
are not forwarded. Do not put credentials in propagation attributes.

Alternatively, use verified Axum middleware that inserts `Principal` into
the request's extension map. Configure it through `NodeConfig.public_router`
with a router-transform callback; it wraps only the public API, not the peer
API. The framework never accepts a principal from public JSON or an identity
header. Schema/list/health routes are not actor invocations; protect those
separately with middleware when required.

Trusted embedders can use `Node::invocation_context`,
`invoke_with_context`, or `invoke_with_metadata`. Normal `invoke` supplies
embedded context; alarm delivery supplies system context. Peer forwarding
includes metadata in the HMAC-signed body. All fleet nodes and the fleet
secret belong to the trusted host boundary; deploy equivalent policies on
every entry node and owner.

Nested calls get a new request ID and immediate actor caller, retain the
originating principal/attributes, and cannot extend the parent's deadline.
`context.data` is a shared, typed, per-invocation map. It is not durable,
transactional, or serialized to peers. Guest code cannot forge host metadata.

## Transactions, outcomes, and cleanup

`before_commit` receives immutable output and restricted `TransactionState`
SQL/alarm access, not a database handle or commit/rollback API. Hook writes
participate in the same transaction and recoverable change as guest writes.
A hook error or panic rolls back and discards the component instance.
Hook data and external effects are not rolled back; use transactional
outboxes or idempotency keys for external integrations.

`TransactionKind::Invocation` covers calls, create migrations, and successful
alarm handlers. `AlarmMaintenance` covers clearing a removed handler's alarm
and retry/backoff maintenance after guest failure. A handler-policy veto
preserves the prior alarm and returns rejection, rather than silently
consuming it or claiming a successful firing. A maintenance veto also rolls
back its changes.

Async critical hooks are bounded by `extension_timeout` and the remaining
invocation deadline. Leases are rechecked after owner hooks; all execution
outcomes, including read-only ones, receive an ownership/lease check before
acknowledgement. Cancelling a caller does not abandon a transaction between
commit, capture, and upload; the caller may still have an unknown outcome.
Synchronous transaction hooks must honor budgets: native code cannot be
forcibly interrupted. Deadlines are checked before and after each callback.

Completion observers run asynchronously with a separate bounded budget.
Failures/timeouts/panics are logged and do not rewrite a durable outcome.
Lifecycle events report activation, code replacement, eviction, deletion,
shutdown, fencing, and failure-driven discard. They describe local resident
transitions, not guarantees of a completed ownership release. Observers are
best-effort, not durable delivery; crashes, runtime shutdown, or cancellation
during admission can prevent notification. Use a transactional outbox when
event delivery must survive crashes.

`HookError` distinguishes unauthorized, denied, invalid, unavailable,
internal, and timeout failures. These surface as explicit HTTP errors and
`call-error::rejected` for actor callers; transport/ownership failures retain
their unknown-outcome `unavailable`/`timeout` semantics. Hooks do not provide
distributed exactly-once execution or deduplicate client/network retries.
