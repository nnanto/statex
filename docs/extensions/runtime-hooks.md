# Runtime execution extensions

Applications embedding `statex-runtime` without a node can register ordered
`ExecutionExtension` implementations through
`RuntimeBuilder::execution_extension(Arc<dyn ExecutionExtension>)`.
The empty chain is the default. This interface is independent of the
[node invocation pipeline](invocation-hooks.md).

Implement `before_guest(&InvocationContext)` to validate, authorize, trace,
or reject each guest execution. `HookError` becomes `CallError::Rejected`;
the guest is not called. `after_guest` observes an immutable guest result,
including failures. Observer errors/panics are logged and do not change it.
This result is **not** a durability acknowledgement: runtime-only embedders
own transaction control and persistence.

```rust
use std::sync::Arc;
use statex::runtime::RuntimeBuilder;
use statex::{ExecutionExtension, HookError, InvocationContext};

struct RequirePrincipal;
impl ExecutionExtension for RequirePrincipal {
    fn before_guest(&self, context: &InvocationContext) -> Result<(), HookError> {
        if context.request.principal.is_none() {
            return Err(HookError::Unauthorized("authentication required".into()));
        }
        Ok(())
    }
}

# fn example() -> anyhow::Result<()> {
let runtime = RuntimeBuilder::default()
    .execution_extension(Arc::new(RequirePrincipal))
    .build()?;
# Ok(())
# }
```

Ordinary instance call helpers construct embedded context. Supply verified
context through `call_with_context` or `call_alarm_with_context` when your
embedding owns authentication. A principal is trusted host metadata, never
automatically extracted from guest arguments.

`HostState::invocation_context()` exposes the current context to registered
host capabilities. It is installed only for the invocation and cleared on
all return paths; reused instances cannot retain the previous caller.
Outgoing actor calls carry bounded child metadata and preserve the principal
without allowing the guest to select a new identity.

Hooks are synchronous trusted native code, not sandboxed plugins. Honor
`context.remaining()` and bound any I/O. WASM epoch interruption cannot stop
native hooks; deadlines are checked before/after callbacks. Rejections and
panics are explicit failures. Actor-local extension memory and external
effects are not transactional; node-owned rollback/discard guarantees require
the node execution path.
