# Actor-call routing

`statex_runtime::ActorCaller` connects typed guest client imports to an
embedding application's router. The node supplies its own caller by default:
calls use the same ownership, transaction, durability and forwarding path as
public invocations. Runtime-only instances without a caller return
`CallFailure::Unavailable`.

Implement `call(&self, CallRequest) -> CallReply` and pass an
`Arc<dyn ActorCaller>` to `AppCode::instantiate_with`, alongside a
backend-neutral `DatabaseHandle`. A request carries the target actor,
method, positional JSON arguments, executing actor chain and remaining
timeout budget.

Preserve the chain and reject re-entry; don't erase it when forwarding. Honor
the timeout without assuming that a timed-out callee rolled back. A custom
router that invokes a node should delegate to `Node::invoke` rather than
executing an actor outside its slot lock and acknowledgement protocol.

Return `CallReply::Ok` only for a successful callee result,
`CallReply::MethodErr` for the callee's business error, and
`CallReply::Failed` for routing/runtime failures. `Unavailable` and `Timeout`
mean **unknown outcome**, not proof the mutation did not happen. The runtime
validates reply values against the imported WIT signature.

Each callee has its own transaction; the caller cannot undo it. This interface
is appropriate for embedding, alternate routing layers and tests, not a
cross-actor transaction mechanism.
