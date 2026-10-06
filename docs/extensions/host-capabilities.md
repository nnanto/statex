# Host capabilities

`Runtime::new()` supplies the standard SQL, context, alarms, HTTP, logging,
actor-call and WASI interfaces. Use `Runtime::builder()` to add capabilities
without forking the executor:

```rust,ignore
let runtime = statex_runtime::Runtime::builder()
    .host_interface("example:metrics/counter@0.1.0", |linker| {
        linker.instance("example:metrics/counter@0.1.0")?
            .func_new("record", |store, _ty, _params, _results| {
                tracing::info!(actor = %store.data().identity.key, "recorded");
                Ok(())
            })?;
        Ok(())
    })?
    .initialize_host(|host| {
        host.extensions.insert(MyActorLocalState::default());
        Ok(())
    })
    .build()?;
```

Generate bindings from your own WIT with Wasmtime's `component::bindgen!` for
typed implementations; the dynamic callback above is useful for small
interfaces. The guest imports that interface using its language's WIT
toolchain. All nodes that can activate the component must register compatible
implementations.

Resource-based host interfaces can use `HostState::resource_table()` to
manage Wasmtime component resources. Resource ownership and cleanup still
belong to the interface implementation; actor-local extension data must not
retain borrowed handles across incompatible instances.

`runtime.build_manifest(...)` admits its registered imports in addition to
standard capabilities and typed actor calls. `Manifest::build(...)` remains
restricted to the defaults. The linker rejects missing or mismatched
implementations; registration and initialization errors propagate. Set
`NodeConfig.runtime = Some(runtime)` when embedding a node.

Names are exact, including versions. Duplicate registrations and replacement
of built-ins are rejected. Linker callbacks are trusted native code, not
sandboxed plugins. Do not grant arbitrary filesystem, network, or environment
access merely because a guest asks for it.

`Extensions` is a typed actor-local map with `insert`, `get`, and `get_mut`.
It is recreated after traps, eviction, or code changes, and is **not**
transactional or durable. Store recoverable state through the actor database.
Host side effects cannot be undone by rolling back an actor transaction.
Engine tuning uses `configure_engine`; epoch interruption remains enabled.

The standard `statex:host/actors` interface also supplies durable scheduling.
Its `spawn` import accepts an app, actor type, key, method, positional JSON
arguments, and a delay in milliseconds; it returns an outbox job ID.
`job` inspects that ID in the current actor's outbox. Guest SDKs expose
`spawn` and `spawn_after` over the same scheduling import.
The SQLite outbox is host-owned: guest SQL and migrations cannot read or
modify its bookkeeping. Use `job` for inspection instead of querying the
internal table.

Scheduling requires an executing actor transaction, a supporting state
backend, and a node actor caller. It is unavailable during component
initialization and in runtime-only embeddings without actor routing.
Enqueue errors are explicit `call-error` values. A successful enqueue is
transactional state, not an immediate external effect: rollback removes it,
and the node dispatches only after durability. See
[background calls](../developer-guide.md#durable-background-calls).
