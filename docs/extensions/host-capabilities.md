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
