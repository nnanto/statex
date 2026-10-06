# statex

statex is an extensible stateful actor framework for WebAssembly. Start with
SQLite, a local filesystem object store, and the standard WASI host; inject
your own implementations as your application grows.

You write a **actor type** as a WIT interface and implement it in a WASM
component. Each actor type can have any number of **actors**, one per key, for
example `counter("alice")`. Every actor has its own private transactional
state (SQLite by default).

- Any node can serve any call, because a node that does not own the actor forwards the call to the node that does.
- Ownership is coordinated with leases and epochs through the `ObjectStore`
  interface, using compare-and-swap writes. No cloud provider is required.
- A write is made durable in the object store before it is acknowledged.

```python
from counter_client import CounterApp

app = CounterApp("http://any-node:9876")
app.counter("alice").increment(by=1)   # -> 1, wherever alice lives
```

Actors can be written in Rust, or in Python via componentize-py (`statex new
--lang python`). Any toolchain that emits a WASI p2 component also works.

There is nothing to register: the WIT is the API. `POST
/v1/apps/<app>/actors/<type>/<key>/<method>` calls `<method>` on actor `<key>`
of `<type>`. The actor is created on first use.

Actors can also schedule an **alarm**: the host calls their `alarm` handler at
the given time, waking the actor on some node if it is not loaded (see the
[developer guide](docs/developer-guide.md#8-alarms)).

## Quick start

```sh
rustup target add wasm32-wasip2
cargo install --path crates/cli          # installs `statex`
export STATEX_GUEST_PATH="$PWD/crates/guest"  # SDK checkout for standalone Rust apps

mkdir my-projects && cd my-projects
statex new hello                         # standalone app; --lang python for Python
cd hello
cargo test                               # unit tests against an in-process mock host
statex dev                               # local node on :9876, rebuilds on change
statex call counter alice increment -a by=2
curl -X POST localhost:9876/v1/apps/hello/actors/counter/alice/get
statex codegen python                    # -> hello_client.py (typed, stdlib only)
statex check
```

When you are ready for a cluster:

```sh
export STATEX_STORE=/path/to/shared/store  # local development; inject a production store when embedding
statex diagnose                            # checks the store's conditional-write semantics
statex node --advertise http://10.0.0.5:9876 &   # on every node
statex deploy                              # from the project directory
```

Applications do not have to live in this repository or carry a team prefix.
Workspaces and registries are optional conveniences for multi-app projects;
namespaced apps remain supported. Incompatible deployments are refused.
See [Monorepo workspaces](docs/developer-guide.md#monorepo-workspaces).

## Embed and extend

Use the `statex` crate as a small entry point, or depend directly on the
individual crates. Until packages are published, use path dependencies to
this checkout.

```rust,no_run
# async fn example() -> anyhow::Result<()> {
let config = statex::local_config("./statex-data", "local")?;
let node = statex::start(config).await?;
// Publish a component with statex::deploy::deploy, or use the CLI.
println!("Actor endpoint: {}", node.url());
node.shutdown().await;
# Ok(())
# }
```

Extension contracts are small, explicit interfaces rather than a plugin
loader. The node retains serialization, durability-before-acknowledgement,
leases, epochs, and fencing; adapters supply the implementations.

| Boundary | Default | Guide |
|---|---|---|
| Shared coordination and durable objects | `LocalFsStore` (Azure adapter also included) | [Object stores](docs/extensions/object-stores.md) |
| Transactional actor state and recovery | SQLite with WAL/page-log replication | [State backends](docs/extensions/state-backends.md) |
| Admission, ACLs, commit policies, and lifecycle | Ordered no-op extension chain | [Invocation hooks](docs/extensions/invocation-hooks.md) |
| Runtime-only per-guest execution | Ordered no-op execution chain | [Runtime hooks](docs/extensions/runtime-hooks.md) |
| Additional guest-to-host capabilities | Standard `statex:host` WIT imports | [Host capabilities](docs/extensions/host-capabilities.md) |
| Calls between actors | Node ownership-aware routing | [Actor callers](docs/extensions/actor-callers.md) |
| Outbound HTTP and host logs | ureq and tracing | [Host services](docs/extensions/host-services.md) |
| Guest-side capability adapters | WASM imports; native mock host for tests | [Guest adapters](docs/extensions/guest-adapters.md) |
| Python client HTTP transport | Standard-library transport | [Client transports](docs/extensions/client-transports.md) |
| Application names and project layout | Standalone project with an explicit name | [Application names](docs/extensions/application-names.md) |

Third-party implementations must uphold their contract; replacing a backend
does not make a weakly consistent store safe or turn external side effects
into transactions. See [guarantees](docs/guarantees.md).

## Layout

| Path | What |
|---|---|
| `wit/statex-host.wit` | The host contract (`context`, `sql`, `http-client`, `log`, `actors`) |
| `crates/statex` | Library entry point, local defaults, and reexports |
| `crates/store` | `ObjectStore` trait with conditional writes; local-fs and Azure Blob backends |
| `crates/ltx` | WAL-frame capture, segment codec, restore planning |
| `crates/runtime` | wasmtime host: WIT/JSON mapping, limits, host imports, compatibility rules |
| `crates/node` | Leases, ownership, durability, routing, HTTP API |
| `crates/guest` | Rust guest SDK (`sql`, `http`, `log`, `context`, `testing`) |
| `crates/cli` | The `statex` CLI and node binary: scaffolding, workspaces, `check`, registry, Python codegen |
| `sdk/python` | Generic Python client runtime (also embedded in generated clients) |
| `examples/counter` | Rust: two actor types, `counter` and `account` (records, enums, variants, results) |
| `examples/caller` | Rust: typed calls to another app (`counter`) and its own actors, with native stubs |
| `examples/python-caller` | Python: typed calls to a Rust app and between its own actors, tested with `statex test` stubs |
| `examples/python-counter` | Python (componentize-py): `counter` and `profile` actor types |
| `sdk/python-guest/` | Python guest helper (`statex.py`) and mock host for tests (`statex_testing.py`); `statex new --lang python` vendors them into standalone projects and links them in workspaces |
| `apps/` | Sample multi-app workspace; optional for consumers |
| `registry/` | Generated index and WIT of every app (`statex registry build`) |
| `scripts/e2e.sh` | End-to-end run: forwarding, actor-to-actor calls, crash takeover, generated client, monorepo checks |

## Docs

- [Developer guide](docs/developer-guide.md): writing, testing and calling actors, including [from other actors](docs/developer-guide.md#7-call-other-actors)
- [Architecture](docs/architecture.md): routing, leases, storage layout and the request path
- [Guarantees](docs/guarantees.md): durability, fencing and failure semantics

## Testing

```sh
cargo test --workspace     # builds examples/counter to wasm on first run
scripts/e2e.sh
```
