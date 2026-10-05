# statex

statex is a stateful execution engine for WebAssembly.

You write a **actor type** as a WIT interface and implement it in a WASM
component. Each actor type can have any number of **actors**, one per key, for
example `counter("alice")`. Every actor has its own private SQLite database.

- Any node can serve any call, because a node that does not own the actor forwards the call to the node that does.
- Ownership is coordinated with leases and epochs stored in an object store (Azure Blob/ADLS, or a local directory), using compare-and-swap writes.
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

# apps live in this repo under apps/<team>/<app>
statex new demo/hello                    # -> apps/demo/hello, with a `counter` actor type (--lang python for Python)
cd apps/demo/hello
cargo test                               # unit tests against an in-process mock host
statex dev                               # local node on :9876, rebuilds on change
statex call counter alice increment -a by=2
curl -X POST localhost:9876/v1/apps/demo/hello/actors/counter/alice/get
statex codegen python                    # -> demo_hello_client.py (typed, stdlib only)
statex check --all && statex registry build   # before committing
```

When you are ready for a cluster:

```sh
export STATEX_STORE=az://statex            # or a local path shared by the nodes
statex diagnose                            # checks the store's conditional-write semantics
statex node --advertise http://10.0.0.5:9876 &   # on every node
statex deploy                              # from the project directory
```

Teams add their apps to this repo. `statex new team/app` creates
`apps/team/app`, linked to the host WIT and SDKs in this repo.
`statex check --all --against origin/main` and `statex registry build --check`
gate PRs (name/path consistency and breaking changes). Incompatible
deploys are refused.
See [Monorepo workspaces](docs/developer-guide.md#monorepo-workspaces).

## Layout

| Path | What |
|---|---|
| `wit/statex-host.wit` | The host contract (`context`, `sql`, `http-client`, `log`, `actors`) |
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
| `apps/<team>/<app>` | Teams' apps; layout configured in `statex-workspace.toml` |
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
