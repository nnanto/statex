# Developer guide

## 1. Create a project

Apps live in this repository under `apps/<team>/<app>` (see
[Apps in this repository](#apps-in-this-repository)). From the repo root:

```sh
statex new payments/shop --actor cart     # Rust app with one actor type named `cart`
cd apps/payments/shop
```

For Python, add `--lang python`; see [Writing actors in Python](#writing-actors-in-python).
The examples below use the short name `shop` for brevity. In this repo, the app
is `payments/shop` and its URLs are `/v1/apps/payments/shop/...`.

```
apps/payments/shop/
  statex.toml                 app name, outbound HTTP allowlist, limits
  wit/app.wit                 your API: one interface per actor type
  migrations/cart/0001_init.sql
  src/lib.rs                  implementation
```

`statex add-actor <name>` adds another actor type. It creates the WIT interface,
adds the `export` to the world, creates a migration and adds a Rust stub.

## 2. Describe the API in WIT

```wit
package local:shop@0.1.0;

interface cart {
  record item { sku: string, qty: u32 }
  variant cart-error { out-of-stock(string), empty }

  add: func(sku: string, qty: u32) -> result<u32, cart-error>;
  items: func() -> list<item>;
  checkout: func(idempotency-key: string) -> result<string, cart-error>;
}

world app {
  export cart;
}
```

That is the whole contract:

- Each exported interface is an actor type.
- Each function is a method.
- There are no routes, handlers or registrations; the HTTP API and the client SDK are derived from the WIT.

Supported types are every WIT value type: primitives, `string`, `list`, `option`, `result`,
`tuple`, `record`, `variant`, `enum` and `flags`. Resources are not supported. The JSON
encoding is:

| WIT | JSON |
|---|---|
| integers, floats, bool, string, char | the same |
| `list<u8>` | base64 string |
| `option<T>` | `null` or the value |
| `record` | object keyed by the WIT field names (kebab-case) |
| `enum` | string |
| `flags` | array of strings |
| `variant` | `{"tag": "case", "value": ...}` (a bare `"case"` is accepted when there is no payload) |
| `tuple` | array |
| nested `result` | `{"ok": ...}` / `{"err": ...}` |
| top-level `result<T, E>` | `T` on success; `E` as `error.detail` in a 422 |

Arguments may be passed as an object (kebab-case, snake_case and camelCase are
all accepted) or as a positional array. Omitted `option` parameters default to
`none`.

## 3. Implement it

```rust
use statex_guest::{params, sql};

wit_bindgen::generate!({ path: "wit", world: "app", additional_derives: [PartialEq] });
use exports::local::shop::cart::{self, CartError, Item};

struct App;

impl cart::Guest for App {
    fn add(sku: String, qty: u32) -> Result<u32, CartError> {
        sql::execute(
            "INSERT INTO items (sku, qty) VALUES (?1, ?2)
             ON CONFLICT(sku) DO UPDATE SET qty = qty + excluded.qty",
            params![sku, qty],
        ).unwrap();
        Ok(sql::query_scalar("SELECT SUM(qty) FROM items", &[]).unwrap().unwrap_or(0))
    }
    // ...
}

export!(App);
```

Points to keep in mind:

- **One database per actor.** `cart("alice")` and `cart("bob")` never see each other's tables. Store the actor's state in ordinary tables; there is no need for a key column.
- **One transaction per call.** The host runs `BEGIN IMMEDIATE` before your method and commits after it returns.
  - Returning `Err(..)` for a `result` method, or panicking, rolls the transaction back.
  - Do not issue `BEGIN`, `COMMIT` or `PRAGMA` yourself.
- **Calls to one actor are serialized**, so there are no concurrency bugs inside an actor.
- **Host APIs** (`statex_guest`):
  - `sql::{execute, query, query_as, query_row, query_scalar}` with the `params![]` macro
  - `http::send`, limited to the hosts listed in `[http] allow`
  - `log::{debug, info, warn, error}`
  - `context::{app, actor_type, key, epoch}`
- **Migrations:** `migrations/<type>/NNNN_name.sql` files run in file-name order inside the first transaction that touches an actor after activation or a deploy. Never edit an applied migration; add a new one.
- **Idempotency:** a `503` means the outcome is unknown (see [guarantees](guarantees.md)). Methods with external effects should accept an idempotency key and record it.

## 4. Test locally

**Unit tests.** `statex_guest` ships with a mock host. Each `(type, key)`
gets an in-memory SQLite database with your migrations applied. Run the tests with `cargo test` or `statex test`.
Python actors have an equivalent harness; see [Testing Python actors](#testing-python-actors).

- `testing::call` runs one invocation in its own transaction and rolls it back on panic.
- `testing::try_call` additionally rolls back when the method returns `Err`.

Between them, they behave exactly as the server does.

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use cart::Guest as _;
    use statex_guest::testing::{call, try_call};

    #[test]
    fn add_items() {
        assert_eq!(try_call("cart", "alice", || App::add("apple".into(), 2)), Ok(2));
        assert_eq!(call("cart", "bob", App::items).len(), 0);
    }
}
```

Run them with `cargo test`. They run natively, so no WASM toolchain is needed.
`testing::mock_http` stubs outbound HTTP, and `testing::reactivate` simulates a
failover by bumping the epoch.

**Verify the component.**

```sh
statex verify
```

This builds `wasm32-wasip2`, then checks:

- that only allowed host imports are used,
- that the exports match the WIT,
- that the migration directories match the actor types,
- that every actor type instantiates against a migrated database.

It then prints the API.

**Run a dev node.**

```sh
statex dev            # :9876, state in .statex/dev (use --clean to reset)
statex call cart alice add '{"sku": "apple", "qty": 2}'
statex call cart alice add -a sku=pear -a qty=1
statex call cart alice items
statex actors
statex delete cart alice
```

`statex dev` runs the real node (with the same leases, durability and code
paths) against a local-directory bucket. It rebuilds and hot-reloads when
`src/`, `wit/`, `migrations/`, `Cargo.toml` or `statex.toml` change.
Resident actors keep their data across reloads, and new migrations are applied
on the next call. If a build fails, the previous version keeps serving.

## Writing actors in Python

Python actors are compiled to a WebAssembly component with
[componentize-py](https://github.com/bytecodealliance/componentize-py). The
component embeds CPython, so the host, routing, durability and generated
clients are exactly the same as for Rust actors.

```sh
statex new shop --lang python --actor cart
cd shop
statex test        # pytest against an in-process mock host
statex verify      # runs the [build] command, then checks the component
statex dev         # hot-reloads on .py / WIT / migration changes
```

The template contains:

```
statex.toml        includes [build] command = "componentize-py ... -o app.wasm", wasm = "app.wasm"
wit/app.wit        your interfaces; the world imports statex:host/{context,sql,http-client,log,alarms}
wit/deps/statex-host/statex-host.wit
app.py             one class per exported interface
statex.py          small helper over the host imports (vendored; edit freely)
statex_testing.py  the mock host for tests (vendored)
test_app.py        pytest tests; conftest.py resets the mock host before each test
pyproject.toml     points editors and type checkers at .statex/bindings
migrations/<type>/0001_init.sql
```

```python
from wit_world import exports
from wit_world.exports.cart import CartError_OutOfStock
import statex

class Cart(exports.Cart):                       # interface `cart` -> class Cart
    def add(self, sku: str, qty: int) -> int:   # add: func(sku: string, qty: u32) -> result<u32, cart-error>
        if sku == "unobtainium":
            raise statex.Err(CartError_OutOfStock(sku))   # err case: rolled back, HTTP 422
        statex.execute("INSERT INTO items (sku, qty) VALUES (?1, ?2) "
                       "ON CONFLICT(sku) DO UPDATE SET qty = qty + excluded.qty", sku, qty)
        return statex.query_scalar("SELECT SUM(qty) FROM items")
```

The helper provides:

- SQL: `execute`, `query` (returns tuples), `query_dicts`, `query_one`, `query_scalar`.
- `key()`, `app()`, `actor_type()`, `epoch()`.
- `http(method, url, body, headers)` and `info`, `warn`, `error`, `debug`.
- `Err`, `Ok` and `Some`.
- `describe(call_error)` and `outcome_unknown(call_error)` for [calls to other actors](#7-call-other-actors).

Python's `None`, `bool`, `int`, `float`, `str` and `bytes` map to SQL values.

Semantics match Rust actors:

| What happens | Effect |
|---|---|
| Normal return | Commits |
| `raise statex.Err(e)` in a `result` method | Rolls back; the client gets `err(e)` as HTTP 422 |
| Any other exception | Traps: rolls back, HTTP 500; the traceback goes to the node log |

The generated WIT bindings follow componentize-py's conventions:

- Records and variant cases are dataclasses, such as `CartError_OutOfStock`.
- `option<T>` is `Optional[T]`.

**Typed bindings.** `statex new`, `build`, `verify`, `dev`, `test` and `calls sync`
generate them into `.statex/bindings/wit_world` whenever the WIT changes. The
scaffolded `pyproject.toml` adds that directory to pyright's and mypy's paths,
so editors complete and type-check `wit_world` imports, including the clients
of other actors.

### Testing Python actors

`statex test` refreshes the bindings and runs pytest (through `uvx --python 3.12`
when the build uses uvx) with the bindings and the guest helpers on `PYTHONPATH`.
`statex_testing` stands in for the host. Each `(actor type, key)` gets its own
in-memory SQLite database with `migrations/<type>/*.sql` applied, and each
`call` is one transaction:

```python
import pytest
import statex
from statex_testing import call
from app import Cart

def test_add():
    assert call("cart", "alice", Cart().add, "apple", 2) == 2   # commits
    with pytest.raises(statex.Err):                                # rolls back
        call("cart", "alice", Cart().add, "unobtainium", 1)
    assert call("cart", "bob", Cart().add, "pear", 1) == 1         # another actor
```

The harness also provides:

- `stub(client, impl)` answers [calls to other actors](#7-call-other-actors).
- `mock_http(handler)` answers outbound HTTP. The handler returns `(status, headers, body)`.
- `logs()` returns the logged messages.
- `reactivate(type, key)` bumps the actor's epoch.
- `alarm(type, key)` returns the actor's committed [alarm](#8-alarms) time, or `None`.
- `set_app(name)` changes the app name.
- `reset()` clears everything. `conftest.py` calls it before every test through `pytest_plugins = ["statex_testing"]`.

Like the host, the harness rejects transaction-control statements such as
`BEGIN` and `PRAGMA`.

Notes:

- **Toolchain.** componentize-py needs Python ≥ 3.10. If `componentize-py` is not on `PATH` when you run `statex new`, the template invokes it via `uvx --python 3.12 --from componentize-py==0.25.1`, which fetches both.
- **Size and startup.** The component is about 18 MB because it includes CPython. Each node compiles it once per deployment, which takes a few seconds in release builds. After that, activating an actor takes tens of milliseconds. On a laptop with `statex dev`, a warm actor answers reads in about 2 ms and durable writes in about 8 ms.
- **Pure-Python dependencies only.** Packages are bundled from the build environment. Native extensions do not work unless they are built for WASI.
- **Other toolchains.** Any language whose toolchain emits a WASI p2 component works through `[build] command` and `[build] wasm`, for example JavaScript (`jco componentize`) or Go (TinyGo). `statex verify` reports anything the host does not support.

## 5. Generate a client

```sh
statex codegen python                     # from the local project
statex codegen python --from-url http://node:9876 --app shop -o shop_client.py
```

The output is one self-contained module (standard library only, Python 3.9+):

```python
from shop_client import ShopApp, CartError, MethodError, Unavailable

shop = ShopApp("http://any-node:9876")
alice = shop.cart("alice")           # a handle; makes no network call
alice.add(sku="apple", qty=2)        # -> 2
for item in alice.items():           # -> [Item(sku='apple', qty=2)]
    print(item.sku, item.qty)

try:
    alice.checkout(idempotency_key="order-17")
except MethodError as e:             # the method returned err(CartError)
    if e.error == CartError.empty():
        ...
```

The generated module provides:

- dataclasses for records, `str` enums for enums, and classes for variants (`.tag` and `.value`, with constructors such as `CartError.out_of_stock("apple")`),
- typed method signatures with docstrings taken from the WIT,
- `create()` and `delete()` on every actor handle,
- automatic retry with backoff on `503`.

Errors are raised as `NotFound`, `BadRequest`, `Conflict`, `Unavailable`
and `MethodError`, all subclasses of `StatexError`.

Without codegen, `sdk/python/statex_client` offers the same calls untyped:
`App("shop").actor("cart", "alice").add(sku="apple", qty=2)`.

## 6. Deploy

```sh
statex deploy --store az://statex          # or STATEX_STORE=...
```

Every node picks up the new version within about 2 seconds. Deploys are atomic
(CAS on `deploy/<app>/current.json`) and content-addressed, so re-deploying
identical bits is a no-op.

**Who may deploy.** statex does not track app owners. Anyone who can write
`deploy/` in the store can deploy any app, so grant that access only to CI.
In this repository, `team/app` names tied to `apps/<team>/<app>` paths keep
teams' apps apart.

**Compatibility.** A deploy is refused if it would break callers or actors of
the deployed version, unless you pass `--allow-breaking`. The following count
as breaking:

- removing an actor type or method;
- changing a method's parameter names or types, or its result (including any field, case or flag of a named type it uses);
- editing, removing or renaming an applied migration;
- adding a migration that sorts before an existing one.

Adding types, methods, named types and trailing migrations is always fine.
`statex dev` skips this check. It does warn when an applied migration was
edited, removed or reordered, because dev actors created earlier keep their old
schema. Run `statex dev --clean` to start over.

## 7. Call other actors

An actor can call any actor of any deployed app, including its own app, through
a typed client. You import the callee's actor type as a WIT interface, and the
host routes each call to the node that owns that actor. Callers and callees can
be written in different languages.

**1. List the apps you call** in `statex.toml`, then generate their clients:

```toml
[calls]
apps = ["counter", "demo/db"]
# Optional: where to find callee source outside a workspace.
paths = { counter = "../counter" }
```

```sh
statex calls add demo/db          # adds it to [calls] apps and syncs
statex calls sync                 # regenerate from local callee source
statex calls sync --from-url http://node:9876   # or from deployed apps
```

`sync` writes one `wit/deps/<ns>-<name>/client.wit` per callee app. In Rust
projects it also writes `src/statex_calls.rs`. The package is `team:app`, or
`statex:app` for single-segment names. Each actor type becomes an interface
whose functions take the actor key first and return `result<T, call-error>`:

```wit
interface counter {
  use statex:host/actors@0.1.0.{call-error};
  increment: func(actor: string, by: s64) -> result<s64, call-error>;
}
```

Callee source comes from this app (for self-calls), then `[calls] paths`, then
the workspace app directory. Generated files carry a "Generated by `statex calls
sync`" header and are refreshed automatically by `build`, `verify`, `deploy`
and `dev`. Only clients of remote apps (fetched with `--from-url`) are kept
until you sync again.

**2. Import the interfaces** in your world. `statex calls sync` prints the lines
to add:

```wit
world app {
  import statex:counter/counter;
  export relay;
}
```

**3. Call them.** In Rust, map the imports to the generated shim. `sync` prints
these `with:` entries too:

```rust
use statex_guest::CallError;
mod statex_calls;

wit_bindgen::generate!({
    path: "wit",
    world: "app",
    with: {
        "statex:host/actors@0.1.0": statex_guest::actors,
        "statex:counter/counter": crate::statex_calls::statex::counter::counter,
    },
});
use statex_calls::statex::counter::counter;

fn bump(key: &str) -> Result<i64, CallError> {
    counter::increment(key, 1)
}
```

When the callee's method returns `result<T, E>`, the call returns
`Result<Result<T, E>, CallError>`. The outer `Err` means the call failed, and
the inner `Err` is the callee's own error, typed with the callee's `E`:

| `CallError` | Meaning |
|---|---|
| `NotFound` | Unknown app, actor type or method |
| `Incompatible` | The deployed callee no longer matches the signature you were built against |
| `Trap` | The callee trapped; its transaction was rolled back |
| `Unavailable` | The callee could not be reached or its write could not be confirmed. **The outcome is unknown** |
| `Cycle` | The call would re-enter an actor already on the call chain, or the chain is deeper than 16 |
| `Timeout` | The caller's deadline ran out. **The outcome is unknown** |

**In Python**, each imported actor type is a typed module under
`wit_world.imports`. Its functions take the actor key first. A failed call
raises `statex.Err(call_error)`, where `call_error` is a dataclass such as
`CallError_Unavailable("...")` from `wit_world.imports.actors`. When the
callee's method returns `result<T, E>`, you get back `statex.Ok(t)` or
`statex.Err(e)` instead of an exception:

```python
from wit_world.imports import account, counter
from wit_world.imports.account import TxError_InvalidAmount
import statex

class Relay(exports.Relay):
    def bump(self, key: str, by: int) -> int:
        try:
            return counter.increment(key, by)
        except statex.Err as e:                      # the call failed
            if statex.outcome_unknown(e.value):      # unavailable / timeout
                ...
            raise statex.Err(statex.describe(e.value))   # e.g. "unavailable: no owner"

    def deposit(self, key: str, amount: int) -> int:
        match account.deposit(key, amount, None):
            case statex.Ok(balance):
                return balance
            case statex.Err(TxError_InvalidAmount()):
                raise statex.Err("refused")
```

Import the module (`from wit_world.imports import counter`) rather than its
functions, so that tests can stub it. If two imported interfaces have the same
name, as `relay` does in `examples/python-caller`, import one under an alias.

**Test with stubs.** Natively, the shim swaps each client for a stub that
implements the interface's trait, so `cargo test` needs no node:

```rust
struct Fake;
impl counter::Counter for Fake {
    fn increment(&self, actor: &str, by: i64) -> Result<i64, CallError> {
        if actor == "down" { Err(CallError::Unavailable("no owner".into())) } else { Ok(by) }
    }
}

#[test]
fn bumps() {
    counter::stub(Fake);   // per thread; counter::clear_stub() removes it
    assert_eq!(call("relay", "r", || App::bump("c".into(), 2)), Ok(2));
}
```

Calling a method that the stub does not override panics with "was called without a
stub". See `examples/caller` and `apps/demo/rustdemo`.

In Python, `statex_testing.stub(module, impl)` answers calls through a client
module with the same-named methods of `impl`, which take the actor key first.
Until a module is stubbed, calls through it fail with "called without a stub".
A stub can return what the real callee would, raise
`statex.Err(CallError_Unavailable(...))` to simulate a failed call, or run
another actor of your own app with `call(...)`. Each actor gets its own
transaction, and re-entering an actor that is already running raises
`CallError_Cycle` as on a node:

```python
from statex_testing import call, stub
from wit_world.imports import counter
from wit_world.imports.actors import CallError_Unavailable

class FakeCounter:
    def increment(self, actor, by):
        if actor == "down":
            raise statex.Err(CallError_Unavailable("no owner"))
        return by

def test_bump():
    stub(counter, FakeCounter())
    assert call("relay", "r", Relay().bump, "c", 2) == 2
```

See `examples/python-caller`.

**Checks and deploys.**

- `statex check` fails when an imported app is not in `[calls] apps`, when an
  import no longer matches the callee's local source, or when generated files
  are stale. It only warns when the callee's source is not available locally.
- `statex deploy` refuses a caller whose imports do not match the deployed
  callees, unless you pass `--allow-unresolved-calls` (for example, to deploy a
  caller before its callee).
- Deploying a callee is refused if it breaks the imports of a deployed caller,
  unless you pass `--allow-breaking`.
- `statex dev` also deploys callees whose source is local, and only warns about
  unresolved calls.

**Semantics.**

- A call is not part of the caller's transaction. The callee commits on its own,
  even if the caller later rolls back. Use idempotency keys for steps that must
  not be repeated.
- The caller's actor stays locked while it waits. A call back into any actor on
  the chain is rejected with `Cycle` rather than deadlocking.
- Time spent in the callee counts against the caller's `timeout_ms`. A call
  gets the caller's remaining time minus a small reserve, and the callee is also
  bounded by its own `timeout_ms`.
- The names `statex` and `wasi` (as teams) and `host` (as an app) are reserved,
  because their WIT packages belong to the host.

## 8. Alarms

Each actor can have at most one **alarm**: a point in time at which the host
calls the actor's `alarm` handler, even if no request arrives and the actor is
not loaded on any node. Use it for timeouts, reminders, retries, TTL cleanup or
periodic work (the handler sets the next alarm).

Declare the handler as a function named `alarm` in the actor type's interface.
It takes either nothing or the retry count, and returns nothing or a `result`:

```wit
interface counter {
  schedule: func(delay-ms: u64);
  /// Runs when the alarm is due. Not callable by clients.
  alarm: func(retry-count: u32) -> result<_, string>;
}
```

`alarm` is not a public method. It is left out of the schema, generated
clients and compatibility checks, and calling it over HTTP returns 404.

Set, read and clear the alarm from any method (including the handler itself):

```rust
use statex_guest::alarm;

fn schedule(delay_ms: u64) {
    alarm::set_in(Duration::from_millis(delay_ms)).unwrap(); // or alarm::set(unix_ms)
}
fn alarm(retry_count: u32) -> Result<(), String> {
    // ... do the work; alarm::set_in(...) here to make it periodic
    Ok(())
}
```

```python
import statex

class Counter(exports.Counter):
    def schedule(self, delay_ms: int) -> None:
        statex.set_alarm_in(delay_ms)        # or set_alarm(unix_ms)
    def alarm(self, retry_count: int) -> None:
        ...
```

`get()` / `statex.get_alarm()` return the scheduled time in Unix milliseconds,
and `clear()` / `statex.clear_alarm()` cancel it. Setting an alarm replaces the
previous one. Python worlds must `import statex:host/alarms@0.1.0;` (new
projects do). Setting an alarm on an actor type without an `alarm` handler fails.

**Semantics.**

- Alarm changes are part of the call's transaction: they take effect only if
  the call commits, and are durable once it is acknowledged.
- The handler runs in its own transaction. The alarm is cleared before it runs,
  so a handler that sets a new alarm keeps it.
- If the handler fails (returns an error or traps), its changes roll back and
  it is retried with backoff (2 s, 4 s, 8 s, ...), passing the retry count. After
  6 failed retries the alarm is dropped and an error is logged.
- Delivery is **at least once**: a handler can run again after a crash, so make
  it idempotent. It may run late: a resident actor fires on time, while an
  actor that is not loaded is woken within about a second (`wake_tick`), or
  within the full-scan interval (30 s) after the waker node fails over.
- Deleting an actor removes its alarm.

**Testing.** Tests call the handler directly, like any other method.
`statex_guest::testing::alarm(type, key)` and `statex_testing.alarm(type, key)`
return the committed alarm time of an actor:

```rust
call("counter", "t", || App::schedule(60_000));
assert!(statex_guest::testing::alarm("counter", "t").is_some());
assert_eq!(try_call("counter", "t", || App::alarm(0)), Ok(()));
```

See `examples/counter` and `examples/python-counter`.

## Apps in this repository

Teams add their apps to this repository, which is already a statex workspace
(`statex-workspace.toml` at the root). Each app lives at `apps/<team>/<app>`
and is named `team/app`, so names cannot collide and the path shows who owns
it. Apps link the host WIT (`wit/`) and guest SDKs (`crates/guest`,
`sdk/python-guest`) from this repo rather than copying them. Every app therefore
builds against the exact host contract the nodes in this repo implement, and a
host change that breaks an app fails CI here.

To add an app:

1. From the repo root, run `statex new payments/shop [--lang python]`.
2. Implement it, then run `statex check --all` and `statex registry build`, and commit the app together with `registry/`.

Who may change an app's directory will be governed by LinkedIn ACL
management.

Rust apps share one Cargo workspace (`apps/Cargo.toml`, maintained by
`statex new`) and one `apps/target/`. It is separate from the platform's
workspace, so `cargo test` in an app directory only builds apps.

```
statex-workspace.toml        apps, host_wit, guest_sdk, python_guest, registry
apps/Cargo.toml              Cargo workspace for Rust apps (shared target/), maintained by `statex new`
apps/payments/shop/          statex.toml, wit/app.wit, migrations/, src/ or app.py
registry/index.json          generated: every app, its types and method signatures
registry/payments/shop.wit   generated copy of each app's WIT, for consumers to browse or import
```

```sh
statex new payments/shop                   # -> apps/payments/shop, added to apps/Cargo.toml
statex new growth/notes --lang python      # wit/deps/statex-host links to the shared host WIT
statex check --all --against origin/main   # what CI runs on every PR
statex registry build                      # commit the result with the change
```

`statex workspace init --statex <checkout>` sets up the same layout in another
repository, should one ever need its own. Paths in `statex-workspace.toml` are
relative to the repo root, or absolute when the statex checkout lives outside
that repo.

For each app, `statex check` verifies that:

- the name is valid, is `team/app`, and matches the app's path;
- the WIT parses, and every `migrations/<type>/` directory matches an exported actor type;
- with `--against <rev>`, nothing changed incompatibly since that git revision. It uses the same rules as deploy and needs no build.

`--allow-breaking` downgrades breaking changes to warnings, so use it for an
intentional, coordinated change.

`statex registry build --check` fails when the committed registry is stale.

A typical pipeline:

```yaml
# on pull request
- statex check --all --against origin/${BASE_BRANCH}
- statex registry build --check
# on merge to main, for each changed app (only CI holds write access to deploy/)
- cd apps/$TEAM/$APP && statex deploy
```

Namespaced apps are addressed with the slash kept in the URL:
`POST /v1/apps/payments/shop/actors/cart/alice/add`. `statex call` works the
same way from the app directory. The generated client class is
`PaymentsShopApp`.

## Running nodes

```sh
export STATEX_STORE=az://statex AZURE_STORAGE_ACCOUNT_NAME=mystorage
statex node --listen 0.0.0.0:9876 --advertise http://$POD_IP:9876 --node-id $POD_NAME --data-dir /var/lib/statex
```

Azure credentials, in order of precedence:

1. `AZURE_STORAGE_KEY`
2. workload identity (`AZURE_FEDERATED_TOKEN_FILE`, `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`)
3. managed identity

`AZURE_STORAGE_ENDPOINT` points the client at Azurite. Run `statex diagnose`
once per bucket.

Operational notes:

- **Node ids must be stable.** A restarted node waits out its own previous lease, then takes over its actors.
- **`--advertise`** must be reachable from the other nodes. `--internal-listen` can put node-to-node traffic on a separate port.
- **Nodes are only fenced on lease loss.** A fenced node exits with code 3; let your supervisor restart it.
- **Local disk is a cache.** Losing it never loses acknowledged data.
