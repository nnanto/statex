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
gets an in-memory SQLite database with your migrations applied.

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
statex verify      # runs the [build] command, then checks the component
statex dev         # hot-reloads on .py / WIT / migration changes
```

The template contains:

```
statex.toml        includes [build] command = "componentize-py ... -o app.wasm", wasm = "app.wasm"
wit/app.wit        your interfaces; the world imports statex:host/{context,sql,http-client,log}
wit/deps/statex-host/statex-host.wit
app.py             one class per exported interface
statex.py          small helper over the host imports (vendored; edit freely)
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
- `Err`.

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

To inspect them, run `componentize-py -d wit -w app bindings out/`.

Notes:

- **Toolchain.** componentize-py needs Python ≥ 3.10. If `componentize-py` is not on `PATH` when you run `statex new`, the template invokes it via `uvx --python 3.12 --from componentize-py==0.25.1`, which fetches both.
- **Size and startup.** The component is about 18 MB because it includes CPython. Each node compiles it once per deployment, which takes a few seconds in release builds. After that, activating an actor takes tens of milliseconds. On a laptop with `statex dev`, a warm actor answers reads in about 2 ms and durable writes in about 8 ms.
- **Pure-Python dependencies only.** Packages are bundled from the build environment. Native extensions do not work unless they are built for WASI.
- **No mock host yet.** Python actors have no in-process mock host, so test them with `statex dev` and `statex call` or the generated client. A pytest suite can run against the dev server.
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

**Ownership.** The first deploy of an app claims its name for an owner, and
later deploys by any other owner are refused. The owner comes from:

1. `--owner` (or `STATEX_OWNER`);
2. `[app] owner` in statex.toml;
3. the namespace of a `team/app` name;
4. otherwise `default`.

`--take-over` transfers the app to the deploying owner. The claim is CAS'd
together with the version, so two teams racing on a first deploy cannot both
win.

**Compatibility.** A deploy is refused if it would break callers or actors of
the deployed version, unless you pass `--allow-breaking`. The following count
as breaking:

- removing an actor type or method;
- changing a method's parameter names or types, or its result (including any field, case or flag of a named type it uses);
- editing, removing or renaming an applied migration;
- adding a migration that sorts before an existing one.

Adding types, methods, named types and trailing migrations is always fine.
`statex dev` skips both checks. It does warn when an applied migration was
edited, removed or reordered, because dev actors created earlier keep their old
schema. Run `statex dev --clean` to start over.

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
management. Within statex, the deploy-time owner claim (`[app] owner`) only
stops one team from accidentally deploying over another team's app.

Rust apps share one Cargo workspace (`apps/Cargo.toml`, maintained by
`statex new`) and one `apps/target/`. It is separate from the platform's
workspace, so `cargo test` in an app directory only builds apps.

```
statex-workspace.toml        apps, host_wit, guest_sdk, python_guest, registry
apps/Cargo.toml              Cargo workspace for Rust apps (shared target/), maintained by `statex new`
apps/payments/shop/          statex.toml, wit/app.wit, migrations/, src/ or app.py
registry/index.json          generated: every app, its owner, types and method signatures
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
- cd apps/$TEAM/$APP && statex deploy --owner $TEAM
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
