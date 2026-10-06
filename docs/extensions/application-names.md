# Application names and source discovery

Application identity is `[app] name` in `statex.toml`, not its directory,
Cargo package, exported WIT package, repository, or team. A standalone `shop`
is the normal starting point. `payments/shop` is an optional namespace
convention.

## Grammar

Runtime app names contain one segment (`shop`) or two segments separated by
one slash (`payments/shop`). Each segment is 1–63 ASCII characters, starts
with a lowercase letter, and contains lowercase alphanumeric words separated
by single dashes. Each word starts with a lowercase letter. The same
WIT-compatible grammar is enforced by runtime manifests, scaffolding and
typed actor-call generation. Empty words or segments, more than one slash,
uppercase, dots, underscores, percent escapes and traversal components are
rejected. Use `shop-v2`, not `shop-2`, `shop--v2` or `shop-`.

The HTTP route delimiters `actors` and `schema` are reserved in either
segment. Namespace `statex` and namespace `wasi` are reserved for host WIT
packages. The standalone name `host` is reserved because `statex:host` is the
host package; `payments/host` is allowed.

WIT keywords such as `type` are allowed and escaped as `%type` in generated WIT.
Rust scaffolding prefixes keyword export package names (for example,
`local:app-type` for app `type`) to avoid a wit-bindgen package-module
limitation. This local export package is implementation metadata, not an alias
for application identity. That limitation also prevents Rust callers from generating
clients when the callee's client-package namespace or name is a Rust keyword;
the CLI reports it explicitly. Python callers and runtime app identity do not
have that restriction.

## Typed actor-call mapping

| App identity | Client WIT package | Rust shim module prefix |
|---|---|---|
| `shop` | `statex:shop` | `statex_calls::statex::shop` |
| `payments/shop` | `payments:shop` | `statex_calls::payments::shop` |

The host derives the callee identity from the imported client package:
`statex:shop/counter` calls app `shop`, and `payments:shop/cart` calls app
`payments/shop`. `statex:host/*` and `wasi:*` are host imports.
Exported application WIT packages, such as `local:shop`, need not
match the deployment name.

Non-actor host capabilities require explicit runtime registration.
`Runtime::build_manifest` admits the runtime's exact registered import names
(including versions) without treating them as actor-call clients.
`Manifest::build`, used by the default CLI, retains built-in and typed-client
validation; there is no wildcard or automatic permission for unrelated WIT
packages.

`statex calls sync` generates `wit/deps/<namespace>--<app>/client.wit` and
the Rust shim. The double dash keeps `a/b-c` distinct from `a-b/c`.
Single-dash dependency directories are not accepted as current generated
clients; regenerate from source or fetch schemas with `--from-url`.
Custom, non-generated files are never silently
overwritten. Commit regenerated files with the corresponding source changes.

## Standalone-first scaffolding

```sh
statex new shop --sdk /path/to/statex/crates/guest
statex new shop --lang python --dir services/checkout
```

Without `--workspace`, the project is independent of any enclosing
`statex-workspace.toml`. `--dir` changes its location, never its identity.
Rust scaffolding resolves `--sdk` or `STATEX_GUEST_PATH` from the invoking
directory, validates the SDK crate, and writes a path relative to the new
project. It does not bake in the checkout where the CLI was compiled or
assume that a version has been published. A relative dependency remains valid
when project and SDK are relocated together; ship or configure the SDK
accordingly. Python standalone projects copy bundled host WIT and guest
helpers.

## Optional workspace conventions

```sh
statex workspace init --statex /path/to/statex
statex new shop --workspace
statex new payments/shop --workspace --dir apps/arbitrary/deep/service
```

A workspace is shared tooling, not a required organizational hierarchy.
Discovery recursively reads projects under configured `apps`; manifest names
must be unique but are independent of directory spelling and depth.
`check --all` and `registry build` use that discovery. Explicit `--dir`
outside the discovery root is allowed, but such a project is checked
individually and is not included in the registry. Only scaffolded Rust
projects under the discovery root join its shared Cargo workspace.

Callee source resolution is: self, `[calls] paths`, then explicitly selected
workspace discovery by manifest name. `build`, `test`, `verify`, `deploy`,
`dev`, `calls add` and `calls sync` read workspace configuration only with
`--workspace`. `check --all` and registry commands explicitly select workspace
discovery; ordinary `check` does not. For example:

```toml
[calls]
apps = ["shop", "payments/ledger"]
paths = { shop = "../checkout-code", "payments/ledger" = "../accounting" }
```

Explicit source paths must contain a manifest with the requested name.
Unavailable local source can instead be fetched with
`statex calls sync --from-url <node>`.

## Identity and boundaries

HTTP paths stay `/v1/apps/shop/...` or `/v1/apps/payments/shop/...`;
namespaced storage keys still use `payments.shop` as a single segment.
The Python client preserves namespace slashes, and CLI URL generation follows
the same rule. No deployed actor data or URLs are renamed by these tooling
changes.

Moving source directories does not change app identity. Changing `[app] name`
does: existing actors and clients still refer to the old name, and
`statex check --against <revision>` reports the rename as breaking. Namespaces,
workspace membership and filesystem layout provide no authentication,
authorization or ownership guarantees.
