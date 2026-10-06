# Object-store providers

statex does not require a blob service. The default for a plain filesystem path
is `LocalFsStore`. `file:///absolute/path` and `local:///absolute/path` select the
same implementation explicitly. `az://container` remains supported and selects
the existing Azure Blob implementation; it requires Azure configuration only
when selected. Unknown schemes, such as `s3://bucket`, are errors, not local
directories. Scheme names are case insensitive.

`statex_store::open` uses the built-in registry. A program can extend resolution
without changing that function or the storage consumers:

```rust
use std::sync::Arc;
use statex_store::{DynStore, LocalFsStore, StoreRegistry};

let mut providers = StoreRegistry::default();
// Example factory alias. A real provider would construct its own ObjectStore.
providers.register("example", |location| {
    Ok(Arc::new(LocalFsStore::new(location)?) as DynStore)
})?;
let store: DynStore = providers.open("example://./data")?;
# Ok::<(), anyhow::Error>(())
```

A factory receives only the location after `://`, and may capture its own
credentials, endpoint configuration, or an existing `DynStore`. Duplicate
registrations are rejected, including case variants. `StoreRegistry::empty()`
provides an allowlist with no built-in schemes. Plain paths resolve through its
`file` registration, if any. URLs require the `scheme://location` spelling;
prefix a relative path containing a colon with `./` to disambiguate it.
Filesystem locations are literal paths, not percent-decoded RFC file URLs.

Consumers still accept `DynStore = Arc<dyn ObjectStore>` directly. A custom
registry is an application-level factory, not a global plugin loader: CLI
scheme support is not automatically extended by registering a provider in a
separate application. No trait changes, remote service, or new dependencies are
needed for direct injection.

## Correctness contract

Implement every `ObjectStore` operation and verify these requirements before
using a provider for ownership records or leases:

* **Atomic conditional create:** `put_if_absent` has exactly one winner for a
  missing key. Existing keys return `StoreError::Precondition`.
* **Atomic conditional update:** `put_if_match` succeeds only if the supplied
  token matches the current object. Missing objects and stale tokens return
  `Precondition`. A clean rejection changes neither bytes nor version.
  Checking a token and then performing an unconditional write is not sufficient.
* **Fresh versions:** every successful write, including unconditional writes,
  same-content updates, and file uploads, returns a nonempty opaque token for
  that version. Do not use a content hash as the sole token: an ABA rewrite must
  not make an old conditional update valid again.
* **Strong visibility:** once a write succeeds, subsequent reads return its
  exact bytes and token, unless a later write intervenes. Listings immediately
  include newly written keys. Deletion is immediately visible to reads and
  listings. These guarantees must hold across all clients sharing a store,
  not just within one process's cache.
* **Ranges:** `get_range` returns at most the requested number of bytes,
  starting at the given offset and clipped at EOF. Empty and past-EOF ranges
  on existing objects return `Some(empty)`; missing keys return `None`, even
  for zero-length ranges. Large range lengths must not overflow arithmetic.
* **Listings:** return all keys beginning with the literal prefix, sorted
  lexicographically, without duplicates, staging objects, or metadata.
  Pagination must not silently omit pages. This is not a transactionally
  consistent snapshot across concurrent changes to different keys.
* **Streaming:** `put_file` uploads the entire stable source file, including
  empty files, with bounded data buffering; it must not publish partial
  content. `get_to_file` reads one object version with bounded buffering,
  creates or truncates its destination on success, and returns `false` for
  missing objects without creating or changing the destination. Pin chunked
  downloads to one version or fail rather than mixing versions. Errors may
  leave a partial destination, which callers must discard.
* **Missing objects:** `get` returns `None`; `delete` is idempotent. Backend
  permission, network, and decoding failures must not be disguised as absence.
* **Ambiguous failures:** only a definite, unchanged-state conditional rejection
  is `Precondition`. All other failures are `Other`, including errors where a
  write may already have been applied. Callers must reconcile such failures by
  reading state rather than assuming no write occurred.

Use relative slash-separated keys with no empty, `.` or `..` path components,
leading slash, or backslash. Avoid using both a key and its descendant
(`a` and `a/b`); localfs maps keys to files and cannot support that combination.
Do not externally modify the provider's backing files or objects.

`conformance_test(&*store).await?` checks sequential read-after-write
content/version visibility, fresh versions on same-content writes, unchanged
state on rejected writes, range semantics, sorted prefix listing, streaming
roundtrips (including empty files), missing-object behavior, and deletion.
It uses a random `probe/` namespace and creates a small scratch directory in
the current working directory. That directory must be writable. It attempts
cleanup on success and failure; a process interruption or unavailable provider
can leave probes behind. The test is a smoke test, not proof of concurrent CAS,
bounded memory use, failure recovery, or service durability. Supplement it with
multi-client contention, fault injection, and provider integration tests.

## Local filesystem scope and limits

Localfs supports cooperating processes on **one machine**, pointing at the same
canonical root on a filesystem with reliable advisory file locks and atomic
rename. Per-key lock files serialize conditional writes and full content/token
reads. Published content is replaced by rename from a separate `.staging`
directory; incomplete uploads never appear in listings. Version tokens are
random, independent of object content.

This is suitable for development and single-machine deployments, not a
distributed lease backend. There is no cross-machine lock protocol, and network
filesystems such as NFS/SMB are not assumed to provide the required semantics.
File locks are advisory: processes that bypass this implementation can violate
the contract. Treat the root as trusted, private storage; do not allow users to
replace directories or introduce symlinks inside it.

Object bytes and `.meta` version records are separate files. Their update is
serialized for cooperating processes but **not crash-atomic**: process
termination, power loss, or an I/O failure between metadata publication and
content rename can leave inconsistent state. Object data is synced before
rename, but metadata and parent directories are not fully crash-durable.
Missing or unreadable metadata is an error, not an empty version. Inspect and
repair backing state after interrupted writes; do not treat a restart as proof
that a lease or ownership record is durable. Long-lived `.locks` files and
staging remnants after crashes may require offline maintenance.

## Azure compatibility

The built-in `az` factory accepts a container name (optionally a trailing slash),
not an object path. The existing `AZURE_STORAGE_ACCOUNT_NAME`, endpoint override,
shared-key, workload-identity, and managed-identity configuration is unchanged.
Azure dependencies remain enabled to preserve existing CLI `az://` support.
No Azure credentials are consulted when opening a filesystem store.

Store tests run without a remote service:

```sh
cargo test -p statex-store
```

The optional Azurite integration test runs the same conformance probe and a
multi-chunk file roundtrip when `STATEX_AZURITE_TEST=1` and the usual
`AZURE_STORAGE_*` variables point at an already running emulator. It is skipped
otherwise; a passing local test run is not a claim that Azure was exercised.
