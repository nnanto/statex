# Actor state backends

SQLite with WAL/LTX replication is the default, not the node's state interface.
Embed a node with a different database by setting
`NodeConfig.database_factory` to `Arc<dyn DatabaseFactory>`. The interfaces
live in `statex_runtime::database`.

```rust
use std::sync::Arc;
use statex_node::NodeConfig;
use statex_runtime::database::DatabaseFactory;

fn configure(mut config: NodeConfig, backend: Arc<dyn DatabaseFactory>) -> NodeConfig {
    config.database_factory = backend;
    config
}
```

## Interfaces

`DatabaseFactory` creates an isolated database for each actor:

- `identity()` returns a stable backend name and a format version.
- `open(path)` creates fresh state if the path is absent, or opens the opaque
  snapshot already downloaded to that path.
- `replay(path, epoch, txid, change)` applies one change to a **closed** snapshot.
  Validate its identity, ordering metadata and integrity before modifying it.

`Database` owns the actor's complete transactional state:

- `execute` and `query` implement the guest SQL interface using driver-neutral
  `SqlValue` and `SqlRows`. Unsupported SQL must return an error. A backend
  need not be SQLite, but must implement the SQL dialect used by its apps.
- `begin`, `commit`, and `rollback` define one actor operation's transaction.
- `apply_migrations` applies named migrations exactly once and returns their
  names. Migration data and the applied-name bookkeeping must roll back
  together. The backend interprets the migration SQL in its supported dialect.
- `alarm`, `set_alarm`, and `clear_alarm` manage authoritative alarms in that
  same transaction. Every installation increments a persistent sequence
  counter; clearing must not reset it. Preserve the supplied epoch and retry.
  Backend failures in the infallible guest `get`/`clear` alarm imports are
  recorded as sticky capability errors. The runtime turns them into traps at
  the invocation boundary, so the node rolls back and discards the instance
  instead of acknowledging an absent alarm or a failed cancellation.
- `capture(epoch, txid)` returns an opaque change payload for committed changes
  since the last capture/checkpoint, including migrations and alarms. Return
  `None` after reads and rollback. A payload may encode a delta or a full image.
- `checkpoint` writes one complete snapshot file and resets the capture
  baseline. Multi-file engines must package their state into that image.

Backends can also implement the durable outbox operations: `enqueue_job`,
`claim_job`, `update_job`, `job`, and `has_pending_jobs`. Enqueue, claims, and
completion updates must participate in the current transaction and its normal
capture/replay path. Claims include overdue running jobs so an interrupted
dispatcher cannot strand work. Completion updates must reject stale attempts
instead of overwriting a newer claim. Backends without outbox support must
reject scheduling explicitly; their ordinary actor calls remain supported.
Job payloads must not turn guest-controlled SQL into trusted invocation
identity. Protect host-owned outbox bookkeeping from guest SQL and migrations:
dispatch derives the actor caller from the authenticated source, but preserves
the job's trusted originating principal and attributes. SQLite enforces this
through its parser authorizer; a custom outbox backend must provide equivalent
protection.

All backend calls are synchronous and run on blocking threads. `Database`
must be `Send`, and the factory must be `Send + Sync`. Local working files
must stay in the directory supplied to `open`; activation replaces that
directory and actor drop removes it. Snapshot paths must remain valid and
unchanged until the next mutable operation or drop. Never checkpoint an
uncommitted transaction.

`DatabaseHandle` is `Arc<Mutex<Box<dyn Database>>>`. Runtime host SQL and alarm
calls use this neutral handle, not a SQLite connection. Runtime embeddings
can instantiate through `AppCode::instantiate` or `instantiate_with`, both
accepting neutral database handles. The explicit `sqlite_handle` constructor
adapts a shared SQLite connection for embeddings that own persistence
themselves; this connection adapter cannot capture or checkpoint node state.
The driver-specific helpers `statex_runtime::sqlite::open_db` and
`statex_runtime::sqlite::apply_migrations` configure raw SQLite connections and
apply their migrations. Generic embeddings should use the `Database` methods instead.

## Durability and compatibility

The node still owns ordering, object uploads, actor ownership and fencing.
It starts a transaction, applies migrations, runs the guest, and commits only
an ordinary successful return. Traps, migration errors and guest `result::err`
roll back. Capture failures or other database failures discard the actor.
The node writes a new alarm's wake hint before uploading its change payload.
It acknowledges writes only after upload and a current-ownership check.

Existing object names remain:

```text
actors/<app>/<type>/<key>/ltx/e<epoch>/backend.json
actors/<app>/<type>/<key>/ltx/e<epoch>/snapshot-<txid>.db
actors/<app>/<type>/<key>/ltx/e<epoch>/<txid>.ltx
```

The `.db` and `.ltx` suffixes are historical names, not requirements on custom
payloads. `backend.json` is published **before** each activation snapshot and
records the factory identity. Restoration checks it before interpreting any
snapshot or change. A mismatching name/version is an error, not a reset or
automatic conversion. Missing identity metadata is also an error for every
backend, including SQLite; snapshots are never guessed to belong to a default
format. There is no implicit old-format acceptance.

Every node that may activate an actor must configure the same format identity.
Do not reuse an identity for incompatible formats. To change formats, perform
an explicit, externally coordinated export/import rather than pointing a new
factory at existing state. Compaction retains the current epoch's identity
and removes superseded snapshots, changes and older epoch identities.

The default `SqliteFactory` retains WAL mode, disabled autocheckpoint,
`synchronous=FULL`, transactional migrations/alarms, page capture, and streaming
snapshots. It validates replayed LTX checksums, epoch/transaction IDs and page
geometry, and resets WAL capture after a successful truncating checkpoint.

## Backend verification

A replacement should test SQL value round trips; migration rollback and
idempotence; alarms across rollback, clear/reinstall and restore; read-only
capture; committed change replay; snapshot/reset/replay; malformed payload
rejection; and backend identity mismatch. Test node handoff and compaction,
not just a database adapter in isolation. The node's custom-backend tests
exercise a JSON state machine without a SQLite connection.
