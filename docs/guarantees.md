# Guarantees and failure semantics

These guarantees require a conforming object store and transactional state
backend. The defaults use local files and SQLite; other implementations must
uphold the same [object-store](extensions/object-stores.md) and
[state-backend](extensions/state-backends.md) contracts. Interface
implementation alone is not evidence of distributed-system correctness.

## Single writer per actor

At most one node executes calls for an actor at a time, and once a newer owner
exists, no older owner can acknowledge a write. This rests on three
mechanisms:

1. **Ownership by CAS.** A node may only activate an actor by replacing `owner.json` with `If-Match` on the etag it read. It may only take over from another node whose node lease is expired, or whose session has changed because that node restarted.
2. **Lease margin.** A node only considers its own lease valid until `expires_at - max(ttl/5, 200ms)`. Expiry is measured from *before* the renewal request was sent. Peers consider the node dead only after `expires_at`. Clock skew between nodes must stay below that margin.
3. **Fencing.** A failed renewal (`412`), or failing to renew before local expiry, fences the node. It drops every resident actor immediately; `statex node` then exits with code 3 and should be restarted by its supervisor.

## Durability (the ack rule)

A successful response (2xx) is sent only after:

1. the transaction's recoverable change has been written to the object store
   (a WAL page segment for the SQLite backend),
2. the node's lease is still valid (with margin), **and**
3. `owner.json` still names this node, session and epoch.

Any write that was acknowledged therefore survives:

- a crash of the owner (the next owner restores snapshot + segments),
- loss of the node's local disk (local state is only a cache),
- a network partition (the partitioned node can no longer satisfy the ack rule).

## Alarms

- Setting or clearing an alarm is part of the call's transaction, so an
  acknowledged alarm survives the same failures as any other write.
- A durable alarm always has a wake hint in the object store (the hint is
  written before the segment), so it fires even if its owner crashes or the
  actor is idle-evicted.
- Delivery is **at least once**: a handler can run more than once (for example,
  after a crash between running it and acknowledging the result), but never
  concurrently with other calls to the same actor. A failed handler is retried
  with backoff and dropped after 6 retries.
- Firing time is not exact. Resident actors fire on time; others are woken by
  the waker node within `wake_tick` (1 s), or within `wake_full_scan` (30 s)
  after the waker fails over, plus any failover time of the actor itself.

## Durable background calls

`spawn` and `spawn_after` record a job in the **calling actor's outbox**.
Scheduling participates in that actor's transaction: a rollback removes the
job, and delivery starts only after the caller's changes are durable.
Returning a job ID from the host import is not itself a durable acknowledgement;
the enclosing actor invocation must commit through the normal ack rule.
An unavailable acknowledgement can still leave a committed job, just as an
unacknowledged ordinary write can become visible after recovery.

- Pending jobs, retry bookkeeping, and terminal results survive activation,
  eviction, and owner crashes through the actor's normal snapshots and changes.
- Discovery markers are published before the source change becomes durable.
  A marker is only a hint: dispatch must claim an actual committed job from
  the source actor, under its ownership and transaction rules.
- The dispatcher releases the source actor before invoking the target. A job
  may therefore call its own actor without re-entering a held source lock.
- Each target call has its own transaction and execution deadline. It is not
  cancelled when the originating invocation's deadline expires.
- Delivery is **at least once**, not exactly once. A target can commit before
  the dispatcher durably records completion. Recovery may call it again.
  Use application-level idempotency keys for non-idempotent effects.
- `spawn_after` is a not-before constraint, measured from enqueue time. It
  does not promise exact execution time. Outages, actor lock contention, and
  retries can delay delivery.
- There is no per-target FIFO guarantee. A delayed job does not block a newer
  job that is ready for delivery.
- Failed jobs remain inspectable in the source actor. Delivery attempts are
  bounded; a job can terminate without a successful target invocation.
- Deleting the source actor deletes its outbox. This cannot undo a target call
  that already started or committed.

The durable outbox is separate from ordinary synchronous requests, whose
in-memory lock waiters are not persisted.

## What is *not* guaranteed

- **Unacknowledged writes may still become visible.** If the segment upload succeeded but the ownership check then failed or timed out, the client receives `503 unavailable`. A later owner may still restore that write, which also happens if the owner crashed between upload and response. Treat 503 as "unknown outcome". Methods that must not be applied twice should take an idempotency key and record it in the actor's database (a `UNIQUE` column makes this a one-line check).
- **HTTP side effects are not transactional.** `http-client.send` runs during the call. If the transaction later rolls back or is never acknowledged, the request has still been sent. Use idempotency keys with external systems.
- **No cross-actor transactions.** Each call is atomic within one actor. This also holds for actor-to-actor calls: the callee commits on its own, even if the caller then rolls back, and a caller does not roll back when a callee fails. Coordinate across actors with application-level protocols (sagas or idempotent steps).
- **Unknown outcomes for calls between actors.** `call-error::unavailable` and `call-error::timeout` mean the callee may or may not have applied the call, just like a 503. Retry only idempotent calls.
- **Calls do not re-enter actors.** A call to an actor that is already on the call chain, or a chain deeper than 16, fails with `call-error::cycle`. Two independent requests that call each other's actors (A→B while B→A) are not detected. They wait on each other's locks until one caller's deadline runs out, so they are resolved by `timeout`.
- **Availability during failover.** If an owner dies, its actors are unavailable until its lease expires, i.e. up to `--lease-ttl` (10s by default). Calls received during that window are retried internally for up to 2×TTL before returning 503. A graceful shutdown (SIGINT or SIGTERM) releases actors and the lease immediately.

- **statex has no deploy access control.** Anyone who can write `deploy/` can deploy any app. Grant write access to `deploy/` only to CI, for example with ADLS directory ACLs. Nodes only need read access to `deploy/`.

## Errors

| Status | `error.code` | Meaning |
|---|---|---|
| 200 | – | `{"result": ...}`. For `result<T, E>` methods this is the `ok` value |
| 201 | – | Actor created (`_create`) |
| 400 | `bad_request` | Invalid JSON or arguments that do not match the WIT signature |
| 400 | `extension_invalid` | An invocation extension rejected input |
| 401 / 403 | `unauthorized` / `forbidden` | Authentication or policy rejected the invocation |
| 404 | `not_found` | Unknown app, actor type or method |
| 409 | `conflict` | `_create` on an existing actor |
| 422 | `method_error` | The method returned `err(E)`; `error.detail` holds E. The transaction is rolled back |
| 429 | `rate_limited` / `overloaded` | Per-app/node guest admission budget exhausted; no guest execution |
| 500 | `trap` / `internal` | The guest trapped (panic, timeout, out of memory); the transaction is rolled back |
| 500 | `extension_error` | A critical hook failed or panicked; no guest transaction is committed |
| 503 | `unavailable` | Ownership could not be established or confirmed; retry. The outcome of a write is unknown |
| 503 / 504 | `extension_unavailable` / `extension_timeout` | A critical hook failed or exceeded its budget; no guest transaction is committed |
| 508 | `cycle` | An actor-to-actor call would re-enter an actor on its call chain, or the chain is too deep |

## Migrations

- Each actor type has its own ordered `migrations/<type>/*.sql`.
- Pending migrations are applied by the state backend lazily inside the transaction of the first call after activation, or after a deploy that adds migrations.
- They are therefore atomic with that call and durable through the same change record.
- The SQLite backend records applied migrations in `_statex_migrations`.
- Migrations are append-only: never edit or reorder an applied file. Add a new one instead.

## Extension responsibilities

Custom state backends must atomically commit or roll back SQL, migrations,
alarms, and any supported outbox jobs together and restore exactly the committed state represented by a
snapshot plus its ordered changes. A backend format mismatch is an activation
failure, not permission to start a fresh actor. Changing engines requires an
explicit data migration; using a different factory does not convert data.

Custom host capabilities, HTTP transports, log sinks and client transports
must report errors and respect caller budgets. Native adapters are trusted:
WASM limits do not interrupt arbitrary blocking native I/O. Additional host
effects and actor-local extension memory are not rolled back or checkpointed.
Guest/client adapters must preserve method-error and unknown-outcome
distinctions rather than treating a failed call as success.

Invocation extensions cannot replace routing, transaction control, capture,
or acknowledgement. Admission rejection precedes actor-state side effects;
owner rejection precedes activation and migrations. A before-commit veto
rolls back guest, migrations, alarms, and hook writes together and discards
the component instance. Actor callers receive explicit `rejected` errors,
not an unknown-outcome transport failure. Hooks and request IDs do not
deduplicate repeated deliveries or provide distributed exactly-once semantics.

Native hooks must bound their own blocking work. Async owner checks use
timeouts and are followed by lease checks; read-only results also require a
current lease and ownership record. Caller cancellation does not abandon an
in-progress commit/capture/upload sequence. Completion and lifecycle
observers are non-vetoing, best-effort notifications, not a durable event bus.
External hook effects and typed local data are not rolled back. See
[invocation hooks](extensions/invocation-hooks.md) and
[runtime hooks](extensions/runtime-hooks.md) for phase and trust boundaries.

Guest budgets combine app settings with host ceilings. WASM memory and fuel,
guest timeouts, and per-app/node rate/concurrency limits are enforced by the
runtime. Quota rejection and guest budget failures do not commit actor state.
Fuel is not exact CPU time and native callbacks cannot be forcibly interrupted;
WASM memory accounting is not a process RSS limit. See
[execution limits](extensions/execution-limits.md).
