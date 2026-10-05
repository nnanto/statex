# Guarantees and failure semantics

## Single writer per actor

At most one node executes calls for an actor at a time, and once a newer owner
exists, no older owner can acknowledge a write. This rests on three
mechanisms:

1. **Ownership by CAS.** A node may only activate an actor by replacing `owner.json` with `If-Match` on the etag it read. It may only take over from another node whose node lease is expired, or whose session has changed because that node restarted.
2. **Lease margin.** A node only considers its own lease valid until `expires_at - max(ttl/5, 200ms)`. Expiry is measured from *before* the renewal request was sent. Peers consider the node dead only after `expires_at`. Clock skew between nodes must stay below that margin.
3. **Fencing.** A failed renewal (`412`), or failing to renew before local expiry, fences the node. It drops every resident actor immediately; `statex node` then exits with code 3 and should be restarted by its supervisor.

## Durability (the ack rule)

A successful response (2xx) is sent only after:

1. the transaction's WAL segment has been written to the object store,
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
| 404 | `not_found` | Unknown app, actor type or method |
| 409 | `conflict` | `_create` on an existing actor |
| 422 | `method_error` | The method returned `err(E)`; `error.detail` holds E. The transaction is rolled back |
| 500 | `trap` / `internal` | The guest trapped (panic, timeout, out of memory); the transaction is rolled back |
| 503 | `unavailable` | Ownership could not be established or confirmed; retry. The outcome of a write is unknown |
| 508 | `cycle` | An actor-to-actor call would re-enter an actor on its call chain, or the chain is too deep |

## Migrations

- Each actor type has its own ordered `migrations/<type>/*.sql`.
- Pending migrations are applied lazily inside the transaction of the first call after activation, or after a deploy that adds migrations.
- They are therefore atomic with that call and durable through the same WAL segment.
- Applied migrations are recorded in `_statex_migrations`.
- Migrations are append-only: never edit or reorder an applied file. Add a new one instead.
