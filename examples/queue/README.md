# Queue: an ordinary Wasm app

This is an application-level queue, not a host queue API. Each `queue` key owns
SQL configuration, payloads, permanent acceptance receipts and active batch
leases. Its alarm leases messages and transactionally spawns `worker.process`.
The host dispatches the worker **after durable commit and outside the queue
actor lock**. The stateless worker synchronously calls the stateful `sink` and
then `queue.settle` through generated clients; it never uses SQL, alarms or spawn.

The manifest opts queue actors into `group_commit=true`. The host groups calls
that accumulate during a previous durability upload, without a coalescing
timer: one outer SQLite transaction contains a savepoint per call. All replies,
including read-only and error replies, remain withheld until durability and
ownership checks pass. Alarms retain their separate transaction path.

## Run

From this directory, with the repository's current CLI/SDK:

```sh
cargo test
cargo build --release --target wasm32-wasip2
statex verify
statex dev
```

In another terminal in this directory:

```sh
statex call queue orders send '{"id":"order-1","body":"aGVsbG8=","delay-ms":0}'
# After the default one-second batching timeout:
statex call sink orders entries
# [{"id":"order-1","body":"aGVsbG8="}]
statex call queue orders info
# pending=0, in-flight=0, receipts=1
statex call queue orders send '{"id":"order-1","body":"aGVsbG8=","delay-ms":0}'
# false: previously accepted, even though it has already been consumed
```

Bytes use the standard statex base64 JSON representation. `send-batch` takes
`{"messages":[{"id":"order-2","body":"AAH/","delay-ms":0}, ...]}` and returns a
boolean per input. These are opaque payloads, not interpreted JSON.

## Configuration

`queue.configure` replaces the whole per-key configuration:

```sh
statex call queue orders configure '{
  "configuration": {
    "max-batch-size": 10,
    "batch-timeout-ms": 1000,
    "max-retries": 3,
    "retry-delay-ms": 1000,
    "lease-ms": 30000,
    "max-concurrency": 4,
    "consumer-key": "sample",
    "dlq-key": "dead-orders"
  }
}'
statex call queue orders pause
statex call queue orders resume
```

Defaults are batch size 100, timeout 1 second, three retries, retry delay
1 second, lease 30 seconds, four concurrent batches, consumer `"sample"` and
no DLQ. A null consumer key disables dispatch; setting a consumer or resuming
reschedules delivery. All consumer keys invoke this app's `worker.process`.
The sample writes into `sink` with the **source queue key**, so different
queues' receipts and observable effects do not collide.

Bounds: batch size 1–100, concurrency 1–32, retries 0–100, lease 1 ms–4 days,
timeout/retry delay/delivery delay less than 4 days. `send-batch` accepts at
most 100 inputs and 1 MiB of payload; each message allows 64 KiB and a
nonempty ID of at most 1024 UTF-8 bytes. Dispatched batches also cap their
combined payload at 1 MiB. The encoded dispatch also stays within spawn's
1 MiB JSON limit (including
base64, message IDs and envelope fields), so large payloads form smaller batches.
There is no strict FIFO guarantee across batches, delays or retries.
Batch timeout starts at the oldest ready message's
availability time, not its original enqueue time.

## Delivery and failure semantics

- Producer IDs are stable idempotency keys. The first acceptance wins;
  retrying an ID with a different body or delay does not replace it. Acceptance
  receipts remain for the actor's lifetime, including after acknowledgement,
  DLQ transfer, expiry and reactivation. Their storage grows with unique IDs;
  deleting the actor also deletes its deduplication history.
- Each lease increments attempts once: initial attempt **1**, then at most
  `max-retries` additional attempts. Outbox replay of one lease does not
  increment attempts. Configuration changes affect subsequent scheduling and
  failure handling; they do not extend existing lease deadlines.
- `settle(token, outcomes)` applies outcomes only to an active, unexpired
  token, requiring exactly one `{id, ack}` outcome for every message in that batch. `ack=true` removes
  the message; `ack=false` retries after the configured delay or exhausts it.
  Partial, duplicate or unknown message outcomes for an active token fail
  without changing messages. Unknown, expired, superseded and already-completed
  tokens return success as a no-op, regardless of their outcomes. An older
  token cannot settle a newer lease, and obsolete/replayed worker outbox tasks
  finish successfully rather than retrying forever.
  Tokens include the actor ownership epoch, preventing collisions when a queue
  actor is deleted and recreated.
- The sample reports successful sink calls (including duplicate records) as
  acknowledgements and sink errors as retries. If the worker traps or cannot
  settle, **nothing auto-acknowledges**: lease expiry follows the same retry
  path. Consumer effects can precede settlement, so consumers must be
  idempotent. The sample sink's primary key makes record replay safe.
- On exhaustion, the same source transaction records a durable spawn of
  `queue.send` to the configured local DLQ key and removes the source message.
  DLQ IDs are `dlq:` plus SHA-256 of the JSON pair `[source-queue-key,id]`;
  payload bytes are unchanged. Destination acceptance receipts deduplicate
  outbox replay. Without a DLQ the exhausted payload is discarded. Provision
  the DLQ with `consumer-key=null` if it should hold rather than consume
  messages. A queue cannot name itself as its DLQ; avoid multi-queue cycles.
- Payload retention is four days **from original acceptance**, including
  delayed, retried and leased messages. Expiry discards rather than
  dead-letters. Alarms continue sweeping while paused or without a consumer.
  Pause stops new leases, not running workers, settlement or lease expiry.
  No empty-queue polling alarm is left behind.
- A full source outbox or another spawn-enqueue error does not consume an
  attempt or fail the queue alarm. Ready deliveries are postponed by one
  second and maintenance commits with a new alarm. Exhausted messages whose
  DLQ enqueue is blocked remain unleased pending transfer, without invoking
  the consumer again; DLQ retry alarms run even while paused or without a
  consumer. The original retention deadline always takes precedence over
  this backoff, so sustained outbox saturation cannot disable sweeping.
  Lease-expiry/DLQ maintenance handles at most 100 messages per alarm and
  re-arms for any remaining expired batches, bounding failed-enqueue work.

`info` exposes configuration, pause state, pending count (including delayed
messages), leased count, active batches, permanent receipt count and alarm
time. This example intentionally does not implement a generic external
consumer registry, polling consumers, receipt garbage collection or
Cloudflare's JavaScript queue API.

## Tests and host integration contract

Native tests use `testing::{call,try_call,reactivate,spawned}`. The
outbox inspection contract is `spawned(type,key) -> Vec<spawn::Request>` with
`id, app, actor_type, key, method, args_json`; only committed requests appear.
The suite covers delayed/size/timeout batching, concurrency, mixed outcomes,
retry accounting, trap expiry, stale-token fencing, persistent receipts,
retention with no consumer or pause, bounds and transaction rollback (including
DLQ and worker outboxes), saturated-outbox recovery and retention, and obsolete
callback completion without hiding transport failures. Time-dependent queue logic receives deterministic
test timestamps internally; production methods always use wall-clock time.

Generated client stubs exercise worker success/failure and replay. Native
`testing::call` does not support nested actors, so these stubs capture calls
and the test applies the sink and settlement effects in separate transactions.
They are not a substitute for the real host lock/durability test:

1. Deploy `target/wasm32-wasip2/release/queue.wasm` with this manifest and its
   queue/sink migrations. Self-client imports must resolve to app `queue`.
2. Call `queue("orders").send` or `send-batch`, then poll
   `sink("orders").entries` and `queue("orders").info` until the effects are
   visible and the payloads are gone. This requires worker-to-queue synchronous
   calls to complete while the dispatcher does not hold the queue lock.
3. Retry the producer ID and replay worker delivery; assert one sink record
   and one acceptance receipt. Restart/re-activate between enqueue and
   dispatch to check committed outbox recovery.
4. Inject a worker trap and confirm lease expiry/retry/DLQ; submit an old
   settlement after a new lease and confirm it cannot alter that lease.

An installed CLI predating `statex:host/spawn.send` cannot verify or run this
component; use a CLI built from the current repository.
