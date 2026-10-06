# Guest execution limits

Applications request budgets in `statex.toml`; hosts impose ceilings. Effective
budgets are the tighter of the two. A host ceiling applies even when the app
omits that optional budget.

```toml
[limits]
timeout_ms = 5000
memory_mb = 64
fuel = 10000000
rps = 100
burst = 20
max_concurrent = 16
```

| Setting | Scope and enforcement |
|---|---|
| `timeout_ms` | Wall-clock guest invocation timeout, also bounded by the caller's remaining deadline |
| `memory_mb` | Aggregate WASM linear-memory growth budget for the actor's component store |
| `fuel` | WASM instruction fuel (gas), reset for each invocation |
| `rps` | Token-bucket refill rate, shared by this app's guest invocations on one node |
| `burst` | Token-bucket capacity; defaults to `rps` when omitted |
| `max_concurrent` | Admitted guest executions for this app on one node |

Memory and timeout retain their defaults of 64 MiB and 5000 ms. Fuel, rate,
and concurrency budgets are optional. All specified values must be positive;
app `burst` requires `rps`. Unknown settings are rejected, not silently ignored.
An app cannot disable a host ceiling by omitting its setting.

## Host configuration

```rust
use statex::HostLimits;

# async fn example() -> anyhow::Result<()> {
let mut config = statex::local_config("./data", "local")?;
config.guest_limits = HostLimits {
    max_timeout_ms: Some(2000),
    max_memory_mb: Some(32),
    max_fuel: Some(5000000),
    max_rps: Some(50),
    max_burst: Some(10),
    max_concurrent: Some(8),
};
let node = statex::start(config).await?;
# node.shutdown().await;
# Ok(())
# }
```

The standard CLI also accepts these ceilings:

```sh
statex node --store ./store \
  --max-guest-timeout-ms 2000 --max-guest-memory-mb 32 \
  --max-guest-fuel 5000000 --max-guest-rps 50 \
  --max-guest-burst 10 --max-guest-concurrent 8
```

Matching environment variables are `STATEX_MAX_GUEST_TIMEOUT_MS`,
`STATEX_MAX_GUEST_MEMORY_MB`, `STATEX_MAX_GUEST_FUEL`, `STATEX_MAX_GUEST_RPS`,
`STATEX_MAX_GUEST_BURST`, and `STATEX_MAX_GUEST_CONCURRENT`.

Runtime-only embedders use `RuntimeBuilder::host_limits`. Node configuration
can tighten a configured runtime's ceilings, never weaken them.
`Runtime::for_node` creates an independent quota scope while sharing the
engine/linker. Ordinary runtime clones share their scope.

## Accounting and errors

Rate and concurrency budgets are **per app per node**, shared across actor
types, keys, alarms, and nested calls. Forwarded calls consume the execution
owner's budget, not a second guest budget at the entry node. These are not
fleet-wide quotas: adding nodes can increase aggregate throughput.
Fuel is per actor invocation, not a shared call-tree gas wallet. Each callee
receives its own effective fuel budget, while its timeout remains bounded
by the parent's remaining deadline. Routing, slot waiting, and migrations
are not part of the guest timeout; callers can supply an end-to-end deadline
through trusted invocation metadata.

Quotas are acquired before fresh component initialization on the actor
execution path. Denied admissions do not execute component start functions.
Changing an app deployment does not reset the bucket or forget running
executions. Concurrency rejection does not consume a rate token.
Initialization has its own fuel/time envelope with the same effective caps;
the invoked method receives a fresh fuel budget.

Rate/concurrency admission uses try-admission, not an unbounded waiting
queue. HTTP failures are `429 rate_limited` or `429 overloaded`; actor callers
receive `call-error::rejected`, not an unknown-outcome transport error.
Nested calls can be rejected when their app has no available slot, rather
than deadlocking behind a permit held by their parent.

Fuel exhaustion, allocation failure, or guest timeout fail explicitly. The
node rolls back the transaction and discards the instance on execution
failure. SQL, migrations, and alarms are not acknowledged as committed.
External effects remain non-transactional, even when the guest exceeds a
budget.

`AppCode::effective_limits()` and the app/health HTTP diagnostics expose the
host-adjusted settings. Request schema manifests retain the app's requested
settings. The counters are process-local and reset on restart; limits are
not durable usage/billing records.

## What CPU and memory budgets do not mean

Fuel bounds WASM work, **not exact CPU milliseconds**. Fuel costs can change
with compiler/runtime versions. SQL, WASI, HTTP, and custom native callbacks
are not instruction-metered. Concurrency bounds execution pressure; it does
not reserve a physical CPU core. Use deployment-level CPU controls for a
hard process CPU allocation.

Epoch interruption checks guest execution at approximately 10 ms intervals.
Returned native callbacks are checked against elapsed deadlines, but
arbitrary blocking native code cannot be forcibly interrupted. Native
adapters must honor the supplied deadline and bound their own I/O.

The memory cap covers approved WASM linear-memory allocation, not total
process RSS, compiled code, native buffers, or actor database size. Memory
already allocated remains charged across reused calls. Use process limits
and backend-specific quotas for those other resources.
