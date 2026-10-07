# HTTP, logging, and metrics adapters

`HttpTransport` and `LogSink` in `statex_runtime::services` are injectable
native services. The defaults are `DefaultHttpTransport` (ureq) and
`TracingLogSink`. `MetricsSink` receives framework counters and duration
measurements; the default `TracingMetricsSink` writes them to the
`statex::metrics` tracing target. Supply shared implementations when
constructing a runtime:

```rust,ignore
let runtime = statex_runtime::Runtime::builder()
    .http_transport(std::sync::Arc::new(MyHttpTransport))
    .log_sink(std::sync::Arc::new(MyLogSink))
    .metrics_sink(std::sync::Arc::new(MyMetricsSink))
    .build()?;
```

`HttpTransport::send(HttpRequest, Duration)` returns
`Result<HttpResponse, String>`. It runs on the actor's blocking execution
thread. The host validates the URL and application host allowlist **before**
calling any transport, and passes the remaining call budget. An adapter must
honor that timeout, bound memory use, and return actual transport failures.
If it follows redirects, it must not access a destination outside the
application's allowed hosts. The default disables redirects and rejects
responses larger than 10 MiB rather than silently truncating them.

`LogSink::log(&ActorIdentity, LogLevel, &str)` receives actor identity with
each entry. Log sinks must not block indefinitely or panic; logging is not a
durability mechanism. Use this hook to integrate existing observability
libraries rather than changing guest code.

`MetricsSink::record(&Metric)` receives typed counter and duration values with
bounded labels. The runtime reports guest execution counts and duration; node
configuration reports invocation counts, end-to-end duration, and actor
lifecycle transitions. Labels omit actor keys, request IDs, and other
unbounded or sensitive values. Metric callbacks are best-effort: a panic is
caught and logged without changing the invocation result. Implement this
interface to connect a metrics backend.
For node-owned invocation measurements, set `NodeConfig.metrics_sink`; for
runtime-only guest measurements, use `RuntimeBuilder::metrics_sink`.

The framework creates `statex.invocation` and `statex.guest_execution` tracing
spans. Invocation spans include request and parent-request IDs, app, actor type,
and operation; IDs propagate across nested and forwarded calls using the
existing invocation metadata. Install a `tracing` subscriber/exporter in the
embedding application to collect these spans and native framework logs.

These adapters are outside the state transaction. An HTTP request, log entry,
or metric can already have been emitted even if the guest later traps, returns
a method error, or the node loses ownership. Use external idempotency keys
where repeating an effect would be incorrect.
