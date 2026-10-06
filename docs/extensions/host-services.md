# HTTP and logging adapters

`HttpTransport` and `LogSink` in `statex_runtime::services` are injectable
native services. The defaults are `DefaultHttpTransport` (ureq) and
`TracingLogSink`. Supply shared implementations when constructing a runtime:

```rust,ignore
let runtime = statex_runtime::Runtime::builder()
    .http_transport(std::sync::Arc::new(MyHttpTransport))
    .log_sink(std::sync::Arc::new(MyLogSink))
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

Both interfaces are outside the state transaction. An HTTP request or log
entry can already have happened even if the guest later traps, returns a
method error, or the node loses ownership. Use external idempotency keys
where repeating an effect would be incorrect.
