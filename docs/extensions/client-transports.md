# Python client transports

`Transport` retains statex JSON encoding, decoding, error mapping and retry
policy. Replace only HTTP I/O by passing `backend=`. The default is
`UrllibBackend`, using the standard library with the existing timeout/base URL.

```python
from statex_client import App, HttpResponse, Transport

class Backend:
    def request(self, method, url, headers, body, timeout):
        # Supply your own connection pool/authentication/recording implementation.
        assert method == "POST"
        return HttpResponse(200, b'{"result":42}')

app = App("counter", transport=Transport(backend=Backend()))
assert app.actor("counter", "alice").get() == 42
```

`HttpBackend` is a structural protocol; no inheritance is required.
`request(method, url, headers, body, timeout)` receives a complete URL, a fresh
header dictionary, optional JSON bytes, and the timeout in seconds. It returns
`HttpResponse(status, body)` for **all** HTTP statuses, including errors. Return
raw response bytes; do not decode statex results. Respect the timeout and avoid
mutating headers. Raise `OSError` for network failures. Other exceptions indicate
adapter/programming errors and propagate rather than being retried.

Backends should not retry independently. `Transport` retries network failures
and statex `unavailable` responses only, up to `retries` additional attempts with
the existing exponential backoff. A retry of a write may have an unknown outcome;
this interface does not provide exactly-once execution or make writes idempotent.

HTTP errors become the existing `StatexError` subclasses, preserving code,
message, status and detail. Typed actor calls decode `method_error` detail into
`MethodError.error`; untyped calls expose raw detail through `MethodError`.
Non-JSON/error envelopes fall back to `http_<status>`. Malformed successful JSON
raises `StatexError(code="transport", status=<HTTP status>)`, without a retry.
Exhausted network failures raise `StatexError(code="transport", status=0)`.

Adapters belong to each `Transport` instance, never global state. Sharing one
instance across clients deliberately shares its backend and retry settings;
backend thread safety and resource lifecycle are the adapter's responsibility.
The SDK does not close supplied backends.

The existing higher-level `App(..., transport=...)` injection remains available.
A full transport must implement `request(method, path, body=None)` and
`actor_path(app, actor_type, key)` and preserve the SDK error contract itself.
Prefer a backend when only replacing networking, so error semantics stay shared.
Generated clients embed this runtime and can use these hooks after regeneration.

Run `PYTHONPATH=sdk/python python3 -m unittest discover -s sdk/python/tests`.
