# Guest capability adapters

Guest helpers remain ordinary library defaults. Native adapters replace outbound
HTTP and logging, not actor identity, SQL storage, migrations, or alarms. No
adapter is installed by default.

The production guest SQL API is backend-neutral: SQLite is the default, while
SQL dialect and migration support belong to the configured host state backend.
These native testing adapters retain the in-memory SQLite mock; they do not
select or replace that host state backend.

## Rust native

Implement `statex_guest::adapters::Capabilities` and scope it with
`with_capabilities`. Both trait methods have defaults: HTTP uses
`testing::mock_http`, and logging uses the mock's log collector.

```rust
use std::rc::Rc;
use statex_guest::{adapters::{self, Capabilities}, http, testing, Result};

struct HttpAdapter;
impl Capabilities for HttpAdapter {
    fn http_send(&self, request: http::Request) -> Result<http::Response> {
        Ok(http::Response {
            status: 200,
            body: request.url.into_bytes(),
            ..Default::default()
        })
    }
}

adapters::with_capabilities(Rc::new(HttpAdapter), || {
    testing::call("counter", "alice", || {
        assert_eq!(http::Request::get("https://example.test").send().unwrap().status, 200);
    });
});
```

Overrides are thread-local, nest, and restore even after a panic. `Rc` permits
non-thread-safe embedding services; use interior mutability for mutable adapter
state. Adapter `Error`s are returned unchanged. Do not call `http::send` from
an HTTP override or `log::log` from a logging override (that recurses). An adapter
may use SQL, identity and alarms when invoked inside a test-host actor call.
This is not a replacement storage engine or a production host transaction API.

## Python native

`statex_testing.capabilities(http=..., log=...)` scopes native handlers. HTTP has
the same contract as `mock_http`: receive the generated `http_client.Request`,
return `(status, headers_dict, body_bytes)` or `http_client.Response`.
Logging receives `(generated_log_level, message)` and replaces the default
collector/printing within that scope. Omitted handlers inherit enclosing/default
behavior.

```python
import statex
from statex_testing import call, capabilities

with capabilities(http=lambda req: (200, {}, b"stubbed")):
    response = call("counter", "alice", statex.http, "GET", "https://example.test")
    assert response == (200, {}, b"stubbed")
```

Overrides use `ContextVar`, restore after exceptions, and do not leak to unrelated
execution contexts. The underlying native Python test host is still
single-threaded: this does not make concurrent actor calls supported. Exceptions,
including `statex.Err`, propagate unchanged. `mock_http`, `stub` and all existing
helper APIs continue working. `reset` resets mock host state, not an active
lexical capability scope.

## Transaction contract

Rust `testing::call` rolls back panics; `try_call` also rolls back returned
`Err`. Python `call` rolls back exceptions, including `statex.Err`. SQL and alarm
mutations still share that transaction and remain isolated per `(actor type, key)`.
Adapters do not begin or commit transactions. HTTP/logging side effects remain
non-transactional, so adapters must not assume rollback undoes them. Capability
adapters are trusted application code, not a sandbox/security boundary.

## WASM extensions

Native adapters are intentionally unavailable on WASM. For additional
capabilities declare your own WIT package/interface imports in the component's
world, generate your own Rust `wit_bindgen` or Python `componentize-py` bindings,
and call those bindings alongside these helpers. Python bindings that must be
bundled should be imported at build time. Do not hide arbitrary imports behind
the standard host SDK. The embedding host must explicitly link the additional
interface, with its own permissions and failure contract; declaring an import
alone does not make the stock runtime implement it.

## Tests

Run `cargo test -p statex-guest` and
`python3 -m unittest discover -s sdk/python-guest/tests`. Python adapter tests
use generated-binding-shaped doubles and do not require componentization.
