"""statex Python client.

Typed clients are generated with `statex codegen python`; this package also
offers an untyped client:

    from statex_client import App
    App("counter").actor("counter", "alice").increment(by=1)
"""

from ._runtime import (  # noqa: F401
    App,
    BadRequest,
    Actor,
    Conflict,
    MethodError,
    NotFound,
    StatexError,
    Transport,
    Unavailable,
)
