"""Ergonomic helpers over the statex host imports for Python actors.

    import statex
    statex.execute("UPDATE t SET v = v + ?1", by)
    n = statex.query_scalar("SELECT v FROM t")

Every method call runs inside one host-managed transaction: returning commits,
raising rolls back. For a method returning `result<T, E>`, `raise statex.Err(e)`
returns the `err` case (HTTP 422 with `e` as detail); any other exception traps.

Calls to other actors (see `statex calls`) raise `Err(call_error)` when the call
itself fails; a callee's own `result<T, E>` comes back as an `Ok` or `Err` value:

    from wit_world.imports import account
    try:
        r = account.deposit("bob", 10, None)
    except statex.Err as e:
        if statex.outcome_unknown(e.value): ...    # it may or may not have happened
        raise statex.Err("deposit failed: " + statex.describe(e.value))
    match r:
        case statex.Ok(balance): ...
        case statex.Err(error): ...

Alarms (the world must `import statex:host/alarms@0.1.0;`, and the actor
class must define the handler `alarm(self, retry_count: int)`, declared in WIT
as `alarm: func(retry-count: u32);`):

    statex.set_alarm_in(60_000)     # call alarm() in a minute
"""

import time
from typing import Any, Dict, List, Optional, Tuple

from componentize_py_types import Err, Ok, Some  # noqa: F401  (re-exported)

from wit_world.imports import context as _context
from wit_world.imports import http_client as _http
from wit_world.imports import log as _log
from wit_world.imports import sql as _sql

# Imported eagerly: componentize-py bundles only the modules loaded at build time.
try:
    from wit_world.imports import alarms as _alarms_import
except ImportError:  # the world does not import statex:host/alarms
    _alarms_import = None


def _to_value(v: Any) -> Any:
    if v is None:
        return _sql.Value_Null()
    if isinstance(v, bool):
        return _sql.Value_Integer(int(v))
    if isinstance(v, int):
        return _sql.Value_Integer(v)
    if isinstance(v, float):
        return _sql.Value_Real(v)
    if isinstance(v, str):
        return _sql.Value_Text(v)
    if isinstance(v, (bytes, bytearray)):
        return _sql.Value_Blob(bytes(v))
    raise TypeError("unsupported SQL parameter type: %s" % type(v).__name__)


def _from_value(v: Any) -> Any:
    return getattr(v, "value", None)


def execute(stmt: str, *params: Any) -> int:
    """Runs a statement; returns the number of changed rows."""
    return _sql.execute(stmt, [_to_value(p) for p in params])


def query(stmt: str, *params: Any) -> List[Tuple[Any, ...]]:
    """Runs a query; returns all rows as tuples."""
    rows = _sql.query(stmt, [_to_value(p) for p in params])
    return [tuple(_from_value(v) for v in r) for r in rows.rows]


def query_dicts(stmt: str, *params: Any) -> List[Dict[str, Any]]:
    """Runs a query; returns all rows as dicts keyed by column name."""
    rows = _sql.query(stmt, [_to_value(p) for p in params])
    return [dict(zip(rows.columns, (_from_value(v) for v in r))) for r in rows.rows]


def query_one(stmt: str, *params: Any) -> Optional[Tuple[Any, ...]]:
    """Returns the first row, or None."""
    rows = query(stmt, *params)
    return rows[0] if rows else None


def query_scalar(stmt: str, *params: Any) -> Any:
    """Returns the first column of the first row, or None."""
    row = query_one(stmt, *params)
    return row[0] if row else None


_CALL_ERRORS = {
    "NotFound": "not found",
    "Incompatible": "incompatible",
    "Trap": "callee trapped",
    "Unavailable": "unavailable",
    "Cycle": "call cycle",
    "Timeout": "timed out",
}


def _call_error_kind(error: Any) -> str:
    name = type(error).__name__
    kind = name[len("CallError_"):] if name.startswith("CallError_") else ""
    if kind not in _CALL_ERRORS:
        raise TypeError("not a call-error: %r" % (error,))
    return kind


def describe(error: Any) -> str:
    """A call-error as text, e.g. "unavailable: no owner" (pass `e.value` of the raised `Err`)."""
    kind = _call_error_kind(error)
    detail = getattr(error, "value", None)
    return _CALL_ERRORS[kind] + (": %s" % detail if detail is not None else "")


def outcome_unknown(error: Any) -> bool:
    """True for `unavailable` and `timeout`: the callee may or may not have applied the call."""
    return _call_error_kind(error) in ("Unavailable", "Timeout")


def app() -> str:
    return _context.app()


def actor_type() -> str:
    return _context.actor_type()


def key() -> str:
    """The key of the actor being invoked, e.g. "alice"."""
    return _context.key()


def epoch() -> int:
    return _context.epoch()


def http(method: str, url: str, body: Optional[bytes] = None,
         headers: Optional[Dict[str, str]] = None) -> Tuple[int, Dict[str, str], bytes]:
    """Outbound HTTP to hosts allowed in statex.toml. Not transactional."""
    req = _http.Request(method=method, url=url, headers=list((headers or {}).items()), body=body)
    resp = _http.send(req)
    return resp.status, dict(resp.headers), bytes(resp.body)


def _alarms() -> Any:
    if _alarms_import is None:
        raise RuntimeError("alarms need `import statex:host/alarms@0.1.0;` in the app's world")
    return _alarms_import


def now_ms() -> int:
    """The current Unix time in milliseconds."""
    return int(time.time() * 1000)


def set_alarm(at_ms: int) -> None:
    """Schedules the actor's alarm for `at_ms` (Unix ms), replacing any earlier
    one. Takes effect if the method commits. Raises `Err` if the actor type has
    no alarm handler."""
    _alarms().set(at_ms)


def set_alarm_in(delay_ms: int) -> None:
    """Schedules the actor's alarm `delay_ms` from now."""
    set_alarm(now_ms() + delay_ms)


def get_alarm() -> Optional[int]:
    """When the actor's alarm is scheduled (Unix ms), or None."""
    return _alarms().get()


def clear_alarm() -> None:
    """Cancels the actor's alarm, if any."""
    _alarms().clear()


def debug(msg: str) -> None:
    _log.log(_log.Level.DEBUG, msg)


def info(msg: str) -> None:
    _log.log(_log.Level.INFO, msg)


def warn(msg: str) -> None:
    _log.log(_log.Level.WARN, msg)


def error(msg: str) -> None:
    _log.log(_log.Level.ERROR, msg)
