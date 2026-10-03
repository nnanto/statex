"""Ergonomic helpers over the statex host imports for Python actors.

    import statex
    statex.execute("UPDATE t SET v = v + ?1", by)
    n = statex.query_scalar("SELECT v FROM t")

Every method call runs inside one host-managed transaction: returning commits,
raising rolls back. For a method returning `result<T, E>`, `raise statex.Err(e)`
returns the `err` case (HTTP 422 with `e` as detail); any other exception traps.
"""

from typing import Any, Dict, List, Optional, Tuple

from componentize_py_types import Err  # noqa: F401  (re-exported)

from wit_world.imports import context as _context
from wit_world.imports import http_client as _http
from wit_world.imports import log as _log
from wit_world.imports import sql as _sql


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


def debug(msg: str) -> None:
    _log.log(_log.Level.DEBUG, msg)


def info(msg: str) -> None:
    _log.log(_log.Level.INFO, msg)


def warn(msg: str) -> None:
    _log.log(_log.Level.WARN, msg)


def error(msg: str) -> None:
    _log.log(_log.Level.ERROR, msg)
