"""Test harness: run Python actor methods natively against a mock host.

    # test_app.py -- run with `statex test`
    from statex_testing import call
    from app import Counter

    def test_increments():
        assert call("counter", "alice", Counter().increment, 2) == 2
        assert call("counter", "alice", Counter().increment, 3) == 5
        assert call("counter", "bob", Counter().get) == 0

Each (actor type, key) gets its own in-memory SQLite database with the
migrations from `migrations/<actor-type>/*.sql` applied, like the default SQLite host.
Each `call` is one transaction: returning commits, raising (including
`statex.Err`) rolls back and re-raises.

Calls to other actors fail with "called without a stub" until you `stub` the
client module:

    from wit_world.imports import account
    from wit_world.imports.actors import CallError_Unavailable

    class FakeAccount:
        def deposit(self, actor, amount, memo):
            if actor == "down":
                raise statex.Err(CallError_Unavailable("no owner"))
            return statex.Ok(amount)

    stub(account, FakeAccount())

`statex test` generates the typed bindings (`.statex/bindings`) and runs pytest;
listing `pytest_plugins = ["statex_testing"]` in conftest.py resets the mock
host before every test.
"""

import importlib
import inspect
import json
import os
import sqlite3
import sys
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

__all__ = ["call", "stub", "clear_stubs", "mock_http", "capabilities", "logs", "alarm", "reactivate", "reset", "set_app", "project_root", "drain_jobs", "mock_spawn"]


def project_root() -> Path:
    """The directory holding statex.toml: $STATEX_PROJECT, or the nearest one above the cwd."""
    env = os.environ.get("STATEX_PROJECT")
    if env:
        return Path(env)
    here = Path.cwd().resolve()
    for d in (here, *here.parents):
        if (d / "statex.toml").exists():
            return d
    return here


_ROOT = project_root()
_BINDINGS = _ROOT / ".statex" / "bindings"
if _BINDINGS.is_dir() and str(_BINDINGS) not in sys.path:
    sys.path.insert(0, str(_BINDINGS))

try:
    from componentize_py_types import Err
    import wit_world.imports as _imports
except ImportError as e:  # pragma: no cover
    raise ImportError(
        "statex_testing needs the generated bindings in %s; run `statex test` "
        "(or `statex build`) to create them" % _BINDINGS
    ) from e

_HOST = ("context", "sql", "http_client", "log", "actors", "alarms")


def _module(name: str) -> Optional[Any]:
    try:
        return importlib.import_module("wit_world.imports." + name)
    except ImportError:
        return None


def _app_name() -> str:
    try:
        import tomllib

        with open(_ROOT / "statex.toml", "rb") as f:
            return tomllib.load(f)["app"]["name"]
    except Exception:
        return "app"


class _State:
    def __init__(self) -> None:
        self.app = _app_name()
        self.dbs: Dict[Tuple[str, str], sqlite3.Connection] = {}
        self.epochs: Dict[Tuple[str, str], int] = {}
        self.stack: List[Tuple[str, str]] = []
        self.logs: List[Tuple[Any, str]] = []
        self.http: Optional[Callable[[Any], Any]] = None
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.clock_ms = 0
        self.next_job = 0
        self.spawn_handlers: Dict[Tuple[str, str, str], Callable[..., Any]] = {}


_state = _State()
_capabilities: ContextVar = ContextVar("statex_native_capabilities", default={})


@contextmanager
def capabilities(http: Optional[Callable[[Any], Any]] = None,
                 log: Optional[Callable[[Any, str], None]] = None,
                 spawn: Optional[Callable[..., str]] = None,
                 job: Optional[Callable[[str], Optional[str]]] = None) -> Any:
    """Temporarily adapts native capabilities without replacing SQL or alarms.

    HTTP uses the same request/response contract as mock_http. Exceptions
    propagate unchanged. Omitted handlers inherit the enclosing scope/default.
    Scheduling and job inspection require explicit handlers in this or an
    enclosing scope; otherwise they raise unsupported errors.
    Nested scopes restore on exit, including exceptions; overrides are local
    to this execution context. The underlying test host remains single-threaded.
    """
    handlers = dict(_capabilities.get())
    handlers["_adapter"] = True
    if http is not None:
        handlers["http"] = http
    if log is not None:
        handlers["log"] = log
    if spawn is not None:
        handlers["spawn"] = spawn
    if job is not None:
        handlers["job"] = job
    token = _capabilities.set(handlers)
    try:
        yield
    finally:
        _capabilities.reset(token)


def _current() -> Tuple[str, str]:
    if not _state.stack:
        raise RuntimeError("host function used outside of statex_testing.call(...)")
    return _state.stack[-1]


def _db(who: Tuple[str, str]) -> sqlite3.Connection:
    conn = _state.dbs.get(who)
    if conn is None:
        conn = sqlite3.connect(":memory:", isolation_level=None)
        mig = _ROOT / "migrations" / who[0]
        for f in sorted(mig.glob("*.sql")) if mig.is_dir() else []:
            conn.executescript(f.read_text())
        _state.dbs[who] = conn
        _state.epochs.setdefault(who, 1)
    return conn


def call(actor_type: str, key: str, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Runs `fn(*args, **kwargs)` as a method call on actor `key` of `actor_type`."""
    who = (actor_type, key)
    if who in _state.stack:
        path = " -> ".join("%s/%s/%s" % (_state.app, *w) for w in _state.stack + [who])
        actors = _module("actors")
        if actors is None:
            raise RuntimeError("call cycle: " + path)
        raise Err(actors.CallError_Cycle(path))
    conn = _db(who)
    conn.execute("BEGIN IMMEDIATE")
    _state.stack.append(who)
    jobs_before = set(_state.jobs)
    try:
        result = fn(*args, **kwargs)
    except BaseException:
        conn.execute("ROLLBACK")
        for id in set(_state.jobs) - jobs_before:
            if _state.jobs[id]["source"] == who:
                del _state.jobs[id]
        raise
    else:
        conn.execute("COMMIT")
        return result
    finally:
        _state.stack.pop()


def mock_spawn(app: str, actor_type: str, method: str, handler: Callable[..., Any]) -> None:
    """Installs a deferred handler taking `(key, *JSON_args)`, not called until drain_jobs."""
    _state.spawn_handlers[(app, actor_type, method)] = handler


def drain_jobs(elapsed_ms: int = 0) -> int:
    """Advances a virtual clock and delivers due jobs in their own transactions."""
    if _state.stack:
        raise RuntimeError("drain jobs outside statex_testing.call")
    if isinstance(elapsed_ms, bool) or not isinstance(elapsed_ms, int) or elapsed_ms < 0:
        raise ValueError("elapsed_ms must be a nonnegative integer")
    _state.clock_ms += elapsed_ms
    due = [j for j in _state.jobs.values()
           if j["status"] == "pending" and j["due_ms"] <= _state.clock_ms]
    for j in due:
        j["status"] = "running"
        j["attempts"] += 1
        previous_app = _state.app
        _state.app = j["app"]
        try:
            handler = _state.spawn_handlers.get((j["app"], j["actor_type"], j["method"]))
            if handler is None:
                raise RuntimeError("no native job handler installed; use mock_spawn")
            j["result"] = call(j["actor_type"], j["key"], handler, j["key"], *j["args"])
        except BaseException as e:
            j["status"], j["error"] = "failed", dict(code="native", message=str(e))
        else:
            j["status"] = "succeeded"
        finally:
            _state.app = previous_app
    return len(due)


def reactivate(actor_type: str, key: str) -> None:
    """Simulates the actor moving to another node: its epoch increases, its data stays."""
    who = (actor_type, key)
    _state.epochs[who] = _state.epochs.get(who, 1) + 1


def set_app(name: str) -> None:
    """The app name `statex.app()` returns (default: [app] name in statex.toml)."""
    _state.app = name


def mock_http(handler: Optional[Callable[[Any], Any]]) -> None:
    """Answers outbound HTTP with `handler(request)`, which returns
    `(status, headers_dict, body_bytes)` or an `http_client.Response`."""
    _state.http = handler


def logs() -> List[Tuple[Any, str]]:
    """Messages logged so far, as (level, message)."""
    return list(_state.logs)


_ALARM_TABLE = """CREATE TABLE IF NOT EXISTS _statex_alarm(
    id INTEGER PRIMARY KEY CHECK (id = 0), at_ms INTEGER,
    retry INTEGER NOT NULL DEFAULT 0, epoch INTEGER NOT NULL DEFAULT 0, seq INTEGER NOT NULL DEFAULT 0)"""


def _read_alarm(conn: sqlite3.Connection) -> Optional[int]:
    conn.execute(_ALARM_TABLE)
    row = conn.execute("SELECT at_ms FROM _statex_alarm WHERE id = 0").fetchone()
    return row[0] if row else None


def alarm(actor_type: str, key: str) -> Optional[int]:
    """When the alarm of actor `key` of `actor_type` is scheduled (Unix ms), or
    None. To test the handler, call it like a method:
    `call("counter", "alice", Counter().alarm, 0)`."""
    return _read_alarm(_db((actor_type, key)))


# ---- clients of other actors ------------------------------------------------

def _client_modules() -> List[Any]:
    out = []
    pkg_dir = Path(_imports.__file__).parent
    for f in sorted(pkg_dir.glob("*.py")):
        if f.stem != "__init__" and f.stem not in _HOST:
            m = _module(f.stem)
            if m is not None:
                out.append(m)
    return out


_NAMES: Dict[str, List[str]] = {}


def _functions(client: Any) -> List[str]:
    """The client's call functions, as generated (before any stubbing)."""
    if client.__name__ not in _NAMES:
        _NAMES[client.__name__] = [
            n for n, f in vars(client).items()
            if inspect.isfunction(f) and f.__module__ == client.__name__ and not n.startswith("_")
        ]
    return _NAMES[client.__name__]


def _short(client: Any) -> str:
    return client.__name__.rsplit(".", 1)[-1]


def _unstubbed(client: Any, name: str) -> Callable[..., Any]:
    def f(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError(
            "%s.%s was called without a stub; use statex_testing.stub(%s, ...)" % (_short(client), name, _short(client))
        )
    return f


def stub(client: Any, impl: Any) -> None:
    """Answers calls through `client` (a generated module such as
    `wit_world.imports.account`) with the same-named methods of `impl`, which
    take the actor key first. Missing methods fail with "called without a stub".
    Raise `Err(CallError_...)` from a method to simulate a failed call."""
    names = _functions(client)
    if not names or _short(client) in _HOST:
        raise TypeError("%r is not a client of another actor type" % (client,))
    extra = [n for n in dir(impl) if not n.startswith("_") and callable(getattr(impl, n)) and n not in names]
    if extra:
        raise TypeError("%s has no function(s) %s; it has %s" % (_short(client), ", ".join(extra), ", ".join(names)))
    for n in names:
        method = getattr(impl, n, None)
        setattr(client, n, method if method is not None else _unstubbed(client, n))


def clear_stubs() -> None:
    """Removes all stubs: calls to other actors fail again."""
    for client in _client_modules():
        for n in _functions(client):
            setattr(client, n, _unstubbed(client, n))


# ---- the mock host ----------------------------------------------------------

_FORBIDDEN = ("BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE", "ATTACH", "DETACH", "VACUUM", "PRAGMA")


def _forbidden(stmt: str) -> Optional[str]:
    s = stmt.lstrip().upper()
    for kw in _FORBIDDEN:
        if s.startswith(kw):
            rest = s[len(kw):]
            if not rest or not (rest[0].isalnum() or rest[0] == "_"):
                return kw
    return None


def _install() -> None:
    actors = _module("actors")
    if actors is not None:
        def spawn(app: str, actor_type: str, key: str, method: str, args: str, delay_ms: int) -> str:
            handler = _capabilities.get().get("spawn")
            if handler is not None:
                return handler(app, actor_type, key, method, args, delay_ms)
            if _capabilities.get().get("_adapter"):
                raise RuntimeError("adapter does not support actor scheduling")
            source = _current()
            parsed = json.loads(args)
            if not isinstance(parsed, list):
                raise ValueError("spawn arguments must be a positional array")
            _state.next_job += 1
            id = "mock-job-%s" % _state.next_job
            _state.jobs[id] = dict(id=id, source=source, source_app=_state.app, app=app, actor_type=actor_type,
                                   key=key, method=method, args=parsed,
                                   due_ms=_state.clock_ms + delay_ms, status="pending",
                                   result=None, error=None, attempts=0)
            return id

        def job(id: str) -> Optional[str]:
            handler = _capabilities.get().get("job")
            if handler is not None:
                return handler(id)
            if _capabilities.get().get("_adapter"):
                raise RuntimeError("adapter does not support job inspection")
            source = _current()
            j = _state.jobs.get(id)
            if j is None or j["source"] != source or j["source_app"] != _state.app:
                return None
            return json.dumps(dict(
                id=j["id"], target={"app": j["app"], "type": j["actor_type"], "key": j["key"]},
                method=j["method"], args=j["args"], status=j["status"],
                not_before_ms=j["due_ms"], next_attempt_ms=j["due_ms"],
                attempts=j["attempts"], result=j["result"], error=j["error"],
                context=dict(request_id="mock-source-" + id, parent_request_id=None,
                             caller=dict(kind="embedded"), principal=None,
                             deadline_unix_ms=2**64 - 1, attributes={})))

        actors.spawn = spawn
        actors.job = job

    ctx = _module("context")
    if ctx is not None:
        def app() -> str:
            _current()
            return _state.app

        ctx.app = app
        ctx.actor_type = lambda: _current()[0]
        ctx.key = lambda: _current()[1]
        ctx.epoch = lambda: _state.epochs.get(_current(), 1)

    sql = _module("sql")
    if sql is not None:
        def to_py(v: Any) -> Any:
            return getattr(v, "value", None)

        def from_py(v: Any) -> Any:
            if v is None:
                return sql.Value_Null()
            if isinstance(v, int):
                return sql.Value_Integer(v)
            if isinstance(v, float):
                return sql.Value_Real(v)
            if isinstance(v, str):
                return sql.Value_Text(v)
            return sql.Value_Blob(bytes(v))

        def run(stmt: str, params: List[Any]) -> sqlite3.Cursor:
            kw = _forbidden(stmt)
            if kw:
                raise Err("%s is not allowed: the host manages transactions" % kw)
            try:
                return _db(_current()).execute(stmt, [to_py(p) for p in params])
            except sqlite3.Error as e:
                raise Err(str(e)) from None

        def execute(stmt: str, params: List[Any]) -> int:
            return max(run(stmt, params).rowcount, 0)

        def query(stmt: str, params: List[Any]) -> Any:
            cur = run(stmt, params)
            cols = [d[0] for d in cur.description or []]
            return sql.Rows(columns=cols, rows=[[from_py(v) for v in r] for r in cur.fetchall()])

        sql.execute = execute
        sql.query = query

    http = _module("http_client")
    if http is not None:
        def send(req: Any) -> Any:
            handler = _capabilities.get().get("http", _state.http)
            if handler is None:
                raise Err("no HTTP mock: use statex_testing.mock_http(handler)")
            resp = handler(req)
            if isinstance(resp, tuple):
                status, headers, body = resp
                resp = http.Response(status=status, headers=list(dict(headers).items()), body=bytes(body))
            return resp

        http.send = send

    alarms = _module("alarms")
    if alarms is not None:
        def set_alarm(at_ms: int) -> None:
            who = _current()
            conn = _db(who)
            conn.execute(_ALARM_TABLE)
            conn.execute(
                "INSERT INTO _statex_alarm(id, at_ms, retry, epoch, seq) VALUES(0, ?1, 0, ?2, 1) "
                "ON CONFLICT(id) DO UPDATE SET at_ms = ?1, retry = 0, epoch = ?2, seq = seq + 1",
                (at_ms, _state.epochs.get(who, 1)),
            )

        def clear_alarm() -> None:
            conn = _db(_current())
            conn.execute(_ALARM_TABLE)
            conn.execute("UPDATE _statex_alarm SET at_ms = NULL, retry = 0 WHERE id = 0")

        alarms.set = set_alarm
        alarms.get = lambda: _read_alarm(_db(_current()))
        alarms.clear = clear_alarm

    log = _module("log")
    if log is not None:
        def record(level: Any, msg: str) -> None:
            handler = _capabilities.get().get("log")
            if handler is not None:
                handler(level, msg)
                return
            _state.logs.append((level, msg))
            print("[%s] %s" % (getattr(level, "name", level), msg))

        log.log = record

    clear_stubs()


def reset() -> None:
    """Forgets all actors, stubs, logs and the HTTP mock."""
    for conn in _state.dbs.values():
        conn.close()
    _state.__init__()
    clear_stubs()


_install()

try:
    import pytest

    @pytest.fixture(autouse=True)
    def _statex_reset() -> Any:
        reset()
        yield
except ImportError:  # pragma: no cover
    pass
