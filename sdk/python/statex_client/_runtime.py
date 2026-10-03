# statex Python client runtime. Standard library only; Python 3.9+.
# This file is embedded verbatim into generated clients.

import base64
import enum
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional


class StatexError(Exception):
    """A call failed. `code` is one of: not_found, bad_request, method_error,
    conflict, unavailable, trap, internal, unauthorized, transport."""

    def __init__(self, code: str, message: str, status: int = 0, detail: Any = None):
        super().__init__("%s: %s" % (code, message))
        self.code = code
        self.message = message
        self.status = status
        self.detail = detail


class MethodError(StatexError):
    """The method returned the `err` case of its `result<T, E>`. `error` is the decoded E."""

    def __init__(self, error: Any, status: int = 422):
        super().__init__("method_error", repr(error), status, error)
        self.error = error


class NotFound(StatexError):
    pass


class BadRequest(StatexError):
    pass


class Conflict(StatexError):
    pass


class Unavailable(StatexError):
    pass


_ERRORS = {"not_found": NotFound, "bad_request": BadRequest, "conflict": Conflict, "unavailable": Unavailable}


def _snake(name: str) -> str:
    return name.replace("-", "_")


def _py(f: Dict[str, Any]) -> str:
    return f.get("py") or _snake(f["name"])


class Transport:
    """HTTP transport. Any statex node accepts any call; it routes to the owner."""

    def __init__(self, base_url: str = "http://127.0.0.1:9876", timeout: float = 30.0, retries: int = 3):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries

    def request(self, method: str, path: str, body: Any = None) -> Any:
        data = None if body is None else json.dumps(body).encode()
        attempt = 0
        while True:
            req = urllib.request.Request(self.base_url + path, data=data, method=method)
            req.add_header("content-type", "application/json")
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    return json.loads(r.read() or b"null")
            except urllib.error.HTTPError as e:
                try:
                    payload = json.loads(e.read() or b"null") or {}
                except ValueError:
                    payload = {}
                err = payload.get("error") or {}
                code = err.get("code", "http_%d" % e.code)
                if code == "unavailable" and attempt < self.retries:
                    attempt += 1
                    time.sleep(0.2 * 2 ** attempt)
                    continue
                if code == "method_error":
                    raise _RawMethodError(err.get("detail"), e.code)
                cls = _ERRORS.get(code, StatexError)
                raise cls(code, err.get("message", str(e)), e.code, err.get("detail"))
            except urllib.error.URLError as e:
                if attempt < self.retries:
                    attempt += 1
                    time.sleep(0.2 * 2 ** attempt)
                    continue
                raise StatexError("transport", str(e.reason))

    def actor_path(self, app: str, ty: str, key: str) -> str:
        q = urllib.parse.quote
        return "/v1/apps/%s/actors/%s/%s" % (q(app, safe="/"), q(ty, safe=""), q(key, safe=""))


class _RawMethodError(Exception):
    def __init__(self, detail: Any, status: int):
        self.detail = detail
        self.status = status


# --- JSON <-> Python mapping driven by the app schema ----------------------

_TYPES: Dict[str, Any] = {}


def encode(ty: Dict[str, Any], v: Any) -> Any:
    k = ty["kind"]
    if k == "list":
        el = ty["element"]
        if el["kind"] == "u8" and isinstance(v, (bytes, bytearray)):
            return base64.b64encode(bytes(v)).decode()
        return [encode(el, x) for x in v]
    if k == "option":
        return None if v is None else encode(ty["inner"], v)
    if k == "tuple":
        return [encode(t, x) for t, x in zip(ty["items"], v)]
    if k == "record":
        if isinstance(v, dict):
            get = lambda f: v[f["name"]] if f["name"] in v else v.get(_py(f))  # noqa: E731
        else:
            get = lambda f: getattr(v, _py(f))  # noqa: E731
        return {f["name"]: encode(f["ty"], get(f)) for f in ty["fields"]}
    if k == "enum":
        return v.value if isinstance(v, enum.Enum) else str(v).replace("_", "-").lower()
    if k == "flags":
        return [x.replace("_", "-") for x in v]
    if k == "variant":
        if isinstance(v, str):
            return {"tag": v}
        tag = v.tag if hasattr(v, "tag") else v["tag"]
        value = v.value if hasattr(v, "value") else v.get("value")
        case = next(c for c in ty["cases"] if c["name"] == tag)
        out = {"tag": tag}
        if case.get("ty") is not None:
            out["value"] = encode(case["ty"], value)
        return out
    if k == "result":
        if isinstance(v, dict) and ("ok" in v or "err" in v):
            side = "ok" if "ok" in v else "err"
            t = ty.get(side)
            return {side: None if t is None else encode(t, v[side])}
        return {"ok": None if ty.get("ok") is None else encode(ty["ok"], v)}
    return v


def decode(ty: Optional[Dict[str, Any]], j: Any) -> Any:
    if ty is None:
        return None
    k = ty["kind"]
    if k == "list":
        el = ty["element"]
        if el["kind"] == "u8" and isinstance(j, str):
            return base64.b64decode(j)
        return [decode(el, x) for x in j]
    if k == "option":
        return None if j is None else decode(ty["inner"], j)
    if k == "tuple":
        return tuple(decode(t, x) for t, x in zip(ty["items"], j))
    if k == "record":
        vals = {_py(f): decode(f["ty"], j.get(f["name"])) for f in ty["fields"]}
        cls = _TYPES.get(ty.get("py", ""))
        return cls(**vals) if cls else vals
    if k == "enum":
        cls = _TYPES.get(ty.get("py", ""))
        return cls(j) if cls else j
    if k == "flags":
        return list(j)
    if k == "variant":
        tag = j if isinstance(j, str) else j["tag"]
        case = next((c for c in ty["cases"] if c["name"] == tag), None)
        value = None
        if case is not None and case.get("ty") is not None and isinstance(j, dict):
            value = decode(case["ty"], j.get("value"))
        cls = _TYPES.get(ty.get("py", ""))
        return cls(tag, value) if cls else {"tag": tag, "value": value}
    if k == "result":
        side = "ok" if "ok" in j else "err"
        return {side: decode(ty.get(side), j[side])}
    return j


class Actor:
    """A handle to one actor. Creating a handle does not contact the server."""

    _type = ""
    _methods: Dict[str, Dict[str, Any]] = {}

    def __init__(self, app: "App", key: str):
        self._app = app
        self.key = key

    def __repr__(self) -> str:
        return "%s(%r)" % (type(self).__name__, self.key)

    def _call(self, method: str, args: Dict[str, Any]) -> Any:
        sig = self._methods.get(method)
        t = self._app._transport
        path = t.actor_path(self._app._name, self._type, self.key) + "/" + method
        if sig is None:
            return t.request("POST", path, args)["result"]
        body = {}
        for p in sig["params"]:
            v = args.get(_py(p))
            if v is None and p["ty"]["kind"] != "option":
                raise TypeError("%s() missing argument %r" % (_snake(method), _py(p)))
            body[p["name"]] = encode(p["ty"], v)
        res = sig.get("result")
        try:
            out = t.request("POST", path, body)["result"]
        except _RawMethodError as e:
            err_ty = res.get("err") if res else None
            raise MethodError(decode(err_ty, e.detail), e.status)
        if res is not None and res["kind"] == "result":
            return decode(res.get("ok"), out)
        return decode(res, out)

    def create(self) -> None:
        """Explicitly creates the actor; raises Conflict if it already exists."""
        t = self._app._transport
        t.request("POST", t.actor_path(self._app._name, self._type, self.key) + "/_create", {})

    def delete(self) -> None:
        """Deletes the actor and all of its data."""
        t = self._app._transport
        t.request("DELETE", t.actor_path(self._app._name, self._type, self.key))

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)

        def call(**kwargs: Any) -> Any:
            return self._call(name.replace("_", "-"), kwargs)

        return call


class App:
    """Untyped client: App("counter").actor("counter", "alice").increment(by=1)."""

    _name = ""

    def __init__(self, name: Optional[str] = None, base_url: str = "http://127.0.0.1:9876",
                 timeout: float = 30.0, retries: int = 3, transport: Optional[Transport] = None):
        if name:
            self._name = name
        self._transport = transport or Transport(base_url, timeout, retries)

    def actor(self, ty: str, key: str) -> Actor:
        c = Actor(self, key)
        c._type = ty
        c._methods = {}
        return c

    def actors(self, ty: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        q = "?limit=%d" % limit + ("&type=" + urllib.parse.quote(ty) if ty else "")
        return self._transport.request("GET", "/v1/apps/%s/actors%s" % (self._name, q))["actors"]
