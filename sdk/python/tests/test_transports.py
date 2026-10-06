import io
import json
import unittest
import urllib.error
from unittest.mock import patch

from statex_client import (
    App, BadRequest, Conflict, HttpResponse, MethodError, NotFound,
    StatexError, Transport, Unavailable, UrllibBackend,
)
from statex_client._runtime import Actor


class Backend:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def __bool__(self):
        return False

    def request(self, *args):
        self.calls.append(args)
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def error(code, detail=None, status=503):
    return HttpResponse(status, json.dumps({
        "error": {"code": code, "message": "failed", "detail": detail}
    }).encode())


class TransportTests(unittest.TestCase):
    def test_custom_backend_is_used_for_calls_and_lifecycle(self):
        backend = Backend(HttpResponse(200, b'{"result":7}'),
                          HttpResponse(204), HttpResponse(204),
                          HttpResponse(200, b'{"actors":[]}'))
        transport = Transport("https://node.test/", timeout=2, backend=backend)
        app = App("demo", transport=transport)
        actor = app.actor("counter", "a/b")
        self.assertEqual(actor.increment(by=3), 7)
        actor.create()
        actor.delete()
        self.assertEqual(app.actors(), [])
        method, url, headers, body, timeout = backend.calls[0]
        self.assertEqual(method, "POST")
        self.assertEqual(url, "https://node.test/v1/apps/demo/actors/counter/a%2Fb/increment")
        self.assertEqual(json.loads(body), {"by": 3})
        self.assertEqual(headers["content-type"], "application/json")
        self.assertEqual(timeout, 2)
        self.assertEqual([c[0] for c in backend.calls], ["POST", "POST", "DELETE", "GET"])

    def test_clients_do_not_share_backends(self):
        one = Backend(HttpResponse(200, b'{"result":1}'))
        two = Backend(HttpResponse(200, b'{"result":2}'))
        self.assertEqual(App("x", transport=Transport(backend=one)).actor("t", "a").get(), 1)
        self.assertEqual(App("x", transport=Transport(backend=two)).actor("t", "a").get(), 2)
        self.assertEqual(len(one.calls), 1)
        self.assertEqual(len(two.calls), 1)
        self.assertIsInstance(Transport().backend, UrllibBackend)

    def test_typed_and_untyped_method_errors(self):
        backend = Backend(error("method_error", "bad", 422),
                          error("method_error", {"why": "bad"}, 422))
        app = App("x", transport=Transport(backend=backend))
        actor = Actor(app, "a")
        actor._methods = {"get": {"params": [], "result": {
            "kind": "result", "ok": {"kind": "u32"}, "err": {"kind": "string"}
        }}}
        with self.assertRaises(MethodError) as raised:
            actor._call("get", {})
        self.assertEqual(raised.exception.error, "bad")
        self.assertEqual(raised.exception.status, 422)
        with self.assertRaises(MethodError) as raised:
            app.actor("t", "a").get()
        self.assertEqual(raised.exception.error, {"why": "bad"})

    def test_error_mapping_and_malformed_responses(self):
        for code, cls in [("not_found", NotFound), ("bad_request", BadRequest),
                          ("conflict", Conflict), ("unavailable", Unavailable),
                          ("trap", StatexError), ("unauthorized", StatexError)]:
            backend = Backend(error(code, {"x": 1}, 409))
            with self.assertRaises(cls) as raised:
                Transport(backend=backend, retries=0).request("GET", "/")
            self.assertEqual(raised.exception.code, code)
            self.assertEqual(raised.exception.detail, {"x": 1})
            self.assertEqual(raised.exception.status, 409)
            self.assertEqual(len(backend.calls), 1)
        for body in [b"not json", b"[]", b'{"error":"bad"}', b'{"error":{"code":[]}}']:
            with self.assertRaises(StatexError) as raised:
                Transport(backend=Backend(HttpResponse(500, body))).request("GET", "/")
            self.assertEqual(raised.exception.code, "http_500")
        with self.assertRaises(StatexError) as raised:
            Transport(backend=Backend(HttpResponse(200, b"\xff"))).request("GET", "/")
        self.assertEqual(raised.exception.code, "transport")
        self.assertEqual(raised.exception.status, 200)

    @patch("statex_client._runtime.time.sleep")
    def test_retry_policy_is_shared(self, sleep):
        backend = Backend(OSError("offline"), error("unavailable"),
                          HttpResponse(200, b'{"result":3}'))
        self.assertEqual(Transport(backend=backend, retries=2).request("POST", "/", {}), {"result": 3})
        self.assertEqual(sleep.call_count, 2)
        self.assertEqual(backend.calls[0], backend.calls[1])
        backend = Backend(OSError("offline"), OSError("offline"))
        with self.assertRaises(StatexError) as raised:
            Transport(backend=backend, retries=1).request("GET", "/")
        self.assertEqual(raised.exception.code, "transport")
        self.assertEqual(raised.exception.status, 0)
        with self.assertRaises(Unavailable):
            Transport(backend=Backend(error("unavailable")), retries=0).request("GET", "/")

    @patch("statex_client._runtime.urllib.request.urlopen")
    def test_default_backend_preserves_http_and_network_errors(self, urlopen):
        urlopen.side_effect = urllib.error.HTTPError(
            "http://node", 404, "missing", {}, io.BytesIO(error("not_found").body)
        )
        with self.assertRaises(NotFound):
            Transport(retries=0).request("GET", "/")
        urlopen.side_effect = urllib.error.URLError("offline")
        with self.assertRaises(StatexError) as raised:
            Transport(retries=0).request("GET", "/")
        self.assertEqual(raised.exception.code, "transport")


if __name__ == "__main__":
    unittest.main()
