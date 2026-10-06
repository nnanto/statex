"""Exercise the native adapter with generated-binding-shaped test doubles."""

import importlib.util
import sys
import types
import unittest
from contextvars import Context
from pathlib import Path
from unittest.mock import patch


class Err(Exception):
    def __init__(self, value):
        self.value = value
        super().__init__(value)


class Value:
    def __init__(self, value=None):
        self.value = value


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class CapabilityTests(unittest.TestCase):
    def setUp(self):
        modules = {}
        for name in ["wit_world", "wit_world.imports", "componentize_py_types",
                     "wit_world.imports.context", "wit_world.imports.sql",
                     "wit_world.imports.http_client", "wit_world.imports.log",
                     "wit_world.imports.alarms"]:
            modules[name] = types.ModuleType(name)
        modules["wit_world"].__path__ = []
        imports = modules["wit_world.imports"]
        imports.__path__ = []
        imports.__file__ = __file__
        modules["componentize_py_types"].Err = Err
        modules["componentize_py_types"].Ok = Value
        modules["componentize_py_types"].Some = Value
        sql = modules["wit_world.imports.sql"]
        for kind in ["Null", "Integer", "Real", "Text", "Blob"]:
            setattr(sql, "Value_" + kind, Value)
        sql.Rows = types.SimpleNamespace
        http = modules["wit_world.imports.http_client"]
        http.Request = types.SimpleNamespace
        http.Response = types.SimpleNamespace
        modules["wit_world.imports.log"].Level = types.SimpleNamespace(
            DEBUG="debug", INFO="info", WARN="warn", ERROR="error"
        )
        for name, module in modules.items():
            if name.startswith("wit_world.imports."):
                setattr(imports, name.rsplit(".", 1)[-1], module)
        self.modules = patch.dict(sys.modules, modules)
        self.modules.start()
        root = Path(__file__).resolve().parents[1]
        self.testing = load("_statex_testing_test", root / "statex_testing.py")
        self.guest = load("_statex_guest_test", root / "statex.py")

    def tearDown(self):
        self.testing.reset()
        self.modules.stop()

    def test_scope_nesting_errors_and_context_isolation(self):
        t, g = self.testing, self.guest
        t.mock_http(lambda req: (204, {}, b"default"))
        with t.capabilities(http=lambda req: (200, {}, b"outer")):
            self.assertEqual(g.http("GET", "https://test")[2], b"outer")
            self.assertEqual(Context().run(g.http, "GET", "https://test")[2], b"default")
            with t.capabilities(http=lambda req: (201, {}, b"inner")):
                self.assertEqual(g.http("GET", "https://test")[2], b"inner")
            def failed(req):
                raise Err("offline")
            with self.assertRaises(Err) as raised:
                with t.capabilities(http=failed):
                    g.http("GET", "https://test")

            self.assertEqual(raised.exception.value, "offline")
            self.assertEqual(g.http("GET", "https://test")[2], b"outer")
        self.assertEqual(g.http("GET", "https://test")[2], b"default")
        t.reset()
        with self.assertRaises(Err):
            g.http("GET", "https://test")

    def test_rejection_is_explicit_and_has_known_outcome(self):
        rejected = type("CallError_Rejected", (), {})()
        rejected.value = "policy veto"
        self.assertEqual(self.guest.describe(rejected), "rejected: policy veto")
        self.assertFalse(self.guest.outcome_unknown(rejected))
        unavailable = type("CallError_Unavailable", (), {})()
        unavailable.value = "lost owner"
        self.assertTrue(self.guest.outcome_unknown(unavailable))

    def test_transactions_and_actor_isolation_with_adapters(self):
        t, g = self.testing, self.guest
        observed = []
        with t.capabilities(http=lambda req: (200, {}, g.key().encode()),
                            log=lambda level, msg: observed.append((level, msg))):
            def initialize():
                g.execute("CREATE TABLE t(v INTEGER)")
                g.execute("INSERT INTO t VALUES(?)", 1)
                g.set_alarm(10)
                g.info("custom")
                return g.http("GET", "https://test")[2]
            self.assertEqual(t.call("kv", "a", initialize), b"a")
            self.assertEqual(observed, [("info", "custom")])
            self.assertEqual(t.logs(), [])
            def fail():
                g.execute("INSERT INTO t VALUES(?)", 2)
                g.set_alarm(20)
                raise Err("rollback")
            with self.assertRaises(Err):
                t.call("kv", "a", fail)
            self.assertEqual(t.call("kv", "a", g.query_scalar, "SELECT count(*) FROM t"), 1)
            self.assertEqual(t.alarm("kv", "a"), 10)
            self.assertIsNone(t.alarm("kv", "b"))
            with self.assertRaises(Err):
                t.call("kv", "b", g.query, "SELECT * FROM t")
            with self.assertRaises(Err):
                t.call("kv", "a", g.execute, "COMMIT")
            with self.assertRaises(RuntimeError):
                with t.capabilities():
                    t.call("kv", "a", lambda: (_ for _ in ()).throw(RuntimeError("trap")))
        t.call("kv", "a", g.info, "default")
        self.assertEqual(t.logs(), [("info", "default")])


if __name__ == "__main__":
    unittest.main()
