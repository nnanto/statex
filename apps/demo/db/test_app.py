# Unit tests against the mock host; run them with `statex test`.
from statex_testing import call
from wit_world.exports.kv import Entry

from app import Kv


def test_put_get_scan():
    call("kv", "b1", Kv().put, "a/1", "x")
    call("kv", "b1", Kv().put, "a/2", "y")
    call("kv", "b1", Kv().put, "b/1", "z")
    assert call("kv", "b1", Kv().get, "a/1") == "x"
    assert call("kv", "b2", Kv().get, "a/1") is None  # every key is its own actor
    assert call("kv", "b1", Kv().scan, "a/") == [Entry("a/1", "x"), Entry("a/2", "y")]
    assert call("kv", "b1", Kv().delete, "a/1") is True
    assert call("kv", "b1", Kv().delete, "a/1") is False
