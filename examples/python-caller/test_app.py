# Unit tests against the mock host; run them with `statex test`.
# Calls to other actors are answered by stubs.
import pytest

import statex
from statex_testing import call, stub
from wit_world.imports import account, counter
from wit_world.imports import relay as relays
from wit_world.imports.account import TxError_InvalidAmount
from wit_world.imports.actors import CallError_Unavailable

from app import Relay


class FakeCounter:
    """Stands in for the counter app's `counter` actors."""

    def increment(self, actor: str, by: int) -> int:
        if actor == "down":
            raise statex.Err(CallError_Unavailable("no owner"))
        return by * 10


class FakeAccount:
    def deposit(self, actor: str, amount: int, memo):
        return statex.Err(TxError_InvalidAmount()) if amount == 0 else statex.Ok(amount + 1)


def test_typed_calls_with_stubs():
    stub(counter, FakeCounter())
    stub(account, FakeAccount())
    assert call("relay", "r", Relay().bump, "c", 2) == 20
    assert call("relay", "r", Relay().deposit, "a", 5, "rent") == 6
    # The callee refused: raising Err rolls this relay's transaction back, so it is not counted.
    with pytest.raises(statex.Err) as e:
        call("relay", "r", Relay().deposit, "a", 0, None)
    assert e.value.value == "refused: invalid amount"
    assert call("relay", "r", Relay().calls) == 2


def test_failed_calls_roll_back():
    stub(counter, FakeCounter())
    with pytest.raises(statex.Err) as e:
        call("relay", "r", Relay().bump, "down", 1)
    assert e.value.value == "unavailable: no owner"
    assert call("relay", "r", Relay().calls) == 0


def test_unstubbed_calls_fail_loudly():
    with pytest.raises(AssertionError, match="without a stub"):
        call("relay", "r", Relay().bump, "c", 1)


class LocalRelays:
    """Routes calls between relays to the real implementation, each in its own
    actor and transaction, like the host does."""

    def ping(self, actor: str, path):
        try:
            return statex.Ok(call("relay", actor, Relay().ping, path))
        except statex.Err as e:
            if type(e.value).__name__.startswith("CallError_"):
                raise  # a failed call (e.g. a cycle) stays a call error
            return statex.Err(e.value)


def test_calls_between_relays():
    stub(relays, LocalRelays())
    assert call("relay", "a", Relay().ping, ["b", "c"]) == ["a", "b", "c"]
    assert call("relay", "b", Relay().calls) == 1
    with pytest.raises(statex.Err) as e:
        call("relay", "a", Relay().ping, ["b", "a"])
    assert e.value.value == "call cycle: pycaller/relay/a -> pycaller/relay/b -> pycaller/relay/a"
