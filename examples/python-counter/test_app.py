# Unit tests against the mock host; run them with `statex test`.
import pytest

import statex
import statex_testing
from statex_testing import call

from app import Counter, Profile


def test_increments_per_key():
    assert call("counter", "alice", Counter().increment, 2) == 2
    assert call("counter", "alice", Counter().increment, 3) == 5
    assert call("counter", "bob", Counter().get) == 0


def test_err_rolls_back():
    with pytest.raises(statex.Err) as e:
        call("counter", "alice", Counter().checked_add, -1)
    assert e.value.value == "negative amount -1 for alice"
    assert call("counter", "alice", Counter().checked_add, 4) == 4
    assert statex_testing.logs()[-1][1] == "added 4"


def test_trap_rolls_back():
    with pytest.raises(RuntimeError):
        call("counter", "alice", Counter().boom)
    assert call("counter", "alice", Counter().get) == 0


def test_profile():
    assert call("profile", "alice", Profile().get) is None
    call("profile", "alice", Profile().set, "hi")
    assert call("profile", "alice", Profile().get) == "hi"


def test_reminder_alarm():
    assert statex_testing.alarm("counter", "alice") is None
    call("counter", "alice", Counter().remind, 60_000)
    at = statex_testing.alarm("counter", "alice")
    assert at is not None and at > statex.now_ms()
    call("counter", "alice", Counter().alarm, 0)
    assert call("counter", "alice", Counter().reminders) == 1
