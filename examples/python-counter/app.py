# Each exported WIT interface is implemented by a class with the same
# (PascalCase) name. Every call runs in one transaction: returning commits,
# raising rolls back. For result<T, E> methods, `raise statex.Err(e)` returns err(e).
from typing import Optional

from wit_world import exports

import statex


class Counter(exports.Counter):
    def increment(self, by: int) -> int:
        statex.execute("UPDATE counter SET value = value + ?1 WHERE id = 0", by)
        return self.get()

    def get(self) -> int:
        return statex.query_scalar("SELECT value FROM counter WHERE id = 0") or 0

    def checked_add(self, by: int) -> int:
        # Written first: raising Err below rolls this update back.
        statex.execute("UPDATE counter SET value = value + ?1 WHERE id = 0", by)
        if by < 0:
            raise statex.Err("negative amount %d for %s" % (by, statex.key()))
        statex.info("added %d" % by)
        return self.get()

    def boom(self) -> None:
        statex.execute("UPDATE counter SET value = 999 WHERE id = 0")
        raise RuntimeError("boom")  # traps: the update is rolled back


class Profile(exports.Profile):
    def set(self, value: str) -> None:
        statex.execute(
            "INSERT INTO kv (k, v) VALUES ('value', ?1) ON CONFLICT(k) DO UPDATE SET v = excluded.v", value
        )

    def get(self) -> Optional[str]:
        return statex.query_scalar("SELECT v FROM kv WHERE k = 'value'")
