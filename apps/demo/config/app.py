from typing import List, Optional
from wit_world import exports
from wit_world.exports.config import Setting
from wit_world.imports import kv          # import the module, not functions, so tests can stub it
import statex

DEFAULTS = "defaults"                      # the kv actor holding shared defaults

def _bump() -> int:
    statex.execute("UPDATE meta SET version = version + 1 WHERE id = 0")
    return statex.query_scalar("SELECT version FROM meta WHERE id = 0")

class Config(exports.Config):
    def set(self, name: str, value: str) -> int:
        statex.execute("INSERT INTO overrides (name, value) VALUES (?1, ?2) "
                       "ON CONFLICT(name) DO UPDATE SET value = excluded.value", name, value)
        return _bump()

    def get(self, name: str) -> Optional[str]:
        return statex.query_scalar(
            "SELECT value FROM overrides WHERE name = ?1 "
            "UNION ALL SELECT value FROM defaults WHERE name = ?1 LIMIT 1", name)

    def all(self) -> List[Setting]:
        rows = statex.query(
            "SELECT name, value, 'override' FROM overrides UNION ALL "
            "SELECT name, value, 'default' FROM defaults "
            "WHERE name NOT IN (SELECT name FROM overrides) ORDER BY 1")
        return [Setting(name=n, value=v, source=s) for n, v, s in rows]

    def refresh(self) -> int:
        prefix = statex.key() + "/"
        # 1. Call kv first. A failed call raises statex.Err(call_error).
        try:
            entries = kv.scan(DEFAULTS, prefix)
        except statex.Err as e:
            raise statex.Err(statex.describe(e.value))   # rolls back: our state is untouched
        # 2. Then update our own state. It all commits together when we return.
        statex.execute("DELETE FROM defaults")
        for e in entries:
            statex.execute("INSERT INTO defaults (name, value) VALUES (?1, ?2)",
                           e.key[len(prefix):], e.value)
        statex.execute("UPDATE meta SET refreshed_at = datetime('now') WHERE id = 0")
        return _bump()
