from typing import List, Optional

from wit_world import exports
from wit_world.exports.kv import Entry

import statex


# The class name must match the WIT interface name in PascalCase: bucket -> Bucket.
# Every method call is one transaction on this bucket's own SQLite DB:
# returning commits, raising rolls back.
class Kv(exports.Kv):
    def put(self, key: str, value: str) -> None:
        statex.execute(
            "INSERT INTO kv (key, value) VALUES (?1, ?2) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            key, value,
        )

    def get(self, key: str) -> Optional[str]:
        return statex.query_scalar("SELECT value FROM kv WHERE key = ?1", key)

    def delete(self, key: str) -> bool:
        return statex.execute("DELETE FROM kv WHERE key = ?1", key) > 0

    def scan(self, prefix: str) -> List[Entry]:
        rows = statex.query(
            "SELECT key, value FROM kv WHERE substr(key, 1, length(?1)) = ?1 "
            "ORDER BY key LIMIT 100",
            prefix,
        )
        return [Entry(key=k, value=v) for k, v in rows]
