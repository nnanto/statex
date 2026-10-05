CREATE TABLE overrides (name TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE defaults  (name TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE meta (id INTEGER PRIMARY KEY CHECK (id = 0), version INTEGER NOT NULL, refreshed_at TEXT);
INSERT INTO meta (id, version) VALUES (0, 0);
