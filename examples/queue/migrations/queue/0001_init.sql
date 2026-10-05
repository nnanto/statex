CREATE TABLE configuration (
    id INTEGER PRIMARY KEY CHECK(id = 0),
    max_batch_size INTEGER NOT NULL DEFAULT 100,
    batch_timeout_ms INTEGER NOT NULL DEFAULT 1000,
    max_retries INTEGER NOT NULL DEFAULT 3,
    retry_delay_ms INTEGER NOT NULL DEFAULT 1000,
    lease_ms INTEGER NOT NULL DEFAULT 30000,
    max_concurrency INTEGER NOT NULL DEFAULT 4,
    consumer_key TEXT DEFAULT 'sample',
    dlq_key TEXT,
    paused INTEGER NOT NULL DEFAULT 0,
    sequence INTEGER NOT NULL DEFAULT 0
);
INSERT INTO configuration(id) VALUES(0);

-- Receipts deliberately outlive the four-day payload retention window.
CREATE TABLE receipts (id TEXT PRIMARY KEY);
CREATE TABLE batches (token TEXT PRIMARY KEY, deadline INTEGER NOT NULL);
CREATE TABLE messages (
    id TEXT PRIMARY KEY REFERENCES receipts(id),
    body BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    available_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    token TEXT REFERENCES batches(token)
);
CREATE INDEX messages_ready ON messages(token, available_at, id);
CREATE INDEX messages_expiry ON messages(expires_at);
CREATE INDEX batches_deadline ON batches(deadline);
