-- Applied once per actor, inside a transaction, the first time the actor is used.
CREATE TABLE config (id INTEGER PRIMARY KEY CHECK (id = 0), value INTEGER NOT NULL);
INSERT INTO config (id, value) VALUES (0, 0);
