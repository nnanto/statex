-- Applied once per actor, inside a transaction, the first time the actor is used.
CREATE TABLE counter (id INTEGER PRIMARY KEY CHECK (id = 0), value INTEGER NOT NULL);
INSERT INTO counter (id, value) VALUES (0, 0);
