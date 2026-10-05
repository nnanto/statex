-- Applied once per actor, inside a transaction, the first time the actor is used.
CREATE TABLE stats (id INTEGER PRIMARY KEY CHECK (id = 0), calls INTEGER NOT NULL);
INSERT INTO stats (id, calls) VALUES (0, 0);
