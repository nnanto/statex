CREATE TABLE counter (id INTEGER PRIMARY KEY CHECK (id = 0), value INTEGER NOT NULL);
INSERT INTO counter (id, value) VALUES (0, 0);
