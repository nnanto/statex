CREATE TABLE alarm_demo (
  id INTEGER PRIMARY KEY CHECK (id = 0),
  fired INTEGER NOT NULL,
  fail_times INTEGER NOT NULL
);
INSERT INTO alarm_demo (id, fired, fail_times) VALUES (0, 0, 0);
