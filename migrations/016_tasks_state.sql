-- add task state (P3)
ALTER TABLE tasks ADD COLUMN state TEXT NOT NULL DEFAULT 'NEW';
CREATE INDEX IF NOT EXISTS ix_tasks_state ON tasks(state);
