ALTER TABLE tasks ADD COLUMN parent_type TEXT NULL;
ALTER TABLE tasks ADD COLUMN parent_id INTEGER NULL;

CREATE INDEX IF NOT EXISTS ix_tasks_parent ON tasks(parent_type, parent_id);
