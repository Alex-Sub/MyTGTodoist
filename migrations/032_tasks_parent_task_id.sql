ALTER TABLE tasks ADD COLUMN parent_task_id INTEGER NULL REFERENCES tasks(id);

CREATE INDEX IF NOT EXISTS ix_tasks_parent_task_id ON tasks(parent_task_id);
