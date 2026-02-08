ALTER TABLE tasks ADD COLUMN planned_at TEXT NULL;

CREATE INDEX IF NOT EXISTS ix_tasks_planned_at ON tasks(planned_at);
