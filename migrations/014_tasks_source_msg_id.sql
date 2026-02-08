ALTER TABLE tasks ADD COLUMN source_msg_id TEXT NULL;
CREATE INDEX IF NOT EXISTS ix_tasks_source_msg_id ON tasks(source_msg_id);
