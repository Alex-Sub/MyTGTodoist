ALTER TABLE subtasks ADD COLUMN source_msg_id TEXT NULL;
CREATE INDEX IF NOT EXISTS ix_subtasks_source_msg_id ON subtasks(source_msg_id);
