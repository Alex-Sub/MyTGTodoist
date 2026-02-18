ALTER TABLE tasks ADD COLUMN goal_id INTEGER NULL REFERENCES goals(id);

CREATE INDEX IF NOT EXISTS ix_tasks_goal_id ON tasks(goal_id);
