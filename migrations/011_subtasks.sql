CREATE TABLE IF NOT EXISTS subtasks (
    id INTEGER PRIMARY KEY,
    task_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(id)
);

CREATE INDEX IF NOT EXISTS ix_subtasks_task_id ON subtasks(task_id);
CREATE INDEX IF NOT EXISTS ix_subtasks_status ON subtasks(status);
