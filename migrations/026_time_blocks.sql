CREATE TABLE IF NOT EXISTS time_blocks (
    id INTEGER PRIMARY KEY,
    task_id INTEGER NOT NULL,
    start_at TEXT NOT NULL,
    end_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(task_id) REFERENCES tasks(id)
);

CREATE INDEX IF NOT EXISTS ix_time_blocks_task_id ON time_blocks(task_id);
CREATE INDEX IF NOT EXISTS ix_time_blocks_start_at ON time_blocks(start_at);
