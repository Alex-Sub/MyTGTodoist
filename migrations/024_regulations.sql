CREATE TABLE IF NOT EXISTS regulations (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    note TEXT NULL,
    status TEXT NOT NULL,
    day_of_month INTEGER NOT NULL,
    due_time_local TEXT NULL,
    source_msg_id TEXT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_regulations_status ON regulations(status);
