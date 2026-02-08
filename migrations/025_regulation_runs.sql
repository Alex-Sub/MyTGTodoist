CREATE TABLE IF NOT EXISTS regulation_runs (
    id INTEGER PRIMARY KEY,
    regulation_id INTEGER NOT NULL,
    period_key TEXT NOT NULL,
    status TEXT NOT NULL,
    due_date TEXT NOT NULL,
    due_time_local TEXT NULL,
    done_at TEXT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(regulation_id) REFERENCES regulations(id),
    UNIQUE(regulation_id, period_key)
);

CREATE INDEX IF NOT EXISTS ix_regulation_runs_period_key ON regulation_runs(period_key);
CREATE INDEX IF NOT EXISTS ix_regulation_runs_regulation_id ON regulation_runs(regulation_id);
