CREATE TABLE IF NOT EXISTS goals (
    id INTEGER PRIMARY KEY,
    cycle_id INTEGER NOT NULL REFERENCES cycles(id),
    title TEXT NOT NULL,
    success_criteria TEXT NOT NULL,
    planned_end_date TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','DONE','DROPPED')),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at TEXT NULL
);

CREATE INDEX IF NOT EXISTS ix_goals_cycle_id ON goals(cycle_id);
CREATE INDEX IF NOT EXISTS ix_goals_status ON goals(status);
CREATE INDEX IF NOT EXISTS ix_goals_planned_end_date ON goals(planned_end_date);
