CREATE TABLE IF NOT EXISTS cycle_goals (
    id INTEGER PRIMARY KEY,
    cycle_id INTEGER NOT NULL,
    text TEXT NOT NULL,
    status TEXT NOT NULL,
    continued_from_goal_id INTEGER NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(cycle_id) REFERENCES cycles(id)
);

CREATE INDEX IF NOT EXISTS ix_cycle_goals_cycle_id ON cycle_goals(cycle_id);
