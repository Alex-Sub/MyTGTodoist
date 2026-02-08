CREATE TABLE IF NOT EXISTS directions (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    note TEXT NULL,
    status TEXT NOT NULL,
    source_msg_id TEXT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projects (
    id INTEGER PRIMARY KEY,
    direction_id INTEGER NULL,
    title TEXT NOT NULL,
    status TEXT NOT NULL,
    source_msg_id TEXT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    closed_at TEXT NULL,
    FOREIGN KEY(direction_id) REFERENCES directions(id)
);

CREATE INDEX IF NOT EXISTS ix_projects_direction_id ON projects(direction_id);

CREATE TABLE IF NOT EXISTS cycles (
    id INTEGER PRIMARY KEY,
    type TEXT NOT NULL,
    period_key TEXT NULL,
    period_start TEXT NULL,
    period_end TEXT NULL,
    status TEXT NOT NULL,
    summary TEXT NULL,
    source_msg_id TEXT NULL UNIQUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    closed_at TEXT NULL
);

CREATE TABLE IF NOT EXISTS cycle_outcomes (
    id INTEGER PRIMARY KEY,
    cycle_id INTEGER NOT NULL,
    kind TEXT NOT NULL,
    text TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(cycle_id) REFERENCES cycles(id)
);

CREATE INDEX IF NOT EXISTS ix_cycle_outcomes_cycle_id ON cycle_outcomes(cycle_id);
