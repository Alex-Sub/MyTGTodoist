CREATE TABLE IF NOT EXISTS user_nudges (
    user_id TEXT NOT NULL,
    nudge_key TEXT NOT NULL,
    next_at TEXT NOT NULL,
    last_shown_at TEXT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(user_id, nudge_key)
);
