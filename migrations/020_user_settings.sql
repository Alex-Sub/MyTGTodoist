CREATE TABLE IF NOT EXISTS user_settings (
    user_id TEXT PRIMARY KEY,
    signals_enabled INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
