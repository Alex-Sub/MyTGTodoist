CREATE TABLE IF NOT EXISTS runtime_command_dedup (
    idempotency_key TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    intent TEXT NOT NULL,
    state TEXT NOT NULL,
    response_json TEXT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
