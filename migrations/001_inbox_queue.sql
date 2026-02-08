CREATE TABLE IF NOT EXISTS inbox_queue (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    tg_chat_id INTEGER,
    tg_update_id INTEGER,
    tg_message_id INTEGER,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'NEW',
    priority INTEGER NOT NULL DEFAULT 100,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    claimed_by TEXT NULL,
    claimed_at TEXT NULL,
    lease_until TEXT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    ingested_at TEXT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_inbox_queue_source_chat_update
ON inbox_queue(source, tg_chat_id, tg_update_id);
