CREATE TABLE IF NOT EXISTS nudge_ack (
    id INTEGER PRIMARY KEY,
    user_id TEXT NOT NULL,
    nudge_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    acked_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(user_id, nudge_type, entity_type, entity_id)
);

CREATE INDEX IF NOT EXISTS ix_nudge_ack_user_id ON nudge_ack(user_id);
CREATE INDEX IF NOT EXISTS ix_nudge_ack_acked_at ON nudge_ack(acked_at);
