-- P2 schema fix: parent_id integer (soft migrate if existing TEXT)
-- Idempotent-ish: safe to run multiple times.
ALTER TABLE items ADD COLUMN parent_id INTEGER;
ALTER TABLE items ADD COLUMN parent_id_int INTEGER;
UPDATE items SET parent_id_int = CAST(parent_id AS INTEGER) WHERE parent_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_items_parent_id ON items(parent_id);
CREATE INDEX IF NOT EXISTS ix_items_parent_id_int ON items(parent_id_int);
