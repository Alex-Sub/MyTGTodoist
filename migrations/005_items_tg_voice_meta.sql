ALTER TABLE items ADD COLUMN tg_update_id INTEGER;
ALTER TABLE items ADD COLUMN tg_voice_file_id TEXT;
ALTER TABLE items ADD COLUMN tg_voice_unique_id TEXT;
ALTER TABLE items ADD COLUMN tg_voice_duration INTEGER;
ALTER TABLE items ADD COLUMN asr_text TEXT;
CREATE INDEX IF NOT EXISTS ix_items_tg_voice_unique_id ON items(tg_voice_unique_id);
