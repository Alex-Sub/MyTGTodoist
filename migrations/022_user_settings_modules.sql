ALTER TABLE user_settings ADD COLUMN overload_enabled INTEGER NOT NULL DEFAULT 0;
ALTER TABLE user_settings ADD COLUMN drift_enabled INTEGER NOT NULL DEFAULT 0;

UPDATE user_settings
SET overload_enabled = COALESCE(overload_enabled, signals_enabled),
    drift_enabled = COALESCE(drift_enabled, signals_enabled);
