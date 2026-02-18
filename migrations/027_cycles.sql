-- Strategic Layer v1: extend cycles with runtime planning fields.
-- Table `cycles` is created earlier in 018; here we add fields used by strategic UX.
ALTER TABLE cycles ADD COLUMN name TEXT NOT NULL DEFAULT '';
ALTER TABLE cycles ADD COLUMN start_date TEXT NOT NULL DEFAULT '1970-01-01';
ALTER TABLE cycles ADD COLUMN end_date TEXT NOT NULL DEFAULT '1970-01-01';

-- Backfill dates from legacy period fields when possible.
UPDATE cycles
SET start_date = COALESCE(NULLIF(period_start, ''), start_date)
WHERE start_date = '1970-01-01';

UPDATE cycles
SET end_date = COALESCE(NULLIF(period_end, ''), end_date)
WHERE end_date = '1970-01-01';

CREATE INDEX IF NOT EXISTS ix_cycles_closed_at ON cycles(closed_at);
CREATE INDEX IF NOT EXISTS ix_cycles_start_date ON cycles(start_date);
CREATE INDEX IF NOT EXISTS ix_cycles_end_date ON cycles(end_date);
