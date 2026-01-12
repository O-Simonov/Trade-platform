-- 0002: bring schema in sync with storage.py upserts (orders/trades snapshots)
-- Safe to run multiple times.
ALTER TABLE IF EXISTS orders
  ADD COLUMN IF NOT EXISTS ts_ms BIGINT,
  ADD COLUMN IF NOT EXISTS raw_json TEXT;

ALTER TABLE IF EXISTS trades
  ADD COLUMN IF NOT EXISTS raw_json TEXT;

-- Optional: set defaults (won't overwrite existing values)
ALTER TABLE IF EXISTS orders
  ALTER COLUMN created_at SET DEFAULT NOW(),
  ALTER COLUMN updated_at SET DEFAULT NOW();

