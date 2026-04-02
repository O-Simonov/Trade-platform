-- 0003_market_state_5m_indexes.sql
-- Indexes for faster retention + queries

BEGIN;

-- ускоряет:
-- - cleanup_market_state_5m(exchange_id, keep_days)
-- - выборки по истории
-- - агрегации по времени
CREATE INDEX IF NOT EXISTS ix_market_state_5m_ex_open_time
ON market_state_5m (exchange_id, open_time);

COMMIT;
