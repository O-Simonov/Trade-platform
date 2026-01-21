-- 0007_drop_duplicate_market_state_indexes.sql
-- Drop duplicated index: ix_market_state_5m_lookup
-- because it's the same as ix_market_state_5m_symbol_time

BEGIN;

DROP INDEX IF EXISTS public.ix_market_state_5m_lookup;

COMMIT;
