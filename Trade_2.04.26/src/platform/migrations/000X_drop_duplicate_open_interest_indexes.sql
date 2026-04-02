BEGIN;

DROP INDEX IF EXISTS idx_oi_lookup;
DROP INDEX IF EXISTS ix_open_interest_5m_last;
DROP INDEX IF EXISTS ix_open_interest_lookup2;

COMMIT;
