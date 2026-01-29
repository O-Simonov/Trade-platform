-- 0008_indexes_cleanup_asc_and_dedup.sql
-- Purpose:
--  - remove duplicate UNIQUE constraints/indexes (duplicates of PK)
--  - replace DESC-heavy indexes with ASC-friendly ones (append-friendly), keeping ability to scan backward
--  - add partial "5m fast" indexes
--  - drop unused/duplicate large indexes that were replaced
--  - tune autovacuum for hot tables
--
-- IMPORTANT: Uses CONCURRENTLY => must NOT run inside a transaction.

------------------------------------------------------------
-- 1) Drop duplicate UNIQUE constraints (keep PK only)
------------------------------------------------------------

-- positions: keep positions_pkey only
ALTER TABLE public.positions
  DROP CONSTRAINT IF EXISTS positions_exchange_account_symbol_uniq;

ALTER TABLE public.positions
  DROP CONSTRAINT IF EXISTS uq_positions_core;

-- position_ledger: keep position_ledger_pkey only
ALTER TABLE public.position_ledger
  DROP CONSTRAINT IF EXISTS ux_position_ledger;

-- ticker_24h: keep ticker_24h_pkey only
ALTER TABLE public.ticker_24h
  DROP CONSTRAINT IF EXISTS uq_ticker_24h;

------------------------------------------------------------
-- 2) Drop redundant unique indexes (funding / ledger) if exist
--    (must drop constraints above first if they were constraint-backed)
------------------------------------------------------------

DROP INDEX CONCURRENTLY IF EXISTS public.ux_funding_ex_sym_time;
DROP INDEX CONCURRENTLY IF EXISTS public.ux_funding_unique;

DROP INDEX CONCURRENTLY IF EXISTS public.ux_position_ledger_lifecycle;
DROP INDEX CONCURRENTLY IF EXISTS public.ux_position_ledger_uid_open;
DROP INDEX CONCURRENTLY IF EXISTS public.ux_posledger_ex_acc_uid_open;

------------------------------------------------------------
-- 3) POSITIONS cleanup: drop duplicate non-constraint indexes
------------------------------------------------------------

-- duplicate of (exchange_id, account_id)
DROP INDEX CONCURRENTLY IF EXISTS public.idx_positions_account;

-- duplicate of PK (exchange_id, account_id, symbol_id)
DROP INDEX CONCURRENTLY IF EXISTS public.ix_positions_ex_acc_sym;

------------------------------------------------------------
-- 4) POSITION_SNAPSHOTS: drop duplicate unique index (keep PK)
------------------------------------------------------------

DROP INDEX CONCURRENTLY IF EXISTS public.ux_possnap_ex_acc_sym;

------------------------------------------------------------
-- 5) Candles: replace DESC-fragmenting indexes with ASC-friendly ones
------------------------------------------------------------

-- build ASC "wm" index (append-friendly)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_candles_wm_asc
ON public.candles (exchange_id, "interval", symbol_id, open_time);

-- partial fast index for 5m (ASC)
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_candles_5m_fast_asc
ON public.candles (exchange_id, symbol_id, open_time)
WHERE "interval" = '5m';

-- drop old/duplicate ones
DROP INDEX CONCURRENTLY IF EXISTS public.idx_candles_wm;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_candles_scan;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_candles_lookup;
DROP INDEX CONCURRENTLY IF EXISTS public.ix_candles_5m_lookup;
DROP INDEX CONCURRENTLY IF EXISTS public.ix_candles_5m_fast;

------------------------------------------------------------
-- 6) Open interest: ASC-friendly index, drop DESC version and duplicates
------------------------------------------------------------

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_open_interest_ex_int_sym_ts_asc
ON public.open_interest (exchange_id, "interval", symbol_id, ts);

DROP INDEX CONCURRENTLY IF EXISTS public.ix_open_interest_ex_int_sym_ts;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_oi_lookup;

------------------------------------------------------------
-- 7) Candles trades agg: 5m partial ASC, drop old 5m lookup
------------------------------------------------------------

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_candles_trades_agg_5m_fast_asc
ON public.candles_trades_agg (exchange_id, symbol_id, open_time)
WHERE "interval" = '5m';

DROP INDEX CONCURRENTLY IF EXISTS public.ix_candles_trades_agg_5m_lookup;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_candles_trades_agg_lookup;

------------------------------------------------------------
-- 8) Liquidation events: ASC-friendly lookup, drop DESC version
------------------------------------------------------------

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_liq_events_lookup_asc
ON public.liquidation_events (exchange_id, symbol_id, ts);

DROP INDEX CONCURRENTLY IF EXISTS public.ix_liq_events_lookup;

------------------------------------------------------------
-- 9) Liquidation 1m: ASC-friendly bucket index, drop old
------------------------------------------------------------

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_liq_1m_lookup2_asc
ON public.liquidation_1m (exchange_id, symbol_id, bucket_ts);

DROP INDEX CONCURRENTLY IF EXISTS public.ix_liq_1m_lookup;
DROP INDEX CONCURRENTLY IF EXISTS public.ix_liq_1m_lookup2;
DROP INDEX CONCURRENTLY IF EXISTS public.ix_liquidation_1m_lookup;

------------------------------------------------------------
-- 10) Funding: ASC-friendly lookup, drop DESC version
------------------------------------------------------------

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_funding_lookup_asc
ON public.funding (exchange_id, symbol_id, funding_time);

DROP INDEX CONCURRENTLY IF EXISTS public.idx_funding_lookup;

------------------------------------------------------------
-- 11) Drop a few clearly redundant "0-scan" large indexes (as you did)
--     (kept: PK + minimal useful indexes)
------------------------------------------------------------

DROP INDEX CONCURRENTLY IF EXISTS public.ix_ticker_24h_symbol_close;
DROP INDEX CONCURRENTLY IF EXISTS public.ix_ticker_24h_symbol_time;
DROP INDEX CONCURRENTLY IF EXISTS public.ix_ticker_24h_ex_close;
DROP INDEX CONCURRENTLY IF EXISTS public.ix_ticker_24h_exchange_close_time;

DROP INDEX CONCURRENTLY IF EXISTS public.ix_market_state_5m_symbol_time;

-- duplicates for account_state / abs (you removed earlier)
DROP INDEX CONCURRENTLY IF EXISTS public.idx_account_state_lookup;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_balance_lookup;

-- optional: signals duplicate (if still exists)
DROP INDEX CONCURRENTLY IF EXISTS public.ix_signals_symbol_text_ts_desc;

------------------------------------------------------------
-- 12) Autovacuum tuning for hot tables
------------------------------------------------------------

ALTER TABLE public.funding SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_vacuum_threshold    = 2000,
  autovacuum_analyze_scale_factor= 0.02,
  autovacuum_analyze_threshold   = 2000
);

ALTER TABLE public.market_state_5m SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_vacuum_threshold    = 5000,
  autovacuum_analyze_scale_factor= 0.02,
  autovacuum_analyze_threshold   = 5000
);

------------------------------------------------------------
-- 13) Refresh planner stats
------------------------------------------------------------

ANALYZE public.candles;
ANALYZE public.open_interest;
ANALYZE public.candles_trades_agg;
ANALYZE public.liquidation_events;
ANALYZE public.liquidation_1m;
ANALYZE public.funding;

ANALYZE public.positions;
ANALYZE public.position_ledger;
ANALYZE public.position_snapshots;
ANALYZE public.ticker_24h;
ANALYZE public.market_state_5m;
ANALYZE public.account_state;
ANALYZE public.account_balance_snapshots;
