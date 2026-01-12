-- db_check_v4.sql
-- Quick sanity checks for Trade-platform PostgreSQL schema
\set ON_ERROR_STOP on

\echo '=== TABLES ==='
\dt

\echo ''
\echo '=== BASIC COUNTS + LAST UPDATE ==='

SELECT 'exchanges' t, count(*) cnt FROM exchanges;
SELECT 'accounts' t, count(*) cnt FROM accounts;

SELECT
  'symbols' t,
  count(*) cnt,
  sum(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_cnt,
  max(last_seen_at) AS last_seen
FROM symbols;

SELECT
  'symbol_filters' t,
  count(*) cnt,
  max(updated_at) AS last_upd
FROM symbol_filters;

SELECT
  'positions' t,
  count(*) cnt,
  max(updated_at) AS last_upd
FROM positions;

SELECT
  'position_snapshots' t,
  count(*) cnt,
  max(updated_at) AS last_upd
FROM position_snapshots;

SELECT
  'position_ledger' t,
  count(*) cnt,
  max(updated_at) AS last_upd
FROM position_ledger;

SELECT
  'orders' t,
  count(*) cnt,
  max(updated_at) AS last_upd,
  sum(CASE WHEN status NOT IN ('FILLED','CANCELED','REJECTED','EXPIRED') THEN 1 ELSE 0 END) AS non_terminal
FROM orders;

SELECT
  'order_events' t,
  count(*) cnt,
  max(recv_ts) AS last_recv
FROM order_events;

SELECT
  'trades' t,
  count(*) cnt,
  max(ts) AS last_ts
FROM trades;

SELECT
  'order_fills' t,
  count(*) cnt,
  max(ts) AS last_ts
FROM order_fills;

SELECT
  'account_state' t,
  count(*) cnt,
  max(ts) AS last_ts
FROM account_state;

SELECT
  'account_balance_snapshots' t,
  count(*) cnt,
  max(ts) AS last_ts
FROM account_balance_snapshots;

SELECT
  'candles' t,
  count(*) cnt,
  max(open_time) AS last_open_time
FROM candles;

SELECT
  'funding' t,
  count(*) cnt,
  max(funding_time) AS last_funding_time
FROM funding;

SELECT
  'open_interest' t,
  count(*) cnt,
  max(ts) AS last_ts
FROM open_interest;

SELECT
  'ticker_24h' t,
  count(*) cnt,
  max(close_time) AS last_close
FROM ticker_24h;

\echo ''
\echo '=== REQUIRED COLUMNS (quick sanity) ==='

SELECT 'positions.fees' AS col, EXISTS (
  SELECT 1
  FROM information_schema.columns
  WHERE table_schema='public' AND table_name='positions' AND column_name='fees'
) AS ok;

SELECT 'positions.last_trade_id' AS col, EXISTS (
  SELECT 1
  FROM information_schema.columns
  WHERE table_schema='public' AND table_name='positions' AND column_name='last_trade_id'
) AS ok;

SELECT 'positions.last_ts' AS col, EXISTS (
  SELECT 1
  FROM information_schema.columns
  WHERE table_schema='public' AND table_name='positions' AND column_name='last_ts'
) AS ok;

SELECT 'order_events.strategy_id' AS col, EXISTS (
  SELECT 1
  FROM information_schema.columns
  WHERE table_schema='public' AND table_name='order_events' AND column_name='strategy_id'
) AS ok;

SELECT 'order_events.pos_uid' AS col, EXISTS (
  SELECT 1
  FROM information_schema.columns
  WHERE table_schema='public' AND table_name='order_events' AND column_name='pos_uid'
) AS ok;

SELECT 'positions_snapshot view' AS table, EXISTS (
  SELECT 1 FROM information_schema.views
  WHERE table_schema='public' AND table_name='positions_snapshot'
) AS ok;
