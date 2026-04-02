\echo '=== TABLES ==='
\dt

\echo '=== BASIC COUNTS + LAST UPDATE ==='
SELECT 'exchanges' t, count(*) cnt FROM exchanges;
SELECT 'accounts' t, count(*) cnt FROM accounts;
SELECT 'symbols' t, count(*) cnt, count(*) FILTER (WHERE is_active) active_cnt, max(last_seen_at) last_seen FROM symbols;
SELECT 'symbol_filters' t, count(*) cnt, max(updated_at) last_upd FROM symbol_filters;

SELECT 'positions' t, count(*) cnt, max(updated_at) last_upd FROM positions;
SELECT 'position_snapshots' t, count(*) cnt, max(updated_at) last_upd FROM position_snapshots;
SELECT 'position_ledger' t, count(*) cnt, max(updated_at) last_upd FROM position_ledger;

SELECT 'orders' t, count(*) cnt, max(updated_at) last_upd, count(*) FILTER (WHERE status NOT IN ('FILLED','CANCELED','REJECTED','EXPIRED')) non_terminal FROM orders;
SELECT 'order_events' t, count(*) cnt, max(recv_ts) last_recv FROM order_events;
SELECT 'trades' t, count(*) cnt, max(ts) last_ts FROM trades;
SELECT 'order_fills' t, count(*) cnt, max(ts) last_ts FROM order_fills;

SELECT 'account_state' t, count(*) cnt, max(updated_at) last_upd FROM account_state;
SELECT 'account_balance_snapshots' t, count(*) cnt, max(ts) last_ts FROM account_balance_snapshots;

SELECT 'candles' t, count(*) cnt, max(open_time) last_open_time FROM candles;
SELECT 'funding' t, count(*) cnt, max(funding_time) last_funding_time FROM funding;
SELECT 'open_interest' t, count(*) cnt, max(ts) last_ts FROM open_interest;
SELECT 'ticker_24h' t, count(*) cnt, max(close_time) last_close FROM ticker_24h;

\echo '=== REQUIRED COLUMNS (quick sanity) ==='
SELECT 'positions.fees' AS col, EXISTS(
  SELECT 1 FROM information_schema.columns WHERE table_name='positions' AND column_name='fees'
) ok;
SELECT 'positions.last_trade_id' AS col, EXISTS(
  SELECT 1 FROM information_schema.columns WHERE table_name='positions' AND column_name='last_trade_id'
) ok;
SELECT 'positions.last_ts' AS col, EXISTS(
  SELECT 1 FROM information_schema.columns WHERE table_name='positions' AND column_name='last_ts'
) ok;
SELECT 'order_events.strategy_id' AS col, EXISTS(
  SELECT 1 FROM information_schema.columns WHERE table_name='order_events' AND column_name='strategy_id'
) ok;
SELECT 'order_events.pos_uid' AS col, EXISTS(
  SELECT 1 FROM information_schema.columns WHERE table_name='order_events' AND column_name='pos_uid'
) ok;
SELECT 'account_state' AS table, EXISTS(
  SELECT 1 FROM information_schema.tables WHERE table_name='account_state'
) ok;
SELECT 'position_snapshots' AS table, EXISTS(
  SELECT 1 FROM information_schema.tables WHERE table_name='position_snapshots'
) ok;
