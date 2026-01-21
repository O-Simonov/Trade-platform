-- 0005_views_ts_compat.sql
-- унификация "ts/bucket_ts" для мониторинга/аналитики

CREATE OR REPLACE VIEW v_funding AS
SELECT *, funding_time AS ts
FROM funding;

CREATE OR REPLACE VIEW v_ticker_24h AS
SELECT *, close_time AS ts
FROM ticker_24h;

CREATE OR REPLACE VIEW v_candles_trades_agg AS
SELECT *, open_time AS bucket_ts
FROM candles_trades_agg;

CREATE OR REPLACE VIEW v_market_state_5m AS
SELECT *, open_time AS bucket_ts
FROM market_state_5m;

CREATE OR REPLACE VIEW v_liquidation_1m AS
SELECT *, bucket_ts AS ts
FROM liquidation_1m;

CREATE OR REPLACE VIEW v_order_events AS
SELECT *,
       to_timestamp(ts_ms/1000.0) AS ts
FROM order_events;
