-- db_pulse.sql (под твою реальную схему)
SELECT 'candles' t, COUNT(*) rows, MAX(open_time) last_ts FROM candles;
SELECT 'funding' t, COUNT(*) rows, MAX(funding_time) last_ts FROM funding;
SELECT 'open_interest' t, COUNT(*) rows, MAX(ts) last_ts FROM open_interest;
SELECT 'ticker_24h' t, COUNT(*) rows, MAX(close_time) last_ts FROM ticker_24h;
SELECT 'price_snapshots' t, COUNT(*) rows, MAX(ts) last_ts FROM price_snapshots;

SELECT 'market_trades' t, COUNT(*) rows, MAX(ts) last_ts FROM market_trades;
SELECT 'candles_trades_agg' t, COUNT(*) rows, MAX(open_time) last_ts FROM candles_trades_agg;
SELECT 'market_state_5m' t, COUNT(*) rows, MAX(open_time) last_ts FROM market_state_5m;

SELECT 'liquidation_events' t, COUNT(*) rows, MAX(ts) last_ts FROM liquidation_events;
SELECT 'liquidation_1m' t, COUNT(*) rows, MAX(bucket_ts) last_ts FROM liquidation_1m;


-- LIVENESS (обновляется ли прямо сейчас)
SELECT COUNT(*) AS candles_5m_last_hour
FROM candles
WHERE interval='5m' AND open_time >= (now() - interval '1 hour');

SELECT COUNT(*) AS market_trades_last_5m
FROM market_trades
WHERE ts >= (now() - interval '5 minutes');

SELECT COUNT(*) AS liq_events_last_10m
FROM liquidation_events
WHERE ts >= (now() - interval '10 minutes');
