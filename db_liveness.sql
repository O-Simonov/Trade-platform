SELECT COUNT(*) AS candles_5m_last_hour
FROM candles
WHERE interval='5m' AND open_time >= (now() - interval '1 hour');

SELECT COUNT(*) AS market_trades_last_5m
FROM market_trades
WHERE ts >= (now() - interval '5 minutes');

SELECT COUNT(*) AS liq_events_last_10m
FROM liquidation_events
WHERE ts >= (now() - interval '10 minutes');
