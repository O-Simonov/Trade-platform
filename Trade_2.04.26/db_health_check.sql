-- db_health_check.sql
-- Universal DB health check (does NOT assume ts column name)

\pset pager off
\timing on
\set ON_ERROR_STOP on

SELECT now() AS server_now_utc;

-- ------------------------------------------------------------
-- Helper: return MAX(timestamp_col) for the first existing col
-- ------------------------------------------------------------
CREATE OR REPLACE FUNCTION public._last_ts(p_table regclass, p_cols text[])
RETURNS timestamptz
LANGUAGE plpgsql
AS $$
DECLARE
  c text;
  v timestamptz;
BEGIN
  FOREACH c IN ARRAY p_cols LOOP
    IF EXISTS (
      SELECT 1
      FROM pg_attribute
      WHERE attrelid = p_table
        AND attname = c
        AND NOT attisdropped
    ) THEN
      EXECUTE format('SELECT max(%I)::timestamptz FROM %s', c, p_table) INTO v;
      RETURN v;
    END IF;
  END LOOP;

  RETURN NULL;
END $$;


-- ------------------------------------------------------------
-- 1) Main tables: row count + last_ts + lag (auto-detected)
-- ------------------------------------------------------------

-- funding
WITH x AS (
  SELECT public._last_ts('funding', ARRAY['ts','funding_time','time','event_time','updated_at','created_at']) AS last_ts
)
SELECT
  'funding' AS t,
  (SELECT COUNT(*) FROM funding) AS rows,
  x.last_ts,
  now() - x.last_ts AS lag
FROM x;

-- open_interest (ts exists ✅)
WITH x AS (
  SELECT public._last_ts('open_interest', ARRAY['ts','time','event_time','updated_at','created_at']) AS last_ts
)
SELECT
  'open_interest' AS t,
  (SELECT COUNT(*) FROM open_interest) AS rows,
  x.last_ts,
  now() - x.last_ts AS lag
FROM x;

-- ticker_24h (NO ts ✅ -> close_time/open_time)
WITH x AS (
  SELECT public._last_ts('ticker_24h', ARRAY['close_time','open_time','ts','time','event_time','updated_at','created_at']) AS last_ts
)
SELECT
  'ticker_24h' AS t,
  (SELECT COUNT(*) FROM ticker_24h) AS rows,
  x.last_ts,
  CASE WHEN x.last_ts IS NULL THEN NULL ELSE now() - x.last_ts END AS lag
FROM x;

-- market_trades
WITH x AS (
  SELECT public._last_ts('market_trades', ARRAY['ts','time','event_time','created_at']) AS last_ts
)
SELECT
  'market_trades' AS t,
  (SELECT COUNT(*) FROM market_trades) AS rows,
  x.last_ts,
  CASE WHEN x.last_ts IS NULL THEN NULL ELSE now() - x.last_ts END AS lag
FROM x;

-- liquidation_events
WITH x AS (
  SELECT public._last_ts('liquidation_events', ARRAY['ts','time','event_time','created_at']) AS last_ts
)
SELECT
  'liquidation_events' AS t,
  (SELECT COUNT(*) FROM liquidation_events) AS rows,
  x.last_ts,
  CASE WHEN x.last_ts IS NULL THEN NULL ELSE now() - x.last_ts END AS lag
FROM x;

-- candles 5m (open_time / ts)
WITH x AS (
  SELECT public._last_ts('candles', ARRAY['open_time','ts','time','event_time','created_at']) AS last_ts
)
SELECT
  'candles_5m' AS t,
  (SELECT COUNT(*) FROM candles WHERE interval='5m') AS rows,
  x.last_ts AS last_open_time,
  CASE WHEN x.last_ts IS NULL THEN NULL ELSE now() - x.last_ts END AS lag
FROM x;


-- ------------------------------------------------------------
-- 2) Open Interest: lag by interval (global)
-- ------------------------------------------------------------
SELECT
  interval,
  MAX(ts) AS last_ts,
  now() - MAX(ts) AS lag
FROM open_interest
GROUP BY interval
ORDER BY interval;


-- ------------------------------------------------------------
-- 3) Open Interest: lag stats for 5m / 15m (all symbols)
-- ------------------------------------------------------------
WITH last AS (
  SELECT symbol_id, MAX(ts) AS last_ts
  FROM open_interest
  WHERE interval = '5m'
  GROUP BY symbol_id
)
SELECT
  'oi_lag_5m' AS t,
  COUNT(*) AS symbols,
  MIN(now() - last_ts) AS best_lag,
  AVG(now() - last_ts) AS avg_lag,
  MAX(now() - last_ts) AS worst_lag
FROM last;

WITH last AS (
  SELECT symbol_id, MAX(ts) AS last_ts
  FROM open_interest
  WHERE interval = '15m'
  GROUP BY symbol_id
)
SELECT
  'oi_lag_15m' AS t,
  COUNT(*) AS symbols,
  MIN(now() - last_ts) AS best_lag,
  AVG(now() - last_ts) AS avg_lag,
  MAX(now() - last_ts) AS worst_lag
FROM last;


-- ------------------------------------------------------------
-- 4) Worst symbols (ACTIVE) 5m / 15m
-- ------------------------------------------------------------
WITH last AS (
  SELECT symbol_id, MAX(ts) AS last_ts
  FROM open_interest
  WHERE interval='5m'
  GROUP BY symbol_id
)
SELECT
  s.symbol,
  now() - l.last_ts AS lag,
  l.last_ts
FROM last l
JOIN symbols s ON s.symbol_id = l.symbol_id
WHERE s.is_active = true
ORDER BY lag DESC
LIMIT 30;

WITH last AS (
  SELECT symbol_id, MAX(ts) AS last_ts
  FROM open_interest
  WHERE interval='15m'
  GROUP BY symbol_id
)
SELECT
  s.symbol,
  now() - l.last_ts AS lag,
  l.last_ts
FROM last l
JOIN symbols s ON s.symbol_id = l.symbol_id
WHERE s.is_active = true
ORDER BY lag DESC
LIMIT 30;


-- ------------------------------------------------------------
-- 5) Is it writing now? quick counters
-- ------------------------------------------------------------

SELECT
  'oi_5m_last_10m' AS t,
  COUNT(*) AS rows
FROM open_interest
WHERE interval='5m'
  AND ts >= now() - interval '10 minutes';

SELECT
  'oi_15m_last_1h' AS t,
  COUNT(*) AS rows
FROM open_interest
WHERE interval='15m'
  AND ts >= now() - interval '1 hour';

SELECT
  'market_trades_last_5m' AS t,
  COUNT(*) AS rows
FROM market_trades
WHERE ts >= now() - interval '5 minutes';

SELECT
  'liq_events_last_10m' AS t,
  COUNT(*) AS rows
FROM liquidation_events
WHERE ts >= now() - interval '10 minutes';

SELECT
  'candles_5m_last_hour' AS t,
  COUNT(*) AS rows
FROM candles
WHERE interval='5m'
  AND open_time >= now() - interval '1 hour';


-- ------------------------------------------------------------
-- 5.5) Open Interest: new rows in last 5 minutes (by interval)
-- ------------------------------------------------------------

SELECT
  'oi_new_rows_last_5m_by_interval' AS t,
  interval,
  COUNT(*) AS rows
FROM public.open_interest
WHERE ts >= now() - interval '5 minutes'
GROUP BY interval
ORDER BY interval;

-- Same but only ACTIVE symbols (recommended)
SELECT
  'oi_new_rows_last_5m_active_only' AS t,
  oi.interval,
  COUNT(*) AS rows
FROM public.open_interest oi
JOIN public.symbols s ON s.symbol_id = oi.symbol_id
WHERE s.is_active = true
  AND oi.ts >= now() - interval '5 minutes'
GROUP BY oi.interval
ORDER BY oi.interval;


-- ------------------------------------------------------------
-- 5.6) Which ACTIVE symbols write OI now? (top 20)
-- ------------------------------------------------------------
SELECT
  'oi_active_top_now' AS t,
  s.symbol,
  oi.interval,
  COUNT(*) AS rows_last_30m,
  MAX(oi.ts) AS last_ts
FROM public.open_interest oi
JOIN public.symbols s ON s.symbol_id = oi.symbol_id
WHERE s.is_active = true
  AND oi.ts >= now() - interval '30 minutes'
GROUP BY s.symbol, oi.interval
ORDER BY rows_last_30m DESC, last_ts DESC
LIMIT 20;


-- ------------------------------------------------------------
-- 5.7) Lag by interval only for ACTIVE symbols
-- ------------------------------------------------------------
WITH last_active AS (
  SELECT
    oi.interval,
    oi.symbol_id,
    MAX(oi.ts) AS last_ts
  FROM public.open_interest oi
  JOIN public.symbols s ON s.symbol_id = oi.symbol_id
  WHERE s.is_active = true
  GROUP BY oi.interval, oi.symbol_id
)
SELECT
  'oi_lag_active_by_interval' AS t,
  interval,
  COUNT(*) AS symbols,
  MIN(now() - last_ts) AS best_lag,
  AVG(now() - last_ts) AS avg_lag,
  MAX(now() - last_ts) AS worst_lag
FROM last_active
GROUP BY interval
ORDER BY interval;


-- ------------------------------------------------------------
-- 6) Rare completeness check
-- ------------------------------------------------------------
SELECT
  symbol_id,
  interval,
  COUNT(*) AS rows
FROM open_interest
GROUP BY symbol_id, interval
HAVING COUNT(*) < 10
ORDER BY interval, rows ASC
LIMIT 50;
