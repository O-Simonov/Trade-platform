-- Pump screener interval/data audit
-- Goal: verify that candles, OI and CVD exist in DB for the SAME interval
-- used by the screener and by the plot.
--
-- How to use:
-- 1) Change values in the params CTE if needed.
-- 2) Run the whole file, or run sections separately.

-- ============================================================
-- SECTION 1. Coverage by interval for the last N hours
-- ============================================================
WITH params AS (
    SELECT
        1::int                       AS p_exchange_id,
        ARRAY['5m','15m','1h']::text[] AS p_intervals,
        24::int                      AS p_hours
),
recent_candles AS (
    SELECT c.symbol_id, c.interval, COUNT(*) AS candles_cnt,
           MIN(c.open_time) AS candles_min_ts,
           MAX(c.open_time) AS candles_max_ts,
           COUNT(*) FILTER (WHERE c.cvd_quote IS NOT NULL) AS cvd_cnt
    FROM candles c, params p
    WHERE c.exchange_id = p.p_exchange_id
      AND c.interval = ANY(p.p_intervals)
      AND c.open_time >= now() - make_interval(hours => p.p_hours)
    GROUP BY c.symbol_id, c.interval
),
recent_oi AS (
    SELECT oi.symbol_id, oi.interval, COUNT(*) AS oi_cnt,
           MIN(oi.ts) AS oi_min_ts,
           MAX(oi.ts) AS oi_max_ts
    FROM open_interest oi, params p
    WHERE oi.exchange_id = p.p_exchange_id
      AND oi.interval = ANY(p.p_intervals)
      AND oi.ts >= now() - make_interval(hours => p.p_hours)
    GROUP BY oi.symbol_id, oi.interval
)
SELECT
    s.symbol,
    COALESCE(c.interval, oi.interval) AS interval,
    COALESCE(c.candles_cnt, 0) AS candles_cnt,
    COALESCE(c.cvd_cnt, 0) AS cvd_cnt,
    COALESCE(oi.oi_cnt, 0) AS oi_cnt,
    c.candles_max_ts,
    oi.oi_max_ts,
    CASE
        WHEN c.symbol_id IS NULL THEN 'NO_CANDLES'
        WHEN c.cvd_cnt = 0 THEN 'NO_CVD'
        WHEN oi.symbol_id IS NULL THEN 'NO_OI'
        ELSE 'OK'
    END AS status
FROM recent_candles c
FULL OUTER JOIN recent_oi oi
  ON oi.symbol_id = c.symbol_id AND oi.interval = c.interval
JOIN symbols s
  ON s.id = COALESCE(c.symbol_id, oi.symbol_id)
ORDER BY status DESC, s.symbol, interval;

-- ============================================================
-- SECTION 2. Exact interval check for one symbol
-- Replace symbol/interval if needed.
-- ============================================================
WITH params AS (
    SELECT
        1::int           AS p_exchange_id,
        'EDGEUSDT'::text AS p_symbol,
        '15m'::text      AS p_interval,
        120::int         AS p_bars
), sym AS (
    SELECT s.id AS symbol_id, s.symbol
    FROM symbols s, params p
    WHERE s.symbol = p.p_symbol
), candles_slice AS (
    SELECT c.open_time AS ts, c.open, c.high, c.low, c.close, c.volume, c.quote_volume, c.cvd_quote
    FROM candles c, params p, sym
    WHERE c.exchange_id = p.p_exchange_id
      AND c.symbol_id = sym.symbol_id
      AND c.interval = p.p_interval
    ORDER BY c.open_time DESC
    LIMIT (SELECT p_bars FROM params)
), oi_slice AS (
    SELECT oi.ts, oi.open_interest, oi.open_interest_value
    FROM open_interest oi, params p, sym
    WHERE oi.exchange_id = p.p_exchange_id
      AND oi.symbol_id = sym.symbol_id
      AND oi.interval = p.p_interval
    ORDER BY oi.ts DESC
    LIMIT (SELECT p_bars FROM params)
)
SELECT 'candles' AS dataset, COUNT(*) AS cnt, MIN(ts) AS min_ts, MAX(ts) AS max_ts FROM candles_slice
UNION ALL
SELECT 'oi' AS dataset, COUNT(*) AS cnt, MIN(ts) AS min_ts, MAX(ts) AS max_ts FROM oi_slice;

-- ============================================================
-- SECTION 3. Timestamp alignment for one symbol/interval
-- Shows whether candle timestamps have matching OI timestamps.
-- ============================================================
WITH params AS (
    SELECT
        1::int           AS p_exchange_id,
        'EDGEUSDT'::text AS p_symbol,
        '15m'::text      AS p_interval,
        120::int         AS p_bars
), sym AS (
    SELECT s.id AS symbol_id
    FROM symbols s, params p
    WHERE s.symbol = p.p_symbol
), candles_slice AS (
    SELECT c.open_time AS ts, c.cvd_quote
    FROM candles c, params p, sym
    WHERE c.exchange_id = p.p_exchange_id
      AND c.symbol_id = sym.symbol_id
      AND c.interval = p.p_interval
    ORDER BY c.open_time DESC
    LIMIT (SELECT p_bars FROM params)
), oi_slice AS (
    SELECT oi.ts, oi.open_interest
    FROM open_interest oi, params p, sym
    WHERE oi.exchange_id = p.p_exchange_id
      AND oi.symbol_id = sym.symbol_id
      AND oi.interval = p.p_interval
    ORDER BY oi.ts DESC
    LIMIT (SELECT p_bars FROM params)
)
SELECT
    c.ts AS candle_ts,
    c.cvd_quote,
    oi.ts AS oi_ts,
    oi.open_interest,
    CASE WHEN oi.ts IS NULL THEN 'MISSING_OI_AT_TS' ELSE 'OK' END AS oi_status
FROM candles_slice c
LEFT JOIN oi_slice oi
  ON oi.ts = c.ts
ORDER BY c.ts DESC;

-- ============================================================
-- SECTION 4. Symbols that are missing OI exactly in screener intervals
-- ============================================================
WITH params AS (
    SELECT
        1::int AS p_exchange_id,
        ARRAY['5m','15m','1h']::text[] AS p_intervals,
        6::int AS p_hours
), candle_syms AS (
    SELECT DISTINCT c.symbol_id, c.interval
    FROM candles c, params p
    WHERE c.exchange_id = p.p_exchange_id
      AND c.interval = ANY(p.p_intervals)
      AND c.open_time >= now() - make_interval(hours => p.p_hours)
), oi_syms AS (
    SELECT DISTINCT oi.symbol_id, oi.interval
    FROM open_interest oi, params p
    WHERE oi.exchange_id = p.p_exchange_id
      AND oi.interval = ANY(p.p_intervals)
      AND oi.ts >= now() - make_interval(hours => p.p_hours)
)
SELECT s.symbol, cs.interval
FROM candle_syms cs
LEFT JOIN oi_syms oi
  ON oi.symbol_id = cs.symbol_id AND oi.interval = cs.interval
JOIN symbols s ON s.id = cs.symbol_id
WHERE oi.symbol_id IS NULL
ORDER BY cs.interval, s.symbol;

-- ============================================================
-- SECTION 5. Freshness summary by interval
-- ============================================================
WITH params AS (
    SELECT 1::int AS p_exchange_id,
           ARRAY['5m','15m','1h']::text[] AS p_intervals
), candle_fresh AS (
    SELECT interval, MAX(open_time) AS last_ts
    FROM candles, params p
    WHERE exchange_id = p.p_exchange_id
      AND interval = ANY(p.p_intervals)
    GROUP BY interval
), oi_fresh AS (
    SELECT interval, MAX(ts) AS last_ts
    FROM open_interest, params p
    WHERE exchange_id = p.p_exchange_id
      AND interval = ANY(p.p_intervals)
    GROUP BY interval
)
SELECT
    i.interval,
    cf.last_ts AS candles_last_ts,
    round(EXTRACT(EPOCH FROM (now() - cf.last_ts))/60.0, 2) AS candles_age_min,
    ofr.last_ts AS oi_last_ts,
    round(EXTRACT(EPOCH FROM (now() - ofr.last_ts))/60.0, 2) AS oi_age_min
FROM unnest(ARRAY['5m','15m','1h']::text[]) AS i(interval)
LEFT JOIN candle_fresh cf ON cf.interval = i.interval
LEFT JOIN oi_fresh ofr ON ofr.interval = i.interval
ORDER BY i.interval;
