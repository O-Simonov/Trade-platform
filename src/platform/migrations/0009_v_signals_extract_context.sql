-- 0009_v_signals_extract_context.sql
-- ============================================================
-- Drop + recreate v_signals (because CREATE OR REPLACE VIEW
-- can't change column names/order)
-- ============================================================

DROP VIEW IF EXISTS public.v_signals;

CREATE VIEW public.v_signals AS
SELECT
    COALESCE(NULLIF(s.symbol, ''), sym.symbol) AS symbol,

    s.id,
    s.exchange_id,
    s.symbol_id,
    s.screener_id,
    sc.name AS screener_name,

    s.timeframe,
    s.signal_ts,
    s.signal_day,
    s.day_seq,

    s.side,
    s.status,

    s.entry_price,
    s.exit_price,
    s.stop_loss,
    s.take_profit,

    s.confidence,
    s.score,
    s.reason,

    -- extracted from context (safe casts)
    NULLIF(s.context->>'touch_ts', '')::timestamptz  AS touch_ts,
    NULLIF(s.context->>'up_level', '')::float8       AS up_level,
    NULLIF(s.context->>'down_level', '')::float8     AS down_level,
    NULLIF(s.context->>'liq_long_usdt', '')::float8  AS liq_long_usdt,
    NULLIF(s.context->>'liq_short_usdt', '')::float8 AS liq_short_usdt,

    s.source,
    s.created_at,
    s.updated_at,

    -- raw json
    s.context
FROM public.signals s
LEFT JOIN public.symbols sym ON sym.symbol_id = s.symbol_id
LEFT JOIN public.screeners sc ON sc.screener_id = s.screener_id;
