-- 0008_signals_symbol_and_indexes.sql
-- ============================================================
-- Purpose:
--  - remove legacy screener_runs
--  - ensure screener_signals has no run_id
--  - add signals.symbol and backfill it from symbols
--  - add useful indexes
--  - recreate view v_signals with "symbol" as first column
--
-- IMPORTANT:
--  Postgres запрещает "урезать" VIEW через CREATE OR REPLACE VIEW,
--  поэтому для v_signals используем DROP VIEW + CREATE VIEW.
-- ============================================================


-- ============================================================
-- 1) Drop legacy screener_runs (если вдруг ещё существует)
-- ============================================================
DROP TABLE IF EXISTS public.screener_runs CASCADE;


-- ============================================================
-- 2) Safety: убираем run_id из screener_signals (если вдруг остался)
-- ============================================================
ALTER TABLE IF EXISTS public.screener_signals
    DROP COLUMN IF EXISTS run_id;


-- ============================================================
-- 3) Add "symbol" column into signals (если ещё нет)
-- ============================================================
ALTER TABLE IF EXISTS public.signals
    ADD COLUMN IF NOT EXISTS symbol text;

-- Сделаем так, чтобы новые записи не оставляли NULL
ALTER TABLE IF EXISTS public.signals
    ALTER COLUMN symbol SET DEFAULT '';


-- ============================================================
-- 4) Backfill: signals.symbol <- symbols.symbol
-- ============================================================
UPDATE public.signals s
SET symbol = sym.symbol
FROM public.symbols sym
WHERE s.symbol_id = sym.symbol_id
  AND (s.symbol IS NULL OR s.symbol = '');

-- На всякий случай: не оставляем NULL
UPDATE public.signals
SET symbol = ''
WHERE symbol IS NULL;

-- (Опционально) Можно сделать NOT NULL (безопасно после UPDATE)
ALTER TABLE IF EXISTS public.signals
    ALTER COLUMN symbol SET NOT NULL;


-- ============================================================
-- 5) Indexes for fast lookups
-- ============================================================

-- быстрый поиск дублей / идемпотентность
CREATE INDEX IF NOT EXISTS idx_signals_lookup_screener_tf_sym_ts
ON public.signals (exchange_id, screener_id, timeframe, symbol_id, signal_ts);

-- удобно смотреть последние сигналы по названию монеты
CREATE INDEX IF NOT EXISTS ix_signals_symbol_name_ts_desc
ON public.signals (symbol, signal_ts DESC);


-- ============================================================
-- 6) View: symbol первым столбцом (удобный просмотр)
-- ============================================================

-- FIX: нельзя "урезать" VIEW через CREATE OR REPLACE
DROP VIEW IF EXISTS public.v_signals;

CREATE VIEW public.v_signals AS
SELECT
    symbol,
    id,
    exchange_id,
    symbol_id,
    screener_id,
    timeframe,
    signal_ts,
    signal_day,
    day_seq,
    side,
    status,
    entry_price,
    score,
    reason,
    context,
    created_at,
    updated_at
FROM public.signals;


-- ============================================================
-- 7) Optional: update planner stats
-- ============================================================
ANALYZE public.signals;
