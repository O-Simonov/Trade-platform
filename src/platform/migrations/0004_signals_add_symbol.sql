BEGIN;

ALTER TABLE public.signals
ADD COLUMN IF NOT EXISTS symbol text;

-- Backfill symbol from symbols table
UPDATE public.signals s
SET symbol = sym.symbol
FROM public.symbols sym
WHERE sym.symbol_id = s.symbol_id
  AND (s.symbol IS NULL OR s.symbol = '');

-- Index for fast visual queries
CREATE INDEX IF NOT EXISTS ix_signals_symbol_text_ts_desc
ON public.signals (symbol, signal_ts DESC);

-- ❗ FIX: drop old view first (Postgres запрещает менять имена колонок через OR REPLACE)
DROP VIEW IF EXISTS public.v_signals;

CREATE VIEW public.v_signals AS
SELECT
  s.symbol,
  s.id,
  s.exchange_id,
  s.symbol_id,
  s.screener_id,
  s.timeframe,
  s.signal_ts,
  s.signal_day,
  s.day_seq,
  s.side,
  s.status,
  s.entry_price,
  s.score,
  s.reason,
  s.context,
  s.source,
  s.created_at,
  s.updated_at
FROM public.signals s;

COMMIT;
