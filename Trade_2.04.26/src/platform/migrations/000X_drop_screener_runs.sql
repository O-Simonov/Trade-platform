-- Drop legacy screener_runs table (not used anymore)
DROP TABLE IF EXISTS public.screener_runs CASCADE;

-- Remove unused FK column from screener_signals
ALTER TABLE public.screener_signals
DROP COLUMN IF EXISTS run_id;

