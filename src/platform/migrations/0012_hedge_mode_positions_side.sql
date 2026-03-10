-- 0012_hedge_mode_positions_side.sql
-- Make positions/position_snapshots side-aware for true hedge mode.

ALTER TABLE public.position_snapshots
  ADD COLUMN IF NOT EXISTS side TEXT DEFAULT 'FLAT';

UPDATE public.position_snapshots
SET side = COALESCE(NULLIF(UPPER(side), ''), 'FLAT')
WHERE side IS NULL OR side = '';

ALTER TABLE public.positions DROP CONSTRAINT IF EXISTS chk_flat_consistency;
ALTER TABLE public.positions DROP CONSTRAINT IF EXISTS chk_qty_non_negative;
ALTER TABLE public.positions DROP CONSTRAINT IF EXISTS chk_status_valid;
ALTER TABLE public.positions DROP CONSTRAINT IF EXISTS chk_symbol_positive;

ALTER TABLE public.positions
  ADD CONSTRAINT chk_qty_non_negative CHECK (qty >= 0);
ALTER TABLE public.positions
  ADD CONSTRAINT chk_status_valid CHECK (status IN ('OPEN','CLOSED'));
ALTER TABLE public.positions
  ADD CONSTRAINT chk_symbol_positive CHECK (symbol_id > 0);
ALTER TABLE public.positions
  ADD CONSTRAINT chk_flat_consistency CHECK (
    (side = 'FLAT' AND qty = 0 AND status = 'CLOSED')
    OR (side IN ('LONG','SHORT') AND qty > 0 AND status = 'OPEN')
    OR (side IN ('LONG','SHORT') AND qty = 0 AND status = 'CLOSED')
  );

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'positions_pkey' AND conrelid = 'public.positions'::regclass) THEN
    ALTER TABLE public.positions DROP CONSTRAINT positions_pkey;
  END IF;
END$$;

ALTER TABLE public.positions
  ADD CONSTRAINT positions_pkey PRIMARY KEY (exchange_id, account_id, symbol_id, side);

DROP INDEX IF EXISTS public.idx_positions_open_symbol;
CREATE INDEX IF NOT EXISTS idx_positions_open_symbol_side
  ON public.positions(exchange_id, symbol_id, side)
  WHERE status = 'OPEN' AND closed_at IS NULL AND abs(qty) > 0;
CREATE INDEX IF NOT EXISTS idx_positions_status_side
  ON public.positions(exchange_id, account_id, status, side);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'position_snapshots_pkey' AND conrelid = 'public.position_snapshots'::regclass) THEN
    ALTER TABLE public.position_snapshots DROP CONSTRAINT position_snapshots_pkey;
  END IF;
END$$;

ALTER TABLE public.position_snapshots
  ADD CONSTRAINT position_snapshots_pkey PRIMARY KEY (exchange_id, account_id, symbol_id, side);

CREATE INDEX IF NOT EXISTS idx_hedge_links_base_uid
  ON public.hedge_links(exchange_id, base_account_id, symbol_id, base_pos_uid);
CREATE INDEX IF NOT EXISTS idx_hedge_links_hedge_uid
  ON public.hedge_links(exchange_id, hedge_account_id, symbol_id, hedge_pos_uid);
