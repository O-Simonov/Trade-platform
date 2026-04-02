-- 0006_fix_market_state_unique_check.sql
-- Fix: operator does not exist: name[] = text[]
-- Safe check for unique index on (exchange_id, symbol_id, open_time)

BEGIN;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_index ix
    JOIN pg_class t ON t.oid = ix.indrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = 'public'
      AND t.relname = 'market_state_5m'
      AND ix.indisunique = true
      AND (
        SELECT array_agg(a.attname::text ORDER BY k.ord)
        FROM unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord)
        JOIN pg_attribute a
          ON a.attrelid = t.oid AND a.attnum = k.attnum
      ) = ARRAY['exchange_id','symbol_id','open_time']::text[]
  ) THEN
    -- Если вдруг нет уникального ключа — добавим PK.
    -- (Если PK уже есть - будет duplicate_object -> игнорируем)
    BEGIN
      ALTER TABLE public.market_state_5m
      ADD CONSTRAINT market_state_5m_pkey
      PRIMARY KEY (exchange_id, symbol_id, open_time);
    EXCEPTION WHEN duplicate_object THEN
      -- ok
      NULL;
    END;
  END IF;
END $$;

COMMIT;
