-- 0005_market_trades_and_cvd_fix.sql
-- Fix for partially-applied migrations: ensure tables exist + remove duplicate indexes

-- =========================================
-- market_trades (ensure)
-- =========================================
CREATE TABLE IF NOT EXISTS market_trades (
  exchange_id smallint NOT NULL,
  symbol_id   bigint   NOT NULL,
  trade_id    bigint   NOT NULL,
  ts          timestamptz NOT NULL,
  price       numeric NOT NULL,
  qty         numeric NOT NULL,
  quote_qty   numeric NOT NULL,
  taker_side  text NOT NULL,
  source      text,
  raw_json    jsonb,

  CONSTRAINT market_trades_pkey PRIMARY KEY (exchange_id, symbol_id, trade_id),
  CONSTRAINT market_trades_taker_side_check CHECK (taker_side IN ('BUY','SELL'))
);

-- главный индекс для чтения по времени (у тебя он уже есть)
CREATE INDEX IF NOT EXISTS idx_market_trades_ts
ON market_trades (exchange_id, symbol_id, ts);

-- =========================================
-- candles_trades_agg (ensure)
-- =========================================
CREATE TABLE IF NOT EXISTS candles_trades_agg (
  exchange_id      smallint NOT NULL,
  symbol_id        bigint   NOT NULL,
  interval         text     NOT NULL,
  open_time        timestamptz NOT NULL,

  quote_volume     numeric NOT NULL DEFAULT 0,
  trades           bigint  NOT NULL DEFAULT 0,
  taker_buy_quote  numeric NOT NULL DEFAULT 0,
  taker_sell_quote numeric NOT NULL DEFAULT 0,
  delta_quote      numeric NOT NULL DEFAULT 0,
  cvd_quote        numeric NOT NULL DEFAULT 0,

  updated_at       timestamptz NOT NULL DEFAULT now(),

  CONSTRAINT candles_trades_agg_pkey PRIMARY KEY (exchange_id, symbol_id, interval, open_time)
);

-- оставим 1 lookup индекс (DESC open_time)
CREATE INDEX IF NOT EXISTS idx_candles_trades_agg_lookup
ON candles_trades_agg (exchange_id, symbol_id, interval, open_time DESC);

-- индекс для быстрых range-выборок
CREATE INDEX IF NOT EXISTS ix_candles_trades_agg_open_time
ON candles_trades_agg (exchange_id, symbol_id, interval, open_time);

-- =========================================
-- Remove duplicated indexes (if exist)
-- =========================================

-- у тебя был дубль idx_candles_trades_agg_lookup и ix_candles_trades_agg_lookup
DROP INDEX IF EXISTS ix_candles_trades_agg_lookup;
