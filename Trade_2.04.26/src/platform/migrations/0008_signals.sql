-- 0008_signals.sql
-- Signals tables for multi-screener entry signals
-- idempotent / safe to run multiple times

-- 1) screeners registry (to identify which screener produced a signal)
CREATE TABLE IF NOT EXISTS screeners (
  id          SMALLSERIAL PRIMARY KEY,
  name        TEXT NOT NULL UNIQUE,
  description TEXT,
  version     TEXT,
  is_enabled  BOOLEAN NOT NULL DEFAULT TRUE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2) enums (created only once)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'signal_side') THEN
    CREATE TYPE signal_side AS ENUM ('BUY', 'SELL');
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'signal_status') THEN
    CREATE TYPE signal_status AS ENUM ('NEW', 'TAKEN', 'CANCELLED', 'EXPIRED');
  END IF;
END $$;

-- 3) daily sequence helper (per symbol+screener+day)
CREATE TABLE IF NOT EXISTS signals_day_seq (
  exchange_id SMALLINT NOT NULL,
  symbol_id   INTEGER  NOT NULL,
  screener_id SMALLINT NOT NULL,
  signal_day  DATE     NOT NULL,
  last_seq    INTEGER  NOT NULL DEFAULT 0,
  PRIMARY KEY (exchange_id, symbol_id, screener_id, signal_day),
  FOREIGN KEY (exchange_id) REFERENCES exchanges(id) ON DELETE CASCADE,
  FOREIGN KEY (symbol_id)   REFERENCES symbols(id)   ON DELETE CASCADE,
  FOREIGN KEY (screener_id) REFERENCES screeners(id) ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION signals_next_seq(
  p_exchange_id SMALLINT,
  p_symbol_id   INTEGER,
  p_screener_id SMALLINT,
  p_signal_day  DATE
) RETURNS INTEGER
LANGUAGE plpgsql AS $$
DECLARE v_seq INTEGER;
BEGIN
  INSERT INTO signals_day_seq(exchange_id, symbol_id, screener_id, signal_day, last_seq)
  VALUES (p_exchange_id, p_symbol_id, p_screener_id, p_signal_day, 1)
  ON CONFLICT (exchange_id, symbol_id, screener_id, signal_day)
  DO UPDATE SET last_seq = signals_day_seq.last_seq + 1
  RETURNING last_seq INTO v_seq;

  RETURN v_seq;
END $$;

-- 4) signals table
CREATE TABLE IF NOT EXISTS signals (
  id           BIGSERIAL PRIMARY KEY,

  exchange_id  SMALLINT NOT NULL REFERENCES exchanges(id) ON DELETE CASCADE,
  symbol_id    INTEGER  NOT NULL REFERENCES symbols(id)   ON DELETE CASCADE,
  screener_id  SMALLINT NOT NULL REFERENCES screeners(id) ON DELETE RESTRICT,

  timeframe    TEXT NOT NULL, -- e.g. 1m/5m/15m/1h/4h
  signal_ts    TIMESTAMPTZ NOT NULL, -- detection time (UTC)
  signal_day   DATE NOT NULL,        -- date(signal_ts) in UTC

  -- "signal # within a day" (helps UI/reporting)
  day_seq      INTEGER NOT NULL,

  side         signal_side NOT NULL,
  status       signal_status NOT NULL DEFAULT 'NEW',

  entry_price  NUMERIC(18, 8),
  exit_price   NUMERIC(18, 8),

  stop_loss    NUMERIC(18, 8),
  take_profit  NUMERIC(18, 8),

  confidence   REAL,
  score        REAL,

  reason       TEXT,

  context      JSONB NOT NULL DEFAULT '{}'::jsonb,
  source       TEXT  NOT NULL DEFAULT 'screener',

  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- idempotency keys
  UNIQUE (exchange_id, symbol_id, screener_id, timeframe, signal_ts),
  UNIQUE (exchange_id, symbol_id, screener_id, signal_day, day_seq)
);

CREATE INDEX IF NOT EXISTS ix_signals_ts_desc
  ON signals (signal_ts DESC);

CREATE INDEX IF NOT EXISTS ix_signals_symbol_ts_desc
  ON signals (symbol_id, signal_ts DESC);

CREATE INDEX IF NOT EXISTS ix_signals_screener_day_desc
  ON signals (screener_id, signal_day DESC, day_seq DESC);

