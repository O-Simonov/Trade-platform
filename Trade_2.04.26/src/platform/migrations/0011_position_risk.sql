-- ------------------------------------------------------------
-- 0011_position_risk.sql
-- Таблица для хранения риск-параметров позиции (SL/TP/Trailing/Scale-in)
--
-- Используется стратегиями/трейдерами, в т.ч. paper-trader trade_liquidation.
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS position_risk (
  exchange_id SMALLINT NOT NULL,
  account_id  SMALLINT NOT NULL,
  symbol_id   INTEGER  NOT NULL,
  pos_uid     TEXT     NOT NULL,

  -- базовые уровни
  sl_price NUMERIC,
  tp_price NUMERIC,

  -- trailing
  trailing_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  trailing_activation_pct NUMERIC,
  trailing_trail_pct NUMERIC,
  trailing_armed BOOLEAN NOT NULL DEFAULT FALSE,
  best_price NUMERIC,

  -- scale-in
  additions_count INTEGER NOT NULL DEFAULT 0,
  max_additions INTEGER NOT NULL DEFAULT 0,
  add_notional_usdt NUMERIC,
  levels_timeframe TEXT,
  levels_lookback_bars INTEGER,
  levels_pivot_left INTEGER,
  levels_pivot_right INTEGER,
  level_touch_pct NUMERIC,
  levels_json JSONB,

  -- мета
  signal_id BIGINT,
  signal_timeframe TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (exchange_id, account_id, symbol_id),
  UNIQUE (exchange_id, account_id, pos_uid)
);

CREATE INDEX IF NOT EXISTS idx_position_risk_pos_uid
  ON position_risk(exchange_id, account_id, pos_uid);
