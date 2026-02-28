-- ======================================================================
-- ALGO ORDERS (Binance conditional / openAlgoOrders)
-- ======================================================================
CREATE TABLE IF NOT EXISTS algo_orders (
  exchange_id     SMALLINT NOT NULL,
  account_id      SMALLINT NOT NULL,
  client_algo_id  TEXT NOT NULL,
  algo_id         TEXT,
  symbol          TEXT NOT NULL,
  side            TEXT,
  position_side   TEXT,
  type            TEXT,
  quantity        NUMERIC(18,8),
  trigger_price   NUMERIC(18,8),
  working_type    TEXT,
  status          TEXT NOT NULL DEFAULT 'OPEN',
  strategy_id     TEXT NOT NULL DEFAULT 'unknown',
  pos_uid         TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  raw_json        JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (exchange_id, account_id, client_algo_id)
);

CREATE INDEX IF NOT EXISTS idx_algo_orders_status ON algo_orders(exchange_id, account_id, status);
CREATE INDEX IF NOT EXISTS idx_algo_orders_symbol ON algo_orders(exchange_id, account_id, symbol);
CREATE INDEX IF NOT EXISTS idx_algo_orders_pos_uid ON algo_orders(exchange_id, account_id, pos_uid);
CREATE INDEX IF NOT EXISTS idx_algo_orders_updated_at ON algo_orders(exchange_id, account_id, updated_at DESC);
