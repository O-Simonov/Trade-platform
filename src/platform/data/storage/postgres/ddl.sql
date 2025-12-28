CREATE TABLE IF NOT EXISTS exchanges (
  exchange_id SMALLSERIAL PRIMARY KEY,
  name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS accounts (
  account_id SMALLSERIAL PRIMARY KEY,
  exchange_id SMALLINT NOT NULL REFERENCES exchanges(exchange_id),
  account_name TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'MIXED',
  is_active BOOLEAN DEFAULT TRUE,
  UNIQUE(exchange_id, account_name)
);

CREATE TABLE IF NOT EXISTS symbols (
  symbol_id BIGSERIAL PRIMARY KEY,
  exchange_id SMALLINT NOT NULL REFERENCES exchanges(exchange_id),
  symbol TEXT NOT NULL,
  UNIQUE(exchange_id, symbol)
);

CREATE TABLE IF NOT EXISTS positions (
  exchange_id SMALLINT NOT NULL,
  account_id SMALLINT NOT NULL,
  symbol_id BIGINT NOT NULL,
  strategy_id TEXT NOT NULL DEFAULT 'unknown',
  pos_uid TEXT,
  side TEXT NOT NULL,
  qty NUMERIC(18,8) NOT NULL,
  entry_price NUMERIC(18,8),
  mark_price NUMERIC(18,8),
  unrealized_pnl NUMERIC(18,8),
  updated_at TIMESTAMPTZ NOT NULL,
  source TEXT DEFAULT 'rest',
  PRIMARY KEY(exchange_id, account_id, symbol_id)
);

CREATE TABLE IF NOT EXISTS hedge_links (
  exchange_id SMALLINT NOT NULL,
  base_account_id SMALLINT NOT NULL,
  hedge_account_id SMALLINT NOT NULL,
  symbol_id BIGINT NOT NULL,
  base_pos_uid TEXT NOT NULL,
  hedge_pos_uid TEXT NOT NULL,
  hedge_ratio NUMERIC(18,8),
  created_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY(exchange_id, base_account_id, hedge_account_id, symbol_id, base_pos_uid, hedge_pos_uid)
);

CREATE TABLE IF NOT EXISTS orders (
  exchange_id SMALLINT NOT NULL,
  account_id SMALLINT NOT NULL,
  order_id TEXT NOT NULL,
  symbol_id BIGINT NOT NULL,
  strategy_id TEXT NOT NULL DEFAULT 'unknown',
  pos_uid TEXT,
  client_order_id TEXT,
  side TEXT,
  type TEXT,
  reduce_only BOOLEAN,
  price NUMERIC(18,8),
  qty NUMERIC(18,8),
  filled_qty NUMERIC(18,8),
  status TEXT,
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ,
  source TEXT DEFAULT 'ws_user',
  PRIMARY KEY(exchange_id, account_id, order_id)
);
CREATE INDEX IF NOT EXISTS idx_orders_client_oid ON orders(exchange_id, account_id, client_order_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(exchange_id, account_id, status);


CREATE TABLE IF NOT EXISTS order_events (
  exchange_id SMALLINT NOT NULL,
  account_id SMALLINT NOT NULL,
  order_id TEXT NOT NULL,
  symbol_id BIGINT NOT NULL,
  client_order_id TEXT,
  status TEXT NOT NULL,
  side TEXT,
  type TEXT,
  reduce_only BOOLEAN,
  price NUMERIC(18,8),
  qty NUMERIC(18,8),
  filled_qty NUMERIC(18,8),
  source TEXT DEFAULT 'ws_user',
  ts_ms BIGINT NOT NULL,
  recv_ts TIMESTAMPTZ NOT NULL,
  raw_json TEXT,
  -- idempotency key: same order_id + ts_ms + status + filled_qty won't be inserted twice
  UNIQUE(exchange_id, account_id, order_id, ts_ms, status, filled_qty)
);
CREATE INDEX IF NOT EXISTS idx_order_events_oid ON order_events(exchange_id, account_id, order_id);
CREATE INDEX IF NOT EXISTS idx_order_events_ts  ON order_events(exchange_id, account_id, ts_ms);

CREATE TABLE IF NOT EXISTS trades (
  exchange_id SMALLINT NOT NULL,
  account_id SMALLINT NOT NULL,
  trade_id TEXT NOT NULL,
  order_id TEXT,
  symbol_id BIGINT NOT NULL,
  strategy_id TEXT NOT NULL DEFAULT 'unknown',
  pos_uid TEXT,
  side TEXT,
  price NUMERIC(18,8),
  qty NUMERIC(18,8),
  fee NUMERIC(18,8),
  fee_asset TEXT,
  realized_pnl NUMERIC(18,8),
  ts TIMESTAMPTZ NOT NULL,
  source TEXT DEFAULT 'ws_user',
  PRIMARY KEY(exchange_id, account_id, trade_id)
);

CREATE TABLE IF NOT EXISTS order_fills (
  exchange_id SMALLINT NOT NULL,
  account_id SMALLINT NOT NULL,
  fill_uid TEXT NOT NULL,
  symbol_id BIGINT NOT NULL,
  order_id TEXT,
  trade_id TEXT,
  client_order_id TEXT,
  price NUMERIC(18,8),
  qty NUMERIC(18,8),
  realized_pnl NUMERIC(18,8),
  ts TIMESTAMPTZ NOT NULL,
  source TEXT DEFAULT 'ws_user',
  PRIMARY KEY(exchange_id, account_id, fill_uid)
);

CREATE TABLE IF NOT EXISTS candles (
  exchange_id SMALLINT NOT NULL,
  symbol_id BIGINT NOT NULL,
  interval TEXT NOT NULL,
  open_time TIMESTAMPTZ NOT NULL,
  open NUMERIC(18,8) NOT NULL,
  high NUMERIC(18,8) NOT NULL,
  low  NUMERIC(18,8) NOT NULL,
  close NUMERIC(18,8) NOT NULL,
  volume NUMERIC(28,8) NOT NULL,
  source TEXT DEFAULT 'ws_kline',
  PRIMARY KEY(exchange_id, symbol_id, interval, open_time)
);

CREATE TABLE IF NOT EXISTS funding (
  exchange_id SMALLINT NOT NULL,
  symbol_id BIGINT NOT NULL,
  funding_time TIMESTAMPTZ NOT NULL,
  funding_rate NUMERIC(10,8) NOT NULL,
  mark_price NUMERIC(18,8),
  source TEXT DEFAULT 'rest',
  PRIMARY KEY(exchange_id, symbol_id, funding_time)
);

CREATE TABLE IF NOT EXISTS price_snapshots (
  exchange_id SMALLINT NOT NULL,
  symbol_id BIGINT NOT NULL,
  price NUMERIC(18,8) NOT NULL,
  price_type TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  source TEXT DEFAULT 'ws'
);

CREATE INDEX IF NOT EXISTS idx_trades_lookup ON trades(exchange_id, account_id, symbol_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_fills_lookup ON order_fills(exchange_id, account_id, symbol_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_candles_lookup ON candles(exchange_id, symbol_id, interval, open_time DESC);
CREATE INDEX IF NOT EXISTS idx_funding_lookup ON funding(exchange_id, symbol_id, funding_time DESC);
CREATE INDEX IF NOT EXISTS idx_snapshots_lookup ON price_snapshots(exchange_id, symbol_id, ts DESC);


-- ============================
-- v9 Market State Layer
-- ============================

CREATE TABLE IF NOT EXISTS account_balance_snapshots (
  exchange_id SMALLINT NOT NULL,
  account_id SMALLINT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  wallet_balance NUMERIC(24,8) NOT NULL,
  equity NUMERIC(24,8) NOT NULL,
  available_balance NUMERIC(24,8) NOT NULL,
  margin_used NUMERIC(24,8) NOT NULL,
  unrealized_pnl NUMERIC(24,8) NOT NULL,
  source TEXT DEFAULT 'rest',
  PRIMARY KEY (exchange_id, account_id, ts),
  FOREIGN KEY(exchange_id) REFERENCES exchanges(exchange_id),
  FOREIGN KEY(account_id) REFERENCES accounts(account_id)
);


CREATE INDEX IF NOT EXISTS idx_balance_lookup ON account_balance_snapshots(exchange_id, account_id, ts DESC);

CREATE TABLE IF NOT EXISTS open_interest (
  exchange_id SMALLINT NOT NULL,
  symbol_id BIGINT NOT NULL,
  interval TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  open_interest NUMERIC(24,8) NOT NULL,
  open_interest_value NUMERIC(24,8) NOT NULL,
  source TEXT DEFAULT 'rest',
  PRIMARY KEY (exchange_id, symbol_id, interval, ts),
  FOREIGN KEY(exchange_id) REFERENCES exchanges(exchange_id),
  FOREIGN KEY(symbol_id) REFERENCES symbols(symbol_id)
);


CREATE INDEX IF NOT EXISTS idx_oi_lookup ON open_interest(exchange_id, symbol_id, interval, ts DESC);

-- v9.1: Downsampled balance tables
CREATE TABLE IF NOT EXISTS account_balance_5m (
  exchange_id SMALLINT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  wallet_balance NUMERIC(18,8) NOT NULL,
  equity NUMERIC(18,8) NOT NULL,
  available_balance NUMERIC(18,8) NOT NULL,
  margin_used NUMERIC(18,8) NOT NULL,
  unrealized_pnl NUMERIC(18,8) NOT NULL,
  PRIMARY KEY(exchange_id, ts)
);

CREATE TABLE IF NOT EXISTS account_balance_1h (
  exchange_id SMALLINT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  wallet_balance NUMERIC(18,8) NOT NULL,
  equity NUMERIC(18,8) NOT NULL,
  available_balance NUMERIC(18,8) NOT NULL,
  margin_used NUMERIC(18,8) NOT NULL,
  unrealized_pnl NUMERIC(18,8) NOT NULL,
  PRIMARY KEY(exchange_id, ts)
);

-- v9.1: Extend positions table with standard trading fields (safe for existing installs)
ALTER TABLE positions ADD COLUMN IF NOT EXISTS avg_price NUMERIC(18,8);
ALTER TABLE positions ADD COLUMN IF NOT EXISTS exit_price NUMERIC(18,8);
ALTER TABLE positions ADD COLUMN IF NOT EXISTS position_value_usdt NUMERIC(18,8);
ALTER TABLE positions ADD COLUMN IF NOT EXISTS leverage INTEGER;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'OPEN';
ALTER TABLE positions ADD COLUMN IF NOT EXISTS opened_at TIMESTAMPTZ;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS realized_pnl NUMERIC(18,8);

ALTER TABLE positions ADD COLUMN IF NOT EXISTS stop_loss_1 NUMERIC(18,8);
ALTER TABLE positions ADD COLUMN IF NOT EXISTS stop_loss_2 NUMERIC(18,8);

ALTER TABLE positions ADD COLUMN IF NOT EXISTS take_profit_1 NUMERIC(18,8);
ALTER TABLE positions ADD COLUMN IF NOT EXISTS take_profit_2 NUMERIC(18,8);
ALTER TABLE positions ADD COLUMN IF NOT EXISTS take_profit_3 NUMERIC(18,8);

ALTER TABLE positions ADD COLUMN IF NOT EXISTS scale_in_count INTEGER DEFAULT 0;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS strategy_name TEXT;


-- v9.2: Position Ledger (fills-driven)
CREATE TABLE IF NOT EXISTS position_ledger (
  exchange_id SMALLINT NOT NULL,
  account_id SMALLINT NOT NULL,
  pos_uid TEXT NOT NULL,
  symbol_id BIGINT NOT NULL,
  strategy_id TEXT NOT NULL DEFAULT 'unknown',
  strategy_name TEXT,
  side TEXT NOT NULL CHECK (side IN ('LONG','SHORT')),
  status TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','CLOSED','PARTIAL','FLIPPED')),
  opened_at TIMESTAMPTZ NOT NULL,
  closed_at TIMESTAMPTZ,
  entry_price NUMERIC(18,8),
  avg_price NUMERIC(18,8),
  exit_price NUMERIC(18,8),
  qty_opened NUMERIC(18,8) NOT NULL DEFAULT 0,
  qty_current NUMERIC(18,8) NOT NULL DEFAULT 0,
  qty_closed NUMERIC(18,8) NOT NULL DEFAULT 0,
  position_value_usdt NUMERIC(18,8),
  scale_in_count INTEGER DEFAULT 0,
  realized_pnl NUMERIC(18,8) DEFAULT 0,
  fees NUMERIC(18,8) DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL,
  source TEXT DEFAULT 'ledger',
  PRIMARY KEY(exchange_id, account_id, pos_uid)
);
CREATE INDEX IF NOT EXISTS idx_position_ledger_symbol ON position_ledger(exchange_id, account_id, symbol_id);
CREATE INDEX IF NOT EXISTS idx_position_ledger_status ON position_ledger(exchange_id, account_id, status);
CREATE INDEX IF NOT EXISTS idx_position_ledger_closed_at ON position_ledger(exchange_id, account_id, closed_at);
