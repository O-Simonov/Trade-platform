CREATE TABLE IF NOT EXISTS orders (
    order_id        BIGINT PRIMARY KEY,          -- exchange orderId
    exchange        TEXT NOT NULL,                -- binance, bybit, etc
    account_id      TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    side            TEXT NOT NULL,                -- BUY / SELL
    type            TEXT NOT NULL,                -- MARKET / LIMIT / STOP / TAKE_PROFIT
    status          TEXT NOT NULL,                -- NEW / OPEN / PARTIALLY_FILLED / FILLED / CANCELED
    price           NUMERIC,
    stop_price      NUMERIC,
    qty             NUMERIC NOT NULL,
    filled_qty      NUMERIC NOT NULL DEFAULT 0,
    avg_price       NUMERIC,
    reduce_only     BOOLEAN DEFAULT FALSE,
    position_side   TEXT,                         -- LONG / SHORT / BOTH
    client_order_id TEXT,
    strategy_id     TEXT,
    created_ts      TIMESTAMPTZ NOT NULL,
    updated_ts      TIMESTAMPTZ NOT NULL,
    last_trade_id   BIGINT
);

CREATE INDEX IF NOT EXISTS idx_orders_symbol  ON orders(symbol);
CREATE INDEX IF NOT EXISTS idx_orders_status  ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_strategy ON orders(strategy_id);
