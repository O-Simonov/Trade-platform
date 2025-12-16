BEGIN;

-- 1) Trade fills (executions)
CREATE TABLE IF NOT EXISTS trade_fills (
    trade_id        BIGINT PRIMARY KEY,
    exchange        TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    order_id        BIGINT NOT NULL,
    side            TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
    position_side   TEXT NOT NULL CHECK (position_side IN ('LONG','SHORT')),
    price           NUMERIC NOT NULL,
    qty             NUMERIC NOT NULL,
    quote_qty       NUMERIC NOT NULL,
    commission      NUMERIC DEFAULT 0,
    commission_asset TEXT,
    realized_pnl    NUMERIC DEFAULT 0,
    is_maker        BOOLEAN,
    executed_at     TIMESTAMPTZ NOT NULL,
    strategy_name   TEXT,
    pos_uid         UUID
);

CREATE INDEX IF NOT EXISTS idx_trade_fills_time
ON trade_fills (exchange, executed_at DESC);

CREATE INDEX IF NOT EXISTS idx_trade_fills_strategy_time
ON trade_fills (strategy_name, executed_at DESC);

-- 2) Funding / income
CREATE TABLE IF NOT EXISTS funding_fees (
    income_id   BIGSERIAL PRIMARY KEY,
    exchange    TEXT NOT NULL,
    symbol      TEXT,
    income_type TEXT NOT NULL, -- FUNDING_FEE / COMMISSION / REALIZED_PNL
    amount      NUMERIC NOT NULL,
    asset       TEXT,
    txn_id      TEXT,
    ts          TIMESTAMPTZ NOT NULL,
    pos_uid     UUID
);

CREATE INDEX IF NOT EXISTS idx_funding_time
ON funding_fees (exchange, ts DESC);

-- 3) Daily strategy stats
CREATE TABLE IF NOT EXISTS strategy_stats_daily (
    strategy_name TEXT NOT NULL,
    date          DATE NOT NULL,
    trades_count  INTEGER NOT NULL DEFAULT 0,
    gross_pnl     NUMERIC NOT NULL DEFAULT 0,
    fees          NUMERIC NOT NULL DEFAULT 0,
    funding       NUMERIC NOT NULL DEFAULT 0,
    net_pnl       NUMERIC NOT NULL DEFAULT 0,
    winrate       NUMERIC NOT NULL DEFAULT 0,
    PRIMARY KEY (strategy_name, date)
);

COMMIT;
