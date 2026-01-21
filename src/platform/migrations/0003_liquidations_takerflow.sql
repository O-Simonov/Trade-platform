-- 0003_liquidations_takerflow.sql
-- liquidations (raw + 1m agg) + taker flow columns in candles

-- 1) RAW liquidation events (public WS forceOrder)
CREATE TABLE IF NOT EXISTS liquidation_events (
    id            BIGSERIAL PRIMARY KEY,
    exchange_id   SMALLINT NOT NULL,
    symbol_id     BIGINT NOT NULL,

    ts            TIMESTAMPTZ NOT NULL,
    event_ms      BIGINT NOT NULL,

    side          TEXT NOT NULL,
    price         NUMERIC(28, 12) NOT NULL,
    qty           NUMERIC(28, 12) NOT NULL,
    filled_qty    NUMERIC(28, 12),
    avg_price     NUMERIC(28, 12),
    status        TEXT,
    order_type    TEXT,
    time_in_force TEXT,

    notional      NUMERIC(28, 12),
    is_long_liq   BOOLEAN,
    raw_json      JSONB NOT NULL,

    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_liq_events_lookup
ON liquidation_events (exchange_id, symbol_id, ts DESC);

CREATE UNIQUE INDEX IF NOT EXISTS ux_liq_events_dedupe
ON liquidation_events (exchange_id, symbol_id, event_ms, side, price, qty);

-- 2) 1m liquidation aggregates
CREATE TABLE IF NOT EXISTS liquidation_1m (
    exchange_id    SMALLINT NOT NULL,
    symbol_id      BIGINT NOT NULL,
    bucket_ts      TIMESTAMPTZ NOT NULL,

    long_notional  NUMERIC(28, 12) NOT NULL DEFAULT 0,
    short_notional NUMERIC(28, 12) NOT NULL DEFAULT 0,
    long_qty       NUMERIC(28, 12) NOT NULL DEFAULT 0,
    short_qty      NUMERIC(28, 12) NOT NULL DEFAULT 0,
    events         INT NOT NULL DEFAULT 0,

    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (exchange_id, symbol_id, bucket_ts)
);

CREATE INDEX IF NOT EXISTS ix_liq_1m_lookup
ON liquidation_1m (exchange_id, symbol_id, bucket_ts DESC);

-- 3) Extend candles with taker/delta columns
ALTER TABLE candles
    ADD COLUMN IF NOT EXISTS quote_volume     NUMERIC(28, 12),
    ADD COLUMN IF NOT EXISTS trades           BIGINT,
    ADD COLUMN IF NOT EXISTS taker_buy_base   NUMERIC(28, 12),
    ADD COLUMN IF NOT EXISTS taker_buy_quote  NUMERIC(28, 12),
    ADD COLUMN IF NOT EXISTS taker_sell_base  NUMERIC(28, 12),
    ADD COLUMN IF NOT EXISTS taker_sell_quote NUMERIC(28, 12),
    ADD COLUMN IF NOT EXISTS delta_quote      NUMERIC(28, 12),
    ADD COLUMN IF NOT EXISTS delta_base       NUMERIC(28, 12),
    ADD COLUMN IF NOT EXISTS cvd_quote        NUMERIC(28, 12);
