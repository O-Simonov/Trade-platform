-- 0006_market_state_perf_indexes.sql
-- Доп. индексы для ускорения market_state_5m апдейта и чтения источников
-- Без DO/pg_catalog проверок — максимально совместимо и безопасно.

-- ------------------------------------------------------------
-- market_state_5m: быстрый доступ по symbol/time
-- ------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_market_state_5m_symbol_time
ON public.market_state_5m (exchange_id, symbol_id, open_time DESC);

-- ------------------------------------------------------------
-- candles_trades_agg: источник данных market_state_5m
-- WHERE interval='5m' AND exchange_id=? AND symbol_id=? AND open_time>=...
-- ------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_candles_trades_agg_5m_lookup
ON public.candles_trades_agg (exchange_id, symbol_id, open_time DESC)
WHERE interval = '5m';

-- ------------------------------------------------------------
-- open_interest: LATERAL "последнее значение <= open_time+5m"
-- ------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_open_interest_5m_last
ON public.open_interest (exchange_id, symbol_id, interval, ts DESC);

-- ------------------------------------------------------------
-- liquidation_1m: окно агрегации по bucket_ts
-- ------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_liquidation_1m_lookup
ON public.liquidation_1m (exchange_id, symbol_id, bucket_ts);

-- ------------------------------------------------------------
-- candles (если используешь вариант воркера от candles напрямую)
-- ------------------------------------------------------------
CREATE INDEX IF NOT EXISTS ix_candles_5m_lookup
ON public.candles (exchange_id, symbol_id, interval, open_time DESC);

-- ------------------------------------------------------------
-- статистика
-- ------------------------------------------------------------
ANALYZE public.market_state_5m;
ANALYZE public.candles_trades_agg;
ANALYZE public.open_interest;
ANALYZE public.liquidation_1m;
ANALYZE public.candles;
