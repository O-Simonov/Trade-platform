# src/platform/data/market_state/market_state_5m_worker.py
from __future__ import annotations

import logging
import random
import threading
import time
from typing import Sequence, Optional, Any, List

import psycopg

log = logging.getLogger("market_state.5m_worker")

UPSERT_CHUNK_SQL = """
INSERT INTO market_state_5m (
  exchange_id, symbol_id, open_time,
  quote_volume, trades, taker_buy_quote, taker_sell_quote,
  delta_quote, cvd_quote,
  oi_ts, open_interest, open_interest_value,
  liq_long_notional, liq_short_notional, liq_events,
  updated_at
)
SELECT
  c.exchange_id,
  c.symbol_id,
  c.open_time,

  COALESCE(c.quote_volume, 0)     AS quote_volume,
  COALESCE(c.trades, 0)           AS trades,
  COALESCE(c.taker_buy_quote, 0)  AS taker_buy_quote,
  COALESCE(c.taker_sell_quote, 0) AS taker_sell_quote,
  COALESCE(c.delta_quote, 0)      AS delta_quote,
  COALESCE(c.cvd_quote, 0)        AS cvd_quote,

  oi.ts AS oi_ts,
  oi.open_interest,
  oi.open_interest_value,

  COALESCE(liq.long_notional, 0)  AS liq_long_notional,
  COALESCE(liq.short_notional, 0) AS liq_short_notional,
  COALESCE(liq.events, 0)         AS liq_events,

  now() AS updated_at
FROM candles c

LEFT JOIN LATERAL (
  SELECT ts, open_interest, open_interest_value
  FROM open_interest
  WHERE exchange_id=c.exchange_id
    AND symbol_id=c.symbol_id
    AND interval='5m'
    AND ts <= (c.open_time + interval '5 minutes')
  ORDER BY ts DESC
  LIMIT 1
) oi ON true

LEFT JOIN LATERAL (
  SELECT
    SUM(long_notional)::double precision  AS long_notional,
    SUM(short_notional)::double precision AS short_notional,
    SUM(events)::int                      AS events
  FROM liquidation_1m
  WHERE exchange_id=c.exchange_id
    AND symbol_id=c.symbol_id
    AND bucket_ts >= c.open_time
    AND bucket_ts <  (c.open_time + interval '5 minutes')
) liq ON true

WHERE c.interval='5m'
  AND c.exchange_id=%s
  AND c.symbol_id = ANY(%s)
  AND c.open_time >= now() - (%s || ' minutes')::interval

ON CONFLICT (exchange_id, symbol_id, open_time) DO UPDATE SET
  quote_volume        = EXCLUDED.quote_volume,
  trades              = EXCLUDED.trades,
  taker_buy_quote     = EXCLUDED.taker_buy_quote,
  taker_sell_quote    = EXCLUDED.taker_sell_quote,
  delta_quote         = EXCLUDED.delta_quote,
  cvd_quote           = EXCLUDED.cvd_quote,
  oi_ts               = EXCLUDED.oi_ts,
  open_interest       = EXCLUDED.open_interest,
  open_interest_value = EXCLUDED.open_interest_value,
  liq_long_notional   = EXCLUDED.liq_long_notional,
  liq_short_notional  = EXCLUDED.liq_short_notional,
  liq_events          = EXCLUDED.liq_events,
  updated_at          = now();
"""


class MarketState5mWorker(threading.Thread):
    """
    Обновляет market_state_5m по всем символам батчами.

    Исправления:
      - вместо SET LOCAL ... = %s используем set_config(), чтобы можно было параметризовать
      - ретраи при deadlock/timeout/serialization/operational
      - авто-рефреш symbol_ids из symbols
    """

    def __init__(
        self,
        *,
        pool: Any,
        exchange_id: int,
        symbol_ids: Sequence[int],
        poll_sec: float = 10.0,
        symbols_per_cycle: int = 40,
        lookback_minutes: int = 180,
        stop_event: Optional[threading.Event] = None,
        logger: Optional[logging.Logger] = None,
        refresh_symbols_sec: int = 600,
        # retry tuning
        retries: int = 6,
        base_sleep_sec: float = 0.20,
        statement_timeout_ms: int = 15000,
        lock_timeout_ms: int = 2000,
    ):
        super().__init__(daemon=True, name="MarketState5mWorker")
        self.pool = pool
        self.exchange_id = int(exchange_id)

        self.poll_sec = float(poll_sec)
        self.symbols_per_cycle = int(symbols_per_cycle) if int(symbols_per_cycle) > 0 else 40
        self.lookback_minutes = int(lookback_minutes) if int(lookback_minutes) > 0 else 180

        self.stop_event = stop_event or threading.Event()
        self.logger = logger or log

        self.refresh_symbols_sec = int(refresh_symbols_sec or 0)
        self.retries = int(retries) if int(retries) >= 0 else 0
        self.base_sleep_sec = float(base_sleep_sec) if float(base_sleep_sec) > 0 else 0.20

        self.statement_timeout_ms = int(statement_timeout_ms) if int(statement_timeout_ms) > 0 else 15000
        self.lock_timeout_ms = int(lock_timeout_ms) if int(lock_timeout_ms) > 0 else 2000

        # внутренние
        self._lock = threading.Lock()
        self._i = 0
        self._last_symbols_refresh_ts = time.time()
        self._last_no_symbols_log_ts = 0.0

        # init list
        init_ids = [int(x) for x in (symbol_ids or [])]
        init_ids = sorted(set(init_ids))
        self.symbol_ids: List[int] = init_ids

        self.logger.info(
            "[MarketState5m] init symbols=%d poll=%.2fs batch=%d lookback=%dm refresh_symbols_sec=%s retries=%d base_sleep=%.2fs timeouts stmt=%dms lock=%dms",
            len(self.symbol_ids),
            self.poll_sec,
            self.symbols_per_cycle,
            self.lookback_minutes,
            self.refresh_symbols_sec,
            self.retries,
            self.base_sleep_sec,
            self.statement_timeout_ms,
            self.lock_timeout_ms,
        )

    # -------------------------
    # Symbols refresh
    # -------------------------
    def _should_refresh_symbols(self) -> bool:
        if self.refresh_symbols_sec <= 0:
            return False
        now = time.time()
        return (now - self._last_symbols_refresh_ts) >= float(self.refresh_symbols_sec)

    def _refresh_symbols_from_db(self) -> None:
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT symbol_id
                        FROM symbols
                        WHERE exchange_id=%s AND is_active=true
                        ORDER BY symbol_id
                        """,
                        (self.exchange_id,),
                    )
                    rows = cur.fetchall() or []
            new_ids = sorted(set(int(r[0]) for r in rows))

            with self._lock:
                old_ids = self.symbol_ids

                if not new_ids:
                    self._last_symbols_refresh_ts = time.time()
                    self.logger.warning("[MarketState5m] symbols refresh -> got 0 rows, keep old=%d", len(old_ids))
                    return

                if new_ids != old_ids:
                    self.symbol_ids = new_ids
                    self._i = 0
                    self.logger.info("[MarketState5m] symbols updated: %d -> %d (reset cursor)", len(old_ids), len(new_ids))

            self._last_symbols_refresh_ts = time.time()

        except Exception:
            self._last_symbols_refresh_ts = time.time()
            self.logger.exception("[MarketState5m] symbols refresh failed (keep old list)")

    # -------------------------
    # DB helpers
    # -------------------------
    def _apply_timeouts(self, cur) -> None:
        """
        SET/SET LOCAL не параметризуются через $1.
        Используем set_config(), который параметризуется нормально.
        3й аргумент true = local (для текущей транзакции).
        """
        cur.execute("SELECT set_config('statement_timeout', %s, true);", (str(self.statement_timeout_ms),))
        cur.execute("SELECT set_config('lock_timeout', %s, true);", (str(self.lock_timeout_ms),))

    def _is_retryable_db_error(self, e: BaseException) -> bool:
        if isinstance(e, psycopg.errors.DeadlockDetected):
            return True
        if isinstance(e, psycopg.errors.LockNotAvailable):
            return True
        if isinstance(e, psycopg.errors.QueryCanceled):
            return True
        if isinstance(e, psycopg.errors.SerializationFailure):
            return True
        if isinstance(e, psycopg.OperationalError):
            return True
        return False

    def _exec_upsert_with_retry(self, chunk: List[int]) -> None:
        attempt = 0
        while True:
            if self.stop_event.is_set():
                return

            try:
                with self.pool.connection() as conn:
                    with conn.cursor() as cur:
                        self._apply_timeouts(cur)
                        cur.execute(UPSERT_CHUNK_SQL, (self.exchange_id, chunk, self.lookback_minutes))
                    conn.commit()
                return

            except Exception as e:
                attempt += 1
                if attempt > self.retries or not self._is_retryable_db_error(e):
                    raise

                sleep_s = self.base_sleep_sec * (2 ** (attempt - 1))
                sleep_s = min(sleep_s, 5.0)
                sleep_s = sleep_s * (0.7 + 0.6 * random.random())

                self.logger.warning(
                    "[MarketState5m] retryable error (attempt %d/%d) sleep=%.2fs err=%s",
                    attempt,
                    self.retries,
                    sleep_s,
                    type(e).__name__,
                )
                self.stop_event.wait(sleep_s)

    # -------------------------
    # Main loop
    # -------------------------
    def run(self) -> None:
        if not self.symbol_ids and self.refresh_symbols_sec > 0:
            self._refresh_symbols_from_db()

        while not self.stop_event.is_set():
            if self._should_refresh_symbols():
                self._refresh_symbols_from_db()

            with self._lock:
                chunk = self._next_chunk_locked() if self.symbol_ids else []

            if not chunk:
                now = time.time()
                if now - self._last_no_symbols_log_ts > 30:
                    self._last_no_symbols_log_ts = now
                    self.logger.warning("[MarketState5m] no symbols -> sleep")
                self.stop_event.wait(max(2.0, self.poll_sec))
                continue

            t0 = time.time()
            try:
                self._exec_upsert_with_retry(chunk)
                dt = time.time() - t0
                self.logger.info(
                    "[MarketState5m] upsert ok chunk=%d (%.2fs) symbols=[%d..%d]",
                    len(chunk),
                    dt,
                    chunk[0],
                    chunk[-1],
                )
            except Exception:
                self.logger.exception("[MarketState5m] upsert failed")

            self.stop_event.wait(self.poll_sec)

    def _next_chunk_locked(self) -> List[int]:
        ids = self.symbol_ids
        n = len(ids)
        if n == 0:
            return []

        k = min(self.symbols_per_cycle, n)
        start = self._i
        end = start + k
        self._i = end % n

        if end <= n:
            return ids[start:end]
        return ids[start:n] + ids[0 : end - n]
