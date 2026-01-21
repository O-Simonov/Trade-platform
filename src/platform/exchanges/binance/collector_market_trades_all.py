# src/platform/exchanges/binance/collector_market_trades_all.py
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Optional, Dict, List, Tuple
from collections import deque

from psycopg.types.json import Json

log = logging.getLogger("collector.market_trades_all")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


@dataclass(eq=False)
class MarketTradesAllWorker(threading.Thread):
    """
    ✅ MarketTrades collector по всем активным символам.

    Делает:
      - читает active symbols из таблицы symbols
      - циклом проходит батчами
      - для каждого symbol -> REST aggTrades from_id
      - пишет в market_trades (dedup ON CONFLICT DO NOTHING)
      - апсертит 5m агрегации в candles_trades_agg
      - CVD пересчитывает редко (раз в N секунд на символ), чтобы не убить БД
    """

    # (не обязательно, но можно оставить)
    __hash__ = object.__hash__

    pool: Any
    rest: Any
    exchange_id: int

    poll_sleep: float = 0.05
    req_sleep_sec: float = 0.08
    symbols_per_cycle: int = 6

    overlap_minutes: int = 120
    lag_tail_sec: int = 120

    refresh_symbols_sec: int = 300
    max_symbols: int = 0  # 0 = все, иначе ограничим сверху (для теста)

    recalc_cvd_every_sec: int = 90
    cvd_hours_back: int = 24

    stop_event: Optional[threading.Event] = None

    def __post_init__(self) -> None:
        # ✅ ВАЖНО: явный вызов Thread.__init__
        threading.Thread.__init__(self, daemon=True, name="MarketTradesAllWorker")

        self.stop_event = self.stop_event or threading.Event()

        self.symbols_per_cycle = max(1, int(self.symbols_per_cycle))
        self.poll_sleep = max(0.01, float(self.poll_sleep))
        self.req_sleep_sec = max(0.01, float(self.req_sleep_sec))

        self.refresh_symbols_sec = max(10, int(self.refresh_symbols_sec))
        self.overlap_minutes = max(10, int(self.overlap_minutes))
        self.lag_tail_sec = max(10, int(self.lag_tail_sec))

        self.recalc_cvd_every_sec = max(30, int(self.recalc_cvd_every_sec))
        self.cvd_hours_back = max(1, int(self.cvd_hours_back))

        self._last_symbols_refresh_ts = 0.0

        # symbol_id -> symbol str
        self._symbols: Dict[int, str] = {}
        self._q: deque[Tuple[int, str]] = deque()

        # symbol_id -> next from_id (or None)
        self._from_id: Dict[int, Optional[int]] = {}

        # symbol_id -> last_ts (market_trades max(ts))
        self._last_ts: Dict[int, Optional[datetime]] = {}

        # symbol_id -> last time we recalculated cvd
        self._last_cvd_recalc_ts: Dict[int, float] = {}

        log.info(
            "[MarketTradesAll] init poll_sleep=%.3fs req_sleep=%.3fs batch=%d refresh_symbols=%ds overlap=%dm lag_tail=%ds",
            self.poll_sleep,
            self.req_sleep_sec,
            self.symbols_per_cycle,
            self.refresh_symbols_sec,
            self.overlap_minutes,
            self.lag_tail_sec,
        )

    # ============================================================
    # DB helpers
    # ============================================================

    def _fetch_active_symbols(self) -> Dict[int, str]:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol_id, symbol
                    FROM symbols
                    WHERE exchange_id=%s AND is_active=true
                    ORDER BY symbol_id
                    """,
                    (int(self.exchange_id),),
                )
                rows = cur.fetchall() or []

        out: Dict[int, str] = {}
        for sid, sym in rows:
            try:
                out[int(sid)] = str(sym).upper()
            except Exception:
                continue
        return out

    def _load_last_trade_ids(self, symbol_ids: List[int]) -> Dict[int, int]:
        if not symbol_ids:
            return {}

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol_id, COALESCE(MAX(trade_id), 0)
                    FROM market_trades
                    WHERE exchange_id=%s AND symbol_id = ANY(%s)
                    GROUP BY symbol_id
                    """,
                    (int(self.exchange_id), symbol_ids),
                )
                rows = cur.fetchall() or []

        return {int(sid): int(mx) for sid, mx in rows}

    def _load_last_ts_map(self, symbol_ids: List[int]) -> Dict[int, Optional[datetime]]:
        if not symbol_ids:
            return {}

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol_id, MAX(ts)
                    FROM market_trades
                    WHERE exchange_id=%s AND symbol_id = ANY(%s)
                    GROUP BY symbol_id
                    """,
                    (int(self.exchange_id), symbol_ids),
                )
                rows = cur.fetchall() or []

        return {int(sid): (ts if ts is not None else None) for sid, ts in rows}

    # ============================================================
    # Normalize + Insert
    # ============================================================

    def _normalize_agg_trade(self, *, symbol_id: int, item: dict[str, Any]) -> dict[str, Any]:
        trade_id = item.get("a", item.get("aggTradeId"))
        price = item.get("p", item.get("price"))
        qty = item.get("q", item.get("qty"))
        ts_ms = item.get("T", item.get("timestamp"))

        if trade_id is None or ts_ms is None:
            raise ValueError(f"Bad aggTrade payload: {item!r}")

        is_buyer_maker = item.get("m", item.get("isBuyerMaker"))
        taker_side = "SELL" if bool(is_buyer_maker) else "BUY"

        price_f = float(price)
        qty_f = float(qty)
        quote_qty = price_f * qty_f

        return {
            "exchange_id": int(self.exchange_id),
            "symbol_id": int(symbol_id),
            "trade_id": int(trade_id),
            "ts": _ms_to_dt(int(ts_ms)),
            "price": price_f,
            "qty": qty_f,
            "quote_qty": quote_qty,
            "taker_side": taker_side,
            "source": "aggTrades",
            "raw_json": item,
        }

    def _insert_trades(self, rows: List[dict[str, Any]]) -> int:
        if not rows:
            return 0

        sql = """
        INSERT INTO market_trades (
          exchange_id, symbol_id, trade_id,
          ts, price, qty, quote_qty,
          taker_side, source, raw_json
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (exchange_id, symbol_id, trade_id) DO NOTHING
        """

        params: List[Tuple[Any, ...]] = []
        for r in rows:
            raw = r.get("raw_json")
            if isinstance(raw, (dict, list)):
                raw = Json(raw)
            elif raw is not None and not isinstance(raw, (str, bytes)):
                raw = Json({"raw": str(raw)})

            params.append(
                (
                    r["exchange_id"],
                    r["symbol_id"],
                    r["trade_id"],
                    r["ts"],
                    r["price"],
                    r["qty"],
                    r["quote_qty"],
                    r["taker_side"],
                    r.get("source"),
                    raw,
                )
            )

        # ✅ возвращаем реальное число вставленных
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, params)
                inserted = int(cur.rowcount or 0)
            conn.commit()

        return inserted

    # ============================================================
    # Aggregation 5m + CVD
    # ============================================================

    def _upsert_agg_5m(self, *, symbol_id: int, ts_from: datetime, ts_to: datetime) -> int:
        sql = """
        WITH agg AS (
          SELECT
            exchange_id,
            symbol_id,
            '5m'::text AS interval,
            date_trunc('hour', ts)
              + (floor(extract(minute from ts)::numeric / 5) * interval '5 minute') AS open_time,
            SUM(quote_qty) AS quote_volume,
            COUNT(*) AS trades,
            SUM(CASE WHEN taker_side='BUY'  THEN quote_qty ELSE 0 END) AS taker_buy_quote,
            SUM(CASE WHEN taker_side='SELL' THEN quote_qty ELSE 0 END) AS taker_sell_quote,
            SUM(CASE WHEN taker_side='BUY'  THEN quote_qty ELSE -quote_qty END) AS delta_quote
          FROM market_trades
          WHERE exchange_id=%s
            AND symbol_id=%s
            AND ts >= %s::timestamptz
            AND ts <  %s::timestamptz
          GROUP BY 1,2,3,4
        ),
        up AS (
          INSERT INTO candles_trades_agg(
            exchange_id, symbol_id, interval, open_time,
            quote_volume, trades, taker_buy_quote, taker_sell_quote,
            delta_quote, cvd_quote, updated_at
          )
          SELECT
            a.exchange_id, a.symbol_id, a.interval, a.open_time,
            a.quote_volume, a.trades, a.taker_buy_quote, a.taker_sell_quote,
            a.delta_quote,
            0::double precision AS cvd_quote,
            now()
          FROM agg a
          ON CONFLICT (exchange_id, symbol_id, interval, open_time) DO UPDATE SET
            quote_volume     = EXCLUDED.quote_volume,
            trades           = EXCLUDED.trades,
            taker_buy_quote  = EXCLUDED.taker_buy_quote,
            taker_sell_quote = EXCLUDED.taker_sell_quote,
            delta_quote      = EXCLUDED.delta_quote,
            updated_at       = now()
          RETURNING open_time
        )
        SELECT COUNT(*) FROM up;
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(self.exchange_id), int(symbol_id), ts_from, ts_to))
                n = int(cur.fetchone()[0])
            conn.commit()
        return n

    def _recalc_cvd_5m(self, *, symbol_id: int, hours_back: int) -> int:
        sql = """
        WITH w AS (
          SELECT
            exchange_id, symbol_id, interval, open_time,
            SUM(delta_quote) OVER (
              PARTITION BY exchange_id, symbol_id, interval
              ORDER BY open_time
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cvd
          FROM candles_trades_agg
          WHERE exchange_id=%s AND symbol_id=%s AND interval='5m'
            AND open_time >= now() - (%s || ' hours')::interval
        )
        UPDATE candles_trades_agg t
        SET cvd_quote = w.cvd,
            updated_at = now()
        FROM w
        WHERE t.exchange_id=w.exchange_id
          AND t.symbol_id=w.symbol_id
          AND t.interval=w.interval
          AND t.open_time=w.open_time;
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(self.exchange_id), int(symbol_id), int(hours_back)))
                n = cur.rowcount
            conn.commit()
        return int(n or 0)

    # ============================================================
    # Symbols refresh / Scheduler
    # ============================================================

    def _should_refresh_symbols(self) -> bool:
        return (time.time() - self._last_symbols_refresh_ts) >= float(self.refresh_symbols_sec)

    def _refresh_symbols(self) -> None:
        try:
            new_map = self._fetch_active_symbols()

            if self.max_symbols > 0:
                keys = sorted(new_map.keys())[: int(self.max_symbols)]
                new_map = {k: new_map[k] for k in keys}

            if not new_map:
                log.warning("[MarketTradesAll] refresh_symbols -> got 0 rows, keep old=%d", len(self._symbols))
                self._last_symbols_refresh_ts = time.time()
                return

            old_map = self._symbols
            self._symbols = dict(new_map)

            # rebuild queue
            self._q = deque([(sid, sym) for sid, sym in sorted(self._symbols.items(), key=lambda x: x[0])])

            # ✅ Инициализируем ТОЛЬКО новые symbol_id (а старые не сбрасываем)
            new_ids = [sid for sid in self._symbols.keys() if sid not in old_map]
            if new_ids:
                last_ids = self._load_last_trade_ids(new_ids)
                last_ts = self._load_last_ts_map(new_ids)

                for sid in new_ids:
                    mx = int(last_ids.get(sid, 0))
                    self._from_id[sid] = (mx + 1) if mx > 0 else None
                    self._last_ts[sid] = last_ts.get(sid)
                    self._last_cvd_recalc_ts[sid] = 0.0

            self._last_symbols_refresh_ts = time.time()
            log.info("[MarketTradesAll] symbols refreshed: %d -> %d (new=%d)", len(old_map), len(self._symbols), len(new_ids))

        except Exception:
            self._last_symbols_refresh_ts = time.time()
            log.exception("[MarketTradesAll] refresh_symbols failed (keep old)")

    def _tail_sync_if_needed(self, *, symbol_id: int, symbol: str) -> None:
        max_ts = self._last_ts.get(symbol_id)
        if max_ts is None:
            need_tail = True
        else:
            lag = (_utc_now() - max_ts).total_seconds()
            need_tail = lag > float(self.lag_tail_sec)

        if not need_tail:
            return

        try:
            batch = self.rest.get_agg_trades(symbol=symbol, from_id=None, limit=1000)
        except Exception:
            log.exception("[MarketTradesAll] tail_sync REST error symbol=%s", symbol)
            return

        if not batch:
            return

        normed: List[dict[str, Any]] = []
        max_id = None
        min_ts = None
        max_ts2 = None

        for item in batch:
            if not isinstance(item, dict):
                continue
            r = self._normalize_agg_trade(symbol_id=symbol_id, item=item)
            normed.append(r)

            tid = int(r["trade_id"])
            ts = r["ts"]

            if max_id is None or tid > max_id:
                max_id = tid
            if min_ts is None or ts < min_ts:
                min_ts = ts
            if max_ts2 is None or ts > max_ts2:
                max_ts2 = ts

        inserted = self._insert_trades(normed)

        if max_id is not None:
            self._from_id[symbol_id] = int(max_id) + 1
        if max_ts2 is not None:
            self._last_ts[symbol_id] = max_ts2

        if inserted > 0 and min_ts and max_ts2:
            buf = timedelta(minutes=int(self.overlap_minutes))
            self._upsert_agg_5m(symbol_id=symbol_id, ts_from=min_ts - buf, ts_to=max_ts2 + buf)

        log.info("[MarketTradesAll] tail_sync symbol=%s fetched=%d inserted=%d", symbol, len(batch), inserted)

    # ============================================================
    # Main loop
    # ============================================================

    def run(self) -> None:
        self._refresh_symbols()

        backoff = 1.0
        while not self.stop_event.is_set():
            try:
                if self._should_refresh_symbols():
                    self._refresh_symbols()

                if not self._q:
                    log.warning("[MarketTradesAll] no symbols -> sleep")
                    self.stop_event.wait(2.0)
                    continue

                batch: List[Tuple[int, str]] = []
                for _ in range(min(self.symbols_per_cycle, len(self._q))):
                    sid, sym = self._q.popleft()
                    batch.append((sid, sym))
                    self._q.append((sid, sym))

                total_inserted = 0
                total_up_agg = 0

                for sid, sym in batch:
                    if self.stop_event.is_set():
                        break

                    self._tail_sync_if_needed(symbol_id=sid, symbol=sym)
                    from_id = self._from_id.get(sid)

                    try:
                        trades = self.rest.get_agg_trades(symbol=sym, from_id=from_id, limit=1000)
                    except Exception:
                        log.exception("[MarketTradesAll] REST error symbol=%s", sym)
                        continue

                    if not trades:
                        if self.stop_event.wait(self.poll_sleep):
                            return
                        continue

                    normed: List[dict[str, Any]] = []
                    max_id = None
                    min_ts = None
                    max_ts2 = None

                    for item in trades:
                        if not isinstance(item, dict):
                            continue
                        r = self._normalize_agg_trade(symbol_id=sid, item=item)
                        normed.append(r)

                        tid = int(r["trade_id"])
                        ts = r["ts"]

                        if max_id is None or tid > max_id:
                            max_id = tid
                        if min_ts is None or ts < min_ts:
                            min_ts = ts
                        if max_ts2 is None or ts > max_ts2:
                            max_ts2 = ts

                    inserted = self._insert_trades(normed)
                    total_inserted += int(inserted)

                    if max_id is not None:
                        self._from_id[sid] = int(max_id) + 1
                    if max_ts2 is not None:
                        self._last_ts[sid] = max_ts2

                    if inserted > 0 and min_ts and max_ts2:
                        buf = timedelta(minutes=int(self.overlap_minutes))
                        upn = self._upsert_agg_5m(symbol_id=sid, ts_from=min_ts - buf, ts_to=max_ts2 + buf)
                        total_up_agg += int(upn)

                        now_ts = time.time()
                        last_recalc = float(self._last_cvd_recalc_ts.get(sid, 0.0))
                        if (now_ts - last_recalc) >= float(self.recalc_cvd_every_sec):
                            try:
                                self._recalc_cvd_5m(symbol_id=sid, hours_back=self.cvd_hours_back)
                                self._last_cvd_recalc_ts[sid] = now_ts
                            except Exception:
                                log.exception("[MarketTradesAll] cvd recalc failed symbol=%s", sym)

                    if self.stop_event.wait(self.req_sleep_sec):
                        return

                log.info(
                    "[MarketTradesAll] cycle batch=%d inserted=%d agg_upserts=%d",
                    len(batch), total_inserted, total_up_agg
                )

                backoff = 1.0

            except Exception:
                log.exception("[MarketTradesAll] loop error")
                self.stop_event.wait(backoff)
                backoff = min(backoff * 2.0, 60.0)


def start_market_trades_all_collector(
    *,
    pool: Any,
    rest: Any,
    exchange_id: int,
    cfg: dict,
    stop_event: threading.Event,
    watermarks: Any | None = None,   # ✅ добавили, чтобы не было warning
    **kwargs: Any,                   # ✅ на будущее (если ещё что-то прилетит)
) -> MarketTradesAllWorker:
    w = MarketTradesAllWorker(
        pool=pool,
        rest=rest,
        exchange_id=int(exchange_id),

        poll_sleep=float(cfg.get("poll_sleep", 0.05)),
        req_sleep_sec=float(cfg.get("req_sleep_sec", 0.08)),
        symbols_per_cycle=int(cfg.get("symbols_per_cycle", 6)),

        overlap_minutes=int(cfg.get("overlap_minutes", 120)),
        lag_tail_sec=int(cfg.get("lag_tail_sec", 120)),

        refresh_symbols_sec=int(cfg.get("refresh_symbols_sec", 300)),
        max_symbols=int(cfg.get("max_symbols", 0)),

        recalc_cvd_every_sec=int(cfg.get("recalc_cvd_every_sec", 90)),
        cvd_hours_back=int(cfg.get("cvd_hours_back", 24)),

        stop_event=stop_event,
    )
    w.start()
    log.info("[MarketTradesAll] worker started")
    return w

