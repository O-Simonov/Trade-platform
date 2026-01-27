# src/platform/exchanges/binance/collector_market_trades_all.py
from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Optional, Dict, List, Tuple
from collections import deque

from psycopg.types.json import Json

from src.platform.exchanges.binance.ws import BinanceWS, WS_STREAM_BASE

log = logging.getLogger("collector.market_trades_all")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)


def _chunk_streams_by_limits(
    streams: List[str],
    *,
    base_url: str,
    max_streams_per_conn: int,
    url_max_len: int,
) -> List[List[str]]:
    out: List[List[str]] = []
    cur: List[str] = []

    def _url_len(ss: List[str]) -> int:
        # base + "s1/s2/s3" (плюс разделители)
        return len(base_url) + sum(len(x) + 1 for x in ss)

    for s in streams:
        if not cur:
            cur = [s]
            continue

        test = cur + [s]
        if len(test) > max_streams_per_conn or _url_len(test) > url_max_len:
            out.append(cur)
            cur = [s]
        else:
            cur = test

    if cur:
        out.append(cur)

    return out


# ============================================================
# REST worker (fallback)
# ============================================================

@dataclass(eq=False)
class MarketTradesAllWorker(threading.Thread):
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
    max_symbols: int = 0

    recalc_cvd_every_sec: int = 90
    cvd_hours_back: int = 24

    stop_event: Optional[threading.Event] = None

    def __post_init__(self) -> None:
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
        self._symbols: Dict[int, str] = {}
        self._q: deque[Tuple[int, str]] = deque()

        self._from_id: Dict[int, Optional[int]] = {}
        self._last_ts: Dict[int, Optional[datetime]] = {}
        self._last_cvd_recalc_ts: Dict[int, float] = {}

        log.info(
            "[MarketTradesAll][REST] init poll_sleep=%.3fs req_sleep=%.3fs batch=%d refresh=%ds overlap=%dm lag_tail=%ds",
            self.poll_sleep,
            self.req_sleep_sec,
            self.symbols_per_cycle,
            self.refresh_symbols_sec,
            self.overlap_minutes,
            self.lag_tail_sec,
        )

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

        return {
            "exchange_id": int(self.exchange_id),
            "symbol_id": int(symbol_id),
            "trade_id": int(trade_id),
            "ts": _ms_to_dt(int(ts_ms)),
            "price": price_f,
            "qty": qty_f,
            "quote_qty": price_f * qty_f,
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

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, params)
                inserted = int(cur.rowcount or 0)
            conn.commit()

        return inserted

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

    def _should_refresh_symbols(self) -> bool:
        return (time.time() - self._last_symbols_refresh_ts) >= float(self.refresh_symbols_sec)

    def _refresh_symbols(self) -> None:
        try:
            new_map = self._fetch_active_symbols()
            if self.max_symbols > 0:
                keys = sorted(new_map.keys())[: int(self.max_symbols)]
                new_map = {k: new_map[k] for k in keys}

            if not new_map:
                log.warning("[MarketTradesAll][REST] refresh_symbols -> got 0 rows, keep old=%d", len(self._symbols))
                self._last_symbols_refresh_ts = time.time()
                return

            old_map = self._symbols
            self._symbols = dict(new_map)
            self._q = deque([(sid, sym) for sid, sym in sorted(self._symbols.items(), key=lambda x: x[0])])

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
            log.info("[MarketTradesAll][REST] symbols refreshed: %d -> %d (new=%d)", len(old_map), len(self._symbols), len(new_ids))

        except Exception:
            self._last_symbols_refresh_ts = time.time()
            log.exception("[MarketTradesAll][REST] refresh_symbols failed (keep old)")

    def _tail_sync_if_needed(self, *, symbol_id: int, symbol: str) -> None:
        max_ts = self._last_ts.get(symbol_id)
        need_tail = True if max_ts is None else ((_utc_now() - max_ts).total_seconds() > float(self.lag_tail_sec))
        if not need_tail:
            return

        try:
            batch = self.rest.get_agg_trades(symbol=symbol, from_id=None, limit=1000)
        except Exception:
            log.exception("[MarketTradesAll][REST] tail_sync REST error symbol=%s", symbol)
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
            max_id = tid if max_id is None or tid > max_id else max_id
            min_ts = ts if min_ts is None or ts < min_ts else min_ts
            max_ts2 = ts if max_ts2 is None or ts > max_ts2 else max_ts2

        inserted = self._insert_trades(normed)
        if max_id is not None:
            self._from_id[symbol_id] = int(max_id) + 1
        if max_ts2 is not None:
            self._last_ts[symbol_id] = max_ts2

        if inserted > 0 and min_ts and max_ts2:
            buf = timedelta(minutes=int(self.overlap_minutes))
            self._upsert_agg_5m(symbol_id=symbol_id, ts_from=min_ts - buf, ts_to=max_ts2 + buf)

        log.info("[MarketTradesAll][REST] tail_sync symbol=%s fetched=%d inserted=%d", symbol, len(batch), inserted)

    def run(self) -> None:
        self._refresh_symbols()

        backoff = 1.0
        while not self.stop_event.is_set():
            try:
                if self._should_refresh_symbols():
                    self._refresh_symbols()

                if not self._q:
                    log.warning("[MarketTradesAll][REST] no symbols -> sleep")
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
                        log.exception("[MarketTradesAll][REST] REST error symbol=%s", sym)
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
                        max_id = tid if max_id is None or tid > max_id else max_id
                        min_ts = ts if min_ts is None or ts < min_ts else min_ts
                        max_ts2 = ts if max_ts2 is None or ts > max_ts2 else max_ts2

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
                                log.exception("[MarketTradesAll][REST] cvd recalc failed symbol=%s", sym)

                    if self.stop_event.wait(self.req_sleep_sec):
                        return

                log.info("[MarketTradesAll][REST] cycle batch=%d inserted=%d agg_upserts=%d", len(batch), total_inserted, total_up_agg)
                backoff = 1.0

            except Exception:
                log.exception("[MarketTradesAll][REST] loop error")
                self.stop_event.wait(backoff)
                backoff = min(backoff * 2.0, 60.0)


# ============================================================
# WS worker + smart tail_sync + WARMUP
# ============================================================

@dataclass(eq=False)
class MarketTradesAllWSWorker(threading.Thread):
    __hash__ = object.__hash__

    pool: Any
    rest: Any
    exchange_id: int

    refresh_symbols_sec: int = 600
    max_symbols: int = 0

    # буферизация
    flush_sec: float = 0.75
    flush_max_rows: int = 5000

    # WS chunking
    ws_base_url: str = ""
    ws_max_streams_per_conn: int = 160
    ws_url_max_len: int = 3500
    ws_ping_interval: int = 60
    ws_ping_timeout: int = 30

    # agg/cvd
    overlap_minutes: int = 60
    agg_upsert_every_sec: int = 10
    recalc_cvd_every_sec: int = 120
    cvd_hours_back: int = 24

    # normal tail sync (REST)
    tail_sync_enabled: bool = True
    lag_tail_sec: int = 600
    tail_sync_every_sec: int = 5
    tail_sync_symbols_per_cycle: int = 2
    tail_sync_limit: int = 1000
    tail_sync_max_pages_per_symbol: int = 3
    tail_sync_max_requests_per_cycle: int = 6
    tail_sync_min_symbol_interval_sec: int = 45
    tail_sync_safety_lag_sec: int = 3

    # ✅ WARMUP (ускоренный догон после рестарта)
    warmup_enabled: bool = True
    warmup_sec: int = 180
    warmup_lag_tail_sec: int = 60
    warmup_tail_sync_every_sec: int = 3
    warmup_tail_sync_symbols_per_cycle: int = 3
    warmup_tail_sync_max_requests_per_cycle: int = 8
    warmup_tail_sync_max_pages_per_symbol: int = 4
    warmup_tail_sync_min_symbol_interval_sec: int = 20

    stop_event: Optional[threading.Event] = None

    def __post_init__(self) -> None:
        threading.Thread.__init__(self, daemon=True, name="MarketTradesAllWSWorker")
        self.stop_event = self.stop_event or threading.Event()

        self.refresh_symbols_sec = max(10, int(self.refresh_symbols_sec))
        self.max_symbols = max(0, int(self.max_symbols))

        self.flush_sec = max(0.2, float(self.flush_sec))
        self.flush_max_rows = max(200, int(self.flush_max_rows))

        self.ws_max_streams_per_conn = max(20, int(self.ws_max_streams_per_conn))
        self.ws_url_max_len = max(800, int(self.ws_url_max_len))
        self.ws_ping_interval = max(10, int(self.ws_ping_interval))
        self.ws_ping_timeout = max(5, int(self.ws_ping_timeout))

        self.overlap_minutes = max(10, int(self.overlap_minutes))
        self.agg_upsert_every_sec = max(2, int(self.agg_upsert_every_sec))
        self.recalc_cvd_every_sec = max(30, int(self.recalc_cvd_every_sec))
        self.cvd_hours_back = max(1, int(self.cvd_hours_back))

        self.tail_sync_enabled = bool(self.tail_sync_enabled)
        self.lag_tail_sec = max(30, int(self.lag_tail_sec))
        self.tail_sync_every_sec = max(1, int(self.tail_sync_every_sec))
        self.tail_sync_symbols_per_cycle = max(1, int(self.tail_sync_symbols_per_cycle))
        self.tail_sync_limit = max(100, int(self.tail_sync_limit))
        self.tail_sync_max_pages_per_symbol = max(1, int(self.tail_sync_max_pages_per_symbol))
        self.tail_sync_max_requests_per_cycle = max(1, int(self.tail_sync_max_requests_per_cycle))
        self.tail_sync_min_symbol_interval_sec = max(5, int(self.tail_sync_min_symbol_interval_sec))
        self.tail_sync_safety_lag_sec = max(1, int(self.tail_sync_safety_lag_sec))

        # warmup
        self.warmup_enabled = bool(self.warmup_enabled)
        self.warmup_sec = max(0, int(self.warmup_sec))
        self.warmup_lag_tail_sec = max(10, int(self.warmup_lag_tail_sec))
        self.warmup_tail_sync_every_sec = max(1, int(self.warmup_tail_sync_every_sec))
        self.warmup_tail_sync_symbols_per_cycle = max(1, int(self.warmup_tail_sync_symbols_per_cycle))
        self.warmup_tail_sync_max_requests_per_cycle = max(1, int(self.warmup_tail_sync_max_requests_per_cycle))
        self.warmup_tail_sync_max_pages_per_symbol = max(1, int(self.warmup_tail_sync_max_pages_per_symbol))
        self.warmup_tail_sync_min_symbol_interval_sec = max(1, int(self.warmup_tail_sync_min_symbol_interval_sec))

        self._start_ts = time.time()
        self._last_symbols_refresh_ts = 0.0
        self._last_tail_sync_ts = 0.0

        self._symbols: Dict[int, str] = {}
        self._sym_to_id: Dict[str, int] = {}

        self._from_id: Dict[int, Optional[int]] = {}
        self._last_ts: Dict[int, Optional[datetime]] = {}
        self._last_agg_upsert_ts: Dict[int, float] = {}
        self._last_cvd_recalc_ts: Dict[int, float] = {}

        # per-symbol tail cooldown
        self._last_tail_symbol_ts: Dict[int, float] = {}

        self._buf_lock = threading.Lock()
        self._buf: List[dict[str, Any]] = []

        self._ws_list: List[BinanceWS] = []

        log.info(
            "[MarketTradesAll][WS] init refresh=%ds conns_streams=%d flush=%.2fs/%d agg_every=%ds tail_sync=%s warmup=%s/%ds",
            self.refresh_symbols_sec,
            self.ws_max_streams_per_conn,
            self.flush_sec,
            self.flush_max_rows,
            self.agg_upsert_every_sec,
            self.tail_sync_enabled,
            self.warmup_enabled,
            self.warmup_sec,
        )

    # ---------------- dynamic params ----------------

    def _in_warmup(self) -> bool:
        if not self.warmup_enabled or self.warmup_sec <= 0:
            return False
        return (time.time() - float(self._start_ts)) < float(self.warmup_sec)

    def _tail_params(self) -> dict[str, int]:
        if self._in_warmup():
            return {
                "lag_tail_sec": int(self.warmup_lag_tail_sec),
                "every_sec": int(self.warmup_tail_sync_every_sec),
                "symbols_per_cycle": int(self.warmup_tail_sync_symbols_per_cycle),
                "limit": int(self.tail_sync_limit),
                "max_pages": int(self.warmup_tail_sync_max_pages_per_symbol),
                "max_requests": int(self.warmup_tail_sync_max_requests_per_cycle),
                "min_symbol_interval_sec": int(self.warmup_tail_sync_min_symbol_interval_sec),
            }
        return {
            "lag_tail_sec": int(self.lag_tail_sec),
            "every_sec": int(self.tail_sync_every_sec),
            "symbols_per_cycle": int(self.tail_sync_symbols_per_cycle),
            "limit": int(self.tail_sync_limit),
            "max_pages": int(self.tail_sync_max_pages_per_symbol),
            "max_requests": int(self.tail_sync_max_requests_per_cycle),
            "min_symbol_interval_sec": int(self.tail_sync_min_symbol_interval_sec),
        }

    # ---------------- DB helpers ----------------

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

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, params)
                inserted = int(cur.rowcount or 0)
            conn.commit()

        return inserted

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

    # ---------------- WS normalize ----------------

    def _normalize_ws_trade(self, *, symbol_id: int, data: dict[str, Any]) -> dict[str, Any]:
        trade_id = data.get("a")
        ts_ms = data.get("T")
        if trade_id is None or ts_ms is None:
            raise ValueError("bad ws aggTrade")

        price_f = float(data.get("p"))
        qty_f = float(data.get("q"))
        is_buyer_maker = bool(data.get("m"))
        taker_side = "SELL" if is_buyer_maker else "BUY"

        return {
            "exchange_id": int(self.exchange_id),
            "symbol_id": int(symbol_id),
            "trade_id": int(trade_id),
            "ts": _ms_to_dt(int(ts_ms)),
            "price": price_f,
            "qty": qty_f,
            "quote_qty": price_f * qty_f,
            "taker_side": taker_side,
            "source": "ws@aggTrade",
            "raw_json": data,
        }

    def _on_ws_message(self, message: str) -> None:
        try:
            obj = json.loads(message)
        except Exception:
            return
        if not isinstance(obj, dict):
            return
        data = obj.get("data")
        if not isinstance(data, dict):
            return

        sym = data.get("s")
        if not sym:
            return
        sym = str(sym).upper()

        sid = self._sym_to_id.get(sym)
        if sid is None:
            return

        try:
            row = self._normalize_ws_trade(symbol_id=int(sid), data=data)
        except Exception:
            return

        with self._buf_lock:
            self._buf.append(row)

    # ---------------- WS control ----------------

    def _stop_ws(self) -> None:
        for ws in self._ws_list:
            try:
                ws.stop()
            except Exception:
                pass
        self._ws_list.clear()

    def _start_ws(self) -> None:
        self._stop_ws()

        base = (self.ws_base_url or "").strip() or str(WS_STREAM_BASE)
        symbols = list(self._sym_to_id.keys())
        streams = [f"{s.lower()}@aggTrade" for s in symbols]

        chunks = _chunk_streams_by_limits(
            streams,
            base_url=base,
            max_streams_per_conn=int(self.ws_max_streams_per_conn),
            url_max_len=int(self.ws_url_max_len),
        )

        for i, ch in enumerate(chunks, start=1):
            url = base + "/".join(ch)
            ws = BinanceWS(
                name=f"MarketTradesAllWS#{i}",
                url=url,
                on_message=self._on_ws_message,
                ping_interval=int(self.ws_ping_interval),
                ping_timeout=int(self.ws_ping_timeout),
            )
            ws.start()
            self._ws_list.append(ws)

        log.info("[MarketTradesAll][WS] started conns=%d streams=%d", len(self._ws_list), len(streams))

    # ---------------- symbols refresh ----------------

    def _should_refresh_symbols(self) -> bool:
        return (time.time() - self._last_symbols_refresh_ts) >= float(self.refresh_symbols_sec)

    def _refresh_symbols(self) -> None:
        try:
            new_map = self._fetch_active_symbols()
            if self.max_symbols > 0:
                keys = sorted(new_map.keys())[: int(self.max_symbols)]
                new_map = {k: new_map[k] for k in keys}

            if not new_map:
                log.warning("[MarketTradesAll][WS] refresh_symbols -> got 0 rows, keep old=%d", len(self._symbols))
                self._last_symbols_refresh_ts = time.time()
                return

            old_ids = set(self._symbols.keys())
            new_ids_set = set(new_map.keys())
            changed = (old_ids != new_ids_set)

            old_map = self._symbols
            self._symbols = dict(new_map)
            self._sym_to_id = {v: k for k, v in self._symbols.items()}

            now_ts = time.time()

            # новые символы
            new_ids = [sid for sid in self._symbols.keys() if sid not in old_map]
            if new_ids:
                last_ids = self._load_last_trade_ids(new_ids)
                last_ts = self._load_last_ts_map(new_ids)
                for sid in new_ids:
                    mx = int(last_ids.get(sid, 0))
                    self._from_id[sid] = (mx + 1) if mx > 0 else None
                    self._last_ts[sid] = last_ts.get(sid)
                    self._last_agg_upsert_ts[sid] = 0.0
                    self._last_cvd_recalc_ts[sid] = 0.0
                    self._last_tail_symbol_ts[sid] = now_ts - 10_000

            # первый запуск: инициализация всем
            if not old_map:
                all_ids = list(self._symbols.keys())
                last_ids = self._load_last_trade_ids(all_ids)
                last_ts = self._load_last_ts_map(all_ids)
                for sid in all_ids:
                    mx = int(last_ids.get(int(sid), 0))
                    self._from_id[int(sid)] = (mx + 1) if mx > 0 else None
                    self._last_ts[int(sid)] = last_ts.get(int(sid))
                    self._last_agg_upsert_ts[int(sid)] = 0.0
                    self._last_cvd_recalc_ts[int(sid)] = 0.0
                    self._last_tail_symbol_ts[int(sid)] = now_ts - 10_000

            self._last_symbols_refresh_ts = time.time()
            log.info("[MarketTradesAll][WS] symbols refreshed: %d -> %d (changed=%s)", len(old_map), len(self._symbols), changed)

            if changed:
                self._start_ws()
            elif not self._ws_list:
                self._start_ws()

        except Exception:
            self._last_symbols_refresh_ts = time.time()
            log.exception("[MarketTradesAll][WS] refresh_symbols failed (keep old)")

    # ---------------- SMART tail sync ----------------

    def _eligible_for_tail(self, sid: int, *, lag_tail_sec: int, min_symbol_interval_sec: int) -> bool:
        mx = self._last_ts.get(int(sid))
        lag = 10**9 if mx is None else (_utc_now() - mx).total_seconds()

        if lag <= float(lag_tail_sec):
            return False

        now = time.time()
        last = float(self._last_tail_symbol_ts.get(int(sid), 0.0))
        if (now - last) < float(min_symbol_interval_sec):
            return False

        return True

    def _tail_sync_some(self) -> None:
        if not self.tail_sync_enabled:
            return

        p = self._tail_params()

        now = time.time()
        if (now - self._last_tail_sync_ts) < float(p["every_sec"]):
            return
        self._last_tail_sync_ts = now

        safety_dt = _utc_now() - timedelta(seconds=int(self.tail_sync_safety_lag_sec))

        # кандидаты по лагу
        cand: List[Tuple[float, int, str]] = []
        for sid, sym in self._symbols.items():
            sid = int(sid)
            if not self._eligible_for_tail(sid, lag_tail_sec=p["lag_tail_sec"], min_symbol_interval_sec=p["min_symbol_interval_sec"]):
                continue

            mx = self._last_ts.get(sid)
            lag = 10**9 if mx is None else (_utc_now() - mx).total_seconds()
            cand.append((float(lag), sid, str(sym)))

        if not cand:
            return

        cand.sort(reverse=True)
        cand = cand[: int(p["symbols_per_cycle"])]

        requests_left = int(p["max_requests"])
        warm = self._in_warmup()

        for lag, sid, sym in cand:
            if self.stop_event.is_set() or requests_left <= 0:
                return

            self._last_tail_symbol_ts[sid] = time.time()

            from_id = self._from_id.get(sid)
            seed_tail = (from_id is None)

            pages = 0
            total_fetched = 0
            total_inserted = 0
            min_ts: Optional[datetime] = None
            max_ts: Optional[datetime] = None
            max_id: Optional[int] = None

            while pages < int(p["max_pages"]) and requests_left > 0:
                pages += 1
                requests_left -= 1

                try:
                    batch = self.rest.get_agg_trades(
                        symbol=sym,
                        from_id=None if seed_tail else from_id,
                        limit=int(p["limit"]),
                    )
                except Exception:
                    log.exception("[MarketTradesAll][WS] tail_sync REST error symbol=%s from_id=%s", sym, from_id)
                    break

                if not batch:
                    break

                seed_tail = False
                total_fetched += int(len(batch))

                normed: List[dict[str, Any]] = []
                for item in batch:
                    if not isinstance(item, dict):
                        continue
                    try:
                        trade_id = item.get("a", item.get("aggTradeId"))
                        ts_ms = item.get("T", item.get("timestamp"))
                        if trade_id is None or ts_ms is None:
                            continue
                        is_buyer_maker = item.get("m", item.get("isBuyerMaker"))
                        taker_side = "SELL" if bool(is_buyer_maker) else "BUY"
                        price_f = float(item.get("p", item.get("price")))
                        qty_f = float(item.get("q", item.get("qty")))
                        ts_dt = _ms_to_dt(int(ts_ms))
                    except Exception:
                        continue

                    row = {
                        "exchange_id": int(self.exchange_id),
                        "symbol_id": int(sid),
                        "trade_id": int(trade_id),
                        "ts": ts_dt,
                        "price": price_f,
                        "qty": qty_f,
                        "quote_qty": price_f * qty_f,
                        "taker_side": taker_side,
                        "source": "aggTrades",
                        "raw_json": item,
                    }
                    normed.append(row)

                    tid = int(row["trade_id"])
                    max_id = tid if max_id is None or tid > max_id else max_id
                    min_ts = ts_dt if min_ts is None or ts_dt < min_ts else min_ts
                    max_ts = ts_dt if max_ts is None or ts_dt > max_ts else max_ts

                inserted = self._insert_trades(normed)
                total_inserted += int(inserted)

                if max_id is not None:
                    from_id = int(max_id) + 1
                    self._from_id[sid] = from_id
                if max_ts is not None:
                    self._last_ts[sid] = max_ts

                # почти догнали хвост
                if max_ts is not None and max_ts >= safety_dt:
                    break

                # если пришло меньше лимита — хвост
                if len(batch) < int(p["limit"]):
                    break

            # агрегация (троттлим)
            if min_ts and max_ts:
                now2 = time.time()
                last_agg = float(self._last_agg_upsert_ts.get(sid, 0.0))
                if (now2 - last_agg) >= float(self.agg_upsert_every_sec):
                    try:
                        buf = timedelta(minutes=int(self.overlap_minutes))
                        self._upsert_agg_5m(symbol_id=sid, ts_from=min_ts - buf, ts_to=max_ts + buf)
                        self._last_agg_upsert_ts[sid] = now2
                    except Exception:
                        log.exception("[MarketTradesAll][WS] agg upsert failed (tail) symbol=%s", sym)

            log.info(
                "[MarketTradesAll][WS] tail_sync%s symbol=%s lag=%.0fs pages=%d fetched=%d inserted=%d from_id=%s",
                " [WARMUP]" if warm else "",
                sym,
                float(lag),
                pages,
                total_fetched,
                total_inserted,
                self._from_id.get(sid),
            )

    # ---------------- flush ----------------

    def _flush_once(self) -> Tuple[int, int]:
        with self._buf_lock:
            if not self._buf:
                return 0, 0
            take_n = min(len(self._buf), int(self.flush_max_rows))
            rows = self._buf[:take_n]
            self._buf = self._buf[take_n:]

        if not rows:
            return 0, 0

        per_minmax: Dict[int, Tuple[Optional[datetime], Optional[datetime]]] = {}
        per_maxid: Dict[int, int] = {}

        for r in rows:
            sid = int(r["symbol_id"])
            ts = r["ts"]
            tid = int(r["trade_id"])

            mn, mx = per_minmax.get(sid, (None, None))
            if mn is None or ts < mn:
                mn = ts
            if mx is None or ts > mx:
                mx = ts
            per_minmax[sid] = (mn, mx)

            prev = per_maxid.get(sid, 0)
            if tid > prev:
                per_maxid[sid] = tid

        inserted = self._insert_trades(rows)

        for sid, mxid in per_maxid.items():
            self._from_id[sid] = int(mxid) + 1
        for sid, (_, mx) in per_minmax.items():
            if mx is not None:
                self._last_ts[sid] = mx

        total_up = 0
        now_ts = time.time()
        buf = timedelta(minutes=int(self.overlap_minutes))

        for sid, (mn, mx) in per_minmax.items():
            if not mn or not mx:
                continue

            last_agg = float(self._last_agg_upsert_ts.get(sid, 0.0))
            if (now_ts - last_agg) >= float(self.agg_upsert_every_sec):
                try:
                    total_up += int(self._upsert_agg_5m(symbol_id=int(sid), ts_from=mn - buf, ts_to=mx + buf))
                    self._last_agg_upsert_ts[sid] = now_ts
                except Exception:
                    log.exception("[MarketTradesAll][WS] agg upsert failed sid=%s", sid)

            last_cvd = float(self._last_cvd_recalc_ts.get(sid, 0.0))
            if (now_ts - last_cvd) >= float(self.recalc_cvd_every_sec):
                try:
                    self._recalc_cvd_5m(symbol_id=int(sid), hours_back=self.cvd_hours_back)
                    self._last_cvd_recalc_ts[sid] = now_ts
                except Exception:
                    pass

        return inserted, total_up

    # ---------------- main loop ----------------

    def run(self) -> None:
        self._refresh_symbols()
        if not self._ws_list:
            self._start_ws()

        last_flush = time.time()
        backoff = 1.0

        while not self.stop_event.is_set():
            try:
                if self._should_refresh_symbols():
                    self._refresh_symbols()

                # ✅ smart tail sync (с warmup автоматически)
                self._tail_sync_some()

                now = time.time()
                if (now - last_flush) >= float(self.flush_sec):
                    ins_total = 0
                    up_total = 0
                    for _ in range(3):
                        ins, up = self._flush_once()
                        ins_total += ins
                        up_total += up
                        if ins == 0 and up == 0:
                            break
                    if ins_total > 0 or up_total > 0:
                        log.info("[MarketTradesAll][WS] flush inserted=%d agg_upserts=%d conns=%d buf=%d",
                                 ins_total, up_total, len(self._ws_list), len(self._buf))
                    last_flush = now

                self.stop_event.wait(0.1)
                backoff = 1.0

            except Exception:
                log.exception("[MarketTradesAll][WS] loop error")
                self.stop_event.wait(backoff)
                backoff = min(backoff * 2.0, 30.0)

        # финальный flush
        try:
            for _ in range(10):
                ins, up = self._flush_once()
                if ins == 0 and up == 0:
                    break
        except Exception:
            pass

        self._stop_ws()


# ============================================================
# Factory
# ============================================================

def start_market_trades_all_collector(
    *,
    pool: Any,
    rest: Any,
    exchange_id: int,
    cfg: dict,
    stop_event: threading.Event,
    watermarks: Any | None = None,
    **kwargs: Any,
):
    use_ws = bool(cfg.get("use_ws", False))

    if use_ws:
        w = MarketTradesAllWSWorker(
            pool=pool,
            rest=rest,
            exchange_id=int(exchange_id),

            refresh_symbols_sec=int(cfg.get("refresh_symbols_sec", 600)),
            max_symbols=int(cfg.get("max_symbols", 0)),

            flush_sec=float(cfg.get("flush_sec", 0.75)),
            flush_max_rows=int(cfg.get("flush_max_rows", 5000)),

            ws_base_url=str(cfg.get("ws_base_url", "")).strip(),
            ws_max_streams_per_conn=int(cfg.get("ws_max_streams_per_conn", 160)),
            ws_url_max_len=int(cfg.get("ws_url_max_len", 3500)),
            ws_ping_interval=int(cfg.get("ws_ping_interval", 60)),
            ws_ping_timeout=int(cfg.get("ws_ping_timeout", 30)),

            overlap_minutes=int(cfg.get("overlap_minutes", 60)),
            agg_upsert_every_sec=int(cfg.get("agg_upsert_every_sec", 10)),
            recalc_cvd_every_sec=int(cfg.get("recalc_cvd_every_sec", 120)),
            cvd_hours_back=int(cfg.get("cvd_hours_back", 24)),

            tail_sync_enabled=bool(cfg.get("tail_sync_enabled", True)),
            lag_tail_sec=int(cfg.get("lag_tail_sec", 600)),
            tail_sync_every_sec=int(cfg.get("tail_sync_every_sec", 5)),
            tail_sync_symbols_per_cycle=int(cfg.get("tail_sync_symbols_per_cycle", 2)),
            tail_sync_limit=int(cfg.get("tail_sync_limit", 1000)),
            tail_sync_max_pages_per_symbol=int(cfg.get("tail_sync_max_pages_per_symbol", 3)),
            tail_sync_max_requests_per_cycle=int(cfg.get("tail_sync_max_requests_per_cycle", 6)),
            tail_sync_min_symbol_interval_sec=int(cfg.get("tail_sync_min_symbol_interval_sec", 45)),
            tail_sync_safety_lag_sec=int(cfg.get("tail_sync_safety_lag_sec", 3)),

            # ✅ warmup keys
            warmup_enabled=bool(cfg.get("warmup_enabled", True)),
            warmup_sec=int(cfg.get("warmup_sec", 180)),
            warmup_lag_tail_sec=int(cfg.get("warmup_lag_tail_sec", 60)),
            warmup_tail_sync_every_sec=int(cfg.get("warmup_tail_sync_every_sec", 3)),
            warmup_tail_sync_symbols_per_cycle=int(cfg.get("warmup_tail_sync_symbols_per_cycle", 3)),
            warmup_tail_sync_max_requests_per_cycle=int(cfg.get("warmup_tail_sync_max_requests_per_cycle", 8)),
            warmup_tail_sync_max_pages_per_symbol=int(cfg.get("warmup_tail_sync_max_pages_per_symbol", 4)),
            warmup_tail_sync_min_symbol_interval_sec=int(cfg.get("warmup_tail_sync_min_symbol_interval_sec", 20)),

            stop_event=stop_event,
        )
        w.start()
        log.info("[MarketTradesAll] WS worker started")
        return w

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
    log.info("[MarketTradesAll] REST worker started")
    return w
