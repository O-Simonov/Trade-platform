# src/platform/exchanges/binance/collector_candles.py
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src.platform.exchanges.binance.ws import WS_STREAM_BASE
from src.platform.exchanges.binance.normalize import norm_kline_event

logger = logging.getLogger("src.platform.exchanges.binance.collector_candles")


# -------------------------
# helpers
# -------------------------

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _from_ms(ms: Any) -> Optional[datetime]:
    try:
        if ms is None:
            return None
        return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)
    except Exception:
        return None


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or x == "":
            return float(default)
        return float(x)
    except Exception:
        return float(default)


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None or x == "":
            return int(default)
        return int(float(x))
    except Exception:
        return int(default)


def _chunk(items: List[str], n: int) -> List[List[str]]:
    n = max(1, int(n))
    return [items[i : i + n] for i in range(0, len(items), n)]


def _make_streams(symbols: List[str], interval: str) -> List[str]:
    itv = str(interval).strip()
    return [f"{s.lower()}@kline_{itv}" for s in symbols]


def _combined_ws_url(streams: List[str]) -> str:
    """
    WS_STREAM_BASE уже содержит '.../stream?streams='
    """
    joined = "/".join(streams)
    return f"{WS_STREAM_BASE}{joined}"

# -------------------------
# REST seed (klines)
# -------------------------

def _rest_klines_call(
    rest: Any,
    *,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int,
) -> List[list]:
    """
    Вызываем REST klines через разные имена методов (на случай отличий в rest.py).
    Ожидаем Binance /fapi/v1/klines формат: list of lists.
    """
    limit = max(1, min(int(limit), 1500))

    fn = getattr(rest, "klines", None)
    if callable(fn):
        return fn(symbol=symbol, interval=interval, start_time_ms=start_ms, end_time_ms=end_ms, limit=limit) or []

    fn = getattr(rest, "fapi_klines", None)
    if callable(fn):
        return fn(symbol=symbol, interval=interval, start_time_ms=start_ms, end_time_ms=end_ms, limit=limit) or []

    fn = getattr(rest, "get_klines", None)
    if callable(fn):
        return fn(symbol=symbol, interval=interval, start_time_ms=start_ms, end_time_ms=end_ms, limit=limit) or []

    raise RuntimeError("REST klines method not found on rest client (expected klines/get_klines/fapi_klines)")


def _parse_rest_kline_rows(
    *,
    exchange_id: int,
    symbol_id: int,
    interval: str,
    payload: List[list],
    source: str = "rest_kline",
) -> List[dict]:
    """
    Binance kline array:
      [
        0 openTime,
        1 open,
        2 high,
        3 low,
        4 close,
        5 volume,
        6 closeTime,
        7 quoteVolume,
        8 trades,
        9 takerBuyBase,
        10 takerBuyQuote,
        11 ignore
      ]
    """
    out: List[dict] = []
    now = _utc_now()

    for r in payload or []:
        if not isinstance(r, (list, tuple)) or len(r) < 11:
            continue

        open_time = _from_ms(r[0])
        if open_time is None:
            continue

        vol = _safe_float(r[5], 0.0)
        qv = _safe_float(r[7], 0.0)
        tbb = _safe_float(r[9], 0.0)
        tbq = _safe_float(r[10], 0.0)

        out.append(
            {
                "exchange_id": int(exchange_id),
                "symbol_id": int(symbol_id),
                "interval": str(interval),
                "open_time": open_time,
                "open": _safe_float(r[1], 0.0),
                "high": _safe_float(r[2], 0.0),
                "low": _safe_float(r[3], 0.0),
                "close": _safe_float(r[4], 0.0),
                "volume": vol,
                "quote_volume": qv,
                "trades": _safe_int(r[8], 0),
                "taker_buy_base": tbb,
                "taker_buy_quote": tbq,
                "taker_sell_base": vol - tbb,
                "taker_sell_quote": qv - tbq,
                "source": str(source),
                "updated_at": now,
            }
        )
    return out


# --- BACKWARD COMPAT for collector_candles_gap_repair.py ---
def _parse_klines_payload(
    *,
    exchange_id: int,
    symbol_id: int,
    interval: str,
    payload: list,
    source: str = "gap_repair",
) -> list[dict]:
    """
    Совместимость со старым GapRepair:
      - он импортирует _parse_klines_payload из collector_candles.py
      - и передаёт source="gap_repair"
    """
    return _parse_rest_kline_rows(
        exchange_id=int(exchange_id),
        symbol_id=int(symbol_id),
        interval=str(interval),
        payload=payload or [],
        source=str(source),
    )


def _load_last_candle_time_from_db(
    *,
    storage: Any,
    exchange_id: int,
    symbol_id: int,
    interval: str,
) -> Optional[datetime]:
    """
    Watermark из БД: MAX(open_time) по (exchange_id, symbol_id, interval)
    Используем только для seed.
    """
    pool = getattr(storage, "pool", None)
    if pool is None:
        return None
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT MAX(open_time)
                    FROM candles
                    WHERE exchange_id=%s AND symbol_id=%s AND interval=%s
                    """,
                    (int(exchange_id), int(symbol_id), str(interval)),
                )
                row = cur.fetchone()
                return row[0] if row and row[0] else None
    except Exception:
        logger.exception("[CandlesSeed] failed to load last candle time from DB")
        return None


def _seed_symbol_interval_forward(
    *,
    storage: Any,
    rest: Any,
    exchange_id: int,
    symbol: str,
    symbol_id: int,
    interval: str,
    seed_days: int,
    limit: int,
    max_pages: int,
    per_request_sleep_sec: float,
    stop_event: threading.Event,
) -> int:
    """
    SEED "вперёд" (pagination-safe), чтобы не было дыр:
      - определяем start_dt (либо now-seed_days, либо last_open_time-overlap)
      - делаем запрос [start_dt..now], limit
      - upsert
      - двигаем start_dt = max_open_time + 1ms
      - если пришло < limit -> конец
      - ограничиваемся max_pages
    """
    seed_days = max(1, int(seed_days))
    limit = max(50, min(int(limit), 1500))
    max_pages = max(1, int(max_pages))
    per_request_sleep_sec = max(0.0, float(per_request_sleep_sec))

    now = _utc_now()
    seed_from = now - timedelta(days=seed_days)

    last_dt = _load_last_candle_time_from_db(
        storage=storage,
        exchange_id=exchange_id,
        symbol_id=symbol_id,
        interval=interval,
    )
    if last_dt is not None:
        # небольшой overlap для восстановления края
        start_dt = max(seed_from, last_dt - timedelta(minutes=30))
    else:
        start_dt = seed_from

    end_dt = now
    total_upserts = 0
    last_progress: Optional[datetime] = None

    for _ in range(max_pages):
        if stop_event.is_set():
            break

        payload = _rest_klines_call(
            rest,
            symbol=str(symbol),
            interval=str(interval),
            start_ms=_to_ms(start_dt),
            end_ms=_to_ms(end_dt),
            limit=limit,
        )
        if not payload:
            break

        rows = _parse_rest_kline_rows(
            exchange_id=exchange_id,
            symbol_id=symbol_id,
            interval=interval,
            payload=payload,
            source="rest_seed",
        )
        if not rows:
            break

        try:
            n = int(storage.upsert_candles(rows) or 0)
            total_upserts += n
        except Exception:
            logger.exception("[CandlesSeed] upsert_candles failed sym=%s itv=%s", symbol, interval)

        best: Optional[datetime] = None
        for r in rows:
            ot = r.get("open_time")
            if isinstance(ot, datetime):
                if best is None or ot > best:
                    best = ot

        if best is None:
            break

        if last_progress is not None and best <= last_progress:
            logger.warning("[CandlesSeed] stalled sym=%s itv=%s best=%s last=%s", symbol, interval, best, last_progress)
            break
        last_progress = best

        if len(rows) < limit:
            break

        start_dt = best + timedelta(milliseconds=1)
        if start_dt >= end_dt:
            break

        if per_request_sleep_sec > 0 and stop_event.wait(per_request_sleep_sec):
            break

    return total_upserts


# -------------------------
# WS worker (kline)
# -------------------------

class _WSKlineWorker(threading.Thread):
    def __init__(
        self,
        *,
        name: str,
        storage: Any,
        exchange_id: int,
        symbol_ids: Dict[str, int],
        interval: str,
        symbols: List[str],
        stop_event: threading.Event,
        update_open_candle: bool,
        flush_sec: float = 1.0,
        max_buffer: int = 2000,
    ) -> None:
        super().__init__(daemon=True, name=name)

        self.storage = storage
        self.exchange_id = int(exchange_id)
        self.symbol_ids = {str(k).upper(): int(v) for k, v in (symbol_ids or {}).items()}

        self.interval = str(interval)
        self.symbols = [str(s).upper() for s in (symbols or [])]
        self.stop_event = stop_event
        self.update_open_candle = bool(update_open_candle)

        self.flush_sec = max(0.2, float(flush_sec))
        self.max_buffer = max(100, int(max_buffer))

        self._buf: List[dict] = []
        self._last_flush = time.time()

    def _flush(self) -> None:
        if not self._buf:
            return

        batch = self._buf
        self._buf = []
        self._last_flush = time.time()

        for r in batch:
            if isinstance(r, dict) and "is_closed" in r:
                r.pop("is_closed", None)

        try:
            n = int(self.storage.upsert_candles(batch) or 0)
            if n > 0:
                logger.info("[CandlesWS][%s] flush upserts=%d", self.name, n)
        except Exception:
            logger.exception("[CandlesWS][%s] upsert_candles failed", self.name)

    def _on_message(self, msg: dict) -> None:
        data = msg.get("data") if isinstance(msg, dict) else None
        if not isinstance(data, dict):
            return

        row = norm_kline_event(self.exchange_id, self.symbol_ids, data)
        if not row:
            return

        is_closed = bool(row.get("is_closed"))
        if (not is_closed) and (not self.update_open_candle):
            return

        self._buf.append(row)

        if len(self._buf) >= self.max_buffer:
            self._flush()

        if (time.time() - self._last_flush) >= self.flush_sec:
            self._flush()

    def run(self) -> None:
        import websocket  # websocket-client

        streams = _make_streams(self.symbols, self.interval)
        url = _combined_ws_url(streams)

        logger.info(
            "[CandlesWS][%s] start interval=%s symbols=%d update_open=%s",
            self.name,
            self.interval,
            len(self.symbols),
            self.update_open_candle,
        )

        backoff = 1.0

        while not self.stop_event.is_set():
            try:

                def on_message(_ws, message: str) -> None:
                    try:
                        msg = json.loads(message)
                    except Exception:
                        return
                    self._on_message(msg)

                def on_open(_ws) -> None:
                    logger.info("[CandlesWS][%s] connected", self.name)

                def on_error(_ws, err) -> None:
                    logger.warning("[CandlesWS][%s] error: %s", self.name, err)

                def on_close(_ws, status_code, msg) -> None:
                    logger.warning("[CandlesWS][%s] closed code=%s msg=%s", self.name, status_code, msg)

                ws_app = websocket.WebSocketApp(
                    url,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
                ws_app.run_forever(ping_interval=20, ping_timeout=10)

            except Exception:
                logger.exception("[CandlesWS][%s] crash", self.name)

            try:
                self._flush()
            except Exception:
                pass

            if self.stop_event.wait(backoff):
                break
            backoff = min(backoff * 2.0, 60.0)

        self._flush()
        logger.info("[CandlesWS][%s] stopped", self.name)


# -------------------------
# public start
# -------------------------

def start_candles_collector(
    *,
    storage: Any,
    rest: Any,
    exchange_id: int,
    symbol_ids: Dict[str, int],     # {SYMBOL: symbol_id}
    intervals: List[str],

    # --- NEW seed+ws mode ---
    seed_on_start: bool = True,
    seed_days: int = 2,
    seed_limit: int = 1500,
    seed_max_pages_per_symbol: int = 2,
    seed_symbols_per_cycle: int = 5,
    per_request_sleep_sec: float = 0.20,

    use_ws: bool = True,
    ws_max_streams_per_conn: int = 120,
    update_open_candle: bool = True,

    stop_event: threading.Event | None = None,

    # --- OLD params (backward compat) ---
    poll_sec: float = 30.0,
    symbols_per_cycle: int = 40,
    seed_chunk_limit: int = 1500,
    loop_limit: int = 0,
    overlap_points: int = 1,
    lookback_days_if_empty: int = 2,
    refresh_symbols_sec: int = 300,

    # allow extra kwargs from config
    **_kwargs,
) -> List[threading.Thread]:
    """
    Режим под твой кейс (много символов):
      1) SEED (REST): на старте догружаем историю батчами.
      2) LIVE (WS): дальше свечи пополняются по kline websocket.

    ВАЖНО:
      - Старые параметры оставлены для совместимости, но в новом режиме почти не нужны.
      - stop_event обязателен: если None — создадим локальный (но лучше передавать из run_market_data).
    """
    if stop_event is None:
        stop_event = threading.Event()

    intervals = [str(x).strip() for x in (intervals or []) if str(x).strip()]
    if not intervals:
        intervals = ["5m"]

    pairs = sorted(
        ((str(sym).upper().strip(), int(sid)) for sym, sid in (symbol_ids or {}).items() if str(sym).strip()),
        key=lambda x: x[0],
    )
    symbols = [sym for sym, _ in pairs]
    if not symbols:
        logger.warning("[Candles] no symbols -> nothing to start")
        return []

    # "backward" compat fallback:
    # - если в коде/конфиге не передали новые seed_* поля,
    #   можно использовать старые seed_chunk_limit/loop_limit как суррогаты.
    # (но мы предпочитаем новые параметры)
    if not seed_max_pages_per_symbol or int(seed_max_pages_per_symbol) <= 0:
        seed_max_pages_per_symbol = max(1, int(loop_limit or 2))
    if not seed_symbols_per_cycle or int(seed_symbols_per_cycle) <= 0:
        # старый seed_chunk_limit у тебя раньше означал "батч символов"
        seed_symbols_per_cycle = max(1, int(seed_chunk_limit or 5))

    # -------------------------
    # SEED: REST history
    # -------------------------
    if bool(seed_on_start) and int(seed_days) > 0:
        batch_size = max(1, int(seed_symbols_per_cycle))
        max_pages = max(1, int(seed_max_pages_per_symbol))
        limit = max(50, min(int(seed_limit), 1500))
        sleep_s = max(0.0, float(per_request_sleep_sec))

        logger.info(
            "[CandlesSeed] start seed_days=%d intervals=%s symbols=%d rest_limit=%d max_pages_per_symbol=%d batch=%d sleep=%.2fs",
            int(seed_days),
            ",".join(intervals),
            len(symbols),
            int(limit),
            int(max_pages),
            int(batch_size),
            float(sleep_s),
        )

        for itv in intervals:
            if stop_event.is_set():
                break

            for i in range(0, len(pairs), batch_size):
                if stop_event.is_set():
                    break

                batch = pairs[i : i + batch_size]
                up_total = 0

                for sym, sid in batch:
                    if stop_event.is_set():
                        break
                    try:
                        n = _seed_symbol_interval_forward(
                            storage=storage,
                            rest=rest,
                            exchange_id=int(exchange_id),
                            symbol=sym,
                            symbol_id=int(sid),
                            interval=str(itv),
                            seed_days=int(seed_days),
                            limit=int(limit),
                            max_pages=int(max_pages),
                            per_request_sleep_sec=float(sleep_s),
                            stop_event=stop_event,
                        )
                        up_total += int(n or 0)
                    except Exception:
                        logger.exception("[CandlesSeed] failed sym=%s itv=%s", sym, itv)

                logger.info("[CandlesSeed] interval=%s batch=%d upserts=%d", itv, len(batch), up_total)

    # -------------------------
    # LIVE: websocket klines
    # -------------------------
    if not bool(use_ws):
        logger.warning("[Candles] use_ws=false -> LIVE WS disabled; with many symbols REST live polling will hit 429. Enable use_ws.")
        return []

    max_streams = max(20, int(ws_max_streams_per_conn))
    chunks = _chunk(symbols, max_streams)

    threads: List[threading.Thread] = []
    for itv in intervals:
        for idx, syms in enumerate(chunks):
            t = _WSKlineWorker(
                name=f"BinanceCandlesWS-{itv}-{idx + 1}",
                storage=storage,
                exchange_id=int(exchange_id),
                symbol_ids=dict(symbol_ids or {}),
                interval=str(itv),
                symbols=syms,
                stop_event=stop_event,
                update_open_candle=bool(update_open_candle),
                flush_sec=1.0,
                max_buffer=2000,
            )
            t.start()
            threads.append(t)

    logger.info(
        "[CandlesWS] started threads=%d intervals=%s symbols=%d chunks=%d",
        len(threads),
        ",".join(intervals),
        len(symbols),
        len(chunks),
    )
    return threads
