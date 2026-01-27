# src/platform/exchanges/binance/collector_candles.py
from __future__ import annotations

import json
import logging
import threading
import time
import hashlib
import websocket  # websocket-client
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

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


def _parse_interval_td(interval: str) -> timedelta:
    s = str(interval).strip().lower()
    if s.endswith("m"):
        return timedelta(minutes=int(s[:-1]))
    if s.endswith("h"):
        return timedelta(hours=int(s[:-1]))
    if s.endswith("d"):
        return timedelta(days=int(s[:-1]))
    return timedelta(hours=1)


def _estimate_pages_for_seed(*, interval: str, seed_days: int, limit: int) -> int:
    """
    Автоматически считаем сколько страниц нужно,
    чтобы покрыть seed_days полностью (без дыр).
    """
    limit = max(50, min(int(limit), 1500))
    dt = _parse_interval_td(interval)
    if dt.total_seconds() <= 0:
        return 2

    candles_needed = int((seed_days * 86400) // dt.total_seconds()) + 5
    pages = int((candles_needed + (limit - 1)) // limit)
    pages = pages + 2  # запас
    return max(2, min(pages, 250))


def _normalize_intervals(intervals: List[str]) -> List[str]:
    out = []
    for x in intervals or []:
        s = str(x).strip()
        if not s:
            continue
        out.append(s)
    seen = set()
    uniq = []
    for itv in out:
        if itv in seen:
            continue
        uniq.append(itv)
        seen.add(itv)
    return uniq


def _make_streams_multi(symbols: List[str], intervals: List[str]) -> List[str]:
    """
    Комбинированные потоки:
      btcusdt@kline_1m
      btcusdt@kline_5m
      ethusdt@kline_1m
      ethusdt@kline_5m
    """
    its = _normalize_intervals(intervals)
    out: List[str] = []
    for s in symbols or []:
        sym_lc = str(s).strip().lower()
        if not sym_lc:
            continue
        for itv in its:
            out.append(f"{sym_lc}@kline_{itv}")
    return out


def _combined_ws_url(streams: List[str]) -> str:
    """
    WS_STREAM_BASE уже содержит '.../stream?streams='
    """
    joined = "/".join(streams)
    return f"{WS_STREAM_BASE}{joined}"


def _estimate_url_len(prefix_len: int, stream_count: int, stream_len_sum: int) -> int:
    """
    URL len = len(prefix) + sum(streams) + separators("/")
    separators = (stream_count - 1) if stream_count>0 else 0
    """
    if stream_count <= 0:
        return prefix_len
    return prefix_len + stream_len_sum + (stream_count - 1)


def _chunk_symbols_by_limits(
    *,
    symbols: List[str],
    intervals: List[str],
    max_streams: int,
    max_url_len: int,
) -> List[List[str]]:
    """
    ✅ Умная нарезка symbols на чанки так, чтобы:
      - total streams per conn <= max_streams
      - URL length <= max_url_len

    streams = len(symbols_chunk) * len(intervals)
    """
    symbols = [str(x).upper().strip() for x in (symbols or []) if str(x).strip()]
    intervals = _normalize_intervals(intervals)

    if not symbols:
        return []
    if not intervals:
        intervals = ["5m"]

    prefix_len = len(str(WS_STREAM_BASE))
    max_streams = max(1, int(max_streams))
    max_url_len = max(256, int(max_url_len))

    if max_streams < len(intervals):
        logger.warning(
            "[CandlesWS] ws_max_streams_per_conn=%d < intervals=%d -> bump to %d",
            max_streams, len(intervals), len(intervals)
        )
        max_streams = len(intervals)

    K = len("@kline_")
    itv_len_sum = sum(len(itv) for itv in intervals)

    def per_symbol_stream_len_sum(sym: str) -> int:
        # stream string: "{sym_lc}@kline_{itv}"
        return (len(sym.lower()) * len(intervals)) + (K * len(intervals)) + itv_len_sum

    chunks: List[List[str]] = []
    cur: List[str] = []
    cur_streams = 0
    cur_len_sum = 0

    for sym in symbols:
        add_streams = len(intervals)
        add_len_sum = per_symbol_stream_len_sum(sym)

        cand_streams = cur_streams + add_streams
        cand_len_sum = cur_len_sum + add_len_sum

        cand_url_len = _estimate_url_len(prefix_len, cand_streams, cand_len_sum)

        too_many_streams = cand_streams > max_streams
        too_long_url = cand_url_len > max_url_len

        if (too_many_streams or too_long_url) and cur:
            chunks.append(cur)
            cur = [sym]
            cur_streams = add_streams
            cur_len_sum = add_len_sum
            continue

        if (too_many_streams or too_long_url) and not cur:
            chunks.append([sym])
            cur = []
            cur_streams = 0
            cur_len_sum = 0
            continue

        cur.append(sym)
        cur_streams = cand_streams
        cur_len_sum = cand_len_sum

    if cur:
        chunks.append(cur)

    return chunks


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def _stop_wait(outer: threading.Event, inner: threading.Event, timeout: float) -> bool:
    """
    Ждём пока сработает любой стоп (outer или inner).
    """
    step = 0.25
    t0 = time.time()
    while (time.time() - t0) < timeout:
        if outer.is_set() or inner.is_set():
            return True
        time.sleep(step)
    return outer.is_set() or inner.is_set()


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

    raise RuntimeError("REST klines method not found (expected klines/get_klines/fapi_klines)")


def _parse_rest_kline_rows(
    *,
    exchange_id: int,
    symbol_id: int,
    interval: str,
    payload: List[list],
    source: str = "rest_kline",
) -> List[dict]:
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
    overlap_minutes: int,
    stop_event: threading.Event,
    source: str,
) -> int:
    seed_days = max(1, int(seed_days))
    limit = max(50, min(int(limit), 1500))
    per_request_sleep_sec = max(0.0, float(per_request_sleep_sec))
    overlap_minutes = max(0, int(overlap_minutes))

    now = _utc_now()
    seed_from = now - timedelta(days=seed_days)

    last_dt = _load_last_candle_time_from_db(
        storage=storage,
        exchange_id=exchange_id,
        symbol_id=symbol_id,
        interval=interval,
    )

    if last_dt is not None:
        start_dt = max(seed_from, last_dt - timedelta(minutes=overlap_minutes))
    else:
        start_dt = seed_from

    end_dt = now
    total_upserts = 0
    last_progress: Optional[datetime] = None

    if int(max_pages) <= 0:
        max_pages = _estimate_pages_for_seed(interval=interval, seed_days=seed_days, limit=limit)

    for _ in range(int(max_pages)):
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
            source=source,
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
# WS worker (COMBINED intervals per chunk)
# -------------------------

class _WSCombinedKlineWorker(threading.Thread):
    """
    ✅ 1 WS connection на chunk symbols
    ✅ внутри сразу все интервалы (1m+5m+...)
    ✅ URL limit + rebuild on reconnect
    ✅ REST catchup async (не блокирует WS event loop)
    """

    def __init__(
        self,
        *,
        name: str,
        storage: Any,
        rest: Any,
        exchange_id: int,
        symbol_ids_ref: Dict[str, int],   # shared ref
        intervals_ref: List[str],         # shared ref

        chunk_index: int,
        ws_max_streams_per_conn: int,
        ws_url_max_len: int,

        outer_stop_event: threading.Event,
        local_stop_event: threading.Event,

        update_open_candle: bool,

        flush_sec: float = 1.0,
        max_buffer: int = 2000,

        rest_catchup_on_connect: bool = True,
        rest_catchup_overlap_minutes: int = 120,
        rest_catchup_limit: int = 1500,
        rest_catchup_max_pages: int = 2,
        per_request_sleep_sec: float = 0.15,

        catchup_seed_days: int = 2,
        catchup_max_symbols: int = 0,
    ) -> None:
        super().__init__(daemon=True, name=name)

        self.storage = storage
        self.rest = rest
        self.exchange_id = int(exchange_id)

        self.symbol_ids_ref = symbol_ids_ref
        self.intervals_ref = intervals_ref

        self.chunk_index = int(chunk_index)
        self.ws_max_streams_per_conn = int(ws_max_streams_per_conn)
        self.ws_url_max_len = int(ws_url_max_len)

        self.outer_stop_event = outer_stop_event
        self.local_stop_event = local_stop_event

        self.update_open_candle = bool(update_open_candle)

        self.flush_sec = max(0.2, float(flush_sec))
        self.max_buffer = max(100, int(max_buffer))

        self.rest_catchup_on_connect = bool(rest_catchup_on_connect)
        self.rest_catchup_overlap_minutes = int(rest_catchup_overlap_minutes)
        self.rest_catchup_limit = int(rest_catchup_limit)
        self.rest_catchup_max_pages = int(rest_catchup_max_pages)
        self.per_request_sleep_sec = float(per_request_sleep_sec)

        self.catchup_seed_days = int(catchup_seed_days)
        self.catchup_max_symbols = int(catchup_max_symbols)

        self._buf: List[dict] = []
        self._last_flush = time.time()

        self._catchup_lock = threading.Lock()
        self._catchup_running = False

        self._active_symbols: List[str] = []
        self._active_intervals: List[str] = []
        self._active_url: str = ""

    def _any_stop(self) -> bool:
        return self.outer_stop_event.is_set() or self.local_stop_event.is_set()

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

        try:
            symbol_ids_snapshot = {str(k).upper(): int(v) for k, v in dict(self.symbol_ids_ref).items()}
        except Exception:
            symbol_ids_snapshot = {}

        row = norm_kline_event(self.exchange_id, symbol_ids_snapshot, data)
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

    def _rebuild_url_snapshot(self) -> None:
        try:
            sym_ids = {str(k).upper(): int(v) for k, v in dict(self.symbol_ids_ref).items()}
        except Exception:
            sym_ids = {}

        symbols_all = sorted(sym_ids.keys())
        intervals = _normalize_intervals(list(self.intervals_ref or []))
        if not intervals:
            intervals = ["5m"]

        if not symbols_all:
            self._active_symbols = []
            self._active_intervals = intervals
            self._active_url = ""
            return

        chunks = _chunk_symbols_by_limits(
            symbols=symbols_all,
            intervals=intervals,
            max_streams=self.ws_max_streams_per_conn,
            max_url_len=self.ws_url_max_len,
        )

        if not chunks or self.chunk_index >= len(chunks):
            self._active_symbols = []
            self._active_intervals = intervals
            self._active_url = ""
            return

        syms = chunks[self.chunk_index]
        streams = _make_streams_multi(syms, intervals)
        url = _combined_ws_url(streams)

        self._active_symbols = syms
        self._active_intervals = intervals
        self._active_url = url

    def _catchup_rest_tail_multi(self) -> None:
        if not self.rest_catchup_on_connect:
            return

        with self._catchup_lock:
            if self._catchup_running:
                return
            self._catchup_running = True

        try:
            syms = list(self._active_symbols or [])
            intervals = list(self._active_intervals or [])
            if not syms or not intervals:
                return

            if self.catchup_max_symbols > 0 and len(syms) > self.catchup_max_symbols:
                syms = syms[: self.catchup_max_symbols]

            try:
                sym_ids = {str(k).upper(): int(v) for k, v in dict(self.symbol_ids_ref).items()}
            except Exception:
                sym_ids = {}

            for sym in syms:
                if self._any_stop():
                    return
                sid = sym_ids.get(sym)
                if not sid:
                    continue

                for itv in intervals:
                    if self._any_stop():
                        return
                    try:
                        _seed_symbol_interval_forward(
                            storage=self.storage,
                            rest=self.rest,
                            exchange_id=self.exchange_id,
                            symbol=sym,
                            symbol_id=int(sid),
                            interval=str(itv),
                            seed_days=max(1, int(self.catchup_seed_days)),
                            limit=int(self.rest_catchup_limit),
                            max_pages=int(self.rest_catchup_max_pages),
                            per_request_sleep_sec=float(self.per_request_sleep_sec),
                            overlap_minutes=int(self.rest_catchup_overlap_minutes),
                            stop_event=self.outer_stop_event,  # outer stop is global kill
                            source="rest_tail_catchup",
                        )
                    except Exception:
                        logger.exception(
                            "[CandlesWS][%s] REST tail catchup failed sym=%s itv=%s",
                            self.name, sym, itv
                        )
        finally:
            with self._catchup_lock:
                self._catchup_running = False

    def run(self) -> None:
        logger.info(
            "[CandlesWS][%s] start chunk_index=%d max_streams=%d url_max_len=%d",
            self.name,
            self.chunk_index,
            self.ws_max_streams_per_conn,
            self.ws_url_max_len,
        )

        backoff = 1.0

        while not self._any_stop():
            try:
                self._rebuild_url_snapshot()

                if not self._active_url:
                    logger.warning("[CandlesWS][%s] no active url (symbols empty / chunks shrink). sleep...", self.name)
                    if _stop_wait(self.outer_stop_event, self.local_stop_event, min(backoff, 10.0)):
                        break
                    backoff = min(backoff * 1.5, 30.0)
                    continue

                logger.info(
                    "[CandlesWS][%s] connect symbols=%d intervals=%s url_len=%d",
                    self.name,
                    len(self._active_symbols),
                    ",".join(self._active_intervals),
                    len(self._active_url),
                )

                def on_message(_ws, message: str) -> None:
                    try:
                        msg = json.loads(message)
                    except Exception:
                        return
                    self._on_message(msg)

                def on_open(_ws) -> None:
                    logger.info("[CandlesWS][%s] connected", self.name)

                    # ✅ catchup в отдельном потоке (не блокирует WS)
                    if self.rest_catchup_on_connect:
                        th = threading.Thread(
                            target=self._catchup_rest_tail_multi,
                            daemon=True,
                            name=f"{self.name}-catchup",
                        )
                        th.start()

                def on_error(_ws, err) -> None:
                    logger.warning("[CandlesWS][%s] error: %s", self.name, err)

                def on_close(_ws, status_code, msg) -> None:
                    logger.warning("[CandlesWS][%s] closed code=%s msg=%s", self.name, status_code, msg)

                ws_app = websocket.WebSocketApp(
                    self._active_url,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )

                # ✅ ВАЖНО: если supervisor решил рестартнуть — нужно уметь закрыть WS
                def stop_watcher() -> None:
                    while not self._any_stop():
                        time.sleep(0.25)
                    try:
                        ws_app.close()
                    except Exception:
                        pass

                watcher = threading.Thread(target=stop_watcher, daemon=True, name=f"{self.name}-stopwatch")
                watcher.start()

                ws_app.run_forever(
                    ping_interval=20,
                    ping_timeout=10,
                )

            except Exception:
                logger.exception("[CandlesWS][%s] crash", self.name)

            try:
                self._flush()
            except Exception:
                pass

            if _stop_wait(self.outer_stop_event, self.local_stop_event, backoff):
                break
            backoff = min(backoff * 2.0, 60.0)

        self._flush()
        logger.info("[CandlesWS][%s] stopped", self.name)


# -------------------------
# WS Supervisor (auto restart on Universe changes)
# -------------------------

class _WSCombinedSupervisor(threading.Thread):
    """
    ✅ Умный супервизор WS:
      - следит за изменениями symbol_ids (universe)
      - пересчитывает чанки
      - если universe меняется -> мягко рестартит все WS воркеры
    """

    def __init__(
        self,
        *,
        storage: Any,
        rest: Any,
        exchange_id: int,
        symbol_ids_ref: Dict[str, int],
        intervals_ref: List[str],

        outer_stop_event: threading.Event,

        update_open_candle: bool,

        ws_max_streams_per_conn: int,
        ws_url_max_len: int,

        rest_catchup_on_connect: bool,
        rest_catchup_overlap_minutes: int,
        rest_catchup_limit: int,
        rest_catchup_max_pages: int,
        per_request_sleep_sec: float,

        catchup_seed_days: int,
        catchup_max_symbols: int,

        check_sec: int = 30,
        min_restart_sec: int = 120,
        debounce_checks: int = 2,
        join_timeout_sec: float = 8.0,
    ) -> None:
        super().__init__(daemon=True, name="BinanceCandlesWS-Supervisor")

        self.storage = storage
        self.rest = rest
        self.exchange_id = int(exchange_id)

        self.symbol_ids_ref = symbol_ids_ref
        self.intervals_ref = intervals_ref

        self.outer_stop_event = outer_stop_event

        self.update_open_candle = bool(update_open_candle)

        self.ws_max_streams_per_conn = int(ws_max_streams_per_conn)
        self.ws_url_max_len = int(ws_url_max_len)

        self.rest_catchup_on_connect = bool(rest_catchup_on_connect)
        self.rest_catchup_overlap_minutes = int(rest_catchup_overlap_minutes)
        self.rest_catchup_limit = int(rest_catchup_limit)
        self.rest_catchup_max_pages = int(rest_catchup_max_pages)
        self.per_request_sleep_sec = float(per_request_sleep_sec)

        self.catchup_seed_days = int(catchup_seed_days)
        self.catchup_max_symbols = int(catchup_max_symbols)

        self.check_sec = max(5, int(check_sec))
        self.min_restart_sec = max(10, int(min_restart_sec))
        self.debounce_checks = max(1, int(debounce_checks))
        self.join_timeout_sec = max(1.0, float(join_timeout_sec))

        self._workers: List[_WSCombinedKlineWorker] = []
        self._gen_stop_event = threading.Event()

        self._last_sig: str = ""
        self._pending_sig: str = ""
        self._pending_hits: int = 0
        self._last_restart_ts: float = 0.0

    def _snapshot_signature(self) -> Tuple[str, int, int]:
        """
        Возвращает:
          signature_hash, symbol_count, chunk_count
        """
        try:
            sym_ids = {str(k).upper(): int(v) for k, v in dict(self.symbol_ids_ref).items()}
        except Exception:
            sym_ids = {}

        symbols_all = sorted(sym_ids.keys())
        intervals = _normalize_intervals(list(self.intervals_ref or []))
        if not intervals:
            intervals = ["5m"]

        chunks = _chunk_symbols_by_limits(
            symbols=symbols_all,
            intervals=intervals,
            max_streams=self.ws_max_streams_per_conn,
            max_url_len=self.ws_url_max_len,
        )

        # signature includes intervals + symbols list
        # (достаточно, чтобы ловить изменения universe)
        s = "I:" + ",".join(intervals) + "|S:" + ",".join(symbols_all) + "|C:" + ",".join(str(len(x)) for x in chunks)
        return _sha1(s), len(symbols_all), len(chunks)

    def _stop_generation(self) -> None:
        self._gen_stop_event.set()
        for w in self._workers:
            try:
                w.join(timeout=self.join_timeout_sec)
            except Exception:
                pass
        self._workers = []

    def _start_generation(self, chunk_count: int) -> None:
        self._gen_stop_event = threading.Event()
        self._workers = []

        if chunk_count <= 0:
            return

        for idx in range(int(chunk_count)):
            w = _WSCombinedKlineWorker(
                name=f"BinanceCandlesWS-chunk{idx + 1}",
                storage=self.storage,
                rest=self.rest,
                exchange_id=self.exchange_id,
                symbol_ids_ref=self.symbol_ids_ref,
                intervals_ref=self.intervals_ref,
                chunk_index=idx,
                ws_max_streams_per_conn=self.ws_max_streams_per_conn,
                ws_url_max_len=self.ws_url_max_len,
                outer_stop_event=self.outer_stop_event,
                local_stop_event=self._gen_stop_event,
                update_open_candle=self.update_open_candle,
                flush_sec=1.0,
                max_buffer=2000,
                rest_catchup_on_connect=self.rest_catchup_on_connect,
                rest_catchup_overlap_minutes=self.rest_catchup_overlap_minutes,
                rest_catchup_limit=self.rest_catchup_limit,
                rest_catchup_max_pages=self.rest_catchup_max_pages,
                per_request_sleep_sec=self.per_request_sleep_sec,
                catchup_seed_days=self.catchup_seed_days,
                catchup_max_symbols=self.catchup_max_symbols,
            )
            w.start()
            self._workers.append(w)

    def _restart_now(self, chunk_count: int, reason: str) -> None:
        now = time.time()
        if (now - self._last_restart_ts) < self.min_restart_sec:
            logger.warning("[CandlesWS][Supervisor] restart skipped (cooldown) reason=%s", reason)
            return

        logger.warning(
            "[CandlesWS][Supervisor] RESTART workers -> chunks=%d reason=%s",
            int(chunk_count),
            reason,
        )
        self._stop_generation()
        self._start_generation(chunk_count)
        self._last_restart_ts = time.time()

    def run(self) -> None:
        logger.info(
            "[CandlesWS][Supervisor] start check_sec=%d min_restart_sec=%d debounce=%d",
            self.check_sec,
            self.min_restart_sec,
            self.debounce_checks,
        )

        # initial start
        sig, sym_count, chunk_count = self._snapshot_signature()
        self._last_sig = sig
        self._start_generation(chunk_count)
        logger.info(
            "[CandlesWS][Supervisor] initial universe symbols=%d chunks=%d",
            sym_count,
            chunk_count,
        )

        while not self.outer_stop_event.is_set():
            if self.outer_stop_event.wait(self.check_sec):
                break

            sig2, sym_count2, chunk_count2 = self._snapshot_signature()

            if sig2 == self._last_sig:
                self._pending_sig = ""
                self._pending_hits = 0
                continue

            # debounce: change must be stable несколько проверок подряд
            if self._pending_sig != sig2:
                self._pending_sig = sig2
                self._pending_hits = 1
            else:
                self._pending_hits += 1

            logger.warning(
                "[CandlesWS][Supervisor] universe changed detected symbols=%d chunks=%d pending=%d/%d",
                sym_count2,
                chunk_count2,
                self._pending_hits,
                self.debounce_checks,
            )

            if self._pending_hits >= self.debounce_checks:
                self._last_sig = sig2
                self._pending_sig = ""
                self._pending_hits = 0
                self._restart_now(chunk_count2, reason="universe_changed")

        # stop all
        self._stop_generation()
        logger.info("[CandlesWS][Supervisor] stopped")


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

    seed_on_start: bool = True,
    seed_days: int = 30,
    seed_limit: int = 1500,
    seed_max_pages_per_symbol: int = 0,  # 0 => auto
    seed_symbols_per_cycle: int = 4,
    per_request_sleep_sec: float = 0.20,
    seed_overlap_minutes: int = 60,

    use_ws: bool = True,

    ws_max_streams_per_conn: int = 120,
    ws_url_max_len: int = 1800,

    update_open_candle: bool = True,

    rest_catchup_on_connect: bool = True,
    rest_catchup_overlap_minutes: int = 120,
    rest_catchup_limit: int = 1500,
    rest_catchup_max_pages: int = 2,

    catchup_seed_days: int = 2,
    catchup_max_symbols: int = 0,

    stop_event: threading.Event | None = None,

    # allow extra kwargs from config
    **_kwargs,
) -> List[threading.Thread]:
    """
    ✅ Seed REST + Combined WS + URL limit + Supervisor restart on universe changes.
    """
    if stop_event is None:
        stop_event = threading.Event()

    intervals = _normalize_intervals(intervals or [])
    if not intervals:
        intervals = ["5m"]

    # snapshot list for seed
    pairs = sorted(
        ((str(sym).upper().strip(), int(sid)) for sym, sid in (symbol_ids or {}).items() if str(sym).strip()),
        key=lambda x: x[0],
    )
    symbols = [sym for sym, _ in pairs]
    if not symbols:
        logger.warning("[Candles] no symbols -> nothing to start")
        return []

    # -------------------------
    # SEED: REST history
    # -------------------------
    if bool(seed_on_start) and int(seed_days) > 0:
        batch_size = max(1, int(seed_symbols_per_cycle))
        limit = max(50, min(int(seed_limit), 1500))
        sleep_s = max(0.0, float(per_request_sleep_sec))

        logger.info(
            "[CandlesSeed] start seed_days=%d intervals=%s symbols=%d rest_limit=%d batch=%d sleep=%.2fs pages=auto(%s)",
            int(seed_days),
            ",".join(intervals),
            len(symbols),
            int(limit),
            int(batch_size),
            float(sleep_s),
            "ON" if int(seed_max_pages_per_symbol) <= 0 else str(seed_max_pages_per_symbol),
        )

        for itv in intervals:
            if stop_event.is_set():
                break

            max_pages = int(seed_max_pages_per_symbol)
            if max_pages <= 0:
                max_pages = _estimate_pages_for_seed(interval=itv, seed_days=int(seed_days), limit=int(limit))

            for i in range(0, len(pairs), batch_size):
                if stop_event.is_set():
                    break

                batch = pairs[i: i + batch_size]
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
                            overlap_minutes=int(seed_overlap_minutes),
                            stop_event=stop_event,
                            source="rest_seed",
                        )
                        up_total += int(n or 0)
                    except Exception:
                        logger.exception("[CandlesSeed] failed sym=%s itv=%s", sym, itv)

                logger.info("[CandlesSeed] interval=%s batch=%d upserts=%d", itv, len(batch), up_total)

    # -------------------------
    # LIVE: websocket klines (COMBINED + SUPERVISOR)
    # -------------------------
    if not bool(use_ws):
        logger.warning("[Candles] use_ws=false -> LIVE WS disabled")
        return []

    # supervisor settings from config (optional)
    sup_cfg = _kwargs.get("ws_supervisor")
    if isinstance(sup_cfg, dict):
        sup_enabled = bool(sup_cfg.get("enabled", True))
        sup_check_sec = int(sup_cfg.get("check_sec", 30))
        sup_min_restart_sec = int(sup_cfg.get("min_restart_sec", 120))
        sup_debounce = int(sup_cfg.get("debounce_checks", 2))
    else:
        sup_enabled = bool(_kwargs.get("ws_supervisor_enabled", True))
        sup_check_sec = int(_kwargs.get("ws_supervisor_check_sec", 30))
        sup_min_restart_sec = int(_kwargs.get("ws_supervisor_min_restart_sec", 120))
        sup_debounce = int(_kwargs.get("ws_supervisor_debounce_checks", 2))

    # если supervisor выключен -> просто стартанём 1 поколение воркеров (без авторестарта)
    if not sup_enabled:
        # one-shot workers (no restart)
        chunks0 = _chunk_symbols_by_limits(
            symbols=symbols,
            intervals=intervals,
            max_streams=max(10, int(ws_max_streams_per_conn)),
            max_url_len=max(256, int(ws_url_max_len)),
        )
        if not chunks0:
            logger.warning("[CandlesWS] chunks empty -> WS not started")
            return []

        local = threading.Event()
        threads: List[threading.Thread] = []
        for idx in range(len(chunks0)):
            w = _WSCombinedKlineWorker(
                name=f"BinanceCandlesWS-chunk{idx + 1}",
                storage=storage,
                rest=rest,
                exchange_id=int(exchange_id),
                symbol_ids_ref=symbol_ids,
                intervals_ref=intervals,
                chunk_index=idx,
                ws_max_streams_per_conn=int(ws_max_streams_per_conn),
                ws_url_max_len=int(ws_url_max_len),
                outer_stop_event=stop_event,
                local_stop_event=local,
                update_open_candle=bool(update_open_candle),
                rest_catchup_on_connect=bool(rest_catchup_on_connect),
                rest_catchup_overlap_minutes=int(rest_catchup_overlap_minutes),
                rest_catchup_limit=int(rest_catchup_limit),
                rest_catchup_max_pages=int(rest_catchup_max_pages),
                per_request_sleep_sec=float(per_request_sleep_sec),
                catchup_seed_days=int(catchup_seed_days),
                catchup_max_symbols=int(catchup_max_symbols),
            )
            w.start()
            threads.append(w)

        logger.info(
            "[CandlesWS] started (NO supervisor) threads=%d intervals=%s symbols=%d",
            len(threads), ",".join(intervals), len(symbols)
        )
        return threads

    # supervisor mode
    sup = _WSCombinedSupervisor(
        storage=storage,
        rest=rest,
        exchange_id=int(exchange_id),
        symbol_ids_ref=symbol_ids,
        intervals_ref=intervals,
        outer_stop_event=stop_event,
        update_open_candle=bool(update_open_candle),
        ws_max_streams_per_conn=int(ws_max_streams_per_conn),
        ws_url_max_len=int(ws_url_max_len),
        rest_catchup_on_connect=bool(rest_catchup_on_connect),
        rest_catchup_overlap_minutes=int(rest_catchup_overlap_minutes),
        rest_catchup_limit=int(rest_catchup_limit),
        rest_catchup_max_pages=int(rest_catchup_max_pages),
        per_request_sleep_sec=float(per_request_sleep_sec),
        catchup_seed_days=int(catchup_seed_days),
        catchup_max_symbols=int(catchup_max_symbols),
        check_sec=int(sup_check_sec),
        min_restart_sec=int(sup_min_restart_sec),
        debounce_checks=int(sup_debounce),
    )
    sup.start()

    logger.info(
        "[CandlesWS] started SUPERVISOR intervals=%s ws_max_streams=%d url_max_len=%d",
        ",".join(intervals),
        int(ws_max_streams_per_conn),
        int(ws_url_max_len),
    )
    return [sup]
