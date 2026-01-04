# src/platform/exchanges/binance/collector_candles.py
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Deque, Dict, List, Tuple, Any, Optional

logger = logging.getLogger(__name__)


# ----------------------------- helpers -----------------------------

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _interval_ms(itv: str) -> int:
    itv = str(itv).strip().lower()
    if itv.endswith("m"):
        return int(itv[:-1]) * 60_000
    if itv.endswith("h"):
        return int(itv[:-1]) * 3_600_000
    if itv.endswith("d"):
        return int(itv[:-1]) * 86_400_000
    raise ValueError(f"Unsupported interval: {itv}")


def _classify_http_error(exc: Exception) -> Optional[int]:
    """
    Возвращает HTTP code (если удаётся достать из RuntimeError("Binance HTTP XXX ..."))
    """
    s = str(exc)
    # примеры:
    # RuntimeError("Binance HTTP 451 GET /fapi/v1/klines: ...")
    # RuntimeError("Binance HTTP 418 GET /fapi/v1/premiumIndex: ...")
    for code in (451, 418, 429, 403, 400):
        if f"HTTP {code}" in s:
            return code
    return None


def _parse_klines_payload(
    *,
    payload: Any,
    exchange_id: int,
    symbol_id: int,
    interval: str,
    source: str,
) -> List[Dict[str, Any]]:
    """
    Приводим ответ Binance klines к rows для upsert_candles().
    Ожидается payload = list[list], где:
      [ open_time, open, high, low, close, volume, close_time, ... ]
    """
    rows: List[Dict[str, Any]] = []
    if not payload:
        return rows

    for k in payload:
        try:
            open_time_ms = int(k[0])
            open_time = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc)
            rows.append(
                dict(
                    exchange_id=int(exchange_id),
                    symbol_id=int(symbol_id),
                    interval=str(interval),
                    open_time=open_time,
                    open=float(k[1]),
                    high=float(k[2]),
                    low=float(k[3]),
                    close=float(k[4]),
                    volume=float(k[5]),
                    source=str(source),
                    updated_at=_utc_now(),
                )
            )
        except Exception:
            # мусор/битые строки просто пропускаем
            continue
    return rows


def _load_watermarks_from_db(*, storage, exchange_id: int, intervals: List[str]) -> Dict[Tuple[int, str], datetime]:
    """
    Должен вернуть {(symbol_id, interval) -> last_open_time_utc}
    """
    # ожидаем, что в storage есть метод:
    # storage.get_candles_watermarks(exchange_id=..., intervals=[...])
    # который возвращает dict ключ -> datetime
    try:
        wm = storage.get_candles_watermarks(exchange_id=int(exchange_id), intervals=list(intervals))
        if isinstance(wm, dict):
            return wm
    except Exception:
        logger.exception("[Candles] failed to load watermarks")
    return {}


def _backfill_symbol_interval(
    *,
    binance_rest,
    storage,
    exchange_id: int,
    symbol: str,
    symbol_id: int,
    interval: str,
    start_dt: datetime,
    end_dt: datetime,
    chunk_limit: int,
    req_sleep_sec: float,
    stop_event: threading.Event,
) -> Tuple[int, Optional[datetime]]:
    """
    Seed-режим: докачка истории [start_dt..end_dt] чанками chunk_limit.
    """
    cur_start = start_dt
    max_open: Optional[datetime] = None
    upserts = 0

    max_loops = 10_000  # safety
    for _ in range(max_loops):
        if stop_event.is_set():
            break

        payload = binance_rest.klines(
            symbol=symbol,
            interval=interval,
            start_time_ms=_to_ms(cur_start),
            end_time_ms=_to_ms(end_dt),
            limit=int(chunk_limit),
        )
        rows = _parse_klines_payload(
            payload=payload,
            exchange_id=exchange_id,
            symbol_id=symbol_id,
            interval=interval,
            source="rest_seed",
        )
        if not rows:
            break

        upserts += int(storage.upsert_candles(rows) or 0)

        last_open = rows[-1]["open_time"]
        if isinstance(last_open, datetime):
            max_open = last_open if (max_open is None or last_open > max_open) else max_open
            cur_start = last_open + timedelta(milliseconds=1)

        if len(rows) < int(chunk_limit):
            break

        if stop_event.wait(req_sleep_sec):
            break

    return upserts, max_open


# ----------------------------- main runner -----------------------------

def run_candles_collector(
    *,
    binance_rest,
    storage,
    exchange_id: int,
    symbol_ids: Dict[str, int],
    intervals: List[str],
    poll_sec: float,
    symbols_per_cycle: int,
    per_request_sleep_sec: float,
    seed_on_start: bool,
    seed_days: int,
    seed_chunk_limit: int,
    overlap_points: int,
    lookback_days_if_empty: int,
    stop_event: threading.Event,
    # NEW:
    loop_limit: int = 3,      # в цикле просим только N свечей
    seed_limit: int = 0,      # альтернативный seed: взять N последних свечей без start/end
) -> None:
    intervals = [str(x).strip() for x in (intervals or []) if str(x).strip()]
    if not intervals:
        logger.info("[Candles] no intervals configured → stop")
        return

    symbols = [(str(s).upper().strip(), int(sid)) for s, sid in (symbol_ids or {}).items()]
    symbols = [(s, sid) for s, sid in symbols if s]
    symbols.sort(key=lambda x: x[0])

    if not symbols:
        logger.info("[Candles] no symbols configured → stop")
        return

    q: Deque[Tuple[str, int]] = deque(symbols)

    overlap_points = max(0, int(overlap_points))
    lookback_days_if_empty = max(1, int(lookback_days_if_empty))
    seed_chunk_limit = max(1, int(seed_chunk_limit))
    loop_limit = max(1, int(loop_limit))
    seed_limit = max(0, int(seed_limit))

    logger.info(
        "[Candles] started symbols=%d intervals=%s poll=%.1fs batch=%d seed=%s seed_days=%d seed_chunk=%d seed_limit=%d loop_limit=%d overlap=%d",
        len(q),
        ",".join(intervals),
        float(poll_sec),
        int(symbols_per_cycle),
        bool(seed_on_start),
        int(seed_days),
        int(seed_chunk_limit),
        int(seed_limit),
        int(loop_limit),
        int(overlap_points),
    )

    # per (sym, itv) cooldown on bans
    cooldown_until: Dict[Tuple[str, str], datetime] = {}

    # load watermarks once
    watermarks = _load_watermarks_from_db(storage=storage, exchange_id=exchange_id, intervals=intervals)
    logger.info("[Candles] loaded watermarks from DB: %d", len(watermarks))

    # ------------------ SEED ONCE ------------------
    if seed_on_start:
        now = _utc_now()
        total_seed = 0
        seeded_pairs = 0

        # Mode A: seed_days (heavy)
        if seed_days > 0:
            end_dt = now
            seed_from = end_dt - timedelta(days=int(seed_days))

            for sym, sid in list(q):
                for itv in intervals:
                    if stop_event.is_set():
                        break
                    if watermarks.get((sid, itv)) is not None:
                        continue

                    try:
                        n, max_open = _backfill_symbol_interval(
                            binance_rest=binance_rest,
                            storage=storage,
                            exchange_id=exchange_id,
                            symbol=sym,
                            symbol_id=sid,
                            interval=itv,
                            start_dt=seed_from,
                            end_dt=end_dt,
                            chunk_limit=int(seed_chunk_limit),
                            req_sleep_sec=float(per_request_sleep_sec),
                            stop_event=stop_event,
                        )
                        total_seed += int(n or 0)
                        seeded_pairs += 1
                        if max_open is not None:
                            watermarks[(sid, itv)] = max_open
                    except Exception as e:
                        code = _classify_http_error(e)
                        if code in (418, 429):
                            logger.warning("[CandlesSeed] %s %s HTTP=%s (rate/ban)", sym, itv, code, exc_info=True)
                            cooldown_until[(sym, itv)] = _utc_now() + timedelta(minutes=30)
                        elif code == 451:
                            logger.warning("[CandlesSeed] %s %s HTTP=451 restricted location", sym, itv, exc_info=True)
                            cooldown_until[(sym, itv)] = _utc_now() + timedelta(hours=6)
                        else:
                            logger.exception("[CandlesSeed] %s %s failed", sym, itv)

                    if stop_event.wait(per_request_sleep_sec):
                        break

        # Mode B: seed_limit (light)
        elif seed_limit > 0:
            for sym, sid in list(q):
                for itv in intervals:
                    if stop_event.is_set():
                        break
                    if watermarks.get((sid, itv)) is not None:
                        continue

                    try:
                        payload = binance_rest.klines(symbol=sym, interval=itv, limit=int(seed_limit))
                        rows = _parse_klines_payload(
                            payload=payload,
                            exchange_id=exchange_id,
                            symbol_id=sid,
                            interval=itv,
                            source="rest_seed_tail",
                        )
                        if rows:
                            total_seed += int(storage.upsert_candles(rows) or 0)
                            seeded_pairs += 1
                            last_open = rows[-1].get("open_time")
                            if isinstance(last_open, datetime):
                                watermarks[(sid, itv)] = last_open
                    except Exception as e:
                        code = _classify_http_error(e)
                        if code == 451:
                            logger.warning("[CandlesSeedTail] %s %s HTTP=451 restricted location", sym, itv, exc_info=True)
                            cooldown_until[(sym, itv)] = _utc_now() + timedelta(hours=6)
                        elif code in (418, 429):
                            logger.warning("[CandlesSeedTail] %s %s HTTP=%s (rate/ban)", sym, itv, code, exc_info=True)
                            cooldown_until[(sym, itv)] = _utc_now() + timedelta(minutes=30)
                        else:
                            logger.warning("[CandlesSeedTail] %s %s failed", sym, itv, exc_info=True)

                    if stop_event.wait(per_request_sleep_sec):
                        break

        logger.info("[CandlesSeed] done seeded_pairs=%d upserts=%d", seeded_pairs, total_seed)

    # ------------------ INCREMENTAL LOOP (IMPORTANT: limit=loop_limit) ------------------
    backoff = 1.0
    while not stop_event.is_set():
        try:
            t0 = time.time()

            n = max(1, int(symbols_per_cycle))
            batch: List[Tuple[str, int]] = []
            for _ in range(n):
                sym, sid = q.popleft()
                batch.append((sym, sid))
                q.append((sym, sid))

            upserts = 0
            now = _utc_now()

            for sym, sid in batch:
                for itv in intervals:
                    if stop_event.is_set():
                        break

                    cd = cooldown_until.get((sym, itv))
                    if cd is not None and cd > now:
                        continue

                    last_dt = watermarks.get((sid, itv))
                    itv_ms = _interval_ms(itv)

                    # если данных нет: берём хвост, но маленький
                    if last_dt is None:
                        try:
                            payload = binance_rest.klines(symbol=sym, interval=itv, limit=int(max(loop_limit, 3)))
                            rows = _parse_klines_payload(
                                payload=payload,
                                exchange_id=exchange_id,
                                symbol_id=sid,
                                interval=itv,
                                source="rest_poll_tail",
                            )
                        except Exception as e:
                            code = _classify_http_error(e)
                            if code == 451:
                                logger.warning("[Candles] %s %s HTTP=451 restricted location", sym, itv, exc_info=True)
                                cooldown_until[(sym, itv)] = _utc_now() + timedelta(hours=6)
                            elif code in (418, 429):
                                logger.warning("[Candles] %s %s HTTP=%s (rate/ban)", sym, itv, code, exc_info=True)
                                cooldown_until[(sym, itv)] = _utc_now() + timedelta(minutes=30)
                            else:
                                logger.debug("[Candles] %s %s poll_tail failed", sym, itv, exc_info=True)
                            rows = []
                    else:
                        overlap_ms = itv_ms * overlap_points
                        start_dt = last_dt - timedelta(milliseconds=overlap_ms)
                        if start_dt < now - timedelta(days=30):
                            start_dt = now - timedelta(days=30)

                        try:
                            payload = binance_rest.klines(
                                symbol=sym,
                                interval=itv,
                                start_time_ms=_to_ms(start_dt),
                                end_time_ms=_to_ms(now),
                                limit=int(loop_limit + max(overlap_points, 1) + 1),
                            )
                            rows = _parse_klines_payload(
                                payload=payload,
                                exchange_id=exchange_id,
                                symbol_id=sid,
                                interval=itv,
                                source="rest_poll",
                            )
                        except Exception as e:
                            code = _classify_http_error(e)
                            if code == 451:
                                logger.warning("[Candles] %s %s HTTP=451 restricted location", sym, itv, exc_info=True)
                                cooldown_until[(sym, itv)] = _utc_now() + timedelta(hours=6)
                            elif code in (418, 429):
                                logger.warning("[Candles] %s %s HTTP=%s (rate/ban)", sym, itv, code, exc_info=True)
                                cooldown_until[(sym, itv)] = _utc_now() + timedelta(minutes=30)
                            else:
                                logger.debug("[Candles] %s %s poll failed", sym, itv, exc_info=True)
                            rows = []

                    if rows:
                        upserts += int(storage.upsert_candles(rows) or 0)
                        last_open = rows[-1].get("open_time")
                        if isinstance(last_open, datetime):
                            prev = watermarks.get((sid, itv))
                            if prev is None or last_open > prev:
                                watermarks[(sid, itv)] = last_open

                    if stop_event.wait(per_request_sleep_sec):
                        break

            logger.info("[Candles] poll batch=%d upserts=%d", len(batch), upserts)
            backoff = 1.0

            dt = time.time() - t0
            sleep = max(0.0, float(poll_sec) - dt)
            stop_event.wait(sleep)

        except Exception as e:
            logger.warning("[Candles] error: %s", e, exc_info=True)
            stop_event.wait(backoff)
            backoff = min(backoff * 2.0, 300.0)


def start_candles_collector(
    *,
    rest,
    storage,
    exchange_id: int,
    symbol_ids: Dict[str, int],
    intervals: List[str],
    poll_sec: float = 60.0,
    symbols_per_cycle: int = 2,
    per_request_sleep_sec: float = 0.5,
    seed_on_start: bool = True,
    seed_days: int = 180,
    seed_chunk_limit: int = 1500,
    overlap_points: int = 1,
    lookback_days_if_empty: int = 2,
    stop_event: threading.Event,
    # NEW:
    loop_limit: int = 3,
    seed_limit: int = 0,
) -> threading.Thread:
    t = threading.Thread(
        target=run_candles_collector,
        kwargs=dict(
            binance_rest=rest,
            storage=storage,
            exchange_id=int(exchange_id),
            symbol_ids=dict(symbol_ids),
            intervals=list(intervals),
            poll_sec=float(poll_sec),
            symbols_per_cycle=int(symbols_per_cycle),
            per_request_sleep_sec=float(per_request_sleep_sec),
            seed_on_start=bool(seed_on_start),
            seed_days=int(seed_days),
            seed_chunk_limit=int(seed_chunk_limit),
            overlap_points=int(overlap_points),
            lookback_days_if_empty=int(lookback_days_if_empty),
            stop_event=stop_event,
            loop_limit=int(loop_limit),
            seed_limit=int(seed_limit),
        ),
        daemon=True,
        name=f"BinanceCandlesCollector-{exchange_id}",
    )
    t.start()
    logger.info("[Candles] collector thread started")
    return t
