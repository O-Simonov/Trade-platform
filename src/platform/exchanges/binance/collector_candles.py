# src/platform/exchanges/binance/collector_candles.py
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _interval_ms(itv: str) -> int:
    """
    Переводим строковый интервал Binance (1m/5m/1h/4h/1d и т.п.) в миллисекунды.
    """
    itv = str(itv).strip().lower()
    if itv.endswith("m"):
        return int(itv[:-1]) * 60_000
    if itv.endswith("h"):
        return int(itv[:-1]) * 3_600_000
    if itv.endswith("d"):
        return int(itv[:-1]) * 86_400_000
    raise ValueError(f"Unsupported interval: {itv}")


def _floor_time_to_interval(dt: datetime, itv_ms: int) -> datetime:
    """
    Округление времени вниз до начала интервала.
    Пример: для 1h -> начало текущего часа, для 4h -> начало текущего 4h-блока.
    """
    ms = _to_ms(dt)
    floored = (ms // itv_ms) * itv_ms
    return datetime.fromtimestamp(floored / 1000, tz=timezone.utc)


def _classify_http_error(exc: Exception) -> Optional[int]:
    """
    Пытаемся извлечь HTTP-код из текста исключения.
    В твоём rest.py ошибки выглядят как:
      "Binance REST HTTP 429: GET /fapi/v1/klines resp=..."
    Также иногда встречается код -1003 (Too many requests) внутри текста.
    """
    s = str(exc)
    for code in (451, 418, 429, 403, 400):
        if f"HTTP {code}" in s:
            return code
    if "-1003" in s:
        return 429
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
    Нормализуем сырой payload Binance klines в список строк для upsert в БД.
    """
    rows: List[Dict[str, Any]] = []
    if not payload:
        return rows

    now = _utc_now()
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
                    updated_at=now,
                )
            )
        except Exception:
            continue
    return rows


def _load_watermarks_from_db(*, storage, exchange_id: int, intervals: List[str]) -> Dict[Tuple[int, str], datetime]:
    """
    Загружаем "watermarks" из БД: (symbol_id, interval) -> max(open_time).
    Это позволяет:
      - не сеедить, если данные уже есть
      - пропускать опрос, если данные "достаточно свежие"
    """
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
    Seed/backfill для ОДНОЙ пары (symbol, interval).

    Делает paging через start/end. Это тяжёлая операция -> используем только при seed.
    """
    cur_start = start_dt
    max_open: Optional[datetime] = None
    upserts = 0

    chunk_limit = max(1, min(int(chunk_limit), 1500))

    for _ in range(50_000):  # safety
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

        last_open = rows[-1].get("open_time")
        if isinstance(last_open, datetime):
            max_open = last_open if (max_open is None or last_open > max_open) else max_open
            # +1ms чтобы не зациклиться на той же свече
            cur_start = last_open + timedelta(milliseconds=1)

        # если вернулось меньше chunk_limit — значит достигли конца
        if len(rows) < int(chunk_limit):
            break

        if stop_event.wait(req_sleep_sec):
            break

    return upserts, max_open


def _calc_interval_min_poll_sec(interval: str, base_poll_sec: float) -> float:
    """
    Интервалы разной "длины" нет смысла поллить одинаково часто.
    Логика по умолчанию:
      - минимум base_poll_sec (как задано конфигом)
      - но также "не чаще чем каждые (interval / divisor)"

    divisor подобран консервативно:
      1h -> ~10 минут
      4h -> ~40 минут
      1d -> ~4 часа
    """
    itv_ms = _interval_ms(interval)
    itv_sec = itv_ms / 1000.0

    # чем больше divisor — тем чаще обновляем
    # можно подобрать под себя позже, но это безопасный старт
    if interval.endswith("m"):
        divisor = 2.0   # минутные можно обновлять чаще
    elif interval.endswith("h"):
        divisor = 6.0
    elif interval.endswith("d"):
        divisor = 6.0
    else:
        divisor = 6.0

    return max(float(base_poll_sec), float(itv_sec / divisor))


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
    stop_event: threading.Event,
    loop_limit: int = 5,
    seed_limit: int = 0,
    # ✅ новое: обновлять ли "текущую незакрытую" свечу слишком часто
    # False = если watermark уже >= open_time текущей свечи, пропускаем запрос
    update_open_candle: bool = False,
) -> None:
    """
    Основной коллектор свечей.

    Режимы:
      - SEED (опционально): один раз заполнить историю
      - MAIN LOOP: "tail mode" — берём небольшой хвост, чтобы поддерживать near-realtime данные

    Ключевое улучшение:
      - interval-aware polling: 1d/4h опрашиваем заметно реже
      - skip-if-fresh: если текущая свеча уже есть в БД — запрос можно пропустить
    """
    intervals = [str(x).strip() for x in (intervals or []) if str(x).strip()]
    if not intervals:
        logger.info("[Candles] intervals не заданы -> стоп")
        return

    symbols = [(str(s).upper().strip(), int(sid)) for s, sid in (symbol_ids or {}).items()]
    symbols = [(s, sid) for s, sid in symbols if s]
    symbols.sort(key=lambda x: x[0])
    if not symbols:
        logger.info("[Candles] symbols не заданы -> стоп")
        return

    q: Deque[Tuple[str, int]] = deque(symbols)

    # clamp входов
    poll_sec = max(5.0, float(poll_sec))
    symbols_per_cycle = max(1, int(symbols_per_cycle))
    per_request_sleep_sec = max(0.05, float(per_request_sleep_sec))
    overlap_points = max(0, int(overlap_points))
    loop_limit = max(1, min(int(loop_limit), 10))
    seed_limit = max(0, min(int(seed_limit), 1500))
    seed_chunk_limit = max(1, min(int(seed_chunk_limit), 1500))
    seed_days = max(0, int(seed_days))

    # tail size: небольшой "хвост" + overlap
    tail_limit = max(5, loop_limit + overlap_points + 1)

    # заранее посчитаем минимальный poll для каждого интервала
    min_poll_by_interval: Dict[str, float] = {itv: _calc_interval_min_poll_sec(itv, poll_sec) for itv in intervals}

    logger.info(
        "[Candles] started symbols=%d intervals=%s base_poll=%.1fs batch=%d req_sleep=%.2fs "
        "seed=%s seed_days=%d seed_chunk=%d seed_limit=%d tail_limit=%d overlap=%d update_open=%s",
        len(q),
        ",".join(intervals),
        poll_sec,
        symbols_per_cycle,
        per_request_sleep_sec,
        seed_on_start,
        seed_days,
        seed_chunk_limit,
        seed_limit,
        tail_limit,
        overlap_points,
        update_open_candle,
    )

    # cooldown на пару (symbol, interval)
    cooldown_until: Dict[Tuple[str, str], datetime] = {}

    # next_due: (symbol_id, interval) -> когда в следующий раз можно опрашивать этот интервал
    next_due: Dict[Tuple[int, str], datetime] = {}

    watermarks = _load_watermarks_from_db(storage=storage, exchange_id=exchange_id, intervals=intervals)
    logger.info("[Candles] loaded watermarks from DB: %d", len(watermarks))

    # ------------------ SEED (однократно) ------------------
    if seed_on_start:
        now = _utc_now()
        total_seed = 0
        seeded_pairs = 0

        if seed_days > 0:
            end_dt = now
            seed_from = end_dt - timedelta(days=seed_days)

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
                            chunk_limit=seed_chunk_limit,
                            req_sleep_sec=per_request_sleep_sec,
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
                            cooldown_until[(sym, itv)] = _utc_now() + timedelta(minutes=5)
                        elif code == 451:
                            logger.warning("[CandlesSeed] %s %s HTTP=451 (restricted)", sym, itv, exc_info=True)
                            cooldown_until[(sym, itv)] = _utc_now() + timedelta(hours=6)
                        else:
                            logger.exception("[CandlesSeed] %s %s failed", sym, itv)

                    if stop_event.wait(per_request_sleep_sec):
                        break

        elif seed_limit > 0:
            # "лёгкий seed": только хвост
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
                        if code in (418, 429):
                            logger.warning("[CandlesSeedTail] %s %s HTTP=%s (rate/ban)", sym, itv, code, exc_info=True)
                            cooldown_until[(sym, itv)] = _utc_now() + timedelta(minutes=5)
                        elif code == 451:
                            logger.warning("[CandlesSeedTail] %s %s HTTP=451 (restricted)", sym, itv, exc_info=True)
                            cooldown_until[(sym, itv)] = _utc_now() + timedelta(hours=6)
                        else:
                            logger.warning("[CandlesSeedTail] %s %s failed", sym, itv, exc_info=True)

                    if stop_event.wait(per_request_sleep_sec):
                        break

        logger.info("[CandlesSeed] done seeded_pairs=%d upserts=%d", seeded_pairs, total_seed)

    # ------------------ MAIN LOOP (tail mode) ------------------
    backoff = 1.0
    while not stop_event.is_set():
        try:
            t0 = time.time()

            # выбираем batch символов round-robin
            batch: List[Tuple[str, int]] = []
            for _ in range(symbols_per_cycle):
                sym, sid = q.popleft()
                batch.append((sym, sid))
                q.append((sym, sid))

            upserts = 0
            now = _utc_now()

            for sym, sid in batch:
                for itv in intervals:
                    if stop_event.is_set():
                        break

                    # 1) cooldown по ошибкам (429/418/451)
                    cd = cooldown_until.get((sym, itv))
                    if cd is not None and cd > now:
                        continue

                    # 2) interval-aware throttling: не опрашиваем интервал чаще, чем нужно
                    due = next_due.get((sid, itv))
                    if due is not None and due > now:
                        continue

                    # 3) skip-if-fresh: если текущая свеча уже есть в БД — можно пропустить
                    # Если update_open_candle=True, то мы НЕ пропускаем и обновляем текущую свечу чаще.
                    itv_ms = _interval_ms(itv)
                    cur_open = _floor_time_to_interval(now, itv_ms)
                    last_open = watermarks.get((sid, itv))

                    if (not update_open_candle) and isinstance(last_open, datetime) and last_open >= cur_open:
                        # следующая свеча начнётся через itv_ms, но дадим небольшой лаг (15с)
                        next_due[(sid, itv)] = cur_open + timedelta(milliseconds=itv_ms) + timedelta(seconds=15)
                        continue

                    # 4) REST запрос "хвоста"
                    try:
                        payload = binance_rest.klines(symbol=sym, interval=itv, limit=int(tail_limit))
                        rows = _parse_klines_payload(
                            payload=payload,
                            exchange_id=exchange_id,
                            symbol_id=sid,
                            interval=itv,
                            source="rest_tail",
                        )

                    except Exception as e:
                        code = _classify_http_error(e)
                        if code in (418, 429):
                            logger.warning("[Candles] %s %s HTTP=%s (rate/ban)", sym, itv, code, exc_info=True)
                            cooldown_until[(sym, itv)] = _utc_now() + timedelta(minutes=5)  # было 2m, делаем безопаснее
                        elif code == 451:
                            logger.warning("[Candles] %s %s HTTP=451 (restricted)", sym, itv, exc_info=True)
                            cooldown_until[(sym, itv)] = _utc_now() + timedelta(hours=6)
                        else:
                            logger.debug("[Candles] %s %s poll failed", sym, itv, exc_info=True)
                        rows = []

                    # 5) upsert + watermark
                    if rows:
                        upserts += int(storage.upsert_candles(rows) or 0)
                        last_open2 = rows[-1].get("open_time")
                        if isinstance(last_open2, datetime):
                            prev = watermarks.get((sid, itv))
                            if prev is None or last_open2 > prev:
                                watermarks[(sid, itv)] = last_open2

                    # 6) выставляем next_due по интервалу (даже если rows пустые — не спамим)
                    next_due[(sid, itv)] = _utc_now() + timedelta(seconds=float(min_poll_by_interval[itv]))

                    # 7) пауза между запросами
                    if stop_event.wait(per_request_sleep_sec):
                        break

            logger.info("[Candles] poll batch=%d upserts=%d", len(batch), upserts)
            backoff = 1.0

            # базовый poll_sec — это пауза между циклами
            dt = time.time() - t0
            sleep = max(0.0, poll_sec - dt)
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
    poll_sec: float = 120.0,
    symbols_per_cycle: int = 2,
    per_request_sleep_sec: float = 0.35,
    seed_on_start: bool = False,
    seed_days: int = 0,
    seed_chunk_limit: int = 1500,
    overlap_points: int = 2,
    stop_event: threading.Event,
    loop_limit: int = 5,
    seed_limit: int = 0,
    update_open_candle: bool = False,  # ✅ новое
) -> threading.Thread:
    """
    Стартуем коллектор в отдельном потоке.
    """
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
            stop_event=stop_event,
            loop_limit=int(loop_limit),
            seed_limit=int(seed_limit),
            update_open_candle=bool(update_open_candle),
        ),
        daemon=True,
        name=f"BinanceCandlesCollector-{exchange_id}",
    )
    t.start()
    logger.info("[Candles] collector thread started")
    return t
