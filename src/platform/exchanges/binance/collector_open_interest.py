# src/platform/exchanges/binance/collector_open_interest.py
from __future__ import annotations

import logging
import os
import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

logger = logging.getLogger("src.platform.exchanges.binance.collector_open_interest")

# Binance openInterestHist periods (USDT-M):
#  "5m","15m","30m","1h","2h","4h","6h","12h","1d"
_PERIOD_MS: dict[str, int] = {
    "5m": 5 * 60 * 1000,
    "15m": 15 * 60 * 1000,
    "30m": 30 * 60 * 1000,
    "1h": 60 * 60 * 1000,
    "2h": 2 * 60 * 60 * 1000,
    "4h": 4 * 60 * 60 * 1000,
    "6h": 6 * 60 * 60 * 1000,
    "12h": 12 * 60 * 60 * 1000,
    "1d": 24 * 60 * 60 * 1000,
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


def _period_ms(period: str) -> int:
    p = str(period).strip()
    if p not in _PERIOD_MS:
        raise ValueError(f"Unsupported OI period: {p}. Allowed: {sorted(_PERIOD_MS.keys())}")
    return _PERIOD_MS[p]


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _classify_http_error(exc: Exception) -> Optional[int]:
    """
    Пытаемся вытащить HTTP код из сообщений вида:
      RuntimeError("Binance HTTP 429 GET /...: ...")
    или бинансовского -1003.
    """
    s = str(exc)

    # Binance weight / rpm overflow:
    if "-1003" in s or "Too many requests" in s:
        return 429

    for code in (451, 418, 429, 403, 400):
        if f"HTTP {code}" in s:
            return code
    return None


def _parse_oi_rows(
    *,
    exchange_id: int,
    symbol_id: int,
    interval: str,
    payload: list[dict[str, Any]],
    source: str,
) -> list[dict[str, Any]]:
    """
    Binance /futures/data/openInterestHist returns rows:
      {
        "sumOpenInterest": "12345.678",
        "sumOpenInterestValue": "999999.12",
        "timestamp": 1690000000000
      }
    """
    out: list[dict[str, Any]] = []
    for r in payload or []:
        ts_ms = r.get("timestamp") or r.get("time") or r.get("ts")
        oi = r.get("sumOpenInterest") or r.get("openInterest") or r.get("oi")
        oiv = r.get("sumOpenInterestValue") or r.get("openInterestValue") or r.get("oiValue")

        if ts_ms is None or oi is None or oiv is None:
            continue

        ts = _ms_to_dt(int(ts_ms))
        out.append(
            {
                "exchange_id": int(exchange_id),
                "symbol_id": int(symbol_id),
                "interval": str(interval),
                "ts": ts,
                "open_interest": _safe_float(oi, 0.0),
                "open_interest_value": _safe_float(oiv, 0.0),
                "source": str(source),
            }
        )
    return out


def _max_ts_from_rows(rows: list[dict[str, Any]]) -> datetime | None:
    if not rows:
        return None
    best: datetime | None = None
    for r in rows:
        ts = r.get("ts")
        if isinstance(ts, datetime):
            if best is None or ts > best:
                best = ts
    return best


def _chunked(seq: list[tuple[str, int]], n: int) -> Iterable[list[tuple[str, int]]]:
    n = max(1, int(n))
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _load_watermarks_from_db(*, storage, exchange_id: int, intervals: list[str]) -> dict[tuple[int, str], datetime]:
    """
    Loads max(ts) per (symbol_id, interval) from DB to continue after restarts.
    """
    wm: dict[tuple[int, str], datetime] = {}
    if not intervals:
        return wm

    pool = getattr(storage, "pool", None)
    if pool is None:
        logger.warning("[OI] storage.pool not found -> will use in-memory watermarks only")
        return wm

    for itv in intervals:
        sql = """
            SELECT symbol_id, MAX(ts) AS last_ts
            FROM open_interest
            WHERE exchange_id = %s AND interval = %s
            GROUP BY symbol_id
        """
        try:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (int(exchange_id), str(itv)))
                    for sid, last_ts in cur.fetchall():
                        if sid is None or last_ts is None:
                            continue
                        wm[(int(sid), str(itv))] = last_ts
        except Exception:
            logger.exception("[OI] failed to load watermarks for interval=%s", itv)

    return wm


class _LocalRateLimiter:
    """
    Локальный лимитер, чтобы не устраивать burst внутри ОДНОГО потока.
    Не заменяет глобальный лимитер BinanceFuturesREST, но снижает шанс 429.

    - max_rps: ограничение по запросам/сек (минимальный интервал)
    - max_rpm: ограничение по запросам/мин (простой бакет 60 секунд)
    """

    def __init__(self, *, max_rps: float, max_rpm: int, jitter_sec: float = 0.15) -> None:
        self.max_rps = float(max_rps)
        self.max_rpm = int(max_rpm)
        self.jitter_sec = float(max(0.0, jitter_sec))

        self._min_interval = (1.0 / self.max_rps) if self.max_rps > 0 else 0.0

        self._lock = threading.Lock()
        self._last_ts = 0.0

        self._bucket_window_start = time.monotonic()
        self._bucket_count = 0

    def wait(self, stop_event: threading.Event) -> None:
        if self.max_rps <= 0 and self.max_rpm <= 0:
            return

        while not stop_event.is_set():
            with self._lock:
                now = time.monotonic()

                # reset rpm bucket
                if self.max_rpm > 0 and (now - self._bucket_window_start) >= 60.0:
                    self._bucket_window_start = now
                    self._bucket_count = 0

                sleep_rpm = 0.0
                if self.max_rpm > 0 and self._bucket_count >= self.max_rpm:
                    sleep_rpm = max(0.05, 60.0 - (now - self._bucket_window_start))

                sleep_rps = 0.0
                if self._min_interval > 0:
                    sleep_rps = max(0.0, self._min_interval - (now - self._last_ts))

                sleep = max(sleep_rpm, sleep_rps)
                if sleep <= 0:
                    # consume one request
                    self._last_ts = time.monotonic()
                    if self.max_rpm > 0:
                        self._bucket_count += 1
                    return

            # небольшой джиттер, чтобы разные потоки (candles/OI) не синхронизировались
            if self.jitter_sec > 0:
                sleep = sleep + random.uniform(0.0, self.jitter_sec)
            stop_event.wait(sleep)


@dataclass
class _SeedPlan:
    enabled: bool
    days: int
    periods: list[str]


def run_open_interest_collector(
    *,
    storage,
    rest,
    exchange_id: int,
    symbol_ids: dict[str, int],
    cfg: dict,
    stop_event: threading.Event,
) -> None:
    cfg = cfg or {}
    enabled = bool(cfg.get("enabled", True))
    if not enabled:
        logger.info("[OI] disabled by config")
        return

    periods = cfg.get("periods") or cfg.get("intervals") or ["5m", "15m", "1h"]
    periods = [str(p).strip() for p in (periods or []) if str(p).strip()]
    if not periods:
        logger.warning("[OI] no periods configured -> skip")
        return

    for p in periods:
        _period_ms(p)

    poll_sec = float(cfg.get("poll_sec", 60))
    symbols_per_cycle = int(cfg.get("symbols_per_cycle", 20))
    req_sleep_sec = float(cfg.get("req_sleep_sec", 0.2))

    limit = int(cfg.get("limit", 500))
    limit = max(1, min(limit, 500))
    if limit < 10:
        logger.warning("[OI] limit=%d is too small -> will cause too many requests", limit)

    overlap_points = int(cfg.get("overlap_points", 2))
    overlap_points = max(0, overlap_points)

    lookback_hours_if_empty = int(cfg.get("lookback_hours_if_empty", 48))
    lookback_hours_if_empty = max(1, lookback_hours_if_empty)

    # Если watermark почти свежий — можно пропускать запрос (сильно снижает трафик)
    skip_if_fresh = bool(cfg.get("skip_if_fresh", True))
    fresh_ratio = float(cfg.get("fresh_ratio", 0.7))  # 0.7 * period

    # SEED controls
    seed_on_start = bool(cfg.get("seed_on_start", True))
    seed_days = int(cfg.get("seed_days", 30))
    seed_days = max(0, seed_days)

    seed_symbols_per_cycle = int(cfg.get("seed_symbols_per_cycle", 5))
    seed_req_sleep_sec = float(cfg.get("seed_req_sleep_sec", 0.35))

    # page throttling (важно для seed)
    seed_page_sleep_sec = float(cfg.get("seed_page_sleep_sec", max(0.20, seed_req_sleep_sec)))
    seed_page_backoff_sec = float(cfg.get("seed_page_backoff_sec", 1.0))

    # local limiter
    max_rps = float(cfg.get("max_rps", float(os.getenv("OI_MAX_RPS", "1.0"))))
    max_rpm = int(cfg.get("max_rpm", int(os.getenv("OI_MAX_RPM", "300"))))
    limiter = _LocalRateLimiter(max_rps=max_rps, max_rpm=max_rpm)

    symbols = sorted(dict(symbol_ids or {}).items(), key=lambda x: x[0])
    total_symbols = len(symbols)

    logger.info(
        "[OI] started symbols=%d periods=%s poll=%.1fs batch=%d limit=%d local_rl=(rps=%.2f rpm=%d) skip_if_fresh=%s",
        total_symbols,
        ",".join(periods),
        poll_sec,
        symbols_per_cycle,
        limit,
        max_rps,
        max_rpm,
        skip_if_fresh,
    )

    watermarks = _load_watermarks_from_db(storage=storage, exchange_id=exchange_id, intervals=periods)
    logger.info("[OI] loaded watermarks from DB: %d", len(watermarks))

    # per (sym, period) cooldown
    cooldown_until: dict[tuple[str, str], datetime] = {}
    cooldown_hits: dict[tuple[str, str], int] = {}

    # ---------- SEED stage ----------
    if seed_on_start and seed_days > 0:
        _seed_all_symbols(
            storage=storage,
            rest=rest,
            exchange_id=exchange_id,
            symbols=symbols,
            periods=periods,
            seed_days=seed_days,
            limit=limit,
            overlap_points=overlap_points,
            symbols_per_cycle=seed_symbols_per_cycle,
            req_sleep_sec=seed_req_sleep_sec,
            page_sleep_sec=seed_page_sleep_sec,
            page_backoff_sec=seed_page_backoff_sec,
            stop_event=stop_event,
            watermarks=watermarks,
            limiter=limiter,
            cooldown_until=cooldown_until,
            cooldown_hits=cooldown_hits,
        )

    # ---------- Incremental loop ----------
    rr_index = 0
    backoff = 1.0

    while not stop_event.is_set():
        t0 = time.time()
        now = _utc_now()

        if total_symbols == 0:
            stop_event.wait(5.0)
            continue

        batch = symbols[rr_index : rr_index + max(1, symbols_per_cycle)]
        if not batch:
            rr_index = 0
            batch = symbols[: max(1, symbols_per_cycle)]
        rr_index = (rr_index + len(batch)) % total_symbols

        upserts = 0

        try:
            for sym, sid in batch:
                for p in periods:
                    if stop_event.is_set():
                        break

                    cd = cooldown_until.get((sym, p))
                    if cd is not None and cd > now:
                        continue

                    last_ts = watermarks.get((sid, p))

                    # optional: skip if watermark is fresh enough
                    if skip_if_fresh and last_ts is not None:
                        age_ms = int((now - last_ts).total_seconds() * 1000)
                        if age_ms < int(_period_ms(p) * max(0.1, fresh_ratio)):
                            continue

                    if last_ts is None:
                        start_dt = now - timedelta(hours=lookback_hours_if_empty)
                    else:
                        start_dt = last_ts - timedelta(milliseconds=_period_ms(p) * max(1, overlap_points))

                    end_dt = now

                    try:
                        limiter.wait(stop_event)

                        payload = rest.open_interest_hist(
                            symbol=sym,
                            period=p,
                            start_time_ms=_to_ms(start_dt),
                            end_time_ms=_to_ms(end_dt),
                            limit=limit,
                        )
                        rows = _parse_oi_rows(
                            exchange_id=exchange_id,
                            symbol_id=sid,
                            interval=p,
                            payload=payload or [],
                            source="rest",
                        )
                        n = storage.upsert_open_interest(rows)
                        upserts += int(n or 0)

                        max_dt = _max_ts_from_rows(rows)
                        if max_dt is not None:
                            watermarks[(sid, p)] = max_dt

                        # дружелюбная пауза между запросами (помимо limiter)
                        if req_sleep_sec > 0:
                            stop_event.wait(req_sleep_sec)

                    except Exception as e:
                        code = _classify_http_error(e)
                        if code in (418, 429):
                            key = (sym, p)
                            cooldown_hits[key] = int(cooldown_hits.get(key, 0)) + 1
                            hit = cooldown_hits[key]

                            # растущий cooldown
                            cd_minutes = min(60, 5 + 5 * hit)
                            cooldown_until[key] = _utc_now() + timedelta(minutes=cd_minutes)

                            logger.warning("[OI] %s %s HTTP=%s -> cooldown %dm (hit=%d)", sym, p, code, cd_minutes, hit, exc_info=True)

                            stop_event.wait(min(30.0, backoff))
                            backoff = min(backoff * 2.0, 120.0)

                        elif code == 451:
                            cooldown_until[(sym, p)] = _utc_now() + timedelta(hours=6)
                            logger.warning("[OI] %s %s HTTP=451 -> cooldown 6h", sym, p, exc_info=True)
                        else:
                            logger.exception("[OI] %s period=%s failed", sym, p)

            logger.info("[OI] cycle upserts=%d symbols=%d periods=%d", upserts, len(batch), len(periods))
            backoff = 1.0

        except Exception as e:
            logger.warning("[OI] loop error: %s", e, exc_info=True)
            stop_event.wait(backoff)
            backoff = min(backoff * 2.0, 300.0)

        dt = time.time() - t0
        sleep = max(0.0, poll_sec - dt)
        stop_event.wait(sleep)


def _seed_all_symbols(
    *,
    storage,
    rest,
    exchange_id: int,
    symbols: list[tuple[str, int]],
    periods: list[str],
    seed_days: int,
    limit: int,
    overlap_points: int,
    symbols_per_cycle: int,
    req_sleep_sec: float,
    page_sleep_sec: float,
    page_backoff_sec: float,
    stop_event: threading.Event,
    watermarks: dict[tuple[int, str], datetime],
    limiter: _LocalRateLimiter,
    cooldown_until: dict[tuple[str, str], datetime],
    cooldown_hits: dict[tuple[str, str], int],
) -> None:
    if not symbols:
        logger.info("[OI][Seed] no symbols -> skip")
        return

    now = _utc_now()
    global_from = now - timedelta(days=int(seed_days))
    global_to = now

    logger.info(
        "[OI][Seed] start ALL symbols=%d periods=%s window=%s..%s limit=%d batch=%d sleep=%.2fs page_sleep=%.2fs",
        len(symbols),
        ",".join(periods),
        global_from.isoformat(timespec="seconds"),
        global_to.isoformat(timespec="seconds"),
        limit,
        symbols_per_cycle,
        req_sleep_sec,
        page_sleep_sec,
    )

    seeded_upserts = 0
    seeded_symbols = 0

    for batch in _chunked(symbols, symbols_per_cycle):
        if stop_event.is_set():
            break

        for sym, sid in batch:
            if stop_event.is_set():
                break

            for p in periods:
                if stop_event.is_set():
                    break

                cd = cooldown_until.get((sym, p))
                if cd is not None and cd > _utc_now():
                    continue

                last_ts = watermarks.get((sid, p))
                if last_ts is None:
                    start_dt = global_from
                else:
                    start_dt = max(
                        global_from,
                        last_ts - timedelta(milliseconds=_period_ms(p) * max(1, overlap_points)),
                    )

                try:
                    n, max_dt = _seed_symbol_period(
                        storage=storage,
                        rest=rest,
                        exchange_id=exchange_id,
                        sym=sym,
                        sid=sid,
                        period=p,
                        start_dt=start_dt,
                        end_dt=global_to,
                        limit=limit,
                        stop_event=stop_event,
                        limiter=limiter,
                        page_sleep_sec=page_sleep_sec,
                        page_backoff_sec=page_backoff_sec,
                    )
                    seeded_upserts += n
                    if max_dt is not None:
                        watermarks[(sid, p)] = max_dt

                except Exception as e:
                    code = _classify_http_error(e)
                    if code in (418, 429):
                        key = (sym, p)
                        cooldown_hits[key] = int(cooldown_hits.get(key, 0)) + 1
                        hit = cooldown_hits[key]
                        cd_minutes = min(120, 10 + 10 * hit)
                        cooldown_until[key] = _utc_now() + timedelta(minutes=cd_minutes)
                        logger.warning("[OI][Seed] %s %s HTTP=%s -> cooldown %dm (hit=%d)", sym, p, code, cd_minutes, hit, exc_info=True)
                        stop_event.wait(10.0)
                    elif code == 451:
                        cooldown_until[(sym, p)] = _utc_now() + timedelta(hours=6)
                        logger.warning("[OI][Seed] %s %s HTTP=451 -> cooldown 6h", sym, p, exc_info=True)
                    else:
                        logger.exception("[OI][Seed] %s period=%s failed", sym, p)

                # пауза между (symbol,period)
                if req_sleep_sec > 0:
                    stop_event.wait(req_sleep_sec)

            seeded_symbols += 1

        logger.info("[OI][Seed] progress symbols=%d/%d upserts=%d", seeded_symbols, len(symbols), seeded_upserts)

    logger.info("[OI][Seed] DONE symbols=%d upserts=%d", seeded_symbols, seeded_upserts)


def _seed_symbol_period(
    *,
    storage,
    rest,
    exchange_id: int,
    sym: str,
    sid: int,
    period: str,
    start_dt: datetime,
    end_dt: datetime,
    limit: int,
    stop_event: threading.Event,
    limiter: _LocalRateLimiter,
    page_sleep_sec: float,
    page_backoff_sec: float,
) -> tuple[int, datetime | None]:
    """
    Paged seed for one symbol+period.

    КРИТИЧНО:
      - пауза ПОСЛЕ каждой страницы
      - при 429/418 — backoff и повтор
    """
    step_ms = _period_ms(period)
    page_ms = step_ms * int(limit)
    if page_ms <= 0:
        return 0, None

    start_ms = _to_ms(start_dt)
    end_ms = _to_ms(end_dt)
    if start_ms >= end_ms:
        return 0, None

    upserts = 0
    cursor = start_ms
    best_dt: datetime | None = None

    backoff = float(max(0.5, page_backoff_sec))
    safety_iters = 0
    max_iters = 200_000  # защита от вечных циклов на странных ответах

    while cursor < end_ms and not stop_event.is_set():
        safety_iters += 1
        if safety_iters > max_iters:
            logger.warning("[OI][SeedPage] %s %s reached max_iters -> stop", sym, period)
            break

        nxt = min(end_ms, cursor + page_ms)

        try:
            limiter.wait(stop_event)

            payload = rest.open_interest_hist(
                symbol=sym,
                period=period,
                start_time_ms=int(cursor),
                end_time_ms=int(nxt),
                limit=int(limit),
            )

            rows = _parse_oi_rows(
                exchange_id=exchange_id,
                symbol_id=sid,
                interval=period,
                payload=payload or [],
                source="rest_seed",
            )
            n = storage.upsert_open_interest(rows)
            upserts += int(n or 0)

            max_dt = _max_ts_from_rows(rows)
            if max_dt is not None:
                best_dt = max_dt if (best_dt is None or max_dt > best_dt) else best_dt

            # если ответ пустой — всё равно двигаемся вперёд, чтобы не зависнуть на дыре
            cursor = nxt + step_ms

            # пауза после страницы (самое важное)
            if page_sleep_sec > 0:
                stop_event.wait(page_sleep_sec)

            backoff = float(max(0.5, page_backoff_sec))

        except Exception as e:
            code = _classify_http_error(e)
            if code in (418, 429):
                logger.warning(
                    "[OI][SeedPage] %s %s HTTP=%s -> backoff %.1fs (cursor=%s..%s)",
                    sym,
                    period,
                    code,
                    backoff,
                    cursor,
                    nxt,
                    exc_info=True,
                )
                stop_event.wait(backoff)
                backoff = min(backoff * 2.0, 120.0)
                continue
            if code == 451:
                logger.warning("[OI][SeedPage] %s %s HTTP=451 restricted -> stop this symbol/period", sym, period, exc_info=True)
                break

            logger.exception("[OI][SeedPage] %s %s failed", sym, period)
            stop_event.wait(min(backoff, 10.0))
            backoff = min(backoff * 2.0, 60.0)

    return upserts, best_dt


def start_open_interest_collector(
    *,
    rest,
    storage,
    exchange_id: int,
    symbol_ids: dict[str, int],
    cfg: dict,
    stop_event: threading.Event,
) -> threading.Thread:
    """
    Thin wrapper: запускает run_open_interest_collector в daemon-thread.
    """
    t = threading.Thread(
        target=run_open_interest_collector,
        kwargs=dict(
            storage=storage,
            rest=rest,
            exchange_id=int(exchange_id),
            symbol_ids=dict(symbol_ids or {}),
            cfg=dict(cfg or {}),
            stop_event=stop_event,
        ),
        daemon=True,
        name="OpenInterestCollector",
    )
    t.start()
    return t
