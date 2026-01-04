# src/platform/exchanges/binance/collector_open_interest.py
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

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


def _parse_oi_rows(
    *,
    exchange_id: int,
    symbol_id: int,
    interval: str,
    payload: list[dict[str, Any]],
    source: str,
) -> list[dict[str, Any]]:
    """
    Binance /futures/data/openInterestHist typically returns rows:
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


def _chunked(seq: list[tuple[str, int]], n: int) -> Iterable[list[tuple[str, int]]]:
    n = max(1, int(n))
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _load_watermarks_from_db(*, storage, exchange_id: int, intervals: list[str]) -> dict[tuple[int, str], datetime]:
    """
    Loads max(ts) per (symbol_id, interval) from DB to continue incrementally
    after restarts.

    Query per interval (fast enough for ~500-1000 symbols).
    """
    wm: dict[tuple[int, str], datetime] = {}
    if not intervals:
        return wm

    # storage is PostgreSQLStorage, has pool inside
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
    """
    Open Interest collector:
      1) SEED once for ALL symbols (history window seed_days)
      2) then incremental updates forever (adds new points only)

    Suggested config:

    open_interest:
      enabled: true
      periods: ["5m","15m","1h"]
      poll_sec: 60
      symbols_per_cycle: 20
      req_sleep_sec: 0.20

      # SEED (one-time)
      seed_on_start: true
      seed_days: 30
      seed_symbols_per_cycle: 5
      seed_req_sleep_sec: 0.35
      limit: 500          # Binance max is typically 500 for this endpoint
      overlap_points: 2   # refetch last N points to avoid gaps
      lookback_hours_if_empty: 48
    """
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

    # validate periods early
    for p in periods:
        _period_ms(p)

    poll_sec = float(cfg.get("poll_sec", 60))
    symbols_per_cycle = int(cfg.get("symbols_per_cycle", 20))
    req_sleep_sec = float(cfg.get("req_sleep_sec", 0.2))

    limit = int(cfg.get("limit", 500))
    limit = max(1, min(limit, 500))

    overlap_points = int(cfg.get("overlap_points", 2))
    overlap_points = max(0, overlap_points)

    lookback_hours_if_empty = int(cfg.get("lookback_hours_if_empty", 48))
    lookback_hours_if_empty = max(1, lookback_hours_if_empty)

    seed_on_start = bool(cfg.get("seed_on_start", True))
    seed_days = int(cfg.get("seed_days", 30))
    seed_days = max(0, seed_days)
    seed_symbols_per_cycle = int(cfg.get("seed_symbols_per_cycle", 5))
    seed_req_sleep_sec = float(cfg.get("seed_req_sleep_sec", 0.35))

    symbols = sorted(symbol_ids.items(), key=lambda x: x[0])
    total_symbols = len(symbols)

    logger.info(
        "[OI] started symbols=%d periods=%s poll=%.1fs batch=%d limit=%d",
        total_symbols,
        ",".join(periods),
        poll_sec,
        symbols_per_cycle,
        limit,
    )

    # ---------- Watermarks (from DB) ----------
    watermarks = _load_watermarks_from_db(storage=storage, exchange_id=exchange_id, intervals=periods)
    logger.info("[OI] loaded watermarks from DB: %d", len(watermarks))

    # ---------- SEED stage (one-time, ALL symbols) ----------
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
            stop_event=stop_event,
            watermarks=watermarks,
        )

    # ---------- Incremental loop ----------
    rr_index = 0
    while not stop_event.is_set():
        t0 = time.time()

        if total_symbols == 0:
            stop_event.wait(5.0)
            continue

        batch = symbols[rr_index : rr_index + max(1, symbols_per_cycle)]
        if not batch:
            rr_index = 0
            batch = symbols[: max(1, symbols_per_cycle)]

        rr_index = (rr_index + len(batch)) % total_symbols

        upserts = 0
        now = _utc_now()

        for sym, sid in batch:
            for p in periods:
                if stop_event.is_set():
                    break

                last_ts = watermarks.get((sid, p))
                if last_ts is None:
                    # If DB empty, fetch a small window first
                    start_dt = now - timedelta(hours=lookback_hours_if_empty)
                else:
                    # overlap last N points to avoid gaps
                    start_dt = last_ts - timedelta(milliseconds=_period_ms(p) * max(1, overlap_points))

                end_dt = now

                try:
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

                    # update watermark
                    max_dt = _max_ts_from_rows(rows)
                    if max_dt is not None:
                        watermarks[(sid, p)] = max_dt

                except Exception:
                    logger.exception("[OI] %s period=%s failed", sym, p)

                time.sleep(req_sleep_sec)

        logger.info("[OI] cycle upserts=%d symbols=%d periods=%d", upserts, len(batch), len(periods))

        dt = time.time() - t0
        sleep = max(0.0, poll_sec - dt)
        stop_event.wait(sleep)


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
    stop_event: threading.Event,
    watermarks: dict[tuple[int, str], datetime],
) -> None:
    """
    SEED once: for ALL symbols, for ALL periods, load historical OI for last seed_days.

    Important:
      - This can be heavy for many symbols. Use throttling:
        seed_symbols_per_cycle + seed_req_sleep_sec
      - Continues from DB watermarks; if a symbol already has data, seed starts
        from max(existing_ts, now-seed_days).
    """
    if not symbols:
        logger.info("[OI][Seed] no symbols -> skip")
        return

    now = _utc_now()
    global_from = now - timedelta(days=int(seed_days))
    global_to = now

    logger.info(
        "[OI][Seed] start ALL symbols=%d periods=%s window=%s..%s limit=%d batch=%d sleep=%.2fs",
        len(symbols),
        ",".join(periods),
        global_from.isoformat(timespec="seconds"),
        global_to.isoformat(timespec="seconds"),
        limit,
        symbols_per_cycle,
        req_sleep_sec,
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

                last_ts = watermarks.get((sid, p))
                if last_ts is None:
                    start_dt = global_from
                else:
                    start_dt = max(
                        global_from,
                        last_ts - timedelta(milliseconds=_period_ms(p) * max(1, overlap_points)),
                    )

                try:
                    n = _seed_symbol_period(
                        storage=storage,
                        rest=rest,
                        exchange_id=exchange_id,
                        sym=sym,
                        sid=sid,
                        period=p,
                        start_dt=start_dt,
                        end_dt=global_to,
                        limit=limit,
                    )
                    seeded_upserts += n
                except Exception:
                    logger.exception("[OI][Seed] %s period=%s failed", sym, p)

                time.sleep(req_sleep_sec)

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
) -> int:
    """
    Paged seed for one symbol+period.

    Uses startTime/endTime with step ~= period_ms * limit
    """
    step_ms = _period_ms(period)
    page_ms = step_ms * int(limit)
    if page_ms <= 0:
        return 0

    start_ms = _to_ms(start_dt)
    end_ms = _to_ms(end_dt)
    if start_ms >= end_ms:
        return 0

    upserts = 0
    cursor = start_ms

    while cursor < end_ms:
        nxt = min(end_ms, cursor + page_ms)

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

        # move cursor forward; if empty payload, still advance by page_ms
        cursor = nxt + step_ms  # avoid re-fetch same boundary

        # safety: don't spin too fast if API returns nonsense
        if cursor <= start_ms:
            cursor = start_ms + page_ms

    return upserts


# ---------------------------------------------------------------------------
# ✅ ДОБАВЛЕНО: starter, который ожидает run_market_data.py
# ---------------------------------------------------------------------------
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
            symbol_ids=symbol_ids,
            cfg=cfg or {},
            stop_event=stop_event,
        ),
        daemon=True,
        name="OpenInterestCollector",
    )
    t.start()
    return t
