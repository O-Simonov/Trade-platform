# src/platform/exchanges/binance/collector_candles_gap_repair.py
from __future__ import annotations

import logging
import random
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Any, List, Tuple, Optional, Sequence

from src.platform.exchanges.binance.collector_candles import _parse_klines_payload

log = logging.getLogger(__name__)


# =========================
# Helpers
# =========================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _itv_to_timedelta(itv: str) -> timedelta:
    itv = str(itv).strip().lower()
    if itv.endswith("m"):
        return timedelta(minutes=int(itv[:-1]))
    if itv.endswith("h"):
        return timedelta(hours=int(itv[:-1]))
    if itv.endswith("d"):
        return timedelta(days=int(itv[:-1]))
    raise ValueError(f"Unsupported interval: {itv}")


def _itv_to_pg_interval_str(itv: str) -> str:
    """
    Строка, которую PostgreSQL понимает как ::interval.
    Примеры: '5 minutes', '4 hours', '1 day'
    """
    itv = str(itv).strip().lower()
    if itv.endswith("m"):
        n = int(itv[:-1])
        return f"{n} minutes"
    if itv.endswith("h"):
        n = int(itv[:-1])
        return f"{n} hours"
    if itv.endswith("d"):
        n = int(itv[:-1])
        return f"{n} days"
    raise ValueError(f"Unsupported interval: {itv}")


def _norm_symbols_list(xs: Any) -> List[str]:
    if xs is None:
        return []
    if isinstance(xs, str):
        xs = [xs]
    if not isinstance(xs, (list, tuple, set)):
        return []
    out: List[str] = []
    for x in xs:
        s = str(x).upper().strip()
        if s:
            out.append(s)
    return out


def _load_symbols(storage, exchange_id: int, only_active: bool) -> List[Tuple[int, str]]:
    where = "WHERE exchange_id=%s"
    args: list[Any] = [int(exchange_id)]
    if only_active:
        where += " AND is_active=true"

    sql = f"SELECT symbol_id, symbol FROM symbols {where} ORDER BY symbol_id"
    with storage.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            rows = cur.fetchall()
    return [(int(r[0]), str(r[1])) for r in (rows or [])]


def _prioritize_symbols(
    symbols: List[Tuple[int, str]],
    *,
    priority: Sequence[str],
    shuffle_tail: bool = True,
) -> List[Tuple[int, str]]:
    """
    В начало списка ставим priority_symbols (если есть в symbols).
    Сохраняем относительный порядок внутри priority.
    Остальную часть (tail) можно перемешать.
    """
    pr = [str(s).upper().strip() for s in priority if str(s).strip()]
    if not pr:
        return symbols

    idx = {sym.upper(): i for i, sym in enumerate(pr)}
    pri: List[Tuple[int, str]] = []
    other: List[Tuple[int, str]] = []

    for sid, sym in symbols:
        if sym.upper() in idx:
            pri.append((sid, sym))
        else:
            other.append((sid, sym))

    pri.sort(key=lambda x: idx.get(x[1].upper(), 10**9))
    if shuffle_tail:
        random.shuffle(other)
    return pri + other


def _align_dt_to_interval_floor(dt: datetime, itv: str) -> datetime:
    """
    Выравниваем время к границе интервала (по epoch).
    Это делает REST pagination стабильнее и исключает "кривые" start_time.
    """
    td = _itv_to_timedelta(itv)
    sec = int(td.total_seconds())
    if sec <= 0:
        return dt

    ts = int(dt.timestamp())
    floored = (ts // sec) * sec
    return datetime.fromtimestamp(floored, tz=timezone.utc)


def _rest_klines_call_safe(
    rest: Any,
    *,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int,
) -> List[list]:

    """
    Универсальный REST klines call, чтобы gap_repair работал с любым rest.py.
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

    raise RuntimeError("REST klines method not found (expected klines/get_klines/fapi_klines)")


# =========================
# Gap scan
# =========================

def _find_gaps(
    storage,
    exchange_id: int,
    symbol_id: int,
    interval: str,
    limit_gaps: int,
    *,
    max_gap_days: int = 0,
    min_gap_points: int = 1,
) -> List[Tuple[datetime, datetime]]:
    """
    Дыра: open_time > prev + interval

    Возвращаем:
      from_ts = prev + interval
      to_ts   = open_time (первая существующая свеча после дыры)

    Фильтры:
      - max_gap_days: если >0, берём только дыры, где to_ts не старше (NOW() - max_gap_days).
      - min_gap_points: минимальная длина дыры (в пропущенных барах).
        k пропущенных баров => gap >= (k+1)*interval.
    """
    interval = str(interval).strip()
    itv_sql = _itv_to_pg_interval_str(interval)
    min_gap_points = max(1, int(min_gap_points))
    max_gap_days = max(0, int(max_gap_days))

    mult = int(min_gap_points) + 1

    sql = """
    WITH t AS (
      SELECT
        open_time,
        LAG(open_time) OVER (ORDER BY open_time) AS prev
      FROM candles
      WHERE exchange_id=%s AND symbol_id=%s AND interval=%s
    ),
    g AS (
      SELECT
        prev,
        open_time,
        (open_time - prev) AS gap
      FROM t
      WHERE prev IS NOT NULL
    )
    SELECT
      (prev + (%s)::interval) AS from_ts,
      open_time               AS to_ts
    FROM g
    WHERE open_time > prev + (%s)::interval
      AND gap >= ((%s)::interval * %s)
    """

    args: list[Any] = [
        int(exchange_id),
        int(symbol_id),
        interval,
        itv_sql,
        itv_sql,
        itv_sql,
        mult,
    ]

    if max_gap_days > 0:
        sql += " AND open_time >= NOW() - (%s)::interval\n"
        args.append(f"{max_gap_days} days")

    sql += """
    ORDER BY gap DESC
    LIMIT %s;
    """
    args.append(int(limit_gaps))

    with storage.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            rows = cur.fetchall()

    out: List[Tuple[datetime, datetime]] = []
    for a, b in (rows or []):
        if isinstance(a, datetime) and isinstance(b, datetime) and a < b:
            out.append((a, b))
    return out


# =========================
# Backfill
# =========================

def _backfill_range(
    *,
    rest,
    storage,
    exchange_id: int,
    symbol: str,
    symbol_id: int,
    interval: str,
    dt_from: datetime,
    dt_to: datetime,
    limit: int,
    sleep_sec: float,
    stop_event: threading.Event,
    max_span_days: int = 0,
    step_by_interval: bool = True,
) -> int:
    """
    Дозаполняем свечи в диапазоне [dt_from, dt_to)

    - dt_to — open_time существующей свечи (её НЕ трогаем)
    - end_time Binance часто inclusive => dt_to - 1ms
    """
    interval = str(interval).strip()
    itv_td = _itv_to_timedelta(interval)

    limit = max(1, min(int(limit), 1500))
    sleep_sec = max(0.05, float(sleep_sec))

    if max_span_days and int(max_span_days) > 0:
        span_limit = dt_to - timedelta(days=int(max_span_days))
        if dt_from < span_limit:
            dt_from = span_limit

    # ✅ выравниваем старт по границе интервала
    cur = _align_dt_to_interval_floor(dt_from, interval)

    total = 0

    # endTime inclusive -> dt_to-1ms
    dt_to_exclusive = dt_to - timedelta(milliseconds=1)
    end_ms = _to_ms(dt_to_exclusive)

    safety_iters = 0
    last_progress_open: Optional[datetime] = None

    while cur < dt_to and not stop_event.is_set():
        safety_iters += 1
        if safety_iters > 20000:
            log.warning("[GapRepair] safety break (too many iters) %s %s", symbol, interval)
            break

        payload = _rest_klines_call_safe(
            rest,
            symbol=symbol,
            interval=interval,
            start_ms=_to_ms(cur),
            end_ms=end_ms,
            limit=limit,
        )

        rows = _parse_klines_payload(
            exchange_id=int(exchange_id),
            symbol_id=int(symbol_id),
            interval=str(interval),
            payload=payload,
            source="gap_repair",
        )

        if not rows:
            break

        now = _utcnow()
        for r in rows:
            r["source"] = "gap_repair"
            r["updated_at"] = now

        n = int(storage.upsert_candles(rows) or 0)
        total += n

        last_open = rows[-1].get("open_time")
        if not isinstance(last_open, datetime):
            break

        if last_progress_open is not None and last_open <= last_progress_open:
            log.warning(
                "[GapRepair] no progress %s %s last_open=%s prev=%s",
                symbol, interval,
                last_open.isoformat(),
                last_progress_open.isoformat(),
            )
            break
        last_progress_open = last_open

        # шагаем на следующий бар
        if step_by_interval:
            nxt = last_open + itv_td
            if nxt <= cur:
                nxt = last_open + timedelta(milliseconds=1)
            cur = nxt
        else:
            cur = last_open + timedelta(milliseconds=1)

        # если вернулось меньше лимита — дальше в этом окне нечего брать
        if len(rows) < limit:
            break

        if stop_event.wait(sleep_sec):
            break

        if cur > dt_to - itv_td:
            break

    return total


# =========================
# Main loop
# =========================

def run_candles_gap_repair(
    *,
    cfg: dict,
    rest,
    storage,
    exchange_id: int,
    stop_event: threading.Event,
) -> None:
    gcfg = (cfg or {}).get("candles_gap_repair", {}) or {}
    if not bool(gcfg.get("enabled", False)):
        log.info("[GapRepair] disabled")
        return

    intervals = [str(x).strip() for x in (gcfg.get("intervals") or []) if str(x).strip()]
    if not intervals:
        log.warning("[GapRepair] intervals empty -> stop")
        return

    every_sec = max(30, int(gcfg.get("every_sec", 1800)))
    max_symbols_per_cycle = max(1, int(gcfg.get("max_symbols_per_cycle", 80)))
    max_gaps_per_cycle = max(1, int(gcfg.get("max_gaps_per_cycle", 50)))
    gaps_per_symbol = max(1, int(gcfg.get("gaps_per_symbol", 3)))

    limit = int(gcfg.get("limit", 1500))
    sleep_sec = float(gcfg.get("sleep_sec", 0.25))
    only_active = bool(gcfg.get("only_active_symbols", True))

    priority_symbols = _norm_symbols_list(gcfg.get("priority_symbols"))

    max_gap_days = int(gcfg.get("max_gap_days", 30) or 30)  # ✅ лучше по умолчанию 30
    max_backfill_span_days = int(gcfg.get("max_backfill_span_days", 0) or 0)
    min_gap_points = int(gcfg.get("min_gap_points", 1) or 1)

    prio_mode = int(gcfg.get("priority", 1) or 1)
    prio_mode = max(0, min(2, prio_mode))

    if prio_mode >= 2:
        limit = min(limit, 600)
        max_gaps_per_cycle = min(max_gaps_per_cycle, 12)
        max_symbols_per_cycle = min(max_symbols_per_cycle, 35)
        sleep_sec = max(sleep_sec, 0.75)
        step_by_interval = True
    elif prio_mode == 1:
        limit = min(limit, 1000)
        max_gaps_per_cycle = min(max_gaps_per_cycle, 20)
        sleep_sec = max(sleep_sec, 0.35)
        step_by_interval = True
    else:
        step_by_interval = True

    log.info(
        "[GapRepair] started intervals=%s every=%ss max_symbols=%d max_gaps=%d gaps_per_symbol=%d "
        "limit=%d sleep=%.2fs only_active=%s priority_symbols=%d max_gap_days=%d max_span_days=%d min_gap_points=%d prio=%d",
        ",".join(intervals),
        every_sec,
        max_symbols_per_cycle,
        max_gaps_per_cycle,
        gaps_per_symbol,
        limit,
        sleep_sec,
        only_active,
        len(priority_symbols),
        max_gap_days,
        max_backfill_span_days,
        min_gap_points,
        prio_mode,
    )

    stop_event.wait(random.uniform(0.1, 1.0))

    while not stop_event.is_set():
        t0 = time.time()

        try:
            symbols = _load_symbols(storage, exchange_id=int(exchange_id), only_active=only_active)
        except Exception:
            log.exception("[GapRepair] load symbols failed")
            if stop_event.wait(min(60, every_sec)):
                break
            continue

        if not symbols:
            log.warning("[GapRepair] no symbols")
            if stop_event.wait(every_sec):
                break
            continue

        if priority_symbols:
            symbols = _prioritize_symbols(symbols, priority=priority_symbols, shuffle_tail=True)
        else:
            random.shuffle(symbols)

        symbols = symbols[:max_symbols_per_cycle]

        gaps_fixed = 0
        upserts_total = 0

        for symbol_id, symbol in symbols:
            if gaps_fixed >= max_gaps_per_cycle or stop_event.is_set():
                break

            for itv in intervals:
                if gaps_fixed >= max_gaps_per_cycle or stop_event.is_set():
                    break

                try:
                    gaps = _find_gaps(
                        storage=storage,
                        exchange_id=int(exchange_id),
                        symbol_id=int(symbol_id),
                        interval=str(itv),
                        limit_gaps=int(gaps_per_symbol),
                        max_gap_days=max_gap_days,
                        min_gap_points=min_gap_points,
                    )
                except Exception:
                    log.exception("[GapRepair] find gaps failed %s %s", symbol, itv)
                    continue

                for dt_from, dt_to in gaps:
                    if gaps_fixed >= max_gaps_per_cycle or stop_event.is_set():
                        break

                    log.info("[GapRepair] gap %s %s %s -> %s", symbol, itv, dt_from.isoformat(), dt_to.isoformat())
                    try:
                        n = _backfill_range(
                            rest=rest,
                            storage=storage,
                            exchange_id=int(exchange_id),
                            symbol=symbol,
                            symbol_id=int(symbol_id),
                            interval=str(itv),
                            dt_from=dt_from,
                            dt_to=dt_to,
                            limit=limit,
                            sleep_sec=sleep_sec,
                            stop_event=stop_event,
                            max_span_days=max_backfill_span_days,
                            step_by_interval=step_by_interval,
                        )
                        upserts_total += int(n)
                        gaps_fixed += 1
                        log.info("[GapRepair] ok %s %s upserts=%d gaps_fixed=%d", symbol, itv, n, gaps_fixed)
                    except Exception:
                        log.exception("[GapRepair] backfill failed %s %s", symbol, itv)

        dt = time.time() - t0
        log.info("[GapRepair] cycle done gaps_fixed=%d upserts_total=%d dt=%.1fs", gaps_fixed, upserts_total, dt)

        sleep = max(1.0, float(every_sec) - dt)
        if stop_event.wait(sleep):
            break


def start_candles_gap_repair_collector(
    *,
    cfg: dict,
    rest,
    storage,
    exchange_id: int,
    stop_event: threading.Event,
) -> threading.Thread:
    t = threading.Thread(
        target=run_candles_gap_repair,
        kwargs=dict(cfg=cfg, rest=rest, storage=storage, exchange_id=int(exchange_id), stop_event=stop_event),
        daemon=True,
        name=f"BinanceCandlesGapRepair-{exchange_id}",
    )
    t.start()
    log.info("[GapRepair] thread started")
    return t
