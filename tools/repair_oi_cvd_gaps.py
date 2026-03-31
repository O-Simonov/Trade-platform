from __future__ import annotations

import sys
from pathlib import Path

# Allow direct запуск as: python tools/repair_oi_cvd_gaps.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import argparse
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage
from src.platform.exchanges.binance.rest import BinanceFuturesREST


@dataclass
class MissingOIRange:
    symbol_id: int
    symbol: str
    interval: str
    start_ts: datetime
    end_ts: datetime
    missing_points: int


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _interval_to_timedelta(interval: str) -> timedelta:
    s = str(interval).strip().lower()
    if s.endswith("m"):
        return timedelta(minutes=int(s[:-1]))
    if s.endswith("h"):
        return timedelta(hours=int(s[:-1]))
    if s.endswith("d"):
        return timedelta(days=int(s[:-1]))
    raise ValueError(f"Unsupported interval: {interval}")


def _dt_to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _dt_from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _parse_oi_hist_payload(*, payload: Iterable[dict], exchange_id: int, symbol_id: int, interval: str, source: str) -> list[dict]:
    rows: list[dict] = []
    for it in (payload or []):
        if not isinstance(it, dict):
            continue
        ts_ms = int(it.get("timestamp") or it.get("time") or 0)
        if not ts_ms:
            continue
        rows.append(
            {
                "exchange_id": int(exchange_id),
                "symbol_id": int(symbol_id),
                "interval": str(interval),
                "ts": _dt_from_ms(ts_ms),
                "open_interest": _safe_float(it.get("sumOpenInterest") or it.get("openInterest")),
                "open_interest_value": _safe_float(it.get("sumOpenInterestValue") or it.get("openInterestValue")),
                "source": str(source),
            }
        )
    rows.sort(key=lambda x: x["ts"])
    return rows


def _load_symbols(storage: PostgreSQLStorage, exchange_id: int, only_active: bool, symbols_filter: Optional[set[str]]) -> list[tuple[int, str]]:
    where = "WHERE exchange_id=%s"
    args: list[Any] = [int(exchange_id)]
    if only_active:
        where += " AND is_active=true"
    if symbols_filter:
        where += " AND symbol = ANY(%s)"
        args.append(sorted(symbols_filter))
    sql = f"SELECT symbol_id, symbol FROM symbols {where} ORDER BY symbol_id"
    with storage.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            rows = cur.fetchall() or []
    return [(int(r[0]), str(r[1]).upper()) for r in rows]


def _find_missing_oi_ranges(
    storage: PostgreSQLStorage,
    *,
    exchange_id: int,
    symbol_id: int,
    symbol: str,
    interval: str,
    from_ts: datetime,
) -> list[MissingOIRange]:
    sql = """
    WITH missing AS (
        SELECT c.open_time AS ts
        FROM candles c
        LEFT JOIN open_interest oi
          ON oi.exchange_id = c.exchange_id
         AND oi.symbol_id = c.symbol_id
         AND oi.interval = c.interval
         AND oi.ts = c.open_time
        WHERE c.exchange_id = %s
          AND c.symbol_id = %s
          AND c.interval = %s
          AND c.open_time >= %s
          AND oi.ts IS NULL
        ORDER BY c.open_time
    ), marked AS (
        SELECT
            ts,
            CASE
                WHEN LAG(ts) OVER (ORDER BY ts) IS NULL THEN 1
                WHEN ts - LAG(ts) OVER (ORDER BY ts) > %s::interval THEN 1
                ELSE 0
            END AS grp_start
        FROM missing
    ), grouped AS (
        SELECT
            ts,
            SUM(grp_start) OVER (ORDER BY ts) AS grp
        FROM marked
    )
    SELECT MIN(ts) AS start_ts,
           MAX(ts) AS end_ts,
           COUNT(*) AS missing_points
    FROM grouped
    GROUP BY grp
    ORDER BY start_ts
    """
    step = str(_interval_to_timedelta(interval))
    out: list[MissingOIRange] = []
    with storage.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (int(exchange_id), int(symbol_id), str(interval), from_ts, step))
            for start_ts, end_ts, cnt in (cur.fetchall() or []):
                out.append(
                    MissingOIRange(
                        symbol_id=int(symbol_id),
                        symbol=str(symbol),
                        interval=str(interval),
                        start_ts=start_ts,
                        end_ts=end_ts,
                        missing_points=int(cnt),
                    )
                )
    return out


def _backfill_oi_range(
    *,
    rest: BinanceFuturesREST,
    storage: PostgreSQLStorage,
    exchange_id: int,
    symbol_id: int,
    symbol: str,
    interval: str,
    start_ts: datetime,
    end_ts: datetime,
    limit: int,
    sleep_sec: float,
) -> int:
    step = _interval_to_timedelta(interval)
    # Add one extra step to the right boundary to allow endpoint to return the closing point.
    start_ms = _dt_to_ms(start_ts)
    end_ms = _dt_to_ms(end_ts + step)

    payload = rest.open_interest_hist(
        symbol=symbol,
        period=interval,
        limit=max(1, min(int(limit), 500)),
        start_time_ms=start_ms,
        end_time_ms=end_ms,
    )
    rows = _parse_oi_hist_payload(
        payload=payload,
        exchange_id=exchange_id,
        symbol_id=symbol_id,
        interval=interval,
        source="gap_repair",
    )
    # Keep only timestamps that actually belong to the requested candle points.
    allowed_end = end_ts
    rows = [r for r in rows if start_ts <= r["ts"] <= allowed_end]
    if not rows:
        return 0
    n = int(storage.upsert_open_interest(rows) or 0)
    if sleep_sec > 0:
        time.sleep(float(sleep_sec))
    return n


def _find_cvd_null_ranges(
    storage: PostgreSQLStorage,
    *,
    exchange_id: int,
    symbol_id: int,
    symbol: str,
    interval: str,
    from_ts: datetime,
) -> list[tuple[datetime, datetime, int]]:
    sql = """
    WITH missing AS (
        SELECT open_time AS ts
        FROM candles
        WHERE exchange_id = %s
          AND symbol_id = %s
          AND interval = %s
          AND open_time >= %s
          AND cvd_quote IS NULL
          AND delta_quote IS NOT NULL
        ORDER BY open_time
    ), marked AS (
        SELECT
            ts,
            CASE
                WHEN LAG(ts) OVER (ORDER BY ts) IS NULL THEN 1
                WHEN ts - LAG(ts) OVER (ORDER BY ts) > %s::interval THEN 1
                ELSE 0
            END AS grp_start
        FROM missing
    ), grouped AS (
        SELECT ts, SUM(grp_start) OVER (ORDER BY ts) AS grp
        FROM marked
    )
    SELECT MIN(ts) AS start_ts,
           MAX(ts) AS end_ts,
           COUNT(*) AS cnt
    FROM grouped
    GROUP BY grp
    ORDER BY start_ts
    """
    step = str(_interval_to_timedelta(interval))
    with storage.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (int(exchange_id), int(symbol_id), str(interval), from_ts, step))
            return [(a, b, int(c)) for a, b, c in (cur.fetchall() or [])]


def _recalc_cvd_range(
    storage: PostgreSQLStorage,
    *,
    exchange_id: int,
    symbol_id: int,
    interval: str,
    start_ts: datetime,
    end_ts: datetime,
) -> int:
    """
    Recalculate CVD on candles using delta_quote.

    Base CVD is taken from the latest candle before the range with non-null cvd_quote.
    If there is no previous CVD, base = 0.
    """
    sql = """
    WITH base AS (
        SELECT COALESCE((
            SELECT c_prev.cvd_quote
            FROM candles c_prev
            WHERE c_prev.exchange_id = %(exchange_id)s
              AND c_prev.symbol_id = %(symbol_id)s
              AND c_prev.interval = %(interval)s
              AND c_prev.open_time < %(start_ts)s
              AND c_prev.cvd_quote IS NOT NULL
            ORDER BY c_prev.open_time DESC
            LIMIT 1
        ), 0) AS base_cvd
    ), w AS (
        SELECT
            c.exchange_id,
            c.symbol_id,
            c.interval,
            c.open_time,
            (SELECT base_cvd FROM base)
              + SUM(COALESCE(c.delta_quote, 0)) OVER (
                    PARTITION BY c.exchange_id, c.symbol_id, c.interval
                    ORDER BY c.open_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cvd_new
        FROM candles c
        WHERE c.exchange_id = %(exchange_id)s
          AND c.symbol_id = %(symbol_id)s
          AND c.interval = %(interval)s
          AND c.open_time >= %(start_ts)s
          AND c.open_time <= %(end_ts)s
    )
    UPDATE candles t
    SET cvd_quote = w.cvd_new,
        updated_at = now()
    FROM w
    WHERE t.exchange_id = w.exchange_id
      AND t.symbol_id = w.symbol_id
      AND t.interval = w.interval
      AND t.open_time = w.open_time
    """
    with storage.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    "exchange_id": int(exchange_id),
                    "symbol_id": int(symbol_id),
                    "interval": str(interval),
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                },
            )
            n = cur.rowcount
        conn.commit()
    return int(n or 0)


def _count_cvd_unrecoverable(storage: PostgreSQLStorage, *, exchange_id: int, symbol_id: int, interval: str, from_ts: datetime) -> int:
    sql = """
    SELECT COUNT(*)
    FROM candles
    WHERE exchange_id = %s
      AND symbol_id = %s
      AND interval = %s
      AND open_time >= %s
      AND cvd_quote IS NULL
      AND delta_quote IS NULL
    """
    with storage.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (int(exchange_id), int(symbol_id), str(interval), from_ts))
            return int((cur.fetchone() or [0])[0] or 0)


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Repair missing OI rows and recalculate missing CVD on candles.")
    p.add_argument("--exchange-id", type=int, default=int(os.environ.get("EXCHANGE_ID", "1")))
    p.add_argument("--intervals", default=os.environ.get("INTERVALS", "5m,15m,1h"))
    p.add_argument("--lookback-hours", type=int, default=int(os.environ.get("LOOKBACK_HOURS", "72")))
    p.add_argument("--symbols", default=os.environ.get("SYMBOLS", ""), help="Comma-separated symbols. Empty = all active symbols")
    p.add_argument("--only-active", action="store_true", default=os.environ.get("ONLY_ACTIVE", "1").strip().lower() not in ("0", "false", "no"))
    p.add_argument("--oi-only", action="store_true", help="Repair only OI")
    p.add_argument("--cvd-only", action="store_true", help="Repair only CVD")
    p.add_argument("--oi-limit", type=int, default=int(os.environ.get("OI_LIMIT", "500")))
    p.add_argument("--sleep-sec", type=float, default=float(os.environ.get("SLEEP_SEC", "0.15")))
    return p


def main() -> None:
    args = build_arg_parser().parse_args()
    dsn = (os.environ.get("POSTGRES_DSN") or os.environ.get("DATABASE_URL") or "").strip()
    if not dsn:
        raise SystemExit("ERROR: POSTGRES_DSN or DATABASE_URL is required")

    intervals = [x.strip() for x in str(args.intervals).split(",") if x.strip()]
    symbols_filter = {s.strip().upper() for s in str(args.symbols).split(",") if s.strip()}
    from_ts = _utcnow() - timedelta(hours=int(args.lookback_hours))

    pool = create_pool(dsn)
    storage = PostgreSQLStorage(pool)
    rest = BinanceFuturesREST(api_key=None, secret_key=None)

    symbols = _load_symbols(storage, args.exchange_id, bool(args.only_active), symbols_filter or None)
    if not symbols:
        print("[REPAIR_OI_CVD] no symbols found")
        return

    do_oi = not args.cvd_only
    do_cvd = not args.oi_only

    total_oi_upserts = 0
    total_oi_ranges = 0
    total_cvd_updates = 0
    total_cvd_ranges = 0
    total_unrecoverable = 0

    print(
        f"[REPAIR_OI_CVD] start exchange_id={args.exchange_id} intervals={intervals} "
        f"lookback_hours={args.lookback_hours} symbols={len(symbols)} do_oi={do_oi} do_cvd={do_cvd}"
    )

    for symbol_id, symbol in symbols:
        for interval in intervals:
            if do_oi:
                oi_ranges = _find_missing_oi_ranges(
                    storage,
                    exchange_id=args.exchange_id,
                    symbol_id=symbol_id,
                    symbol=symbol,
                    interval=interval,
                    from_ts=from_ts,
                )
                for rng in oi_ranges:
                    print(
                        f"[REPAIR_OI] {symbol} {interval} missing_points={rng.missing_points} "
                        f"range={rng.start_ts.isoformat()} -> {rng.end_ts.isoformat()}"
                    )
                    try:
                        n = _backfill_oi_range(
                            rest=rest,
                            storage=storage,
                            exchange_id=args.exchange_id,
                            symbol_id=symbol_id,
                            symbol=symbol,
                            interval=interval,
                            start_ts=rng.start_ts,
                            end_ts=rng.end_ts,
                            limit=args.oi_limit,
                            sleep_sec=args.sleep_sec,
                        )
                        total_oi_upserts += n
                        total_oi_ranges += 1
                        print(f"[REPAIR_OI] ok {symbol} {interval} upserts={n}")
                    except Exception as e:
                        print(f"[REPAIR_OI] fail {symbol} {interval}: {e}")

            if do_cvd:
                cvd_ranges = _find_cvd_null_ranges(
                    storage,
                    exchange_id=args.exchange_id,
                    symbol_id=symbol_id,
                    symbol=symbol,
                    interval=interval,
                    from_ts=from_ts,
                )
                for start_ts, end_ts, cnt in cvd_ranges:
                    step = _interval_to_timedelta(interval)
                    recalc_start = start_ts - step
                    print(
                        f"[REPAIR_CVD] {symbol} {interval} null_rows={cnt} "
                        f"range={start_ts.isoformat()} -> {end_ts.isoformat()}"
                    )
                    try:
                        n = _recalc_cvd_range(
                            storage,
                            exchange_id=args.exchange_id,
                            symbol_id=symbol_id,
                            interval=interval,
                            start_ts=recalc_start,
                            end_ts=end_ts,
                        )
                        total_cvd_updates += n
                        total_cvd_ranges += 1
                        print(f"[REPAIR_CVD] ok {symbol} {interval} updated_rows={n}")
                    except Exception as e:
                        print(f"[REPAIR_CVD] fail {symbol} {interval}: {e}")

                unrecoverable = _count_cvd_unrecoverable(
                    storage,
                    exchange_id=args.exchange_id,
                    symbol_id=symbol_id,
                    interval=interval,
                    from_ts=from_ts,
                )
                if unrecoverable > 0:
                    total_unrecoverable += unrecoverable
                    print(
                        f"[REPAIR_CVD] warn {symbol} {interval} unrecoverable_nulls={unrecoverable} "
                        f"(delta_quote is NULL; need taker/trades backfill)"
                    )

    print(
        f"[REPAIR_OI_CVD] done oi_ranges={total_oi_ranges} oi_upserts={total_oi_upserts} "
        f"cvd_ranges={total_cvd_ranges} cvd_updated={total_cvd_updates} unrecoverable_cvd_nulls={total_unrecoverable}"
    )


if __name__ == "__main__":
    main()
