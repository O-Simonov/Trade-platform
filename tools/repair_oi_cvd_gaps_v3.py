from __future__ import annotations

import argparse
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

# make `src` importable when launched as: python tools/repair_oi_cvd_gaps_v3.py
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage
from src.platform.exchanges.binance.rest import BinanceFuturesREST


@dataclass(frozen=True)
class MissingPoint:
    symbol_id: int
    symbol: str
    interval: str
    ts: datetime


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
        oi = _safe_float(it.get("sumOpenInterest") or it.get("openInterest"))
        oiv = _safe_float(it.get("sumOpenInterestValue") or it.get("openInterestValue"))
        if oi is None or oiv is None:
            continue
        rows.append(
            {
                "exchange_id": int(exchange_id),
                "symbol_id": int(symbol_id),
                "interval": str(interval),
                "ts": _dt_from_ms(ts_ms),
                "open_interest": oi,
                "open_interest_value": oiv,
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


def _find_missing_oi_points(
    storage: PostgreSQLStorage,
    *,
    exchange_id: int,
    symbol_id: int,
    symbol: str,
    interval: str,
    from_ts: datetime,
) -> list[MissingPoint]:
    sql = """
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
    """
    out: list[MissingPoint] = []
    with storage.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (int(exchange_id), int(symbol_id), str(interval), from_ts))
            for (ts,) in (cur.fetchall() or []):
                out.append(MissingPoint(symbol_id=int(symbol_id), symbol=str(symbol), interval=str(interval), ts=ts))
    return out


def _fetch_oi_hist_window(
    *,
    rest: BinanceFuturesREST,
    exchange_id: int,
    symbol_id: int,
    symbol: str,
    interval: str,
    start_ts: datetime,
    end_ts: datetime,
    limit: int,
) -> list[dict]:
    payload = rest.open_interest_hist(
        symbol=symbol,
        period=interval,
        limit=max(1, min(int(limit), 500)),
        start_time_ms=_dt_to_ms(start_ts),
        end_time_ms=_dt_to_ms(end_ts),
    )
    return _parse_oi_hist_payload(
        payload=payload,
        exchange_id=exchange_id,
        symbol_id=symbol_id,
        interval=interval,
        source="gap_repair_v3",
    )


def _load_existing_oi_ts(storage: PostgreSQLStorage, *, exchange_id: int, symbol_id: int, interval: str, from_ts: datetime) -> set[datetime]:
    sql = """
    SELECT ts
    FROM open_interest
    WHERE exchange_id=%s
      AND symbol_id=%s
      AND interval=%s
      AND ts >= %s
    """
    with storage.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (int(exchange_id), int(symbol_id), str(interval), from_ts))
            return {r[0] for r in (cur.fetchall() or [])}


def _upsert_oi_exact_missing(
    *,
    rest: BinanceFuturesREST,
    storage: PostgreSQLStorage,
    exchange_id: int,
    symbol_id: int,
    symbol: str,
    interval: str,
    missing_points: list[MissingPoint],
    oi_limit: int,
    sleep_sec: float,
    dry_run: bool,
) -> tuple[int, int, int]:
    """
    Returns (filled_exact, api_rows_seen, still_missing).
    """
    if not missing_points:
        return 0, 0, 0

    step = _interval_to_timedelta(interval)
    wanted = {p.ts for p in missing_points}
    windows: dict[tuple[datetime, datetime], list[datetime]] = defaultdict(list)

    # group timestamps into compact API windows using exact missing candle timestamps
    current_group: list[datetime] = [missing_points[0].ts]
    for p in missing_points[1:]:
        prev = current_group[-1]
        if p.ts - prev <= step * 20:
            current_group.append(p.ts)
        else:
            start = current_group[0] - step * 2
            end = current_group[-1] + step * 2
            windows[(start, end)] = current_group.copy()
            current_group = [p.ts]
    if current_group:
        start = current_group[0] - step * 2
        end = current_group[-1] + step * 2
        windows[(start, end)] = current_group.copy()

    fetched_by_ts: dict[datetime, dict] = {}
    api_rows_seen = 0
    for (start, end), group_ts in windows.items():
        rows = _fetch_oi_hist_window(
            rest=rest,
            exchange_id=exchange_id,
            symbol_id=symbol_id,
            symbol=symbol,
            interval=interval,
            start_ts=start,
            end_ts=end,
            limit=oi_limit,
        )
        api_rows_seen += len(rows)
        for row in rows:
            ts = row["ts"]
            if ts in wanted:
                fetched_by_ts[ts] = row
        if sleep_sec > 0:
            time.sleep(float(sleep_sec))

    exact_rows = [fetched_by_ts[ts] for ts in sorted(fetched_by_ts.keys())]
    if exact_rows and not dry_run:
        storage.upsert_open_interest(exact_rows)

    filled_exact = len(exact_rows)
    still_missing = len(wanted - set(fetched_by_ts.keys()))
    return filled_exact, api_rows_seen, still_missing


def _find_cvd_null_ranges(
    storage: PostgreSQLStorage,
    *,
    exchange_id: int,
    symbol_id: int,
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


def _recalc_cvd_range(storage: PostgreSQLStorage, *, exchange_id: int, symbol_id: int, interval: str, start_ts: datetime, end_ts: datetime) -> int:
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
    p = argparse.ArgumentParser(description="Repair missing OI rows exactly by missing candle timestamps and recalc missing CVD.")
    p.add_argument("--exchange-id", type=int, default=int(os.environ.get("EXCHANGE_ID", "1")))
    p.add_argument("--intervals", default=os.environ.get("INTERVALS", "5m,15m,1h"))
    p.add_argument("--lookback-hours", type=int, default=int(os.environ.get("LOOKBACK_HOURS", "72")))
    p.add_argument("--symbols", default=os.environ.get("SYMBOLS", ""), help="Comma-separated symbols. Empty = all active symbols")
    p.add_argument("--only-active", action="store_true", default=os.environ.get("ONLY_ACTIVE", "1").strip().lower() not in ("0", "false", "no"))
    p.add_argument("--oi-only", action="store_true", help="Repair only OI")
    p.add_argument("--cvd-only", action="store_true", help="Repair only CVD")
    p.add_argument("--oi-limit", type=int, default=int(os.environ.get("OI_LIMIT", "500")))
    p.add_argument("--sleep-sec", type=float, default=float(os.environ.get("SLEEP_SEC", "0.15")))
    p.add_argument("--dry-run", action="store_true", help="Show what would be repaired without writing to DB")
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
        print("[REPAIR_OI_CVD_V3] no symbols found")
        return

    do_oi = not args.cvd_only
    do_cvd = not args.oi_only

    total_filled_exact = 0
    total_api_rows_seen = 0
    total_still_missing = 0
    total_missing_points = 0
    total_cvd_updates = 0
    total_cvd_ranges = 0
    total_unrecoverable = 0

    print(
        f"[REPAIR_OI_CVD_V3] start exchange_id={args.exchange_id} intervals={intervals} "
        f"lookback_hours={args.lookback_hours} symbols={len(symbols)} do_oi={do_oi} do_cvd={do_cvd} dry_run={args.dry_run}"
    )

    for symbol_id, symbol in symbols:
        for interval in intervals:
            if do_oi:
                missing_points = _find_missing_oi_points(
                    storage,
                    exchange_id=args.exchange_id,
                    symbol_id=symbol_id,
                    symbol=symbol,
                    interval=interval,
                    from_ts=from_ts,
                )
                if missing_points:
                    total_missing_points += len(missing_points)
                    print(
                        f"[REPAIR_OI_V3] {symbol} {interval} exact_missing={len(missing_points)} "
                        f"first={missing_points[0].ts.isoformat()} last={missing_points[-1].ts.isoformat()}"
                    )
                    try:
                        filled_exact, api_rows_seen, still_missing = _upsert_oi_exact_missing(
                            rest=rest,
                            storage=storage,
                            exchange_id=args.exchange_id,
                            symbol_id=symbol_id,
                            symbol=symbol,
                            interval=interval,
                            missing_points=missing_points,
                            oi_limit=args.oi_limit,
                            sleep_sec=args.sleep_sec,
                            dry_run=args.dry_run,
                        )
                        total_filled_exact += filled_exact
                        total_api_rows_seen += api_rows_seen
                        total_still_missing += still_missing
                        print(
                            f"[REPAIR_OI_V3] ok {symbol} {interval} filled_exact={filled_exact} "
                            f"api_rows_seen={api_rows_seen} still_missing={still_missing}"
                        )
                    except Exception as e:
                        print(f"[REPAIR_OI_V3] fail {symbol} {interval}: {e}")

            if do_cvd:
                cvd_ranges = _find_cvd_null_ranges(
                    storage,
                    exchange_id=args.exchange_id,
                    symbol_id=symbol_id,
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
                        if args.dry_run:
                            n = cnt
                        else:
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
        f"[REPAIR_OI_CVD_V3] done missing_points={total_missing_points} filled_exact={total_filled_exact} "
        f"api_rows_seen={total_api_rows_seen} still_missing={total_still_missing} "
        f"cvd_ranges={total_cvd_ranges} cvd_updated={total_cvd_updates} unrecoverable_cvd_nulls={total_unrecoverable}"
    )


if __name__ == "__main__":
    main()
