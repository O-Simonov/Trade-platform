# src/platform/tools/gap_repair_candles_now.py
from __future__ import annotations

import os
import argparse
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any,List, Optional, Sequence, Tuple

from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage

from src.platform.exchanges.binance.rest import BinanceFuturesREST
from src.platform.exchanges.binance.collector_candles import _parse_klines_payload


# =========================================================
# Helpers
# =========================================================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _itv_to_pg_interval_str(itv: str) -> str:
    s = str(itv).strip().lower()
    if s.endswith("m"):
        n = int(s[:-1])
        return f"{n} minutes"
    if s.endswith("h"):
        n = int(s[:-1])
        return f"{n} hours"
    if s.endswith("d"):
        n = int(s[:-1])
        return f"{n} days"
    raise ValueError(f"Unsupported interval: {itv}")


def _itv_to_timedelta(itv: str) -> timedelta:
    s = str(itv).strip().lower()
    if s.endswith("m"):
        return timedelta(minutes=int(s[:-1]))
    if s.endswith("h"):
        return timedelta(hours=int(s[:-1]))
    if s.endswith("d"):
        return timedelta(days=int(s[:-1]))
    raise ValueError(f"Unsupported interval: {itv}")


def _parse_csv_list(s: str) -> list[str]:
    out: list[str] = []
    for x in (s or "").split(","):
        x = x.strip()
        if x:
            out.append(x)
    return out


def _fmt_dt(x: Any) -> str:
    if isinstance(x, datetime):
        return x.strftime("%Y-%m-%d %H:%M:%S")
    return str(x)


def _fmt_gap_seconds(sec: float) -> str:
    sec = float(sec or 0)
    if sec < 60:
        return f"{sec:.0f}s"
    if sec < 3600:
        return f"{sec/60:.1f}m"
    if sec < 86400:
        return f"{sec/3600:.2f}h"
    return f"{sec/86400:.2f}d"


@dataclass
class GapRow:
    symbol_id: int
    symbol: str
    interval: str
    prev_open: datetime
    next_open: datetime
    from_ts: datetime
    to_ts: datetime
    gap_seconds: float
    missed_bars: int


# =========================================================
# DB: symbols + gaps
# =========================================================

def _load_symbols(storage: PostgreSQLStorage, *, exchange_id: int, only_active: bool, only_symbols: Sequence[str]) -> List[Tuple[int, str]]:
    only_symbols_u = {str(x).upper().strip() for x in (only_symbols or []) if str(x).strip()}

    where = "WHERE exchange_id=%s"
    args: list[Any] = [int(exchange_id)]

    if only_active:
        where += " AND is_active=true"

    if only_symbols_u:
        where += " AND UPPER(symbol) = ANY(%s)"
        args.append(list(sorted(only_symbols_u)))

    sql = f"SELECT symbol_id, symbol FROM symbols {where} ORDER BY symbol_id"
    with storage.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            rows = cur.fetchall() or []
    return [(int(r[0]), str(r[1]).upper()) for r in rows]


def _find_gaps(
    storage: PostgreSQLStorage,
    *,
    exchange_id: int,
    symbol_id: int,
    interval: str,
    limit_gaps: int,
    min_gap_points: int,
    max_gap_days: int,
) -> List[GapRow]:
    """
    Находим дыры:
      open_time > prev + interval

    Возвращаем:
      from_ts = prev + interval
      to_ts   = open_time (первая существующая свеча после дыры)
    """
    interval = str(interval).strip()
    itv_sql = _itv_to_pg_interval_str(interval)
    min_gap_points = max(1, int(min_gap_points))
    limit_gaps = max(1, int(limit_gaps))
    max_gap_days = max(0, int(max_gap_days))

    mult = int(min_gap_points) + 1  # gap >= (min_gap_points+1)*interval

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
      open_time               AS to_ts,
      prev                    AS prev_open,
      open_time               AS next_open,
      EXTRACT(EPOCH FROM gap) AS gap_seconds,
      GREATEST(
        0,
        CAST(FLOOR(EXTRACT(EPOCH FROM gap) / EXTRACT(EPOCH FROM (%s)::interval)) AS int) - 1
      ) AS missed_bars
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
        itv_sql,
        int(mult),
    ]

    if max_gap_days > 0:
        sql += " AND open_time >= NOW() - (%s)::interval\n"
        args.append(f"{int(max_gap_days)} days")

    sql += """
    ORDER BY gap_seconds DESC
    LIMIT %s;
    """
    args.append(int(limit_gaps))

    out: List[GapRow] = []
    with storage.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            rows = cur.fetchall() or []

    for r in rows:
        out.append(
            GapRow(
                symbol_id=int(symbol_id),
                symbol="",  # заполним позже
                interval=str(interval),
                prev_open=r[2],
                next_open=r[3],
                from_ts=r[0],
                to_ts=r[1],
                gap_seconds=float(r[4] or 0),
                missed_bars=int(r[5] or 0),
            )
        )
    return out


# =========================================================
# REST: backfill
# =========================================================

def _rest_public() -> BinanceFuturesREST:
    # публичные эндпоинты без ключей
    return BinanceFuturesREST("", "")


def _backfill_gap(
    *,
    rest: BinanceFuturesREST,
    storage: PostgreSQLStorage,
    exchange_id: int,
    symbol: str,
    symbol_id: int,
    interval: str,
    dt_from: datetime,
    dt_to: datetime,
    limit: int,
    sleep_sec: float,
    max_span_days: int,
) -> int:
    """
    Заполняем свечи в диапазоне [dt_from, dt_to)
    dt_to — open_time существующей свечи, её НЕ трогаем.
    """
    interval = str(interval).strip()
    itv_td = _itv_to_timedelta(interval)

    limit = max(1, min(int(limit), 1500))
    sleep_sec = max(0.0, float(sleep_sec))
    max_span_days = max(0, int(max_span_days))

    if max_span_days > 0:
        span_limit = dt_to - timedelta(days=max_span_days)
        if dt_from < span_limit:
            dt_from = span_limit

    # Binance endTime inclusive -> делаем dt_to - 1ms
    dt_to_exclusive = dt_to - timedelta(milliseconds=1)
    end_ms = _to_ms(dt_to_exclusive)

    total = 0
    cur = dt_from
    last_progress: Optional[datetime] = None
    safety_iters = 0

    while cur < dt_to:
        safety_iters += 1
        if safety_iters > 20000:
            print(f"[REPAIR] safety break: too many iters {symbol} {interval}")
            break

        payload = rest.klines(
            symbol=str(symbol),
            interval=str(interval),
            start_time_ms=_to_ms(cur),
            end_time_ms=end_ms,
            limit=limit,
        )

        rows = _parse_klines_payload(
            exchange_id=int(exchange_id),
            symbol_id=int(symbol_id),
            interval=str(interval),
            payload=payload or [],
            source="gap_repair_now",
        )

        if not rows:
            break

        now = _utcnow()
        for r in rows:
            r["source"] = "gap_repair_now"
            r["updated_at"] = now

        n = int(storage.upsert_candles(rows) or 0)
        total += n

        last_open = rows[-1].get("open_time")
        if not isinstance(last_open, datetime):
            break

        if last_progress is not None and last_open <= last_progress:
            print(f"[REPAIR] no progress {symbol} {interval} last_open={last_open} prev={last_progress}")
            break
        last_progress = last_open

        # следующий бар
        nxt = last_open + itv_td
        if nxt <= cur:
            nxt = last_open + timedelta(milliseconds=1)
        cur = nxt

        # меньше лимита -> дальше нет
        if len(rows) < limit:
            break

        if sleep_sec > 0:
            time.sleep(sleep_sec)

        # guard
        if cur > dt_to - itv_td:
            break

    return total


# =========================================================
# CLI main
# =========================================================

def main() -> None:
    ap = argparse.ArgumentParser(
        prog="gap_repair_candles_now",
        description="Find TOP gaps in candles and repair them immediately via REST klines()",
    )
    ap.add_argument("--exchange-id", type=int, default=1)
    ap.add_argument("--intervals", type=str, default="4h", help='Comma list: "5m,15m,1h,4h"')
    ap.add_argument("--only-active", action="store_true", help="Repair only active symbols from symbols table")
    ap.add_argument("--symbols", type=str, default="", help='Optional comma list: "BTCUSDT,ETHUSDT"')

    ap.add_argument("--top", type=int, default=50, help="Max gaps to fix TOTAL (global limit)")
    ap.add_argument("--gaps-per-symbol", type=int, default=2, help="How many gaps to take per symbol per interval")
    ap.add_argument("--max-symbols", type=int, default=60, help="How many symbols to scan in one run (0 = all)")

    ap.add_argument("--min-gap-points", type=int, default=1, help="Min missed bars to count as gap (>=1)")
    ap.add_argument("--max-gap-days", type=int, default=60, help="Search gaps only in last N days (0 = no filter)")

    ap.add_argument("--limit", type=int, default=1500, help="REST klines limit (<=1500)")
    ap.add_argument("--sleep-sec", type=float, default=0.25, help="Sleep between REST pages")
    ap.add_argument("--max-span-days", type=int, default=0, help="Limit how far back to backfill inside a gap (0 = no limit)")

    ap.add_argument("--dry-run", action="store_true", help="Only print gaps, do not write to DB")
    args = ap.parse_args()

    dsn = os.environ.get("PG_DSN") or os.environ.get("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("PG_DSN (or POSTGRES_DSN) env is required")

    pool = create_pool(dsn)
    storage = PostgreSQLStorage(pool=pool)

    try:
        rest = _rest_public()

        intervals = _parse_csv_list(args.intervals) or ["4h"]
        only_symbols = [x.upper() for x in _parse_csv_list(args.symbols)]

        symbols = _load_symbols(
            storage,
            exchange_id=int(args.exchange_id),
            only_active=bool(args.only_active),
            only_symbols=only_symbols,
        )

        if not symbols:
            print("❌ symbols list empty (check symbols table / only-active / --symbols)")
            return

        if int(args.max_symbols) > 0:
            symbols = symbols[: int(args.max_symbols)]

        print("")
        print("=" * 120)
        print(
            f"[GAP REPAIR NOW] exchange_id={args.exchange_id} intervals={intervals} "
            f"symbols={len(symbols)} only_active={args.only_active} "
            f"max_gap_days={args.max_gap_days} min_gap_points={args.min_gap_points} "
            f"top_total={args.top} gaps_per_symbol={args.gaps_per_symbol} dry_run={args.dry_run}"
        )
        print("=" * 120)

        fixed = 0
        upserts_total = 0
        scanned_gaps = 0

        t0 = time.time()

        for itv in intervals:
            if fixed >= int(args.top):
                break

            print("")
            print(f"--- interval={itv} ---")

            for symbol_id, symbol in symbols:
                if fixed >= int(args.top):
                    break

                gaps = _find_gaps(
                    storage,
                    exchange_id=int(args.exchange_id),
                    symbol_id=int(symbol_id),
                    interval=str(itv),
                    limit_gaps=int(args.gaps_per_symbol),
                    min_gap_points=int(args.min_gap_points),
                    max_gap_days=int(args.max_gap_days),
                )

                if not gaps:
                    continue

                # заполняем symbol для печати
                for g in gaps:
                    g.symbol = symbol

                for g in gaps:
                    if fixed >= int(args.top):
                        break

                    scanned_gaps += 1
                    print(
                        f"[GAP] {symbol:<12} {itv:<4} missed={g.missed_bars:<4} gap={_fmt_gap_seconds(g.gap_seconds):>8}  "
                        f"FROM {_fmt_dt(g.from_ts)}  -> TO {_fmt_dt(g.to_ts)}"
                    )

                    if args.dry_run:
                        fixed += 1
                        continue

                    try:
                        n = _backfill_gap(
                            rest=rest,
                            storage=storage,
                            exchange_id=int(args.exchange_id),
                            symbol=symbol,
                            symbol_id=int(symbol_id),
                            interval=str(itv),
                            dt_from=g.from_ts,
                            dt_to=g.to_ts,
                            limit=int(args.limit),
                            sleep_sec=float(args.sleep_sec),
                            max_span_days=int(args.max_span_days),
                        )
                        upserts_total += int(n)
                        fixed += 1
                        print(f"      ✅ repaired upserts={n}")
                    except Exception as e:
                        print(f"      ❌ repair failed: {e}")

        dt = time.time() - t0
        print("")
        print("=" * 120)
        print(f"[DONE] gaps_seen={scanned_gaps} gaps_processed={fixed} upserts_total={upserts_total} dt={dt:.1f}s")
        print("=" * 120)

    finally:
        try:
            pool.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
