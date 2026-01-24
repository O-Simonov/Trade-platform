# src/platform/tools/gap_check_candles.py
from __future__ import annotations

import os
import argparse
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage


# =========================================================
# Helpers
# =========================================================

def _itv_to_pg_interval_str(itv: str) -> str:
    """
    Превращаем "5m"/"1h"/"4h"/"1d" в строку PostgreSQL interval.
    """
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


def _parse_intervals(s: str) -> list[str]:
    out: list[str] = []
    for x in (s or "").split(","):
        x = x.strip()
        if x:
            out.append(x)
    return out


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
# SQL
# =========================================================

def _fetch_gaps_top(
    storage: PostgreSQLStorage,
    *,
    exchange_id: int,
    interval: str,
    top: int,
    only_active: bool,
    min_gap_points: int,
    max_gap_days: int,
    per_symbol: int,
) -> list[GapRow]:
    """
    Если per_symbol > 0 -> берём TOP дырок на каждый symbol_id (через row_number).
    Если per_symbol == 0 -> берём общий TOP (по всем символам).
    """
    itv_sql = _itv_to_pg_interval_str(interval)
    top = max(1, int(top))
    min_gap_points = max(1, int(min_gap_points))

    # gap >= (min_gap_points+1)*interval
    mult = min_gap_points + 1

    # symbol filter
    sym_where = "s.exchange_id = %s"
    if only_active:
        sym_where += " AND s.is_active = true"

    extra_time_filter = ""
    args_extra: list[Any] = []
    if int(max_gap_days) > 0:
        extra_time_filter = " AND t.next_open >= NOW() - (%s)::interval "
        args_extra.append(f"{int(max_gap_days)} days")

    # ВАЖНО: считаем missed_bars как FLOOR(gap/itv)-1
    # Например gap=3*itv => missed=2
    base_sql = f"""
    WITH base AS (
      SELECT
        c.symbol_id,
        c.interval,
        c.open_time,
        LAG(c.open_time) OVER (PARTITION BY c.symbol_id, c.interval ORDER BY c.open_time) AS prev_open
      FROM candles c
      WHERE c.exchange_id = %s
        AND c.interval = %s
    ),
    g AS (
      SELECT
        b.symbol_id,
        b.interval,
        b.prev_open,
        b.open_time AS next_open,
        (b.open_time - b.prev_open) AS gap
      FROM base b
      WHERE b.prev_open IS NOT NULL
        AND b.open_time > b.prev_open + (%s)::interval
        AND (b.open_time - b.prev_open) >= ((%s)::interval * %s)
    ),
    t AS (
      SELECT
        g.symbol_id,
        g.interval,
        g.prev_open,
        g.next_open,
        g.gap
      FROM g
      WHERE 1=1
      {extra_time_filter}
    ),
    joined AS (
      SELECT
        t.symbol_id,
        s.symbol,
        t.interval,
        t.prev_open,
        t.next_open,
        (t.prev_open + (%s)::interval) AS from_ts,
        t.next_open AS to_ts,
        EXTRACT(EPOCH FROM t.gap) AS gap_seconds,
        GREATEST(
          0,
          CAST(FLOOR(EXTRACT(EPOCH FROM t.gap) / EXTRACT(EPOCH FROM (%s)::interval)) AS int) - 1
        ) AS missed_bars
      FROM t
      JOIN symbols s ON s.symbol_id = t.symbol_id
      WHERE {sym_where}
    )
    """

    if int(per_symbol) > 0:
        per_symbol = max(1, int(per_symbol))
        sql = base_sql + """
        , ranked AS (
          SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY gap_seconds DESC) AS rn
          FROM joined
        )
        SELECT
          symbol_id, symbol, interval, prev_open, next_open, from_ts, to_ts, gap_seconds, missed_bars
        FROM ranked
        WHERE rn <= %s
        ORDER BY gap_seconds DESC
        LIMIT %s;
        """
    else:
        sql = base_sql + """
        SELECT
          symbol_id, symbol, interval, prev_open, next_open, from_ts, to_ts, gap_seconds, missed_bars
        FROM joined
        ORDER BY gap_seconds DESC
        LIMIT %s;
        """

    args: list[Any] = [
        int(exchange_id),
        str(interval),
        itv_sql,
        itv_sql,
        int(mult),
    ]

    # extra time filter args
    args += args_extra

    # from_ts calc interval param + missed bars calc interval param
    args += [itv_sql, itv_sql]

    # symbols join filter args
    args += [int(exchange_id)]

    if only_active:
        # sym_where already added is_active condition without extra args
        pass

    if int(per_symbol) > 0:
        args += [int(per_symbol), int(top)]
    else:
        args += [int(top)]

    rows: list[GapRow] = []
    with storage.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            res = cur.fetchall() or []

    for r in res:
        rows.append(
            GapRow(
                symbol_id=int(r[0]),
                symbol=str(r[1]),
                interval=str(r[2]),
                prev_open=r[3],
                next_open=r[4],
                from_ts=r[5],
                to_ts=r[6],
                gap_seconds=float(r[7] or 0.0),
                missed_bars=int(r[8] or 0),
            )
        )
    return rows


def _print_table(rows: list[GapRow], *, title: str) -> None:
    print("")
    print("=" * 120)
    print(title)
    print("=" * 120)

    if not rows:
        print("✅ Дырок не найдено (по заданным условиям).")
        return

    header = (
        f"{'#':>3}  {'SYMBOL':<14} {'ITV':<4} {'MISSED':>6}  {'GAP':>8}  "
        f"{'FROM (missing starts)':<20}  {'TO (next exists)':<20}"
    )
    print(header)
    print("-" * 120)

    for i, g in enumerate(rows, start=1):
        print(
            f"{i:>3}  {g.symbol:<14} {g.interval:<4} {g.missed_bars:>6}  {_fmt_gap_seconds(g.gap_seconds):>8}  "
            f"{_fmt_dt(g.from_ts):<20}  {_fmt_dt(g.to_ts):<20}"
        )


# =========================================================
# CLI
# =========================================================

def main() -> None:
    p = argparse.ArgumentParser(
        prog="gap_check_candles",
        description="Auto gap-check for candles table: finds biggest gaps and prints missing ranges.",
    )
    p.add_argument("--exchange-id", type=int, default=1)
    p.add_argument("--intervals", type=str, default="4h", help='Comma list like "5m,15m,1h,4h"')
    p.add_argument("--top", type=int, default=50, help="How many rows to show per interval")
    p.add_argument("--only-active", action="store_true", help="Use only active symbols from symbols table")

    p.add_argument("--min-gap-points", type=int, default=1, help="Min missed bars to count as gap (>=1)")
    p.add_argument("--max-gap-days", type=int, default=0, help="If >0: search gaps only in last N days")

    p.add_argument("--per-symbol", type=int, default=0, help="If >0: show top N gaps per symbol (row_number)")

    args = p.parse_args()

    dsn = os.environ.get("PG_DSN") or os.environ.get("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("PG_DSN (or POSTGRES_DSN) env is required")

    pool = create_pool(dsn)
    storage = PostgreSQLStorage(pool=pool)

    try:
        intervals = _parse_intervals(args.intervals) or ["4h"]

        for itv in intervals:
            rows = _fetch_gaps_top(
                storage,
                exchange_id=int(args.exchange_id),
                interval=str(itv),
                top=int(args.top),
                only_active=bool(args.only_active),
                min_gap_points=int(args.min_gap_points),
                max_gap_days=int(args.max_gap_days),
                per_symbol=int(args.per_symbol),
            )

            mode = f"per_symbol={args.per_symbol}" if int(args.per_symbol) > 0 else "global_top"
            title = (
                f"[Candles GAP CHECK] interval={itv}  mode={mode}  "
                f"top={args.top}  only_active={args.only_active}  "
                f"min_gap_points={args.min_gap_points}  max_gap_days={args.max_gap_days}"
            )
            _print_table(rows, title=title)

    finally:
        try:
            pool.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
