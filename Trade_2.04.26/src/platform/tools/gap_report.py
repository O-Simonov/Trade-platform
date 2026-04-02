# src/platform/tools/gap_report.py
from __future__ import annotations

import os
import argparse
import time
from typing import Any, Dict, List, Sequence, Tuple

from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage


def _itv_to_pg_interval_str(itv: str) -> str:
    s = str(itv).strip().lower()
    if s.endswith("m"):
        return f"{int(s[:-1])} minutes"
    if s.endswith("h"):
        return f"{int(s[:-1])} hours"
    if s.endswith("d"):
        return f"{int(s[:-1])} days"
    raise ValueError(f"Unsupported interval: {itv}")


def _parse_csv_list(s: str) -> list[str]:
    out: list[str] = []
    for x in (s or "").split(","):
        x = x.strip()
        if x:
            out.append(x)
    return out


def _fmt_num(x: Any) -> str:
    try:
        return f"{int(x):,}".replace(",", " ")
    except Exception:
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


def _gap_stats_for_interval(
    storage: PostgreSQLStorage,
    *,
    exchange_id: int,
    interval: str,
    only_active: bool,
    max_gap_days: int,
    min_gap_points: int,
    only_symbols: Sequence[str],
) -> List[Tuple[int, str, int, int, float]]:
    """
    Возвращает список:
      (symbol_id, symbol, gaps_count, missed_bars_sum, max_gap_seconds)
    по одному interval
    """
    itv_sql = _itv_to_pg_interval_str(interval)
    min_gap_points = max(1, int(min_gap_points))
    max_gap_days = max(0, int(max_gap_days))
    mult = min_gap_points + 1

    only_symbols_u = {str(x).upper().strip() for x in (only_symbols or []) if str(x).strip()}
    symbols_filter_sql = ""
    symbols_args: list[Any] = []
    if only_symbols_u:
        symbols_filter_sql = " AND UPPER(s.symbol) = ANY(%s) "
        symbols_args.append(list(sorted(only_symbols_u)))

    active_sql = " AND s.is_active=true " if only_active else ""

    sql = f"""
    WITH t AS (
      SELECT
        c.symbol_id,
        s.symbol,
        c.open_time,
        LAG(c.open_time) OVER (PARTITION BY c.symbol_id ORDER BY c.open_time) AS prev
      FROM candles c
      JOIN symbols s
        ON s.exchange_id=c.exchange_id AND s.symbol_id=c.symbol_id
      WHERE c.exchange_id=%s
        AND c.interval=%s
        {active_sql}
        {symbols_filter_sql}
    ),
    g AS (
      SELECT
        symbol_id,
        symbol,
        open_time,
        prev,
        (open_time - prev) AS gap
      FROM t
      WHERE prev IS NOT NULL
    )
    SELECT
      symbol_id,
      symbol,
      COUNT(*)::int AS gaps_count,
      SUM(
        GREATEST(
          0,
          CAST(FLOOR(EXTRACT(EPOCH FROM gap) / EXTRACT(EPOCH FROM (%s)::interval)) AS int) - 1
        )
      )::int AS missed_bars_sum,
      MAX(EXTRACT(EPOCH FROM gap))::float AS max_gap_seconds
    FROM g
    WHERE open_time > prev + (%s)::interval
      AND gap >= ((%s)::interval * %s)
    """

    args: list[Any] = [
        int(exchange_id),
        str(interval),
    ]
    args.extend(symbols_args)

    args.extend([itv_sql, itv_sql, itv_sql, int(mult)])

    if max_gap_days > 0:
        sql += " AND open_time >= NOW() - (%s)::interval\n"
        args.append(f"{int(max_gap_days)} days")

    sql += """
    GROUP BY symbol_id, symbol
    ORDER BY missed_bars_sum DESC;
    """

    with storage.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            rows = cur.fetchall() or []

    out: List[Tuple[int, str, int, int, float]] = []
    for r in rows:
        out.append((int(r[0]), str(r[1]).upper(), int(r[2]), int(r[3]), float(r[4] or 0.0)))
    return out


def main() -> None:
    ap = argparse.ArgumentParser(
        prog="gap_report",
        description="TOP symbols by candle gaps (sum missed bars) across intervals",
    )
    ap.add_argument("--exchange-id", type=int, default=1)
    ap.add_argument("--intervals", type=str, default="4h", help='Comma list: "5m,1h,4h"')
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--only-active", action="store_true")
    ap.add_argument("--symbols", type=str, default="", help='Optional: "BTCUSDT,ETHUSDT"')
    ap.add_argument("--max-gap-days", type=int, default=60, help="0 = no filter")
    ap.add_argument("--min-gap-points", type=int, default=1, help="min missed bars")
    args = ap.parse_args()

    dsn = os.environ.get("PG_DSN") or os.environ.get("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("PG_DSN (or POSTGRES_DSN) env is required")

    pool = create_pool(dsn)
    storage = PostgreSQLStorage(pool=pool)

    try:
        intervals = _parse_csv_list(args.intervals) or ["4h"]
        only_symbols = [x.upper() for x in _parse_csv_list(args.symbols)]

        t0 = time.time()

        agg: Dict[int, Dict[str, Any]] = {}
        # agg[symbol_id] = {"symbol":..., "missed":..., "gaps":..., "max_gap":...}

        for itv in intervals:
            rows = _gap_stats_for_interval(
                storage,
                exchange_id=int(args.exchange_id),
                interval=str(itv),
                only_active=bool(args.only_active),
                max_gap_days=int(args.max_gap_days),
                min_gap_points=int(args.min_gap_points),
                only_symbols=only_symbols,
            )
            for sid, sym, gaps_count, missed_sum, max_gap_sec in rows:
                rec = agg.get(sid)
                if rec is None:
                    rec = {"symbol": sym, "missed": 0, "gaps": 0, "max_gap": 0.0, "by_itv": {}}
                    agg[sid] = rec
                rec["missed"] += int(missed_sum)
                rec["gaps"] += int(gaps_count)
                rec["max_gap"] = max(float(rec["max_gap"]), float(max_gap_sec))
                rec["by_itv"][str(itv)] = {"missed": int(missed_sum), "gaps": int(gaps_count), "max_gap": float(max_gap_sec)}

        data = []
        for sid, rec in agg.items():
            data.append((sid, rec["symbol"], int(rec["missed"]), int(rec["gaps"]), float(rec["max_gap"]), rec["by_itv"]))

        data.sort(key=lambda x: (x[2], x[3], x[4]), reverse=True)
        data = data[: max(1, int(args.top))]

        dt = time.time() - t0

        print("")
        print("=" * 130)
        print(
            f"[GAP REPORT] exchange_id={args.exchange_id} intervals={intervals} only_active={args.only_active} "
            f"max_gap_days={args.max_gap_days} min_gap_points={args.min_gap_points} dt={dt:.1f}s"
        )
        print("=" * 130)

        if not data:
            print("✅ дыр не найдено")
            print("=" * 130)
            return

        for i, (sid, sym, missed, gaps, max_gap, by_itv) in enumerate(data, start=1):
            itv_parts = []
            for itv in intervals:
                st = by_itv.get(str(itv))
                if not st:
                    continue
                itv_parts.append(f"{itv}:missed={st['missed']} gaps={st['gaps']} max={_fmt_gap_seconds(st['max_gap'])}")
            itv_str = " | ".join(itv_parts) if itv_parts else "-"
            print(
                f"{i:>2}) {sym:<12}  missed={_fmt_num(missed):>8}  gaps={_fmt_num(gaps):>6}  "
                f"max_gap={_fmt_gap_seconds(max_gap):>8}   [{itv_str}]"
            )

        print("=" * 130)

    finally:
        try:
            pool.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
