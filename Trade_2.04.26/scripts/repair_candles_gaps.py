# scripts/repair_candles_gaps.py
from __future__ import annotations

import os
import time
import random
from datetime import datetime, timezone, timedelta
from typing import Any, Iterable

from src.platform.exchanges.binance.rest import BinanceFuturesREST
from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage
from src.platform.exchanges.binance.collector_candles import _parse_klines_payload


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _itv_to_timedelta(itv: str) -> timedelta:
    itv = itv.strip().lower()
    if itv.endswith("m"):
        return timedelta(minutes=int(itv[:-1]))
    if itv.endswith("h"):
        return timedelta(hours=int(itv[:-1]))
    if itv.endswith("d"):
        return timedelta(days=int(itv[:-1]))
    raise ValueError(f"Unsupported interval: {itv}")


def _load_symbols(storage: PostgreSQLStorage, exchange_id: int, only_active: bool) -> list[tuple[int, str]]:
    where = "WHERE exchange_id=%s"
    args: list[Any] = [exchange_id]
    if only_active:
        where += " AND is_active=true"
    sql = f"SELECT symbol_id, symbol FROM symbols {where} ORDER BY symbol_id;"
    with storage.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            rows = cur.fetchall()
    return [(int(r[0]), str(r[1])) for r in rows]


def _find_gaps(
    storage: PostgreSQLStorage,
    exchange_id: int,
    symbol_id: int,
    interval: str,
    limit_gaps: int = 5,
) -> list[tuple[datetime, datetime]]:
    """
    Возвращает список дыр: (from_ts, to_ts) где from_ts = prev + itv, to_ts = current_open_time.
    """
    itv_td = _itv_to_timedelta(interval)

    sql = """
    WITH t AS (
      SELECT open_time,
             LAG(open_time) OVER (ORDER BY open_time) AS prev
      FROM candles
      WHERE exchange_id=%s AND symbol_id=%s AND interval=%s
    )
    SELECT (prev + %s::interval) AS from_ts,
           open_time            AS to_ts
    FROM t
    WHERE prev IS NOT NULL
      AND open_time - prev > %s::interval
    ORDER BY (open_time - prev) DESC
    LIMIT %s;
    """
    itv_str = str(itv_td)
    # timedelta -> postgres interval строкой (например "0:05:00")
    # Postgres это понимает.
    with storage.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (exchange_id, symbol_id, interval, itv_str, itv_str, limit_gaps))
            rows = cur.fetchall()

    out: list[tuple[datetime, datetime]] = []
    for a, b in rows:
        if isinstance(a, datetime) and isinstance(b, datetime) and a < b:
            out.append((a, b))
    return out


def _backfill_range(
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
) -> int:
    """
    Backfill дырки [dt_from, dt_to] REST klines paging'ом.
    """
    itv_td = _itv_to_timedelta(interval)
    total = 0
    cur = dt_from

    # страховка
    limit = max(1, min(int(limit), 1500))
    sleep_sec = max(0.05, float(sleep_sec))

    # Binance ожидает ms
    def to_ms(dt: datetime) -> int:
        return int(dt.timestamp() * 1000)

    while cur < dt_to:
        payload = rest.klines(
            symbol=symbol,
            interval=interval,
            start_time_ms=to_ms(cur),
            end_time_ms=to_ms(dt_to),
            limit=limit,
        )
        rows = _parse_klines_payload(
            payload=payload,
            exchange_id=exchange_id,
            symbol_id=symbol_id,
            interval=interval,
            source="gap_repair",
        )
        if not rows:
            break

        n = int(storage.upsert_candles(rows) or 0)
        total += n

        last_open = rows[-1]["open_time"]
        # шаг на следующую свечу
        cur = last_open + itv_td

        time.sleep(sleep_sec)

        # если меньше лимита — значит дошли до конца диапазона
        if len(rows) < limit:
            break

    return total


def main() -> None:
    exchange_id = int(os.environ.get("EXCHANGE_ID", "1"))
    dsn = os.environ.get("POSTGRES_DSN", "").strip()
    if not dsn:
        print("ERROR: POSTGRES_DSN is empty")
        raise SystemExit(2)

    intervals = os.environ.get("INTERVALS", "5m,1h,4h,1d").split(",")
    intervals = [x.strip() for x in intervals if x.strip()]

    every_sec = int(os.environ.get("EVERY_SEC", "1800"))
    max_symbols_per_cycle = int(os.environ.get("MAX_SYMBOLS_PER_CYCLE", "80"))
    max_gaps_per_cycle = int(os.environ.get("MAX_GAPS_PER_CYCLE", "50"))
    gaps_per_symbol = int(os.environ.get("GAPS_PER_SYMBOL", "3"))

    limit = int(os.environ.get("LIMIT", "1500"))
    sleep_sec = float(os.environ.get("SLEEP_SEC", "0.25"))
    only_active = os.environ.get("ONLY_ACTIVE", "1").strip() not in ("0", "false", "False")

    # REST public OK
    rest = BinanceFuturesREST(api_key=None, secret_key=None)

    pool = create_pool(dsn)
    storage = PostgreSQLStorage(pool)

    print(f"[REPAIR] started exchange_id={exchange_id} intervals={intervals} every_sec={every_sec}")

    while True:
        t0 = time.time()

        symbols = _load_symbols(storage, exchange_id=exchange_id, only_active=only_active)
        if not symbols:
            print("[REPAIR] no symbols found")
            time.sleep(every_sec)
            continue

        # ограничим символы за цикл и перемешаем, чтобы равномерно чинить
        random.shuffle(symbols)
        symbols = symbols[: max(1, max_symbols_per_cycle)]

        fixed_total = 0
        gaps_fixed = 0

        for symbol_id, symbol in symbols:
            for itv in intervals:
                if gaps_fixed >= max_gaps_per_cycle:
                    break

                gaps = _find_gaps(
                    storage=storage,
                    exchange_id=exchange_id,
                    symbol_id=symbol_id,
                    interval=itv,
                    limit_gaps=gaps_per_symbol,
                )
                for dt_from, dt_to in gaps:
                    if gaps_fixed >= max_gaps_per_cycle:
                        break

                    print(f"[REPAIR] gap {symbol} {itv}: {dt_from.isoformat()} -> {dt_to.isoformat()}")
                    try:
                        n = _backfill_range(
                            rest=rest,
                            storage=storage,
                            exchange_id=exchange_id,
                            symbol=symbol,
                            symbol_id=symbol_id,
                            interval=itv,
                            dt_from=dt_from,
                            dt_to=dt_to,
                            limit=limit,
                            sleep_sec=sleep_sec,
                        )
                        fixed_total += n
                        gaps_fixed += 1
                        print(f"[REPAIR] ok {symbol} {itv} upserts={n} (gaps_fixed={gaps_fixed})")
                    except Exception as e:
                        print(f"[REPAIR] fail {symbol} {itv}: {e}")

        dt = time.time() - t0
        print(f"[REPAIR] cycle done gaps_fixed={gaps_fixed} upserts_total={fixed_total} dt={dt:.1f}s")

        # спим до следующего цикла
        time.sleep(max(5, every_sec))


if __name__ == "__main__":
    main()
