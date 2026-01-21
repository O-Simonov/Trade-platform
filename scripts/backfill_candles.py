# scripts/backfill_candles.py
from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta, timezone

from src.platform.exchanges.binance.rest import BinanceFuturesREST
from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage


def parse_klines_payload(*, payload, exchange_id: int, symbol_id: int, interval: str, source: str):
    rows = []
    now = datetime.now(timezone.utc)
    if not payload:
        return rows

    for k in payload:
        open_time_ms = int(k[0])
        open_time = datetime.fromtimestamp(open_time_ms / 1000, tz=timezone.utc)

        o = float(k[1]); h = float(k[2]); l = float(k[3]); c = float(k[4])
        vol_base = float(k[5])

        quote_vol = float(k[7]) if len(k) > 7 and k[7] is not None else None
        trades = int(k[8]) if len(k) > 8 and k[8] is not None else None

        taker_buy_base = float(k[9]) if len(k) > 9 and k[9] is not None else None
        taker_buy_quote = float(k[10]) if len(k) > 10 and k[10] is not None else None

        taker_sell_base = None
        taker_sell_quote = None
        delta_base = None
        delta_quote = None

        if taker_buy_base is not None:
            taker_sell_base = vol_base - taker_buy_base
            delta_base = taker_buy_base - taker_sell_base

        if quote_vol is not None and taker_buy_quote is not None:
            taker_sell_quote = quote_vol - taker_buy_quote
            delta_quote = taker_buy_quote - taker_sell_quote

        rows.append({
            "exchange_id": int(exchange_id),
            "symbol_id": int(symbol_id),
            "interval": str(interval),
            "open_time": open_time,
            "open": o, "high": h, "low": l, "close": c,
            "volume": vol_base,
            "quote_volume": quote_vol,
            "trades": trades,
            "taker_buy_base": taker_buy_base,
            "taker_buy_quote": taker_buy_quote,
            "taker_sell_base": taker_sell_base,
            "taker_sell_quote": taker_sell_quote,
            "delta_base": delta_base,
            "delta_quote": delta_quote,
            "cvd_quote": None,
            "source": str(source),
            "updated_at": now,
        })
    return rows


def _parse_dt(s: str) -> datetime:
    # "2026-01-04T07:45:00Z" или "2026-01-04 07:45:00"
    s = s.strip().replace("Z", "+00:00").replace(" ", "T")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def main():
    # --- настройки через env (проще запускать в PowerShell) ---
    dsn = os.environ.get("POSTGRES_DSN")
    if not dsn:
        print("ERROR: POSTGRES_DSN is empty")
        sys.exit(2)

    symbol = os.environ.get("SYMBOL", "LTCUSDT").upper()
    interval = os.environ.get("INTERVAL", "5m")
    exchange_id = int(os.environ.get("EXCHANGE_ID", "1"))
    symbol_id = int(os.environ.get("SYMBOL_ID", "64"))  # можно руками как у тебя

    # backfill диапазон:
    # BACKFILL_FROM (куда хотим дойти) и BACKFILL_TO (до куда)
    backfill_from = _parse_dt(os.environ.get("BACKFILL_FROM", "2026-01-04T07:45:00Z"))
    backfill_to = _parse_dt(os.environ.get("BACKFILL_TO", "2026-01-13T14:00:00Z"))

    limit = int(os.environ.get("LIMIT", "1500"))
    sleep_sec = float(os.environ.get("SLEEP_SEC", "0.25"))

    pool = create_pool(dsn)
    storage = PostgreSQLStorage(pool)
    rest = BinanceFuturesREST(api_key=None, secret_key=None)

    # начинаем с backfill_from и идём вперёд чанками
    cur = backfill_from
    total = 0

    print(f"[BACKFILL] {symbol} {interval} from={backfill_from} to={backfill_to} limit={limit}")

    while cur < backfill_to:
        start_ms = int(cur.timestamp() * 1000)
        end_ms = int(backfill_to.timestamp() * 1000)

        payload = rest.klines(symbol=symbol, interval=interval, start_time_ms=start_ms, end_time_ms=end_ms, limit=limit)
        rows = parse_klines_payload(payload=payload, exchange_id=exchange_id, symbol_id=symbol_id, interval=interval, source="backfill")

        if not rows:
            print("[BACKFILL] no rows -> stop")
            break

        n = storage.upsert_candles(rows)
        total += int(n or 0)

        last_open = rows[-1]["open_time"]
        print(f"[BACKFILL] upserts={n} last_open={last_open} total={total}")

        # защита от зацикливания
        if last_open <= cur:
            cur = cur + timedelta(minutes=5)
        else:
            cur = last_open + timedelta(milliseconds=1)

        time.sleep(sleep_sec)

    print(f"[BACKFILL] DONE total_upserts={total}")


if __name__ == "__main__":
    main()
