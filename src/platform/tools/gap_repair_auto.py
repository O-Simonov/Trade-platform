# src/platform/tools/gap_repair_auto.py
from __future__ import annotations

import os
import argparse
import time
import random
import logging
import threading
from typing import Sequence

from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage
from src.platform.exchanges.binance.rest import BinanceFuturesREST

# Берём готовые “боевые” функции ремонта, которые ты уже используешь
from src.platform.exchanges.binance.collector_candles_gap_repair import (
    _load_symbols,
    _prioritize_symbols,
    _norm_symbols_list,
    _find_gaps,
    _backfill_range,
)

log = logging.getLogger("tools.gap_repair_auto")


def _parse_csv_list(s: str) -> list[str]:
    out: list[str] = []
    for x in (s or "").split(","):
        x = x.strip()
        if x:
            out.append(x)
    return out


def _make_rest_public_no_keys() -> BinanceFuturesREST:
    # public endpoints OK без ключей
    return BinanceFuturesREST("", "")


def _run_one_cycle(
    *,
    rest: BinanceFuturesREST,
    storage: PostgreSQLStorage,
    exchange_id: int,
    intervals: list[str],
    only_active: bool,
    priority_symbols: Sequence[str],
    only_symbols: Sequence[str],
    max_symbols_per_cycle: int,
    max_gaps_per_cycle: int,
    gaps_per_symbol: int,
    limit: int,
    sleep_sec: float,
    max_gap_days: int,
    max_backfill_span_days: int,
    min_gap_points: int,
    stop_event: threading.Event,
) -> tuple[int, int, int]:
    """
    Возвращает:
      gaps_fixed, upserts_total, gaps_seen
    """
    # грузим symbols из БД
    symbols = _load_symbols(storage, exchange_id=int(exchange_id), only_active=bool(only_active))

    if only_symbols:
        only_set = {str(x).upper().strip() for x in only_symbols if str(x).strip()}
        symbols = [(sid, sym) for sid, sym in symbols if sym.upper() in only_set]

    if not symbols:
        log.warning("[AutoRepair] no symbols")
        return 0, 0, 0

    if priority_symbols:
        symbols = _prioritize_symbols(symbols, priority=priority_symbols, shuffle_tail=True)
    else:
        random.shuffle(symbols)

    if max_symbols_per_cycle > 0:
        symbols = symbols[: int(max_symbols_per_cycle)]

    gaps_fixed = 0
    upserts_total = 0
    gaps_seen = 0

    for symbol_id, symbol in symbols:
        if stop_event.is_set() or gaps_fixed >= max_gaps_per_cycle:
            break

        for itv in intervals:
            if stop_event.is_set() or gaps_fixed >= max_gaps_per_cycle:
                break

            try:
                gaps = _find_gaps(
                    storage=storage,
                    exchange_id=int(exchange_id),
                    symbol_id=int(symbol_id),
                    interval=str(itv),
                    limit_gaps=int(gaps_per_symbol),
                    max_gap_days=int(max_gap_days),
                    min_gap_points=int(min_gap_points),
                )
            except Exception:
                log.exception("[AutoRepair] find_gaps failed %s %s", symbol, itv)
                continue

            if gaps:
                gaps_seen += len(gaps)

            for dt_from, dt_to in gaps:
                if stop_event.is_set() or gaps_fixed >= max_gaps_per_cycle:
                    break

                log.info("[AutoRepair] GAP %s %s %s -> %s", symbol, itv, dt_from.isoformat(), dt_to.isoformat())
                try:
                    n = _backfill_range(
                        rest=rest,
                        storage=storage,
                        exchange_id=int(exchange_id),
                        symbol=str(symbol),
                        symbol_id=int(symbol_id),
                        interval=str(itv),
                        dt_from=dt_from,
                        dt_to=dt_to,
                        limit=int(limit),
                        sleep_sec=float(sleep_sec),
                        stop_event=stop_event,
                        max_span_days=int(max_backfill_span_days),
                        step_by_interval=True,
                    )
                    upserts_total += int(n or 0)
                    gaps_fixed += 1
                    log.info("[AutoRepair] OK %s %s upserts=%d gaps_fixed=%d", symbol, itv, int(n or 0), gaps_fixed)
                except Exception:
                    log.exception("[AutoRepair] backfill failed %s %s", symbol, itv)

    return gaps_fixed, upserts_total, gaps_seen


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        force=True,
    )

    ap = argparse.ArgumentParser(
        prog="gap_repair_auto",
        description="Auto repair candle gaps using REST klines until clean (optional).",
    )

    ap.add_argument("--exchange-id", type=int, default=1)
    ap.add_argument("--intervals", type=str, default="4h", help='Comma list: "5m,1h,4h"')

    ap.add_argument("--only-active", action="store_true")
    ap.add_argument("--symbols", type=str, default="", help='Optional: "BTCUSDT,ETHUSDT"')
    ap.add_argument("--priority-symbols", type=str, default="", help='Optional: "BTCUSDT,ETHUSDT"')

    ap.add_argument("--until-clean", action="store_true", help="loop until no gaps fixed in cycle")
    ap.add_argument("--max-cycles", type=int, default=50)
    ap.add_argument("--sleep-between-cycles", type=float, default=5.0)

    ap.add_argument("--max-symbols-per-cycle", type=int, default=60)
    ap.add_argument("--max-gaps-per-cycle", type=int, default=20)
    ap.add_argument("--gaps-per-symbol", type=int, default=2)

    ap.add_argument("--limit", type=int, default=1000)
    ap.add_argument("--sleep-sec", type=float, default=0.35)

    ap.add_argument("--max-gap-days", type=int, default=60, help="0=no filter")
    ap.add_argument("--max-backfill-span-days", type=int, default=0, help="0=no limit")
    ap.add_argument("--min-gap-points", type=int, default=1)

    args = ap.parse_args()

    dsn = os.environ.get("PG_DSN") or os.environ.get("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("PG_DSN (or POSTGRES_DSN) env is required")

    intervals = _parse_csv_list(args.intervals) or ["4h"]
    only_symbols = [x.upper() for x in _parse_csv_list(args.symbols)]
    priority_symbols = _norm_symbols_list(_parse_csv_list(args.priority_symbols))

    pool = create_pool(dsn)
    storage = PostgreSQLStorage(pool=pool)

    rest = _make_rest_public_no_keys()
    stop_event = threading.Event()

    log.info(
        "[AutoRepair] START exchange_id=%d intervals=%s only_active=%s until_clean=%s max_cycles=%d",
        int(args.exchange_id),
        ",".join(intervals),
        bool(args.only_active),
        bool(args.until_clean),
        int(args.max_cycles),
    )

    try:
        cycle = 0
        while True:
            cycle += 1
            if 0 < int(args.max_cycles) < cycle:
                log.warning("[AutoRepair] reached max_cycles=%d -> stop", int(args.max_cycles))
                break

            t0 = time.time()
            gaps_fixed, upserts_total, gaps_seen = _run_one_cycle(
                rest=rest,
                storage=storage,
                exchange_id=int(args.exchange_id),
                intervals=intervals,
                only_active=bool(args.only_active),
                priority_symbols=priority_symbols,
                only_symbols=only_symbols,
                max_symbols_per_cycle=int(args.max_symbols_per_cycle),
                max_gaps_per_cycle=int(args.max_gaps_per_cycle),
                gaps_per_symbol=int(args.gaps_per_symbol),
                limit=int(args.limit),
                sleep_sec=float(args.sleep_sec),
                max_gap_days=int(args.max_gap_days),
                max_backfill_span_days=int(args.max_backfill_span_days),
                min_gap_points=int(args.min_gap_points),
                stop_event=stop_event,
            )
            dt = time.time() - t0

            log.info(
                "[AutoRepair] CYCLE=%d done gaps_fixed=%d gaps_seen=%d upserts_total=%d dt=%.1fs",
                cycle, gaps_fixed, gaps_seen, upserts_total, dt
            )

            # режим “до чистой базы”
            if bool(args.until_clean):
                if gaps_fixed == 0:
                    log.info("[AutoRepair] CLEAN ✅ no gaps fixed in cycle -> stop")
                    break
                time.sleep(max(0.1, float(args.sleep_between_cycles)))
                continue

            # если не until-clean -> 1 цикл и выходим
            break

    except KeyboardInterrupt:
        log.warning("[AutoRepair] CTRL+C -> stop")
    finally:
        stop_event.set()
        try:
            pool.close()
        except Exception:
            pass
        log.info("[AutoRepair] STOP")


if __name__ == "__main__":
    main()
