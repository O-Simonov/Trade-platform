# src/platform/exchanges/binance/collector_symbol_filters.py
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Iterable

logger = logging.getLogger("src.platform.exchanges.binance.collector_symbol_filters")


def _to_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _extract_filters_for_symbol(sym_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse Binance exchangeInfo["symbols"][i]["filters"] into normalized fields.
    """
    out: Dict[str, Any] = {
        "price_tick": None,
        "qty_step": None,
        "min_qty": None,
        "max_qty": None,
        "min_notional": None,
        "max_leverage": None,
        "margin_type": None,
    }

    filters = sym_info.get("filters") or []
    for f in filters:
        ftype = f.get("filterType")

        if ftype == "PRICE_FILTER":
            out["price_tick"] = _to_float(f.get("tickSize"))

        elif ftype == "LOT_SIZE":
            out["qty_step"] = _to_float(f.get("stepSize"))
            out["min_qty"] = _to_float(f.get("minQty"))
            out["max_qty"] = _to_float(f.get("maxQty"))

        elif ftype == "MIN_NOTIONAL":
            out["min_notional"] = _to_float(f.get("notional") or f.get("minNotional"))

    # safety defaults
    if out["price_tick"] is None:
        out["price_tick"] = 0.0
    if out["qty_step"] is None:
        out["qty_step"] = 0.0

    return out


def _fresh_symbol_ids_from_db(
    *,
    storage,
    exchange_id: int,
    symbol_ids: Iterable[int],
    fresh_sec: int,
) -> set[int]:
    """
    Return set(symbol_id) which have updated_at >= NOW() - fresh_sec.
    """
    ids = [int(x) for x in symbol_ids]
    if not ids:
        return set()

    try:
        with storage.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol_id
                    FROM symbol_filters
                    WHERE exchange_id = %s
                      AND symbol_id = ANY(%s::int[])
                      AND updated_at >= NOW() - (%s * INTERVAL '1 second')
                    """,
                    (int(exchange_id), ids, int(fresh_sec)),
                )
                rows = cur.fetchall()
        return {int(r[0]) for r in (rows or [])}

    except Exception:
        logger.exception("[SymbolFilters] DB freshness check failed → will refresh all")
        return set()


def run_symbol_filters_collector(
    *,
    binance_rest,
    storage,
    exchange_id: int,
    symbol_ids: Dict[str, int],
    interval_sec: int = 3600,
    do_seed_on_start: bool = True,
) -> None:
    """
    Daemon loop: refresh symbol_filters only when stale.

    IMPORTANT:
      - НЕ дергаем exchangeInfo при каждом рестарте, если в БД уже есть свежие фильтры.
      - do_seed_on_start=True значит "разрешить обновление сразу на старте", но
        по факту мы всё равно проверяем свежесть в БД.
    """
    interval_sec = int(interval_sec or 3600)
    logger.info("[SymbolFilters] collector started interval=%ss seed=%s", interval_sec, bool(do_seed_on_start))

    backoff = 1.0  # grows on errors up to 300s

    while True:
        try:
            need_map = dict(symbol_ids or {})
            if not need_map:
                logger.info("[SymbolFilters] no symbols configured → sleep")
                time.sleep(interval_sec)
                continue

            # если do_seed_on_start=False, всё равно проверим свежесть и обновим только если нужно
            fresh = _fresh_symbol_ids_from_db(
                storage=storage,
                exchange_id=exchange_id,
                symbol_ids=need_map.values(),
                fresh_sec=interval_sec,
            )

            # если все свежие — НЕ делаем REST exchangeInfo
            if len(fresh) >= len(need_map):
                logger.info("[SymbolFilters] up-to-date (fresh<%ss) → skip", interval_sec)
                time.sleep(interval_sec)
                continue

            # --- fetch exchangeInfo once ---
            info = binance_rest.fetch_exchange_info()
            symbols = info.get("symbols") or []

            now = datetime.now(timezone.utc)
            rows = []
            need_syms = set(need_map.keys())

            for s in symbols:
                sym = (s.get("symbol") or "").upper()
                if not sym or sym not in need_syms:
                    continue

                parsed = _extract_filters_for_symbol(s)
                rows.append(
                    {
                        "exchange_id": int(exchange_id),
                        "symbol_id": int(need_map[sym]),
                        "price_tick": parsed["price_tick"],
                        "qty_step": parsed["qty_step"],
                        "min_qty": parsed["min_qty"],
                        "max_qty": parsed["max_qty"],
                        "min_notional": parsed["min_notional"],
                        "max_leverage": parsed.get("max_leverage"),
                        "margin_type": parsed.get("margin_type"),
                        "updated_at": now,
                    }
                )

            n = 0
            if rows:
                n = storage.upsert_symbol_filters(rows)

            logger.info("[SymbolFilters] upserted %s symbols", n)
            backoff = 1.0
            time.sleep(interval_sec)

        except Exception as e:
            logger.warning("[SymbolFilters] error: %s", e, exc_info=True)
            time.sleep(backoff)
            backoff = min(backoff * 2.0, 300.0)


def start_symbol_filters_collector(
    *,
    rest,
    storage,
    exchange_id: int,
    symbol_ids: Dict[str, int],
    interval_sec: int = 3600,
    do_seed_on_start: bool = True,
) -> None:
    t = threading.Thread(
        target=run_symbol_filters_collector,
        kwargs=dict(
            binance_rest=rest,
            storage=storage,
            exchange_id=exchange_id,
            symbol_ids=symbol_ids,
            interval_sec=int(interval_sec or 3600),
            do_seed_on_start=bool(do_seed_on_start),
        ),
        daemon=True,
        name="BinanceSymbolFiltersCollector",
    )
    t.start()
    logger.info("[SymbolFilters] collector thread started")
