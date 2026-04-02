# src/platform/exchanges/binance/collector_symbol_filters.py
from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional, List, Set

logger = logging.getLogger("src.platform.exchanges.binance.collector_symbol_filters")

# защита от "мусорных" значений и переполнений в старых схемах
_NUM_MAX_ABS = 9_999_999_999.0


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if not (v == v):  # NaN
            return None
        if abs(v) >= _NUM_MAX_ABS:
            return None
        return v
    except Exception:
        return None


def _extract_filters_for_symbol(sym_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Из exchangeInfo.symbols[].filters вынимаем основные торговые фильтры.

    Покрываем варианты Binance:
      - PRICE_FILTER.tickSize
      - LOT_SIZE.stepSize/minQty/maxQty
      - MARKET_LOT_SIZE.stepSize/minQty/maxQty (иногда полезно)
      - MIN_NOTIONAL.notional или minNotional
      - NOTIONAL.minNotional / notional (встречается на futures)
    """
    out: Dict[str, Any] = {
        "price_tick": None,
        "qty_step": None,
        "min_qty": None,
        "max_qty": None,
        "min_notional": None,
        "max_leverage": None,  # в exchangeInfo обычно нет, это отдельные endpoints
        "margin_type": None,   # это per-account, не per-symbol
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

        elif ftype == "MARKET_LOT_SIZE":
            # если LOT_SIZE отсутствует (редко), используем этот как fallback
            if out["qty_step"] is None:
                out["qty_step"] = _to_float(f.get("stepSize"))
            if out["min_qty"] is None:
                out["min_qty"] = _to_float(f.get("minQty"))
            if out["max_qty"] is None:
                out["max_qty"] = _to_float(f.get("maxQty"))

        elif ftype in ("MIN_NOTIONAL", "NOTIONAL"):
            # Binance бывает: notional / minNotional / minNotionalValue
            out["min_notional"] = _to_float(
                f.get("notional") or f.get("minNotional") or f.get("minNotionalValue")
            )

    # safe defaults
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
) -> Set[int]:
    """
    Возвращает symbol_id, у которых updated_at достаточно свежий.
    Важно: symbol_id в схеме BIGINT, поэтому используем bigint[].
    """
    ids = [int(x) for x in symbol_ids if x is not None]
    if not ids:
        return set()

    # PostgreSQL имеет лимиты на размер запроса/параметров; на 500-1000 ids всё ок,
    # но сделаем аккуратно chunk-ами на всякий случай.
    CHUNK = 2000
    fresh: Set[int] = set()

    try:
        with storage.pool.connection() as conn:
            with conn.cursor() as cur:
                for i in range(0, len(ids), CHUNK):
                    part = ids[i : i + CHUNK]
                    cur.execute(
                        """
                        SELECT symbol_id
                        FROM symbol_filters
                        WHERE exchange_id = %s
                          AND symbol_id = ANY(%s::bigint[])
                          AND updated_at >= NOW() - (%s * INTERVAL '1 second')
                        """,
                        (int(exchange_id), part, int(fresh_sec)),
                    )
                    rows = cur.fetchall()
                    fresh.update(int(r[0]) for r in (rows or []) if r and r[0] is not None)

        return fresh
    except Exception:
        logger.exception("[SymbolFilters] DB freshness check failed -> will refresh all")
        return set()


def run_symbol_filters_collector(
    *,
    rest,
    storage,
    exchange_id: int,
    symbol_ids: Dict[str, int],
    interval_sec: int = 3600,
    do_seed_on_start: bool = True,
    stop_event: threading.Event,
) -> None:
    interval_sec = max(60, int(interval_sec or 3600))
    logger.info("[SymbolFilters] collector started interval=%ss seed=%s", interval_sec, bool(do_seed_on_start))

    backoff = 1.0
    first = True

    while not stop_event.is_set():
        try:
            need_map = {str(k).upper().strip(): int(v) for k, v in (symbol_ids or {}).items() if str(k).strip()}
            if not need_map:
                stop_event.wait(interval_sec)
                continue

            if first and not do_seed_on_start:
                first = False
                stop_event.wait(interval_sec)
                continue
            first = False

            fresh = _fresh_symbol_ids_from_db(
                storage=storage,
                exchange_id=int(exchange_id),
                symbol_ids=need_map.values(),
                fresh_sec=interval_sec,
            )

            if len(fresh) >= len(need_map):
                logger.info("[SymbolFilters] up-to-date (fresh<%ss) -> skip", interval_sec)
                stop_event.wait(interval_sec)
                continue

            info = rest.fetch_exchange_info() or {}
            symbols = info.get("symbols") or []
            if not symbols:
                logger.warning("[SymbolFilters] exchangeInfo has empty symbols -> retry later")
                stop_event.wait(interval_sec)
                continue

            now = datetime.now(timezone.utc)
            rows: List[Dict[str, Any]] = []
            need_syms = set(need_map.keys())

            for s in symbols:
                sym = (s.get("symbol") or "").upper().strip()
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
                n = int(storage.upsert_symbol_filters(rows) or 0)

            logger.info("[SymbolFilters] upserted=%s need=%d fresh=%d", n, len(need_map), len(fresh))

            backoff = 1.0
            stop_event.wait(interval_sec)

        except Exception as e:
            logger.warning("[SymbolFilters] error: %s", e, exc_info=True)
            stop_event.wait(backoff)
            backoff = min(backoff * 2.0, 300.0)


def start_symbol_filters_collector(
    *,
    rest,
    storage,
    exchange_id: int,
    symbol_ids: Dict[str, int],
    interval_sec: int = 3600,
    do_seed_on_start: bool = True,
    stop_event: threading.Event,
) -> threading.Thread:
    t = threading.Thread(
        target=run_symbol_filters_collector,
        kwargs=dict(
            rest=rest,
            storage=storage,
            exchange_id=int(exchange_id),
            symbol_ids=dict(symbol_ids or {}),
            interval_sec=int(interval_sec or 3600),
            do_seed_on_start=bool(do_seed_on_start),
            stop_event=stop_event,
        ),
        daemon=True,
        name="BinanceSymbolFiltersCollector",
    )
    t.start()
    logger.info("[SymbolFilters] collector thread started")
    return t
