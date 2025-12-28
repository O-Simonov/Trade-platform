# src/platform/exchanges/binance/collector_symbol_filters.py
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

log = logging.getLogger("src.platform.exchanges.binance.collector_symbol_filters")


def _to_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _extract_filters_from_exchange_info_symbol(sym: dict) -> dict[str, Any]:
    """
    Parse Binance exchangeInfo symbol entry -> our symbol_filters fields.
    We derive:
      price_tick  from PRICE_FILTER.tickSize
      qty_step    from LOT_SIZE.stepSize
      min_qty     from LOT_SIZE.minQty
      max_qty     from LOT_SIZE.maxQty
      min_notional from MIN_NOTIONAL.notional or MIN_NOTIONAL.minNotional (some variants)
    """
    price_tick = None
    qty_step = None
    min_qty = None
    max_qty = None
    min_notional = None

    for f in (sym.get("filters") or []):
        ftype = f.get("filterType")
        if ftype == "PRICE_FILTER":
            price_tick = _to_float(f.get("tickSize"))
        elif ftype == "LOT_SIZE":
            qty_step = _to_float(f.get("stepSize"))
            min_qty = _to_float(f.get("minQty"))
            max_qty = _to_float(f.get("maxQty"))
        elif ftype == "MIN_NOTIONAL":
            # Binance futures can use "notional" field
            min_notional = _to_float(f.get("notional")) or _to_float(f.get("minNotional"))

    return {
        "price_tick": price_tick,
        "qty_step": qty_step,
        "min_qty": min_qty,
        "max_qty": max_qty,
        "min_notional": min_notional,
        # We don't have these reliably from exchangeInfo -> keep None for now
        "max_leverage": None,
        "margin_type": None,
    }


def run_symbol_filters_collector(
    *,
    binance_rest,
    storage,
    exchange_id: int,
    symbol_ids: Dict[str, int],
    interval_sec: int = 3600,
) -> None:
    """
    Periodically load exchangeInfo and upsert trading filters for configured symbols.
    """
    interval_sec = int(interval_sec)
    log.info("[SymbolFilters] collector started interval=%ss", interval_sec)

    while True:
        try:
            info = binance_rest.fetch_exchange_info()
            symbols_info = info.get("symbols") or []

            # build dict: "LTCUSDT" -> parsed filters
            parsed: Dict[str, dict] = {}
            for s in symbols_info:
                name = str(s.get("symbol") or "").upper()
                if not name:
                    continue
                parsed[name] = _extract_filters_from_exchange_info_symbol(s)

            now = datetime.now(timezone.utc)

            rows: List[Tuple[Any, ...]] = []
            for sym, sid in symbol_ids.items():
                f = parsed.get(sym)
                if not f:
                    continue

                # must be 10 fields (see storage.upsert_symbol_filters)
                rows.append((
                    int(exchange_id),
                    int(sid),
                    f.get("price_tick"),
                    f.get("qty_step"),
                    f.get("min_qty"),
                    f.get("max_qty"),
                    f.get("min_notional"),
                    f.get("max_leverage"),   # None пока
                    f.get("margin_type"),    # None пока
                    now,
                ))

            if rows:
                n = storage.upsert_symbol_filters(rows)
                log.info("[SymbolFilters] upserted %s symbols", n)
            else:
                log.warning("[SymbolFilters] nothing to upsert (no rows)")

        except Exception as e:
            # если это бан 418 от Binance, твой REST клиент обычно кидает RuntimeError(...)
            # просто логируем и не валим поток
            log.exception("[SymbolFilters] error: %s", e)

        time.sleep(interval_sec)
