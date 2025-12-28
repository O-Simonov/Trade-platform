# src/platform/exchanges/binance/collector_symbol_filters.py
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional

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
    We care about:
      - price_tick (PRICE_FILTER.tickSize)
      - qty_step, min_qty, max_qty (LOT_SIZE)
      - min_notional (MIN_NOTIONAL.notional)
    """
    out: Dict[str, Any] = {
        "price_tick": None,
        "qty_step": None,
        "min_qty": None,
        "max_qty": None,
        "min_notional": None,
        # optional extras
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
            # on futures it's usually "notional"
            out["min_notional"] = _to_float(f.get("notional") or f.get("minNotional"))

    # safety defaults (so preflight won't crash on None)
    if out["price_tick"] is None:
        out["price_tick"] = 0.0
    if out["qty_step"] is None:
        out["qty_step"] = 0.0

    return out


def run_symbol_filters_collector(
    *,
    binance_rest,
    storage,
    exchange_id: int,
    symbol_ids: Dict[str, int],
    interval_sec: int = 3600,
) -> None:
    """
    Daemon loop: fetch exchangeInfo and upsert symbol_filters for given symbol_ids.
    """
    interval_sec = int(interval_sec or 3600)
    logger.info("[SymbolFilters] collector started interval=%ss", interval_sec)

    while True:
        try:
            info = binance_rest.fetch_exchange_info()
            symbols = info.get("symbols") or []

            now = datetime.now(timezone.utc)
            rows = []

            # build only for our symbols
            need = set((symbol_ids or {}).keys())

            for s in symbols:
                sym = (s.get("symbol") or "").upper()
                if not sym or sym not in need:
                    continue

                parsed = _extract_filters_for_symbol(s)
                rows.append(
                    {
                        "exchange_id": int(exchange_id),
                        "symbol_id": int(symbol_ids[sym]),
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

        except Exception:
            logger.exception("[SymbolFilters] error")

        time.sleep(interval_sec)


def start_symbol_filters_collector(
    *,
    rest,
    storage,
    exchange_id: int,
    symbol_ids: Dict[str, int],
    interval_sec: int = 3600,
) -> None:
    """
    Helper to start collector in a daemon thread (like you call from run_instances.py).
    """
    t = threading.Thread(
        target=run_symbol_filters_collector,
        kwargs=dict(
            binance_rest=rest,
            storage=storage,
            exchange_id=exchange_id,
            symbol_ids=symbol_ids,
            interval_sec=int(interval_sec or 3600),
        ),
        daemon=True,
        name="BinanceSymbolFiltersCollector",
    )
    t.start()
    logger.info("[SymbolFilters] collector thread started")

