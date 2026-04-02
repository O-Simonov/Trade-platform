# src/platform/exchanges/binance/parsers/symbol_filters.py
from __future__ import annotations


def parse_symbol_filters(filters: list[dict]) -> dict:
    out: dict = {}

    for f in filters:
        t = f.get("filterType")

        if t == "LOT_SIZE":
            out["min_qty"] = float(f.get("minQty", 0))
            out["step_size"] = float(f.get("stepSize", 0))
            out["max_qty"] = float(f.get("maxQty", 0))

        elif t == "PRICE_FILTER":
            out["tick_size"] = float(f.get("tickSize", 0))

        elif t == "MIN_NOTIONAL":
            # Binance Futures использует "notional"
            out["min_notional"] = float(f.get("notional", 0))

    return out
