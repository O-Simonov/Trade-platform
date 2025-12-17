def parse_symbol_filters(s: dict) -> dict:
    """
    Extract filters from Binance exchangeInfo symbol block.
    """
    out = {}

    for f in s.get("filters", []):
        t = f.get("filterType")

        if t == "LOT_SIZE":
            out["qty_step"] = float(f["stepSize"])
            out["min_qty"] = float(f["minQty"])
            out["max_qty"] = float(f["maxQty"])

        elif t == "PRICE_FILTER":
            out["price_tick"] = float(f["tickSize"])

        elif t == "MIN_NOTIONAL":
            out["min_notional"] = float(f.get("notional", f.get("minNotional", 0)))

    return out
