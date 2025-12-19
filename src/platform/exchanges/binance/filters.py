def parse_symbol_filters(sym: dict) -> dict:
    """
    Parse Binance Futures exchangeInfo symbol filters
    """
    qty_step = None
    min_qty = None
    max_qty = None
    price_tick = None
    min_notional = None

    for f in sym.get("filters", []):
        t = f.get("filterType")

        if t == "LOT_SIZE":
            qty_step = float(f["stepSize"])
            min_qty = float(f["minQty"])
            max_qty = float(f["maxQty"])

        elif t == "PRICE_FILTER":
            price_tick = float(f["tickSize"])

        elif t == "MIN_NOTIONAL":
            min_notional = float(f.get("notional", f.get("minNotional", 0)))

    if qty_step is None or min_qty is None or price_tick is None:
        raise ValueError(f"Incomplete filters for symbol {sym.get('symbol')}")

    return {
        "qty_step": qty_step,
        "min_qty": min_qty,
        "max_qty": max_qty,
        "price_tick": price_tick,
        "min_notional": min_notional,
    }
