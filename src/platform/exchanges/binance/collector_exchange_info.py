# src/platform/exchanges/binance/collector_exchange_info.py

from src.platform.exchanges.binance.filters import parse_symbol_filters


def sync_exchange_info(
    *,
    binance_rest,
    storage,
    exchange_id: int,
    symbol_ids: dict[str, int],
) -> int:
    """
    Fetch Binance Futures exchangeInfo and store symbol filters.
    Bootstrap only (best-effort).
    """

    info = binance_rest.fetch_exchange_info()
    rows: list[dict] = []

    for s in info.get("symbols", []):
        if s.get("contractType") != "PERPETUAL":
            continue

        symbol = s.get("symbol")
        if symbol not in symbol_ids:
            continue

        parsed = parse_symbol_filters(s)

        # 🔑 НОРМАЛИЗАЦИЯ — ВСЕ поля всегда есть
        row = {
            "exchange_id": exchange_id,
            "symbol_id": symbol_ids[symbol],

            "price_tick": parsed.get("price_tick"),
            "qty_step": parsed.get("qty_step"),
            "min_qty": parsed.get("min_qty"),
            "max_qty": parsed.get("max_qty"),
            "min_notional": parsed.get("min_notional"),

            # ❗ ОБЯЗАТЕЛЬНО
            "max_leverage": parsed.get("max_leverage"),
            "margin_type": parsed.get("margin_type"),
        }

        rows.append(row)

    if not rows:
        return 0

    return storage.upsert_symbol_filters(rows)
