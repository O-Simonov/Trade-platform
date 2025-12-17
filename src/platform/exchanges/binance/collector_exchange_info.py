
from __future__ import annotations

from src.platform.exchanges.binance.filters import parse_symbol_filters


def sync_exchange_info(
    binance_rest,
    storage,
    exchange_id: int,
    symbol_ids: dict[str, int],
):
    """
    Load Binance Futures exchangeInfo and persist symbol filters to DB.
    """
    info = binance_rest.fetch_exchange_info()

    for s in info.get("symbols", []):
        if s.get("contractType") != "PERPETUAL":
            continue

        symbol = s.get("symbol")
        if not symbol or symbol not in symbol_ids:
            continue

        filters = parse_symbol_filters(s)

        storage.upsert_symbol_filters(
            exchange_id=exchange_id,
            symbol_id=symbol_ids[symbol],
            **filters,
        )
