from __future__ import annotations
from platform.exchanges.binance.exchange import BinanceExchange

def build_exchange(name: str):
    name = name.lower()
    if name == "binance":
        return BinanceExchange()
    raise ValueError(f"Unknown exchange: {name}")
