# src/platform/market_state/reader.py
from __future__ import annotations


class MarketStateReader:
    def __init__(self, storage, exchange: str, account: str):
        self.storage = storage
        self.exchange = exchange
        self.account = account

    def get_positions(self) -> dict:
        return self.storage.get_latest_positions(
            exchange=self.exchange,
            account=self.account,
        )

    def get_balances(self) -> dict:
        return self.storage.get_latest_balances(
            exchange=self.exchange,
            account=self.account,
        )
