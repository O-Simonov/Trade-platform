# src/platform/exchanges/base/exchange.py
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Protocol, Callable, Any

from src.platform.core.models.order import OrderIntent


# -------- callbacks --------

TickCallback = Callable[[str, float], None]
UserEventCallback = Callable[[dict], None]
CandleCallback = Callable[[dict], None]


# -------- base exchange --------

class ExchangeAdapter(ABC):
    """
    Base exchange adapter.
    Exchange implementations MUST be stateless wrt OMS logic.
    """

    name: str

    # ---- lifecycle ----

    @abstractmethod
    def bind(self, *, storage, exchange_id: int, symbol_ids: dict[str, int]) -> None:
        ...

    # ---- subscriptions ----

    @abstractmethod
    def subscribe_ticks(self, *, account: str, symbols: list[str], cb: TickCallback) -> None:
        ...

    @abstractmethod
    def subscribe_user_stream(self, *, account: str, cb: UserEventCallback) -> None:
        ...

    @abstractmethod
    def subscribe_candles(
        self,
        *,
        account: str,
        symbols: list[str],
        intervals: list[str],
        cb: CandleCallback,
    ) -> None:
        ...

    # ---- trading ----

    @abstractmethod
    def place_order(
        self,
        intent: OrderIntent,
    ) -> dict:
        """
        Submit order to exchange.
        """
        ...

    # ---- state ----

    @abstractmethod
    def fetch_positions(self, *, account: str) -> list[Any]:
        ...

    @abstractmethod
    def fetch_account_state(self, *, account: str) -> dict:
        ...
