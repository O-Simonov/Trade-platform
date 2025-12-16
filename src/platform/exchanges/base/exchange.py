from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Callable
from src.platform.core.models.order import OrderIntent
from src.platform.core.models.position import Position

TickCallback = Callable[[str, float], None]
UserEventCallback = Callable[[dict], None]
CandleCallback = Callable[[dict], None]

class ExchangeAdapter(ABC):
    name: str

    @abstractmethod
    def subscribe_ticks(self, account: str, symbols: list[str], cb: TickCallback) -> None: ...

    @abstractmethod
    def subscribe_user_stream(self, account: str, cb: UserEventCallback) -> None: ...

    @abstractmethod
    def subscribe_candles(self, account: str, symbols: list[str], intervals: list[str], cb: CandleCallback) -> None: ...

    @abstractmethod
    def fetch_positions(self, account: str) -> list[Position]: ...

    @abstractmethod
    def fetch_funding_snapshot(self, symbols: list[str]) -> list[dict]: ...

    @abstractmethod
    def fetch_open_orders(self, account: str, symbol: str | None = None) -> list[dict]: ...

    @abstractmethod
    def place_order(self, intent: OrderIntent) -> dict: ...

    # --- v9 Market State Layer ---
    @abstractmethod
    def fetch_account_state(self, account: str) -> dict:
        """Return a normalized account state (wallet/equity/available/margin/unrealized)."""
        ...

    @abstractmethod
    def fetch_open_interest_hist(self, account: str, *, symbol: str, interval: str, limit: int = 30) -> list[dict]:
        """Return OI history items (ts, open_interest, open_interest_value)."""
        ...
