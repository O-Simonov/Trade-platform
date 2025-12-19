from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Callable, Dict, Optional

from src.platform.core.models.order import OrderIntent
from src.platform.core.models.position import Position
from src.platform.data.storage.base import Storage

TickCallback = Callable[[str, float], None]
UserEventCallback = Callable[[dict], None]
CandleCallback = Callable[[dict], None]


class ExchangeAdapter(ABC):
    """
    Base exchange adapter (interface).
    Concrete exchanges MUST bind runtime context via bind().
    """

    name: str

    # --- runtime-bound context ---
    storage: Optional[Storage] = None
    exchange_id: Optional[int] = None
    symbol_ids: Dict[str, int] = {}

    # ------------------------------------------------------------------
    # RUNTIME BIND
    # ------------------------------------------------------------------

    def bind(
        self,
        *,
        storage: Storage,
        exchange_id: int,
        symbol_ids: dict[str, int],
    ) -> None:
        """
        Bind runtime context (called by TradingInstance).
        """
        self.storage = storage
        self.exchange_id = int(exchange_id)
        self.symbol_ids = dict(symbol_ids)

    # ------------------------------------------------------------------
    # STREAMS
    # ------------------------------------------------------------------

    @abstractmethod
    def subscribe_ticks(
        self,
        account: str,
        symbols: list[str],
        cb: TickCallback,
    ) -> None:
        ...

    @abstractmethod
    def subscribe_user_stream(
        self,
        account: str,
        cb: UserEventCallback,
    ) -> None:
        ...

    @abstractmethod
    def subscribe_candles(
        self,
        account: str,
        symbols: list[str],
        intervals: list[str],
        cb: CandleCallback,
    ) -> None:
        ...

    # ------------------------------------------------------------------
    # FETCH / REST
    # ------------------------------------------------------------------

    @abstractmethod
    def fetch_positions(self, account: str) -> list[Position]:
        ...

    @abstractmethod
    def fetch_open_orders(
        self,
        account: str,
        symbol: str | None = None,
    ) -> list[dict]:
        ...

    @abstractmethod
    def fetch_account_state(self, account: str) -> dict:
        """
        Return normalized account state:
        wallet_balance, equity, available_balance, margin_used, unrealized_pnl
        """
        ...

    @abstractmethod
    def fetch_funding_snapshot(
        self,
        symbols: list[str],
    ) -> list[dict]:
        """
        Funding / premium index snapshot (used by FundingPoller / MarketState).
        """
        ...

    @abstractmethod
    def fetch_open_interest_hist(
        self,
        account: str,
        *,
        symbol: str,
        interval: str,
        limit: int = 30,
    ) -> list[dict]:
        ...

    # ------------------------------------------------------------------
    # TRADING
    # ------------------------------------------------------------------

    @abstractmethod
    def place_order(self, intent: OrderIntent) -> dict:
        ...
