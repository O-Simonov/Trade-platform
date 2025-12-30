# src/platform/core/strategy/base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable, List, Optional, Any, Dict

from src.platform.core.models.order import OrderIntent
from src.platform.core.models.position import Position


class Strategy(ABC):
    """
    Base Strategy interface.

    Strategy:
      • receives market / position events
      • produces OrderIntent(s)
      • is stateless or minimally stateful
    """
    # --- identity ---
    strategy_id: str = "base"

    def __init__(self, *, exchange: str, account: str):
        self.exchange = exchange
        self.account = account

    # ------------------------------------------------------------------
    # lifecycle hooks (optional)
    # ------------------------------------------------------------------

    def on_start(self) -> None:
        """Called once when instance starts."""
        pass

    def on_stop(self) -> None:
        """Called once when instance stops."""
        pass

    # ------------------------------------------------------------------
    # market data
    # ------------------------------------------------------------------

    def on_tick(self, *, symbol: str, price: float) -> None:
        """Mark price / tick update."""
        pass

    def on_candle(self, *, candle: dict) -> None:
        """OHLCV candle event."""
        pass

    # ------------------------------------------------------------------
    # position updates
    # ------------------------------------------------------------------

    def on_position(self, *, position: Position) -> None:
        """Position update from WS/REST."""
        pass

    # ------------------------------------------------------------------
    # intents
    # ------------------------------------------------------------------

    @abstractmethod
    def get_intents(self) -> List[OrderIntent]:
        """
        Return list of OrderIntent(s) to be processed by OMS.

        IMPORTANT:
          • must return NEW intents only
          • already returned intents must not be repeated
        """
        raise NotImplementedError

    def on_positions_update(self, *, positions: List[Dict[str, Any]]) -> None:
        """
        STEP I: Called by TradingInstance when positions are updated
        (from trades stream rebuild / REST reconcile).
        positions: list of normalized dict snapshots
        """
        return