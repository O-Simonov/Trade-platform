from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Optional
from src.platform.core.models.order import OrderIntent
from src.platform.core.models.position import Position

class Strategy(ABC):
    strategy_id: str
    @abstractmethod
    def on_tick(self, symbol: str, price: float, position: Optional[Position], pos_uid: str | None) -> Optional[OrderIntent]:
        ...
