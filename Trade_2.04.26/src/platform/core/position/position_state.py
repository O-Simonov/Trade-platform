# src/platform/core/position/position_state.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(slots=True)
class PositionState:
    exchange_id: int
    account_id: int
    symbol_id: int

    pos_uid: str
    qty: float = 0.0
    entry_price: float = 0.0

    realized_pnl: float = 0.0          # 🔵 наш PnL
    unrealized_pnl: float = 0.0        # ✅ floating PnL
    exchange_realized_pnl: float = 0.0 # 🟠 Binance realized PnL
    fees: float = 0.0

    mark_price: float = 0.0            # ✅ markPrice

    opened_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None
    status: str = "FLAT"

    last_trade_id: Optional[str] = None
    last_ts: Optional[datetime] = None

    # ✅ ОБЯЗАТЕЛЬНО для UPSERT/WS тик-обновлений
    updated_at: Optional[datetime] = None

    # (опционально) кто обновил
    source: Optional[str] = None

    @property
    def side(self) -> str:
        if self.qty > 0:
            return "LONG"
        if self.qty < 0:
            return "SHORT"
        return "FLAT"

    @property
    def is_open(self) -> bool:
        return self.status == "OPEN"

    @property
    def is_closed(self) -> bool:
        return self.status == "CLOSED"
