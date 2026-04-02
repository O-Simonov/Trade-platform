# src/platform/market_state/models.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict
from datetime import datetime


@dataclass(slots=True)
class PositionState:
    symbol: str
    qty: float
    entry_price: float
    unrealized_pnl: float
    leverage: int
    side: str  # LONG / SHORT / FLAT


@dataclass(slots=True)
class AccountState:
    wallet_balance: float
    available_balance: float
    margin_balance: float
    positions: Dict[str, PositionState]
    ts_ms: int


