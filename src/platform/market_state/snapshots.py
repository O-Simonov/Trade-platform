# src/platform/market_state/snapshots.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class AccountBalanceSnapshot:
    exchange_id: int
    account_id: int
    asset: str
    wallet_balance: float
    available_balance: float
    unrealized_pnl: float
    ts: datetime


@dataclass(slots=True)
class OpenInterestPoint:
    exchange_id: int
    symbol_id: int
    interval: str
    value: float
    ts: datetime
