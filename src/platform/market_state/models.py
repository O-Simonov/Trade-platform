from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime

@dataclass(frozen=True)
class AccountBalanceSnapshot:
    ts: datetime
    wallet_balance: float
    equity: float
    available_balance: float
    margin_used: float
    unrealized_pnl: float

@dataclass(frozen=True)
class OpenInterestPoint:
    ts: datetime
    open_interest: float
    open_interest_value: float
