# src/platform/core/oms/events.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True, slots=True)
class OrderEvent:
    exchange_id: int
    account_id: int
    symbol_id: int

    symbol: str
    order_id: Optional[int]
    client_order_id: Optional[str]

    side: Optional[str]          # "BUY"/"SELL"
    order_type: Optional[str]    # "MARKET"/"LIMIT"/...
    status: Optional[str]        # "NEW"/"FILLED"/"CANCELED"/...

    price: Optional[float]
    avg_price: Optional[float]
    orig_qty: Optional[float]
    executed_qty: Optional[float]

    reduce_only: Optional[bool]
    event_ts: datetime


@dataclass(frozen=True, slots=True)
class TradeEvent:
    exchange_id: int
    account_id: int
    symbol_id: int

    symbol: str
    order_id: Optional[int]
    client_order_id: Optional[str]
    trade_id: Optional[int]

    side: Optional[str]          # "BUY"/"SELL"
    qty: Optional[float]
    price: Optional[float]

    realized_pnl: Optional[float]
    commission: Optional[float]
    commission_asset: Optional[str]

    is_maker: Optional[bool]
    trade_ts: datetime
