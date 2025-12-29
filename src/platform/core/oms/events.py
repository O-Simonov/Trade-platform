# src/platform/core/oms/events.py
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class OrderStatus(str, Enum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class OrderEvent:
    exchange: str
    account: str
    symbol: str
    order_id: int
    client_order_id: Optional[str]
    status: OrderStatus
    side: str
    price: float
    qty: float
    filled_qty: float
    event_ts: float


@dataclass(frozen=True)
class TradeEvent:
    exchange: str
    account: str
    symbol: str
    trade_id: int
    order_id: int
    side: str
    price: float
    qty: float
    realized_pnl: float
    fee: float
    fee_asset: str
    event_ts: float
