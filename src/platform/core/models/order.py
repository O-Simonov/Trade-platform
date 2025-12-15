from __future__ import annotations
from dataclasses import dataclass
from .enums import Side, OrderType

@dataclass(frozen=True)
class OrderIntent:
    exchange: str
    account: str
    symbol: str
    side: Side
    qty: float
    order_type: OrderType = OrderType.MARKET
    price: float | None = None
    reduce_only: bool = False
    client_order_id: str | None = None
    strategy_id: str | None = None
    pos_uid: str | None = None
    intent_type: str = "ENTRY"
