# src/platform/core/orders/models.py
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(slots=True)
class Order:
    order_id: int
    exchange: str
    account_id: str
    symbol: str

    side: str                  # BUY / SELL
    type: str                  # MARKET / LIMIT / STOP
    status: str                # NEW / OPEN / FILLED / ...

    qty: float
    filled_qty: float = 0.0

    price: Optional[float] = None
    stop_price: Optional[float] = None
    avg_price: Optional[float] = None

    reduce_only: bool = False
    position_side: Optional[str] = None

    client_order_id: Optional[str] = None
    strategy_id: Optional[str] = None

    created_ts: datetime = None
    updated_ts: datetime = None

    last_trade_id: Optional[int] = None
