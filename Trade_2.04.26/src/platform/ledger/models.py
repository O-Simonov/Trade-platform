from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from uuid import UUID

@dataclass(frozen=True)
class Fill:
    trade_id: int
    exchange: str
    symbol: str
    order_id: int
    side: str              # BUY/SELL
    position_side: str     # LONG/SHORT
    price: float
    qty: float
    quote_qty: float
    commission: float = 0.0
    commission_asset: Optional[str] = None
    realized_pnl: float = 0.0
    is_maker: Optional[bool] = None
    executed_at: datetime = None
    strategy_name: Optional[str] = None
    pos_uid: Optional[UUID] = None
