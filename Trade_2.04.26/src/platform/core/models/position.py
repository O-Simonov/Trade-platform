from __future__ import annotations
from dataclasses import dataclass
from .enums import Side

@dataclass
class Position:
    exchange: str
    account: str
    symbol: str
    side: Side
    qty: float

    # Prices
    entry_price: float | None
    avg_price: float | None = None
    exit_price: float | None = None
    mark_price: float | None = None

    # PnL
    unrealized_pnl: float | None = None
    realized_pnl: float | None = None

    # Risk / metadata
    position_value_usdt: float | None = None
    leverage: int | None = None
    status: str = "OPEN"

    # Levels
    stop_loss_1: float | None = None
    stop_loss_2: float | None = None
    take_profit_1: float | None = None
    take_profit_2: float | None = None
    take_profit_3: float | None = None

    # Scaling
    scale_in_count: int = 0

    # Strategy
    strategy_name: str | None = None

    # Timestamps
    opened_at_ts: float | None = None
    closed_at_ts: float | None = None
    updated_at_ts: float = 0.0
