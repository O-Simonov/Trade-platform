# src/platform/screeners/base.py
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Any, Dict, Optional


@dataclass
class Signal:
    # обязательное (что выдаёт скринер)
    symbol_id: int
    symbol: str
    timeframe: str
    signal_ts: datetime

    side: str  # "BUY" | "SELL"

    entry_price: float
    exit_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None

    confidence: float = 0.5
    score: float = 0.0
    reason: str = ""

    context: Dict[str, Any] = field(default_factory=dict)

    # ✅ необязательное (это будет заполнять runner перед записью в БД)
    exchange_id: Optional[int] = None
    screener_id: Optional[int] = None
    signal_day: Optional[date] = None
    day_seq: Optional[int] = None
    status: str = "NEW"
    source: str = "screener"
