# src/platform/screeners/base.py
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol, Iterable


@dataclass(slots=True)
class Signal:
    """Universal signal produced by any screener."""

    exchange_id: int
    symbol_id: int
    screener_id: int

    timeframe: str
    signal_ts: datetime
    signal_day: Any  # date (from datetime.date) - kept Any to avoid circular imports

    day_seq: int

    side: str  # 'BUY' | 'SELL'
    status: str = "NEW"

    entry_price: float | None = None
    exit_price: float | None = None
    stop_loss: float | None = None
    take_profit: float | None = None

    confidence: float | None = None
    score: float | None = None

    reason: str | None = None
    context: dict = field(default_factory=dict)
    source: str = "screener"

    def to_row(self) -> dict:
        return {
            "exchange_id": int(self.exchange_id),
            "symbol_id": int(self.symbol_id),
            "screener_id": int(self.screener_id),
            "timeframe": str(self.timeframe),
            "signal_ts": self.signal_ts,
            "signal_day": self.signal_day,
            "day_seq": int(self.day_seq),
            "side": str(self.side).upper(),
            "status": str(self.status).upper(),
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit,
            "confidence": self.confidence,
            "score": self.score,
            "reason": self.reason,
            "context": self.context,
            "source": self.source,
        }


class Screener(Protocol):
    """Base interface for any screener."""

    name: str
    version: str

    def scan(self, *, now: datetime, storage: Any, exchange_id: int, symbol_ids: Iterable[int]) -> list[Signal]:
        ...
