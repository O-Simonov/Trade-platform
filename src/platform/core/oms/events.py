# src/platform/core/oms/events.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


@dataclass(slots=True)
class OrderEvent:
    """Normalized OMS order event (from WS/REST).

    IMPORTANT: This is a *fact* event coming from exchange. No business logic here.
    """
    exchange: str
    account: str
    symbol: str

    exchange_id: int
    account_id: int
    symbol_id: int

    order_id: str
    client_order_id: str | None

    status: str                 # NEW / PARTIALLY_FILLED / FILLED / CANCELED / REJECTED / EXPIRED / ...
    side: str | None            # BUY / SELL
    type: str | None            # LIMIT / MARKET / STOP / ...
    reduce_only: bool = False

    price: float | None = None
    qty: float | None = None
    filled_qty: float | None = None

    source: str = "ws_user"     # ws_user / rest / etc

    ts_ms: int = 0              # exchange event time in ms (best effort)
    recv_ts: datetime | None = None

    raw: dict[str, Any] | None = None

    def ensure_recv_ts(self) -> datetime:
        if self.recv_ts is None:
            self.recv_ts = _utcnow()
        return self.recv_ts


@dataclass(slots=True)
class TradeEvent:
    """Normalized OMS trade(fill) event."""
    exchange: str
    account: str
    symbol: str

    exchange_id: int
    account_id: int
    symbol_id: int

    trade_id: str
    order_id: str

    side: str | None
    price: float
    qty: float

    realized_pnl: float | None = None
    fee: float | None = None
    fee_asset: str | None = None

    source: str = "ws_user"

    ts_ms: int = 0
    recv_ts: datetime | None = None

    raw: dict[str, Any] | None = None

    def ensure_recv_ts(self) -> datetime:
        if self.recv_ts is None:
            self.recv_ts = _utcnow()
        return self.recv_ts

    def to_row(self) -> dict[str, Any]:
        self.ensure_recv_ts()
        # storage.trades expects TIMESTAMPTZ 'ts'
        ts = datetime.fromtimestamp((self.ts_ms or 0) / 1000.0, tz=timezone.utc)
        return {
            "exchange_id": int(self.exchange_id),
            "account_id": int(self.account_id),
            "trade_id": str(self.trade_id),
            "order_id": str(self.order_id or ""),
            "symbol_id": int(self.symbol_id),
            "strategy_id": "unknown",
            "pos_uid": None,
            "side": str(self.side or ""),
            "price": float(self.price),
            "qty": float(self.qty),
            "fee": float(self.fee or 0.0),
            "fee_asset": str(self.fee_asset or ""),
            "realized_pnl": float(self.realized_pnl or 0.0),
            "ts": ts,
            "source": str(self.source or "ws_user"),
        }
