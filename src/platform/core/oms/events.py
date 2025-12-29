# src/platform/core/oms/events.py
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any
from datetime import datetime, timezone


# ------------------------------------------------------------
# Order status enum (optional, for future FSM strictness)
# ------------------------------------------------------------
class OrderStatus(str, Enum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"


# ------------------------------------------------------------
# OrderEvent
# ------------------------------------------------------------
@dataclass(slots=True)
class OrderEvent:
    """
    Domain order event produced by WS / REST parser.

    Routing fields (exchange/account/symbol) are strings.
    Numeric ids are injected later by Instance / OMS.
    """

    # routing (from WS)
    exchange: str
    account: str
    symbol: str

    # numeric ids (optional, injected later)
    exchange_id: int | None = None
    account_id: int | None = None
    symbol_id: int | None = None

    # order identifiers
    order_id: str = ""
    client_order_id: str | None = None

    # order state
    status: str | None = None
    side: str | None = None          # BUY / SELL
    type: str | None = None          # MARKET / LIMIT / ...
    reduce_only: bool = False

    # quantities / prices
    price: float | None = None
    qty: float | None = None
    filled_qty: float | None = None

    # meta
    ts_ms: int = 0                   # event timestamp (ms)
    source: str | None = None        # ws_user / rest / reconcile
    raw_json: dict[str, Any] | None = None

    # --------------------------------------------------------
    def to_row(self) -> dict[str, Any]:
        """Serialize for orders_events table."""
        return {
            "exchange_id": int(self.exchange_id or 0),
            "account_id": int(self.account_id or 0),
            "symbol_id": int(self.symbol_id or 0),

            "order_id": str(self.order_id or ""),
            "client_order_id": str(self.client_order_id or ""),

            "status": str(self.status or ""),
            "side": str(self.side or ""),
            "type": str(self.type or ""),
            "reduce_only": bool(self.reduce_only),

            "price": float(self.price) if self.price is not None else None,
            "qty": float(self.qty) if self.qty is not None else None,
            "filled_qty": float(self.filled_qty) if self.filled_qty is not None else None,

            "source": str(self.source or "ws_user"),
            "ts_ms": int(self.ts_ms or 0),
            "raw_json": self.raw_json or {},
        }


# ------------------------------------------------------------
# TradeEvent (fills)
# ------------------------------------------------------------
@dataclass(slots=True)
class TradeEvent:
    """
    Trade (fill) event.
    """

    # routing
    exchange: str
    account: str
    symbol: str

    # numeric ids (injected later)
    exchange_id: int | None = None
    account_id: int | None = None
    symbol_id: int | None = None

    trade_id: str = ""
    order_id: str = ""

    side: str | None = None
    price: float = 0.0
    qty: float = 0.0
    realized_pnl: float = 0.0
    fee: float = 0.0
    fee_asset: str = ""

    ts_ms: int = 0
    source: str | None = None
    raw_json: dict[str, Any] | None = None

    # --------------------------------------------------------
    def to_row(self) -> dict[str, Any]:
        """Serialize for trades table."""
        return {
            "exchange_id": int(self.exchange_id or 0),
            "account_id": int(self.account_id or 0),
            "symbol_id": int(self.symbol_id or 0),

            "trade_id": self.trade_id,
            "order_id": self.order_id,
            "side": self.side,

            "price": float(self.price),
            "qty": float(self.qty),
            "fee": float(self.fee),
            "fee_asset": self.fee_asset,
            "realized_pnl": float(self.realized_pnl),

            # TIMESTAMPTZ
            "ts": datetime.fromtimestamp(self.ts_ms / 1000, tz=timezone.utc),

            "source": self.source or "ws_user",
            "raw_json": self.raw_json or {},
        }
