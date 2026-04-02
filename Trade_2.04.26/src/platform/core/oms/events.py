# src/platform/core/oms/events.py
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any
from datetime import datetime, timezone


class OrderStatus(str, Enum):
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"


@dataclass(slots=True)
class OrderEvent:
    exchange: str
    account: str
    symbol: str

    exchange_id: int | None = None
    account_id: int | None = None
    symbol_id: int | None = None

    order_id: str = ""
    client_order_id: str | None = None

    status: str | None = None
    side: str | None = None
    type: str | None = None
    reduce_only: bool = False

    price: float | None = None
    qty: float | None = None
    filled_qty: float | None = None

    ts_ms: int = 0
    source: str | None = None
    raw_json: dict[str, Any] | None = None

    # ✅ ДОБАВИТЬ
    strategy_id: str | None = None
    pos_uid: str | None = None

    def to_row(self) -> dict[str, Any]:
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

            # ✅ ДОБАВИТЬ
            "strategy_id": str(self.strategy_id or "unknown"),
            "pos_uid": (str(self.pos_uid) if self.pos_uid else None),

            "source": str(self.source or "ws_user"),
            "ts_ms": int(self.ts_ms or 0),
            "raw_json": self.raw_json or {},
        }


@dataclass(slots=True)
class TradeEvent:
    exchange: str
    account: str
    symbol: str

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

    # ✅ ДОБАВИТЬ
    strategy_id: str | None = None
    pos_uid: str | None = None

    def to_row(self) -> dict[str, Any]:
        return {
            "exchange_id": int(self.exchange_id or 0),
            "account_id": int(self.account_id or 0),
            "symbol_id": int(self.symbol_id or 0),

            "trade_id": self.trade_id,
            "order_id": self.order_id,
            "side": self.side,

            # ✅ ДОБАВИТЬ (у вас trades.strategy_id NOT NULL)
            "strategy_id": str(self.strategy_id or "unknown"),
            "pos_uid": (str(self.pos_uid) if self.pos_uid else None),

            "price": float(self.price),
            "qty": float(self.qty),
            "fee": float(self.fee),
            "fee_asset": self.fee_asset,
            "realized_pnl": float(self.realized_pnl),

            "ts": datetime.fromtimestamp(self.ts_ms / 1000, tz=timezone.utc),

            "source": self.source or "ws_user",
            "raw_json": self.raw_json or {},
        }
