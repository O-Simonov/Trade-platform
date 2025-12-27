# src/platform/core/oms/events.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class OrderEvent:
    exchange_id: int
    account_id: int
    symbol_id: int

    order_id: str
    client_order_id: str

    side: str
    type: str
    status: str

    qty: float
    price: Optional[float]
    reduce_only: bool

    ts_ms: int
    raw: Dict[str, Any]

    # dict-style access
    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "exchange_id": self.exchange_id,
            "account_id": self.account_id,
            "symbol_id": self.symbol_id,
            "order_id": self.order_id,
            "client_order_id": self.client_order_id,
            "side": self.side,
            "type": self.type,
            "status": self.status,
            "qty": self.qty,
            "price": self.price,
            "reduce_only": self.reduce_only,
            "ts_ms": self.ts_ms,
            "raw": self.raw,
        }

    # 🔥 КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ
    def to_row(self) -> Dict[str, Any]:
        return self.to_dict()


@dataclass
class TradeEvent:
    exchange_id: int
    account_id: int
    symbol_id: int

    trade_id: int
    order_id: int
    client_order_id: str

    side: str
    price: float
    qty: float
    fee: float
    fee_asset: str
    realized_pnl: float

    ts: datetime
    raw: Dict[str, Any]

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "exchange_id": self.exchange_id,
            "account_id": self.account_id,
            "symbol_id": self.symbol_id,
            "trade_id": self.trade_id,
            "order_id": self.order_id,
            "client_order_id": self.client_order_id,
            "side": self.side,
            "price": self.price,
            "qty": self.qty,
            "fee": self.fee,
            "fee_asset": self.fee_asset,
            "realized_pnl": self.realized_pnl,
            "ts": self.ts,
            "raw": self.raw,
        }

    # 🔥 КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ
    def to_row(self) -> Dict[str, Any]:
        return self.to_dict()
