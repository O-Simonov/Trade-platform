from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict


@dataclass
class OrderEvent:
    exchange_id: int
    account_id: int
    symbol_id: int

    order_id: str
    client_order_id: str

    side: str
    type: str           # MARKET / LIMIT / ...
    status: str         # NEW / FILLED / CANCELED / ...

    qty: float
    price: float
    reduce_only: bool

    ts_ms: int
    raw: dict

    # --- compatibility: dict-style access ---
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
    raw: dict

    # --- compatibility: dict-style access ---
    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)
