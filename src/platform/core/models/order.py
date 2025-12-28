# src/platform/core/models/order.py
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from uuid import uuid4

from src.platform.core.models.enums import Side


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class OrderIntentType(str, Enum):
    """
    High-level semantic type of intent.
    Used by OMS / Risk / logs.
    """
    OPEN = "OPEN"
    CLOSE = "CLOSE"
    REDUCE = "REDUCE"
    INTERNAL = "INTERNAL"   # OMS / reconcile / cleanup


@dataclass(slots=True)
class OrderIntent:
    """
    OrderIntent — canonical OMS order description.

    This object is:
      • produced by Strategy
      • validated by Risk
      • normalized by OMS (symbol_filters)
      • submitted by Exchange
      • reconciled with WS/REST
    """

    # --- identity ---
    symbol: str
    side: Side
    qty: float

    # --- execution ---
    order_type: OrderType = OrderType.MARKET
    price: Optional[float] = None
    reduce_only: bool = False

    # --- context ---
    account: str = ""
    exchange: str = ""

    # --- lifecycle ---
    intent_type: OrderIntentType = OrderIntentType.OPEN

    # --- position binding ---
    pos_uid: Optional[str] = None

    # --- idempotency ---
    client_order_id: str = field(default_factory=lambda: uuid4().hex)

    # --- metadata ---
    comment: Optional[str] = None
    created_ts: float = field(default_factory=lambda: __import__("time").time())

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def is_open(self) -> bool:
        return self.intent_type == OrderIntentType.OPEN

    def is_close(self) -> bool:
        return self.intent_type in (OrderIntentType.CLOSE, OrderIntentType.REDUCE)

    def clone_with(
        self,
        *,
        qty: Optional[float] = None,
        price: Optional[float] = None,
        reduce_only: Optional[bool] = None,
    ) -> "OrderIntent":
        """
        Used by OMS after normalization (symbol_filters).
        """
        return OrderIntent(
            symbol=self.symbol,
            side=self.side,
            qty=qty if qty is not None else self.qty,
            order_type=self.order_type,
            price=price if price is not None else self.price,
            reduce_only=reduce_only if reduce_only is not None else self.reduce_only,
            account=self.account,
            exchange=self.exchange,
            intent_type=self.intent_type,
            pos_uid=self.pos_uid,
            client_order_id=self.client_order_id,
            comment=self.comment,
        )

    def __repr__(self) -> str:
        return (
            f"OrderIntent("
            f"{self.symbol} {self.side.value} "
            f"qty={self.qty} "
            f"type={self.order_type.value} "
            f"reduce={self.reduce_only} "
            f"intent={self.intent_type.value} "
            f"cid={self.client_order_id}"
            f")"
        )
