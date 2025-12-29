# src/platform/core/oms/aggregate.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

from .events import OrderEvent


_FINAL_STATUSES = {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}


@dataclass(slots=True)
class OrderAggregate:
    exchange_id: int
    account_id: int
    symbol_id: int

    order_id: str
    client_order_id: str | None

    status: str
    side: str | None
    type: str | None
    reduce_only: bool

    price: float | None
    qty: float | None
    filled_qty: float | None

    ts_ms: int

    # ------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------
    @classmethod
    def from_event(cls, evt: OrderEvent) -> "OrderAggregate":
        """
        Create new aggregate from first OrderEvent.
        """
        return cls(
            exchange_id=int(evt.exchange_id),
            account_id=int(evt.account_id),
            symbol_id=int(evt.symbol_id),
            order_id=str(evt.order_id),
            client_order_id=evt.client_order_id,
            status=str(evt.status),
            side=evt.side,
            type=evt.type,
            reduce_only=bool(evt.reduce_only),
            price=evt.price,
            qty=evt.qty,
            filled_qty=evt.filled_qty,
            ts_ms=int(evt.ts_ms),
        )

    # ------------------------------------------------------------
    # FSM / apply
    # ------------------------------------------------------------
    def apply(self, evt: OrderEvent) -> bool:
        """
        Apply OrderEvent to aggregate.
        Returns True if state changed.
        """

        # ignore older / duplicate events
        if evt.ts_ms is None or int(evt.ts_ms) < self.ts_ms:
            return False

        changed = False

        # last-write-wins fields
        for attr in ("status", "side", "type"):
            v = getattr(evt, attr, None)
            if v is not None and getattr(self, attr) != v:
                setattr(self, attr, v)
                changed = True

        # numeric fields
        for attr in ("price", "qty", "filled_qty"):
            v = getattr(evt, attr, None)
            if v is not None and getattr(self, attr) != v:
                setattr(self, attr, v)
                changed = True

        # flags
        if evt.reduce_only is not None and self.reduce_only != bool(evt.reduce_only):
            self.reduce_only = bool(evt.reduce_only)
            changed = True

        if changed:
            self.ts_ms = int(evt.ts_ms)

        return changed

    # ------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------
    @property
    def is_final(self) -> bool:
        return self.status in _FINAL_STATUSES
