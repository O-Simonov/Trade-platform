# src/platform/core/oms/aggregate.py
from __future__ import annotations

from dataclasses import dataclass

from .events import OrderEvent


_FINAL_STATUSES = {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}


def _u(v: str | None) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    return s.upper() if s else None


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
            exchange_id=int(evt.exchange_id or 0),
            account_id=int(evt.account_id or 0),
            symbol_id=int(evt.symbol_id or 0),

            order_id=str(evt.order_id or ""),
            client_order_id=str(evt.client_order_id) if evt.client_order_id else None,

            status=str(_u(evt.status) or ""),
            side=_u(evt.side),
            type=_u(evt.type),
            reduce_only=bool(evt.reduce_only),

            price=float(evt.price) if evt.price is not None else None,
            qty=float(evt.qty) if evt.qty is not None else None,
            filled_qty=float(evt.filled_qty) if evt.filled_qty is not None else None,

            ts_ms=int(evt.ts_ms or 0),
        )

    # ------------------------------------------------------------
    # FSM / apply
    # ------------------------------------------------------------
    def apply(self, evt: OrderEvent) -> bool:
        """
        Apply OrderEvent to aggregate.
        Returns True if state changed.
        """

        # terminal state is immutable
        if self.is_final:
            return False

        ts = int(evt.ts_ms or 0)
        if ts and ts < int(self.ts_ms or 0):
            return False

        changed = False

        # status
        status = _u(evt.status)
        if status is not None and self.status != status:
            self.status = status
            changed = True

        # side
        side = _u(evt.side)
        if side is not None and self.side != side:
            self.side = side
            changed = True

        # type
        typ = _u(evt.type)
        if typ is not None and self.type != typ:
            self.type = typ
            changed = True

        # numeric fields
        for attr in ("price", "qty", "filled_qty"):
            v = getattr(evt, attr, None)
            if v is None:
                continue

            fv = float(v)

            if attr == "filled_qty":
                old = float(self.filled_qty or 0.0)
                if fv < old:
                    continue  # 🔒 monotonic

            if getattr(self, attr) != fv:
                setattr(self, attr, fv)
                changed = True

        # reduce_only flag
        if evt.reduce_only is not None and self.reduce_only != bool(evt.reduce_only):
            self.reduce_only = bool(evt.reduce_only)
            changed = True

        # update ts_ms last
        if ts and ts > int(self.ts_ms or 0):
            self.ts_ms = ts

        return changed

    # ------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------
    @property
    def is_final(self) -> bool:
        return str(self.status or "").upper() in _FINAL_STATUSES
