# src/platform/core/position/order_fsm.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

Side = Literal["BUY", "SELL"]
Mode = Literal["OPEN", "REDUCE", "BLOCKED"]


@dataclass(frozen=True)
class OrderDecision:
    allowed: bool
    mode: Mode
    reduce_only: bool
    qty: float
    reason: Optional[str] = None


def resolve_order(
    *,
    pos_qty: float,
    order_side: Side,
    order_qty: float,
) -> OrderDecision:
    """
    K6.8 Order ↔ Position FSM
    """

    if order_qty <= 0:
        return OrderDecision(False, "BLOCKED", False, 0.0, "qty<=0")

    # ---------------------------
    # FLAT
    # ---------------------------
    if pos_qty == 0:
        return OrderDecision(
            allowed=True,
            mode="OPEN",
            reduce_only=False,
            qty=order_qty,
        )

    # ---------------------------
    # LONG
    # ---------------------------
    if pos_qty > 0:
        if order_side == "BUY":
            return OrderDecision(
                allowed=True,
                mode="OPEN",
                reduce_only=False,
                qty=order_qty,
            )
        else:
            reduce_qty = min(order_qty, abs(pos_qty))
            if reduce_qty <= 0:
                return OrderDecision(False, "BLOCKED", True, 0.0, "nothing to reduce")

            return OrderDecision(
                allowed=True,
                mode="REDUCE",
                reduce_only=True,
                qty=reduce_qty,
            )

    # ---------------------------
    # SHORT
    # ---------------------------
    if pos_qty < 0:
        if order_side == "SELL":
            return OrderDecision(
                allowed=True,
                mode="OPEN",
                reduce_only=False,
                qty=order_qty,
            )
        else:
            reduce_qty = min(order_qty, abs(pos_qty))
            if reduce_qty <= 0:
                return OrderDecision(False, "BLOCKED", True, 0.0, "nothing to reduce")

            return OrderDecision(
                allowed=True,
                mode="REDUCE",
                reduce_only=True,
                qty=reduce_qty,
            )

    return OrderDecision(False, "BLOCKED", False, 0.0, "unreachable")
