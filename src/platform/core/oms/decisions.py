# src/platform/core/oms/decisions.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Any

from src.platform.core.models.enums import Side  # LONG / SHORT


@dataclass(slots=True)
class OrderDecision:
    allowed: bool
    mode: str                 # "OPEN" | "REDUCE" | "BLOCKED"
    reduce_only: bool
    qty: float
    reason: Optional[str] = None


def _norm_side(v: Any) -> Optional[Side]:
    if v is None:
        return None
    if isinstance(v, Side):
        return v
    s = str(v).strip().upper()
    if s == "LONG":
        return Side.LONG
    if s == "SHORT":
        return Side.SHORT
    return None


def resolve_order(
    *,
    pos_side: Any,       # Side | "LONG"/"SHORT"/None
    pos_qty: float,      # ALWAYS >= 0
    order_side: Side,    # intent.side (LONG/SHORT)
    order_qty: float,
) -> OrderDecision:
    """
    K6.8 Order ↔ Position FSM (qty>=0 + side).

    IMPORTANT:
      - This function decides OPEN vs REDUCE based on current position side/qty.
      - It does NOT know whether the intent is a CLOSE intent. That check is done in OMS
        (so CLOSE on FLAT does NOT become OPEN by mistake).
    """
    order_qty = float(order_qty or 0.0)
    if order_qty <= 0:
        return OrderDecision(False, "BLOCKED", False, 0.0, "qty<=0")

    pos_qty = float(pos_qty or 0.0)
    ps = _norm_side(pos_side)

    # FLAT
    if pos_qty <= 0.0 or ps is None:
        return OrderDecision(True, "OPEN", False, order_qty)

    # LONG position
    if ps == Side.LONG:
        if order_side == Side.LONG:
            return OrderDecision(True, "OPEN", False, order_qty)

        reduce_qty = min(order_qty, pos_qty)
        if reduce_qty <= 0:
            return OrderDecision(False, "BLOCKED", True, 0.0, "nothing to reduce")
        return OrderDecision(True, "REDUCE", True, reduce_qty)

    # SHORT position
    if ps == Side.SHORT:
        if order_side == Side.SHORT:
            return OrderDecision(True, "OPEN", False, order_qty)

        reduce_qty = min(order_qty, pos_qty)
        if reduce_qty <= 0:
            return OrderDecision(False, "BLOCKED", True, 0.0, "nothing to reduce")
        return OrderDecision(True, "REDUCE", True, reduce_qty)

    return OrderDecision(False, "BLOCKED", False, 0.0, "unreachable")
