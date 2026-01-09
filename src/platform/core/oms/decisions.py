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
    pos_side: Any,
    pos_qty: float,
    order_side: Any,
    order_qty: float,
) -> OrderDecision:

    try:
        order_qty = float(order_qty)
    except Exception:
        order_qty = 0.0

    if not (order_qty > 0.0):
        return OrderDecision(False, "BLOCKED", False, 0.0, "qty<=0")

    pos_qty = float(pos_qty or 0.0)

    ps = _norm_side(pos_side)
    os = _norm_side(order_side)

    if os is None:
        return OrderDecision(False, "BLOCKED", False, 0.0, "invalid order_side")

    # FLAT
    if pos_qty <= 0.0 or ps is None:
        return OrderDecision(True, "OPEN", False, order_qty)

    # LONG
    if ps == Side.LONG:
        if os == Side.LONG:
            return OrderDecision(True, "OPEN", False, order_qty)

        reduce_qty = min(order_qty, pos_qty)
        if reduce_qty <= 0:
            return OrderDecision(False, "BLOCKED", True, 0.0, "nothing to reduce")
        return OrderDecision(True, "REDUCE", True, reduce_qty)

    # SHORT
    if ps == Side.SHORT:
        if os == Side.SHORT:
            return OrderDecision(True, "OPEN", False, order_qty)

        reduce_qty = min(order_qty, pos_qty)
        if reduce_qty <= 0:
            return OrderDecision(False, "BLOCKED", True, 0.0, "nothing to reduce")
        return OrderDecision(True, "REDUCE", True, reduce_qty)

    return OrderDecision(False, "BLOCKED", False, 0.0, "unreachable")
