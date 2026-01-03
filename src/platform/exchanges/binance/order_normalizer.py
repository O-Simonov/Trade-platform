# src/platform/exchanges/binance/order_normalizer.py
from __future__ import annotations

from decimal import Decimal, ROUND_DOWN
from typing import Optional, Tuple


def _d(x: float | str | int) -> Decimal:
    """
    Safe Decimal conversion.
    IMPORTANT: use str(float) to avoid binary float artifacts.
    """
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


def _floor_to_step(value: float, step: float) -> float:
    """
    Floors value down to the nearest multiple of step.
    Uses Decimal(str(...)) to avoid float artifacts.
    """
    step_f = float(step or 0.0)
    if step_f <= 0:
        return float(value)

    v = _d(value)
    s = _d(step_f)

    if s <= 0:
        return float(value)

    # floor to multiple of step: floor(v/s) * s
    q = (v / s).to_integral_value(rounding=ROUND_DOWN)
    out = q * s

    # normalize -0.0
    if out == 0:
        out = Decimal("0")

    return float(out)


def normalize_order(
    *,
    qty: float,
    price: Optional[float],
    qty_step: float,
    min_qty: float,
    max_qty: Optional[float],
    price_tick: Optional[float],
    min_notional: Optional[float],
) -> Tuple[float, Optional[float]]:
    """
    Normalize order params according to Binance filters.

    Rules:
      - qty floored to qty_step
      - qty must be >= min_qty
      - qty must be <= max_qty if max_qty is provided
      - price floored to price_tick if provided
      - min_notional checked only if price is provided (LIMIT orders)
    Raises ValueError if invalid.
    """

    qty = float(qty or 0.0)
    if qty <= 0:
        raise ValueError(f"qty {qty} <= 0")

    qty_step = float(qty_step or 0.0)
    min_qty = float(min_qty or 0.0)
    max_qty = float(max_qty) if max_qty is not None else None
    price_tick = float(price_tick) if price_tick is not None else None
    min_notional = float(min_notional) if min_notional is not None else None

    # --- QTY ---
    qty = _floor_to_step(qty, qty_step)

    if qty < min_qty:
        raise ValueError(f"qty {qty} < min_qty {min_qty}")

    if max_qty is not None and 0 < max_qty < qty:
        raise ValueError(f"qty {qty} > max_qty {max_qty}")

    # --- PRICE ---
    if price is not None:
        p = float(price or 0.0)
        if p <= 0:
            raise ValueError(f"price {p} <= 0")
        if price_tick is not None and price_tick > 0:
            p = _floor_to_step(p, price_tick)
        price = p

    # --- NOTIONAL ---
    # Binance minNotional is meaningful when we know the price (LIMIT).
    if min_notional is not None and min_notional > 0 and price is not None:
        notional = qty * float(price)
        if notional < min_notional:
            raise ValueError(f"notional {notional:.8f} < min_notional {min_notional}")

    return qty, price
