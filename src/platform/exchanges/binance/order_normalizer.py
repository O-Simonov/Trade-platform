from __future__ import annotations
from math import floor
from decimal import Decimal, ROUND_DOWN


def _floor_to_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return float(
        (Decimal(value) // Decimal(step)) * Decimal(step)
    )


def normalize_order(
    *,
    qty: float,
    price: float | None,
    qty_step: float,
    min_qty: float,
    max_qty: float | None,
    price_tick: float | None,
    min_notional: float | None,
) -> tuple[float, float | None]:
    """
    Normalize order params according to Binance filters.
    Raises ValueError if order is invalid.
    """

    # --- QTY ---
    qty = _floor_to_step(qty, qty_step)

    if qty < min_qty:
        raise ValueError(f"qty {qty} < min_qty {min_qty}")

    if max_qty and qty > max_qty:
        raise ValueError(f"qty {qty} > max_qty {max_qty}")

    # --- PRICE ---
    if price is not None and price_tick:
        price = _floor_to_step(price, price_tick)

    # --- NOTIONAL ---
    if min_notional and price is not None:
        notional = qty * price
        if notional < min_notional:
            raise ValueError(
                f"notional {notional:.4f} < min_notional {min_notional}"
            )

    return qty, price
