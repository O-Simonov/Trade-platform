# src/platform/core/oms/preflight.py
from __future__ import annotations

from dataclasses import replace
from typing import Optional

from src.platform.core.models.order import OrderIntent


def normalize_qty(qty: float, *, step: float, min_qty: float | None) -> float:
    if step <= 0:
        return 0.0
    q = float(qty)
    # floor to step
    q = (q // step) * step
    if min_qty is not None and q < float(min_qty):
        return 0.0
    return float(q)


def normalize_price(price: float, *, tick: float) -> float:
    if tick <= 0:
        return float(price)
    p = float(price)
    p = (p // tick) * tick
    return float(p)


def preflight_intent(
    intent: OrderIntent,
    *,
    filters: dict,
    last_price: float,
    logger=None,
) -> Optional[OrderIntent]:
    """
    Normalize intent using symbol_filters and check minNotional.

    Returns normalized OrderIntent or None (skip).
    """
    try:
        qty_step = float(filters["qty_step"])
        price_tick = float(filters["price_tick"])
        min_qty = float(filters["min_qty"]) if filters.get("min_qty") is not None else None
        min_notional = float(filters["min_notional"]) if filters.get("min_notional") is not None else None
    except Exception:
        if logger:
            logger.warning("[OMS][PREFLIGHT] bad filters -> skip")
        return None

    qty_norm = normalize_qty(float(intent.qty), step=qty_step, min_qty=min_qty)
    if qty_norm <= 0:
        if logger:
            logger.warning("[OMS][PREFLIGHT] qty too small after normalize: %s raw=%s", intent.symbol, intent.qty)
        return None

    price_norm = intent.price
    if price_norm is not None:
        price_norm = normalize_price(float(price_norm), tick=price_tick)

    # MARKET: use last_price for notional check
    px_for_notional = float(price_norm) if price_norm is not None else float(last_price)
    notional = qty_norm * px_for_notional

    if min_notional is not None and notional < min_notional:
        if logger:
            logger.warning(
                "[OMS][PREFLIGHT] notional too small: %s notional=%s min=%s",
                intent.symbol, notional, min_notional,
            )
        return None

    return replace(intent, qty=qty_norm, price=price_norm)
