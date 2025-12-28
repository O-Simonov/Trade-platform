# src/platform/core/oms/parser.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Tuple

from .events import OrderEvent, TradeEvent


def _utc_from_ms(ms: int | float | None) -> datetime:
    if not ms:
        return datetime.now(tz=timezone.utc)
    return datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc)


def _f(x, default: Optional[float] = None) -> Optional[float]:
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def _i(x, default: Optional[int] = None) -> Optional[int]:
    try:
        if x is None or x == "":
            return default
        return int(x)
    except Exception:
        return default


def parse_binance_user_event(
    event: dict,
    *,
    exchange_id: int,
    account_id: int,
    symbol_id: int,
) -> Optional[Tuple[OrderEvent, Optional[TradeEvent]]]:
    """
    Binance Futures USER DATA stream parser.

    Expected main event:
      e == "ORDER_TRADE_UPDATE"
      payload in event["o"]

    Returns:
      (OrderEvent, TradeEvent|None)
    """
    et = event.get("e")
    if et != "ORDER_TRADE_UPDATE":
        return None

    o = event.get("o") or {}
    symbol = (o.get("s") or "").upper()
    if not symbol:
        return None

    # Order fields
    client_order_id = o.get("c")
    order_id = _i(o.get("i"))
    side = o.get("S")
    order_type = o.get("o")
    status = o.get("X")

    price = _f(o.get("p"))
    avg_price = _f(o.get("ap"))
    orig_qty = _f(o.get("q"))
    executed_qty = _f(o.get("z"))

    reduce_only = o.get("R")
    if reduce_only is None:
        # Some payloads have "rp" or omit entirely; keep None if unknown
        reduce_only = None
    else:
        reduce_only = bool(reduce_only)

    event_ts = _utc_from_ms(o.get("T") or event.get("E"))

    order_evt = OrderEvent(
        exchange_id=int(exchange_id),
        account_id=int(account_id),
        symbol_id=int(symbol_id),
        symbol=symbol,
        order_id=order_id,
        client_order_id=client_order_id,
        side=side,
        order_type=order_type,
        status=status,
        price=price,
        avg_price=avg_price,
        orig_qty=orig_qty,
        executed_qty=executed_qty,
        reduce_only=reduce_only,
        event_ts=event_ts,
    )

    # Trade part: present when "l" (last filled qty) > 0
    last_fill_qty = _f(o.get("l"), 0.0) or 0.0
    trade_evt: Optional[TradeEvent] = None

    if last_fill_qty > 0:
        trade_evt = TradeEvent(
            exchange_id=int(exchange_id),
            account_id=int(account_id),
            symbol_id=int(symbol_id),
            symbol=symbol,
            order_id=order_id,
            client_order_id=client_order_id,
            trade_id=_i(o.get("t")),
            side=side,
            qty=last_fill_qty,
            price=_f(o.get("L")),
            realized_pnl=_f(o.get("rp")),
            commission=_f(o.get("n")),
            commission_asset=o.get("N"),
            is_maker=bool(o.get("m")) if o.get("m") is not None else None,
            trade_ts=_utc_from_ms(o.get("T") or event.get("E")),
        )

    return order_evt, trade_evt
