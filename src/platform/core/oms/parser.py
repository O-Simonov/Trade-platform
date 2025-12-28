# src/platform/core/oms/parser.py
from __future__ import annotations
from typing import Iterable

from .events import OrderEvent, TradeEvent, OrderStatus


def parse_binance_user_event(
    event: dict,
    *,
    exchange: str,
    account: str,
) -> Iterable[object]:
    """
    Parse Binance USER_DATA event into OMS domain events.
    Yields OrderEvent / TradeEvent.
    """

    if event.get("e") != "ORDER_TRADE_UPDATE":
        return []

    o = event.get("o", {})
    ts = event.get("E", 0) / 1000

    out = []

    # --- OrderEvent ---
    out.append(
        OrderEvent(
            exchange=exchange,
            account=account,
            symbol=o["s"],
            order_id=int(o["i"]),
            client_order_id=o.get("c"),
            status=OrderStatus(o["X"]),
            side=o["S"],
            price=float(o.get("p") or 0),
            qty=float(o.get("q") or 0),
            filled_qty=float(o.get("z") or 0),
            event_ts=ts,
        )
    )

    # --- TradeEvent (only if execution happened) ---
    if o.get("l") and float(o["l"]) > 0:
        out.append(
            TradeEvent(
                exchange=exchange,
                account=account,
                symbol=o["s"],
                trade_id=int(o["t"]),
                order_id=int(o["i"]),
                side=o["S"],
                price=float(o["L"]),
                qty=float(o["l"]),
                realized_pnl=float(o.get("rp") or 0),
                fee=float(o.get("n") or 0),
                fee_asset=o.get("N") or "",
                event_ts=ts,
            )
        )

    return out
