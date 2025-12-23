# src/platform/core/oms/parser.py
from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional, Tuple

from .events import OrderEvent, TradeEvent


def parse_binance_user_event(
    event: dict,
    *,
    exchange_id: int,
    account_id: int,
    symbol_id: int,
) -> Optional[Tuple[OrderEvent, Optional[TradeEvent]]]:

    if event.get("e") != "ORDER_TRADE_UPDATE":
        return None

    o = event.get("o")
    if not isinstance(o, dict):
        return None

    ts_ms = int(event.get("E") or 0)
    ts_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

    order_evt = OrderEvent(
        exchange_id=exchange_id,
        account_id=account_id,
        symbol_id=symbol_id,
        order_id=str(o.get("i") or ""),
        client_order_id=str(o.get("c") or ""),
        side=str(o.get("S") or ""),
        type=str(o.get("o") or ""),
        status=str(o.get("X") or ""),
        qty=float(o.get("q") or 0.0),
        price=float(o.get("ap") or o.get("p") or 0.0),
        reduce_only=bool(o.get("R", False)),
        ts_ms=ts_ms,
        raw=event,
    )

    trade_evt: TradeEvent | None = None

    if o.get("x") == "TRADE":
        trade_evt = TradeEvent(
            exchange_id=exchange_id,
            account_id=account_id,
            symbol_id=symbol_id,
            trade_id=str(o.get("t") or ""),
            order_id=str(o.get("i") or ""),
            client_order_id=str(o.get("c") or ""),
            side=str(o.get("S") or ""),
            price=float(o.get("L") or 0.0),
            qty=float(o.get("l") or 0.0),
            fee=float(o.get("n") or 0.0),
            fee_asset=str(o.get("N") or ""),
            realized_pnl=float(o.get("rp") or 0.0),
            ts=ts_dt,
            raw=event,
        )

    return order_evt, trade_evt
