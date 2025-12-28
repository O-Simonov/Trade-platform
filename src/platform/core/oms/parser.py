# src/platform/core/oms/parser.py
from __future__ import annotations

from typing import Iterable, Any

from .events import OrderEvent, TradeEvent


def _to_int_ms(v: Any) -> int:
    try:
        if v is None:
            return 0
        # already ms
        if isinstance(v, (int, float)) and v > 10_000_000_000:
            return int(v)
        # seconds
        if isinstance(v, (int, float)):
            return int(float(v) * 1000.0)
        return int(v)
    except Exception:
        return 0


def _f(v: Any, default: float | None = None) -> float | None:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def parse_binance_user_event(
    event: dict,
    *,
    exchange: str,
    account: str,
    exchange_id: int | None = None,
    account_id: int | None = None,
    symbol_ids: dict[str, int] | None = None,
    source: str = "ws_user",
) -> Iterable[object]:
    """Parse Binance (UM Futures) USER_DATA event into OMS domain events.

    Supports:
      - Futures: ORDER_TRADE_UPDATE (most important)
      - Spot: executionReport (best-effort)
    Yields: OrderEvent / TradeEvent
    """
    out: list[object] = []

    ex_id = int(exchange_id or 0)
    acc_id = int(account_id or 0)
    sym_ids = symbol_ids or {}

    et = (event.get("e") or event.get("eventType") or "").upper()

    # ------------------------------------------------------------
    # Futures user stream: ORDER_TRADE_UPDATE
    # https://binance-docs.github.io/apidocs/futures/en/#event-order-update
    # ------------------------------------------------------------
    if et == "ORDER_TRADE_UPDATE":
        o = event.get("o") or {}
        symbol = str(o.get("s") or "").upper()
        if not symbol:
            return out

        symbol_id = int(sym_ids.get(symbol) or 0)

        ts_ms = _to_int_ms(o.get("T") or event.get("E") or event.get("T"))
        order_id = str(o.get("i") or "")
        client_order_id = str(o.get("c") or "") or None

        status = str(o.get("X") or "").upper()
        side = str(o.get("S") or "").upper() or None
        typ = str(o.get("o") or "").upper() or None
        reduce_only = bool(o.get("R") or False)

        qty = _f(o.get("q"), None)
        filled_qty = _f(o.get("z"), None)

        # orig price can be 0 for market orders
        price = _f(o.get("p"), None)
        if price is not None and price == 0.0:
            price = None

        out.append(
            OrderEvent(
                exchange=exchange,
                account=account,
                symbol=symbol,
                exchange_id=ex_id,
                account_id=acc_id,
                symbol_id=symbol_id,
                order_id=order_id,
                client_order_id=client_order_id,
                status=status,
                side=side,
                type=typ,
                reduce_only=reduce_only,
                price=price,
                qty=qty,
                filled_qty=filled_qty,
                source=source,
                ts_ms=ts_ms,
                raw=event,
            )
        )

        # If this update contains a trade fill (last filled qty > 0) and tradeId exists
        last_qty = _f(o.get("l"), 0.0) or 0.0
        trade_id = o.get("t")  # tradeId (futures)
        if trade_id is not None and last_qty and float(last_qty) > 0:
            out.append(
                TradeEvent(
                    exchange=exchange,
                    account=account,
                    symbol=symbol,
                    exchange_id=ex_id,
                    account_id=acc_id,
                    symbol_id=symbol_id,
                    trade_id=str(trade_id),
                    order_id=order_id,
                    side=side,
                    price=float(_f(o.get("L"), 0.0) or 0.0),
                    qty=float(last_qty),
                    realized_pnl=_f(o.get("rp"), 0.0),
                    fee=_f(o.get("n"), 0.0),
                    fee_asset=str(o.get("N") or ""),
                    source=source,
                    ts_ms=ts_ms,
                    raw=event,
                )
            )

        return out

    # ------------------------------------------------------------
    # Spot user stream: executionReport (best-effort)
    # ------------------------------------------------------------
    if et == "EXECUTIONREPORT":
        symbol = str(event.get("s") or "").upper()
        if not symbol:
            return out
        symbol_id = int(sym_ids.get(symbol) or 0)

        ts_ms = _to_int_ms(event.get("T") or event.get("E"))
        order_id = str(event.get("i") or "")
        client_order_id = str(event.get("c") or "") or None

        status = str(event.get("X") or "").upper()
        side = str(event.get("S") or "").upper() or None
        typ = str(event.get("o") or "").upper() or None
        reduce_only = False

        qty = _f(event.get("q"), None)
        filled_qty = _f(event.get("z"), None)
        price = _f(event.get("p"), None)
        if price is not None and price == 0.0:
            price = None

        out.append(
            OrderEvent(
                exchange=exchange,
                account=account,
                symbol=symbol,
                exchange_id=ex_id,
                account_id=acc_id,
                symbol_id=symbol_id,
                order_id=order_id,
                client_order_id=client_order_id,
                status=status,
                side=side,
                type=typ,
                reduce_only=reduce_only,
                price=price,
                qty=qty,
                filled_qty=filled_qty,
                source=source,
                ts_ms=ts_ms,
                raw=event,
            )
        )

        last_qty = _f(event.get("l"), 0.0) or 0.0
        trade_id = event.get("t")
        if trade_id is not None and last_qty and float(last_qty) > 0:
            out.append(
                TradeEvent(
                    exchange=exchange,
                    account=account,
                    symbol=symbol,
                    exchange_id=ex_id,
                    account_id=acc_id,
                    symbol_id=symbol_id,
                    trade_id=str(trade_id),
                    order_id=order_id,
                    side=side,
                    price=float(_f(event.get("L"), 0.0) or 0.0),
                    qty=float(last_qty),
                    realized_pnl=None,
                    fee=_f(event.get("n"), 0.0),
                    fee_asset=str(event.get("N") or ""),
                    source=source,
                    ts_ms=ts_ms,
                    raw=event,
                )
            )

        return out

    return out
