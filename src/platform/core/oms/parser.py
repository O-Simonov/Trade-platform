# src/platform/core/oms/parser.py
from __future__ import annotations

from typing import Iterable, Any

from .events import OrderEvent, TradeEvent


# ------------------------------------------------------------
# helpers
# ------------------------------------------------------------
def _to_int_ms(v: Any) -> int:
    try:
        if v is None:
            return 0
        # already milliseconds
        if isinstance(v, (int, float)) and v > 10_000_000_000:
            return int(v)
        # seconds -> ms
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


def _norm_side(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip().upper()
    if s in ("BUY", "LONG"):
        return "BUY"
    if s in ("SELL", "SHORT"):
        return "SELL"
    return None


# ------------------------------------------------------------
# main parser
# ------------------------------------------------------------
def parse_binance_user_event(
    event: dict,
    *,
    exchange: str,
    account: str,
    symbol_ids: dict[str, int] | None = None,
    source: str = "ws_user",
) -> Iterable[object]:
    """
    Parse Binance USDⓈ-M Futures USER_DATA events into domain events.

    Produces:
      - OrderEvent (from ORDER_TRADE_UPDATE)
      - TradeEvent (fills) from:
          * ORDER_TRADE_UPDATE (legacy)
          * TRADE_LITE (fast fills stream)
    """
    out: list[object] = []
    sym_ids = symbol_ids or {}

    et = (event.get("e") or event.get("eventType") or "").upper()

    # ============================================================
    # FUTURES — TRADE_LITE (fast fills stream)
    # https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams/Event-Trade-Lite
    # ============================================================
    if et == "TRADE_LITE":
        symbol = str(event.get("s") or "").upper()
        if not symbol:
            return out

        symbol_id = int(sym_ids.get(symbol) or 0)

        ts_ms = _to_int_ms(event.get("T") or event.get("E"))
        order_id = str(event.get("i") or "")
        client_order_id = str(event.get("c") or "") or None

        side = _norm_side(event.get("S"))  # BUY/SELL
        last_qty = float(_f(event.get("l"), 0.0) or 0.0)
        trade_id = event.get("t")

        # TRADE_LITE = только execution, так что OrderEvent тут НЕ делаем
        if trade_id is not None and last_qty > 0.0:
            out.append(
                TradeEvent(
                    exchange=exchange,
                    account=account,
                    symbol=symbol,

                    symbol_id=symbol_id,
                    trade_id=str(trade_id),
                    order_id=order_id,

                    side=side,
                    price=float(_f(event.get("L"), 0.0) or 0.0),
                    qty=float(last_qty),
                    realized_pnl=0.0,
                    fee=0.0,
                    fee_asset="",

                    ts_ms=ts_ms,
                    source=source,
                    raw_json=event,
                )
            )
        return out

    # ============================================================
    # FUTURES — ORDER_TRADE_UPDATE (order updates + sometimes fills)
    # https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams/Event-Order-Update
    # ============================================================
    if et == "ORDER_TRADE_UPDATE":
        o = event.get("o") or {}

        symbol = str(o.get("s") or "").upper()
        if not symbol:
            return out

        symbol_id = int(sym_ids.get(symbol) or 0)

        ts_ms = _to_int_ms(o.get("T") or event.get("E"))

        order_id = str(o.get("i") or "")
        client_order_id = str(o.get("c") or "") or None

        status = str(o.get("X") or "").upper() or None
        side = _norm_side(o.get("S"))  # BUY/SELL
        typ = str(o.get("o") or "").upper() or None
        reduce_only = bool(o.get("R") or False)

        qty = _f(o.get("q"), None)
        filled_qty = _f(o.get("z"), None)

        price = _f(o.get("p"), None)
        if price == 0.0:
            price = None

        out.append(
            OrderEvent(
                exchange=exchange,
                account=account,
                symbol=symbol,

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

                ts_ms=ts_ms,
                source=source,
                raw_json=event,
            )
        )

        # fill (legacy)
        last_qty = float(_f(o.get("l"), 0.0) or 0.0)
        trade_id = o.get("t")

        if trade_id is not None and last_qty > 0.0:
            out.append(
                TradeEvent(
                    exchange=exchange,
                    account=account,
                    symbol=symbol,

                    symbol_id=symbol_id,
                    trade_id=str(trade_id),
                    order_id=order_id,

                    side=side,
                    price=float(_f(o.get("L"), 0.0) or 0.0),
                    qty=float(last_qty),
                    realized_pnl=float(_f(o.get("rp"), 0.0) or 0.0),
                    fee=float(_f(o.get("n"), 0.0) or 0.0),
                    fee_asset=str(o.get("N") or ""),

                    ts_ms=ts_ms,
                    source=source,
                    raw_json=event,
                )
            )

        return out

    return out
