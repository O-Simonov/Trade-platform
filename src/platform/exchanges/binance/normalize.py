from __future__ import annotations
import time
from datetime import datetime, timezone
from src.platform.core.models.position import Position
from src.platform.core.models.enums import Side

def ts_ms_to_dt(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms/1000.0, tz=timezone.utc)

def norm_position(exchange: str, account: str, raw: dict) -> Position | None:
    sym = str(raw.get("symbol", "")).upper()
    amt = float(raw.get("positionAmt") or 0.0)
    if sym == "" or amt == 0.0:
        return None
    side = Side.LONG if amt > 0 else Side.SHORT
    entry = float(raw.get("entryPrice") or 0.0) or None
    mark = float(raw.get("markPrice") or 0.0) or None
    avg = entry  # Binance positionRisk has entryPrice; avg_price is same fallback
    # Try to use Binance-provided notional/leverage when present
    try:
        notional = float(raw.get("notional") or 0.0) or None
    except Exception:
        notional = None
    try:
        lev = int(float(raw.get("leverage") or 0.0)) or None
    except Exception:
        lev = None
    return Position(
        exchange=exchange,
        account=account,
        symbol=sym,
        side=side,
        qty=abs(amt),
        entry_price=entry,
        avg_price=avg,
        mark_price=mark,
        position_value_usdt=notional,
        leverage=lev,
        unrealized_pnl=float(raw.get("unRealizedProfit") or 0.0),
        updated_at_ts=time.time(),
        opened_at_ts=None,
        closed_at_ts=None,
        strategy_name=None,
    )
def norm_markprice_tick(raw: dict) -> tuple[str, float] | None:
    sym = raw.get("s")
    px = raw.get("p")
    if not sym or px is None:
        return None
    return str(sym).upper(), float(px)

def norm_kline_event(raw: dict) -> dict | None:
    if raw.get("e") != "kline":
        return None
    k = raw.get("k") or {}
    sym = str(raw.get("s","")).upper()
    if not sym:
        return None
    if not bool(k.get("x")):
        return None
    interval = str(k.get("i"))
    open_time = ts_ms_to_dt(int(k.get("t")))
    return {
        "symbol": sym,
        "interval": interval,
        "open_time": open_time,
        "open": float(k.get("o")),
        "high": float(k.get("h")),
        "low": float(k.get("l")),
        "close": float(k.get("c")),
        "volume": float(k.get("v")),
        "source": "ws_kline",
    }

def norm_premium_index_item(item: dict) -> dict | None:
    sym = str(item.get("symbol","")).upper()
    if not sym:
        return None
    nft = item.get("nextFundingTime")
    if nft is None:
        return None
    return {
        "symbol": sym,
        "funding_time": ts_ms_to_dt(int(nft)),
        "funding_rate": float(item.get("lastFundingRate") or 0.0),
        "mark_price": float(item.get("markPrice") or 0.0) if item.get("markPrice") is not None else None,
        "source": "rest_premiumIndex",
    }

def norm_open_order(raw: dict) -> dict:
    # Binance openOrders response fields:
    # symbol, orderId, clientOrderId, side, type, price, origQty, executedQty, status, time
    sym = str(raw.get("symbol","")).upper()
    return {
        "symbol": sym,
        "order_id": str(raw.get("orderId")),
        "client_order_id": raw.get("clientOrderId"),
        "side": raw.get("side"),
        "type": raw.get("type"),
        "price": float(raw.get("price") or 0.0) if raw.get("price") is not None else None,
        "qty": float(raw.get("origQty") or 0.0) if raw.get("origQty") is not None else None,
        "filled_qty": float(raw.get("executedQty") or 0.0) if raw.get("executedQty") is not None else None,
        "status": raw.get("status"),
        "created_at": ts_ms_to_dt(int(raw.get("time") or 0)),
    }

def norm_user_event(evt: dict) -> dict:
    # returns: order_row, trade_row, fill_row (if present)
    et = evt.get("e")
    out = {}
    if et == "ORDER_TRADE_UPDATE":
        o = evt.get("o", {}) or {}
        sym = str(o.get("s", "")).upper()
        order_id = str(o.get("i"))
        client_id = o.get("c")
        out["order_row"] = {
            "order_id": order_id,
            "client_order_id": client_id,
            "symbol": sym,
            "side": str(o.get("S")) if o.get("S") else None,
            "type": str(o.get("o")) if o.get("o") else None,
            "reduce_only": bool(o.get("R")) if o.get("R") is not None else None,
            "price": float(o.get("p") or 0.0) if o.get("p") is not None else None,
            "qty": float(o.get("q") or 0.0) if o.get("q") is not None else None,
            "filled_qty": float(o.get("z") or 0.0) if o.get("z") is not None else None,
            "status": str(o.get("X")) if o.get("X") else None,
            "created_at": ts_ms_to_dt(int(o.get("T") or evt.get("E") or 0)),
            "updated_at": ts_ms_to_dt(int(evt.get("E") or 0)),
            "source": "ws_user",
        }
        trade_id = o.get("t")
        last_qty = float(o.get("l") or 0.0)
        if trade_id is not None and last_qty > 0.0:
            px = float(o.get("L") or 0.0) if o.get("L") is not None else None
            out["trade_row"] = {
                "trade_id": str(trade_id),
                "order_id": order_id,
                "symbol": sym,
                "side": str(o.get("S")) if o.get("S") else None,
                "price": px,
                "qty": last_qty,
                "fee": None,
                "fee_asset": None,
                "realized_pnl": float(o.get("rp") or 0.0) if o.get("rp") is not None else None,
                "ts": ts_ms_to_dt(int(o.get("T") or evt.get("E") or 0)),
                "source": "ws_user",
            }
            # normalized fill (one per execution)
            out["fill_row"] = {
                "fill_uid": f"{order_id}:{trade_id}",
                "symbol": sym,
                "order_id": order_id,
                "trade_id": str(trade_id),
                "client_order_id": client_id,
                "price": px,
                "qty": last_qty,
                "realized_pnl": float(o.get("rp") or 0.0) if o.get("rp") is not None else None,
                "ts": ts_ms_to_dt(int(o.get("T") or evt.get("E") or 0)),
                "source": "ws_user",
            }
    return out
