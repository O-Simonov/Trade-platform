# src/platform/exchanges/binance/normalize.py
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from src.platform.core.models.position import Position
from src.platform.core.models.enums import Side


def ts_ms_to_dt(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)


def _f(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return float(default)
        return float(v)
    except Exception:
        return float(default)


def norm_position(exchange: str, account: str, raw: dict) -> Position | None:
    """
    Binance Futures /fapi/v2/positionRisk item -> Position

    ВАЖНО:
      - side возвращаем как enum Side (LONG/SHORT)
      - qty всегда abs()
      - если qty == 0 -> None (чтобы fetch_positions отдавал только открытые позиции)
    """
    sym = str(raw.get("symbol", "")).upper()
    if not sym:
        return None

    amt = _f(raw.get("positionAmt"), 0.0)
    if amt == 0.0:
        return None

    side = Side.LONG if amt > 0 else Side.SHORT

    entry = _f(raw.get("entryPrice"), 0.0) or None
    mark = _f(raw.get("markPrice"), 0.0) or None

    # Binance дает updateTime в ms
    upd_ms = raw.get("updateTime")
    if upd_ms is not None:
        updated_at_ts = int(upd_ms) / 1000.0
    else:
        updated_at_ts = time.time()

    # notional/leverage optional
    notional = None
    try:
        n = _f(raw.get("notional"), 0.0)
        notional = n if n != 0.0 else None
    except Exception:
        notional = None

    lev = None
    try:
        lv = raw.get("leverage")
        if lv is not None and str(lv).strip() != "":
            lev = int(float(lv))
    except Exception:
        lev = None

    upnl = _f(raw.get("unRealizedProfit"), 0.0)

    return Position(
        exchange=exchange,
        account=account,
        symbol=sym,
        side=side,
        qty=abs(amt),
        entry_price=entry,
        avg_price=entry,  # для Binance positionRisk entryPrice == avg
        mark_price=mark,
        position_value_usdt=notional,
        leverage=lev,
        unrealized_pnl=upnl,
        updated_at_ts=updated_at_ts,
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
    sym = str(raw.get("s", "")).upper()
    if not sym:
        return None
    if not bool(k.get("x")):
        return None

    interval = str(k.get("i"))
    open_time = ts_ms_to_dt(int(k.get("t") or 0))

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
    sym = str(item.get("symbol", "")).upper()
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
    sym = str(raw.get("symbol", "")).upper()
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
