# src/platform/exchanges/binance/normalize.py
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from src.platform.core.models.position import Position
from src.platform.core.models.enums import Side


# -------------------------
# time helpers
# -------------------------

def ts_ms_to_dt(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# -------------------------
# parsing helpers
# -------------------------

def _f(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return float(default)
        return float(v)
    except Exception:
        return float(default)


def _i(v: Any, default: int = 0) -> int:
    try:
        if v is None or v == "":
            return int(default)
        return int(float(v))
    except Exception:
        return int(default)


# -------------------------
# positions
# -------------------------

def norm_position(exchange: str, account: str, raw: dict) -> Position | None:
    """
    Binance Futures /fapi/v2/positionRisk item -> Position

    ВАЖНО:
      - side возвращаем как enum Side (LONG/SHORT)
      - qty всегда abs()
      - если qty == 0 -> None (чтобы fetch_positions отдавал только открытые позиции)
    """
    sym = str(raw.get("symbol", "")).upper().strip()
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


# -------------------------
# ticks
# -------------------------

def norm_markprice_tick(raw: dict) -> Tuple[str, float] | None:
    sym = raw.get("s")
    px = raw.get("p")
    if not sym or px is None:
        return None
    return str(sym).upper(), float(px)


# -------------------------
# candles / klines
# -------------------------

def norm_kline_event(
    exchange_id: int,
    symbol_ids: Dict[str, int],
    e: dict,
) -> Optional[dict]:
    """
    Binance WS kline event -> row для upsert в public.candles.

    Возвращаем dict с ПРАВИЛЬНЫМИ колонками таблицы candles:
      exchange_id, symbol_id, interval, open_time,
      open, high, low, close, volume,
      quote_volume, trades,
      taker_buy_base, taker_buy_quote,
      taker_sell_base, taker_sell_quote,
      source, updated_at

    Плюс служебный флаг:
      is_closed (НЕ колонка БД, можно удалить перед upsert)

    ВАЖНО:
      - НЕ фильтруем по k['x'] здесь. Решение писать "открытую" свечу делает коллектор.
      - symbol_id берём из symbol_ids по символу.
    """
    if not isinstance(e, dict):
        return None
    if e.get("e") != "kline":
        return None

    k = e.get("k")
    if not isinstance(k, dict):
        return None

    sym = str(e.get("s") or "").upper().strip()
    if not sym:
        return None

    sid = (symbol_ids or {}).get(sym)
    if sid is None:
        return None

    interval = str(k.get("i") or "").strip()
    if not interval:
        return None

    ot = k.get("t")
    if ot is None:
        return None
    open_time = ts_ms_to_dt(int(ot))

    # base OHLCV
    o = _f(k.get("o"), 0.0)
    h = _f(k.get("h"), 0.0)
    l = _f(k.get("l"), 0.0)
    c = _f(k.get("c"), 0.0)
    v = _f(k.get("v"), 0.0)

    # optional fields in kline payload
    # q: quote volume, n: trades, V/Q: taker buy base/quote
    q_raw = k.get("q")
    n_raw = k.get("n")
    v_buy_raw = k.get("V")
    q_buy_raw = k.get("Q")

    qv = _f(q_raw, 0.0)
    trades = _i(n_raw, 0)

    taker_buy_base = _f(v_buy_raw, 0.0)
    taker_buy_quote = _f(q_buy_raw, 0.0)

    # taker sell = total - taker buy
    taker_sell_base = v - taker_buy_base
    taker_sell_quote = qv - taker_buy_quote

    is_closed = bool(k.get("x"))

    return {
        "exchange_id": int(exchange_id),
        "symbol_id": int(sid),
        "interval": interval,
        "open_time": open_time,

        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "volume": v,

        "quote_volume": (qv if q_raw is not None else None),
        "trades": (trades if n_raw is not None else None),

        "taker_buy_base": (taker_buy_base if v_buy_raw is not None else None),
        "taker_buy_quote": (taker_buy_quote if q_buy_raw is not None else None),

        "taker_sell_base": (taker_sell_base if v_buy_raw is not None else None),
        "taker_sell_quote": (taker_sell_quote if q_buy_raw is not None else None),

        "source": "ws_kline",
        "updated_at": _utc_now(),

        # служебное (не колонка БД)
        "is_closed": is_closed,
    }


# -------------------------
# funding (legacy premiumIndex item)
# -------------------------

def norm_premium_index_item(item: dict) -> dict | None:
    sym = str(item.get("symbol", "")).upper().strip()
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


# -------------------------
# orders
# -------------------------

def norm_open_order(raw: dict) -> dict:
    sym = str(raw.get("symbol", "")).upper().strip()
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
