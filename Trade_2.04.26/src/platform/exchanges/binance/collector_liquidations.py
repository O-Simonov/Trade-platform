from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger(__name__)

WS_URL = "wss://fstream.binance.com/ws/!forceOrder@arr"


def _utc_dt_from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


def _floor_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


@dataclass
class _Agg1m:
    long_notional: float = 0.0
    short_notional: float = 0.0
    long_qty: float = 0.0
    short_qty: float = 0.0
    events: int = 0


def _parse_force_order_event(e: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Binance forceOrder event:
      {
        "e":"forceOrder",
        "E": eventTime,
        "o": {
          "s": "BTCUSDT",
          "S": "SELL" / "BUY",
          "o": "LIMIT",
          "f": "GTC",
          "q": "0.1",
          "p": "43000",
          "ap":"42980",
          "X":"FILLED",
          "l":"0.1",
          ...
        }
      }
    """
    try:
        o = e.get("o") or {}
        sym = str(o.get("s") or "").upper().strip()
        if not sym:
            return None

        event_ms = int(e.get("E") or 0)
        if event_ms <= 0:
            return None

        side = str(o.get("S") or "").upper().strip()  # BUY/SELL
        price = float(o.get("p") or 0.0)
        qty = float(o.get("q") or 0.0)

        filled_qty = None
        try:
            filled_qty = float(o.get("l")) if o.get("l") is not None else None
        except Exception:
            filled_qty = None

        avg_price = None
        try:
            avg_price = float(o.get("ap")) if o.get("ap") is not None else None
        except Exception:
            avg_price = None

        status = str(o.get("X") or "") if o.get("X") is not None else None
        order_type = str(o.get("o") or "") if o.get("o") is not None else None
        tif = str(o.get("f") or "") if o.get("f") is not None else None

        # notional: лучше использовать filled_qty если есть
        qty_for_notional = filled_qty if (filled_qty is not None and filled_qty > 0) else qty
        notional = float(price) * float(qty_for_notional) if price > 0 and qty_for_notional > 0 else None

        # Интерпретация: SELL => ликвидация long, BUY => ликвидация short
        is_long_liq = None
        if side == "SELL":
            is_long_liq = True
        elif side == "BUY":
            is_long_liq = False

        return dict(
            symbol=sym,
            event_ms=event_ms,
            ts=_utc_dt_from_ms(event_ms),
            side=side,
            price=price,
            qty=qty,
            filled_qty=filled_qty,
            avg_price=avg_price,
            status=status,
            order_type=order_type,
            time_in_force=tif,
            notional=notional,
            is_long_liq=is_long_liq,
            raw_json=e,
        )
    except Exception:
        return None


def _ws_loop(
    *,
    storage,
    exchange_id: int,
    symbol_ids: Dict[str, int],
    stop_event: threading.Event,
    flush_sec: float,
) -> None:
    """
    WS читает поток ликвидаций и пишет:
      - liquidation_events (raw)
      - liquidation_1m (aggregate) батчами
    """
    # lazy import (чтобы проект не падал, если либы нет)
    ws_client = None
    use_websocket_client = False

    try:
        import websocket  # type: ignore
        ws_client = websocket
        use_websocket_client = True
    except Exception:
        ws_client = None

    if not use_websocket_client:
        # fallback на websockets (asyncio в отдельном треде)
        try:
            import asyncio
            import websockets  # type: ignore
        except Exception as e:
            log.error("[Liquidations] no websocket libs found: %s", e)
            return

        async def _async_run():
            backoff = 1.0
            while not stop_event.is_set():
                try:
                    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                        log.info("[Liquidations] WS connected %s", WS_URL)
                        backoff = 1.0

                        buf_rows = []
                        agg: Dict[Tuple[int, datetime], _Agg1m] = {}
                        last_flush = time.time()

                        while not stop_event.is_set():
                            msg = await ws.recv()
                            data = json.loads(msg)

                            # приходит список событий
                            events = data if isinstance(data, list) else [data]
                            for e in events:
                                pe = _parse_force_order_event(e)
                                if not pe:
                                    continue
                                sid = symbol_ids.get(pe["symbol"])
                                if not sid:
                                    continue

                                # raw row
                                buf_rows.append(
                                    dict(
                                        exchange_id=int(exchange_id),
                                        symbol_id=int(sid),
                                        ts=pe["ts"],
                                        event_ms=int(pe["event_ms"]),
                                        side=pe["side"],
                                        price=float(pe["price"]),
                                        qty=float(pe["qty"]),
                                        filled_qty=pe["filled_qty"],
                                        avg_price=pe["avg_price"],
                                        status=pe["status"],
                                        order_type=pe["order_type"],
                                        time_in_force=pe["time_in_force"],
                                        notional=pe["notional"],
                                        is_long_liq=pe["is_long_liq"],
                                        raw_json=pe["raw_json"],
                                    )
                                )

                                # aggregate 1m
                                bucket = _floor_minute(pe["ts"])
                                key = (int(sid), bucket)
                                a = agg.get(key) or _Agg1m()
                                notional = float(pe["notional"] or 0.0)
                                qtyv = float(pe["filled_qty"] or pe["qty"] or 0.0)

                                if pe["side"] == "SELL":
                                    a.long_notional += notional
                                    a.long_qty += qtyv
                                elif pe["side"] == "BUY":
                                    a.short_notional += notional
                                    a.short_qty += qtyv
                                a.events += 1
                                agg[key] = a

                            if time.time() - last_flush >= float(flush_sec):
                                _flush(storage, exchange_id, buf_rows, agg)
                                buf_rows.clear()
                                agg.clear()
                                last_flush = time.time()

                except Exception as e:
                    log.warning("[Liquidations] WS error: %s", e, exc_info=True)
                    # перед повтором — спим с backoff
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2.0, 30.0)

            # финальный flush
            # (здесь нет доступа к буферам, но это ок — мы flush'им регулярно)

        asyncio.run(_async_run())
        return

    # --- websocket-client branch (sync) ---
    import websocket  # type: ignore

    buf_rows = []
    agg: Dict[Tuple[int, datetime], _Agg1m] = {}
    last_flush = time.time()

    def on_message(_ws, message: str):
        nonlocal last_flush
        try:
            data = json.loads(message)
        except Exception:
            return

        events = data if isinstance(data, list) else [data]
        for e in events:
            pe = _parse_force_order_event(e)
            if not pe:
                continue
            sid = symbol_ids.get(pe["symbol"])
            if not sid:
                continue

            buf_rows.append(
                dict(
                    exchange_id=int(exchange_id),
                    symbol_id=int(sid),
                    ts=pe["ts"],
                    event_ms=int(pe["event_ms"]),
                    side=pe["side"],
                    price=float(pe["price"]),
                    qty=float(pe["qty"]),
                    filled_qty=pe["filled_qty"],
                    avg_price=pe["avg_price"],
                    status=pe["status"],
                    order_type=pe["order_type"],
                    time_in_force=pe["time_in_force"],
                    notional=pe["notional"],
                    is_long_liq=pe["is_long_liq"],
                    raw_json=pe["raw_json"],
                )
            )

            bucket = _floor_minute(pe["ts"])
            key = (int(sid), bucket)
            a = agg.get(key) or _Agg1m()
            notional = float(pe["notional"] or 0.0)
            qtyv = float(pe["filled_qty"] or pe["qty"] or 0.0)

            if pe["side"] == "SELL":
                a.long_notional += notional
                a.long_qty += qtyv
            elif pe["side"] == "BUY":
                a.short_notional += notional
                a.short_qty += qtyv
            a.events += 1
            agg[key] = a

        if time.time() - last_flush >= float(flush_sec):
            _flush(storage, exchange_id, buf_rows, agg)
            buf_rows.clear()
            agg.clear()
            last_flush = time.time()

    def on_error(_ws, error):
        log.warning("[Liquidations] WS error: %s", error)

    def on_close(_ws, *_):
        log.warning("[Liquidations] WS closed")

    def on_open(_ws):
        log.info("[Liquidations] WS connected %s", WS_URL)

    backoff = 1.0
    while not stop_event.is_set():
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            log.warning("[Liquidations] run_forever exception: %s", e, exc_info=True)

        if stop_event.wait(backoff):
            break
        backoff = min(backoff * 2.0, 30.0)

    # финальный flush
    _flush(storage, exchange_id, buf_rows, agg)


def _flush(storage, exchange_id: int, buf_rows: list, agg: Dict[Tuple[int, datetime], _Agg1m]) -> None:
    if buf_rows:
        try:
            storage.insert_liquidation_events(buf_rows)
        except Exception:
            log.exception("[Liquidations] insert_liquidation_events failed")

    if agg:
        rows = []
        for (sid, bucket), a in agg.items():
            rows.append(
                dict(
                    exchange_id=int(exchange_id),
                    symbol_id=int(sid),
                    bucket_ts=bucket,
                    long_notional=float(a.long_notional),
                    short_notional=float(a.short_notional),
                    long_qty=float(a.long_qty),
                    short_qty=float(a.short_qty),
                    events=int(a.events),
                    updated_at=datetime.now(timezone.utc),
                )
            )
        try:
            storage.upsert_liquidation_1m(rows)
        except Exception:
            log.exception("[Liquidations] upsert_liquidation_1m failed")


def start_liquidations_collector(
    *,
    storage,
    exchange_id: int,
    symbol_ids: Dict[str, int],
    stop_event: threading.Event,
    flush_sec: float = 2.0,
) -> threading.Thread:
    t = threading.Thread(
        target=_ws_loop,
        kwargs=dict(
            storage=storage,
            exchange_id=int(exchange_id),
            symbol_ids=dict(symbol_ids),
            stop_event=stop_event,
            flush_sec=float(flush_sec),
        ),
        daemon=True,
        name=f"BinanceLiquidationsCollector-{exchange_id}",
    )
    t.start()
    log.info("[Liquidations] collector thread started flush_sec=%.2f", float(flush_sec))
    return t
