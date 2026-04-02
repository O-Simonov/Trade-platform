# src/platform/exchanges/binance/collector_ticker_24h.py
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Union

from src.platform.exchanges.binance.ws import BinanceWS, WS_WS_BASE

logger = logging.getLogger("src.platform.exchanges.binance.collector_ticker_24h")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_dt_from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def _i(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return int(default)


def _as_bool(x: Any, default: bool = False) -> bool:
    if x is None:
        return default
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return default


def _extract_ws_items(payload: Any) -> List[dict]:
    """
    Поддерживает оба варианта:
    1) raw list: [ {...}, {...} ]
    2) combined stream wrapper: { "stream": "...", "data": [...] } или { "data": {...} }
    3) single dict: { ... }
    """
    if payload is None:
        return []

    # combined wrapper
    if isinstance(payload, dict) and "data" in payload:
        d = payload.get("data")
        if isinstance(d, list):
            return [x for x in d if isinstance(x, dict)]
        if isinstance(d, dict):
            return [d]
        return []

    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]

    if isinstance(payload, dict):
        return [payload]

    return []


def _normalize_ws_ticker_row(
    *,
    exchange_id: int,
    symbol_id: int,
    it: dict,
    source: str,
) -> dict:
    """
    Binance futures 24hrTicker stream fields (обычно):
      s symbol
      O openTime, C closeTime
      o openPrice, h highPrice, l lowPrice, c lastPrice
      v volume, q quoteVolume
      p priceChange, P priceChangePercent
      w weightedAvgPrice
      n count
    """
    open_ms = it.get("O") or it.get("openTime") or it.get("open_time") or 0
    close_ms = it.get("C") or it.get("closeTime") or it.get("close_time") or 0

    now_utc = _utc_now()

    return {
        "exchange_id": int(exchange_id),
        "symbol_id": int(symbol_id),

        "open_time": _utc_dt_from_ms(open_ms) if open_ms else now_utc,
        "close_time": _utc_dt_from_ms(close_ms) if close_ms else now_utc,

        "open_price": _f(it.get("o", it.get("openPrice"))),
        "high_price": _f(it.get("h", it.get("highPrice"))),
        "low_price": _f(it.get("l", it.get("lowPrice"))),
        "last_price": _f(it.get("c", it.get("lastPrice"))),

        "volume": _f(it.get("v", it.get("volume"))),
        "quote_volume": _f(it.get("q", it.get("quoteVolume"))),

        "price_change": _f(it.get("p", it.get("priceChange"))),
        "price_change_percent": _f(it.get("P", it.get("priceChangePercent"))),
        "weighted_avg_price": _f(it.get("w", it.get("weightedAvgPrice"))),

        "trades": _i(it.get("n", it.get("count"))),
        "source": str(source),
    }


def run_ticker_24h_collector(
    *,
    rest,
    storage,
    exchange_id: int,
    # prefer dict[str,int] {SYMBOL: symbol_id}
    symbol_ids: Union[Dict[str, int], List[int], None],
    cfg: dict,
    stop_event: threading.Event,
) -> None:
    cfg = cfg or {}

    enabled = bool(cfg.get("enabled", True))
    if not enabled:
        logger.info("[Ticker24h] disabled by config")
        return

    # режим
    source = str(cfg.get("source", "rest_24h")).strip().lower()
    use_ws = _as_bool(cfg.get("use_ws", False), default=False)

    # если явно source=ws... — включаем WS
    if source.startswith("ws"):
        use_ws = True
    if use_ws:
        # нормализуем source
        source = "ws_24h"

    # кеш symbol->id (из run_market_data у тебя приходит dict — это идеально)
    sym_id_cache: Dict[str, int] = {}
    if isinstance(symbol_ids, dict):
        sym_id_cache = {str(k).upper().strip(): int(v) for k, v in symbol_ids.items() if str(k).strip()}

    def _get_symbol_id(symbol: str) -> Optional[int]:
        s = str(symbol).upper().strip()
        if not s:
            return None

        sid = sym_id_cache.get(s)
        if sid is not None:
            return int(sid)

        # fallback: через БД (если не передали dict)
        try:
            sid = storage.get_symbol_id(exchange_id=int(exchange_id), symbol=s)
        except Exception:
            logger.debug("[Ticker24h] get_symbol_id failed symbol=%s", s, exc_info=True)
            return None

        if sid is None:
            return None

        sym_id_cache[s] = int(sid)
        return int(sid)

    # -------------------------
    # WS MODE
    # -------------------------
    if use_ws:
        stream = str(cfg.get("ws_stream", "!ticker@arr")).strip() or "!ticker@arr"
        ws_url = str(cfg.get("ws_url", "")).strip() or (WS_WS_BASE + stream)

        flush_sec = float(cfg.get("flush_sec", 10.0))
        flush_sec = max(1.0, flush_sec)

        ping_interval = int(cfg.get("ws_ping_interval", 60))
        ping_timeout = int(cfg.get("ws_ping_timeout", 30))

        # опционально: редкий REST safety poll
        rest_safety_poll = _as_bool(cfg.get("rest_safety_poll", False), default=False)
        rest_poll_sec = float(cfg.get("rest_poll_sec", cfg.get("poll_sec", 1800)))
        rest_poll_sec = max(60.0, rest_poll_sec)

        logger.info(
            "[Ticker24h] WS start url=%s flush=%.1fs cache_symbols=%d safety_rest=%s rest_poll=%.0fs",
            ws_url, flush_sec, len(sym_id_cache), rest_safety_poll, rest_poll_sec
        )

        lock = threading.Lock()
        latest_by_symbol: Dict[str, dict] = {}
        last_msg_ts = 0.0

        def _on_message(msg: str) -> None:
            nonlocal last_msg_ts
            try:
                payload = json.loads(msg)
            except Exception:
                return

            items = _extract_ws_items(payload)
            if not items:
                return

            with lock:
                for it in items:
                    sym = str(it.get("s") or it.get("symbol") or "").upper().strip()
                    if not sym:
                        continue
                    latest_by_symbol[sym] = it
                last_msg_ts = time.time()

        ws = BinanceWS(
            name="BinanceTicker24hWS",
            url=ws_url,
            on_message=_on_message,
            ping_interval=ping_interval,
            ping_timeout=ping_timeout,
            reconnect_min_delay=1.0,
            reconnect_max_delay=30.0,
        )
        ws.start()

        def _flush_once() -> int:
            with lock:
                snap = dict(latest_by_symbol)
                latest_by_symbol.clear()

            if not snap:
                return 0

            rows: List[dict] = []
            for sym, it in snap.items():
                sid = _get_symbol_id(sym)
                if sid is None:
                    continue
                try:
                    row = _normalize_ws_ticker_row(
                        exchange_id=int(exchange_id),
                        symbol_id=int(sid),
                        it=it,
                        source="ws_24h",
                    )
                    rows.append(row)
                except Exception:
                    continue

            if not rows:
                return 0

            try:
                n = int(storage.upsert_ticker_24h(rows) or 0)
                return n
            except Exception:
                logger.exception("[Ticker24h] upsert failed (ws flush)")
                return 0

        # отдельный “тикер-флашер”
        backoff = 1.0
        last_rest_poll = 0.0

        try:
            while not stop_event.is_set():
                stop_event.wait(flush_sec)

                # если WS молчит долго — просто лог (не падаем)
                now = time.time()
                if last_msg_ts > 0 and (now - last_msg_ts) > max(60.0, flush_sec * 6):
                    logger.warning("[Ticker24h] WS no messages for %.1fs", now - last_msg_ts)

                n = _flush_once()
                if n > 0:
                    logger.info("[Ticker24h] ws flush ok upserts=%d", n)

                # safety rest poll (редко)
                if rest_safety_poll and (now - last_rest_poll) >= rest_poll_sec:
                    last_rest_poll = now
                    try:
                        data = rest.ticker_24h(symbol=None)
                        items = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
                        if items:
                            rows2: List[dict] = []
                            for it in items:
                                sym = str(it.get("symbol", "")).upper().strip()
                                if not sym:
                                    continue
                                sid = _get_symbol_id(sym)
                                if sid is None:
                                    continue
                                rows2.append(
                                    _normalize_ws_ticker_row(
                                        exchange_id=int(exchange_id),
                                        symbol_id=int(sid),
                                        it={
                                            # приводим REST поля к единому виду
                                            "openTime": it.get("openTime"),
                                            "closeTime": it.get("closeTime"),
                                            "openPrice": it.get("openPrice"),
                                            "highPrice": it.get("highPrice"),
                                            "lowPrice": it.get("lowPrice"),
                                            "lastPrice": it.get("lastPrice"),
                                            "volume": it.get("volume"),
                                            "quoteVolume": it.get("quoteVolume"),
                                            "priceChange": it.get("priceChange"),
                                            "priceChangePercent": it.get("priceChangePercent"),
                                            "weightedAvgPrice": it.get("weightedAvgPrice"),
                                            "count": it.get("count"),
                                        },
                                        source="rest_safety",
                                    )
                                )
                            if rows2:
                                nn = int(storage.upsert_ticker_24h(rows2) or 0)
                                logger.info("[Ticker24h] safety REST ok upserts=%d items=%d rows=%d", nn, len(items), len(rows2))
                    except Exception:
                        logger.exception("[Ticker24h] safety REST failed")

                backoff = 1.0

        except Exception:
            logger.exception("[Ticker24h] WS loop crashed")
            stop_event.wait(backoff)
        finally:
            try:
                ws.stop()
            except Exception:
                pass
            logger.info("[Ticker24h] WS stopped")
        return

    # -------------------------
    # REST MODE (как было)
    # -------------------------
    poll_sec = float(cfg.get("poll_sec", 300))
    poll_sec = max(5.0, poll_sec)
    source = source or "rest_24h"

    logger.info(
        "[Ticker24h] REST start poll=%.1fs source=%s cache_symbols=%d",
        poll_sec,
        source,
        len(sym_id_cache),
    )

    backoff = 1.0
    while not stop_event.is_set():
        try:
            data = rest.ticker_24h(symbol=None)

            items = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
            if not items:
                logger.warning("[Ticker24h] empty response")
                stop_event.wait(poll_sec)
                continue

            rows: List[dict] = []
            for it in items:
                sym = str(it.get("symbol", "")).upper().strip()
                if not sym:
                    continue
                sid = _get_symbol_id(sym)
                if sid is None:
                    continue

                rows.append(
                    _normalize_ws_ticker_row(
                        exchange_id=int(exchange_id),
                        symbol_id=int(sid),
                        it=it,
                        source=str(source),
                    )
                )

            n = 0
            if rows:
                n = int(storage.upsert_ticker_24h(rows) or 0)

            logger.info("[Ticker24h] REST ok upserts=%d items=%d rows=%d", n, len(items), len(rows))

            backoff = 1.0
            stop_event.wait(poll_sec)

        except Exception:
            logger.exception("[Ticker24h] REST loop failed")
            stop_event.wait(backoff)
            backoff = min(backoff * 2.0, 60.0)


def start_ticker_24h_collector(
    *,
    rest,
    storage,
    exchange_id: int,
    symbol_ids: Union[Dict[str, int], List[int], None],
    cfg: dict,
    stop_event: threading.Event,
) -> threading.Thread:
    t = threading.Thread(
        target=run_ticker_24h_collector,
        kwargs=dict(
            rest=rest,
            storage=storage,
            exchange_id=int(exchange_id),
            symbol_ids=symbol_ids,
            cfg=dict(cfg or {}),
            stop_event=stop_event,
        ),
        daemon=True,
        name="BinanceTicker24hCollector",
    )
    t.start()
    logger.info("[Ticker24h] thread started")
    return t
