# src/platform/exchanges/binance/collector_ticker_24h.py
from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger("src.platform.exchanges.binance.collector_ticker_24h")


def _utc_dt_from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)


def _f(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def _i(x, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return int(default)


def run_ticker_24h_collector(
    *,
    rest,
    storage,
    exchange_id: int,
    symbol_ids: List[int],
    cfg: dict,
    stop_event: threading.Event,
) -> None:
    """
    Collector: /fapi/v1/ticker/24hr (ALL symbols in one call) -> upsert_ticker_24h()

    storage expected:
      - get_symbol(symbol_id) -> symbol str
      - get_symbol_id(exchange_id, symbol) -> int|None
      - upsert_ticker_24h(rows) -> int
    rest expected:
      - ticker_24h(symbol: str|None = None) -> dict or list[dict]
    """
    cfg = cfg or {}

    enabled = bool(cfg.get("enabled", True))
    if not enabled:
        logger.info("[Ticker24h] disabled by config")
        return

    poll_sec = float(cfg.get("poll_sec", 300))
    active_ttl_sec = int(cfg.get("active_ttl_sec", 1800))  # если будешь брать active symbols
    source = str(cfg.get("source", "rest_24h"))

    logger.info("[Ticker24h] started poll=%.1fs source=%s", poll_sec, source)

    # cache: symbol -> symbol_id
    sym_id_cache: Dict[str, int] = {}

    def _get_symbol_id(symbol: str) -> Optional[int]:
        symbol = str(symbol).upper().strip()
        if not symbol:
            return None
        if symbol in sym_id_cache:
            return sym_id_cache[symbol]
        try:
            sid = storage.get_symbol_id(exchange_id=int(exchange_id), symbol=symbol)
        except Exception:
            logger.debug("[Ticker24h] get_symbol_id failed symbol=%s", symbol, exc_info=True)
            return None
        if sid is None:
            return None
        sym_id_cache[symbol] = int(sid)
        return int(sid)

    backoff = 1.0
    while not stop_event.is_set():
        try:
            # Один запрос на все символы
            data = rest.ticker_24h(symbol=None)

            # Binance может вернуть dict (если symbol задан) или list (если symbol=None)
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

                # В futures /fapi/v1/ticker/24hr обычно есть openTime/closeTime
                open_ms = it.get("openTime") or it.get("open_time") or 0
                close_ms = it.get("closeTime") or it.get("close_time") or 0

                row = {
                    "exchange_id": int(exchange_id),
                    "symbol_id": int(sid),
                    "open_time": _utc_dt_from_ms(open_ms) if open_ms else datetime.now(timezone.utc),
                    "close_time": _utc_dt_from_ms(close_ms) if close_ms else datetime.now(timezone.utc),

                    "open_price": _f(it.get("openPrice")),
                    "high_price": _f(it.get("highPrice")),
                    "low_price": _f(it.get("lowPrice")),
                    "last_price": _f(it.get("lastPrice")),

                    "volume": _f(it.get("volume")),
                    "quote_volume": _f(it.get("quoteVolume")),

                    "price_change": _f(it.get("priceChange")),
                    "price_change_percent": _f(it.get("priceChangePercent")),
                    "weighted_avg_price": _f(it.get("weightedAvgPrice")),

                    # В твоей таблице колонка называется trades (bigint)
                    # В Binance поле обычно "count"
                    "trades": _i(it.get("count")),
                    "source": source,
                }
                rows.append(row)

            n = 0
            if rows:
                n = int(storage.upsert_ticker_24h(rows) or 0)

            logger.info("[Ticker24h] poll upserts=%d items=%d rows=%d", n, len(items), len(rows))

            backoff = 1.0
            stop_event.wait(poll_sec)

        except Exception:
            logger.exception("[Ticker24h] cycle failed")
            stop_event.wait(backoff)
            backoff = min(backoff * 2.0, 60.0)


def start_ticker_24h_collector(
    *,
    rest,
    storage,
    exchange_id: int,
    symbol_ids: List[int],
    cfg: dict,
    stop_event: threading.Event,
) -> threading.Thread:
    t = threading.Thread(
        target=run_ticker_24h_collector,
        kwargs=dict(
            rest=rest,
            storage=storage,
            exchange_id=int(exchange_id),
            symbol_ids=list(symbol_ids or []),
            cfg=dict(cfg or {}),
            stop_event=stop_event,
        ),
        daemon=True,
        name="BinanceTicker24hCollector",
    )
    t.start()
    logger.info("[Ticker24h] collector thread started")
    return t
