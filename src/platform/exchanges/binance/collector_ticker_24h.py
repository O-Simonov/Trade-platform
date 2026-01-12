# src/platform/exchanges/binance/collector_ticker_24h.py
from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Union

logger = logging.getLogger("src.platform.exchanges.binance.collector_ticker_24h")


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


def run_ticker_24h_collector(
    *,
    rest,
    storage,
    exchange_id: int,
    # ВАЖНО:
    # - может быть dict[str,int] (symbol -> symbol_id) — предпочтительно
    # - может быть list[int] или None — тогда symbol_id берём через storage.get_symbol_id()
    symbol_ids: Union[Dict[str, int], List[int], None],
    cfg: dict,
    stop_event: threading.Event,
) -> None:
    """
    Коллектор: /fapi/v1/ticker/24hr (ВСЕ символы одним запросом) -> storage.upsert_ticker_24h(rows)

    Ожидается, что:
      storage:
        - get_symbol_id(exchange_id, symbol) -> int|None
        - upsert_ticker_24h(rows) -> int
      rest:
        - ticker_24h(symbol=None) -> list[dict] (для всех) или dict (для одного)
    """
    cfg = cfg or {}

    enabled = bool(cfg.get("enabled", True))
    if not enabled:
        logger.info("[Ticker24h] выключено в конфиге")
        return

    poll_sec = float(cfg.get("poll_sec", 300))
    source = str(cfg.get("source", "rest_24h"))

    # Если передали dict(symbol->id), используем его как быстрый кеш.
    # Если передали list[int] — оно нам тут по сути не помогает, игнорируем.
    sym_id_cache: Dict[str, int] = {}
    if isinstance(symbol_ids, dict):
        # нормализуем ключи
        sym_id_cache = {str(k).upper().strip(): int(v) for k, v in symbol_ids.items() if str(k).strip()}

    logger.info(
        "[Ticker24h] старт poll=%.1fs source=%s cache_symbols=%d",
        poll_sec,
        source,
        len(sym_id_cache),
    )

    def _get_symbol_id(symbol: str) -> Optional[int]:
        symbol = str(symbol).upper().strip()
        if not symbol:
            return None

        # 1) быстрый кеш из symbol_map
        sid = sym_id_cache.get(symbol)
        if sid is not None:
            return int(sid)

        # 2) fallback: через БД
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
                logger.warning("[Ticker24h] пустой ответ")
                stop_event.wait(poll_sec)
                continue

            rows: List[dict] = []
            now_utc = datetime.now(timezone.utc)

            for it in items:
                sym = str(it.get("symbol", "")).upper().strip()
                if not sym:
                    continue

                sid = _get_symbol_id(sym)
                if sid is None:
                    continue

                # На futures /fapi/v1/ticker/24hr обычно есть openTime/closeTime
                open_ms = it.get("openTime") or it.get("open_time") or 0
                close_ms = it.get("closeTime") or it.get("close_time") or 0

                row = {
                    "exchange_id": int(exchange_id),
                    "symbol_id": int(sid),

                    "open_time": _utc_dt_from_ms(open_ms) if open_ms else now_utc,
                    "close_time": _utc_dt_from_ms(close_ms) if close_ms else now_utc,

                    "open_price": _f(it.get("openPrice")),
                    "high_price": _f(it.get("highPrice")),
                    "low_price": _f(it.get("lowPrice")),
                    "last_price": _f(it.get("lastPrice")),

                    "volume": _f(it.get("volume")),
                    "quote_volume": _f(it.get("quoteVolume")),

                    "price_change": _f(it.get("priceChange")),
                    "price_change_percent": _f(it.get("priceChangePercent")),
                    "weighted_avg_price": _f(it.get("weightedAvgPrice")),

                    # В Binance поле обычно "count"
                    "trades": _i(it.get("count")),
                    "source": source,
                }
                rows.append(row)

            n = 0
            if rows:
                n = int(storage.upsert_ticker_24h(rows) or 0)

            logger.info("[Ticker24h] цикл ok upserts=%d items=%d rows=%d", n, len(items), len(rows))

            backoff = 1.0
            stop_event.wait(poll_sec)

        except Exception:
            logger.exception("[Ticker24h] цикл упал")
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
    """
    Запускает run_ticker_24h_collector в отдельном потоке.
    symbol_ids можно передавать как:
      - dict[str,int] symbol->id (лучше)
      - list[int] или None (тогда коллектор сам будет спрашивать storage.get_symbol_id)
    """
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
    logger.info("[Ticker24h] поток коллектора запущен")
    return t
