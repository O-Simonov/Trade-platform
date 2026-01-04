# src/platform/exchanges/binance/collector_candles_active.py
from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

logger = logging.getLogger("src.platform.exchanges.binance.collector_candles_active")


def _utc_dt_from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)


def _to_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def _iter_chunks(items: List[str], n: int) -> Iterator[List[str]]:
    n = max(1, int(n))
    for i in range(0, len(items), n):
        yield items[i : i + n]


def _klines_to_rows(
    *,
    exchange_id: int,
    symbol_id: int,
    interval: str,
    klines: list,
    source: str,
) -> List[dict]:
    rows: List[dict] = []
    for k in klines or []:
        if not k or len(k) < 6:
            continue
        try:
            open_ms = int(k[0])
        except Exception:
            continue

        rows.append(
            {
                "exchange_id": int(exchange_id),
                "symbol_id": int(symbol_id),
                "interval": str(interval),
                "open_time": _utc_dt_from_ms(open_ms),
                "open": _to_float(k[1]),
                "high": _to_float(k[2]),
                "low": _to_float(k[3]),
                "close": _to_float(k[4]),
                "volume": _to_float(k[5]),
                "source": str(source),
            }
        )
    return rows


def run_candles_active_collector(
    *,
    storage,
    rest,
    exchange_id: int,
    cfg: dict,
    extra_symbols: List[str] | None,
    stop_event: threading.Event,
) -> None:
    cfg = cfg or {}

    enabled = bool(cfg.get("enabled", True))
    if not enabled:
        logger.info("[CandlesActive] disabled by config")
        return

    intervals = [str(x).strip() for x in (cfg.get("candle_intervals") or ["1m"]) if str(x).strip()]
    poll_sec = float(cfg.get("poll_sec", 5))
    batch = int(cfg.get("symbols_per_cycle", 10))
    req_sleep = float(cfg.get("req_sleep_sec", 0.15))
    seed_on_start = bool(cfg.get("seed_on_start", True))
    seed_limit = int(cfg.get("seed_limit", 300))
    active_ttl_sec = int(cfg.get("active_ttl_sec", 1800))
    loop_limit = int(cfg.get("loop_limit", 3))

    if not intervals:
        logger.warning("[CandlesActive] no candle_intervals -> stop")
        return

    extra_symbols = [str(s).upper().strip() for s in (extra_symbols or []) if str(s).strip()]

    cfg_extra = cfg.get("extra_symbols")
    if cfg_extra is not None and not isinstance(cfg_extra, (list, tuple)):
        logger.warning("[CandlesActive] cfg.extra_symbols is not a list -> ignored (use list of symbols)")

    logger.info(
        "[CandlesActive] started intervals=%s poll=%.1fs batch=%d ttl=%ds extra_symbols=%d seed=%s seed_limit=%d",
        ",".join(intervals),
        poll_sec,
        batch,
        active_ttl_sec,
        len(extra_symbols),
        seed_on_start,
        seed_limit,
    )

    sym_id_cache: Dict[str, int] = {}

    def _get_symbol_id(symbol: str) -> Optional[int]:
        symbol = symbol.upper()
        if symbol in sym_id_cache:
            return sym_id_cache[symbol]
        try:
            sid = storage.get_symbol_id(exchange_id=exchange_id, symbol=symbol)
        except Exception:
            logger.debug("[CandlesActive] get_symbol_id failed symbol=%s", symbol, exc_info=True)
            return None
        if sid is None:
            return None
        sym_id_cache[symbol] = int(sid)
        return int(sid)

    def _process_symbol_interval(symbol: str, interval: str, limit: int, source: str) -> None:
        sid = _get_symbol_id(symbol)
        if sid is None:
            return
        kl = rest.klines(symbol=symbol, interval=interval, limit=int(limit))
        rows = _klines_to_rows(
            exchange_id=exchange_id,
            symbol_id=sid,
            interval=interval,
            klines=kl,
            source=source,
        )
        if rows:
            storage.upsert_candles(rows)

    def _fetch_active_symbols() -> List[str]:
        try:
            active = storage.list_active_symbols(exchange_id=exchange_id, active_ttl_sec=active_ttl_sec) or []
        except Exception:
            logger.exception("[CandlesActive] list_active_symbols failed")
            active = []
        symbols = sorted(set([str(s).upper().strip() for s in active if str(s).strip()]) | set(extra_symbols))
        return symbols

    if seed_on_start:
        symbols = _fetch_active_symbols()
        if symbols:
            logger.info("[CandlesActiveSeed] start symbols=%d intervals=%s limit=%d", len(symbols), ",".join(intervals), seed_limit)
            try:
                for sym_batch in _iter_chunks(symbols, batch):
                    for sym in sym_batch:
                        for itv in intervals:
                            if stop_event.is_set():
                                break
                            _process_symbol_interval(sym, itv, seed_limit, source="rest_active_seed")
                            if stop_event.wait(req_sleep):
                                break
            except Exception:
                logger.exception("[CandlesActiveSeed] failed")
        else:
            logger.info("[CandlesActiveSeed] no active symbols")

    backoff = 1.0
    while not stop_event.is_set():
        try:
            symbols = _fetch_active_symbols()
            if not symbols:
                stop_event.wait(poll_sec)
                continue

            for sym_batch in _iter_chunks(symbols, batch):
                for sym in sym_batch:
                    for itv in intervals:
                        if stop_event.is_set():
                            break
                        _process_symbol_interval(sym, itv, loop_limit, source="rest_active")
                        if stop_event.wait(req_sleep):
                            break

            backoff = 1.0
            stop_event.wait(poll_sec)

        except Exception as e:
            logger.warning("[CandlesActive] loop error: %s", e, exc_info=True)
            stop_event.wait(backoff)
            backoff = min(backoff * 2.0, 60.0)


def start_candles_active_collector(
    *,
    rest,
    storage,
    exchange_id: int,
    cfg: dict,
    extra_symbols: List[str] | None,
    stop_event: threading.Event,
) -> threading.Thread:
    t = threading.Thread(
        target=run_candles_active_collector,
        kwargs=dict(
            storage=storage,
            rest=rest,
            exchange_id=int(exchange_id),
            cfg=dict(cfg or {}),
            extra_symbols=list(extra_symbols or []),
            stop_event=stop_event,
        ),
        daemon=True,
        name=f"BinanceCandlesActiveCollector-{exchange_id}",
    )
    t.start()
    logger.info("[CandlesActive] collector thread started")
    return t
