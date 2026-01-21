# src/platform/exchanges/binance/collector_candles_active.py
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple

logger = logging.getLogger("src.platform.exchanges.binance.collector_candles_active")


def _utc_dt_from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)


def _to_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def _classify_http_error(exc: Exception) -> Optional[int]:
    """
    Достаём HTTP code из исключения.

    1) Если это BinanceRESTError (или похожее), у него может быть поле .status
    2) Иначе — fallback парсинг по строке
    3) code=-1003 / Too many requests считаем как 429
    """
    st = getattr(exc, "status", None)
    try:
        if st is not None:
            st_i = int(st)
            if st_i > 0:
                return st_i
    except Exception:
        pass

    s = str(exc)
    for code in (451, 418, 429, 403, 400):
        if f"HTTP {code}" in s:
            return code

    if "-1003" in s or "Too many requests" in s:
        return 429

    return None


def _klines_to_rows(
    *,
    exchange_id: int,
    symbol_id: int,
    interval: str,
    klines: list,
    source: str,
) -> List[dict]:
    """
    Преобразуем ответ Binance /klines в строки для upsert_candles().
    """
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
    symbol_ids: Dict[str, int] | None = None,
) -> None:
    """
    CandlesActive: быстрые свечи (обычно 1m) по "активным" символам.

    Особенность: round-robin
      ✅ каждый poll обрабатываем ТОЛЬКО symbols_per_cycle символов
         вместо полного списка активных — это снижает RPM и риск 429.
    """
    cfg = cfg or {}

    enabled = bool(cfg.get("enabled", True))
    if not enabled:
        logger.info("[CandlesActive] отключено в конфиге")
        return

    intervals = [str(x).strip() for x in (cfg.get("candle_intervals") or ["1m"]) if str(x).strip()]
    if not intervals:
        logger.warning("[CandlesActive] candle_intervals пустой -> стоп")
        return

    poll_sec = float(cfg.get("poll_sec", 30.0))
    poll_sec = max(1.0, poll_sec)

    symbols_per_cycle = int(cfg.get("symbols_per_cycle", 3))
    symbols_per_cycle = max(1, symbols_per_cycle)

    req_sleep_sec = float(cfg.get("req_sleep_sec", 0.8))
    req_sleep_sec = max(0.0, req_sleep_sec)

    active_ttl_sec = int(cfg.get("active_ttl_sec", 900))
    active_ttl_sec = max(10, active_ttl_sec)

    active_refresh_sec = float(cfg.get("active_refresh_sec", 60.0))
    active_refresh_sec = max(5.0, active_refresh_sec)

    max_active_symbols = int(cfg.get("max_active_symbols", 20))
    max_active_symbols = max(0, max_active_symbols)

    # ВАЖНО: limit для /klines (а не "лимит циклов")
    if "klines_limit" in cfg:
        klines_limit = int(cfg.get("klines_limit", 2))
    elif "limit" in cfg:
        klines_limit = int(cfg.get("limit", 2))
    else:
        klines_limit = int(cfg.get("loop_limit", 2))  # legacy

    klines_limit = max(1, min(1500, klines_limit))

    seed_on_start = bool(cfg.get("seed_on_start", False))
    seed_limit = int(cfg.get("seed_limit", 120))
    seed_limit = max(1, min(1500, seed_limit))

    extra_symbols = [str(s).upper().strip() for s in (extra_symbols or []) if str(s).strip()]

    logger.info(
        "[CandlesActive] старт intervals=%s poll=%.1fs symbols_per_cycle=%d req_sleep=%.2fs ttl=%ds refresh=%.1fs "
        "max_active=%d extra=%d seed=%s seed_limit=%d klines_limit=%d",
        ",".join(intervals),
        poll_sec,
        symbols_per_cycle,
        req_sleep_sec,
        active_ttl_sec,
        active_refresh_sec,
        max_active_symbols,
        len(extra_symbols),
        seed_on_start,
        seed_limit,
        klines_limit,
    )

    sym_id_cache: Dict[str, int] = {}
    if symbol_ids:
        sym_id_cache.update({str(k).upper(): int(v) for k, v in symbol_ids.items()})

    def _get_symbol_id(symbol: str) -> Optional[int]:
        symbol = symbol.upper().strip()
        if not symbol:
            return None

        if symbol in sym_id_cache:
            return sym_id_cache[symbol]

        try:
            sid = storage.get_symbol_id(exchange_id=int(exchange_id), symbol=symbol)
        except Exception:
            logger.debug("[CandlesActive] get_symbol_id failed symbol=%s", symbol, exc_info=True)
            return None

        if sid is None:
            return None

        sym_id_cache[symbol] = int(sid)
        return int(sid)

    cooldown_until: Dict[Tuple[str, str], datetime] = {}

    def _process_symbol_interval(symbol: str, interval: str, limit: int, source: str) -> None:
        sid = _get_symbol_id(symbol)
        if sid is None:
            return

        now = datetime.now(timezone.utc)

        cd = cooldown_until.get((symbol, interval))
        if cd is not None and cd > now:
            return

        try:
            kl = rest.klines(symbol=symbol, interval=interval, limit=int(limit))
            rows = _klines_to_rows(
                exchange_id=int(exchange_id),
                symbol_id=int(sid),
                interval=str(interval),
                klines=kl,
                source=str(source),
            )
            if rows:
                storage.upsert_candles(rows)

        except Exception as e:
            code = _classify_http_error(e)

            if code in (418, 429):
                cooldown_until[(symbol, interval)] = now + timedelta(minutes=10)
                logger.warning("[CandlesActive] %s %s HTTP=%s -> cooldown 10m", symbol, interval, code, exc_info=True)
            elif code == 451:
                cooldown_until[(symbol, interval)] = now + timedelta(hours=6)
                logger.warning("[CandlesActive] %s %s HTTP=451 -> cooldown 6h", symbol, interval, exc_info=True)
            else:
                logger.debug("[CandlesActive] %s %s failed", symbol, interval, exc_info=True)

    def _fetch_active_symbols() -> List[str]:
        try:
            active = storage.list_active_symbols(exchange_id=int(exchange_id), active_ttl_sec=int(active_ttl_sec)) or []
        except Exception:
            logger.exception("[CandlesActive] list_active_symbols failed")
            active = []

        merged = set()

        for s in active:
            ss = str(s).upper().strip()
            if ss:
                merged.add(ss)

        for s in extra_symbols:
            ss = str(s).upper().strip()
            if ss:
                merged.add(ss)

        symbols = sorted(merged)

        if 0 < max_active_symbols < len(symbols):
            symbols = symbols[:max_active_symbols]

        return symbols

    q: Deque[str] = deque()
    last_refresh_mono = 0.0

    def _refresh_queue_if_needed(force: bool = False) -> None:
        nonlocal last_refresh_mono, q

        now_mono = time.monotonic()
        if (not force) and (now_mono - last_refresh_mono) < active_refresh_sec:
            return

        symbols = _fetch_active_symbols()
        last_refresh_mono = now_mono

        if not symbols:
            q.clear()
            return

        new_set = set(symbols)
        old_list = list(q)
        old_set = set(old_list)

        if not old_list:
            q = deque(symbols)
            return

        kept = [s for s in old_list if s in new_set]
        added = [s for s in symbols if s not in old_set]
        q = deque(kept + added)

    if seed_on_start:
        _refresh_queue_if_needed(force=True)
        all_syms = list(q)
        if all_syms:
            logger.info(
                "[CandlesActiveSeed] старт symbols=%d intervals=%s limit=%d",
                len(all_syms),
                ",".join(intervals),
                seed_limit,
            )
            for sym in all_syms:
                if stop_event.is_set():
                    break
                for itv in intervals:
                    if stop_event.is_set():
                        break
                    _process_symbol_interval(sym, itv, seed_limit, source="rest_active_seed")
                    if stop_event.wait(req_sleep_sec):
                        break
        else:
            logger.info("[CandlesActiveSeed] нет активных символов")

    backoff = 1.0
    while not stop_event.is_set():
        try:
            _refresh_queue_if_needed(force=False)

            if not q:
                stop_event.wait(poll_sec)
                continue

            picked: List[str] = []
            for _ in range(symbols_per_cycle):
                if not q:
                    break
                sym = q.popleft()
                picked.append(sym)
                q.append(sym)

            if not picked:
                stop_event.wait(poll_sec)
                continue

            for sym in picked:
                if stop_event.is_set():
                    break
                for itv in intervals:
                    if stop_event.is_set():
                        break
                    _process_symbol_interval(sym, itv, klines_limit, source="rest_active")
                    if stop_event.wait(req_sleep_sec):
                        break

            backoff = 1.0
            stop_event.wait(poll_sec)

        except Exception as e:
            logger.warning("[CandlesActive] ошибка цикла: %s", e, exc_info=True)
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
    symbol_ids: Dict[str, int] | None = None,
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
            symbol_ids=dict(symbol_ids or {}) if symbol_ids else None,
        ),
        daemon=True,
        name=f"BinanceCandlesActiveCollector-{exchange_id}",
    )
    t.start()
    logger.info("[CandlesActive] поток коллектора запущен")
    return t
