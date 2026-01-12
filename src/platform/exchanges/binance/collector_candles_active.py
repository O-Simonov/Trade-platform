# src/platform/exchanges/binance/collector_candles_active.py
from __future__ import annotations

import logging
import threading
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
    Достаём HTTP code из текста исключения Binance REST,
    например: "Binance HTTP 429 GET /fapi/v1/klines: ..."

    Иногда Binance кладёт только code=-1003 (Too many requests).
    """
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
    # можно передать готовую карту SYMBOL->symbol_id, чтобы не дергать БД
    symbol_ids: Dict[str, int] | None = None,
) -> None:
    """
    CandlesActive: быстрые свечи (обычно 1m) по "активным" символам.

    Ключевая особенность этой версии:
      ✅ round-robin: в каждом цикле poll_sec обрабатываем ТОЛЬКО symbols_per_cycle символов,
         а не весь список активных. Это сильно снижает RPM и риск 429.

    Ожидаемые методы:
      rest.klines(symbol, interval, limit)
      storage.upsert_candles(rows)
      storage.list_active_symbols(exchange_id, active_ttl_sec) -> list[str]
      storage.get_symbol_id(exchange_id, symbol) -> int|None (только если не передан symbol_ids)
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

    # Частота цикла (в секундах)
    poll_sec = float(cfg.get("poll_sec", 30.0))

    # Сколько символов брать на один цикл (round-robin)
    symbols_per_cycle = int(cfg.get("symbols_per_cycle", 3))
    symbols_per_cycle = max(1, symbols_per_cycle)

    # Пауза между REST запросами (важно для 429)
    req_sleep_sec = float(cfg.get("req_sleep_sec", 0.8))
    req_sleep_sec = max(0.0, req_sleep_sec)

    # Настройки активного списка
    active_ttl_sec = int(cfg.get("active_ttl_sec", 900))
    active_ttl_sec = max(10, active_ttl_sec)

    # Как часто обновлять список активных символов из БД (НЕ каждый poll)
    active_refresh_sec = float(cfg.get("active_refresh_sec", 60.0))
    active_refresh_sec = max(5.0, active_refresh_sec)

    # Ограничение количества активных символов (на случай "шумной" таблицы активности)
    max_active_symbols = int(cfg.get("max_active_symbols", 20))
    max_active_symbols = max(0, max_active_symbols)

    # Сколько свечей брать в обычном режиме
    loop_limit = int(cfg.get("loop_limit", 2))
    loop_limit = max(1, loop_limit)

    # Seed (обычно лучше держать выключенным)
    seed_on_start = bool(cfg.get("seed_on_start", False))
    seed_limit = int(cfg.get("seed_limit", 120))
    seed_limit = max(1, seed_limit)

    extra_symbols = [str(s).upper().strip() for s in (extra_symbols or []) if str(s).strip()]

    logger.info(
        "[CandlesActive] старт intervals=%s poll=%.1fs symbols_per_cycle=%d req_sleep=%.2fs ttl=%ds refresh=%.1fs "
        "max_active=%d extra=%d seed=%s seed_limit=%d loop_limit=%d",
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
        loop_limit,
    )

    # -----------------------------
    # КЭШ symbol_id
    # -----------------------------
    sym_id_cache: Dict[str, int] = {}
    if symbol_ids:
        sym_id_cache.update({str(k).upper(): int(v) for k, v in symbol_ids.items()})

    def _get_symbol_id(symbol: str) -> Optional[int]:
        symbol = symbol.upper().strip()
        if not symbol:
            return None

        if symbol in sym_id_cache:
            return sym_id_cache[symbol]

        # fallback, если карту symbol_ids не передали
        try:
            sid = storage.get_symbol_id(exchange_id=int(exchange_id), symbol=symbol)
        except Exception:
            logger.debug("[CandlesActive] get_symbol_id failed symbol=%s", symbol, exc_info=True)
            return None

        if sid is None:
            return None

        sym_id_cache[symbol] = int(sid)
        return int(sid)

    # -----------------------------
    # COOLDOWN на пару (symbol, interval)
    # -----------------------------
    cooldown_until: Dict[Tuple[str, str], datetime] = {}

    def _process_symbol_interval(symbol: str, interval: str, limit: int, source: str) -> None:
        """
        Один запрос klines для (symbol, interval) + upsert.
        С учетом cooldown при 429/418/451.
        """
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

    # -----------------------------
    # ACTIVE SYMBOLS: обновление списка + round-robin очередь
    # -----------------------------
    def _fetch_active_symbols() -> List[str]:
        """
        Берем список активных символов из БД (TTL) + добавляем extra_symbols.
        Ограничиваем max_active_symbols.
        """
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

        if max_active_symbols > 0 and len(symbols) > max_active_symbols:
            symbols = symbols[:max_active_symbols]

        return symbols

    q: Deque[str] = deque()
    last_refresh = 0.0  # time.monotonic() отметка обновления активного списка

    def _refresh_queue_if_needed(force: bool = False) -> None:
        """
        Обновляем очередь активных символов не чаще, чем active_refresh_sec.
        Важно: стараемся не "ломать" round-robin — если очередь уже есть,
        добавляем новые символы и удаляем исчезнувшие.
        """
        nonlocal last_refresh, q

        now_mono = threading.get_native_id()  # не используем, просто чтобы не было импорта time
        # ^ не подходит: get_native_id не время. Поэтому делаем без time: обновление по datetime.

    # без time.monotonic() (чтобы не тянуть time) используем datetime.utcnow() в секундах
    def _now_ts() -> float:
        return datetime.now(timezone.utc).timestamp()

    def _refresh_queue_if_needed_real(force: bool = False) -> None:
        nonlocal last_refresh, q

        now_ts = _now_ts()
        if (not force) and (now_ts - last_refresh) < active_refresh_sec:
            return

        symbols = _fetch_active_symbols()
        last_refresh = now_ts

        if not symbols:
            q.clear()
            return

        new_set = set(symbols)
        old_list = list(q)
        old_set = set(old_list)

        # если очередь пустая — просто загрузим
        if not old_list:
            q = deque(symbols)
            return

        # удаляем то, чего больше нет
        kept = [s for s in old_list if s in new_set]

        # добавляем новые в конец
        added = [s for s in symbols if s not in old_set]

        q = deque(kept + added)

    # -----------------------------
    # SEED (опционально): один раз по всем активным (но с лимитами)
    # -----------------------------
    if seed_on_start:
        _refresh_queue_if_needed_real(force=True)
        all_syms = list(q)
        if all_syms:
            logger.info("[CandlesActiveSeed] старт symbols=%d intervals=%s limit=%d", len(all_syms), ",".join(intervals), seed_limit)
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

    # -----------------------------
    # ОСНОВНОЙ ЦИКЛ: round-robin batch
    # -----------------------------
    backoff = 1.0
    while not stop_event.is_set():
        try:
            _refresh_queue_if_needed_real(force=False)

            if not q:
                # нет активных — поспим и попробуем позже
                stop_event.wait(poll_sec)
                continue

            # берём строго symbols_per_cycle символов из очереди (round-robin)
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

            # делаем запросы только по выбранным символам
            for sym in picked:
                if stop_event.is_set():
                    break
                for itv in intervals:
                    if stop_event.is_set():
                        break
                    _process_symbol_interval(sym, itv, loop_limit, source="rest_active")
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
    """
    Обёртка: запускает run_candles_active_collector в daemon-thread.
    """
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
