from __future__ import annotations

import inspect
import logging
import os
import signal
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from src.platform.data.retention.retention_worker import RetentionWorker
from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage
from src.platform.exchanges.binance.collector_candles import start_candles_collector
from src.platform.exchanges.binance.collector_symbol_filters import start_symbol_filters_collector
from src.platform.exchanges.binance.collector_universe import sync_tradable_usdtm_perpetual_symbols
from src.platform.exchanges.binance.rest import BinanceFuturesREST

# -------------------------
# Optional collectors
# -------------------------
try:
    from src.platform.exchanges.binance.collector_open_interest import start_open_interest_collector
except Exception:
    start_open_interest_collector = None  # type: ignore

try:
    from src.platform.exchanges.binance.collector_funding import start_funding_collector
except Exception:
    start_funding_collector = None  # type: ignore

try:
    from src.platform.exchanges.binance.collector_candles_active import start_candles_active_collector
except Exception:
    start_candles_active_collector = None  # type: ignore

try:
    from src.platform.exchanges.binance.collector_ticker_24h import start_ticker_24h_collector
except Exception:
    start_ticker_24h_collector = None  # type: ignore


log = logging.getLogger("platform.run_market_data")


# ============================================================
# LOGGING / CONFIG
# ============================================================

def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(threadName)s | %(message)s",
    )


def _find_cfg_candidate(base: Path) -> Optional[Path]:
    base = base.resolve()
    for _ in range(0, 12):
        p = (base / "config" / "market_data.yaml").resolve()
        if p.exists():
            return p
        if base.parent == base:
            break
        base = base.parent
    return None


def _resolve_cfg_path() -> Path:
    tried: list[str] = []

    p_env = os.environ.get("MARKETDATA_CONFIG")
    if p_env:
        cand = Path(p_env).expanduser()
        cand = (Path.cwd() / cand).resolve() if not cand.is_absolute() else cand.resolve()
        tried.append(str(cand))
        if cand.exists():
            return cand

    cand = _find_cfg_candidate(Path.cwd())
    if cand:
        return cand
    tried.append(str((Path.cwd() / "config" / "market_data.yaml").resolve()))

    here = Path(__file__).resolve()
    cand = _find_cfg_candidate(here.parent)
    if cand:
        return cand
    tried.append(str((here.parent / "config" / "market_data.yaml").resolve()))

    raise FileNotFoundError(
        "Не найден MarketData конфиг. Пробовали:\n- " + "\n- ".join(tried)
        + "\nПодсказка: установи MARKETDATA_CONFIG на явный путь к YAML."
    )


def _load_cfg() -> dict:
    path = _resolve_cfg_path()
    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    cfg["_path"] = str(path)
    return cfg


def _has_method(obj: Any, name: str) -> bool:
    return callable(getattr(obj, name, None))


# ============================================================
# REST LIMITS / REST CLIENT
# ============================================================

def _apply_rest_limits_from_cfg(cfg: dict) -> None:
    limits = cfg.get("rest_limits") or {}

    def _f(x, d: float) -> float:
        try:
            return float(x)
        except Exception:
            return float(d)

    def _i(x, d: int) -> int:
        try:
            return int(x)
        except Exception:
            return int(d)

    max_rps = _f(limits.get("max_rps", 0.8), 0.8)
    max_rpm = _i(limits.get("max_rpm", 300), 300)
    force = bool(limits.get("force", False))

    env_rps = os.environ.get("BINANCE_REST_MAX_RPS")
    env_rpm = os.environ.get("BINANCE_REST_MAX_RPM")

    if force or not env_rps:
        os.environ["BINANCE_REST_MAX_RPS"] = str(max_rps)
    if force or not env_rpm:
        os.environ["BINANCE_REST_MAX_RPM"] = str(max_rpm)

    log.info(
        "[REST] лимиты: BINANCE_REST_MAX_RPS=%s BINANCE_REST_MAX_RPM=%s (force=%s)",
        os.environ.get("BINANCE_REST_MAX_RPS"),
        os.environ.get("BINANCE_REST_MAX_RPM"),
        force,
    )


def _make_rest_public_no_keys() -> BinanceFuturesREST:
    # ВСЕГДА без ключей
    log.info("Binance REST: запуск БЕЗ api_key/secret_key (только public endpoints)")
    return BinanceFuturesREST("", "")


# ============================================================
# DB HELPERS
# ============================================================

def _fetch_active_symbols_map(storage: PostgreSQLStorage, exchange_id: int) -> Dict[str, int]:
    with storage.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, symbol_id
                FROM symbols
                WHERE exchange_id = %s AND is_active = true
                ORDER BY symbol
                """,
                (int(exchange_id),),
            )
            rows = cur.fetchall()
    return {str(sym).upper(): int(sid) for sym, sid in (rows or [])}


# ============================================================
# SIGNATURE-SAFE CALLS
# ============================================================

def _call_supported(fn, name: str, **kwargs):
    sig = inspect.signature(fn)
    params = sig.parameters

    if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
        log.info("[%s] вызов с kwargs=%s", name, sorted(kwargs.keys()))
        return fn(**kwargs)

    supported = {k: v for k, v in kwargs.items() if k in params}
    dropped = sorted([k for k in kwargs.keys() if k not in params])

    log.info("[%s] передано=%s", name, sorted(supported.keys()))
    if dropped:
        log.warning("[%s] отброшено (нет в сигнатуре)=%s", name, dropped)

    return fn(**supported)


def _safe_start(name: str, fn, **kwargs) -> None:
    try:
        _call_supported(fn, name, **kwargs)
        log.info("%s запущен", name)
    except Exception:
        log.exception("%s: ошибка запуска", name)


def _safe_start_async(name: str, fn, **kwargs) -> None:
    """
    Ключевая защита от “зависаний” на старте:
    стартуем любой коллектор в отдельном starter-thread,
    чтобы MainThread никогда не блокировался.
    """
    t = threading.Thread(
        target=_safe_start,
        kwargs=dict(name=name, fn=fn, **kwargs),
        name=f"Starter:{name}",
        daemon=True,
    )
    t.start()


def _stagger_wait(stop_event: threading.Event, cfg: dict, key: str, default_sec: float) -> None:
    try:
        sec = float((cfg.get("startup_stagger") or {}).get(key, default_sec))
    except Exception:
        sec = default_sec
    if sec > 0:
        stop_event.wait(sec)


# ============================================================
# UNIVERSE WORKERS
# ============================================================

def _universe_seed_worker(*, rest, storage, exchange_id: int, universe_cfg: dict) -> None:
    try:
        log.info("[UniverseSeed] start (async)")
        sync_tradable_usdtm_perpetual_symbols(
            binance_rest=rest,
            storage=storage,
            exchange_id=int(exchange_id),
            min_size=int(universe_cfg.get("min_size", 200)),
            max_drop_ratio=float(universe_cfg.get("max_drop_ratio", 0.30)),
        )
        log.info("[UniverseSeed] done")
    except Exception:
        log.exception("[UniverseSeed] failed (ignored)")


def _universe_daemon(*, rest, storage, exchange_id: int, cfg: dict, stop_event: threading.Event) -> None:
    refresh_sec = int(cfg.get("refresh_sec", 21600))
    min_size = int(cfg.get("min_size", 200))
    max_drop_ratio = float(cfg.get("max_drop_ratio", 0.30))

    log.info("[UniverseDaemon] старт refresh_sec=%s min_size=%d max_drop_ratio=%.2f", refresh_sec, min_size, max_drop_ratio)

    backoff = 5.0
    while not stop_event.is_set():
        try:
            sync_tradable_usdtm_perpetual_symbols(
                binance_rest=rest,
                storage=storage,
                exchange_id=int(exchange_id),
                min_size=min_size,
                max_drop_ratio=max_drop_ratio,
            )
            backoff = 5.0
            stop_event.wait(refresh_sec)
        except Exception as e:
            log.warning("[UniverseDaemon] ошибка: %s", e, exc_info=True)
            stop_event.wait(backoff)
            backoff = min(backoff * 2.0, 300.0)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    _setup_logging()
    cfg = _load_cfg()

    log.info("=== RUN MARKET DATA START ===")
    log.info("Config: %s", cfg.get("_path"))

    dsn = os.environ.get("PG_DSN")
    if not dsn:
        raise RuntimeError("PG_DSN env is required")

    pool = create_pool(dsn)
    storage = PostgreSQLStorage(pool=pool)
    log.info("PostgreSQL storage initialized")

    exchange_id = int(cfg.get("exchange_id", 1))

    _apply_rest_limits_from_cfg(cfg)

    # (опционально) сделай сетевой таймаут жёстче через env:
    # os.environ.setdefault("BINANCE_REST_TIMEOUT_SEC", "10")
    rest = _make_rest_public_no_keys()
    log.info("[BOOT] REST client created")

    stop_event = threading.Event()

    def _sig_handler(signum, _frame):
        log.warning("Signal %s -> останавливаемся...", signum)
        stop_event.set()

    try:
        signal.signal(signal.SIGINT, _sig_handler)
    except Exception:
        pass
    try:
        signal.signal(signal.SIGTERM, _sig_handler)
    except Exception:
        pass

    # --------------------------------------------------------
    # Universe (НЕ блокируем старт)
    # --------------------------------------------------------
    universe_cfg = cfg.get("universe") or {}
    if universe_cfg.get("enabled", True):
        if not _has_method(rest, "fetch_exchange_info"):
            log.warning("[Universe] выключено: нет BinanceFuturesREST.fetch_exchange_info()")
        else:
            # seed_on_start -> только async, без ожидания
            if bool(universe_cfg.get("seed_on_start", False)):
                threading.Thread(
                    target=_universe_seed_worker,
                    kwargs=dict(rest=rest, storage=storage, exchange_id=exchange_id, universe_cfg=universe_cfg),
                    name="UniverseSeed",
                    daemon=True,
                ).start()

            # daemon -> тоже отдельный поток
            threading.Thread(
                target=_universe_daemon,
                kwargs=dict(rest=rest, storage=storage, exchange_id=exchange_id, cfg=universe_cfg, stop_event=stop_event),
                name="UniverseDaemon",
                daemon=True,
            ).start()
            log.info("Universe daemon started")
    else:
        log.warning("Universe disabled by config")

    _stagger_wait(stop_event, cfg, "after_universe", 0.50)

    # --------------------------------------------------------
    # Symbols from DB (сразу)
    # --------------------------------------------------------
    symbol_map = _fetch_active_symbols_map(storage, exchange_id)
    log.info("[Universe] active symbols in DB: %d", len(symbol_map))
    if not symbol_map:
        log.warning("[Universe] В БД нет активных символов -> коллекторы будут пустые до заполнения symbols")

    # --------------------------------------------------------
    # SymbolFilters (async start)
    # --------------------------------------------------------
    sf_cfg = cfg.get("symbol_filters") or {}
    if sf_cfg.get("enabled", True):
        if not _has_method(rest, "fetch_exchange_info"):
            log.warning("[SymbolFilters] выключено: нет BinanceFuturesREST.fetch_exchange_info()")
        else:
            _safe_start_async(
                "SymbolFilters collector",
                start_symbol_filters_collector,
                rest=rest,
                storage=storage,
                exchange_id=exchange_id,
                symbol_ids=symbol_map,
                interval_sec=int(sf_cfg.get("refresh_sec", 3600)),
                do_seed_on_start=bool(sf_cfg.get("seed_on_start", True)),
                stop_event=stop_event,
            )
    else:
        log.warning("SymbolFilters disabled by config")

    _stagger_wait(stop_event, cfg, "after_symbol_filters", 0.75)

    # --------------------------------------------------------
    # Candles (async start)
    # --------------------------------------------------------
    md_cfg = cfg.get("market_data") or {}
    if md_cfg.get("enabled", True):
        intervals = list(md_cfg.get("candle_intervals") or [])
        if not intervals:
            log.warning("Candles включены, но candle_intervals пуст -> пропуск")
        elif not _has_method(rest, "klines"):
            log.warning("[Candles] выключено: нет BinanceFuturesREST.klines()")
        else:
            _safe_start_async(
                "Candles collector",
                start_candles_collector,
                rest=rest,
                storage=storage,
                exchange_id=exchange_id,
                symbol_ids=symbol_map,
                intervals=intervals,
                poll_sec=float(md_cfg.get("poll_sec", 60)),
                symbols_per_cycle=int(md_cfg.get("symbols_per_cycle", 2)),
                per_request_sleep_sec=float(md_cfg.get("req_sleep_sec", 0.5)),
                seed_on_start=bool(md_cfg.get("seed_on_start", False)),
                seed_days=int(md_cfg.get("seed_days", 0)),
                seed_chunk_limit=int(md_cfg.get("seed_chunk_limit", 1500)),
                seed_limit=int(md_cfg.get("seed_limit", 0)),
                loop_limit=int(md_cfg.get("loop_limit", 3)),
                overlap_points=int(md_cfg.get("overlap_points", 1)),
                lookback_days_if_empty=int(md_cfg.get("lookback_days_if_empty", 2)),
                stop_event=stop_event,
            )
    else:
        log.warning("Candles disabled by config")

    _stagger_wait(stop_event, cfg, "after_candles", 1.00)

    # --------------------------------------------------------
    # CandlesActive (async start)
    # --------------------------------------------------------
    mda_cfg = cfg.get("market_data_active") or {}
    if mda_cfg.get("enabled", False):
        if start_candles_active_collector is None:
            log.warning("CandlesActive включён, но collector_candles_active.py не найден")
        elif not _has_method(rest, "klines"):
            log.warning("[CandlesActive] выключено: нет BinanceFuturesREST.klines()")
        else:
            extra = mda_cfg.get("extra_symbols")
            if extra is None:
                extra_list: list[str] = []
            elif isinstance(extra, (list, tuple)):
                extra_list = [str(x).upper().strip() for x in extra if str(x).strip()]
            else:
                log.warning("market_data_active.extra_symbols должен быть list -> игнорируем")
                extra_list = []

            _safe_start_async(
                "CandlesActive collector",
                start_candles_active_collector,
                rest=rest,
                storage=storage,
                exchange_id=exchange_id,
                cfg=mda_cfg,
                extra_symbols=extra_list,
                stop_event=stop_event,
                symbol_ids=symbol_map,
            )
    else:
        log.info("CandlesActive disabled by config")

    _stagger_wait(stop_event, cfg, "after_candles_active", 1.00)

    # --------------------------------------------------------
    # Open Interest (async start)
    # --------------------------------------------------------
    oi_cfg = cfg.get("open_interest") or {}
    if oi_cfg.get("enabled", False):
        if start_open_interest_collector is None:
            log.warning("OpenInterest включён, но collector_open_interest.py не найден")
        elif not _has_method(rest, "open_interest_hist"):
            log.warning("[OpenInterest] выключено: нет BinanceFuturesREST.open_interest_hist()")
        else:
            _safe_start_async(
                "OpenInterest collector",
                start_open_interest_collector,
                rest=rest,
                storage=storage,
                exchange_id=exchange_id,
                symbol_ids=symbol_map,
                cfg=oi_cfg,
                stop_event=stop_event,
            )
    else:
        log.info("OpenInterest disabled by config")

    _stagger_wait(stop_event, cfg, "after_open_interest", 1.00)

    # --------------------------------------------------------
    # Funding (async start)
    # --------------------------------------------------------
    f_cfg = cfg.get("funding") or {}
    if f_cfg.get("enabled", False):
        if start_funding_collector is None:
            log.warning("Funding включён, но collector_funding.py не найден")
        else:
            missing = [m for m in ("funding_rate", "premium_index") if not _has_method(rest, m)]
            if missing:
                log.warning("[Funding] выключено: отсутствуют методы REST: %s", ", ".join(missing))
            else:
                _safe_start_async(
                    "Funding collector",
                    start_funding_collector,
                    rest=rest,
                    storage=storage,
                    exchange_id=exchange_id,
                    symbol_ids=symbol_map,
                    cfg=f_cfg,
                    stop_event=stop_event,
                )
    else:
        log.info("Funding disabled by config")

    _stagger_wait(stop_event, cfg, "after_funding", 0.75)

    # --------------------------------------------------------
    # Ticker 24h (async start)
    # --------------------------------------------------------
    t24_cfg = cfg.get("ticker_24h") or {}
    if t24_cfg.get("enabled", False):
        if start_ticker_24h_collector is None:
            log.warning("Ticker24h включён, но collector_ticker_24h.py не найден")
        elif not _has_method(rest, "ticker_24h"):
            log.warning("[Ticker24h] выключено: нет BinanceFuturesREST.ticker_24h()")
        else:
            _safe_start_async(
                "Ticker24h collector",
                start_ticker_24h_collector,
                rest=rest,
                storage=storage,
                exchange_id=exchange_id,
                symbol_ids=symbol_map,
                cfg=t24_cfg,
                stop_event=stop_event,
            )
    else:
        log.info("Ticker24h disabled by config")

    _stagger_wait(stop_event, cfg, "after_ticker_24h", 0.75)

    # --------------------------------------------------------
    # Retention
    # --------------------------------------------------------
    rw: Optional[RetentionWorker] = None
    r_cfg = cfg.get("retention") or {}
    if r_cfg.get("enabled", True):
        run_sec = int(r_cfg.get("run_sec", 3600))
        rw = RetentionWorker(storage=storage, exchange_id=exchange_id, cfg=r_cfg, run_sec=run_sec)
        rw.start()
        log.info("RetentionWorker started run_sec=%s", run_sec)
    else:
        log.warning("Retention disabled by config")

    # --------------------------------------------------------
    # Main loop + heartbeat
    # --------------------------------------------------------
    last_hb = time.time()
    try:
        while not stop_event.is_set():
            time.sleep(1.0)
            if time.time() - last_hb >= 30.0:
                last_hb = time.time()
                log.info("[HEARTBEAT] alive threads=%d active_symbols=%d", threading.active_count(), len(symbol_map))
    except KeyboardInterrupt:
        log.warning("CTRL+C -> останавливаемся...")
        stop_event.set()
    finally:
        stop_event.set()
        time.sleep(0.25)

        if rw is not None:
            try:
                rw.stop()
                rw.join(timeout=5)
            except Exception:
                log.exception("RetentionWorker stop/join failed (ignored)")

        try:
            pool.close()
        except Exception:
            pass

        log.info("=== RUN MARKET DATA STOP ===")


if __name__ == "__main__":
    main()
