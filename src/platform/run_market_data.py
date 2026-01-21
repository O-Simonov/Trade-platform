# src/platform/run_market_data.py
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

from src.platform.exchanges.binance.collector_candles_gap_repair import (
    start_candles_gap_repair_collector,
)

from src.platform.data.market_state.market_state_5m_worker import MarketState5mWorker

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

try:
    from src.platform.exchanges.binance.collector_liquidations import start_liquidations_collector
except Exception:
    start_liquidations_collector = None  # type: ignore

try:
    from src.platform.exchanges.binance.collector_market_trades import MarketTradesCollector
except Exception:
    MarketTradesCollector = None  # type: ignore

try:
    from src.platform.exchanges.binance.collector_market_trades_all import start_market_trades_all_collector
except Exception:
    start_market_trades_all_collector = None  # type: ignore


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
    """
    Ищем config/market_data.yaml вверх по дереву.
    MARKETDATA_CONFIG (env) имеет приоритет.
    """
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

    max_rps = _f(limits.get("max_rps", 5.0), 5.0)
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
    """
    ВНИМАНИЕ:
      - Публичные endpoints работают без ключей.
      - UserData / ордера / account / positionRisk требуют ключей.
    """
    log.info("Binance REST: запуск БЕЗ api_key/secret_key (только public endpoints)")
    return BinanceFuturesREST("", "")


# ============================================================
# DB HELPERS
# ============================================================

def _fetch_active_symbols_map(storage: PostgreSQLStorage, exchange_id: int) -> Dict[str, int]:
    """
    Возвращает {SYMBOL: symbol_id} только для active symbols.
    """
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
            rows = cur.fetchall() or []
    return {str(sym).upper(): int(sid) for sym, sid in rows}


def _make_symbol_ids(symbol_map: Dict[str, int]) -> list[int]:
    return sorted(set(int(x) for x in (symbol_map or {}).values()))


def _make_symbol_id_to_symbol(symbol_map: Dict[str, int]) -> Dict[int, str]:
    """
    Делает reverse-map:
      { 64: "BTCUSDT", ... }
    """
    out: Dict[int, str] = {}
    for sym, sid in (symbol_map or {}).items():
        try:
            out[int(sid)] = str(sym).upper()
        except Exception:
            continue
    return out


# ============================================================
# WATERMARKS (1 SQL query) — ускорение старта
# Под твою реальную схему БД:
#  - candles(exchange_id, symbol_id, interval, open_time)
#  - open_interest(exchange_id, symbol_id, interval, ts)
#  - candles_trades_agg(exchange_id, symbol_id, interval, open_time)
# ============================================================

def _uniq_keep_order(xs: list[str]) -> list[str]:
    out: list[str] = []
    seen = set()
    for x in xs or []:
        s = str(x).strip()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _get_candle_intervals_from_cfg(cfg: dict) -> list[str]:
    md_cfg = cfg.get("market_data") or {}
    itvs = [str(x).strip() for x in (md_cfg.get("candle_intervals") or []) if str(x).strip()]
    return _uniq_keep_order(itvs)


def _get_oi_intervals_from_cfg(cfg: dict) -> list[str]:
    oi_cfg = cfg.get("open_interest") or {}
    itvs = [str(x).strip() for x in (oi_cfg.get("intervals") or []) if str(x).strip()]
    return _uniq_keep_order(itvs)


def _get_trades_agg_intervals_from_cfg(cfg: dict) -> list[str]:
    """
    candles_trades_agg обычно считается по 1m/5m (дельта/CVD).
    Берём из конфига market_trades_all.intervals если есть,
    иначе — по пересечению с candle_intervals.
    """
    mta_cfg = cfg.get("market_trades_all") or {}
    itvs = [str(x).strip() for x in (mta_cfg.get("intervals") or []) if str(x).strip()]
    if itvs:
        return _uniq_keep_order(itvs)

    candle_itvs = _get_candle_intervals_from_cfg(cfg)
    prefer = [x for x in candle_itvs if x in ("1m", "5m")]
    return _uniq_keep_order(prefer or ["1m", "5m"])


def _load_watermarks_one_query(
    storage: PostgreSQLStorage,
    *,
    exchange_id: int,
    candle_intervals: list[str],
    oi_intervals: list[str],
    trades_agg_intervals: list[str],
) -> dict:
    """
    Возвращает dict:
      {
        "candles": { "123": { "5m": ts, "1h": ts, ... }, ... },
        "open_interest": { "123": { "5m": ts, ... }, ... },
        "trades_agg": { "123": { "1m": ts, "5m": ts }, ... },
      }

    Важно:
      - candles: MAX(open_time)
      - open_interest: MAX(ts)
      - candles_trades_agg: MAX(open_time)
    """
    candle_intervals = _uniq_keep_order([x for x in (candle_intervals or []) if x])
    oi_intervals = _uniq_keep_order([x for x in (oi_intervals or []) if x])
    trades_agg_intervals = _uniq_keep_order([x for x in (trades_agg_intervals or []) if x])

    if not candle_intervals and not oi_intervals and not trades_agg_intervals:
        return {"candles": {}, "open_interest": {}, "trades_agg": {}}

    sql = """
    WITH
    cw AS (
      SELECT symbol_id, interval, MAX(open_time) AS last_open_time
      FROM candles
      WHERE exchange_id = %s
        AND (%s::text[] IS NULL OR interval = ANY(%s))
      GROUP BY symbol_id, interval
    ),
    candles_json AS (
      SELECT COALESCE(jsonb_object_agg(symbol_id::text, by_symbol), '{}'::jsonb) AS data
      FROM (
        SELECT symbol_id, jsonb_object_agg(interval, last_open_time) AS by_symbol
        FROM cw
        GROUP BY symbol_id
      ) t
    ),

    oiw AS (
      SELECT symbol_id, interval, MAX(ts) AS last_ts
      FROM open_interest
      WHERE exchange_id = %s
        AND (%s::text[] IS NULL OR interval = ANY(%s))
      GROUP BY symbol_id, interval
    ),
    open_interest_json AS (
      SELECT COALESCE(jsonb_object_agg(symbol_id::text, by_symbol), '{}'::jsonb) AS data
      FROM (
        SELECT symbol_id, jsonb_object_agg(interval, last_ts) AS by_symbol
        FROM oiw
        GROUP BY symbol_id
      ) t
    ),

    tw AS (
      SELECT symbol_id, interval, MAX(open_time) AS last_open_time
      FROM candles_trades_agg
      WHERE exchange_id = %s
        AND (%s::text[] IS NULL OR interval = ANY(%s))
      GROUP BY symbol_id, interval
    ),
    trades_agg_json AS (
      SELECT COALESCE(jsonb_object_agg(symbol_id::text, by_symbol), '{}'::jsonb) AS data
      FROM (
        SELECT symbol_id, jsonb_object_agg(interval, last_open_time) AS by_symbol
        FROM tw
        GROUP BY symbol_id
      ) t
    )

    SELECT
      (SELECT data FROM candles_json)       AS candles,
      (SELECT data FROM open_interest_json) AS open_interest,
      (SELECT data FROM trades_agg_json)    AS trades_agg;
    """

    c_itvs = candle_intervals if candle_intervals else None
    oi_itvs = oi_intervals if oi_intervals else None
    ta_itvs = trades_agg_intervals if trades_agg_intervals else None

    args = (
        int(exchange_id), c_itvs, c_itvs,
        int(exchange_id), oi_itvs, oi_itvs,
        int(exchange_id), ta_itvs, ta_itvs,
    )

    t0 = time.time()
    try:
        with storage.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, args)
                row = cur.fetchone()

        if not row:
            return {"candles": {}, "open_interest": {}, "trades_agg": {}}

        wm = {
            "candles": row[0] or {},
            "open_interest": row[1] or {},
            "trades_agg": row[2] or {},
        }

        dt = time.time() - t0
        log.info(
            "[Watermarks] loaded in %.3fs candles_syms=%d oi_syms=%d trades_agg_syms=%d",
            dt,
            len(wm.get("candles") or {}),
            len(wm.get("open_interest") or {}),
            len(wm.get("trades_agg") or {}),
        )
        return wm

    except Exception:
        dt = time.time() - t0
        log.exception("[Watermarks] failed (ignored) dt=%.3fs -> fallback", dt)
        return {"candles": {}, "open_interest": {}, "trades_agg": {}}


# ============================================================
# SIGNATURE-SAFE CALLS
# ============================================================

def _call_supported(fn, name: str, **kwargs):
    """
    Вызывает функцию, передавая только те kwargs, которые есть в её сигнатуре.
    Если у функции есть **kwargs — передаёт всё.
    """
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
    Стартуем любой коллектор в отдельном starter-thread,
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

    log.info(
        "[UniverseDaemon] старт refresh_sec=%s min_size=%d max_drop_ratio=%.2f",
        refresh_sec, min_size, max_drop_ratio
    )

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
# MARKET TRADES (MarketTradesCollector) - wrapper
# ============================================================

def _run_market_trades_forever(
    *,
    pool,
    rest: BinanceFuturesREST,
    exchange_id: int,
    symbol_id: int,
    interval: str,
    poll_sleep: float,
    overlap_minutes: int,
    stop_event: threading.Event,
) -> None:
    if MarketTradesCollector is None:
        log.warning("[MarketTrades] MarketTradesCollector не найден (collector_market_trades.py отсутствует)")
        return

    try:
        ctor_kwargs = dict(
            pool=pool,
            rest=rest,
            exchange_id=int(exchange_id),
            symbol_id=int(symbol_id),
            interval=str(interval),
            poll_sleep=float(poll_sleep),
            overlap_minutes=int(overlap_minutes),
        )

        try:
            sig = inspect.signature(MarketTradesCollector.__init__)
            allowed = set(sig.parameters.keys())
            allowed.discard("self")
            ctor_kwargs = {k: v for k, v in ctor_kwargs.items() if k in allowed}
        except Exception:
            pass

        collector = MarketTradesCollector(**ctor_kwargs)

        if hasattr(collector, "set_stop_event") and callable(getattr(collector, "set_stop_event")):
            try:
                collector.set_stop_event(stop_event)  # type: ignore
            except Exception:
                pass

        rf = getattr(collector, "run_forever", None)
        if callable(rf):
            _call_supported(rf, "MarketTradesCollector.run_forever", stop_event=stop_event)
            return

        log.error("[MarketTrades] У collector нет метода run_forever()")
    except Exception:
        log.exception("[MarketTrades] не удалось создать/запустить MarketTradesCollector")


# ============================================================
# SYMBOLS REFRESHER
# ============================================================

def _symbols_refresher(
    *,
    storage: PostgreSQLStorage,
    exchange_id: int,
    shared: dict,
    shared_lock: threading.Lock,
    stop_event: threading.Event,
    refresh_sec: int = 60,
) -> None:
    refresh_sec = max(5, int(refresh_sec))
    last_n = -1

    while not stop_event.is_set():
        try:
            smap = _fetch_active_symbols_map(storage, exchange_id)
            sids = _make_symbol_ids(smap)
            id2sym = _make_symbol_id_to_symbol(smap)

            with shared_lock:
                shared["symbol_map"] = smap
                shared["symbol_ids"] = sids
                shared["symbol_id_to_symbol"] = id2sym

            if len(sids) != last_n:
                last_n = len(sids)
                log.info("[SymbolsRefresher] active symbols=%d", last_n)

        except Exception:
            log.exception("[SymbolsRefresher] failed (ignored)")

        stop_event.wait(refresh_sec)


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    _setup_logging()
    cfg = _load_cfg()

    log.info("=== RUN MARKET DATA START ===")
    log.info("Config: %s", cfg.get("_path"))

    dsn = os.environ.get("PG_DSN") or os.environ.get("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("PG_DSN or POSTGRES_DSN env is required")

    pool = create_pool(dsn)
    storage = PostgreSQLStorage(pool=pool)
    log.info("PostgreSQL storage initialized")

    exchange_id = int(cfg.get("exchange_id", 1))

    _apply_rest_limits_from_cfg(cfg)
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
    # Universe daemon
    # --------------------------------------------------------
    universe_cfg = cfg.get("universe") or {}
    if universe_cfg.get("enabled", True):
        if not _has_method(rest, "fetch_exchange_info"):
            log.warning("[Universe] выключено: нет BinanceFuturesREST.fetch_exchange_info()")
        else:
            if bool(universe_cfg.get("seed_on_start", False)):
                threading.Thread(
                    target=_universe_seed_worker,
                    kwargs=dict(rest=rest, storage=storage, exchange_id=exchange_id, universe_cfg=universe_cfg),
                    name="UniverseSeed",
                    daemon=True,
                ).start()

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
    # Shared symbols state + refresher
    # --------------------------------------------------------
    shared_lock = threading.Lock()
    shared = {"symbol_map": {}, "symbol_ids": [], "symbol_id_to_symbol": {}}

    try:
        smap0 = _fetch_active_symbols_map(storage, exchange_id)
        sids0 = _make_symbol_ids(smap0)
        id2sym0 = _make_symbol_id_to_symbol(smap0)

        with shared_lock:
            shared["symbol_map"] = smap0
            shared["symbol_ids"] = sids0
            shared["symbol_id_to_symbol"] = id2sym0

        log.info("[Universe] active symbols in DB: %d", len(smap0))
        log.info("[Universe] symbol_ids prepared: %d", len(sids0))
    except Exception:
        log.exception("[Universe] initial symbols load failed")

    sym_refresh_cfg = cfg.get("symbols_refresh") or {}
    sym_refresh_sec = int(sym_refresh_cfg.get("refresh_sec", 60))

    threading.Thread(
        target=_symbols_refresher,
        kwargs=dict(
            storage=storage,
            exchange_id=exchange_id,
            shared=shared,
            shared_lock=shared_lock,
            stop_event=stop_event,
            refresh_sec=sym_refresh_sec,
        ),
        name="SymbolsRefresher",
        daemon=True,
    ).start()

    _stagger_wait(stop_event, cfg, "after_symbols_refresher", 0.25)

    # --------------------------------------------------------
    # helper snapshots
    # --------------------------------------------------------
    def _symbol_map_snapshot() -> Dict[str, int]:
        """{SYMBOL: symbol_id}"""
        with shared_lock:
            smap = shared.get("symbol_map") or {}
            return dict(smap)

    def _symbol_ids_snapshot() -> list[int]:
        """list[symbol_id]"""
        with shared_lock:
            sids = shared.get("symbol_ids") or []
            return [int(x) for x in sids]

    def _symbol_id_to_symbol_snapshot() -> Dict[int, str]:
        """{symbol_id: SYMBOL}"""
        with shared_lock:
            m = shared.get("symbol_id_to_symbol") or {}
            return {int(k): str(v).upper() for k, v in m.items()}

    # --------------------------------------------------------
    # SymbolFilters
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
                symbol_ids=_symbol_map_snapshot(),  # оставляем как было (у тебя уже работает)
                interval_sec=int(sf_cfg.get("refresh_sec", 3600)),
                do_seed_on_start=bool(sf_cfg.get("seed_on_start", True)),
                stop_event=stop_event,
            )
    else:
        log.warning("SymbolFilters disabled by config")

    _stagger_wait(stop_event, cfg, "after_symbol_filters", 0.75)

    # --------------------------------------------------------
    # WATERMARKS (ускорение старта)
    # --------------------------------------------------------
    candle_itvs = _get_candle_intervals_from_cfg(cfg)
    oi_itvs = _get_oi_intervals_from_cfg(cfg)
    trades_agg_itvs = _get_trades_agg_intervals_from_cfg(cfg)

    watermarks = _load_watermarks_one_query(
        storage,
        exchange_id=exchange_id,
        candle_intervals=candle_itvs,
        oi_intervals=oi_itvs,
        trades_agg_intervals=trades_agg_itvs,
    )

    # --------------------------------------------------------
    # Candles
    # --------------------------------------------------------
    md_cfg = cfg.get("market_data") or {}
    if md_cfg.get("enabled", True):
        intervals = [str(x).strip() for x in (md_cfg.get("candle_intervals") or []) if str(x).strip()]
        if not intervals:
            log.warning("Candles включены, но candle_intervals пуст -> пропуск")
        elif not _has_method(rest, "klines"):
            log.warning("[Candles] выключено: нет BinanceFuturesREST.klines()")
        else:
            _safe_start_async(
                "Candles collector",
                start_candles_collector,
                storage=storage,
                rest=rest,
                exchange_id=int(exchange_id),
                symbol_ids=_symbol_map_snapshot(),  # {SYMBOL: symbol_id}
                intervals=intervals,
                update_open_candle=bool(md_cfg.get("update_open_candle", True)),
                stop_event=stop_event,
                watermarks=watermarks,
                seed_on_start=bool(md_cfg.get("seed_on_start", True)),
                seed_days=int(md_cfg.get("seed_days", 2)),
                seed_limit=int(md_cfg.get("seed_limit", 1500)),
                seed_max_pages_per_symbol=int(md_cfg.get("seed_max_pages_per_symbol", 3)),
                seed_symbols_per_cycle=int(md_cfg.get("seed_symbols_per_cycle", 5)),
                per_request_sleep_sec=float(md_cfg.get("per_request_sleep_sec", 0.20)),
                use_ws=bool(md_cfg.get("use_ws", True)),
                ws_max_streams_per_conn=int(md_cfg.get("ws_max_streams_per_conn", 120)),
            )
    else:
        log.warning("Candles disabled by config")

    _stagger_wait(stop_event, cfg, "after_candles", 1.00)

    # --------------------------------------------------------
    # MarketTradesCollector (1 символ)
    # --------------------------------------------------------
    mt_cfg = cfg.get("market_trades") or {}
    if mt_cfg.get("enabled", False):
        if MarketTradesCollector is None:
            log.warning("[MarketTrades] включён, но collector_market_trades.py не найден")
        else:
            mt_symbol_id = int(mt_cfg.get("symbol_id", 1))
            mt_interval = str(mt_cfg.get("interval", "5m"))
            mt_poll_sleep = float(mt_cfg.get("poll_sleep", 1.0))
            mt_overlap = int(mt_cfg.get("overlap_minutes", 120))

            threading.Thread(
                target=_run_market_trades_forever,
                kwargs=dict(
                    pool=pool,
                    rest=rest,
                    exchange_id=exchange_id,
                    symbol_id=mt_symbol_id,
                    interval=mt_interval,
                    poll_sleep=mt_poll_sleep,
                    overlap_minutes=mt_overlap,
                    stop_event=stop_event,
                ),
                name=f"MarketTrades:{mt_symbol_id}:{mt_interval}",
                daemon=True,
            ).start()

            log.info(
                "[MarketTrades] started symbol_id=%s interval=%s poll_sleep=%.2f overlap_minutes=%d",
                mt_symbol_id, mt_interval, mt_poll_sleep, mt_overlap
            )
    else:
        log.info("MarketTrades disabled by config")

    _stagger_wait(stop_event, cfg, "after_market_trades", 0.75)

    # --------------------------------------------------------
    # MarketTrades ALL symbols (aggTrades)
    # --------------------------------------------------------
    mta_cfg = cfg.get("market_trades_all") or {}
    if mta_cfg.get("enabled", False):
        if start_market_trades_all_collector is None:
            log.warning("[MarketTradesAll] enabled but collector_market_trades_all.py not found")
        else:
            _safe_start_async(
                "MarketTradesAll collector",
                start_market_trades_all_collector,
                pool=pool,
                rest=rest,
                exchange_id=exchange_id,
                cfg=mta_cfg,
                stop_event=stop_event,
                watermarks=watermarks,
            )
    else:
        log.info("MarketTradesAll disabled by config")

    # --------------------------------------------------------
    # CandlesActive
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
                symbol_ids=_symbol_map_snapshot(),
            )
    else:
        log.info("CandlesActive disabled by config")

    _stagger_wait(stop_event, cfg, "after_candles_active", 1.00)

    # --------------------------------------------------------
    # Open Interest  ✅✅✅ FIX HERE
    # --------------------------------------------------------
    oi_cfg = cfg.get("open_interest") or {}
    if oi_cfg.get("enabled", False):
        if start_open_interest_collector is None:
            log.warning("OpenInterest включён, но collector_open_interest.py не найден")
        elif not _has_method(rest, "open_interest_hist") and not _has_method(rest, "open_interest"):
            log.warning("[OpenInterest] выключено: нет REST метода open_interest_hist/open_interest()")
        else:
            _safe_start_async(
                "OpenInterest collector",
                start_open_interest_collector,
                rest=rest,
                storage=storage,
                exchange_id=exchange_id,

                # ВАЖНО: list[int]
                symbol_ids=_symbol_ids_snapshot(),

                # ✅ reverse-map id -> symbol достаточно
                symbol_id_to_symbol=_symbol_id_to_symbol_snapshot(),

                cfg=oi_cfg,
                stop_event=stop_event,

                # ✅ watermarks поддерживаются
                watermarks=watermarks,
            )
    else:
        log.info("OpenInterest disabled by config")

    _stagger_wait(stop_event, cfg, "after_open_interest", 1.00)

    # --------------------------------------------------------
    # Funding
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
                    symbol_ids=_symbol_map_snapshot(),
                    cfg=f_cfg,
                    stop_event=stop_event,
                )
    else:
        log.info("Funding disabled by config")

    _stagger_wait(stop_event, cfg, "after_funding", 0.75)

    # --------------------------------------------------------
    # Ticker 24h
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
                symbol_ids=_symbol_map_snapshot(),
                cfg=t24_cfg,
                stop_event=stop_event,
            )
    else:
        log.info("Ticker24h disabled by config")

    _stagger_wait(stop_event, cfg, "after_ticker_24h", 0.75)

    # --------------------------------------------------------
    # Liquidations
    # --------------------------------------------------------
    liq_cfg = cfg.get("liquidations") or {}
    if liq_cfg.get("enabled", False):
        if start_liquidations_collector is None:
            log.warning("Liquidations включён, но collector_liquidations.py не найден")
        else:
            _safe_start_async(
                "Liquidations collector",
                start_liquidations_collector,
                storage=storage,
                exchange_id=exchange_id,
                symbol_ids=_symbol_map_snapshot(),
                stop_event=stop_event,
                flush_sec=float(liq_cfg.get("flush_sec", 2.0)),
            )
    else:
        log.info("Liquidations disabled by config")

    _stagger_wait(stop_event, cfg, "after_liquidations", 0.50)

    # --------------------------------------------------------
    # MarketState5mWorker (expects list[int])
    # --------------------------------------------------------
    ms5_cfg = cfg.get("market_state_5m", {}) or {}
    if ms5_cfg.get("enabled", True):
        poll_sec = float(ms5_cfg.get("poll_sec", 10.0))
        symbols_per_cycle = int(ms5_cfg.get("symbols_per_cycle", 40))
        lookback_minutes = int(ms5_cfg.get("lookback_minutes", 180))
        refresh_symbols_sec = int(ms5_cfg.get("refresh_symbols_sec", 600))

        w = MarketState5mWorker(
            pool=pool,
            exchange_id=int(exchange_id),
            symbol_ids=_symbol_ids_snapshot(),
            poll_sec=poll_sec,
            symbols_per_cycle=symbols_per_cycle,
            lookback_minutes=lookback_minutes,
            refresh_symbols_sec=refresh_symbols_sec,
            stop_event=stop_event,
        )
        w.start()
        log.info(
            "[MarketState5m] worker started poll=%.2fs batch=%d lookback=%d refresh_symbols_sec=%d",
            poll_sec, symbols_per_cycle, lookback_minutes, refresh_symbols_sec
        )
    else:
        log.info("[MarketState5m] disabled by config")

    # --------------------------------------------------------
    # Candles Gap Repair
    # --------------------------------------------------------
    gpr_cfg = cfg.get("candles_gap_repair") or {}
    if gpr_cfg.get("enabled", False):
        _safe_start_async(
            "CandlesGapRepair collector",
            start_candles_gap_repair_collector,
            cfg=cfg,
            rest=rest,
            storage=storage,
            exchange_id=exchange_id,
            stop_event=stop_event,
        )
    else:
        log.info("CandlesGapRepair disabled by config")

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
                smap = _symbol_map_snapshot()
                sids = _symbol_ids_snapshot()
                log.info(
                    "[HEARTBEAT] alive threads=%d active_symbols=%d symbol_ids=%d",
                    threading.active_count(),
                    len(smap),
                    len(sids),
                )
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
