# src/platform/run_market_data.py
from __future__ import annotations

import inspect
import logging
import os
import threading
import time
from pathlib import Path
from typing import Dict, Optional

import yaml

from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage
from src.platform.exchanges.binance.rest import BinanceFuturesREST
from src.platform.exchanges.binance.collector_universe import sync_tradable_usdtm_perpetual_symbols
from src.platform.exchanges.binance.collector_symbol_filters import start_symbol_filters_collector
from src.platform.exchanges.binance.collector_candles import start_candles_collector
from src.platform.data.retention.retention_worker import RetentionWorker



# --- SAFE IMPORTS ---
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


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(threadName)s | %(message)s",
    )


def _resolve_cfg_path() -> Path:
    """
    1) MARKETDATA_CONFIG env (absolute/relative)
    2) ./config/market_data.yaml (project root)
    3) fallback: рядом с src/platform/run_market_data.py -> ../../../config/market_data.yaml
    """
    p = os.environ.get("MARKETDATA_CONFIG")
    if p:
        return Path(p).expanduser().resolve()

    cwd_candidate = (Path.cwd() / "config" / "market_data.yaml").resolve()
    if cwd_candidate.exists():
        return cwd_candidate

    here = Path(__file__).resolve()
    return (here.parents[2] / "config" / "market_data.yaml").resolve()


def _load_cfg() -> dict:
    path = _resolve_cfg_path()
    if not path.exists():
        raise FileNotFoundError(f"MarketData config not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    cfg["_path"] = str(path)
    return cfg


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


def _call_supported(fn, **kwargs):
    """
    Вызов функции только с теми kwargs, которые реально есть в сигнатуре.
    Это спасает от рассинхрона версий collector/storage.
    """
    try:
        sig = inspect.signature(fn)
        supported = {k: v for k, v in kwargs.items() if k in sig.parameters}
        return fn(**supported)
    except Exception:
        return fn(**kwargs)


def _universe_daemon(*, rest, storage, exchange_id: int, cfg: dict, stop_event: threading.Event) -> None:
    refresh_sec = int(cfg.get("refresh_sec", 21600))
    min_size = int(cfg.get("min_size", 200))
    max_drop_ratio = float(cfg.get("max_drop_ratio", 0.30))

    log.info(
        "[UniverseDaemon] started refresh_sec=%s seed=%s min_size=%d max_drop_ratio=%.2f",
        refresh_sec,
        False,
        min_size,
        max_drop_ratio,
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
            log.warning("[UniverseDaemon] error: %s", e, exc_info=True)
            stop_event.wait(backoff)
            backoff = min(backoff * 2.0, 300.0)


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
    rest = BinanceFuturesREST(api_key="", api_secret="")  # public REST

    stop_event = threading.Event()

    # 1) Universe initial sync + daemon
    universe_cfg = cfg.get("universe") or {}
    if universe_cfg.get("enabled", True):
        if bool(universe_cfg.get("seed_on_start", True)):
            try:
                sync_tradable_usdtm_perpetual_symbols(
                    binance_rest=rest,
                    storage=storage,
                    exchange_id=exchange_id,
                    min_size=int(universe_cfg.get("min_size", 200)),
                    max_drop_ratio=float(universe_cfg.get("max_drop_ratio", 0.30)),
                )
            except Exception:
                log.exception("[Universe] initial sync failed (ignored)")

        t = threading.Thread(
            target=_universe_daemon,
            kwargs=dict(rest=rest, storage=storage, exchange_id=exchange_id, cfg=universe_cfg, stop_event=stop_event),
            daemon=True,
            name="UniverseDaemon",
        )
        t.start()
        log.info("Universe daemon started")
    else:
        log.warning("Universe daemon disabled by config")

    # universe from DB
    symbol_ids = _fetch_active_symbols_map(storage, exchange_id)
    log.info("[Universe] active symbols in DB: %d", len(symbol_ids))

    # 2) Symbol filters
    sf_cfg = cfg.get("symbol_filters") or {}
    if sf_cfg.get("enabled", True):
        start_symbol_filters_collector(
            rest=rest,
            storage=storage,
            exchange_id=exchange_id,
            symbol_ids=symbol_ids,
            interval_sec=int(sf_cfg.get("refresh_sec", 3600)),
            do_seed_on_start=bool(sf_cfg.get("seed_on_start", True)),
            stop_event=stop_event,
        )
        log.info("SymbolFilters collector started")
    else:
        log.warning("SymbolFilters collector disabled by config")

    # 3) Candles
    md_cfg = cfg.get("market_data") or {}
    if md_cfg.get("enabled", True):
        intervals = list(md_cfg.get("candle_intervals") or [])
        if not intervals:
            log.warning("Candles enabled, but candle_intervals is empty -> skip candles collector")
        else:
            _call_supported(
                start_candles_collector,
                rest=rest,
                storage=storage,
                exchange_id=exchange_id,
                symbol_ids=symbol_ids,
                intervals=intervals,
                poll_sec=float(md_cfg.get("poll_sec", 60)),
                symbols_per_cycle=int(md_cfg.get("symbols_per_cycle", 2)),
                per_request_sleep_sec=float(md_cfg.get("req_sleep_sec", 0.5)),
                seed_on_start=bool(md_cfg.get("seed_on_start", True)),
                seed_days=int(md_cfg.get("seed_days", 0)),
                seed_chunk_limit=int(md_cfg.get("seed_chunk_limit", 1500)),
                seed_limit=int(md_cfg.get("seed_limit", 0)),
                loop_limit=int(md_cfg.get("loop_limit", 3)),
                stop_event=stop_event,
            )
            log.info("Candles collector started")
    else:
        log.warning("Candles collector disabled by config")

    # 3.1) CandlesActive
    mda_cfg = cfg.get("market_data_active") or {}
    if mda_cfg.get("enabled", False):
        if start_candles_active_collector is None:
            log.warning("CandlesActive enabled in config, but collector_candles_active.py missing")
        else:
            extra_symbols = mda_cfg.get("extra_symbols")
            if extra_symbols is None:
                extra_symbols_list = []
            elif isinstance(extra_symbols, (list, tuple)):
                extra_symbols_list = [str(x).upper().strip() for x in extra_symbols if str(x).strip()]
            else:
                log.warning("market_data_active.extra_symbols must be a list -> ignored")
                extra_symbols_list = []

            start_candles_active_collector(
                rest=rest,
                storage=storage,
                exchange_id=exchange_id,
                cfg=mda_cfg,
                extra_symbols=extra_symbols_list,
                stop_event=stop_event,
            )
            log.info("CandlesActive collector started")
    else:
        log.info("CandlesActive collector disabled by config")

    # 4) Open Interest
    oi_cfg = cfg.get("open_interest") or {}
    if oi_cfg.get("enabled", False):
        if start_open_interest_collector is None:
            log.warning("OpenInterest enabled in config, but collector_open_interest.py missing")
        else:
            start_open_interest_collector(
                rest=rest,
                storage=storage,
                exchange_id=exchange_id,
                symbol_ids=symbol_ids,
                cfg=oi_cfg,
                stop_event=stop_event,
            )
            log.info("OpenInterest collector started")

    # 5) Funding
    f_cfg = cfg.get("funding") or {}
    if f_cfg.get("enabled", False):
        if start_funding_collector is None:
            log.warning("Funding enabled in config, but collector_funding.py missing")
        else:
            start_funding_collector(
                rest=rest,
                storage=storage,
                exchange_id=exchange_id,
                symbol_ids=symbol_ids,
                cfg=f_cfg,
                stop_event=stop_event,
            )
            log.info("Funding collector started")

    # 6) Ticker 24h snapshot
    t24_cfg = cfg.get("ticker_24h") or {}
    if t24_cfg.get("enabled", False):
        if start_ticker_24h_collector is None:
            log.warning("Ticker24h enabled in config, but collector_ticker_24h.py / start_ticker_24h_collector missing")
        else:
            start_ticker_24h_collector(
                rest=rest,
                storage=storage,
                exchange_id=exchange_id,
                symbol_ids=symbol_ids,
                cfg=t24_cfg,
                stop_event=stop_event,
            )
            log.info("Ticker24h collector started")

    # 7) Retention
    rw: Optional[RetentionWorker] = None
    r_cfg = cfg.get("retention") or {}
    if r_cfg.get("enabled", True):
        run_sec = int(r_cfg.get("run_sec", 3600))  # Интервал работы для retention (в секундах)
        rw = RetentionWorker(storage=storage, exchange_id=exchange_id, cfg=r_cfg, run_sec=run_sec)
        rw.start()  # Запускаем worker в отдельном потоке
        log.info("RetentionWorker started run_sec=%s", run_sec)
    else:
        log.warning("Retention disabled by config")

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        log.warning("CTRL+C -> stopping...")
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
