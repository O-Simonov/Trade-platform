# src/platform/run_trade_liquidation.py
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

import yaml

from src.platform.config.env import (
    get_environment,
    get_log_level,
    get_pg_dsn,
    require,
)
from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage
from src.platform.notifications.telegram import load_dotenv_file
from src.platform.traders.trade_liquidation import (
    TradeLiquidation,
    TradeLiquidationParams,
)

log = logging.getLogger("platform.run_trade_liquidation")


def _setup_logging() -> None:
    level_name = get_log_level()
    level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _resolve_cfg_path() -> Path:
    raw_path = require("TRADE_LIQUIDATION_CONFIG")
    path = Path(raw_path).expanduser()

    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()

    if not path.exists():
        raise SystemExit(f"Config not found: {path}")

    if not path.is_file():
        raise SystemExit(f"TRADE_LIQUIDATION_CONFIG is not a file: {path}")

    return path


def _load_yaml(path: Path) -> Dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError("Config root must be a mapping (dict).")
    return data


def _coerce_float(v: Any, default: float) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _ping_db(store: PostgreSQLStorage) -> Tuple[datetime, int]:
    """Проверка связи с БД + быстрые метрики для логов.

    В PostgreSQLStorage нет fetch_value(), поэтому используем query_one().
    """
    r_now = store.query_one("SELECT now()")
    now = r_now[0] if r_now else datetime.now(timezone.utc)

    r_total = store.query_one("SELECT COUNT(*) FROM signals")
    total = int(r_total[0] if r_total else 0)
    return now, total


def _as_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _as_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _extract_stats(res: Any) -> Tuple[int, int, int, int, int, float]:
    """Нормализует результат trader.run_once() к счётчикам.

    Возвращает:
      found, created, closed, checked, expired, elapsed

    Поддерживаем:
      1) новый dict-формат TradeLiquidation.run_once()
      2) старый tuple/list формат (found, created, ...)
    """
    found = created = closed = checked = expired = 0
    elapsed = 0.0

    if isinstance(res, dict):
        close_stats = res.get("closed") if isinstance(res.get("closed"), dict) else {}
        open_stats = res.get("opened") if isinstance(res.get("opened"), dict) else {}

        checked = _as_int(close_stats.get("checked"), 0)
        closed = _as_int(close_stats.get("closed"), 0)

        created = _as_int(open_stats.get("opened"), 0)
        found = _as_int(open_stats.get("considered"), 0)

        expired = _as_int(res.get("expired"), 0)

        if "found" in res:
            found = _as_int(res.get("found"), found)
        if "created" in res:
            created = _as_int(res.get("created"), created)
        if "elapsed_s" in res:
            elapsed = _as_float(res.get("elapsed_s"), 0.0)
        else:
            elapsed = _as_float(res.get("elapsed_sec"), 0.0)

        if not isinstance(res.get("closed"), dict) and "closed" in res:
            closed = _as_int(res.get("closed"), closed)
        if not isinstance(res.get("checked"), dict) and "checked" in res:
            checked = _as_int(res.get("checked"), checked)

        return found, created, closed, checked, expired, elapsed

    if isinstance(res, (tuple, list)):
        if len(res) >= 1:
            found = _as_int(res[0], 0)
        if len(res) >= 2:
            created = _as_int(res[1], 0)
        if len(res) >= 3:
            closed = _as_int(res[2], 0)
        if len(res) >= 4:
            checked = _as_int(res[3], 0)
        return found, created, closed, checked, expired, elapsed

    return found, created, closed, checked, expired, elapsed


def main() -> None:
    _setup_logging()

    log.info("=== RUN TRADE LIQUIDATION START ===")

    environment = get_environment()

    # Для live-режима оставляем совместимость:
    # ключи могут лежать в .env, а стратегия ждёт их в ENV.
    try:
        loaded = int(load_dotenv_file(".env", override=False) or 0)
        if loaded:
            log.info("Loaded .env: %s", str(Path(".env").resolve()))
    except Exception:
        log.debug("Failed to load .env", exc_info=True)

    cfg_file = _resolve_cfg_path()
    log.info("Environment: %s", environment)
    log.info("TRADE_LIQUIDATION_CONFIG=%s", str(cfg_file))

    cfg = _load_yaml(cfg_file)

    restart_interval_minutes = _coerce_float(cfg.get("restart_interval_minutes", 1), 1.0)
    traders = cfg.get("traders") or []
    if not isinstance(traders, list):
        raise ValueError("Config: traders must be a list")

    enabled = [t for t in traders if isinstance(t, dict) and bool(t.get("enabled", True))]
    log.info("Traders in config: %d (enabled=%d)", len(traders), len(enabled))
    log.info("------------------------------------------------------------")

    dsn = get_pg_dsn()
    pool = create_pool(dsn)
    store = PostgreSQLStorage(pool)

    try:
        now, total = _ping_db(store)
        log.info("DB ping OK: now=%s | signals_total=%s", str(now), str(total))
    except Exception as e:
        log.exception("DB ping failed: %s", e)

    traders_runtime: Dict[str, Tuple[str, TradeLiquidation]] = {}

    while True:
        cycle_started = _utc_now()

        for t in enabled:
            name = str(t.get("name", "")).strip() or "trade_liquidation"
            version = str(t.get("version", "")).strip() or "unknown"
            params_dict = t.get("params") or {}
            if not isinstance(params_dict, dict):
                log.error("Trader %s: params must be a dict", name)
                continue

            try:
                rt = traders_runtime.get(name)
                if rt is None:
                    params = TradeLiquidationParams.from_dict(params_dict)
                    traders_runtime[name] = (version, TradeLiquidation(store=store, params=params))
                    log.info("Trader initialized: name=%s version=%s", name, version)
                else:
                    traders_runtime[name] = (version, rt[1])

                res = traders_runtime[name][1].run_once()
                found, created, closed, checked, expired, elapsed = _extract_stats(res)

                if expired:
                    log.info(
                        "Run trader=%s v=%s found=%d created=%d closed=%d checked=%d expired=%d elapsed=%.2fs",
                        name,
                        version,
                        found,
                        created,
                        closed,
                        checked,
                        expired,
                        elapsed,
                    )
                else:
                    log.info(
                        "Run trader=%s v=%s found=%d created=%d closed=%d checked=%d elapsed=%.2fs",
                        name,
                        version,
                        found,
                        created,
                        closed,
                        checked,
                        elapsed,
                    )
            except Exception as e:
                log.exception("Trader %s failed: %s", name, e)

        sleep_s = max(1.0, restart_interval_minutes * 60.0)
        log.info(
            "Cycle done started_at=%s sleep=%.1fs restart_interval_minutes=%s",
            cycle_started.isoformat(),
            sleep_s,
            str(restart_interval_minutes),
        )
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()