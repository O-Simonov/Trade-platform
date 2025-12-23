# src/platform/run_instances.py
from __future__ import annotations

import os
import logging
from pathlib import Path
import yaml
from dotenv import load_dotenv

from src.platform.exchanges.registry import build_exchange
from src.platform.exchanges.binance.collector_exchange_info import sync_exchange_info

from src.platform.core.engine.instance import TradingInstance
from src.platform.core.engine.runner import run_instances
from src.platform.core.risk.risk_engine import RiskLimits

from src.platform.core.strategy.one_shot_test import OneShotTestStrategy

from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage
from src.platform.data.retention.retention_worker import RetentionWorker



# DRY_RUN=1 -> dry run ON (no real orders)
# DRY_RUN=0 -> real orders enabled
DRY_RUN: bool = os.getenv("DRY_RUN", "1") == "1"


def _load_yaml(path: Path) -> dict:
    if not path.exists():
        raise SystemExit(f"Config file not found: {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("platform.run_instances")

    logger.info("=== RUN INSTANCES START ===")
    logger.warning("DRY_RUN=%s (%s)", DRY_RUN, "NO REAL ORDERS" if DRY_RUN else "REAL ORDERS ENABLED")

    exchange_id: int | None = None

    load_dotenv()
    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise SystemExit("PG_DSN env var is required")

    root = Path(__file__).resolve().parents[2]
    cfg_root = _load_yaml(root / "config" / "strategies.yaml")
    instances_cfg = cfg_root.get("instances") or []
    if not instances_cfg:
        raise SystemExit("No instances defined in strategies.yaml")

    pool = create_pool(dsn)
    store = PostgreSQLStorage(pool)
    logger.info("PostgreSQL storage initialized")

    # -------------------------------------------------------------------
    # REGISTRY: exchange/account/symbol -> ids
    # -------------------------------------------------------------------
    ids_by_key: dict[tuple[str, str], dict] = {}

    for icfg in instances_cfg:
        exchange = icfg["exchange"]
        account = icfg["account"]
        symbols = [s.upper() for s in icfg.get("symbols", [])]

        ids = store.ensure_exchange_account_symbol(
            exchange=exchange,
            account=account,
            symbols=symbols,
        )
        ids_by_key[(exchange, account)] = ids

        logger.info("Registry OK: exchange=%s account=%s symbols=%s", exchange, account, ",".join(symbols))

    # -------------------------------------------------------------------
    # BOOTSTRAP exchangeInfo -> symbol_filters (лучше 1 раз на старте)
    # -------------------------------------------------------------------
    binance_keys = [(ex, acc) for (ex, acc) in ids_by_key.keys() if ex == "binance"]

    if binance_keys:
        _, rest_account = binance_keys[0]
        binance_ex = build_exchange("binance")

        # ВАЖНО: REST клиент строим по аккаунту (ключи)
        rest = binance_ex._rest(rest_account)

        exchange_id = ids_by_key[("binance", rest_account)]["_exchange_id"]

        # объединяем symbols всех binance-инстансов
        symbol_ids: dict[str, int] = {}
        for (ex, acc) in binance_keys:
            ids = ids_by_key[(ex, acc)]
            for k, v in ids.items():
                if not k.startswith("_"):
                    symbol_ids[k] = v
        try:
            sync_exchange_info(
                binance_rest=rest,
                storage=store,
                exchange_id=exchange_id,
                symbol_ids=symbol_ids,
            )
            logger.info("ExchangeInfo synced: exchange=binance symbols=%s", ",".join(sorted(symbol_ids.keys())))
        except Exception:
            logger.exception("ExchangeInfo sync failed (will continue without refresh)")

    # -----------------------------------------------------------------------
    # RETENTION (cleanup old data)
    # -----------------------------------------------------------------------
    retention_cfg = cfg_root.get("retention", {}) or {}

    if exchange_id is not None:
        retention = RetentionWorker(
            storage=store,
            exchange_id=exchange_id,
            cfg=retention_cfg,
            run_sec=int(retention_cfg.get("run_sec", 3600)),
        )
        retention.start()
        logger.info("RetentionWorker started: exchange_id=%s", exchange_id)
    else:
        logger.warning("RetentionWorker NOT started: exchange_id is None (no binance instances?)")

    # -------------------------------------------------------------------
    # BUILD INSTANCES
    # -------------------------------------------------------------------
    instances: list[TradingInstance] = []

    for icfg in instances_cfg:
        exchange_name = icfg["exchange"]
        account = icfg["account"]
        role = icfg.get("role", "MIXED")
        symbols = [s.upper() for s in icfg.get("symbols", [])]

        params = icfg.get("params") or {}
        risk_cfg = icfg.get("risk") or {}

        ex = build_exchange(exchange_name)
        ids = ids_by_key[(exchange_name, account)]

        # 🔥 ВАЖНО: bind exchange к storage + ids
        ex.bind(
            storage=store,
            exchange_id=ids["_exchange_id"],
            symbol_ids={k: v for k, v in ids.items() if not k.startswith("_")},
        )

        # --- strategy ---
        strategy_name = str(icfg.get("strategy") or "").strip()

        strat = None

        if strategy_name == "one_shot_test":
            if role == "BASE":
                strat = OneShotTestStrategy(
                    exchange=exchange_name,
                    account=account,
                    qty=float(params.get("qty", 0.1)),
                    hold_sec=float(params.get("hold_sec", 10)),
                )
            else:
                logger.info(
                    "Skip strategy for non-BASE role: exchange=%s account=%s role=%s strategy=%s",
                    exchange_name, account, role, strategy_name
                )

        else:
            raise SystemExit(f"Unknown strategy: {strategy_name}")

        # 🔥 КЛЮЧЕВОЕ: если стратегии нет — не создаём instance и идём дальше
        if strat is None:
            logger.info(
                "Skip instance (no strategy): %s / %s | role=%s",
                exchange_name, account, role
            )
            continue

        limits = RiskLimits(
            max_open_positions=int(risk_cfg.get("max_open_positions", 10)),
            max_notional_usdt=float(risk_cfg.get("max_notional_usdt", 1e9)),
            max_order_qty=float(risk_cfg.get("max_order_qty", 1e9)),
            max_daily_loss_usdt=float(risk_cfg.get("max_daily_loss_usdt", 1e9)),
            max_drawdown_pct=float(risk_cfg.get("max_drawdown_pct", 1e9)),
            min_available_margin_pct=float(risk_cfg.get("min_available_margin_pct", 0.0)),
            max_margin_used_pct=float(risk_cfg.get("max_margin_used_pct", 1e9)),
        )

        inst = TradingInstance(
            exchange=ex,
            storage=store,
            strategy=strat,
            account=account,
            role=role,
            symbols=symbols,
            candle_intervals=icfg.get("candle_intervals") or [],
            funding_poll_sec=float(icfg.get("funding_poll_sec", 120)),
            oms_reconcile_sec=float(icfg.get("oms_reconcile_sec", 15)),
            oms_pending_timeout_sec=float(icfg.get("oms_pending_timeout_sec", 20)),
            ids=ids,
            base_ref=icfg.get("base_ref"),
            hedge_ratio=icfg.get("hedge_ratio"),
            risk_limits=limits,
            dry_run=DRY_RUN,
        )

        instances.append(inst)
        logger.info("Instance ready: %s / %s | symbols=%s | role=%s",
                    exchange_name, account, ",".join(symbols), role)

    logger.info("Starting %d trading instance(s)", len(instances))
    run_instances(instances)


if __name__ == "__main__":
    main()
