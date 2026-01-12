# src/platform/run_instances.py
from __future__ import annotations

import os
import logging
import threading
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from src.platform.exchanges.registry import build_exchange
from src.platform.core.engine.instance import TradingInstance
from src.platform.core.engine.runner import run_instances
from src.platform.core.risk.risk_engine import RiskLimits
from src.platform.core.strategy.one_shot_test import OneShotTestStrategy

from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage


# -------------------------------------------------------------------------
# helpers
# -------------------------------------------------------------------------
def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Config file not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return data or {}


def main() -> None:
    # -------------------------------------------------------------------------
    # ENV (.env first) + DRY_RUN
    # -------------------------------------------------------------------------
    load_dotenv()
    dry_run: bool = os.getenv("DRY_RUN", "1") == "1"

    # -------------------------------------------------------------------------
    # LOGGING
    # -------------------------------------------------------------------------
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("platform.run_instances")

    logger.info("=== RUN INSTANCES START ===")
    logger.warning(
        "DRY_RUN=%s (%s)",
        dry_run,
        "NO REAL ORDERS" if dry_run else "REAL ORDERS ENABLED",
    )

    # -------------------------------------------------------------------------
    # CONFIG
    # -------------------------------------------------------------------------
    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise SystemExit("PG_DSN env var is required")

    root = Path(__file__).resolve().parents[2]  # src/platform/ -> src/
    cfg_root = _load_yaml(root / "config" / "strategies.yaml")

    instances_cfg = cfg_root.get("instances") or []
    if not instances_cfg:
        raise SystemExit("No instances defined in strategies.yaml")

    # -------------------------------------------------------------------------
    # STORAGE
    # -------------------------------------------------------------------------
    pool = create_pool(dsn)
    store = PostgreSQLStorage(pool)
    logger.info("PostgreSQL storage initialized")

    # -------------------------------------------------------------------------
    # REGISTRY: ensure exchange/account/symbol ids
    # -------------------------------------------------------------------------
    ids_by_key: dict[tuple[str, str], dict[str, Any]] = {}

    for icfg in instances_cfg:
        exchange = str(icfg["exchange"])
        account = str(icfg["account"])
        symbols = [str(s).upper() for s in (icfg.get("symbols", []) or [])]

        ids = store.ensure_exchange_account_symbol_map(
            exchange=exchange,
            account=account,
            symbols=symbols,
        )
        ids_by_key[(exchange, account)] = ids

        logger.info(
            "Registry OK: exchange=%s account=%s symbols=%s",
            exchange,
            account,
            ",".join(symbols),
        )

    # -------------------------------------------------------------------------
    # BUILD INSTANCES
    # -------------------------------------------------------------------------
    instances: list[TradingInstance] = []

    for icfg in instances_cfg:
        exchange_name = str(icfg["exchange"])
        account = str(icfg["account"])
        role = str(icfg.get("role", "MIXED"))
        symbols = [str(s).upper() for s in (icfg.get("symbols", []) or [])]

        params = icfg.get("params") or {}
        risk_cfg = icfg.get("risk") or {}

        ex = build_exchange(exchange_name)
        ids = ids_by_key[(exchange_name, account)]

        ex.bind(
            storage=store,
            exchange_id=int(ids["_exchange_id"]),
            symbol_ids={k: int(v) for k, v in ids.items() if not str(k).startswith("_")},  # Ensure symbol ids are correct
        )

        strat = None
        if icfg.get("strategy") == "one_shot_test" and role == "BASE":
            strat = OneShotTestStrategy(
                exchange=exchange_name,
                account=account,
                qty=float(params.get("qty", 0.1)),
                hold_sec=float(params.get("hold_sec", 10)),
            )

        if not strat:
            logger.info(
                "Skip instance (no strategy): %s / %s | role=%s",
                exchange_name,
                account,
                role,
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
            balance_snapshot_sec=float(icfg.get("balance_snapshot_sec", icfg.get("funding_poll_sec", 120))),
            oms_reconcile_sec=float(icfg.get("oms_reconcile_sec", 15)),
            oms_pending_timeout_sec=float(icfg.get("oms_pending_timeout_sec", 20)),
            ids=ids,
            base_ref=icfg.get("base_ref"),
            hedge_ratio=icfg.get("hedge_ratio"),
            risk_limits=limits,
            dry_run=dry_run,
        )

        instances.append(inst)
        logger.info(
            "Instance ready: %s / %s | symbols=%s | role=%s",
            exchange_name,
            account,
            ",".join(symbols),
            role,
        )

    # -------------------------------------------------------------------------
    # RUN
    # -------------------------------------------------------------------------
    logger.info("Starting %d trading instance(s)", len(instances))
    run_instances(instances)


if __name__ == "__main__":
    main()
