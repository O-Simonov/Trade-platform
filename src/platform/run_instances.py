# src/platform/run_instances.py
from __future__ import annotations

import time
import os
import logging
import time
from pathlib import Path
import yaml
from dotenv import load_dotenv

from src.platform.exchanges.registry import build_exchange
from src.platform.core.strategy.example_nohedge import ExampleNoHedge
from src.platform.core.engine.instance import TradingInstance
from src.platform.core.engine.runner import run_instances
from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage
from src.platform.core.risk.risk_engine import RiskLimits


# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

DRY_RUN: bool = os.getenv("DRY_RUN", "1") == "1"


# ---------------------------------------------------------------------------
# UTILS
# ---------------------------------------------------------------------------

def _load_yaml(path: Path) -> dict:
    if not path.exists():
        raise SystemExit(f"Config file not found: {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    # --- logging ---
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("platform.run_instances")

    logger.info("=== RUN INSTANCES START ===")
    logger.warning("DRY_RUN=%s (NO REAL ORDERS WILL BE SENT)", DRY_RUN)

    # --- env ---
    load_dotenv()
    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise SystemExit("PG_DSN env var is required")

    # --- config ---
    root = Path(__file__).resolve().parents[2]  # -> Trade-platform
    cfg_path = root / "config" / "strategies.yaml"
    cfg_root = _load_yaml(cfg_path)
    instances_cfg = cfg_root.get("instances") or []

    if not instances_cfg:
        raise SystemExit("No instances defined in strategies.yaml")

    # --- storage ---
    pool = create_pool(dsn)
    store = PostgreSQLStorage(pool)
    logger.info("PostgreSQL storage initialized")

    # -----------------------------------------------------------------------
    # REGISTRY (TEMP B2)
    # -----------------------------------------------------------------------

    ids_by_key: dict[tuple[str, str], dict] = {}

    for icfg in instances_cfg:
        exchange_name = icfg["exchange"]
        account = icfg["account"]
        symbols = [s.upper() for s in icfg.get("symbols", [])]

        ids = store.ensure_exchange_account_symbol(
            exchange=exchange_name,
            account=account,
            symbols=symbols,
        )

        ids_by_key[(exchange_name, account)] = ids

        logger.info(
            "Registry OK: exchange=%s account=%s symbols=%s",
            exchange_name, account, ",".join(symbols)
        )

    # -----------------------------------------------------------------------
    # BUILD INSTANCES
    # -----------------------------------------------------------------------

    instances: list[TradingInstance] = []

    for icfg in instances_cfg:
        exchange_name = icfg["exchange"]
        account = icfg["account"]
        role = icfg.get("role", "MIXED")
        symbols = [s.upper() for s in icfg.get("symbols", [])]

        candle_intervals = icfg.get("candle_intervals") or []
        funding_poll_sec = float(icfg.get("funding_poll_sec", 120))
        oms_reconcile_sec = float(icfg.get("oms_reconcile_sec", 15))
        oms_pending_timeout_sec = float(icfg.get("oms_pending_timeout_sec", 20))

        base_ref = icfg.get("base_ref")
        hedge_ratio = icfg.get("hedge_ratio")

        params = icfg.get("params") or {}
        risk_cfg = icfg.get("risk") or {}

        # --- exchange adapter ---
        ex = build_exchange(exchange_name)

        # --- strategy ---
        if icfg["strategy"] == "example_nohedge":
            strat = ExampleNoHedge(
                exchange=exchange_name,
                account=account,
                qty=float(params.get("qty", 0.001)),
                tp_usdt=float(params.get("tp_usdt", 5.0)),
            )
        else:
            raise SystemExit(f"Unknown strategy: {icfg['strategy']}")

        # --- risk ---
        limits = RiskLimits(
            max_open_positions=int(risk_cfg.get("max_open_positions", 10)),
            max_notional_usdt=float(risk_cfg.get("max_notional_usdt", 1e9)),
            max_order_qty=float(risk_cfg.get("max_order_qty", 1e9)),
            max_daily_loss_usdt=float(risk_cfg.get("max_daily_loss_usdt", 1e9)),
            max_drawdown_pct=float(risk_cfg.get("max_drawdown_pct", 1e9)),
            min_available_margin_pct=float(risk_cfg.get("min_available_margin_pct", 0.0)),
            max_margin_used_pct=float(risk_cfg.get("max_margin_used_pct", 1e9)),
        )

        ids = ids_by_key[(exchange_name, account)]

        if base_ref:
            bex = base_ref.get("exchange")
            bacc = base_ref.get("account")
            if (bex, bacc) in ids_by_key:
                base_ref = dict(base_ref)
                base_ref["account_id"] = ids_by_key[(bex, bacc)]["_account_id"]

        inst = TradingInstance(
            exchange=ex,
            storage=store,
            strategy=strat,
            account=account,
            role=role,
            symbols=symbols,
            candle_intervals=candle_intervals,
            funding_poll_sec=funding_poll_sec,
            oms_reconcile_sec=oms_reconcile_sec,
            oms_pending_timeout_sec=oms_pending_timeout_sec,
            ids=ids,
            base_ref=base_ref,
            hedge_ratio=hedge_ratio,
            risk_limits=limits,
            dry_run=DRY_RUN,
        )

        instances.append(inst)

        logger.info(
            "Instance ready: %s / %s | symbols=%s | role=%s",
            exchange_name, account, ",".join(symbols), role
        )

    # -----------------------------------------------------------------------
    # RUN
    # -----------------------------------------------------------------------

    logger.info("Starting %d trading instance(s)", len(instances))
    run_instances(instances)


if __name__ == "__main__":
    main()
