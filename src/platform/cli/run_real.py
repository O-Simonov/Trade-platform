from __future__ import annotations
import os
from pathlib import Path
import yaml
from dotenv import load_dotenv

from platform.exchanges.registry import build_exchange
from platform.core.strategy.example_nohedge import ExampleNoHedge
from platform.core.engine.instance import TradingInstance
from platform.core.engine.runner import run_instances
from platform.data.storage.postgres.pool import create_pool
from platform.data.storage.postgres.storage import PostgreSQLStorage
from platform.core.risk.risk_engine import RiskLimits

def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))

def main():
    load_dotenv()
    dsn = os.environ.get("PG_DSN")
    if not dsn:
        raise SystemExit("PG_DSN env var is required")

    root = Path(__file__).resolve().parents[3]
    cfg_root = _load_yaml(root / "config" / "strategies.yaml")
    cfg = cfg_root["instances"]

    pool = create_pool(dsn)
    store = PostgreSQLStorage(pool)

    ids_by_key = {}
    for icfg in cfg:
        exchange_name = icfg["exchange"]
        account = icfg["account"]
        symbols = [s.upper() for s in icfg["symbols"]]
        ids_by_key[(exchange_name, account)] = store.ensure_exchange_account_symbol(exchange_name, account, symbols)

    instances = []
    for icfg in cfg:
        exchange_name = icfg["exchange"]
        account = icfg["account"]
        role = icfg.get("role","MIXED")
        symbols = [s.upper() for s in icfg["symbols"]]
        candle_intervals = icfg.get("candle_intervals", []) or []
        funding_poll_sec = float(icfg.get("funding_poll_sec", 120) or 120)
        oms_reconcile_sec = float(icfg.get("oms_reconcile_sec", 15) or 15)
        oms_pending_timeout_sec = float(icfg.get("oms_pending_timeout_sec", 20) or 20)
        base_ref = icfg.get("base_ref")
        hedge_ratio = icfg.get("hedge_ratio")
        risk_cfg = icfg.get("risk", {}) or {}

        params = icfg.get("params", {}) or {}

        ex = build_exchange(exchange_name)
        ids = ids_by_key[(exchange_name, account)]

        if base_ref:
            bex = base_ref.get("exchange")
            bacc = base_ref.get("account")
            if (bex, bacc) in ids_by_key:
                base_ref = dict(base_ref)
                base_ref["account_id"] = ids_by_key[(bex, bacc)]["_account_id"]

        if icfg["strategy"] == "example_nohedge":
            strat = ExampleNoHedge(exchange=exchange_name, account=account,
                                  qty=float(params.get("qty", 0.001)),
                                  tp_usdt=float(params.get("tp_usdt", 5.0)))
        else:
            raise SystemExit(f"Unknown strategy: {icfg['strategy']}")

        limits = RiskLimits(
            max_open_positions=int(risk_cfg.get("max_open_positions", 10)),
            max_notional_usdt=float(risk_cfg.get("max_notional_usdt", 1e9)),
            max_order_qty=float(risk_cfg.get("max_order_qty", 1e9)),
            max_daily_loss_usdt=float(risk_cfg.get("max_daily_loss_usdt", 1e9)),
            max_drawdown_pct=float(risk_cfg.get("max_drawdown_pct", 1e9)),
            min_available_margin_pct=float(risk_cfg.get("min_available_margin_pct", 0.0)),
            max_margin_used_pct=float(risk_cfg.get("max_margin_used_pct", 1e9)),
        )

        instances.append(TradingInstance(
            exchange=ex, storage=store, strategy=strat,
            account=account, role=role,
            symbols=symbols, candle_intervals=candle_intervals,
            funding_poll_sec=funding_poll_sec,
            oms_reconcile_sec=oms_reconcile_sec,
            oms_pending_timeout_sec=oms_pending_timeout_sec,
            ids=ids, base_ref=base_ref, hedge_ratio=hedge_ratio,
            risk_limits=limits,
        ))

    run_instances(instances)

if __name__ == "__main__":
    main()
