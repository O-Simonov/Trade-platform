from __future__ import annotations
import os
from pathlib import Path
import yaml
from dotenv import load_dotenv
from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage

# from platform.data.storage.postgres.pool import create_pool
# from platform.data.storage.postgres.storage import PostgreSQLStorage

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    load_dotenv()
    dsn = os.environ.get("PG_DSN")
    if not dsn:
        raise SystemExit("PG_DSN env var is required")

    root = Path(__file__).resolve().parents[3]
    cfg = yaml.safe_load((root / "config" / "strategies.yaml").read_text(encoding="utf-8"))
    r = cfg.get("retention", {}) or {}

    pool = create_pool(dsn)
    store = PostgreSQLStorage(pool)

    # v9.1: downsample balance snapshots into 5m/1h tables (safe idempotent upserts)
    for inst in (cfg.get("instances") or []):
        ex = str(inst.get("exchange","")).lower()
        acc = str(inst.get("account",""))
        syms = inst.get("symbols") or []
        ids = store.ensure_exchange_account_symbol(ex, acc, list(syms)[:1] or ["BTCUSDT"])
        try:
            ds = store.downsample_balance(exchange_id=ids["_exchange_id"], account_id=ids["_account_id"])
            print(f"[Downsample] {ex}:{acc} -> {ds}")
        except Exception as e:
            print(f"[Downsample] {ex}:{acc} failed: {e}")

    res = store.retention_cleanup(
        candles_days=int(r.get("candles_days", 180)),
        snapshots_days=int(r.get("snapshots_days", 14)),
        funding_days=int(r.get("funding_days", 365)),
        orders_days=int(r.get("orders_days", 365)),
        trades_days=int(r.get("trades_days", 365)),
        fills_days=int(r.get("fills_days", 365)),
        balance_days=int(r.get("balance_days", 30)),
        balance_5m_days=int(r.get("balance_5m_days", 180)),
        balance_1h_days=int(r.get("balance_1h_days", 1825)),
        oi_5m_days=int(r.get("oi_5m_days", 90)),
        oi_15m_days=int(r.get("oi_15m_days", 180)),
        oi_1h_days=int(r.get("oi_1h_days", 365)),
        dry_run=bool(args.dry_run),
    )
    mode = "DRY-RUN (would delete)" if args.dry_run else "DELETED"
    print(f"[{mode}] {res}")

if __name__ == "__main__":
    main()
