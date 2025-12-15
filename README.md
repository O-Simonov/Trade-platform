# Trading Platform — v8 (Production-ish OMS + REST Reconcile)

Adds on top of v7:
- **order_fills** table (normalized fills, from WS user stream)
- **OMS reconcile loop** (REST):
  - fetches open orders
  - updates `orders.status`, `filled_qty`, `updated_at`
  - resolves "CLIENT:<clientOrderId>" placeholders to real `order_id` when found
  - times out stuck PENDING_SUBMIT placeholders (marks as EXPIRED)
- **Idempotent submit remains** (clientOrderId hash)
- **Risk Engine remains**

## Commands
```bash
pip install -r requirements.txt
python -m platform.cli.migrate
python -m platform.cli.run_real
python -m platform.cli.retention --dry-run
python -m platform.cli.retention
```

## Config
In `config/strategies.yaml`:
- `oms_reconcile_sec`: how often to REST-reconcile open orders (default 15s)
- `oms_pending_timeout_sec`: how long to wait for PENDING_SUBMIT before marking EXPIRED (default 20s)
