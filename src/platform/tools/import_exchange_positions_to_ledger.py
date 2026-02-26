# -*- coding: utf-8 -*-
"""
Import Binance Futures hedge positions into public.position_ledger
so TradeLiquidation bot (strategy_id='trade_liquidation') will manage them дальше.

- Loads .env from repo root automatically (no need PowerShell $env:...)
- Fetches /fapi/v2/positionRisk
- Inserts missing OPEN ledger rows for (symbol, positionSide) present on exchange
  but absent in ledger OPEN for the same strategy_id.

Run:
  python src/platform/tools/import_exchange_positions_to_ledger.py
"""

from __future__ import annotations

import os
import sys
import time
import uuid
import hmac
import hashlib
import urllib.parse
from decimal import Decimal, InvalidOperation
from typing import Any

import requests

# --- Make "src" importable even when running by file path ---
_THIS_DIR = os.path.dirname(__file__)
_REPO_ROOT = os.path.abspath(os.path.join(_THIS_DIR, "..", "..", "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _load_env_from_repo_root() -> None:
    """Load .env from repo root, without overriding existing env vars."""
    env_path = os.path.join(_REPO_ROOT, ".env")
    if not os.path.isfile(env_path):
        return

    try:
        from dotenv import load_dotenv  # type: ignore

        load_dotenv(env_path, override=False)
        return
    except Exception:
        pass

    # fallback minimal parser
    with open(env_path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


_load_env_from_repo_root()

# --- DB drivers ---
try:
    import psycopg2  # type: ignore
except Exception:
    psycopg2 = None  # type: ignore

try:
    import psycopg  # type: ignore
except Exception:
    psycopg = None  # type: ignore


def db_connect(dsn: str):
    if psycopg2 is not None:
        return psycopg2.connect(dsn)
    if psycopg is not None:
        return psycopg.connect(dsn)
    raise RuntimeError("Install psycopg2-binary or psycopg[binary]")


def d(x: Any) -> Decimal:
    try:
        return Decimal(str(x))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def sign_query(params: dict[str, Any], secret: str) -> str:
    qs = urllib.parse.urlencode(params, doseq=True)
    sig = hmac.new(secret.encode("utf-8"), qs.encode("utf-8"), hashlib.sha256).hexdigest()
    return qs + "&signature=" + sig


def binance_get(base_url: str, api_key: str, api_secret: str, path: str, params: dict[str, Any]) -> Any:
    p = dict(params)
    p["timestamp"] = int(time.time() * 1000)
    p.setdefault("recvWindow", 5000)
    url = f"{base_url}{path}?{sign_query(p, api_secret)}"
    r = requests.get(url, headers={"X-MBX-APIKEY": api_key}, timeout=15)
    if r.status_code != 200:
        raise RuntimeError(f"Binance GET {path} HTTP {r.status_code}: {r.text}")
    return r.json()


def fetch_exchange_positions(base_url: str, api_key: str, api_secret: str) -> dict[tuple[str, str], dict[str, Any]]:
    data = binance_get(base_url, api_key, api_secret, "/fapi/v2/positionRisk", {})
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for p in data:
        symbol = (p.get("symbol") or "").strip().upper()
        pside = (p.get("positionSide") or "").strip().upper()  # LONG/SHORT/BOTH
        amt = d(p.get("positionAmt"))
        if not symbol or pside not in ("LONG", "SHORT"):
            continue
        if amt == 0:
            continue

        out[(symbol, pside)] = {
            "symbol": symbol,
            "positionSide": pside,
            "positionAmt": abs(amt),
            "entryPrice": d(p.get("entryPrice")),
            "markPrice": d(p.get("markPrice")),
            "notional": abs(d(p.get("notional"))),
            "unRealizedProfit": d(p.get("unRealizedProfit")),
        }
    return out


LEDGER_OPEN_KEYS_SQL = """
WITH last_live AS (
  SELECT
    pl.exchange_id, pl.account_id, pl.strategy_id, pl.symbol_id, pl.side, pl.status, pl.source,
    ROW_NUMBER() OVER (
      PARTITION BY pl.exchange_id, pl.account_id, pl.strategy_id, pl.symbol_id, pl.side
      ORDER BY pl.updated_at DESC
    ) AS rn
  FROM public.position_ledger pl
)
SELECT s.symbol, ll.side
FROM last_live ll
JOIN public.symbols s ON s.symbol_id=ll.symbol_id AND s.exchange_id=ll.exchange_id
WHERE ll.rn=1
  AND ll.exchange_id=%s
  AND ll.account_id=%s
  AND ll.strategy_id=%s
  AND ll.status='OPEN'
  AND ll.source='live';
"""


SYMBOL_ID_SQL = """
SELECT symbol_id
FROM public.symbols
WHERE exchange_id=%s AND symbol=%s
LIMIT 1;
"""


INSERT_LEDGER_SQL = """
INSERT INTO public.position_ledger (
  exchange_id,
  account_id,
  pos_uid,
  symbol_id,
  strategy_id,
  strategy_name,
  side,
  status,
  opened_at,
  closed_at,
  entry_price,
  avg_price,
  exit_price,
  qty_opened,
  qty_current,
  qty_closed,
  position_value_usdt,
  scale_in_count,
  realized_pnl,
  fees,
  updated_at,
  source
) VALUES (
  %(exchange_id)s,
  %(account_id)s,
  %(pos_uid)s,
  %(symbol_id)s,
  %(strategy_id)s,
  %(strategy_name)s,
  %(side)s,
  'OPEN',
  now(),
  NULL,
  %(entry_price)s,
  %(avg_price)s,
  NULL,
  %(qty_opened)s,
  %(qty_current)s,
  0,
  %(position_value_usdt)s,
  0,
  0,
  0,
  now(),
  'live'
);
"""


def main() -> None:
    pg_dsn = (os.getenv("PG_DSN") or "").strip()
    if not pg_dsn:
        raise SystemExit("PG_DSN is not set (can be loaded from .env)")

    exchange_id = int(os.getenv("EXCHANGE_ID", "1"))
    account_id = int(os.getenv("ACCOUNT_ID", "1"))

    # IMPORTANT: to be picked up by TradeLiquidation
    strategy_id = (os.getenv("IMPORT_STRATEGY_ID") or "trade_liquidation").strip()
    strategy_name = (os.getenv("IMPORT_STRATEGY_NAME") or "trade_liquidation").strip()

    base_url = (os.getenv("BINANCE_BASE_URL") or "https://fapi.binance.com").strip()
    api_key = (os.getenv("BINANCE_BASE_MAIN_API_KEY") or os.getenv("BINANCE_API_KEY") or "").strip()
    api_secret = (os.getenv("BINANCE_BASE_MAIN_API_SECRET") or os.getenv("BINANCE_API_SECRET") or "").strip()
    if not api_key or not api_secret:
        raise SystemExit("BINANCE_* API key/secret not set (can be loaded from .env)")

    dry_run = (os.getenv("IMPORT_DRY_RUN") or "1").strip() not in ("0", "false", "False")

    conn = db_connect(pg_dsn)
    cur = conn.cursor()

    # what exchange has
    exch = fetch_exchange_positions(base_url, api_key, api_secret)

    # what ledger already has OPEN for this strategy
    cur.execute(LEDGER_OPEN_KEYS_SQL, (exchange_id, account_id, strategy_id))
    existing = {(str(r[0]).upper(), str(r[1]).upper()) for r in cur.fetchall()}

    missing = [(k, v) for k, v in exch.items() if k not in existing]

    print(f"Strategy target: {strategy_id}")
    print(f"Exchange non-zero (LONG/SHORT): {len(exch)}")
    print(f"Ledger OPEN live (this strategy): {len(existing)}")
    print(f"To import (missing on ledger): {len(missing)}")
    print(f"Dry-run: {dry_run}\n")

    if not missing:
        print("Nothing to import.")
        cur.close()
        conn.close()
        return

    # resolve symbol_ids + insert
    imported = 0
    skipped = 0

    for (symbol, side), e in sorted(missing, key=lambda x: (x[0][0], x[0][1])):
        cur.execute(SYMBOL_ID_SQL, (exchange_id, symbol))
        row = cur.fetchone()
        if not row:
            print(f"[SKIP] {symbol} not found in public.symbols for exchange_id={exchange_id}")
            skipped += 1
            continue

        symbol_id = int(row[0])
        qty = d(e["positionAmt"])
        entry = d(e["entryPrice"])
        notional = d(e["notional"])
        mark = d(e["markPrice"])
        value_usdt = (notional if notional != 0 else abs(qty * mark)).quantize(Decimal("0.00000001"))

        payload = {
            "exchange_id": exchange_id,
            "account_id": account_id,
            "pos_uid": str(uuid.uuid4()),
            "symbol_id": symbol_id,
            "strategy_id": strategy_id,
            "strategy_name": strategy_name,
            "side": side,
            "entry_price": entry.quantize(Decimal("0.00000001")) if entry != 0 else None,
            "avg_price": entry.quantize(Decimal("0.00000001")) if entry != 0 else None,
            "qty_opened": qty.quantize(Decimal("0.00000001")),
            "qty_current": qty.quantize(Decimal("0.00000001")),
            "position_value_usdt": value_usdt,
        }

        print(
            f"[IMPORT] {symbol} {side} | qty={payload['qty_current']} "
            f"entry={payload['entry_price']} mark={mark} value_usdt={payload['position_value_usdt']}"
        )

        if not dry_run:
            cur.execute(INSERT_LEDGER_SQL, payload)
            imported += 1

    if not dry_run:
        conn.commit()
        print(f"\nCommitted. Imported rows: {imported}, skipped: {skipped}")
    else:
        print(f"\nDry-run only. Would import rows: {len(missing) - skipped}, skipped: {skipped}")
        print("Set IMPORT_DRY_RUN=0 to actually write into DB.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()