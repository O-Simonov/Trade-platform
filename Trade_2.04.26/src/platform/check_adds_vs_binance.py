# src/platform/check_adds_vs_binance.py
from __future__ import annotations

import os
import time
import hmac
import hashlib
import urllib.parse
import re
import json
from pathlib import Path
from typing import Any

import psycopg
import requests

# =============================================================================
# .env auto-load
# =============================================================================

def _load_env_file(env_path: Path) -> None:
    """Load KEY=VALUE lines into os.environ if not already set."""
    if not env_path.exists():
        return

    text = env_path.read_text(encoding="utf-8", errors="ignore")
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue

        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip()

        # strip quotes
        if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
            v = v[1:-1]

        os.environ.setdefault(k, v)


def load_env() -> None:
    """
    Try python-dotenv if installed; fallback to manual parser.
    Loads .env from project root (two levels up from this file).
    """
    project_root = Path(__file__).resolve().parents[2]
    env_path = project_root / ".env"

    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(env_path, override=False)
    except Exception:
        _load_env_file(env_path)


def env_pick(*names: str, default: str | None = None) -> str | None:
    for n in names:
        val = os.environ.get(n)
        if val is not None and str(val).strip() != "":
            return val
    return default


load_env()

# =============================================================================
# Config / creds
# =============================================================================

PG_DSN = env_pick("PG_DSN", "POSTGRES_DSN", "DATABASE_URL")
if not PG_DSN:
    raise RuntimeError(
        "PG_DSN is not set (env or .env). Example:\n"
        "PG_DSN=host=localhost port=5432 dbname=trade_platform user=postgres sslmode=prefer connect_timeout=10"
    )

BINANCE_KEY = env_pick(
    "BINANCE_BASE_MAIN_API_KEY",
    "BINANCE_API_KEY",
    "BINANCE_KEY",
    "BINANCE_FAPI_KEY",
    "API_KEY",
)
BINANCE_SECRET = env_pick(
    "BINANCE_BASE_MAIN_API_SECRET",
    "BINANCE_API_SECRET",
    "BINANCE_SECRET",
    "BINANCE_FAPI_SECRET",
    "API_SECRET",
)

if not BINANCE_KEY or not BINANCE_SECRET:
    raise RuntimeError(
        "Binance API keys not found. Add to .env:\n"
        "  BINANCE_BASE_MAIN_API_KEY=...\n"
        "  BINANCE_BASE_MAIN_API_SECRET=...\n"
    )

BINANCE_SECRET_BYTES = BINANCE_SECRET.encode("utf-8")

BASE_URL = env_pick("BINANCE_FAPI_URL", default="https://fapi.binance.com")
REQUEST_TIMEOUT_SEC = float(env_pick("BINANCE_HTTP_TIMEOUT", default="30"))

ADD_RE = re.compile(r"_ADD(\d+)$")


# =============================================================================
# Binance signed REST
# =============================================================================

def signed_request(method: str, path: str, params: dict[str, Any]) -> Any:
    params = dict(params)
    params["timestamp"] = int(time.time() * 1000)
    params.setdefault("recvWindow", 5000)

    qs = urllib.parse.urlencode(params, doseq=True)
    sig = hmac.new(BINANCE_SECRET_BYTES, qs.encode("utf-8"), hashlib.sha256).hexdigest()

    url = f"{BASE_URL}{path}?{qs}&signature={sig}"
    headers = {"X-MBX-APIKEY": BINANCE_KEY}

    r = requests.request(method, url, headers=headers, timeout=REQUEST_TIMEOUT_SEC)
    if r.status_code >= 400:
        raise RuntimeError(f"binance http {r.status_code} {method} {path}: {r.text}")
    return r.json()


# =============================================================================
# DB
# =============================================================================

def fetch_open_positions() -> list[tuple[int, int, str, str, int]]:
    """
    Returns list of:
    (exchange_id, account_id, pos_uid, symbol, ledger_adds)
    """
    with psycopg.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    exchange_id,
                    account_id,
                    pos_uid,
                    symbol,
                    COALESCE(scale_in_count, 0) AS ledger_adds
                FROM position_ledger
                WHERE status='OPEN'
                ORDER BY symbol
                """
            )
            rows = cur.fetchall()
    # typing: psycopg returns tuples
    return [(int(a), int(b), str(c), str(d), int(e)) for (a, b, c, d, e) in rows]


# =============================================================================
# Compare: Binance orders vs ledger
# =============================================================================

def max_add_from_binance_orders(symbol: str) -> int:
    """
    Uses /fapi/v1/allOrders and detects executed ADDn by clientOrderId suffix _ADD<n>.
    Counts only orders that have executedQty > 0 (or FILLED/PARTIALLY_FILLED).
    """
    orders = signed_request("GET", "/fapi/v1/allOrders", {"symbol": symbol, "limit": 1000})

    max_add = 0
    for o in orders:
        cid = (o.get("clientOrderId") or "").strip()
        if not cid:
            continue

        m = ADD_RE.search(cid)
        if not m:
            continue

        status = (o.get("status") or "").upper()
        try:
            exec_qty = float(o.get("executedQty") or 0.0)
        except Exception:
            exec_qty = 0.0

        if status in ("FILLED", "PARTIALLY_FILLED") or exec_qty > 0:
            n = int(m.group(1))
            if n > max_add:
                max_add = n

    return max_add


def main() -> None:
    rows = fetch_open_positions()
    diffs: list[tuple[str, str, int, int]] = []

    print(f"Open positions: {len(rows)}")
    print(f"Binance base URL: {BASE_URL}")
    print("Checking adds (scale-ins) ...\n")

    for exchange_id, account_id, pos_uid, symbol, ledger_adds in rows:
        try:
            ex_adds = max_add_from_binance_orders(symbol)
        except Exception as e:
            print(f"[ERROR] {symbol} pos_uid={pos_uid}: {e}")
            continue

        if int(ledger_adds) != int(ex_adds):
            diffs.append((symbol, pos_uid, int(ledger_adds), int(ex_adds)))
            print(f"[DIFF] {symbol} pos_uid={pos_uid} ledger={ledger_adds} binance={ex_adds}")
        else:
            print(f"[OK]   {symbol} pos_uid={pos_uid} adds={ledger_adds}")

    print("\n=== SUMMARY ===")
    if not diffs:
        print("No differences. DB matches Binance for adds.")
    else:
        print("Differences found:")
        print(json.dumps(diffs, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()