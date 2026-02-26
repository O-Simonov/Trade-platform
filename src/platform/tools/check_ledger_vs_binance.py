# -*- coding: utf-8 -*-
"""
Check position_ledger (qty_current, position_value_usdt) vs Binance Futures /fapi/v2/positionRisk.

- Loads .env from repo root automatically.

Checks:
- Our ledger OPEN live rows vs exchange positions (non-zero).
- Also prints [EXTRA ON EXCHANGE].

Optional auto-fix:
- AUTO_FIX_LEDGER_VALUE_USDT=1:
    when qty matches but USDT mismatch > tolerance, update ledger position_value_usdt
    to abs(notional) (or abs(qty*mark)) for the latest LIVE OPEN row.

Optional recheck:
- RECHECK_AFTER_FIX=1 (default when AUTO_FIX=1):
    do a second pass after applying fixes and print final DIFFs.
"""
from __future__ import annotations

import os
import sys
import time
import hmac
import hashlib
import urllib.parse
from decimal import Decimal, InvalidOperation
from typing import Any

import requests

_THIS_DIR = os.path.dirname(__file__)
_REPO_ROOT = os.path.abspath(os.path.join(_THIS_DIR, "..", "..", "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _load_env_from_repo_root() -> None:
    env_path = os.path.join(_REPO_ROOT, ".env")
    if not os.path.isfile(env_path):
        return
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(env_path, override=False)
        return
    except Exception:
        pass
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and (k not in os.environ):
                    os.environ[k] = v
    except Exception:
        return


_load_env_from_repo_root()

try:
    import psycopg2  # type: ignore
except Exception:
    psycopg2 = None  # type: ignore

try:
    import psycopg  # type: ignore
except Exception:
    psycopg = None  # type: ignore


def d(x: Any) -> Decimal:
    try:
        return Decimal(str(x))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def db_connect(dsn: str):
    if psycopg2 is not None:
        return psycopg2.connect(dsn)
    if psycopg is not None:
        return psycopg.connect(dsn)
    raise RuntimeError("Install psycopg2-binary or psycopg[binary]")


def _to_bool_env(name: str, default: str = "0") -> bool:
    v = (os.getenv(name) or default).strip()
    return v in ("1", "true", "True", "yes", "YES", "on", "ON")


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


LEDGER_SQL = """
WITH last_live AS (
  SELECT
    pl.exchange_id, pl.account_id, pl.strategy_id, pl.symbol_id, pl.side, pl.status, pl.source,
    pl.qty_current, pl.position_value_usdt, pl.updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY pl.exchange_id, pl.account_id, pl.strategy_id, pl.symbol_id, pl.side
      ORDER BY pl.updated_at DESC
    ) AS rn
  FROM public.position_ledger pl
  WHERE pl.source='live'
)
SELECT
  s.symbol,
  ll.exchange_id,
  ll.account_id,
  ll.strategy_id,
  ll.symbol_id,
  ll.side,
  ll.status,
  ll.source,
  ll.qty_current,
  ll.position_value_usdt,
  ll.updated_at
FROM last_live ll
JOIN public.symbols s ON s.symbol_id=ll.symbol_id AND s.exchange_id=ll.exchange_id
WHERE ll.rn=1
  AND ll.status='OPEN'
  AND ll.exchange_id=%s
  AND ll.account_id=%s
ORDER BY s.symbol, ll.strategy_id, ll.side;
"""

UPDATE_LEDGER_VALUE_SQL = """
WITH last_live AS (
  SELECT
    pl.ctid AS _ctid,
    pl.exchange_id, pl.account_id, pl.strategy_id, pl.symbol_id, pl.side, pl.status,
    ROW_NUMBER() OVER (
      PARTITION BY pl.exchange_id, pl.account_id, pl.strategy_id, pl.symbol_id, pl.side
      ORDER BY pl.updated_at DESC
    ) AS rn
  FROM public.position_ledger pl
  WHERE pl.source='live'
)
UPDATE public.position_ledger pl
SET position_value_usdt = %s,
    updated_at = now()
FROM last_live ll
WHERE pl.ctid = ll._ctid
  AND ll.rn = 1
  AND ll.status='OPEN'
  AND ll.exchange_id=%s
  AND ll.account_id=%s
  AND ll.strategy_id=%s
  AND ll.symbol_id=%s
  AND ll.side=%s;
"""


def fetch_ledger_open(conn, exchange_id: int, account_id: int) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(LEDGER_SQL, (exchange_id, account_id))
    cols = [c[0] for c in cur.description]
    out = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    return out


def fetch_exchange_positions(base_url: str, api_key: str, api_secret: str) -> dict[tuple[str, str], dict[str, Any]]:
    data = binance_get(base_url, api_key, api_secret, "/fapi/v2/positionRisk", {})
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for p in data:
        symbol = p.get("symbol")
        pside = p.get("positionSide")
        amt = d(p.get("positionAmt"))
        if not symbol or not pside or amt == 0:
            continue
        out[(symbol, pside)] = {
            "symbol": symbol,
            "positionSide": pside,
            "positionAmt": amt,
            "entryPrice": d(p.get("entryPrice")),
            "markPrice": d(p.get("markPrice")),
            "notional": d(p.get("notional")),
            "unRealizedProfit": d(p.get("unRealizedProfit")),
        }
    return out


def ledger_side_to_pside(side: str) -> str:
    s = (side or "").upper()
    return "LONG" if s == "LONG" else "SHORT"


def _compare_and_fix(
    *,
    conn,
    ledger: list[dict[str, Any]],
    exch: dict[tuple[str, str], dict[str, Any]],
    exchange_id: int,
    account_id: int,
    qty_tolerance: Decimal,
    value_tolerance_usdt: Decimal,
    auto_fix_value: bool,
    print_diffs: bool,
) -> tuple[set[tuple[str, str]], int]:
    ledger_keys: set[tuple[str, str]] = set()
    fixed_count = 0

    for r in ledger:
        symbol = r["symbol"]
        pside = ledger_side_to_pside(r["side"])
        ledger_keys.add((symbol, pside))

        e = exch.get((symbol, pside))
        l_qty = d(r.get("qty_current"))
        l_usdt = d(r.get("position_value_usdt"))

        if e is None:
            if print_diffs:
                print(f"[MISSING ON EXCHANGE] {symbol} {pside}: ledger_qty={l_qty} ledger_usdt={l_usdt}")
            continue

        e_qty = abs(d(e["positionAmt"]))
        e_usdt = abs(d(e["notional"])) if d(e["notional"]) != 0 else abs(e_qty * d(e["markPrice"]))
        e_usdt_q = e_usdt.quantize(Decimal("0.00000001"))

        dq = l_qty - e_qty
        du = (l_usdt - e_usdt_q) if l_usdt != 0 else Decimal("0")

        qty_bad = abs(dq) > qty_tolerance
        usdt_bad = (l_usdt != 0) and (abs(du) > value_tolerance_usdt)

        if auto_fix_value and (not qty_bad) and usdt_bad:
            cur = conn.cursor()
            cur.execute(
                UPDATE_LEDGER_VALUE_SQL,
                (
                    e_usdt_q,
                    exchange_id,
                    account_id,
                    r["strategy_id"],
                    r["symbol_id"],
                    r["side"],
                ),
            )
            updated = cur.rowcount
            conn.commit()
            cur.close()
            if updated == 1:
                fixed_count += 1
                if print_diffs:
                    print(f"[FIXED] {symbol} {pside}: ledger_usdt {l_usdt} -> {e_usdt_q}")
                # treat as fixed for this pass
                usdt_bad = False
                du = Decimal("0")

        if print_diffs and (qty_bad or usdt_bad):
            print(
                f"[DIFF] {symbol} {pside} | "
                f"ledger_qty={l_qty} exch_qty={e_qty} dq={dq} | "
                f"ledger_usdt={l_usdt} exch_usdt={e_usdt_q} du={du} | "
                f"entry={e['entryPrice']} mark={e['markPrice']}"
            )

    return ledger_keys, fixed_count


def main() -> None:
    pg_dsn = os.getenv("PG_DSN", "").strip()
    if not pg_dsn:
        raise SystemExit("PG_DSN is not set (can be loaded from .env)")

    exchange_id = int(os.getenv("EXCHANGE_ID", "1"))
    account_id = int(os.getenv("ACCOUNT_ID", "1"))

    base_url = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com").strip()
    api_key = (os.getenv("BINANCE_BASE_MAIN_API_KEY") or os.getenv("BINANCE_API_KEY") or "").strip()
    api_secret = (os.getenv("BINANCE_BASE_MAIN_API_SECRET") or os.getenv("BINANCE_API_SECRET") or "").strip()
    if not api_key or not api_secret:
        raise SystemExit("BINANCE API key/secret not set (can be loaded from .env)")

    qty_tolerance = d(os.getenv("RECONCILE_QTY_TOLERANCE", "1e-8"))
    value_tolerance_usdt = d(os.getenv("RECONCILE_VALUE_TOLERANCE_USDT", "0.5"))
    auto_fix_value = _to_bool_env("AUTO_FIX_LEDGER_VALUE_USDT", "0")
    recheck_after_fix = _to_bool_env("RECHECK_AFTER_FIX", "1" if auto_fix_value else "0")

    conn = db_connect(pg_dsn)

    # fetch once per run (exchange marks are live anyway)
    ledger = fetch_ledger_open(conn, exchange_id=exchange_id, account_id=account_id)
    exch = fetch_exchange_positions(base_url, api_key, api_secret)

    print(f"Ledger OPEN rows (our positions): {len(ledger)}")
    print(f"Exchange positions (non-zero): {len(exch)}")
    print(f"Tolerances: qty={qty_tolerance} value_usdt={value_tolerance_usdt}")
    print(f"Auto-fix value_usdt: {auto_fix_value}")
    print(f"Recheck after fix: {recheck_after_fix}\n")

    # pass 1: print diffs and fix (if enabled)
    ledger_keys, fixed_count = _compare_and_fix(
        conn=conn,
        ledger=ledger,
        exch=exch,
        exchange_id=exchange_id,
        account_id=account_id,
        qty_tolerance=qty_tolerance,
        value_tolerance_usdt=value_tolerance_usdt,
        auto_fix_value=auto_fix_value,
        print_diffs=True,
    )

    # print extras (based on pass 1 keys)
    extras = [(k, v) for k, v in exch.items() if k not in ledger_keys]
    if extras:
        print("\nExchange positions NOT present in ledger OPEN:")
        for (symbol, pside), e in sorted(extras, key=lambda x: (x[0][0], x[0][1])):
            e_qty = abs(d(e["positionAmt"]))
            e_usdt = abs(d(e["notional"])) if d(e["notional"]) != 0 else abs(e_qty * d(e["markPrice"]))
            e_usdt_q = e_usdt.quantize(Decimal("0.00000001"))
            print(
                f"[EXTRA ON EXCHANGE] {symbol} {pside} | "
                f"exch_qty={e_qty} exch_usdt={e_usdt_q} | "
                f"entry={e['entryPrice']} mark={e['markPrice']} upnl={e['unRealizedProfit']}"
            )

    # pass 2: re-read ledger and recheck after fixes (optional)
    if auto_fix_value and recheck_after_fix and fixed_count > 0:
        print("\n--- RECHECK AFTER FIX ---\n")
        ledger2 = fetch_ledger_open(conn, exchange_id=exchange_id, account_id=account_id)
        # use fresh exchange snapshot too, to match current mark
        exch2 = fetch_exchange_positions(base_url, api_key, api_secret)
        _compare_and_fix(
            conn=conn,
            ledger=ledger2,
            exch=exch2,
            exchange_id=exchange_id,
            account_id=account_id,
            qty_tolerance=qty_tolerance,
            value_tolerance_usdt=value_tolerance_usdt,
            auto_fix_value=False,   # do not loop-fix endlessly
            print_diffs=True,
        )

    conn.close()


if __name__ == "__main__":
    main()
