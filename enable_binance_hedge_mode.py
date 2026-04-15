#!/usr/bin/env python3
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, List


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if not value:
        raise RuntimeError(f"Environment variable is required: {name}")
    return value


def _sign(secret: str, query: str) -> str:
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def _request(
    method: str,
    base_url: str,
    path: str,
    api_key: str,
    api_secret: str,
    params: Dict[str, Any] | None = None,
) -> Any:
    params = dict(params or {})
    params["timestamp"] = int(time.time() * 1000)
    params.setdefault("recvWindow", 5000)

    query = urllib.parse.urlencode(params, doseq=True)
    signature = _sign(api_secret, query)
    url = f"{base_url}{path}?{query}&signature={signature}"

    req = urllib.request.Request(url=url, method=method.upper())
    req.add_header("X-MBX-APIKEY", api_key)

    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code}: {body}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Network error: {e}") from e


def get_position_mode(base_url: str, api_key: str, api_secret: str) -> bool:
    data = _request(
        "GET",
        base_url,
        "/fapi/v1/positionSide/dual",
        api_key,
        api_secret,
    )
    dual = data.get("dualSidePosition")
    if isinstance(dual, bool):
        return dual
    if isinstance(dual, str):
        return dual.lower() == "true"
    raise RuntimeError(f"Unexpected response for position mode: {data}")


def set_position_mode(base_url: str, api_key: str, api_secret: str, enable_hedge: bool) -> Any:
    return _request(
        "POST",
        base_url,
        "/fapi/v1/positionSide/dual",
        api_key,
        api_secret,
        params={"dualSidePosition": "true" if enable_hedge else "false"},
    )


def get_open_orders(base_url: str, api_key: str, api_secret: str) -> List[Dict[str, Any]]:
    data = _request(
        "GET",
        base_url,
        "/fapi/v1/openOrders",
        api_key,
        api_secret,
    )
    return data if isinstance(data, list) else []


def get_position_risk(base_url: str, api_key: str, api_secret: str) -> List[Dict[str, Any]]:
    data = _request(
        "GET",
        base_url,
        "/fapi/v3/positionRisk",
        api_key,
        api_secret,
    )
    return data if isinstance(data, list) else []


def summarize_positions(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        amt = str(row.get("positionAmt", "0"))
        try:
            if float(amt) == 0.0:
                continue
        except Exception:
            pass
        out.append(
            {
                "symbol": row.get("symbol"),
                "positionSide": row.get("positionSide"),
                "positionAmt": row.get("positionAmt"),
                "entryPrice": row.get("entryPrice"),
                "unRealizedProfit": row.get("unRealizedProfit"),
            }
        )
    return out


def main() -> int:
    base_url = os.getenv("BINANCE_FAPI_BASE_URL", "https://fapi.binance.com").rstrip("/")
    api_key = _env("BINANCE_API_KEY")
    api_secret = _env("BINANCE_API_SECRET")

    print("[BINANCE] Checking USD-M Futures position mode...")
    current = get_position_mode(base_url, api_key, api_secret)
    print(f"[BINANCE] Current mode: {'HEDGE' if current else 'ONE_WAY'}")

    if current:
        print("[BINANCE] Hedge mode is already enabled and confirmed.")
        return 0

    print("[BINANCE] Hedge mode is disabled. Attempting to enable dual-side mode...")
    try:
        resp = set_position_mode(base_url, api_key, api_secret, True)
        print(f"[BINANCE] Exchange response: {resp}")
    except Exception as e:
        print(f"[BINANCE] Failed to switch mode: {e}", file=sys.stderr)
        try:
            orders = get_open_orders(base_url, api_key, api_secret)
            positions = summarize_positions(get_position_risk(base_url, api_key, api_secret))
            print(f"[BINANCE] Open orders count: {len(orders)}", file=sys.stderr)
            if orders:
                print("[BINANCE] Sample open orders:", file=sys.stderr)
                for row in orders[:10]:
                    print(
                        {
                            "symbol": row.get("symbol"),
                            "side": row.get("side"),
                            "positionSide": row.get("positionSide"),
                            "type": row.get("type"),
                            "origQty": row.get("origQty"),
                        },
                        file=sys.stderr,
                    )
            print(f"[BINANCE] Non-zero positions count: {len(positions)}", file=sys.stderr)
            if positions:
                print("[BINANCE] Non-zero positions:", file=sys.stderr)
                for row in positions[:10]:
                    print(row, file=sys.stderr)
        except Exception as diag_err:
            print(f"[BINANCE] Diagnostic fetch failed: {diag_err}", file=sys.stderr)
        return 2

    confirmed = get_position_mode(base_url, api_key, api_secret)
    print(f"[BINANCE] Confirmed mode after switch: {'HEDGE' if confirmed else 'ONE_WAY'}")
    if not confirmed:
        print("[BINANCE] Switch request completed but hedge mode is still NOT enabled.", file=sys.stderr)
        return 3

    print("[BINANCE] Hedge mode enabled and confirmed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())