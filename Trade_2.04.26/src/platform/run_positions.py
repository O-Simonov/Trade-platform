# src/platform/run_positions.py
import os
import time
import hmac
import hashlib
import requests
from urllib.parse import urlencode
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("BINANCE_BASE_MAIN_API_KEY")
API_SECRET_RAW = os.getenv("BINANCE_BASE_MAIN_API_SECRET")
if not API_KEY or not API_SECRET_RAW:
    raise RuntimeError("Не найдены BINANCE_BASE_MAIN_API_KEY / BINANCE_BASE_MAIN_API_SECRET")

API_SECRET = API_SECRET_RAW.encode()
BASE = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com")


def signed_get(path: str, params: dict | None = None):
    params = params or {}
    params["timestamp"] = int(time.time() * 1000)
    query = urlencode(params, doseq=True)
    sig = hmac.new(API_SECRET, query.encode(), hashlib.sha256).hexdigest()
    headers = {"X-MBX-APIKEY": API_KEY}
    url = f"{BASE}{path}?{query}&signature={sig}"
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    return r.json()


def to_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


print("\n=== POSITION RISK (/fapi/v2/positionRisk) ===")
all_positions = signed_get("/fapi/v2/positionRisk")

open_positions = [
    p for p in all_positions
    if p.get("positionSide") in ("LONG", "SHORT")
    and abs(to_float(p.get("positionAmt"))) > 0
]

open_positions = sorted(
    open_positions,
    key=lambda p: (p.get("symbol", ""), p.get("positionSide", ""))
)

symbols = sorted({p.get("symbol") for p in open_positions if p.get("symbol")})

print(f"OPEN POSITIONS COUNT: {len(open_positions)}")
print(f"OPEN SYMBOLS COUNT: {len(symbols)}")
print(f"SYMBOLS: {symbols}")

for p in open_positions:
    print({
        "symbol": p.get("symbol"),
        "positionSide": p.get("positionSide"),
        "positionAmt": p.get("positionAmt"),
        "entryPrice": p.get("entryPrice"),
        "breakEvenPrice": p.get("breakEvenPrice"),
        "markPrice": p.get("markPrice"),
        "unRealizedProfit": p.get("unRealizedProfit"),
        "liquidationPrice": p.get("liquidationPrice"),
        "notional": p.get("notional"),
        "isolatedWallet": p.get("isolatedWallet"),
        "leverage": p.get("leverage"),
        "marginType": p.get("marginType"),
    })

print("\n=== OPEN NORMAL ORDERS (/fapi/v1/openOrders) ===")
if not symbols:
    print("Нет открытых позиций на бирже.")
else:
    total_normal_orders = 0
    for sym in symbols:
        data = signed_get("/fapi/v1/openOrders", {"symbol": sym})
        total_normal_orders += len(data)

        print(f"\n--- {sym} ({len(data)}) ---")
        if not data:
            print("[]")
            continue

        for o in data:
            print({
                "symbol": o.get("symbol"),
                "orderId": o.get("orderId"),
                "clientOrderId": o.get("clientOrderId"),
                "side": o.get("side"),
                "positionSide": o.get("positionSide"),
                "type": o.get("type"),
                "origType": o.get("origType"),
                "status": o.get("status"),
                "origQty": o.get("origQty"),
                "executedQty": o.get("executedQty"),
                "price": o.get("price"),
                "stopPrice": o.get("stopPrice"),
                "activatePrice": o.get("activatePrice"),
                "priceRate": o.get("priceRate"),
                "reduceOnly": o.get("reduceOnly"),
                "closePosition": o.get("closePosition"),
                "workingType": o.get("workingType"),
                "timeInForce": o.get("timeInForce"),
            })

    print(f"\nTOTAL NORMAL OPEN ORDERS: {total_normal_orders}")

print("\n=== OPEN ALGO ORDERS (/fapi/v1/openAlgoOrders) ===")
if not symbols:
    print("Нет открытых позиций на бирже.")
else:
    total_algo_orders = 0
    for sym in symbols:
        try:
            data = signed_get("/fapi/v1/openAlgoOrders", {"symbol": sym})
        except requests.HTTPError as e:
            print(f"\n--- {sym} (ERROR) ---")
            print(f"HTTP error for openAlgoOrders: {e}")
            continue

        if isinstance(data, dict):
            orders = data.get("orders") or data.get("data") or []
        else:
            orders = data or []

        total_algo_orders += len(orders)

        print(f"\n--- {sym} ({len(orders)}) ---")
        if not orders:
            print("[]")
            continue

        for o in orders:
            print({
                "symbol": o.get("symbol"),
                "clientAlgoId": o.get("clientAlgoId"),
                "algoId": o.get("algoId"),
                "algoType": o.get("algoType"),
                "orderType": o.get("orderType"),
                "side": o.get("side"),
                "positionSide": o.get("positionSide"),
                "quantity": o.get("quantity"),
                "triggerPrice": o.get("triggerPrice"),
                "price": o.get("price"),
                "activatePrice": o.get("activatePrice"),
                "callbackRate": o.get("callbackRate"),
                "reduceOnly": o.get("reduceOnly"),
                "closePosition": o.get("closePosition"),
                "workingType": o.get("workingType"),
                "algoStatus": o.get("algoStatus"),
            })

    print(f"\nTOTAL ALGO OPEN ORDERS: {total_algo_orders}")

acct = signed_get("/fapi/v2/account")
print("\n=== ACCOUNT (/fapi/v2/account) ===")
print({
    "canTrade": acct.get("canTrade"),
    "totalWalletBalance": acct.get("totalWalletBalance"),
    "totalUnrealizedProfit": acct.get("totalUnrealizedProfit"),
    "totalMarginBalance": acct.get("totalMarginBalance"),
    "availableBalance": acct.get("availableBalance"),
    "maxWithdrawAmount": acct.get("maxWithdrawAmount"),
})