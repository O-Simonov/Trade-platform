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
BASE = "https://fapi.binance.com"


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


symbols = ["ENAUSDT", "SOLUSDT", "FILUSDT", "XRPUSDT", "PUMPUSDT"]

print("\n=== OPEN NORMAL ORDERS (/fapi/v1/openOrders) ===")
for sym in symbols:
    data = signed_get("/fapi/v1/openOrders", {"symbol": sym})
    print(f"\n--- {sym} ({len(data)}) ---")
    for o in data:
        print({
            "symbol": o.get("symbol"),
            "clientOrderId": o.get("clientOrderId"),
            "side": o.get("side"),
            "type": o.get("type"),
            "origType": o.get("origType"),
            "origQty": o.get("origQty"),
            "price": o.get("price"),
            "stopPrice": o.get("stopPrice"),
            "activatePrice": o.get("activatePrice"),
            "priceRate": o.get("priceRate"),
            "reduceOnly": o.get("reduceOnly"),
            "positionSide": o.get("positionSide"),
            "status": o.get("status"),
            "workingType": o.get("workingType"),
        })

print("\n=== OPEN ALGO ORDERS (/fapi/v1/openAlgoOrders) ===")
for sym in symbols:
    data = signed_get("/fapi/v1/openAlgoOrders", {"symbol": sym})
    print(f"\n--- {sym} ({len(data)}) ---")
    for o in data:
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

print("\n=== POSITION RISK ===")
pos = signed_get("/fapi/v2/positionRisk")
for p in pos:
    if p.get("symbol") in symbols and abs(float(p.get("positionAmt", "0"))) > 0:
        print({
            "symbol": p.get("symbol"),
            "positionSide": p.get("positionSide"),
            "positionAmt": p.get("positionAmt"),
            "entryPrice": p.get("entryPrice"),
            "markPrice": p.get("markPrice"),
            "unRealizedProfit": p.get("unRealizedProfit"),
        })

acct = signed_get("/fapi/v2/account")
print("\n=== ACCOUNT ===")
print({
    "canTrade": acct.get("canTrade"),
    "totalWalletBalance": acct.get("totalWalletBalance"),
    "totalUnrealizedProfit": acct.get("totalUnrealizedProfit"),
})