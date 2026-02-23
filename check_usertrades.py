from dotenv import load_dotenv
load_dotenv()

import os, time, json, hmac, hashlib, requests
from urllib.parse import urlencode

API_KEY = os.getenv("BINANCE_BASE_MAIN_API_KEY") or os.getenv("BINANCE_MAIN_API_KEY")
API_SECRET = os.getenv("BINANCE_BASE_MAIN_API_SECRET") or os.getenv("BINANCE_MAIN_API_SECRET")
print("API_KEY set:", bool(API_KEY), "API_SECRET set:", bool(API_SECRET))
if not API_KEY or not API_SECRET:
    raise SystemExit("No BINANCE API keys found. Check .env names.")

base = "https://fapi.binance.com"
symbol = "SOLUSDT"
start = int((time.time() - 7*24*3600)*1000)

params = {
    "symbol": symbol,
    "startTime": start,
    "limit": 5,
    "recvWindow": 5000,
    "timestamp": int(time.time()*1000),
}
qs = urlencode(params)
sig = hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
url = f"{base}/fapi/v1/userTrades?{qs}&signature={sig}"

r = requests.get(url, headers={"X-MBX-APIKEY": API_KEY}, timeout=20)
print("status:", r.status_code)
data = r.json()
if isinstance(data, list):
    print("trades_count:", len(data))
    print(json.dumps(data[:2], indent=2))
else:
    print(json.dumps(data, indent=2))
