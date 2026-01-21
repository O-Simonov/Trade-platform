import sys
from pathlib import Path
from datetime import datetime, timezone
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def dt(ms: int):
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)

BASE = "https://fapi.binance.com"
symbol = "BNBUSDT"

for period in ["5m", "15m", "1h", "4h"]:
    r = requests.get(
        BASE + "/futures/data/openInterestHist",
        params={"symbol": symbol, "period": period, "limit": 10},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()

    print("\n=== period:", period, "items:", len(data))
    for x in data[-3:]:
        ts = int(x.get("timestamp"))
        print(" ", ts, "->", dt(ts))

