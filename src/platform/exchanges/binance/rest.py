from __future__ import annotations
import hmac, hashlib, time
from urllib.parse import urlencode
import requests

BASE_URL = "https://fapi.binance.com"

class BinanceFuturesREST:
    def __init__(self, api_key: str, api_secret: str, timeout: float = 10.0):
        self.api_key = api_key
        self.api_secret = api_secret.encode("utf-8")
        self.timeout = timeout
        self.sess = requests.Session()
        self.sess.headers.update({"X-MBX-APIKEY": api_key})

    def _sign(self, params: dict) -> dict:
        params = dict(params)
        params["timestamp"] = int(time.time() * 1000)
        qs = urlencode(params, doseq=True)
        sig = hmac.new(self.api_secret, qs.encode("utf-8"), hashlib.sha256).hexdigest()
        params["signature"] = sig
        return params

    def _get(self, path: str, params: dict | None = None, signed: bool = False):
        params = params or {}
        if signed:
            params = self._sign(params)
        r = self.sess.get(BASE_URL + path, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, params: dict | None = None, signed: bool = False):
        params = params or {}
        if signed:
            params = self._sign(params)
        r = self.sess.post(BASE_URL + path, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def _put(self, path: str, params: dict | None = None, signed: bool = False):
        params = params or {}
        if signed:
            params = self._sign(params)
        r = self.sess.put(BASE_URL + path, params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def position_risk(self):
        return self._get("/fapi/v2/positionRisk", signed=True)

    def new_order(self, **kwargs):
        return self._post("/fapi/v1/order", params=kwargs, signed=True)

    def open_orders(self, symbol: str | None = None):
        params = {}
        if symbol:
            params["symbol"] = symbol
        return self._get("/fapi/v1/openOrders", params=params, signed=True)

    def create_listen_key(self):
        return self._post("/fapi/v1/listenKey", params={}, signed=False)

    def keepalive_listen_key(self, listen_key: str):
        return self._put("/fapi/v1/listenKey", params={"listenKey": listen_key}, signed=False)

    def premium_index(self, symbol: str | None = None):
        params = {}
        if symbol:
            params["symbol"] = symbol
        return self._get("/fapi/v1/premiumIndex", params=params, signed=False)


    # --- v9 Market State Layer ---
    def account(self):
        """Signed account endpoint (wallet / available / etc)."""
        return self._get("/fapi/v2/account", params={}, signed=True)

    def open_interest_hist(self, *, symbol: str, period: str, limit: int = 30):
        """Open interest history for USDⓈ-M Perpetuals."""
        params = {"symbol": symbol, "period": period, "limit": int(limit)}
        return self._get("/futures/data/openInterestHist", params=params, signed=False)
