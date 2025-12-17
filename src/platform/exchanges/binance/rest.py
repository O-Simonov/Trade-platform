from __future__ import annotations

import hmac
import hashlib
import time
import logging
from urllib.parse import urlencode
import requests

BASE_URL = "https://fapi.binance.com"

logger = logging.getLogger(__name__)


class BinanceFuturesREST:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        timeout: float = 10.0,
        max_retries: int = 5,
        backoff_base: float = 1.5,
    ):
        self.api_key = api_key
        self.api_secret = api_secret.encode("utf-8")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_base = backoff_base

        self.sess = requests.Session()
        self.sess.headers.update({"X-MBX-APIKEY": api_key})

    # ---------------------------------------------------------------------
    # SIGN
    # ---------------------------------------------------------------------

    def _sign(self, params: dict) -> dict:
        params = dict(params)
        params["timestamp"] = int(time.time() * 1000)
        qs = urlencode(params, doseq=True)
        sig = hmac.new(self.api_secret, qs.encode(), hashlib.sha256).hexdigest()
        params["signature"] = sig
        return params

    # ---------------------------------------------------------------------
    # CORE REQUEST (WITH BACKOFF)
    # ---------------------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        signed: bool = False,
    ):
        params = params or {}
        if signed:
            params = self._sign(params)

        url = BASE_URL + path

        for attempt in range(1, self.max_retries + 1):
            r = self.sess.request(
                method,
                url,
                params=params,
                timeout=self.timeout,
            )

            # --- RATE LIMIT ---
            if r.status_code == 429:
                sleep = self.backoff_base * attempt
                logger.warning(
                    "Binance 429 rate limit (%s %s), retry %d/%d, sleep %.1fs",
                    method,
                    path,
                    attempt,
                    self.max_retries,
                    sleep,
                )
                time.sleep(sleep)
                continue

            # --- TEMP SERVER ERRORS ---
            if r.status_code >= 500:
                sleep = self.backoff_base * attempt
                logger.warning(
                    "Binance %d server error (%s %s), retry %d/%d, sleep %.1fs",
                    r.status_code,
                    method,
                    path,
                    attempt,
                    self.max_retries,
                    sleep,
                )
                time.sleep(sleep)
                continue

            # --- OK / OTHER ERRORS ---
            r.raise_for_status()
            return r.json()

        raise RuntimeError(f"Binance request failed after {self.max_retries} retries: {path}")

    # ---------------------------------------------------------------------
    # HTTP WRAPPERS
    # ---------------------------------------------------------------------

    def _get(self, path: str, params: dict | None = None, signed: bool = False):
        return self._request("GET", path, params=params, signed=signed)

    def _post(self, path: str, params: dict | None = None, signed: bool = False):
        return self._request("POST", path, params=params, signed=signed)

    def _put(self, path: str, params: dict | None = None, signed: bool = False):
        return self._request("PUT", path, params=params, signed=signed)

    # ---------------------------------------------------------------------
    # API METHODS
    # ---------------------------------------------------------------------

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
        return self._post("/fapi/v1/listenKey", signed=False)

    def keepalive_listen_key(self, listen_key: str):
        return self._put("/fapi/v1/listenKey", params={"listenKey": listen_key}, signed=False)

    def premium_index(self, symbol: str | None = None):
        params = {}
        if symbol:
            params["symbol"] = symbol
        return self._get("/fapi/v1/premiumIndex", params=params, signed=False)

    # --- Market State (v9) ---

    def account(self):
        return self._get("/fapi/v2/account", signed=True)

    def open_interest_hist(self, *, symbol: str, period: str, limit: int = 30):
        params = {"symbol": symbol, "period": period, "limit": int(limit)}
        return self._get("/futures/data/openInterestHist", params=params, signed=False)

    def fetch_exchange_info(self) -> dict:
        """
        Binance Futures exchangeInfo
        Filters, symbols, limits.
        """
        return self._get("/fapi/v1/exchangeInfo", signed=False)
