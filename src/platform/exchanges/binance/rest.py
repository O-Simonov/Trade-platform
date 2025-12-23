# src/platform/exchanges/binance/rest.py
from __future__ import annotations

import hashlib
import hmac
import logging
import time
from typing import Any
from urllib.parse import urlencode

import requests

BASE_URL = "https://fapi.binance.com"

log = logging.getLogger("src.platform.exchanges.binance.rest")


def _ts_ms() -> int:
    return int(time.time() * 1000)


class BinanceFuturesREST:
    """
    Binance USDⓈ-M Futures REST client (signed + public), with retry/backoff for 429/5xx.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        base_url: str = BASE_URL,
        timeout: float = 10.0,
        max_retries: int = 5,
        backoff_base: float = 1.5,
        recv_window: int = 6000,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key or ""
        self.api_secret = (api_secret or "").encode("utf-8")

        self.timeout = float(timeout)
        self.max_retries = int(max_retries)
        self.backoff_base = float(backoff_base)
        self.recv_window = int(recv_window)

        self.sess = requests.Session()
        if self.api_key:
            self.sess.headers.update({"X-MBX-APIKEY": self.api_key})

    # ---------------------------------------------------------------------
    # SIGN
    # ---------------------------------------------------------------------

    def _sign_params(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Adds timestamp (+ recvWindow) and signature to params and returns NEW dict.
        signature = HMAC_SHA256(secret, query_string)
        """
        if not self.api_secret:
            raise RuntimeError("Binance signed request requires api_secret")

        p: dict[str, Any] = dict(params or {})
        p.setdefault("recvWindow", self.recv_window)
        p["timestamp"] = _ts_ms()

        qs = urlencode(p, doseq=True)
        sig = hmac.new(self.api_secret, qs.encode("utf-8"), hashlib.sha256).hexdigest()
        p["signature"] = sig
        return p

    # ---------------------------------------------------------------------
    # CORE REQUEST (WITH BACKOFF)
    # ---------------------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        signed: bool = False,
    ) -> Any:
        url = f"{self.base_url}{path}"
        req_params = dict(params or {})

        if signed:
            req_params = self._sign_params(req_params)

        last_err: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                r = self.sess.request(
                    method=method,
                    url=url,
                    params=req_params,
                    timeout=self.timeout,
                )

                # --- RATE LIMIT ---
                if r.status_code == 429:
                    sleep = self.backoff_base * attempt
                    log.warning(
                        "Binance 429 rate limit (%s %s), retry %d/%d, sleep %.1fs",
                        method, path, attempt, self.max_retries, sleep,
                    )
                    time.sleep(sleep)
                    continue

                # --- TEMP SERVER ERRORS ---
                if r.status_code >= 500:
                    sleep = self.backoff_base * attempt
                    log.warning(
                        "Binance %d server error (%s %s), retry %d/%d, sleep %.1fs",
                        r.status_code, method, path, attempt, self.max_retries, sleep,
                    )
                    time.sleep(sleep)
                    continue

                # --- OTHER ERRORS ---
                if r.status_code >= 400:
                    # Binance часто возвращает {"code":..., "msg":...}
                    try:
                        payload = r.json()
                        code = payload.get("code")
                        msg = payload.get("msg")
                        raise RuntimeError(
                            f"Binance HTTP {r.status_code} {method} {path}: code={code} msg={msg}"
                        )
                    except ValueError:
                        raise RuntimeError(
                            f"Binance HTTP {r.status_code} {method} {path}: {r.text[:500]}"
                        )

                # --- OK ---
                if r.text:
                    return r.json()
                return {}

            except Exception as e:
                last_err = e
                sleep = self.backoff_base * attempt
                # для сетевых/requests ошибок тоже backoff
                log.warning(
                    "Binance request error (%s %s), retry %d/%d, sleep %.1fs | %s",
                    method, path, attempt, self.max_retries, sleep, repr(e),
                )
                time.sleep(sleep)

        raise RuntimeError(
            f"Binance request failed after {self.max_retries} retries: {method} {path} | last_err={last_err!r}"
        )

    # ---------------------------------------------------------------------
    # HTTP WRAPPERS
    # ---------------------------------------------------------------------

    def _get(self, path: str, *, params: dict[str, Any] | None = None, signed: bool = False):
        return self._request("GET", path, params=params, signed=signed)

    def _post(self, path: str, *, params: dict[str, Any] | None = None, signed: bool = False):
        return self._request("POST", path, params=params, signed=signed)

    def _put(self, path: str, *, params: dict[str, Any] | None = None, signed: bool = False):
        return self._request("PUT", path, params=params, signed=signed)

    # ---------------------------------------------------------------------
    # API METHODS
    # ---------------------------------------------------------------------

    def fetch_exchange_info(self) -> dict:
        return self._get("/fapi/v1/exchangeInfo", signed=False)

    def position_risk(self):
        return self._get("/fapi/v2/positionRisk", signed=True)

    def account(self):
        return self._get("/fapi/v2/account", signed=True)

    def fetch_position_mode(self) -> dict:
        # {"dualSidePosition": true/false}
        return self._get("/fapi/v1/positionSide/dual", signed=True)

    def new_order(self, **kwargs):
        # IMPORTANT: kwargs must contain proper params for Binance: symbol, side, type, quantity, etc.
        return self._post("/fapi/v1/order", params=kwargs, signed=True)

    def open_orders(self, symbol: str | None = None):
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        return self._get("/fapi/v1/openOrders", params=params, signed=True)

    def create_listen_key(self):
        return self._post("/fapi/v1/listenKey", signed=False)

    def keepalive_listen_key(self, listen_key: str):
        return self._put("/fapi/v1/listenKey", params={"listenKey": listen_key}, signed=False)

    def premium_index(self, symbol: str | None = None):
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        return self._get("/fapi/v1/premiumIndex", params=params, signed=False)

    def open_interest_hist(self, *, symbol: str, period: str, limit: int = 30):
        params = {"symbol": symbol, "period": period, "limit": int(limit)}
        return self._get("/futures/data/openInterestHist", params=params, signed=False)
