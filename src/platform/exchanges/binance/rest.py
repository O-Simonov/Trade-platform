# src/platform/exchanges/binance/rest.py
from __future__ import annotations

import hashlib
import hmac
import inspect
import logging
import os
import re
import threading
import time
from typing import Any,Optional
from urllib.parse import urlencode

import requests

BASE_URL = "https://fapi.binance.com"

log = logging.getLogger("src.platform.exchanges.binance.rest")


def _ts_ms() -> int:
    return int(time.time() * 1000)


def _truthy_env(name: str, default: str = "0") -> bool:
    v = str(os.environ.get(name, default)).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _fmt_params(params: dict[str, Any] | None, max_len: int = 220) -> str:
    if not params:
        return ""
    keys = ("symbol", "symbols", "interval", "period", "limit", "startTime", "endTime")
    out: list[str] = []
    for k in keys:
        if k in params and params[k] is not None:
            out.append(f"{k}={params[k]}")
    s = " ".join(out)
    if len(s) > max_len:
        s = s[: max_len - 3] + "..."
    return s


def _retry_after_sec(headers: dict[str, Any]) -> float | None:
    try:
        ra = headers.get("Retry-After") or headers.get("retry-after")
        if ra is None:
            return None
        v = float(str(ra).strip())
        if v <= 0:
            return None
        return min(v, 60.0)
    except Exception:
        return None


class BinanceNonRetryableError(RuntimeError):
    """Ошибка, которую не надо ретраить (напр. 451 restricted location)."""


class BinanceFuturesREST:
    """
    Binance USDⓈ-M Futures REST client.

    Diagnostics env:
      BINANCE_REST_CALLSITE=1  -> caller=file:line:function
      BINANCE_REST_HEADERS=1   -> log used-weight headers
      BINANCE_REST_COUNTERS=1  -> per-minute counters
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

        # ban circuit breaker: "banned until <ms>"
        self._ban_until_ms: int = 0
        self._ban_owner: str = ""      # <-- КТО поставил бан (thread+caller+params)
        self._ban_reason: str = ""     # <-- почему (418/429/451 + msg)

        # if 451 happens once -> we stop all further requests fast
        self._restricted_location: bool = False

        # Diagnostics toggles
        self._dbg_callsite = _truthy_env("BINANCE_REST_CALLSITE", "0")
        self._dbg_headers = _truthy_env("BINANCE_REST_HEADERS", "0")
        self._dbg_counters = _truthy_env("BINANCE_REST_COUNTERS", "0")

        # Counters (per-minute)
        self._ctr_lock = threading.Lock()
        self._ctr_win_start = int(time.time())
        self._ctr_total = 0
        self._ctr_by_path: dict[str, int] = {}

        self._ctr_warn_total = int(os.environ.get("BINANCE_REST_WARN_TOTAL_PER_MIN", "2000"))
        self._ctr_warn_path = int(os.environ.get("BINANCE_REST_WARN_PATH_PER_MIN", "800"))

        # cooldown for 451 restricted location (seconds)
        self._cooldown_451_sec = int(os.environ.get("BINANCE_REST_451_COOLDOWN_SEC", "900"))

    # ---------------------------------------------------------------------
    # SIGN
    # ---------------------------------------------------------------------

    def _sign_params(self, params: dict[str, Any]) -> dict[str, Any]:
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
    # BAN / RATE LIMIT HELPERS
    # ---------------------------------------------------------------------

    @staticmethod
    def _parse_ban_until_ms(text: str) -> int | None:
        if not text:
            return None
        m = re.search(r"banned until (\d+)", text, re.IGNORECASE)
        if not m:
            return None
        try:
            return int(m.group(1))
        except Exception:
            return None

    def _sleep_if_banned(self, *, meta: str = "") -> None:
        while True:
            now = _ts_ms()
            if now >= self._ban_until_ms:
                return
            remain_sec = max(0.0, (self._ban_until_ms - now) / 1000.0) + 1.0
            chunk = min(60.0, remain_sec)
            log.warning(
                "Binance BAN active until_ms=%s -> sleep %.1fs | owner=%s | reason=%s | %s",
                self._ban_until_ms,
                chunk,
                (self._ban_owner or "?")[:260],
                (self._ban_reason or "?")[:200],
                meta,
            )
            time.sleep(chunk)

    # ---------------------------------------------------------------------
    # DIAGNOSTICS
    # ---------------------------------------------------------------------

    @staticmethod
    def _thread_tag() -> str:
        try:
            t = threading.current_thread()
            return f"thread={t.name}"
        except Exception:
            return "thread=?"

    def _callsite_tag(self) -> str:
        if not self._dbg_callsite:
            return ""
        try:
            this_file = os.path.normcase(__file__)
            for fr in inspect.stack()[2:12]:
                fname = os.path.normcase(fr.filename)
                if fname != this_file:
                    base = os.path.basename(fr.filename)
                    return f"caller={base}:{fr.lineno}:{fr.function}"
        except Exception:
            return "caller=?"
        return ""

    def _counter_tick(self, path: str) -> str:
        if not self._dbg_counters:
            return ""

        now = int(time.time())
        with self._ctr_lock:
            if now - self._ctr_win_start >= 60:
                self._ctr_win_start = now
                self._ctr_total = 0
                self._ctr_by_path = {}

            self._ctr_total += 1
            self._ctr_by_path[path] = self._ctr_by_path.get(path, 0) + 1

            total = self._ctr_total
            pctr = self._ctr_by_path[path]

        if total >= self._ctr_warn_total or pctr >= self._ctr_warn_path:
            log.warning("Binance REST counters high: total_1m=%d path_1m=%d path=%s", total, pctr, path)

        return f"ctr_1m={total} path_1m={pctr}"

    # ---------------------------------------------------------------------
    # CORE REQUEST
    # ---------------------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        signed: bool = False,
    ) -> Any:
        if self._restricted_location:
            raise BinanceNonRetryableError("Binance restricted location already detected (451); stop requests.")

        url = f"{self.base_url}{path}"
        req_params = dict(params or {})

        thr = self._thread_tag()
        caller = self._callsite_tag()
        ctr = self._counter_tick(path)
        ptxt = _fmt_params(req_params)

        meta_parts = [thr]
        if caller:
            meta_parts.append(caller)
        if ctr:
            meta_parts.append(ctr)
        if ptxt:
            meta_parts.append(f"params[{ptxt}]")
        meta = " ".join(meta_parts)

        if signed:
            req_params = self._sign_params(req_params)

        last_err: Exception | None = None
        attempt = 1

        while attempt <= self.max_retries:
            self._sleep_if_banned(meta=meta)

            try:
                r = self.sess.request(
                    method=method,
                    url=url,
                    params=req_params,
                    timeout=self.timeout,
                )

                if self._dbg_headers:
                    w1m = r.headers.get("X-MBX-USED-WEIGHT-1M")
                    oc1m = r.headers.get("X-MBX-ORDER-COUNT-1M")
                    if w1m or oc1m:
                        log.info(
                            "Binance headers (%s %s): USED_WEIGHT_1M=%s ORDER_COUNT_1M=%s %s",
                            method,
                            path,
                            w1m,
                            oc1m,
                            meta,
                        )

                # ----------------------------
                # 451: RESTRICTED LOCATION
                # ----------------------------
                if r.status_code == 451:
                    msg_text = ""
                    try:
                        payload = r.json()
                        msg_text = str(payload.get("msg", "") or "")
                    except Exception:
                        msg_text = (r.text or "")[:400]

                    now = _ts_ms()
                    self._restricted_location = True
                    self._ban_until_ms = max(self._ban_until_ms, now + self._cooldown_451_sec * 1000)
                    self._ban_owner = meta
                    self._ban_reason = f"451 restricted location: {msg_text[:180]}"

                    log.error(
                        "Binance HTTP 451 (%s %s): restricted location. Cooldown %ss until_ms=%s | %s | %s",
                        method,
                        path,
                        self._cooldown_451_sec,
                        self._ban_until_ms,
                        msg_text[:200],
                        meta,
                    )
                    raise BinanceNonRetryableError(f"Binance restricted location (451): {msg_text[:200]}")

                # --- BAN / RATE LIMIT (418/429) ---
                if r.status_code in (418, 429):
                    ban_until: int | None = None
                    msg_text = ""

                    try:
                        payload = r.json()
                        msg_text = str(payload.get("msg", "") or "")
                        ban_until = self._parse_ban_until_ms(msg_text)
                    except Exception:
                        msg_text = r.text or ""
                        ban_until = self._parse_ban_until_ms(msg_text)

                    if ban_until:
                        self._ban_until_ms = max(self._ban_until_ms, int(ban_until))
                        self._ban_owner = meta
                        self._ban_reason = f"{r.status_code} banned-until: {(msg_text or '')[:180]}"
                        log.warning(
                            "Binance HTTP %d (%s %s) banned until_ms=%s -> wait | %s | %s",
                            r.status_code,
                            method,
                            path,
                            self._ban_until_ms,
                            (msg_text or "")[:200],
                            meta,
                        )
                        continue

                    ra = _retry_after_sec(r.headers) or 0.0
                    sleep = max(ra, self.backoff_base * attempt)

                    if r.status_code == 429:
                        self._ban_owner = meta
                        self._ban_reason = f"429 rate-limit: {(msg_text or '')[:180]}"

                    log.warning(
                        "Binance %d (%s %s), retry %d/%d, sleep %.1fs | %s | %s",
                        r.status_code,
                        method,
                        path,
                        attempt,
                        self.max_retries,
                        sleep,
                        (msg_text or "")[:200],
                        meta,
                    )
                    time.sleep(sleep)
                    attempt += 1
                    continue

                # --- TEMP SERVER ERRORS ---
                if r.status_code >= 500:
                    sleep = self.backoff_base * attempt
                    log.warning(
                        "Binance %d server error (%s %s), retry %d/%d, sleep %.1fs | %s",
                        r.status_code,
                        method,
                        path,
                        attempt,
                        self.max_retries,
                        sleep,
                        meta,
                    )
                    time.sleep(sleep)
                    attempt += 1
                    continue

                # --- OTHER 4xx ---
                if r.status_code >= 400:
                    try:
                        payload = r.json()
                        code = payload.get("code")
                        msg = payload.get("msg")
                        raise RuntimeError(
                            f"Binance HTTP {r.status_code} {method} {path}: code={code} msg={msg} | {meta}"
                        )
                    except ValueError:
                        raise RuntimeError(f"Binance HTTP {r.status_code} {method} {path}: {r.text[:500]} | {meta}")

                # --- OK ---
                if r.text:
                    return r.json()
                return {}

            except BinanceNonRetryableError:
                raise
            except Exception as e:
                last_err = e
                sleep = self.backoff_base * attempt
                log.warning(
                    "Binance request error (%s %s), retry %d/%d, sleep %.1fs | %s | %s",
                    method,
                    path,
                    attempt,
                    self.max_retries,
                    sleep,
                    repr(e),
                    meta,
                )
                time.sleep(sleep)
                attempt += 1

        raise RuntimeError(
            f"Binance request failed after {self.max_retries} retries: {method} {path} | last_err={last_err!r}"
        )

    # ---------------------------------------------------------------------
    # HTTP WRAPPERS
    # ---------------------------------------------------------------------

    def _get(self, path: str, *, params: dict[str, Any] | None = None, signed: bool = False):
        return self._request("GET", path, params=params, signed=signed)

    # ---------------------------------------------------------------------
    # API METHODS
    # ---------------------------------------------------------------------

    def fetch_exchange_info(self) -> dict:
        return self._get("/fapi/v1/exchangeInfo", signed=False)

    def premium_index(self, symbol: str | None = None):
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        return self._get("/fapi/v1/premiumIndex", params=params, signed=False)

    def klines(
        self,
        *,
        symbol: str,
        interval: str,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 1500,
    ):
        params: dict[str, Any] = {"symbol": str(symbol).upper(), "interval": str(interval), "limit": int(limit)}
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)
        return self._get("/fapi/v1/klines", params=params, signed=False)

    def funding_rate(
        self,
        *,
        symbol: str,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 1000,
    ):
        params: dict[str, Any] = {"symbol": str(symbol).upper(), "limit": int(limit)}
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)
        return self._get("/fapi/v1/fundingRate", params=params, signed=False)

    def open_interest_hist(
        self,
        *,
        symbol: str,
        period: str,
        limit: int = 30,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ):
        params: dict[str, Any] = {"symbol": str(symbol).upper(), "period": str(period), "limit": int(limit)}
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)
        return self._get("/futures/data/openInterestHist", params=params, signed=False)

    def ticker_24h(self, *, symbol: Optional[str] = None) -> Any:
        """
        GET /fapi/v1/ticker/24hr
        - symbol=None -> returns list[dict] for all symbols
        - symbol='BTCUSDT' -> returns dict for one symbol
        """
        params = {}
        if symbol:
            params["symbol"] = str(symbol).upper()

        return self._request(
            method="GET",
            path="/fapi/v1/ticker/24hr",
            params=params,
            signed=False,
        )

    # alias for your collector name
    def get_24hr_ticker(self, *, symbol: str | None = None) -> Any:
        return self.ticker_24h(symbol=symbol)
