# src/platform/exchanges/binance/rest.py
from __future__ import annotations

import hashlib
import hmac
import logging
import os
import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Tuple
from urllib.parse import urlencode

import requests
from requests import Response

log = logging.getLogger("src.platform.exchanges.binance.rest")


# ============================================================
# Base URL
# ============================================================
def _env_str(name: str, default: str) -> str:
    v = os.environ.get(name, None)
    if v is None:
        return str(default)
    return str(v).strip()


BASE_URL = _env_str("BINANCE_FAPI_BASE_URL", "https://fapi.binance.com")


# ============================================================
# Env utils
# ============================================================
def _truthy_env(name: str, default: str = "0") -> bool:
    v = str(os.environ.get(name, default)).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def _safe_float_env(name: str, default: float) -> float:
    v = os.environ.get(name, None)
    if v is None:
        return float(default)
    try:
        return float(str(v).strip())
    except Exception:
        return float(default)


def _safe_int_env(name: str, default: int) -> int:
    v = os.environ.get(name, None)
    if v is None:
        return int(default)
    try:
        return int(str(v).strip())
    except Exception:
        return int(default)


def _fmt_params(params: dict[str, Any] | None, max_len: int = 220) -> str:
    """
    Короткий вывод ключевых параметров (чтобы лог не раздувался).
    """
    if not params:
        return ""
    keys = (
        "symbol",
        "symbols",
        "interval",
        "period",
        "limit",
        "startTime",
        "endTime",
        "listenKey",
        "orderId",
        "origClientOrderId",
        "fromId",
        "recvWindow",
        "timestamp",
    )
    out: list[str] = []
    for k in keys:
        if k in params and params[k] is not None:
            out.append(f"{k}={params[k]}")
    s = " ".join(out)
    if len(s) > max_len:
        s = s[:max_len] + "…"
    return s


# ============================================================
# Retry / rate-limit helpers
# ============================================================
def _is_rate_limited(resp_json: Any, status_code: int) -> bool:
    """
    Считаем rate-limit если:
      - HTTP 429
      - code == -1003 (Binance)
    """
    if status_code == 429:
        return True
    if isinstance(resp_json, dict):
        try:
            code = int(resp_json.get("code") or 0)
        except Exception:
            code = 0
        return code == -1003
    return False


def _is_retryable_http(status_code: int) -> bool:
    """
    418/429 — лимиты/бан, 5xx — временная проблема.
    """
    return status_code in (418, 429) or (500 <= status_code <= 599)


def _looks_like_dns_error(exc: Exception) -> bool:
    s = str(exc).lower()
    return (
        ("getaddrinfo" in s)
        or ("name or service not known" in s)
        or ("nodename nor servname provided" in s)
        or ("temporary failure in name resolution" in s)
    )


# ============================================================
# Token bucket limiter (per-process)
# ============================================================
class _TokenBucket:
    def __init__(self, *, capacity: float, refill_per_sec: float) -> None:
        self.capacity = float(max(0.05, capacity))
        self.refill_per_sec = float(max(0.001, refill_per_sec))
        self.tokens = float(self.capacity)
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def _refill(self, now: float) -> None:
        dt = max(0.0, now - self.last)
        if dt <= 0.0:
            return
        self.tokens = min(self.capacity, self.tokens + dt * self.refill_per_sec)
        self.last = now

    def acquire(self, amount: float, *, stop_event: threading.Event | None = None) -> None:
        amt = float(max(0.0, amount))
        if amt <= 0.0:
            return

        while True:
            if stop_event is not None and stop_event.is_set():
                return

            now = time.monotonic()
            with self.lock:
                self._refill(now)
                if self.tokens >= amt:
                    self.tokens -= amt
                    return

                missing = amt - self.tokens
                wait_sec = missing / self.refill_per_sec

            wait_sec = max(0.0, float(wait_sec)) + random.uniform(0.0, 0.05)
            time.sleep(min(wait_sec, 1.0))


class _GlobalRateLimiter:
    def __init__(self) -> None:
        max_rps = _safe_float_env("BINANCE_REST_MAX_RPS", 8.0)
        max_rpm = _safe_float_env("BINANCE_REST_MAX_RPM", 900.0)

        max_rps = max(0.05, float(max_rps))
        max_rpm = max(10.0, float(max_rpm))

        self.max_rps = float(max_rps)
        self.max_rpm = float(max_rpm)

        self.bucket_rps = _TokenBucket(capacity=max_rps, refill_per_sec=max_rps)
        self.bucket_rpm = _TokenBucket(capacity=max_rpm, refill_per_sec=max_rpm / 60.0)

        self.stop_event = threading.Event()

        log.info(
            "[REST RL] init max_rps=%.2f max_rpm=%.0f (env BINANCE_REST_MAX_RPS/BINANCE_REST_MAX_RPM)",
            max_rps,
            max_rpm,
        )

    def acquire(self, weight: float) -> None:
        w = float(max(1.0, weight))
        self.bucket_rpm.acquire(w, stop_event=self.stop_event)
        self.bucket_rps.acquire(w, stop_event=self.stop_event)


_GLOBAL_LIMITER: Optional[_GlobalRateLimiter] = None
_LIMITER_LOCK = threading.Lock()
_LIMITER_LOGGED = False


def _get_limiter() -> _GlobalRateLimiter:
    global _GLOBAL_LIMITER, _LIMITER_LOGGED
    with _LIMITER_LOCK:
        if _GLOBAL_LIMITER is None:
            _GLOBAL_LIMITER = _GlobalRateLimiter()
        if not _LIMITER_LOGGED:
            _LIMITER_LOGGED = True
            log.warning(
                "[REST RL] effective max_rps=%.2f max_rpm=%.0f (per-process limiter)",
                _GLOBAL_LIMITER.max_rps,
                _GLOBAL_LIMITER.max_rpm,
            )
        return _GLOBAL_LIMITER


class _GlobalCooldown:
    """
    Глобальный cooldown на процесс после 429/-1003.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._until_mono: float = 0.0
        self._last_reason: str | None = None

    def arm(self, *, seconds: float, reason: str) -> None:
        s = float(max(0.0, seconds))
        if s <= 0.0:
            return
        now = time.monotonic()
        s = s + random.uniform(0.0, 0.4)  # jitter
        with self._lock:
            new_until = now + s
            if new_until > self._until_mono:
                self._until_mono = new_until
                self._last_reason = reason

    def wait_if_needed(self) -> None:
        while True:
            now = time.monotonic()
            with self._lock:
                until = self._until_mono
                reason = self._last_reason
            if now >= until:
                return
            sleep_s = min(1.0, max(0.05, until - now))
            log.warning("[REST COOLDOWN] sleep=%.2fs reason=%s", sleep_s, reason)
            time.sleep(sleep_s)


_GLOBAL_COOLDOWN = _GlobalCooldown()


def _endpoint_weight(method: str, path: str) -> float:
    """
    Грубые веса. (главное — чтобы position/account были тяжелее)
    """
    m = (method or "").upper()
    p = str(path or "")

    if p.startswith("/fapi/v1/listenKey"):
        return 1.0

    if p.startswith("/fapi/v1/order") or p.startswith("/fapi/v1/openOrders") or p.startswith("/fapi/v1/allOrders"):
        return 1.0

    if p.startswith("/fapi/v2/account"):
        return 10.0
    if p.startswith("/fapi/v3/positionRisk"):
        return 10.0

    if p.startswith("/fapi/v1/exchangeInfo"):
        return 2.0
    if p.startswith("/fapi/v1/klines"):
        return 2.0
    if p.startswith("/futures/data/openInterestHist"):
        return 2.0
    if p.startswith("/fapi/v1/openInterest"):
        return 1.0
    if p.startswith("/fapi/v1/fundingRate"):
        return 2.0
    if p.startswith("/fapi/v1/premiumIndex"):
        return 2.0
    if p.startswith("/fapi/v1/ticker/24hr"):
        return 2.0
    if p.startswith("/fapi/v1/time"):
        return 1.0
    if p.startswith("/fapi/v1/aggTrades"):
        return 2.0

    if m in ("GET", "POST", "PUT", "DELETE"):
        return 1.0
    return 1.0


# ============================================================
# Ошибка REST с контекстом
# ============================================================
@dataclass
class BinanceRESTError(RuntimeError):
    method: str
    path: str
    status: int | None = None
    payload: Any | None = None
    message: str = ""

    def __str__(self) -> str:
        base = f"Binance REST error: {self.method} {self.path}"
        if self.status is not None:
            base += f" HTTP {self.status}"
        if self.message:
            base += f" | {self.message}"
        if self.payload is not None:
            base += f" | resp={self.payload!r}"
        return base


# ============================================================
# Основной REST клиент
# ============================================================
class BinanceFuturesREST:
    """
    REST клиент для Binance USDⓈ-M Futures (fapi).
    """

    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key or ""
        self.secret_key = secret_key or ""

        # retry настройки
        self.max_retries = _safe_int_env("BINANCE_REST_RETRY_MAX", 6)
        self.base_backoff = _safe_float_env("BINANCE_REST_RETRY_BASE_SEC", 0.35)
        self.max_backoff = _safe_float_env("BINANCE_REST_RETRY_MAX_SEC", 30.0)

        # таймауты
        self.timeout_sec = _safe_float_env("BINANCE_REST_TIMEOUT_SEC", 10.0)
        # connect/read timeout (чуть стабильнее)
        self.timeout: Tuple[float, float] = (min(5.0, self.timeout_sec), float(self.timeout_sec))

        # debug
        self.debug_headers = _truthy_env("BINANCE_REST_DEBUG_HEADERS", "0")

        # доверять ли env (HTTP_PROXY/HTTPS_PROXY и т.п.)
        self.trust_env = _truthy_env("BINANCE_REST_TRUST_ENV", "0")

        # TTL cache
        self._cache_lock = threading.Lock()
        self._cache: Dict[str, Tuple[float, Any]] = {}

        self.position_risk_ttl_sec = _safe_float_env("BINANCE_REST_POSITION_RISK_TTL_SEC", 2.0)
        self.account_ttl_sec = _safe_float_env("BINANCE_REST_ACCOUNT_TTL_SEC", 3.0)

        # time offset (-1021)
        self._time_offset_ms = 0
        self._time_lock = threading.Lock()
        self._time_last_sync_mono = 0.0
        self._time_sync_ttl_sec = _safe_float_env("BINANCE_REST_TIME_SYNC_TTL_SEC", 30.0)

        # session
        self._session_lock = threading.Lock()
        self.session = self._make_session()

    # ----------------------------
    # Session / cache / time sync
    # ----------------------------
    def _make_session(self) -> requests.Session:
        s = requests.Session()
        if self.api_key:
            s.headers.update({"X-MBX-APIKEY": self.api_key})
        s.trust_env = bool(self.trust_env)
        return s

    def _reset_session(self) -> None:
        with self._session_lock:
            try:
                old = self.session
                self.session = self._make_session()
                try:
                    old.close()
                except Exception:
                    pass
            except Exception:
                self.session = requests.Session()
                if self.api_key:
                    self.session.headers.update({"X-MBX-APIKEY": self.api_key})
        log.warning("[REST] session reset")

    def _cache_get(self, key: str) -> Any | None:
        now = time.monotonic()
        with self._cache_lock:
            item = self._cache.get(key)
            if not item:
                return None
            until, val = item
            if now <= float(until):
                return val
            self._cache.pop(key, None)
            return None

    def _cache_put(self, key: str, ttl_sec: float, value: Any) -> None:
        ttl = float(max(0.0, ttl_sec))
        if ttl <= 0.0:
            return
        until = time.monotonic() + ttl
        with self._cache_lock:
            self._cache[key] = (until, value)

    def _ts_ms_with_offset(self) -> int:
        return int(time.time() * 1000) + int(self._time_offset_ms)

    def _sync_time_offset(self, *, force: bool = False) -> None:
        now = time.monotonic()
        with self._time_lock:
            if (not force) and (now - self._time_last_sync_mono) < float(self._time_sync_ttl_sec):
                return
            self._time_last_sync_mono = now

        try:
            data = self._get("/fapi/v1/time", params=None, signed=False)
            srv = int((data or {}).get("serverTime", 0))
            if srv > 0:
                local = int(time.time() * 1000)
                self._time_offset_ms = srv - local
                log.warning("[REST TIME] synced offset_ms=%d", int(self._time_offset_ms))
        except Exception:
            log.exception("[REST TIME] sync failed")

    # ----------------------------
    # low-level request
    # ----------------------------
    def _sign_from_items(self, items: Iterable[tuple[str, Any]]) -> str:
        query_string = urlencode(list(items), doseq=True)
        return hmac.new(self.secret_key.encode(), query_string.encode(), hashlib.sha256).hexdigest()

    def _request(self, method: str, path: str, params: dict[str, Any] | None = None, signed: bool = False) -> Any:
        method = str(method).upper()
        url = f"{BASE_URL}{path}"
        params = params or {}

        weight = _endpoint_weight(method, path)
        attempt = 0
        backoff = float(self.base_backoff)

        while True:
            attempt += 1

            _GLOBAL_COOLDOWN.wait_if_needed()
            _get_limiter().acquire(weight)

            if signed:
                params_to_send = dict(params)
                params_to_send.setdefault("recvWindow", 5000)
                params_to_send["timestamp"] = self._ts_ms_with_offset()

                items = list(params_to_send.items())
                signature = self._sign_from_items(items)
                items.append(("signature", signature))
                send_params: Any = items
            else:
                send_params = params if params else None

            try:
                with self._session_lock:
                    sess = self.session

                if method == "GET":
                    response = sess.get(url, params=send_params, timeout=self.timeout)
                elif method == "POST":
                    response = sess.post(url, params=send_params, timeout=self.timeout)
                elif method == "PUT":
                    response = sess.put(url, params=send_params, timeout=self.timeout)
                elif method == "DELETE":
                    response = sess.delete(url, params=send_params, timeout=self.timeout)
                else:
                    raise ValueError(f"Unknown method {method}")

            except Exception as e:
                if _looks_like_dns_error(e) or isinstance(e, requests.exceptions.ConnectionError):
                    self._reset_session()

                if attempt <= self.max_retries:
                    sleep_s = min(backoff, self.max_backoff) + random.uniform(0.0, 0.3)
                    log.warning(
                        "[REST NET] %s %s retry=%d/%d sleep=%.2fs ctx=%s err=%s",
                        method,
                        path,
                        attempt,
                        self.max_retries,
                        sleep_s,
                        _fmt_params(params),
                        e,
                    )
                    time.sleep(sleep_s)
                    backoff = min(backoff * 2.0, self.max_backoff)
                    continue

                raise BinanceRESTError(method=method, path=path, status=None, payload=None, message=str(e)) from e

            status = int(response.status_code)

            try:
                data = self._safe_json(response)
            except BinanceRESTError as e:
                if attempt <= self.max_retries:
                    sleep_s = min(backoff, self.max_backoff) + random.uniform(0.0, 0.3)
                    log.warning(
                        "[REST JSON] %s %s retry=%d/%d sleep=%.2fs ctx=%s err=%s",
                        method,
                        path,
                        attempt,
                        self.max_retries,
                        sleep_s,
                        _fmt_params(params),
                        e,
                    )
                    time.sleep(sleep_s)
                    backoff = min(backoff * 2.0, self.max_backoff)
                    continue
                raise

            if self.debug_headers:
                used = response.headers.get("X-MBX-USED-WEIGHT-1M") or response.headers.get("x-mbx-used-weight-1m")
                if used:
                    log.info("[REST HDR] used_weight_1m=%s %s %s", used, method, path)

            if _is_rate_limited(data, status):
                retry_after = response.headers.get("Retry-After")
                try:
                    ra = float(retry_after) if retry_after else 0.0
                except Exception:
                    ra = 0.0

                cooldown = max(ra, min(backoff, self.max_backoff), 20.0)
                cooldown = min(cooldown, 120.0)

                _GLOBAL_COOLDOWN.arm(seconds=cooldown, reason=f"{method} {path} 429/-1003")

                if attempt <= self.max_retries:
                    sleep_s = max(1.0, cooldown * 0.5) + random.uniform(0.0, 0.5)
                    log.warning(
                        "[REST 429] %s %s retry=%d/%d sleep=%.2fs cooldown=%.2fs ctx=%s resp=%r",
                        method,
                        path,
                        attempt,
                        self.max_retries,
                        sleep_s,
                        cooldown,
                        _fmt_params(params),
                        data,
                    )
                    time.sleep(sleep_s)
                    backoff = min(max(backoff * 2.0, 1.0), self.max_backoff)
                    continue

                raise BinanceRESTError(method=method, path=path, status=status, payload=data, message="rate-limited")

            if status >= 400:
                if attempt <= self.max_retries and _is_retryable_http(status):
                    sleep_s = min(backoff, self.max_backoff) + random.uniform(0.0, 0.3)
                    log.warning(
                        "[REST HTTP] %s %s status=%s retry=%d/%d sleep=%.2fs ctx=%s resp=%r",
                        method,
                        path,
                        status,
                        attempt,
                        self.max_retries,
                        sleep_s,
                        _fmt_params(params),
                        data,
                    )
                    time.sleep(sleep_s)
                    backoff = min(backoff * 2.0, self.max_backoff)
                    continue

                raise BinanceRESTError(method=method, path=path, status=status, payload=data, message="http error")

            if isinstance(data, dict) and "code" in data:
                try:
                    code = int(data.get("code") or 0)
                except Exception:
                    code = 0

                if code < 0:
                    if code == -1021 and attempt <= self.max_retries:
                        self._sync_time_offset(force=True)
                        sleep_s = min(backoff, self.max_backoff) + random.uniform(0.0, 0.3)
                        log.warning(
                            "[REST API] %s %s code=%s retry=%d/%d sleep=%.2fs ctx=%s resp=%r",
                            method,
                            path,
                            code,
                            attempt,
                            self.max_retries,
                            sleep_s,
                            _fmt_params(params),
                            data,
                        )
                        time.sleep(sleep_s)
                        backoff = min(backoff * 2.0, self.max_backoff)
                        continue

                    raise BinanceRESTError(method=method, path=path, status=status, payload=data, message="api error")

            return data

    @staticmethod
    def _safe_json(response: Response) -> Any:
        try:
            return response.json()
        except Exception:
            txt = (response.text or "").strip()
            raise BinanceRESTError(
                method="(parse)",
                path="(json)",
                status=int(getattr(response, "status_code", 0) or 0),
                payload=(txt[:500] if txt else ""),
                message="non-json response",
            )

    def _get(self, path: str, params: dict[str, Any] | None = None, signed: bool = False) -> Any:
        return self._request("GET", path, params=params, signed=signed)

    def _post(self, path: str, params: dict[str, Any] | None = None, signed: bool = False) -> Any:
        return self._request("POST", path, params=params, signed=signed)

    def _put(self, path: str, params: dict[str, Any] | None = None, signed: bool = False) -> Any:
        return self._request("PUT", path, params=params, signed=signed)

    def _delete(self, path: str, params: dict[str, Any] | None = None, signed: bool = False) -> Any:
        return self._request("DELETE", path, params=params, signed=signed)

    # ============================================================
    # USER DATA STREAM (listenKey)
    # ============================================================
    def create_listen_key(self) -> dict[str, Any]:
        return dict(self._post("/fapi/v1/listenKey", params=None, signed=False) or {})

    def keepalive_listen_key(self, listen_key: str) -> dict[str, Any]:
        if not listen_key:
            raise ValueError("listen_key не может быть пустым")
        return dict(self._put("/fapi/v1/listenKey", params={"listenKey": str(listen_key)}, signed=False) or {})

    def close_listen_key(self, listen_key: str) -> dict[str, Any]:
        if not listen_key:
            raise ValueError("listen_key не может быть пустым")
        return dict(self._delete("/fapi/v1/listenKey", params={"listenKey": str(listen_key)}, signed=False) or {})

    # ============================================================
    # Trading
    # ============================================================
    def new_order(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float | None = None,
        time_in_force: str | None = None,
        reduce_only: bool | None = None,
        client_order_id: str | None = None,
        **extra: Any,
    ) -> dict[str, Any]:
        if not symbol:
            raise ValueError("symbol не может быть пустым")
        if not side:
            raise ValueError("side не может быть пустым")
        if not order_type:
            raise ValueError("order_type не может быть пустым")

        params: dict[str, Any] = {
            "symbol": str(symbol).upper(),
            "side": str(side).upper(),
            "type": str(order_type).upper(),
            "quantity": float(quantity),
        }
        if price is not None:
            params["price"] = float(price)
        if time_in_force is not None:
            params["timeInForce"] = str(time_in_force)
        if reduce_only is not None:
            params["reduceOnly"] = "true" if bool(reduce_only) else "false"
        if client_order_id:
            params["newClientOrderId"] = str(client_order_id)

        params.update({k: v for k, v in (extra or {}).items() if v is not None})

        data = self._post("/fapi/v1/order", params=params, signed=True)
        return dict(data or {})

    def cancel_order(
        self,
        *,
        symbol: str,
        order_id: int | None = None,
        orig_client_order_id: str | None = None,
    ) -> dict[str, Any]:
        if not symbol:
            raise ValueError("symbol не может быть пустым")
        if order_id is None and not orig_client_order_id:
            raise ValueError("нужно указать order_id или orig_client_order_id")

        params: dict[str, Any] = {"symbol": str(symbol).upper()}
        if order_id is not None:
            params["orderId"] = int(order_id)
        if orig_client_order_id:
            params["origClientOrderId"] = str(orig_client_order_id)

        data = self._delete("/fapi/v1/order", params=params, signed=True)
        return dict(data or {})

    def fetch_open_orders(self, *, symbol: str | None = None) -> Any:
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = str(symbol).upper()
        return self._get("/fapi/v1/openOrders", params=params or None, signed=True)

    def fetch_order(
        self,
        *,
        symbol: str,
        order_id: int | None = None,
        orig_client_order_id: str | None = None,
    ) -> dict[str, Any]:
        if not symbol:
            raise ValueError("symbol не может быть пустым")
        if order_id is None and not orig_client_order_id:
            raise ValueError("нужно указать order_id или orig_client_order_id")

        params: dict[str, Any] = {"symbol": str(symbol).upper()}
        if order_id is not None:
            params["orderId"] = int(order_id)
        if orig_client_order_id:
            params["origClientOrderId"] = str(orig_client_order_id)

        data = self._get("/fapi/v1/order", params=params, signed=True)
        return dict(data or {})

    # ============================================================
    # Account / positions
    # ============================================================
    def position_risk(self, *, symbol: str | None = None) -> Any:
        sym = str(symbol).upper() if symbol else ""
        cache_key = f"positionRisk:{sym}"

        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = sym

        data = self._get("/fapi/v3/positionRisk", params=params or None, signed=True)
        self._cache_put(cache_key, self.position_risk_ttl_sec, data)
        return data

    def account(self) -> Any:
        cache_key = "account"

        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        data = self._get("/fapi/v2/account", params=None, signed=True)
        self._cache_put(cache_key, self.account_ttl_sec, data)
        return data

    # ============================================================
    # Public market data (collectors)
    # ============================================================
    def fetch_exchange_info(self, *, cache_ttl_sec: float = 300.0, force: bool = False) -> dict[str, Any]:
        key = "exchange_info"
        if not force:
            cached = self._cache_get(key)
            if cached is not None:
                try:
                    return dict(cached)
                except Exception:
                    pass

        data = self._get("/fapi/v1/exchangeInfo", params=None, signed=False) or {}
        try:
            self._cache_put(key, float(cache_ttl_sec), data)
        except Exception:
            pass
        return dict(data) if isinstance(data, dict) else {}

    def time(self) -> dict[str, Any]:
        """
        GET /fapi/v1/time
        """
        return dict(self._get("/fapi/v1/time", params=None, signed=False) or {})

    def klines(
        self,
        *,
        symbol: str,
        interval: str,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 500,
    ) -> Any:
        params: dict[str, Any] = {"symbol": str(symbol).upper(), "interval": str(interval), "limit": int(limit)}
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)
        return self._get("/fapi/v1/klines", params=params, signed=False)

    def open_interest_hist(
        self,
        *,
        symbol: str,
        period: str,
        limit: int = 500,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
    ) -> Any:
        """
        GET /futures/data/openInterestHist
        """
        params: dict[str, Any] = {"symbol": str(symbol).upper(), "period": str(period), "limit": int(limit)}
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)
        return self._get("/futures/data/openInterestHist", params=params, signed=False)

    def open_interest(self, *, symbol: str) -> dict[str, Any]:
        """
        GET /fapi/v1/openInterest
        """
        params: dict[str, Any] = {"symbol": str(symbol).upper()}
        return dict(self._get("/fapi/v1/openInterest", params=params, signed=False) or {})

    # alias для совместимости с коллекторами
    def get_open_interest_hist(
        self,
        *,
        symbol: str,
        period: str,
        limit: int = 500,
        endTime: int | None = None,
        startTime: int | None = None,
    ) -> Any:
        """
        Совместимый wrapper под Binance API naming:
          endTime/startTime вместо end_time_ms/start_time_ms.
        """
        return self.open_interest_hist(
            symbol=symbol,
            period=period,
            limit=limit,
            start_time_ms=startTime,
            end_time_ms=endTime,
        )


    def funding_rate(
        self,
        *,
        symbol: str,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int = 1000,
    ) -> Any:
        params: dict[str, Any] = {"symbol": str(symbol).upper(), "limit": int(limit)}
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)
        return self._get("/fapi/v1/fundingRate", params=params, signed=False)

    def premium_index(self, *, symbol: str | None = None) -> Any:
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = str(symbol).upper()
        return self._get("/fapi/v1/premiumIndex", params=params or None, signed=False)

    def ticker_24h(self, *, symbol: str | None = None) -> Any:
        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = str(symbol).upper()
        return self._get("/fapi/v1/ticker/24hr", params=params or None, signed=False)

    def get_agg_trades(
        self,
        *,
        symbol: str,
        from_id: Optional[int] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"symbol": str(symbol).upper(), "limit": int(limit)}
        if from_id is not None:
            params["fromId"] = int(from_id)
        if start_time is not None:
            params["startTime"] = int(start_time)
        if end_time is not None:
            params["endTime"] = int(end_time)
        data = self._get("/fapi/v1/aggTrades", params=params, signed=False)
        return list(data or [])
