# src/platform/exchanges/binance/rest.py
from __future__ import annotations

import hashlib
import hmac
import logging
import os
import random
import threading
import time
from typing import Any, Optional, Dict, Tuple
from urllib.parse import urlencode

import requests

BASE_URL = "https://fapi.binance.com"
log = logging.getLogger("src.platform.exchanges.binance.rest")


def _ts_ms() -> int:
    return int(time.time() * 1000)


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
    if not params:
        return ""
    keys = ("symbol", "symbols", "interval", "period", "limit", "startTime", "endTime", "listenKey")
    out: list[str] = []
    for k in keys:
        if k in params and params[k] is not None:
            out.append(f"{k}={params[k]}")
    s = " ".join(out)
    if len(s) > max_len:
        s = s[:max_len] + "…"
    return s


def _is_rate_limited(resp_json: Any, status_code: int) -> bool:
    if status_code == 429:
        return True
    if isinstance(resp_json, dict):
        try:
            code = int(resp_json.get("code") or 0)
        except Exception:
            code = 0
        # -1003 Too many requests
        if code == -1003:
            return True
    return False


def _is_retryable_http(status_code: int) -> bool:
    return status_code in (418, 429) or (500 <= status_code <= 599)


class _TokenBucket:
    """
    Простой token-bucket с ожиданием.
    tokens пополняются со скоростью refill_per_sec до capacity.
    """

    def __init__(self, *, capacity: float, refill_per_sec: float) -> None:
        self.capacity = float(max(1.0, capacity))
        self.refill_per_sec = float(max(0.001, refill_per_sec))
        self.tokens = float(self.capacity)
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def _refill(self, now: float) -> None:
        dt = max(0.0, now - self.last)
        if dt <= 0:
            return
        self.tokens = min(self.capacity, self.tokens + dt * self.refill_per_sec)
        self.last = now

    def acquire(self, amount: float, *, stop_event: threading.Event | None = None) -> None:
        amt = float(max(0.0, amount))
        if amt <= 0:
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

            # jitter чтобы много потоков не просыпались одновременно
            wait_sec = max(0.0, float(wait_sec)) + random.uniform(0.0, 0.05)

            # НЕ делаем долгий sleep одной порцией — чтобы stop_event работал
            time.sleep(min(wait_sec, 1.0))


class _GlobalRateLimiter:
    """
    Глобальный (на процесс) ограничитель:
      - bucket по RPS
      - bucket по RPM
    Вес запроса = weight (условный).
    """

    def __init__(self) -> None:
        max_rps = _safe_float_env("BINANCE_REST_MAX_RPS", 8.0)
        max_rpm = _safe_float_env("BINANCE_REST_MAX_RPM", 900.0)

        max_rps = max(1.0, max_rps)
        max_rpm = max(60.0, max_rpm)

        self.max_rps = float(max_rps)
        self.max_rpm = float(max_rpm)

        self.bucket_rps = _TokenBucket(capacity=max_rps, refill_per_sec=max_rps)
        self.bucket_rpm = _TokenBucket(capacity=max_rpm, refill_per_sec=max_rpm / 60.0)

        self.stop_event = threading.Event()

        # INFO может быть выключен — поэтому продублируем WARN один раз позже (см. get_limiter())
        log.info(
            "[REST RL] init max_rps=%.2f max_rpm=%.0f (env BINANCE_REST_MAX_RPS/BINANCE_REST_MAX_RPM)",
            max_rps, max_rpm
        )

    def acquire(self, weight: float) -> None:
        w = float(max(1.0, weight))
        # порядок не принципиален, но обычно RPM важнее (меньше лимит) — берём сначала RPM
        self.bucket_rpm.acquire(w, stop_event=self.stop_event)
        self.bucket_rps.acquire(w, stop_event=self.stop_event)


# ---------- Global singletons (per process) ----------
_GLOBAL_LIMITER: Optional[_GlobalRateLimiter] = None
_LIMITER_LOCK = threading.Lock()
_LIMITER_LOGGED = False


def _get_limiter() -> _GlobalRateLimiter:
    global _GLOBAL_LIMITER, _LIMITER_LOGGED
    with _LIMITER_LOCK:
        if _GLOBAL_LIMITER is None:
            _GLOBAL_LIMITER = _GlobalRateLimiter()
        if not _LIMITER_LOGGED:
            # даже если INFO выключен — покажем один раз WARN-ом реальные лимиты
            _LIMITER_LOGGED = True
            log.warning(
                "[REST RL] effective max_rps=%.2f max_rpm=%.0f (env RPS/RPM, per-process limiter!)",
                _GLOBAL_LIMITER.max_rps, _GLOBAL_LIMITER.max_rpm
            )
        return _GLOBAL_LIMITER


class _GlobalCooldown:
    """
    Глобальный cooldown на процесс после 429/-1003.
    Если словили rate-limit — "замораживаем" REST на N секунд,
    чтобы избежать ретрай-шторма.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._until_mono: float = 0.0
        self._last_reason: str | None = None

    def until(self) -> float:
        with self._lock:
            return float(self._until_mono)

    def arm(self, *, seconds: float, reason: str) -> None:
        s = float(max(0.0, seconds))
        if s <= 0:
            return
        now = time.monotonic()
        # jitter, чтобы разные потоки не стартовали одновременно после cooldown
        s = s + random.uniform(0.0, 0.4)
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
    Грубые веса, чтобы не долбить тяжёлые эндпоинты.
    Реальные веса Binance могут отличаться, но нам важен safety.
    """
    m = (method or "").upper()
    p = str(path or "")

    if p.startswith("/fapi/v1/listenKey"):
        return 1.0

    if p.startswith("/fapi/v1/order"):
        return 1.0

    # signed heavy
    if p.startswith("/fapi/v2/account"):
        return 10.0

    if p.startswith("/fapi/v3/positionRisk"):
        return 10.0

    if m in ("GET", "POST", "PUT", "DELETE"):
        return 1.0
    return 1.0


class BinanceFuturesREST:
    def __init__(self, api_key: str, secret_key: str):
        self.api_key = api_key
        self.secret_key = secret_key
        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": api_key})

        # retry настройки
        self.max_retries = _safe_int_env("BINANCE_REST_RETRY_MAX", 6)
        self.base_backoff = _safe_float_env("BINANCE_REST_RETRY_BASE_SEC", 0.35)
        self.max_backoff = _safe_float_env("BINANCE_REST_RETRY_MAX_SEC", 30.0)

        # таймауты
        self.timeout_sec = _safe_float_env("BINANCE_REST_TIMEOUT_SEC", 10.0)

        # debug
        self.debug_headers = _truthy_env("BINANCE_REST_DEBUG_HEADERS", "0")

        # TTL cache (чтобы не спамить heavy endpoints из tight-loop)
        self._cache_lock = threading.Lock()
        self._cache: Dict[str, Tuple[float, Any]] = {}  # key -> (until_monotonic, value)

        self.position_risk_ttl_sec = _safe_float_env("BINANCE_REST_POSITION_RISK_TTL_SEC", 2.0)
        self.account_ttl_sec = _safe_float_env("BINANCE_REST_ACCOUNT_TTL_SEC", 3.0)

    def _cache_get(self, key: str) -> Any | None:
        now = time.monotonic()
        with self._cache_lock:
            item = self._cache.get(key)
            if not item:
                return None
            until, val = item
            if now <= float(until):
                return val
            # expired
            self._cache.pop(key, None)
            return None

    def _cache_put(self, key: str, ttl_sec: float, value: Any) -> None:
        ttl = float(max(0.0, ttl_sec))
        if ttl <= 0:
            return
        until = time.monotonic() + ttl
        with self._cache_lock:
            self._cache[key] = (until, value)

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        signed: bool = False,
    ):
        method = str(method).upper()
        url = f"{BASE_URL}{path}"
        params = params or {}

        weight = _endpoint_weight(method, path)

        attempt = 0
        backoff = float(self.base_backoff)

        while True:
            attempt += 1

            # если недавно был 429 — подождём (глобально на процесс)
            _GLOBAL_COOLDOWN.wait_if_needed()

            # глобальный limiter (на процесс)
            _get_limiter().acquire(weight)

            # prepare params
            if signed:
                params_to_send = dict(params)
                params_to_send.setdefault("recvWindow", 5000)
                params_to_send["timestamp"] = _ts_ms()
                params_to_send["signature"] = self._sign(params_to_send)
                send_params: Any = list(params_to_send.items())
            else:
                send_params = params

            # request
            try:
                if method == "GET":
                    response = self.session.get(url, params=send_params, timeout=self.timeout_sec)
                elif method == "POST":
                    response = self.session.post(url, params=send_params, timeout=self.timeout_sec)
                elif method == "PUT":
                    response = self.session.put(url, params=send_params, timeout=self.timeout_sec)
                elif method == "DELETE":
                    response = self.session.delete(url, params=send_params, timeout=self.timeout_sec)
                else:
                    raise ValueError(f"Неизвестный метод {method}")
            except Exception as e:
                if attempt <= self.max_retries:
                    sleep_s = min(backoff, self.max_backoff) + random.uniform(0.0, 0.3)
                    log.warning(
                        "[REST NET] %s %s retry=%d/%d sleep=%.2fs ctx=%s err=%s",
                        method, path, attempt, self.max_retries, sleep_s, _fmt_params(params), e
                    )
                    time.sleep(sleep_s)
                    backoff = min(backoff * 2.0, self.max_backoff)
                    continue
                raise RuntimeError(f"Binance REST request failed: {method} {path} err={e}") from e

            status = int(response.status_code)

            # parse JSON
            try:
                data = response.json()
            except Exception:
                txt = (response.text or "").strip()
                if attempt <= self.max_retries and _is_retryable_http(status):
                    sleep_s = min(backoff, self.max_backoff) + random.uniform(0.0, 0.3)
                    log.warning(
                        "[REST NONJSON] %s %s status=%s retry=%d/%d sleep=%.2fs body=%s",
                        method, path, status, attempt, self.max_retries, sleep_s,
                        (txt[:300] + ("…" if len(txt) > 300 else ""))
                    )
                    time.sleep(sleep_s)
                    backoff = min(backoff * 2.0, self.max_backoff)
                    continue
                raise RuntimeError(
                    f"Binance REST non-JSON response: {method} {path} status={status} body={txt[:500]}"
                )

            if self.debug_headers:
                used = response.headers.get("X-MBX-USED-WEIGHT-1M") or response.headers.get("x-mbx-used-weight-1m")
                if used:
                    log.info("[REST HDR] used_weight_1m=%s %s %s", used, method, path)

            # rate-limited
            if _is_rate_limited(data, status):
                # ставим глобальный cooldown, чтобы не ретраить штормом
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        ra = float(retry_after)
                    except Exception:
                        ra = 0.0
                else:
                    ra = 0.0

                # Минимальный cooldown для 429: 20–60 сек (важно!)
                # Берём max(retry_after, backoff, 20)
                cooldown = max(ra, min(backoff, self.max_backoff), 20.0)
                cooldown = min(cooldown, 120.0)

                _GLOBAL_COOLDOWN.arm(seconds=cooldown, reason=f"{method} {path} 429/-1003")

                if attempt <= self.max_retries:
                    sleep_s = max(1.0, cooldown * 0.5) + random.uniform(0.0, 0.5)
                    log.warning(
                        "[REST 429] %s %s retry=%d/%d sleep=%.2fs cooldown=%.2fs ctx=%s resp=%r",
                        method, path, attempt, self.max_retries, sleep_s, cooldown, _fmt_params(params), data
                    )
                    time.sleep(sleep_s)
                    backoff = min(max(backoff * 2.0, 1.0), self.max_backoff)
                    continue

                raise RuntimeError(f"Binance REST HTTP {status}: {method} {path} resp={data!r}")

            # http errors
            if status >= 400:
                if attempt <= self.max_retries and _is_retryable_http(status):
                    sleep_s = min(backoff, self.max_backoff) + random.uniform(0.0, 0.3)
                    log.warning(
                        "[REST HTTP] %s %s status=%s retry=%d/%d sleep=%.2fs ctx=%s resp=%r",
                        method, path, status, attempt, self.max_retries, sleep_s, _fmt_params(params), data
                    )
                    time.sleep(sleep_s)
                    backoff = min(backoff * 2.0, self.max_backoff)
                    continue
                raise RuntimeError(f"Binance REST HTTP {status}: {method} {path} resp={data!r}")

            # binance api error in json
            if isinstance(data, dict) and "code" in data:
                try:
                    code = int(data.get("code") or 0)
                except Exception:
                    code = 0

                if code < 0:
                    # -1021 timestamp drift: ретраим, но без спама
                    if attempt <= self.max_retries and code in (-1021,):
                        sleep_s = min(backoff, self.max_backoff) + random.uniform(0.0, 0.3)
                        log.warning(
                            "[REST API] %s %s code=%s retry=%d/%d sleep=%.2fs ctx=%s resp=%r",
                            method, path, code, attempt, self.max_retries, sleep_s, _fmt_params(params), data
                        )
                        time.sleep(sleep_s)
                        backoff = min(backoff * 2.0, self.max_backoff)
                        continue

                    raise RuntimeError(f"Binance REST API error: {method} {path} resp={data!r}")

            return data

    def _sign(self, params: dict[str, Any]) -> str:
        # ВАЖНО: без сортировки, чтобы совпало с тем, что реально уходит в запрос
        query_string = urlencode(params, doseq=True)
        return hmac.new(self.secret_key.encode(), query_string.encode(), hashlib.sha256).hexdigest()

    def _get(self, path: str, params: dict[str, Any] | None = None, signed: bool = False):
        return self._request("GET", path, params=params, signed=signed)

    def _post(self, path: str, params: dict[str, Any] | None = None, signed: bool = False):
        return self._request("POST", path, params=params, signed=signed)

    def _put(self, path: str, params: dict[str, Any] | None = None, signed: bool = False):
        return self._request("PUT", path, params=params, signed=signed)

    def _delete(self, path: str, params: dict[str, Any] | None = None, signed: bool = False):
        return self._request("DELETE", path, params=params, signed=signed)

    # ----------------------------
    # USER DATA STREAM (listenKey)
    # ----------------------------
    def create_listen_key(self) -> dict[str, Any]:
        return dict(self._request("POST", "/fapi/v1/listenKey", params=None, signed=False) or {})

    def keepalive_listen_key(self, listen_key: str) -> dict[str, Any]:
        if not listen_key:
            raise ValueError("listen_key не может быть пустым")
        return dict(self._request("PUT", "/fapi/v1/listenKey", params={"listenKey": str(listen_key)}, signed=False) or {})

    def close_listen_key(self, listen_key: str) -> dict[str, Any]:
        if not listen_key:
            raise ValueError("listen_key не может быть пустым")
        return dict(self._request("DELETE", "/fapi/v1/listenKey", params={"listenKey": str(listen_key)}, signed=False) or {})

    # ----------------------------
    # ТОРГОВЛЯ
    # ----------------------------
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
            "quantity": quantity,
        }
        if price is not None:
            params["price"] = price
        if time_in_force is not None:
            params["timeInForce"] = str(time_in_force)
        if reduce_only is not None:
            params["reduceOnly"] = "true" if bool(reduce_only) else "false"
        if client_order_id:
            params["newClientOrderId"] = str(client_order_id)

        params.update({k: v for k, v in (extra or {}).items() if v is not None})

        data = self._post("/fapi/v1/order", params=params, signed=True)
        return dict(data or {})

    # ----------------------------
    # АККАУНТ / ПОЗИЦИИ
    # ----------------------------
    def position_risk(self, *, symbol: str | None = None) -> Any:
        """
        GET /fapi/v3/positionRisk (ПОДПИСАНО)
        TTL cache по умолчанию 2 секунды (env BINANCE_REST_POSITION_RISK_TTL_SEC)
        """
        sym = str(symbol).upper() if symbol else ""
        cache_key = f"positionRisk:{sym}"

        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        params: dict[str, Any] = {}
        if symbol:
            params["symbol"] = sym

        data = self._get("/fapi/v3/positionRisk", params=params, signed=True)
        self._cache_put(cache_key, self.position_risk_ttl_sec, data)
        return data

    def account(self) -> Any:
        """
        GET /fapi/v2/account (ПОДПИСАНО)
        TTL cache по умолчанию 3 секунды (env BINANCE_REST_ACCOUNT_TTL_SEC)
        """
        cache_key = "account"

        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        data = self._get("/fapi/v2/account", signed=True)
        self._cache_put(cache_key, self.account_ttl_sec, data)
        return data
