from __future__ import annotations

import json
import logging
import random
import time
import hmac
import hashlib
import urllib.parse
import re
from concurrent.futures import Future
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional
import requests
from .params import *
from .params import _utc_now
from .params import _sanitize_coid_prefix

log = logging.getLogger("traders.trade_liquidation")


def _activation_close_enough(a: float, b: float, tick: float = 0.0) -> bool:
    try:
        a = float(a or 0.0)
        b = float(b or 0.0)
        t = float(tick or 0.0)
        eps_abs = max(t * 8.0, max(abs(a), abs(b)) * 0.005, 1e-9)
        return abs(a - b) <= eps_abs
    except Exception:
        return False


class BinanceUMFuturesRest:
    """Minimal Binance USD-M Futures REST client with HMAC signature.

    Purpose: keep exchange interaction tiny and explicit.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str,
        recv_window_ms: int = 5000,
        timeout_sec: int = 10,
        debug: bool = False,
        *,
        connect_timeout_sec: float | None = None,
        read_timeout_sec: float | None = None,
        max_retries: int = 2,
        retry_backoff_sec: float = 1.0,
        symbol_filters_resolver: Any = None,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self.recv_window_ms = int(recv_window_ms)
        self.timeout_sec = int(timeout_sec)
        self.debug = bool(debug)

        self._symbol_filters_resolver = symbol_filters_resolver

        # Requests timeout can be a tuple: (connect_timeout, read_timeout).
        # If not provided explicitly, derive from timeout_sec.
        ct = float(connect_timeout_sec) if connect_timeout_sec is not None else min(3.0, float(self.timeout_sec))
        rt = float(read_timeout_sec) if read_timeout_sec is not None else float(self.timeout_sec)
        self._timeout = (ct, rt)

        # Retry policy for transient network errors (timeouts, TLS handshake hiccups, etc.).
        self.max_retries = max(0, int(max_retries))
        self.retry_backoff_sec = max(0.0, float(retry_backoff_sec))

        # Reuse TCP/TLS connections (faster + fewer handshakes).
        self._session = requests.Session()

        # Binance server time drift compensation.
        self._time_offset_ms = 0.0
        self._last_time_sync_monotonic = 0.0

    def _sync_server_time(self, *, force: bool = False) -> float:
        """Best-effort sync with Binance server time.

        Returns current offset in milliseconds (serverTime - localTime).
        """
        try:
            now_m = time.monotonic()
            if (not force) and self._last_time_sync_monotonic and (now_m - self._last_time_sync_monotonic) < 30.0:
                return float(self._time_offset_ms or 0.0)
            url = self.base_url + "/fapi/v1/time"
            r = self._session.request("GET", url, timeout=self._timeout)
            r.raise_for_status()
            data = r.json() if r.text else {}
            server_ms = float((data or {}).get("serverTime") or 0.0)
            local_ms = float(time.time() * 1000.0)
            if server_ms > 0.0:
                self._time_offset_ms = server_ms - local_ms
                self._last_time_sync_monotonic = now_m
        except Exception:
            if self.debug:
                log.exception("[TL][binance_rest] server time sync failed")
        return float(self._time_offset_ms or 0.0)

    def _sign(self, params: Dict[str, Any]) -> Dict[str, Any]:
        p = {k: v for k, v in params.items() if v is not None}
        p["timestamp"] = int((_utc_now().timestamp() * 1000) + float(self._time_offset_ms or 0.0))
        if self.recv_window_ms:
            p["recvWindow"] = int(self.recv_window_ms)
        query = urllib.parse.urlencode(p, doseq=True)
        sig = hmac.new(self.api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
        p["signature"] = sig
        return p

    def _apply_symbol_precision(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Apply symbol-specific precision to REST parameters (best-effort).

        Why this exists:
          - Some parts of the strategy call REST endpoints (incl. positionRisk).
          - Older versions of the client had this helper; when it's missing,
            the whole reconciliation breaks (ledger shows stale OPEN positions).

        For GET endpoints (like positionRisk) rounding is not required; so if we
        can't resolve filters we simply return params unchanged.
        """

        try:
            sym = str(params.get("symbol") or "").upper()
            if not sym or not self._symbol_filters_resolver:
                return params

            flt = self._symbol_filters_resolver(sym)
            if not isinstance(flt, dict) or not flt:
                return params

            price_tick = flt.get("price_tick") or flt.get("tickSize")
            qty_step = flt.get("qty_step") or flt.get("stepSize")

            def _floor_to_step(x: Any, step: Any) -> Any:
                if x is None or step in (None, 0, "0", "0.0"):
                    return x
                try:
                    dx = Decimal(str(x))
                    ds = Decimal(str(step))
                    if ds <= 0:
                        return x
                    return str((dx // ds) * ds)
                except Exception:
                    return x

            # Standard Binance param names used by this strategy
            for k in ("price", "stopPrice", "triggerPrice", "activatePrice"):
                if k in params:
                    params[k] = _floor_to_step(params[k], price_tick)
            for k in ("quantity", "origQty"):
                if k in params:
                    params[k] = _floor_to_step(params[k], qty_step)

            return params
        except Exception:
            return params

    def _request(
        self,
        method: str,
        path: str,
        signed: bool = True,
        *,
        timeout: tuple[float, float] | None = None,
        **params: Any,
    ) -> Any:
        url = self.base_url + path
        headers = {"X-MBX-APIKEY": self.api_key}
        # IMPORTANT: apply symbol precision BEFORE signing; otherwise rounding
        # would mutate params after signature and Binance returns -1022.
        raw = {k: v for k, v in params.items() if v is not None}
        raw = self._apply_symbol_precision(dict(raw))

        # Use per-call timeout override if provided; otherwise use client default.
        _timeout = timeout if timeout is not None else self._timeout

        last_exc: Exception | None = None
        attempts = 1 + max(0, int(self.max_retries))
        for i in range(attempts):
            p = self._sign(raw) if signed else raw
            if self.debug:
                try:
                    safe_p = dict(p)
                    if "signature" in safe_p:
                        safe_p["signature"] = "<redacted>"
                    log.info("[TL][binance_rest] %s %s signed=%s params=%s", method, path, "Y" if signed else "N", json.dumps(safe_p, ensure_ascii=False))
                except Exception:
                    pass
            try:
                r = self._session.request(method, url, params=p, headers=headers, timeout=_timeout)
                if self.debug:
                    log.info("[TL][binance_rest] HTTP %s for %s", r.status_code, path)

                if r.status_code >= 400:
                    # include response text (short) for debugging
                    txt = (r.text or "").strip()
                    if len(txt) > 600:
                        txt = txt[:600] + "..."
                    if signed and i < attempts - 1 and ("outside of the recvWindow" in txt or '"code":-1021' in txt):
                        self._sync_server_time(force=True)
                        delay = max(0.15, self.retry_backoff_sec * (2 ** i) * 0.25)
                        if self.debug:
                            log.warning("[TL][binance_rest] timestamp drift on %s %s. resync time and retry in %.2fs (%d/%d)", method, path, delay, i + 1, attempts)
                        time.sleep(delay)
                        continue
                    raise RuntimeError(f"binance http {r.status_code} {method} {path}: {txt}")

                return r.json() if r.text else None

            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout, requests.exceptions.Timeout,
                    requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
                last_exc = e
                # retry with exponential backoff + small jitter
                if i < attempts - 1:
                    delay = self.retry_backoff_sec * (2 ** i) + (random.random() * 0.2)
                    if self.debug:
                        log.warning("[TL][binance_rest] transient error on %s %s (%s). retry in %.2fs (%d/%d)",
                                    method, path, type(e).__name__, delay, i + 1, attempts)
                    time.sleep(delay)
                    continue
                break
            except Exception as e:
                # non-transient
                raise

        raise RuntimeError(f"binance request failed: {method} {path}: {last_exc}")


    # ---- wrappers
    def change_leverage(self, symbol: str, leverage: int) -> Any:
        return self._request("POST", "/fapi/v1/leverage", symbol=symbol, leverage=int(leverage))

    def change_margin_type(self, symbol: str, marginType: str) -> Any:
        return self._request("POST", "/fapi/v1/marginType", symbol=symbol, marginType=str(marginType))

    def new_order(self, **kwargs: Any) -> Any:
        """Place a new order.

        NOTE: Binance USD-M Futures moved conditional order types to Algo endpoints.
        If we send these types to `/fapi/v1/order`, Binance may return:
          -4120: "Order type not supported for this endpoint. Please use the Algo Order API endpoints instead."

        We auto-route conditional types to `/fapi/v1/algoOrder` with the required
        parameter mapping.
        """

        order_type = str(kwargs.get("type") or "").upper()
        conditional_types = {
            "STOP",
            "STOP_MARKET",
            "TAKE_PROFIT",
            "TAKE_PROFIT_MARKET",
            "TRAILING_STOP_MARKET",
        }
        if order_type in conditional_types:
            return self.new_algo_order(**kwargs)

        return self._request("POST", "/fapi/v1/order", **kwargs)

    def new_algo_order(self, **kwargs: Any) -> Any:
        """Place a new conditional (algo) order via POST /fapi/v1/algoOrder.

        Parameter mapping:
          newClientOrderId -> clientAlgoId
          stopPrice        -> triggerPrice
          activationPrice  -> activatePrice

        See Binance docs: New Algo Order (USD-M Futures).

        Idempotency note:
          Binance may return -4116 (duplicated clientAlgoId) while the algo order is
          already open on the exchange. In that case we treat the request as success
          and return the existing open algo order snapshot instead of raising.
        """

        p = dict(kwargs)

        # Required by endpoint
        p["algoType"] = "CONDITIONAL"

        # Rename fields to match algo endpoint
        if "newClientOrderId" in p and "clientAlgoId" not in p:
            p["clientAlgoId"] = p.pop("newClientOrderId")
        if "stopPrice" in p and "triggerPrice" not in p:
            p["triggerPrice"] = p.pop("stopPrice")
        if "activationPrice" in p and "activatePrice" not in p:
            p["activatePrice"] = p.pop("activationPrice")

        # Algo endpoint expects closePosition as string "true"/"false".
        if "closePosition" in p and isinstance(p.get("closePosition"), bool):
            p["closePosition"] = "true" if bool(p["closePosition"]) else "false"

        try:
            return self._request("POST", "/fapi/v1/algoOrder", **p)
        except Exception as e:
            msg = str(e)
            if "-4116" not in msg and "ClientOrderId is duplicated" not in msg:
                raise

            symbol = str(p.get("symbol") or "").upper().strip()
            cid = str(p.get("clientAlgoId") or "").strip()
            try:
                rows = self.open_algo_orders(symbol=symbol) if symbol else self.open_algo_orders()
            except Exception:
                rows = []
            for row in rows if isinstance(rows, list) else []:
                try:
                    if str(row.get("symbol") or "").upper().strip() != symbol:
                        continue
                    if str(row.get("clientAlgoId") or "").strip() != cid:
                        continue
                    if isinstance(row, dict):
                        row.setdefault("_duplicate", True)
                    return row
                except Exception:
                    continue

            # Best-effort synthetic response; caller can treat it as already present.
            return {
                "symbol": symbol,
                "clientAlgoId": cid,
                "algoStatus": "NEW",
                "status": "NEW",
                "_duplicate": True,
            }

    def open_algo_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        res = self._request("GET", "/fapi/v1/openAlgoOrders", signed=True, **params)
        return res if isinstance(res, list) else []

    def cancel_algo_order(
        self,
        symbol: str,
        *,
        algoId: Optional[str] = None,
        clientAlgoId: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"symbol": symbol}
        if algoId:
            params["algoId"] = str(algoId)
        if clientAlgoId:
            params["clientAlgoId"] = str(clientAlgoId)
        return self._request("DELETE", "/fapi/v1/algoOrder", signed=True, **params)

    def open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        res = self._request("GET", "/fapi/v1/openOrders", signed=True, **params)
        return res if isinstance(res, list) else []

    def all_orders(
        self,
        *,
        symbol: str,
        startTime: Optional[int] = None,
        endTime: Optional[int] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """Futures allOrders (order history). Helpful for reconcile after WS gaps."""
        params: Dict[str, Any] = {"symbol": symbol, "limit": int(limit)}
        if startTime is not None:
            params["startTime"] = int(startTime)
        if endTime is not None:
            params["endTime"] = int(endTime)
        res = self._request("GET", "/fapi/v1/allOrders", signed=True, **params)
        return res if isinstance(res, list) else []

    def cancel_order(
        self,
        symbol: str,
        *,
        origClientOrderId: Optional[str] = None,
        orderId: Optional[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"symbol": symbol}
        if origClientOrderId:
            params["origClientOrderId"] = origClientOrderId
        if orderId:
            params["orderId"] = str(orderId)
        return self._request("DELETE", "/fapi/v1/order", signed=True, **params)

    def cancel_all_open_orders(self, symbol: str) -> Dict[str, Any]:
        return self._request("DELETE", "/fapi/v1/allOpenOrders", signed=True, symbol=symbol)

    def cancel_all_open_algo_orders(self, symbol: str) -> Dict[str, Any]:
        """Cancel all open algo (conditional) orders for a symbol.

        Prefer the bulk endpoint if available. If the endpoint is not supported
        by the account/symbol/API version, we fall back to cancelling each algo
        order one-by-one.
        """
        try:
            res = self._request("DELETE", "/fapi/v1/allOpenAlgoOrders", signed=True, symbol=symbol)
            return res if isinstance(res, dict) else {"result": res}
        except Exception:
            # fallback: cancel individually
            canceled = 0
            failed = 0
            for o in self.open_algo_orders(symbol=symbol):
                algo_id = o.get("algoId")
                client_algo_id = o.get("clientAlgoId")
                try:
                    self.cancel_algo_order(symbol=symbol, algoId=algo_id, clientAlgoId=client_algo_id)
                    canceled += 1
                except Exception:
                    failed += 1
            return {"canceled": canceled, "failed": failed, "fallback": True}

    def balance(self) -> Any:
        return self._request("GET", "/fapi/v2/balance", signed=True)

    def get_position_mode(self) -> bool:
        """Return True when Binance USD-M account is in Hedge Mode (dual-side)."""
        res = self._request("GET", "/fapi/v1/positionSide/dual", signed=True)
        if isinstance(res, dict):
            val = res.get("dualSidePosition")
            if isinstance(val, bool):
                return bool(val)
            return str(val or "").strip().lower() == "true"
        return False

    def change_position_mode(self, dual_side_position: bool) -> Dict[str, Any]:
        """Switch Binance USD-M position mode.

        dual_side_position=True  -> Hedge Mode
        dual_side_position=False -> One-way Mode
        """
        return self._request(
            "POST",
            "/fapi/v1/positionSide/dual",
            signed=True,
            dualSidePosition="true" if bool(dual_side_position) else "false",
        )

    def ensure_position_mode(self, *, hedge_enabled: bool, fail_if_cannot_switch: bool = True) -> bool:
        """Ensure Binance account position mode matches strategy hedge setting.

        Returns current Hedge Mode state after verification.
        Raises RuntimeError on mismatch when fail_if_cannot_switch=True.
        """
        want_hedge = bool(hedge_enabled)
        try:
            current = bool(self.get_position_mode())
        except Exception as e:
            raise RuntimeError(f"failed to fetch Binance position mode: {e}") from e

        if current == want_hedge:
            return current

        try:
            self.change_position_mode(want_hedge)
        except Exception as e:
            msg = str(e)
            if fail_if_cannot_switch:
                raise RuntimeError(
                    "failed to switch Binance position mode to "
                    f"{'HEDGE' if want_hedge else 'ONE_WAY'}: {msg}"
                ) from e
            return current

        try:
            verified = bool(self.get_position_mode())
        except Exception as e:
            raise RuntimeError(f"position mode switch requested but verification failed: {e}") from e

        if verified != want_hedge:
            raise RuntimeError(
                "Binance position mode mismatch after switch attempt: "
                f"wanted={'HEDGE' if want_hedge else 'ONE_WAY'} got={'HEDGE' if verified else 'ONE_WAY'}"
            )
        return verified

    def position_risk(self) -> List[Dict[str, Any]]:
        """GET /fapi/v2/positionRisk (signed)."""
        res = self._request("GET", "/fapi/v2/positionRisk", signed=True)
        return res if isinstance(res, list) else []


    def user_trades(
        self,
        symbol: str,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """GET /fapi/v1/userTrades (signed).
        Used for reconciliation: to infer close price and realized PnL when a ledger position is already flat on exchange.
        """
        params: Dict[str, Any] = {"symbol": symbol.upper(), "limit": int(limit)}
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)
        res = self._request("GET", "/fapi/v1/userTrades", signed=True, params=params)
        return res if isinstance(res, list) else []

class TradeLiquidationOrderBuilderMixin:
    def _rest_submit(self, fn: Any, *args: Any, **kwargs: Any) -> Optional[Future]:
        """Submit a REST call into thread-pool if enabled."""
        pool = getattr(self, "_rest_pool", None)
        if pool is None:
            return None
        try:
            return pool.submit(fn, *args, **kwargs)
        except Exception:
            return None

    def _refresh_open_algo_snapshot_for_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        """Refresh openAlgoOrders snapshot for one symbol and merge it into the cached global snapshot."""
        sym = (symbol or "").upper().strip()
        if not sym or getattr(self, "_binance", None) is None:
            return []
        try:
            fresh = self._binance.open_algo_orders(symbol=sym)
        except Exception:
            return []
        if not isinstance(fresh, list):
            fresh = []
        try:
            with self._rest_snapshot_lock:
                prev = self._rest_snapshot.get("open_algo_orders_all")
                merged = []
                if isinstance(prev, list):
                    for row in prev:
                        if not isinstance(row, dict):
                            continue
                        if str(row.get("symbol") or "").upper().strip() == sym:
                            continue
                        merged.append(row)
                merged.extend(fresh)
                self._rest_snapshot["open_algo_orders_all"] = merged
        except Exception:
            pass

        return fresh

    def _trl_cache_key(self, symbol: str, side: str, client_id: str) -> str:
        return f"{str(symbol or '').upper().strip()}|{str(side or '').upper().strip()}|{str(client_id or '').strip()}"

    def _get_cached_trl_activation(self, symbol: str, side: str, client_id: str) -> Optional[float]:
        try:
            cache = getattr(self, "_last_trl_activation", None)
            if not isinstance(cache, dict):
                cache = {}
                setattr(self, "_last_trl_activation", cache)
            v = cache.get(self._trl_cache_key(symbol, side, client_id))
            return float(v) if v is not None else None
        except Exception:
            return None

    def _set_cached_trl_activation(self, symbol: str, side: str, client_id: str, activation: float) -> None:
        try:
            cache = getattr(self, "_last_trl_activation", None)
            if not isinstance(cache, dict):
                cache = {}
                setattr(self, "_last_trl_activation", cache)
            cache[self._trl_cache_key(symbol, side, client_id)] = float(activation)
        except Exception:
            pass

    def _clear_cached_trl_activation(self, symbol: str, side: str, client_id: str) -> None:
        try:
            cache = getattr(self, "_last_trl_activation", None)
            if isinstance(cache, dict):
                cache.pop(self._trl_cache_key(symbol, side, client_id), None)
        except Exception:
            pass

    def _cancel_algo_by_client_id_safe(
        self,
        symbol: str,
        client_algo_id: str,
        algo_id: Optional[str] = None,
        *,
        verify: bool = True,
    ) -> bool:
        """Best-effort cancel of a Binance algo/conditional order.

        Returns True when the order is confirmed absent from openAlgoOrders for the symbol
        (or was already absent), otherwise False.
        """
        if not getattr(self, "_is_live", False) or getattr(self, "_binance", None) is None:
            return False
        sym = (symbol or "").upper().strip()
        cid = (client_algo_id or "").strip()
        aid = str(algo_id or "").strip()
        if not sym or (not cid and not aid):
            return False

        if cid:
            try:
                self._clear_local_algo_open(cid)
            except Exception:
                pass

        found_in_snapshot = False
        try:
            snap = self._rest_snapshot_get("open_algo_orders_all")
            if isinstance(snap, list) and snap:
                for o in snap:
                    if not isinstance(o, dict):
                        continue
                    if str(o.get("symbol") or "").upper().strip() != sym:
                        continue
                    if cid and str(o.get("clientAlgoId") or "").strip() == cid:
                        found_in_snapshot = True
                        if not aid:
                            aid = str(o.get("algoId") or "").strip()
                        break
                    if aid and str(o.get("algoId") or "").strip() == aid:
                        found_in_snapshot = True
                        break
        except Exception:
            pass

        if not found_in_snapshot and not verify:
            return True

        if found_in_snapshot or aid or cid:
            cancel_attempts = []
            if aid and cid:
                cancel_attempts.append({"algoId": aid, "clientAlgoId": cid})
            if aid:
                cancel_attempts.append({"algoId": aid})
            if cid:
                cancel_attempts.append({"clientAlgoId": cid})
            if cid:
                cancel_attempts.append({"origClientOrderId": cid})
            for kwargs in cancel_attempts:
                try:
                    self._binance.cancel_algo_order(symbol=sym, **kwargs)  # type: ignore[arg-type]
                    break
                except Exception:
                    continue

        if not verify:
            return True

        fresh = self._refresh_open_algo_snapshot_for_symbol(sym)
        for row in fresh or []:
            if not isinstance(row, dict):
                continue
            if cid and str(row.get("clientAlgoId") or "").strip() == cid:
                return False
            if aid and str(row.get("algoId") or "").strip() == aid:
                return False
        return True

    def _find_known_algo_order_by_client_id(self, symbol: str, client_algo_id: str) -> Optional[Dict[str, Any]]:
        sym = (symbol or "").upper().strip()
        cid = (client_algo_id or "").strip()
        if not sym or not cid:
            return None

        try:
            cache = self._get_local_algo_open_cache()
            row = cache.get(cid)
            if isinstance(row, dict) and str(row.get("symbol") or "").upper().strip() == sym:
                return dict(row)
        except Exception:
            pass

        row = self._find_open_algo_order_by_client_id(sym, cid)
        if isinstance(row, dict):
            return row

        try:
            db_row = self.store.get_algo_order(exchange_id=self.exchange_id, account_id=self.account_id, client_algo_id=cid)
        except Exception:
            db_row = None
        if db_row and str(db_row.get("status") or "").upper() == "OPEN":
            return {
                "symbol": sym,
                "clientAlgoId": cid,
                "positionSide": str(db_row.get("position_side") or "").upper(),
                "orderType": str(db_row.get("type") or "").upper(),
                "type": str(db_row.get("type") or "").upper(),
                "side": str(db_row.get("side") or "").upper(),
                "quantity": db_row.get("quantity"),
                "origQty": db_row.get("quantity"),
                "triggerPrice": db_row.get("trigger_price"),
            }
        return None

    def _find_semantic_open_algo(
        self,
        symbol: str,
        *,
        client_suffix: Optional[str] = None,
        position_side: Optional[str] = None,
        order_type: Optional[str] = None,
        side: Optional[str] = None,
        close_position: Optional[bool] = None,
    ) -> Optional[Dict[str, Any]]:
        sym = str(symbol or "").upper().strip()
        if not sym:
            return None
        want_ps = str(position_side or "").upper().strip()
        want_type = str(order_type or "").upper().strip()
        want_side = str(side or "").upper().strip()
        suffix = str(client_suffix or "").strip().upper()

        rows: List[Dict[str, Any]] = []
        try:
            rows.extend(list(self._get_local_algo_open_cache().values()))
        except Exception:
            pass
        try:
            snap = self._rest_snapshot_get("open_algo_orders_all")
            if isinstance(snap, list):
                rows.extend([r for r in snap if isinstance(r, dict)])
        except Exception:
            pass

        def _to_bool_local(x: Any) -> bool:
            if isinstance(x, bool):
                return bool(x)
            if x is None:
                return False
            return str(x).strip().lower() in {"1", "true", "yes", "y", "on"}

        for row in rows:
            if str(row.get("symbol") or "").upper().strip() != sym:
                continue
            cid = str(row.get("clientAlgoId") or row.get("clientOrderId") or "").strip().upper()
            if suffix and not cid.endswith(suffix):
                continue
            row_ps = str(row.get("positionSide") or row.get("position_side") or "").upper().strip()
            if want_ps and row_ps not in {want_ps, "BOTH", ""}:
                continue
            row_type = str(row.get("orderType") or row.get("type") or "").upper().strip()
            if want_type and row_type and row_type != want_type:
                continue
            row_side = str(row.get("side") or "").upper().strip()
            if want_side and row_side and row_side != want_side:
                continue
            if close_position is not None and _to_bool_local(row.get("closePosition")) != bool(close_position):
                continue
            return row
        return None

    def _find_open_algo_order_by_client_id(self, symbol: str, client_algo_id: str) -> Optional[Dict[str, Any]]:
        """Return an open algo order dict from the current openAlgoOrders snapshot by (symbol, clientAlgoId).

        Used for recovery after restart: DB shadow may be missing/stale while the exchange still has the order open.
        """
        sym = (symbol or "").upper().strip()
        cid = (client_algo_id or "").strip()
        if not sym or not cid:
            return None
        try:
            snap = self._rest_snapshot_get("open_algo_orders_all")
        except Exception:
            snap = None
        if not isinstance(snap, list) or not snap:
            return None
        for o in snap:
            if not isinstance(o, dict):
                continue
            if str(o.get("symbol") or "").upper().strip() != sym:
                continue
            if str(o.get("clientAlgoId") or "").strip() != cid:
                continue
            return o
        return None

    def _safe_price_above_mark(self, price: float, mark: float, tick: float, buffer_pct: float = 0.2) -> float:
        """Force price to be safely ABOVE current mark by buffer_pct (percent)."""
        p = float(price or 0.0)
        m = float(mark or 0.0)
        t = float(tick or 0.0)
        # Keep at least 0.20% away from mark; cheap symbols often need a bit more than 1 tick.
        buf = max(float(buffer_pct or 0.0) / 100.0, 0.0020)
        if m > 0:
            floor = m * (1.0 + buf)
            p = max(p, floor)
            if t > 0:
                p = max(p, floor + t)
                p = _round_price_to_tick(p, t, mode="up")
        elif t > 0:
            p = _round_price_to_tick(p, t, mode="up")
        return float(p)

    def _safe_price_below_mark(self, price: float, mark: float, tick: float, buffer_pct: float = 0.2) -> float:
        """Force price to be safely BELOW current mark by buffer_pct (percent)."""
        p = float(price or 0.0)
        m = float(mark or 0.0)
        t = float(tick or 0.0)
        # Keep at least 0.20% away from mark; cheap symbols often need a bit more than 1 tick.
        buf = max(float(buffer_pct or 0.0) / 100.0, 0.0020)
        if m > 0:
            ceil = m * (1.0 - buf)
            p = min(p if p > 0 else ceil, ceil)
            if t > 0:
                p = min(p, ceil - t)
                p = _round_price_to_tick(p, t, mode="down")
        elif t > 0:
            p = _round_price_to_tick(p, t, mode="down")
        return float(p)

    def _live_refresh_scale_in_count(self, *, pos_uid: str, prefix: str, sym: str, pos_side: str) -> int:
        """Best-effort compute how many averaging adds were actually executed and store into ledger.

        Robust rule (matches our DB validation):
          - Work only with the correct *order side* for the position (LONG->BUY, SHORT->SELL).
          - Pick the active series token from the most recent ADD with status NEW/PARTIALLY_FILLED;
            if none exists, fallback to the most recent FILLED/PARTIALLY_FILLED ADD.
          - Compute adds_done = MAX(ADDn) among FILLED/PARTIALLY_FILLED for that token.

        This avoids:
          - double-counting duplicate rows for the same client_order_id,
          - mixing BUY/SELL series,
          - mixing different TL_<token>_* series after restarts/rollovers.
        """
        if not self._is_live:
            return 0

        sym_u = str(sym or "").upper()
        pos_side_u = str(pos_side or "").upper()
        desired_order_side = "BUY" if pos_side_u == "LONG" else "SELL"

        def _find_token(*, with_pos_uid: bool, statuses: tuple[str, ...]) -> str | None:
            """Find the current TL_<token> from the orders table.

            We **prefer** filtering by pos_uid to avoid mixing series across different
            lifecycles/restarts. If older DB rows don't have pos_uid filled, we fallback
            to the legacy non-pos_uid query.
            """
            try:
                qtok = """
                SELECT client_order_id
                FROM public.orders
                WHERE exchange_id=%(ex)s
                  AND account_id=%(acc)s
                  AND strategy_id=%(sid)s
                  AND upper(symbol)=%(sym)s
                  AND upper(side)=%(oside)s
                  AND client_order_id LIKE %(like_any)s
                  AND upper(coalesce(status,'')) = ANY(%(sts)s)
                """
                if with_pos_uid:
                    qtok += " AND pos_uid=%(pos_uid)s\n"
                qtok += "ORDER BY updated_at DESC\nLIMIT 50;"
                params = {
                    "ex": int(self.exchange_id),
                    "acc": int(self.account_id),
                    "sid": self.STRATEGY_ID,
                    "sym": sym_u,
                    "oside": desired_order_side,
                    "like_any": f"{prefix}_%_ADD%",
                    "sts": list(statuses),
                }
                if with_pos_uid:
                    params["pos_uid"] = str(pos_uid)
                rows = list(self.store.query_dict(qtok, params))
                for r in rows:
                    cid = str(r.get("client_order_id") or "")
                    m = re.match(rf"^{re.escape(prefix)}_([^_]+)_ADD\d+$", cid)
                    if m:
                        return str(m.group(1) or "")
            except Exception:
                return None
            return None

        token: str | None = None

        # 1) Prefer active series token (NEW/PARTIALLY_FILLED) for this pos_uid
        token = _find_token(with_pos_uid=True, statuses=("NEW", "PARTIALLY_FILLED"))

        # 1b) Fallback: active series without pos_uid (legacy rows)
        if token is None:
            token = _find_token(with_pos_uid=False, statuses=("NEW", "PARTIALLY_FILLED"))

        # 2) Fallback to the most recent completed series token
        if token is None:
            token = _find_token(with_pos_uid=True, statuses=("FILLED", "PARTIALLY_FILLED"))
        if token is None:
            token = _find_token(with_pos_uid=False, statuses=("FILLED", "PARTIALLY_FILLED"))

        # 3) Compute adds_done as MAX(ADDn) among FILLED/PARTIALLY_FILLED for that token
        adds_done = 0
        if token:
            def _max_add_n(*, with_pos_uid: bool) -> int:
                try:
                    qmax = """
                    SELECT client_order_id
                    FROM public.orders
                    WHERE exchange_id=%(ex)s
                      AND account_id=%(acc)s
                      AND strategy_id=%(sid)s
                      AND upper(symbol)=%(sym)s
                      AND upper(side)=%(oside)s
                      AND client_order_id LIKE %(like_tok)s
                      AND upper(coalesce(status,'')) IN ('FILLED','PARTIALLY_FILLED')
                    """
                    if with_pos_uid:
                        qmax += " AND pos_uid=%(pos_uid)s\n"
                    qmax += "ORDER BY updated_at DESC\nLIMIT 1000;"
                    params = {
                        "ex": int(self.exchange_id),
                        "acc": int(self.account_id),
                        "sid": self.STRATEGY_ID,
                        "sym": sym_u,
                        "oside": desired_order_side,
                        "like_tok": f"{prefix}_{token}_ADD%",
                    }
                    if with_pos_uid:
                        params["pos_uid"] = str(pos_uid)
                    rows = list(self.store.query_dict(qmax, params))
                    done: set[int] = set()
                    for r in rows:
                        n = self._parse_add_n(str(r.get("client_order_id") or ""))
                        if n > 0:
                            done.add(n)
                    return int(max(done) if done else 0)
                except Exception:
                    return 0

            adds_done = _max_add_n(with_pos_uid=True)
            if adds_done <= 0:
                adds_done = _max_add_n(with_pos_uid=False)
        try:
            self.store.execute(
                """
                UPDATE public.position_ledger
                SET scale_in_count = GREATEST(COALESCE(scale_in_count,0), %(n)s),
                    updated_at = now()
                WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s
                  AND status IN ('OPEN','CLOSED') AND source='live' AND strategy_id=%(sid)s;
                """,
                {
                    "ex": int(self.exchange_id),
                    "acc": int(self.account_id),
                    "pos_uid": str(pos_uid),
                    "sid": self.STRATEGY_ID,
                    "n": int(adds_done),
                },
            )
        except Exception:
            # non-fatal
            pass

        # Keep positions.scale_in_count in sync (used by dashboards/risk logic).
        try:
            self.store.execute(
                """
                UPDATE public.positions
                   SET scale_in_count = GREATEST(COALESCE(scale_in_count,0), %(n)s),
                       updated_at = now()
                 WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s;
                """,
                {
                    "ex": int(self.exchange_id),
                    "acc": int(self.account_id),
                    "pos_uid": str(pos_uid),
                    "n": int(adds_done),
                },
            )
        except Exception:
            pass

        return int(adds_done)

    def _live_last_add_fill_price(
        self,
        *,
        sym: str,
        prefix: str,
        tok: str,
        add_n: int,
    ) -> float:
        """Return VWAP fill price for the given ADDn using order_fills.

        We rely on DB order_fills (client_order_id is duplicated there) to avoid
        exchange REST calls during recovery/loops.
        """
        try:
            cid = f"{prefix}_{tok}_ADD{int(add_n)}"

            # order_fills stores symbol_id (no 'symbol' text column). Resolve it once.
            sym_row = self.store.query_one(
                """
                SELECT symbol_id
                FROM public.symbols
                WHERE exchange_id=%(ex)s AND upper(symbol)=%(sym)s
                LIMIT 1;
                """,
                {"ex": int(self.exchange_id), "sym": str(sym).upper()},
            )
            symbol_id = int(sym_row[0]) if sym_row and sym_row[0] is not None else 0
            if symbol_id <= 0:
                return 0.0

            row = self.store.query_one(
                """
                SELECT
                  SUM(price * qty) / NULLIF(SUM(qty), 0) AS vwap
                FROM public.order_fills
                WHERE exchange_id=%(ex)s
                  AND account_id=%(acc)s
                  AND symbol_id=%(symbol_id)s
                  AND client_order_id=%(cid)s
                  AND qty > 0;
                """,
                {"ex": int(self.exchange_id), "acc": int(self.account_id), "symbol_id": int(symbol_id), "cid": cid},
            )
            if not row:
                return 0.0
            vwap = row[0]
            return float(vwap or 0.0)
        except Exception:
            return 0.0

    def _memo_recent_desired_order(self, bucket: str, key: str, payload: dict, ttl_sec: float) -> bool:
        """Best-effort in-process debounce for identical desired orders.

        Returns True when the same desired payload was seen recently and the caller
        should skip reissuing in this cycle.
        """
        try:
            ttl = float(ttl_sec or 0.0)
        except Exception:
            ttl = 0.0
        if ttl <= 0:
            return False
        try:
            if not hasattr(self, bucket) or not isinstance(getattr(self, bucket), dict):
                setattr(self, bucket, {})
            store = getattr(self, bucket)
            now = time.time()
            cur = store.get(str(key))
            if isinstance(cur, dict):
                ts = float(cur.get("ts") or 0.0)
                prev = cur.get("payload")
                if ts and (now - ts) < ttl and prev == payload:
                    return True
            store[str(key)] = {"ts": now, "payload": payload}
        except Exception:
            return False
        return False

    def _forget_recent_desired_order(self, bucket: str, key: str) -> None:
        try:
            store = getattr(self, bucket, None)
            if isinstance(store, dict):
                store.pop(str(key), None)
        except Exception:
            pass

    def _remember_recent_desired_order(self, bucket: str, key: str, payload: dict) -> None:
        try:
            if not hasattr(self, bucket) or not isinstance(getattr(self, bucket), dict):
                setattr(self, bucket, {})
            store = getattr(self, bucket)
            store[str(key)] = {"ts": time.time(), "payload": payload}
        except Exception:
            pass

    def _tl_safe_trigger_price(self, *, symbol: str, close_side: str, desired: float, mark: float, tick: float, buffer_pct: float) -> float:
        """Ensure conditional trigger price won't immediately trigger.

        For close_side=SELL (closing LONG): trigger/activation must be ABOVE current mark.
        For close_side=BUY  (closing SHORT): trigger/activation must be BELOW current mark.

        Notes:
          - Binance can still reject a trigger that is technically on the correct side but too close
            to current mark with -2021 ("Order would immediately trigger.").
          - We therefore enforce a stronger minimum gap (0.20%) and, when tick is known, push the
            trigger one extra tick away from the current mark.
        """
        p = float(desired or 0.0)
        m = float(mark or 0.0)
        t = float(tick or 0.0)
        # buffer_pct comes in percent units; keep at least 0.20% away from mark.
        buf = max(float(buffer_pct or 0.0) / 100.0, 0.0020)

        if m > 0:
            close_side_u = str(close_side).upper()
            if close_side_u == "SELL":
                floor = m * (1.0 + buf)
                p = max(p, floor)
                if t > 0:
                    p = max(p, floor + t)
                    p = _round_price_to_tick(p, t, mode="up")
            else:
                ceil = m * (1.0 - buf)
                p = min(p if p > 0 else ceil, ceil)
                if t > 0:
                    p = min(p, ceil - t)
                    p = _round_price_to_tick(p, t, mode="down")
        else:
            if t > 0:
                p = _round_price_to_tick(p, t, mode="up" if str(close_side).upper() == "SELL" else "down")
        return float(p)

    def _live_ensure_tp_trailing_after_last_add(
        self,
        *,
        sym: str,
        pos_side: str,
        prefix: str,
        tok: str,
        adds_done: int,
        max_adds: int,
    ) -> bool:
        """After the last allowed add, (re)place a trailing order for the MAIN position to take profit.

        Uses config:
          - trailing_enabled
          - trailing_activation_pct
          - trailing_trail_pct
          - trailing_activation_buffer_pct (anti -2021)

        Client id:
          - {prefix}_{tok}_TRL   (kept for backwards compatibility)
        """
        if not bool(getattr(self.p, "trailing_enabled", False)):
            return False
        if int(adds_done) < int(max_adds) or int(max_adds) <= 0:
            return False

        # last add fill (used only as a "freshness" anchor + debug)
        last_n = int(min(int(adds_done), int(max_adds)))
        last_fill = float(self._live_last_add_fill_price(sym=sym, prefix=prefix, tok=tok, add_n=last_n) or 0.0)
        if last_fill <= 0:
            return False

        side_u = str(pos_side or "").upper()
        close_side = "SELL" if side_u == "LONG" else "BUY"

        # Activation base MUST be the average entry price (per config requirement).
        # In Binance positionRisk, entryPrice is the (avg) entry price for that position side.
        entry_price = 0.0
        try:
            pr = self._rest_snapshot_get("position_risk") or []
            for r in pr if isinstance(pr, list) else []:
                if str(r.get("symbol") or "").upper() != str(sym).upper():
                    continue
                ps = str(r.get("positionSide") or "").upper()
                if ps and ps in {"LONG", "SHORT"} and ps != side_u:
                    continue
                entry_price = float(r.get("entryPrice") or 0.0)
                if entry_price > 0:
                    break
        except Exception:
            entry_price = 0.0

        # Fallback (shouldn't happen for our positions, but keep safe): use last add fill.
        if entry_price <= 0:
            entry_price = float(last_fill)

        act_pct = float(getattr(self.p, "trailing_activation_pct", 0.0) or 0.0)
        trail_pct = float(getattr(self.p, "trailing_trail_pct", 0.0) or 0.0)
        buf_pct = float(getattr(self.p, "trailing_activation_buffer_pct", 0.0) or 0.0)
        if act_pct <= 0 or trail_pct <= 0:
            return False

        # Activation towards profit direction, based on AVG ENTRY PRICE (not last add fill).
        if side_u == "LONG":
            activation = entry_price * (1.0 + act_pct / 100.0)
        else:
            activation = entry_price * (1.0 - act_pct / 100.0)

        # Tick rounding
        tick = 0.0
        try:
            tick = float(self._price_tick_for_symbol(sym) or 0.0)
        except Exception:
            tick = 0.0
        if tick and tick > 0:
            activation = float(_round_price_to_tick(activation, tick, mode=("up" if side_u == "LONG" else "down")))

        # Ensure activation doesn't immediately trigger relative to mark price (buffer)
        mark = 0.0
        try:
            pr = self._rest_snapshot_get("position_risk") or []
            for r in pr if isinstance(pr, list) else []:
                if str(r.get("symbol") or "").upper() != str(sym).upper():
                    continue
                ps = str(r.get("positionSide") or "").upper()
                if ps and ps in {"LONG", "SHORT"} and ps != side_u:
                    continue
                mark = float(r.get("markPrice") or 0.0)
                if mark > 0:
                    break
        except Exception:
            mark = 0.0

        if mark > 0 and tick and tick > 0:
            if side_u == "LONG":
                # activation must be ABOVE mark
                min_act = mark * (1.0 + buf_pct / 100.0)
                if activation <= min_act:
                    activation = float(_round_price_to_tick(min_act + tick, tick, mode="up"))
            else:
                # activation must be BELOW mark
                max_act = mark * (1.0 - buf_pct / 100.0)
                if activation >= max_act:
                    activation = float(_round_price_to_tick(max_act - tick, tick, mode="down"))

        cid_trl = f"{prefix}_{tok}_TRL"

        # If existing TRL is already TRAILING_STOP_MARKET, do not re-place unless older than last add in DB.
        # (We compare by orders.updated_at.)
        symbol_id = 0
        try:
            sym_row = self.store.query_one(
                """
                SELECT symbol_id
                FROM public.symbols
                WHERE exchange_id=%(ex)s AND upper(symbol)=%(sym)s
                LIMIT 1;
                """,
                {"ex": int(self.exchange_id), "sym": str(sym).upper()},
            )
            symbol_id = int(sym_row[0]) if sym_row and sym_row[0] is not None else 0
        except Exception:
            symbol_id = 0

        last_add_seen_at = None
        trl_seen_at = None
        if symbol_id > 0:
            try:
                cid_add = f"{prefix}_{tok}_ADD{int(last_n)}"
                row = self.store.query_one(
                    """
                    SELECT MAX(updated_at)
                      FROM public.orders
                     WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(symbol_id)s
                       AND client_order_id=%(cid)s AND status IN ('FILLED','PARTIALLY_FILLED');
                    """,
                    {"ex": int(self.exchange_id), "acc": int(self.account_id), "symbol_id": int(symbol_id), "cid": cid_add},
                )
                last_add_seen_at = row[0] if row else None
            except Exception:
                last_add_seen_at = None
            try:
                row = self.store.query_one(
                    """
                    SELECT MAX(updated_at)
                      FROM public.orders
                     WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(symbol_id)s
                       AND client_order_id=%(cid)s;
                    """,
                    {"ex": int(self.exchange_id), "acc": int(self.account_id), "symbol_id": int(symbol_id), "cid": cid_trl},
                )
                trl_seen_at = row[0] if row else None
            except Exception:
                trl_seen_at = None

        must_refresh = True
        try:
            if last_add_seen_at is not None and trl_seen_at is not None and trl_seen_at >= last_add_seen_at:
                must_refresh = False
        except Exception:
            must_refresh = True

        # Open algo orders snapshot
        open_algos = self._rest_snapshot_get("open_algo_orders_all") or []
        existing = None
        for ao in open_algos if isinstance(open_algos, list) else []:
            try:
                if str(ao.get("symbol") or "").upper() != str(sym).upper():
                    continue
                if str(ao.get("clientAlgoId") or "") != cid_trl:
                    continue
                otype = str(ao.get("orderType") or ao.get("type") or "").upper()
                if otype == "TRAILING_STOP_MARKET":
                    existing = ao
                    break
            except Exception:
                continue

        # Cooldown/semantic dedupe: recovery/openAlgo snapshots can lag. Treat any OPEN trailing
        # for the same symbol+position side(+pos_uid when available) as authoritative.
        pos_uid = None
        semantic_db_row = None
        try:
            semantic_db_row = self.store.query_one(
                """
                SELECT client_algo_id, trigger_price, quantity, updated_at, pos_uid
                  FROM public.algo_orders
                 WHERE exchange_id=%(ex)s AND account_id=%(acc)s
                   AND upper(symbol)=%(sym)s
                   AND upper(COALESCE(position_side,'')) IN (%(side)s, 'BOTH', '')
                   AND upper(type)='TRAILING_STOP_MARKET'
                   AND status='OPEN'
                 ORDER BY updated_at DESC
                 LIMIT 1;
                """,
                {
                    "ex": int(self.exchange_id),
                    "acc": int(self.account_id),
                    "sym": str(sym).upper(),
                    "side": str(side_u).upper(),
                },
            )
        except Exception:
            semantic_db_row = None
        if existing is None and semantic_db_row and semantic_db_row[0]:
            existing = {
                "clientAlgoId": str(semantic_db_row[0] or ""),
                "activatePrice": float(semantic_db_row[1] or 0.0),
            }
            try:
                pos_uid = str(semantic_db_row[4] or "")
            except Exception:
                pos_uid = None

        # raw_meta cooldown for after-last-add trailing placement
        try:
            cd_sec = float(getattr(self.p, "trailing_reissue_cooldown_sec", 90) or 90)
        except Exception:
            cd_sec = 90.0
        if cd_sec and cd_sec > 0 and symbol_id > 0 and self._pl_has_raw_meta:
            try:
                row = self.store.query_one(
                    """
                    SELECT raw_meta
                      FROM public.position_ledger
                     WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND strategy_id=%(st)s
                       AND symbol_id=%(symbol_id)s AND side=%(side)s AND source='live'
                     ORDER BY updated_at DESC
                     LIMIT 1;
                    """,
                    {
                        "ex": int(self.exchange_id),
                        "acc": int(self.account_id),
                        "st": str(self.strategy_id),
                        "symbol_id": int(symbol_id),
                        "side": str(side_u),
                    },
                )
                rm = row[0] if row else None
                if isinstance(rm, str) and rm:
                    rm = json.loads(rm)
                if isinstance(rm, dict):
                    ts_s = ((rm.get("after_last_add_tp_trl") or {}).get("ts") or ((rm.get("live_entry") or {}).get("after_last_add_tp_trl") or {}).get("ts"))
                    ts_s = ts_s if 'ts_s' in locals() else None
                    if ts_s:
                        now_ms = self._to_epoch_ms(_utc_now()) or 0
                        ts_ms = self._to_epoch_ms(ts_s) or 0
                        if now_ms and ts_ms and (now_ms - ts_ms) < int(cd_sec * 1000) and existing is not None:
                            return False
            except Exception:
                pass

        # If TRL already exists and matches our desired params, keep it.
        # We prefer exchange snapshot over DB timestamps to avoid "thrash" when DB sync lags.
        if existing is not None:
            try:
                ex_act = float(existing.get("activatePrice") or existing.get("activationPrice") or 0.0)
            except Exception:
                ex_act = 0.0
            try:
                ex_cb = float(existing.get("callbackRate") or 0.0)
            except Exception:
                ex_cb = 0.0
            if _activation_close_enough(ex_act, float(activation), float(tick or 0.0)) and abs(ex_cb - float(trail_pct)) <= 1e-9:
                try:
                    self._set_cached_trl_activation(sym, side_u, cid_trl, float(ex_act or activation))
                except Exception:
                    pass
                return False

        # Local cache dedupe: Binance can round/echo activatePrice with tiny drift, which can still
        # make semantic comparisons look different across cycles. Prefer what we most recently placed.
        try:
            cached_act = self._get_cached_trl_activation(sym, side_u, cid_trl)
        except Exception:
            cached_act = None
        if cached_act is not None and _activation_close_enough(float(cached_act), float(activation), float(tick or 0.0)):
            return False

        if existing is not None and not must_refresh:
            # already fresh by DB/open order semantics
            return False

        try:
            trl_guard_sec = float(getattr(self.p, "trailing_reissue_cooldown_sec", 120) or 120)
        except Exception:
            trl_guard_sec = 120.0
        hedge_mode = bool(getattr(self.p, "hedge_enabled", False))
        position_side = side_u if hedge_mode else "BOTH"

        # NOTE: Binance UM Futures TRAILING_STOP_MARKET in hedge mode is reliably accepted with
        # explicit quantity. Using closePosition=true for trailing is rejected on some accounts/
        # endpoints (HTTP 400), which caused a "place then cancel" loop with recovery.
        qty_main = 0.0
        try:
            pr = self._rest_snapshot_get("position_risk") or []
            for r in pr if isinstance(pr, list) else []:
                if str(r.get("symbol") or "").upper() != str(sym).upper():
                    continue
                ps = str(r.get("positionSide") or "").upper()
                if hedge_mode and ps in {"LONG", "SHORT"} and ps != side_u:
                    continue
                amt = float(r.get("positionAmt") or 0.0)
                qty_main = abs(amt)
                if qty_main > 0:
                    break
        except Exception:
            qty_main = 0.0

        if qty_main <= 0:
            return False

        # round to step
        try:
            step = float(self._qty_step_for_symbol(sym) or 0.0)
        except Exception:
            step = 0.0
        if step and step > 0:
            qty_main = float(_round_qty_to_step(qty_main, step, mode="down"))
        if qty_main <= 0:
            return False

        _trl_payload = {
            "symbol": str(sym).upper(),
            "position_side": side_u,
            "side": close_side,
            "activation": round(float(activation), 12),
            "trail_pct": round(float(trail_pct), 8),
            "qty": round(float(qty_main), 12),
            "mode": "after_last_add",
        }
        if self._memo_recent_desired_order("_recent_trl_desired", cid_trl, _trl_payload, trl_guard_sec):
            return False
        self._forget_recent_desired_order("_recent_trl_desired", cid_trl)

        # Cancel existing TRL (best-effort)
        try:
            self._binance.cancel_algo_order(symbol=sym, clientAlgoId=cid_trl)
        except Exception:
            pass

        try:
            # Guard: never (re)place trailing if MAIN position is not open on exchange
            if hedge_mode and (not self._has_open_position_live(sym, str(position_side))):
                self._dlog("skip ensure TRL: no open MAIN position sym=%s side=%s", sym, position_side)
                try:
                        self._cancel_algo_by_client_id_safe(sym, cid_trl)
                except Exception:
                    pass
                return None
            if (not hedge_mode) and position_side:
                if (not self._has_open_position_live(sym, str(position_side))):
                    self._dlog("skip ensure TRL: no open position sym=%s side=%s", sym, position_side)
                    try:
                        self._cancel_algo_by_client_id_safe(sym, cid_trl)
                    except Exception:
                        pass
                    return None

            resp_trl = self._binance.new_order(
                symbol=sym,
                side=close_side,
                type="TRAILING_STOP_MARKET",
                quantity=float(qty_main),
                callbackRate=float(trail_pct),
                activationPrice=float(activation),
                workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                newClientOrderId=cid_trl,
                positionSide=position_side if hedge_mode else None,
            )
            self._remember_recent_desired_order("_recent_trl_desired", cid_trl, _trl_payload)
            try:
                self._upsert_algo_order_shadow(resp_trl, pos_uid=str(pos_uid or ""), strategy_id=self.STRATEGY_ID)
            except Exception:
                pass
            try:
                self._mark_local_algo_open(
                    cid_trl,
                    symbol=sym,
                    position_side=position_side if hedge_mode else side_u,
                    order_type="TRAILING_STOP_MARKET",
                    side=close_side,
                    quantity=float(qty_main),
                    trigger_price=float(activation),
                    close_position=None,
                    pos_uid=str(pos_uid or ""),
                )
            except Exception:
                pass
            try:
                self._refresh_open_algo_snapshot_for_symbol(sym)
            except Exception:
                pass
            try:
                self._set_cached_trl_activation(sym, side_u, cid_trl, float(activation))
            except Exception:
                pass
        except Exception:
            self._forget_recent_desired_order("_recent_trl_desired", cid_trl)
            try:
                if hasattr(self, "_recent_main_trl_place_ts") and isinstance(self._recent_main_trl_place_ts, dict):
                    self._recent_main_trl_place_ts.pop(cid_trl, None)
            except Exception:
                pass
            return False

        # store meta flag (best-effort)
        try:
            if self._pl_has_raw_meta and symbol_id > 0:
                meta = {
                    "after_last_add_tp_trl": {
                        "add_n": int(last_n),
                        "fill": float(last_fill),
                        "entry_price": float(entry_price),
                        "activation": float(activation),
                        "trail_pct": float(trail_pct),
                        "act_pct": float(act_pct),
                        "buf_pct": float(buf_pct),
                        "ts": _utc_now().isoformat(),
                    }
                }
                self.store.execute(
                    """
                    UPDATE public.position_ledger
                       SET raw_meta = COALESCE(raw_meta, '{}'::jsonb) || %(meta)s::jsonb
                     WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND strategy_id=%(st)s
                       AND symbol_id=%(symbol_id)s AND side=%(side)s AND source='live'
                       AND updated_at = (
                           SELECT MAX(updated_at) FROM public.position_ledger
                            WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND strategy_id=%(st)s
                              AND symbol_id=%(symbol_id)s AND side=%(side)s AND source='live'
                       );
                    """,
                    {
                        "meta": json.dumps(meta),
                        "ex": int(self.exchange_id),
                        "acc": int(self.account_id),
                        "st": str(self.strategy_id),
                        "symbol_id": int(symbol_id),
                        "side": str(side_u),
                    },
                )
        except Exception:
            pass

        self.log.info(
            "[trade_liquidation][LIVE][TP_TRL] placed after-last-add TRAILING_STOP_MARKET %s %s activation=%.8f (entry=%.8f last_add=%.8f add_n=%s act_pct=%.4f trail_pct=%.4f)",
            str(sym).upper(),
            str(side_u),
            float(activation),
            float(entry_price),
            float(last_fill),
            str(last_n),
            float(act_pct),
            float(trail_pct),
        )
        return True

    def _live_ensure_tp_trailing_pre_last_add(
        self,
        pos: dict,
        sym: str,
        pos_side: str,
        pos_uid: str,
        prefix: str,
        tok: str,
        adds_done: int,
        max_adds: int,
    ) -> bool:
        """Ensure main TRAILING_STOP_MARKET exists before the last averaging add is filled.

        This path is active whenever averaging is enabled and there are still
        pending adds left for the main position. Unlike STOP_MARKET SL deferral,
        main trailing should be allowed *before* the last add so profit can be
        protected even if price never returns to averaging levels.
        """
        try:
            if not bool(getattr(self.p, "trailing_enabled", False)):
                return False
            if int(max_adds or 0) <= 0 or int(adds_done or 0) >= int(max_adds or 0):
                return False

            side_u = str(pos_side or "").upper()
            if side_u not in {"LONG", "SHORT"}:
                return False

            qty = _safe_float(pos.get("qty_current") or pos.get("qty_opened") or 0.0, 0.0)
            if qty <= 0.0:
                return False
            if not self._has_open_position_live(str(sym).upper(), side_u):
                return False

            sym_u = str(sym).upper()

            # Prefer exchange-reported avg entry from positionRisk for live positions.
            # This avoids stale activation prices when avg entry changed after fills/adds.
            entry_price = 0.0
            try:
                pr = self._rest_snapshot_get("position_risk") or []
                for r in pr if isinstance(pr, list) else []:
                    if str(r.get("symbol") or "").upper() != sym_u:
                        continue
                    ps = str(r.get("positionSide") or "").upper()
                    if ps and ps in {"LONG", "SHORT"} and ps != side_u:
                        continue
                    entry_price = abs(float(r.get("entryPrice") or r.get("avgEntryPrice") or r.get("avgPrice") or 0.0))
                    if entry_price > 0.0:
                        break
            except Exception:
                entry_price = 0.0
            if entry_price <= 0.0:
                entry_price = _safe_float(pos.get("avg_price") or pos.get("entry_price") or 0.0, 0.0)
            if entry_price <= 0.0:
                return False
            close_side = "SELL" if side_u == "LONG" else "BUY"
            hedge_mode = bool(getattr(self.p, "hedge_enabled", False))
            position_side = side_u if hedge_mode else None
            act_pct = float(getattr(self.p, "trailing_activation_pct", 0.0) or 0.0)
            trail_pct = float(getattr(self.p, "trailing_trail_pct", 0.0) or 0.0)
            buf_pct = float(getattr(self.p, "trailing_activation_buffer_pct", 0.20) or 0.20)
            tick_dec = self._price_tick_for_symbol(sym_u)
            tick = float(tick_dec) if tick_dec is not None else 0.0
            cid_trl = f"{prefix}_{tok}_TRL"

            try:
                known = self._find_open_algo_order_by_client_id(sym_u, cid_trl) or self._find_semantic_open_algo(
                    sym_u,
                    client_suffix="_TRL",
                    position_side=position_side,
                    order_type="TRAILING_STOP_MARKET",
                    side=close_side,
                )
                if known:
                    return False
            except Exception:
                pass

            try:
                guard_sec = float(getattr(self.p, "trailing_place_guard_sec", 180) or 180)
            except Exception:
                guard_sec = 20.0
            try:
                recent_ts = float(getattr(self, "_recent_main_trl_place_ts", {}).get(cid_trl, 0.0) or 0.0)
            except Exception:
                recent_ts = 0.0
            if recent_ts and (time.time() - recent_ts) < guard_sec:
                return False

            mark = self._get_mark_price_live(sym_u, position_side)
            if mark <= 0.0:
                mark = entry_price

            if close_side == "SELL":
                desired = entry_price * (1.0 + act_pct / 100.0)
            else:
                desired = entry_price * (1.0 - act_pct / 100.0)
            desired = _ensure_tp_trail_side(desired, entry_price, tick, side_u, kind="trail_activate")
            activation = self._tl_safe_trigger_price(
                symbol=sym_u,
                close_side=close_side,
                desired=desired,
                mark=mark,
                tick=tick,
                buffer_pct=buf_pct,
            )
            activation = _round_price_to_tick(
                activation,
                tick,
                mode="up" if close_side == "SELL" else "down",
            )
            activation = _ensure_tp_trail_side(activation, entry_price, tick, side_u, kind="trail_activate")

            try:
                trl_guard_sec = float(getattr(self.p, "trailing_reissue_cooldown_sec", 120) or 120)
            except Exception:
                trl_guard_sec = 120.0
            _trl_payload = {
                "symbol": sym_u,
                "position_side": position_side or "BOTH",
                "side": close_side,
                "activation": round(float(activation), 12),
                "trail_pct": round(float(trail_pct), 8),
                "qty": round(float(qty), 12),
                "mode": "pre_last_add",
            }
            if self._memo_recent_desired_order("_recent_trl_desired", cid_trl, _trl_payload, trl_guard_sec):
                return False
            self._forget_recent_desired_order("_recent_trl_desired", cid_trl)

            resp = self._binance.new_order(
                symbol=sym_u,
                side=close_side,
                type="TRAILING_STOP_MARKET",
                quantity=float(qty),
                activationPrice=float(activation),
                callbackRate=float(trail_pct),
                workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                newClientOrderId=cid_trl,
                positionSide=position_side,
            )
            _dup_resp = bool(isinstance(resp, dict) and resp.get("_duplicate"))
            self._remember_recent_desired_order("_recent_trl_desired", cid_trl, _trl_payload)
            try:
                self._upsert_order_shadow(
                    pos_uid=pos_uid,
                    order_id=resp.get("orderId") or resp.get("order_id") or resp.get("algoId") or "",
                    client_order_id=cid_trl,
                    symbol_id=int(pos.get("symbol_id") or 0),
                    side=close_side,
                    order_type="TRAILING_STOP_MARKET",
                    qty=float(qty),
                    price=None,
                    reduce_only=True,
                    status=str(resp.get("status") or "NEW"),
                )
            except Exception:
                pass
            try:
                self._upsert_algo_order_shadow(resp, pos_uid=str(pos_uid or ""), strategy_id=self.STRATEGY_ID)
            except Exception:
                pass
            try:
                self._mark_local_algo_open(
                    cid_trl,
                    symbol=sym_u,
                    position_side=position_side or side_u,
                    order_type="TRAILING_STOP_MARKET",
                    side=close_side,
                    quantity=float(qty),
                    trigger_price=float(activation),
                    close_position=None,
                    pos_uid=str(pos_uid or ""),
                )
            except Exception:
                pass
            try:
                self._refresh_open_algo_snapshot_for_symbol(sym_u)
            except Exception:
                pass
            try:
                if not hasattr(self, "_recent_main_trl_place_ts") or not isinstance(self._recent_main_trl_place_ts, dict):
                    self._recent_main_trl_place_ts = {}
                self._recent_main_trl_place_ts[cid_trl] = time.time()
            except Exception:
                pass
            if _dup_resp:
                return False
            self.log.info(
                "[trade_liquidation][LIVE][TP_TRL] placed pre-last-add TRAILING_STOP_MARKET %s %s activation=%.8f (entry=%.8f adds_done=%s/%s trail_pct=%.4f)",
                sym_u,
                side_u,
                float(activation),
                float(entry_price),
                int(adds_done),
                int(max_adds),
                float(trail_pct),
            )
            return True
        except Exception:
            try:
                self._forget_recent_desired_order("_recent_trl_desired", cid_trl)
            except Exception:
                pass
            try:
                if hasattr(self, "_recent_main_trl_place_ts") and isinstance(self._recent_main_trl_place_ts, dict):
                    self._recent_main_trl_place_ts.pop(cid_trl, None)
            except Exception:
                pass
            self.log.exception(
                "[trade_liquidation][LIVE][TP_TRL] failed pre-last-add ensure for %s %s (pos_uid=%s)",
                str(sym).upper(),
                str(pos_side).upper(),
                str(pos_uid),
            )
            return False

    def _live_ensure_next_add_order(self, *, pos: dict) -> bool:
        """Ensure the *next* averaging add order exists (LIVE).

        Rules:
          - Never place more than cfg_max_adds.
          - Place only one missing order per cycle (reduces spam).
          - Add levels are derived from avg_price with a minimum distance percentage.
        """
        if not self._is_live or self._binance is None:
            return False

        if not bool(getattr(self.p, "averaging_enabled", False)):
            return False

        max_adds = self._cfg_max_adds()
        if max_adds <= 0:
            return False

        sym = str(pos.get("symbol") or "").upper()
        if not sym:
            return False

        side = str(pos.get("side") or "").upper()  # LONG/SHORT
        pos_uid = str(pos.get("pos_uid") or "")
        if not pos_uid:
            return False

        prefix = _sanitize_coid_prefix(str(getattr(self.p, "client_order_prefix", "TL") or "TL"))
        # IMPORTANT: token must match the live order series token used in client ids
        # (e.g. TL_<token>_ENTRY / TL_<token>_ADDn / TL_<token>_TRL). It is NOT
        # derived from pos_uid in production; it is generated at signal time and
        # stored in raw_meta / orders. If we use a pos_uid-derived token here,
        # we will fail to find fills/orders and won't refresh protection.
        tok = ""

        # 0) Prefer DB truth: read ENTRY client_order_id for this pos_uid.
        # This is the most reliable across restarts and does not depend on raw_meta.
        try:
            row = self.store.query_one(
                """
                SELECT client_order_id
                FROM public.orders
                WHERE exchange_id=%(ex)s AND account_id=%(acc)s
                  AND strategy_id=%(sid)s
                  AND pos_uid=%(u)s
                  AND client_order_id LIKE %(like_entry)s
                ORDER BY updated_at DESC
                LIMIT 1;
                """,
                {
                    "ex": int(self.exchange_id),
                    "acc": int(self.account_id),
                    "sid": self.STRATEGY_ID,
                    "u": str(pos_uid),
                    "like_entry": f"{prefix}_%_ENTRY",
                },
            )
            if row and row[0]:
                coid = str(row[0])
                if coid.startswith(prefix + "_"):
                    m = re.match(rf"^{re.escape(prefix)}_([^_]+)_", coid)
                    if m:
                        tok = str(m.group(1) or "")
        except Exception:
            tok = ""

        # 1) raw_meta fallback
        try:
            rm = pos.get("raw_meta")
            if isinstance(rm, str) and rm:
                rm = json.loads(rm)
            if isinstance(rm, dict):
                coid = (
                    (((rm.get("live_entry") or {}).get("entry_order") or {}).get("clientOrderId"))
                    or (((rm.get("live_entry") or {}).get("entry_order") or {}).get("client_order_id"))
                    or ""
                )
                if isinstance(coid, str) and coid.startswith(prefix + "_"):
                    # prefix_token_SUFFIX
                    m = re.match(rf"^{re.escape(prefix)}_([^_]+)_", coid)
                    if m:
                        tok = str(m.group(1) or "")
        except Exception:
            tok = ""
        if not tok:
            tok = _coid_token(pos_uid, n=20)
        # Refresh adds_done from DB truth (deduped by ADDn, series token & position side)
        adds_done = self._live_refresh_scale_in_count(pos_uid=pos_uid, prefix=prefix, sym=sym, pos_side=side)
        # Ledger snapshot may already have something
        try:
            adds_done = max(adds_done, int(pos.get("scale_in_count", 0) or 0))
        except Exception:
            pass

        try:
            if int(max_adds or 0) > 0 and int(adds_done or 0) < int(max_adds or 0):
                self._live_ensure_tp_trailing_pre_last_add(
                    pos=pos,
                    sym=sym,
                    pos_side=side,
                    pos_uid=pos_uid,
                    prefix=prefix,
                    tok=tok,
                    adds_done=int(adds_done),
                    max_adds=int(max_adds),
                )
        except Exception:
            self.log.exception(
                "[trade_liquidation][LIVE][TP_TRL] failed pre-last-add call for %s %s (pos_uid=%s)",
                str(sym).upper(),
                str(side).upper(),
                str(pos_uid),
            )

        next_n = int(adds_done) + 1
        if next_n > max_adds:
            # Max additions cap reached -> do NOT place any new ADD orders.
            # Also cancel any leftover pending ADD orders for this series token (after restart / duplicates),
            # while still allowing hedge/SL logic after last add depending on settings.
            try:
                open_all = self._rest_snapshot_get("open_orders_all") or []
                open_algos = self._rest_snapshot_get("open_algo_orders_all") or []
                _add_pref = f"{prefix}_{tok}_ADD"
                for oo in open_all if isinstance(open_all, list) else []:
                    try:
                        if str(oo.get("symbol") or "").upper() != sym:
                            continue
                        cid = str(oo.get("clientOrderId") or "")
                        if cid and cid.startswith(_add_pref):
                            try:
                                self._binance.cancel_order(symbol=sym, origClientOrderId=cid)
                            except Exception:
                                pass
                    except Exception:
                        continue
                for ao in open_algos if isinstance(open_algos, list) else []:
                    try:
                        if str(ao.get("symbol") or "").upper() != sym:
                            continue
                        cid = str(ao.get("clientAlgoId") or "")
                        if cid and cid.startswith(_add_pref):
                            try:
                                self._binance.cancel_algo_order(symbol=sym, clientAlgoId=cid)
                            except Exception:
                                pass
                    except Exception:
                        continue
            except Exception:
                pass
            # After the last allowed add is filled, place/refresh a protective hedge/SL
            # relative to that last add price.
            try:
                self._live_ensure_hedge_after_last_add(
                    pos=pos,
                    sym=sym,
                    pos_side=side,
                    pos_uid=pos_uid,
                    prefix=prefix,
                    tok=tok,
                    adds_done=int(adds_done),
                    max_adds=int(max_adds),
                )
                self._live_ensure_tp_trailing_after_last_add(
                    sym=sym,
                    pos_side=side,
                    prefix=prefix,
                    tok=tok,
                    adds_done=int(adds_done),
                    max_adds=int(max_adds),
                )
            except Exception:
                log.exception(
                    "[trade_liquidation][LIVE][AFTER_LAST_ADD] failed ensure after-last-add orders for %s %s (pos_uid=%s)",
                    sym,
                    side,
                    pos_uid,
                )
            return False

        # Check open ADD orders (openOrders/openAlgoOrders).
        # IMPORTANT: we must not spam multiple ADDs at once.
        want_cid = f"{prefix}_{tok}_ADD{next_n}"
        open_all = self._rest_snapshot_get("open_orders_all") or []
        open_algos = self._rest_snapshot_get("open_algo_orders_all") or []

        # Recovery can place ADD1 just before LIVE AVG runs, while Binance/openAlgo snapshots still lag.
        # Use both a short in-process guard and DB shadow check to avoid re-sending the same clientOrderId.
        try:
            recent_add_guard = float(getattr(self.p, "averaging_recovery_place_guard_sec", 30) or 30)
        except Exception:
            recent_add_guard = 30.0
        try:
            recent_map = getattr(self, "_recent_recovery_add_place_ts", {})
            recent_ts = float(recent_map.get(want_cid, 0.0) or 0.0) if isinstance(recent_map, dict) else 0.0
        except Exception:
            recent_ts = 0.0
        if recent_ts and (time.time() - recent_ts) < recent_add_guard:
            return False

        try:
            add_reissue_guard = float(getattr(self.p, "averaging_reissue_cooldown_sec", 120) or 120)
        except Exception:
            add_reissue_guard = 120.0
        _add_payload = {
            "symbol": sym,
            "side": side,
            "cid": want_cid,
            "next_n": int(next_n),
        }
        if self._memo_recent_desired_order("_recent_add_desired", want_cid, _add_payload, add_reissue_guard):
            return False

        def _db_add_already_executed(cid: str) -> bool:
            cid = str(cid or "")
            if not cid:
                return False
            try:
                row_done = self.store.fetch_one(
                    """
                    SELECT 1 AS ok
                    FROM public.orders
                    WHERE exchange_id=%s AND account_id=%s AND strategy_id=%s
                      AND client_order_id=%s
                      AND upper(coalesce(status,'')) IN ('FILLED','PARTIALLY_FILLED')
                    LIMIT 1
                    """,
                    (int(self.exchange_id), int(self.account_id), str(self.STRATEGY_ID), cid),
                )
                if row_done:
                    return True
            except Exception:
                pass
            try:
                fill_row = self.store.fetch_one(
                    """
                    SELECT 1 AS ok
                    FROM public.order_fills
                    WHERE exchange_id=%s AND account_id=%s AND strategy_id=%s
                      AND client_order_id=%s
                    LIMIT 1
                    """,
                    (int(self.exchange_id), int(self.account_id), str(self.STRATEGY_ID), cid),
                )
                if fill_row:
                    return True
            except Exception:
                pass
            return False

        if _db_add_already_executed(want_cid):
            try:
                if next_n > int(adds_done or 0):
                    self.store.execute(
                        """
                        UPDATE public.position_ledger
                           SET scale_in_count = GREATEST(COALESCE(scale_in_count,0), %(n)s),
                               updated_at = NOW()
                         WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND strategy_id=%(sid)s
                           AND pos_uid=%(pos_uid)s AND status IN ('OPEN','CLOSED') AND source='live';
                        """,
                        {
                            "ex": int(self.exchange_id),
                            "acc": int(self.account_id),
                            "sid": self.STRATEGY_ID,
                            "pos_uid": str(pos_uid),
                            "n": int(next_n),
                        },
                    )
                
            except Exception:
                pass
            return False

        try:
            row = self.store.query_one(
                """
                SELECT status, COALESCE(source, '')
                FROM public.orders
                WHERE exchange_id=%(ex)s AND account_id=%(acc)s
                  AND strategy_id=%(sid)s
                  AND client_order_id=%(cid)s
                ORDER BY updated_at DESC
                LIMIT 1;
                """,
                {
                    "ex": int(self.exchange_id),
                    "acc": int(self.account_id),
                    "sid": self.STRATEGY_ID,
                    "cid": str(want_cid),
                },
            )
            if row:
                db_status = str(row[0] or "").upper()
                db_source = str(row[1] or "")
                if db_status in ("FILLED", "PARTIALLY_FILLED"):
                    return False
                if db_status in ("NEW", "OPEN", "PARTIALLY_FILLED"):
                    # Shadow order can lag after manual cancel / recovery cleanup.
                    # Let DB block placement only if the same ADD is still present in current exchange snapshots.
                    add_is_live_on_exchange = any(str(a.get("cid") or "") == want_cid for a in (
                        ([
                            {
                                "cid": str(oo.get("clientOrderId") or "")
                            }
                            for oo in (open_all if isinstance(open_all, list) else [])
                            if str(oo.get("symbol") or "").upper() == sym
                        ]) + ([
                            {
                                "cid": str(ao.get("clientAlgoId") or "")
                            }
                            for ao in (open_algos if isinstance(open_algos, list) else [])
                            if str(ao.get("symbol") or "").upper() == sym
                        ])
                    ))
                    if add_is_live_on_exchange:
                        return False
                    # If we already have a recorded fill for the same ADDn, never recycle this clientOrderId.
                    if _db_add_already_executed(want_cid):
                        return False
                    # Poisoned recovery ids must not block new averaging orders.
                    if "#BADTP#" not in str(want_cid).upper():
                        # Extra debounce: when we have just tried to keep/place the same ADD series,
                        # do not churn DB shadow state in the same/next couple of cycles.
                        try:
                            if self._memo_recent_desired_order("_recent_add_shadow_guard", want_cid, _add_payload, add_reissue_guard):
                                return False
                        except Exception:
                            pass
                        try:
                            self.store.execute(
                                """
                                UPDATE public.orders
                                   SET status='CANCELED', updated_at=NOW()
                                 WHERE exchange_id=%(ex)s AND account_id=%(acc)s
                                   AND strategy_id=%(sid)s AND client_order_id=%(cid)s
                                   AND status IN ('NEW','OPEN','PARTIALLY_FILLED');
                                """,
                                {
                                    "ex": int(self.exchange_id),
                                    "acc": int(self.account_id),
                                    "sid": self.STRATEGY_ID,
                                    "cid": str(want_cid),
                                },
                            )
                            self._rate_limited_log(logging.DEBUG, 'stale_add', f'stale_add:{sym}:{side}:{want_cid}', '[trade_liquidation][LIVE][AVG] shadow ADD marked CANCELED as stale %s %s cid=%s source=%s', sym, side, want_cid, db_source)
                        except Exception:
                            pass
        except Exception:
            pass

        def _is_valid_add_cid(cid: str) -> bool:
            cid = str(cid or "")
            if not cid.startswith(f"{prefix}_{tok}_ADD"):
                return False
            # Historical poisoned ids created during bad TP/add recovery must never block new adds.
            if "#BADTP#" in cid.upper():
                return False
            return int(self._parse_add_n(cid) or 0) > 0

        def _iter_open_adds() -> list[dict[str, Any]]:
            out: list[dict[str, Any]] = []
            # Regular openOrders
            for oo in open_all if isinstance(open_all, list) else []:
                try:
                    if str(oo.get("symbol") or "").upper() != sym:
                        continue
                    cid = str(oo.get("clientOrderId") or "")
                    if not _is_valid_add_cid(cid):
                        continue
                    out.append({
                        "kind": "order",
                        "cid": cid,
                        "id": str(oo.get("orderId") or ""),
                        "add_n": self._parse_add_n(cid),
                    })
                except Exception:
                    continue
            # Conditional openAlgoOrders
            for ao in open_algos if isinstance(open_algos, list) else []:
                try:
                    if str(ao.get("symbol") or "").upper() != sym:
                        continue
                    cid = str(ao.get("clientAlgoId") or "")
                    if not _is_valid_add_cid(cid):
                        continue
                    out.append({
                        "kind": "algo",
                        "cid": cid,
                        "id": str(ao.get("algoId") or ""),
                        "add_n": self._parse_add_n(cid),
                    })
                except Exception:
                    continue
            return out

        def _cancel_open_add(a: dict[str, Any]) -> bool:
            """Best-effort cancel one open ADD (regular or algo) by client id."""
            cid = str(a.get("cid") or "")
            if not cid:
                return False
            try:
                if str(a.get("kind")) == "algo":
                    resp_cancel = self._binance.cancel_algo_order(symbol=sym, clientAlgoId=cid)
                    try:
                        self.store.set_algo_order_status(
                            exchange_id=int(self.exchange_id),
                            account_id=int(self.account_id),
                            client_algo_id=str(cid),
                            status="CANCELED",
                            raw_json=resp_cancel if isinstance(resp_cancel, dict) else {"result": resp_cancel},
                        )
                    except Exception:
                        pass
                else:
                    self._binance.cancel_order(symbol=sym, origClientOrderId=cid)
                return True
            except Exception:
                return False

        # 1) Cleanup: if we already have FILLED adds_done, any older open ADD1/ADD2... that is <= adds_done
        # should be canceled (stale orders after restart / duplicated placement).
        open_adds = _iter_open_adds()

        # Drop obviously invalid numbers / poisoned ids
        open_adds = [a for a in open_adds if _is_valid_add_cid(str(a.get("cid") or ""))]

        # Cancel obsolete (<= adds_done) and out-of-range (> max_adds) orders.
        canceled_any = False
        for a in list(open_adds):
            n = int(a.get("add_n") or 0)
            if n <= int(adds_done) or n > int(max_adds):
                if _cancel_open_add(a):
                    canceled_any = True
                    log.info("[trade_liquidation][LIVE][AVG] canceled stale ADD%d %s (pos_uid=%s)", n, sym, pos_uid)

        # Cancel duplicates for the same ADDn (keep only the newest by numeric id).
        # This is rare, but protects from accidental double placement.
        if open_adds:
            by_n: dict[int, list[dict[str, Any]]] = {}
            for a in open_adds:
                by_n.setdefault(int(a.get("add_n") or 0), []).append(a)
            for n, items in by_n.items():
                if len(items) <= 1:
                    continue
                # keep max numeric id
                def _id_num(x: dict[str, Any]) -> int:
                    try:
                        return int(str(x.get("id") or "0"))
                    except Exception:
                        return 0
                items_sorted = sorted(items, key=_id_num, reverse=True)
                for extra in items_sorted[1:]:
                    if _cancel_open_add(extra):
                        canceled_any = True
                        log.info("[trade_liquidation][LIVE][AVG] canceled duplicate ADD%d %s (pos_uid=%s)", int(n), sym, pos_uid)

        if canceled_any:
            # Snapshot will refresh next cycle; avoid placing a new ADD in the same cycle.
            return False

        # Re-check open adds after cleanup.
        open_adds = _iter_open_adds()
        open_adds = [a for a in open_adds if _is_valid_add_cid(str(a.get("cid") or ""))]

        # If any ADD is still open that is NOT the desired next one, do not place a new one.
        # This enforces: at most one active ADD per position.
        for a in open_adds:
            if str(a.get("cid") or "") != want_cid:
                return False

        # If the next ADD is already open, we're done.
        for a in open_adds:
            if str(a.get("cid") or "") == want_cid:
                return False

        # Respect cooldown between additions
        cd_min = float(getattr(self.p, "averaging_cooldown_minutes", 0) or 0)
        if cd_min > 0:
            try:
                meta = pos.get("raw_meta") if isinstance(pos.get("raw_meta"), dict) else None
                last_add_ts = None
                if isinstance(meta, dict):
                    last_add_ts = meta.get("last_add_at")
                # We also store last_add_at in paper_position_risk, but for LIVE best-effort.
                if isinstance(last_add_ts, datetime):
                    age_min = (_utc_now() - last_add_ts.astimezone(timezone.utc)).total_seconds() / 60.0
                    if 0 <= age_min < cd_min:
                        return False
            except Exception:
                pass

        # Re-anchor TP/TRAIL when avg changes after an add fill
        avg_price = _dec(pos.get("avg_price") or pos.get("entry_price") or 0)
        if avg_price <= 0:
            return False
        try:
            self._live_reanchor_tp_and_trailing_after_avg_change(pos, float(avg_price))
        except Exception:
            pass

        # Compute next averaging level with "min distance" rule.
        # Reference price for averaging levels.
        # Prefer live exchange entryPrice (it reflects the current weighted average after fills/restarts).
        min_dist_pct = float(self._cfg_averaging_min_level_distance_pct())
        ref_source = "ledger"
        live_entry_ref = _dec("0")
        try:
            pr = self._rest_snapshot_get("position_risk") or []
            for r in pr if isinstance(pr, list) else []:
                if str(r.get("symbol") or "").upper() != str(sym).upper():
                    continue
                ps = str(r.get("positionSide") or "").upper()
                if ps and ps in {"LONG", "SHORT"} and ps != str(side).upper():
                    continue
                live_entry_ref = _dec(r.get("entryPrice") or r.get("avgEntryPrice") or r.get("avgPrice") or 0)
                if live_entry_ref > 0:
                    ref_source = "live_entry"
                    break
        except Exception:
            live_entry_ref = _dec("0")
        ref_price = live_entry_ref if live_entry_ref > 0 else (_dec(pos.get("entry_price") or 0) if int(adds_done) == 0 else avg_price)
        if ref_price <= 0:
            ref_price = avg_price
            ref_source = "avg_price"

        add_side = "BUY" if side == "LONG" else "SELL"

        picked: Optional[float] = None
        level_source = "fallback"
        try:
            sid = int(pos.get("symbol_id") or 0)
            if sid > 0:
                picked = self._pick_averaging_level(
                    symbol_id=sid,
                    side=str(side or ""),
                    ref_price=float(ref_price),
                    timeframe=str(getattr(self.p, "averaging_levels_tf", pos.get("timeframe") or "4h") or pos.get("timeframe") or "4h"),
                    level_index=int(next_n),
                )
        except Exception:
            picked = None

        if picked is not None and picked > 0:
            level = _dec(str(picked))
            level_source = "picked_level"
        else:
            # fallback spacing from reference
            pct = _dec(str(min_dist_pct)) / _dec("100")
            if pct <= 0:
                pct = _dec("0.10")
            if side == "LONG":
                level = ref_price * (_dec("1") - pct * _dec(str(next_n)))
            else:
                level = ref_price * (_dec("1") + pct * _dec(str(next_n)))

        tick = self._price_tick_for_symbol(sym) or _dec("0")
        if tick and tick > 0:
            level = _round_to_step(level, tick, rounding=("floor" if add_side == "BUY" else "ceiling"))

        # Ensure trigger is not immediate wrt mark price
        mark = _dec("0")
        try:
            pr = self._rest_snapshot_get("position_risk") or []
            for r in pr if isinstance(pr, list) else []:
                if str(r.get("symbol") or "").upper() == sym and str(r.get("positionSide") or "").upper() == side:
                    mark = _dec(r.get("markPrice") or 0)
                    break
        except Exception:
            mark = _dec("0")

        if mark > 0 and tick and tick > 0:
            if add_side == "BUY" and level >= mark:
                level = mark - tick
            if add_side == "SELL" and level <= mark:
                level = mark + tick

        # Qty sizing
        qty_step = _dec(self._qty_step_for_symbol(sym) or "0")
        pos_qty = _dec(pos.get("qty_current") or pos.get("qty_opened") or 0)
        if pos_qty <= 0:
            return False

        add_qty, qty_source = self._calc_averaging_add_qty(pos_qty=pos_qty, next_n=next_n, sym=sym)
        if add_qty <= 0:
            return False

        hedge_mode = bool(getattr(self.p, "hedge_enabled", False))
        position_side = side if hedge_mode else None

        # Place conditional MARKET add
        params = dict(
            symbol=sym,
            side=add_side,
            type="TAKE_PROFIT_MARKET",
            stopPrice=float(level),
            quantity=float(add_qty),
            workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
            priceProtect=True,
            newClientOrderId=want_cid,
            positionSide=position_side,
        )

        log.info("[AVG] %s %s ADD%d qty=%.8f source=%s pos_qty=%.8f ref=%.8f ref_source=%s level=%.8f level_source=%s", sym, str(side).upper(), int(next_n), float(add_qty), str(qty_source), float(pos_qty), float(ref_price), str(ref_source), float(level), str(level_source))

        try:
            resp = self._binance.new_order(**params)
        except Exception as e:
            log.warning("[trade_liquidation][LIVE][AVG] failed to place ADD%d for %s: %s", int(next_n), sym, e)
            return False
        _dup_resp = bool(isinstance(resp, dict) and resp.get("_duplicate"))

        # Shadow
        try:
            if isinstance(resp, dict):
                self._upsert_order_shadow(
                    pos_uid=pos_uid,
                    order_id=str(resp.get("orderId") or resp.get("order_id") or resp.get("algoId") or ""),
                    client_order_id=want_cid,
                    symbol_id=int(pos.get("symbol_id") or 0),
                    side=add_side,
                    order_type=str(resp.get("type") or "TAKE_PROFIT_MARKET"),
                    qty=float(add_qty),
                    price=float(level),
                    reduce_only=False,
                    status=str(resp.get("status") or "NEW"),
                )
        except Exception:
            pass

        try:
            if not hasattr(self, "_recent_add_place_ts") or not isinstance(self._recent_add_place_ts, dict):
                self._recent_add_place_ts = {}
            self._recent_add_place_ts[want_cid] = time.time()
            try:
                if not hasattr(self, "_recent_add_shadow_guard") or not isinstance(self._recent_add_shadow_guard, dict):
                    self._recent_add_shadow_guard = {}
                self._recent_add_shadow_guard[want_cid] = {"ts": time.time(), "payload": _add_payload}
            except Exception:
                pass
        except Exception:
            pass
        if _dup_resp:
            return False
        log.info(
            "[trade_liquidation][LIVE][AVG] placed ADD%d %s %s stop=%.8f qty=%.8f (adds_done=%d/%d)",
            int(next_n),
            sym,
            add_side,
            float(level),
            float(add_qty),
            int(adds_done),
            int(max_adds),
        )
        return True

    def _live_cleanup_remaining_brackets(self) -> int:
        """LIVE: если SL или TP был FILLED (по order_events/order_fills),
        то отменяем второй защитный ордер на бирже (чтобы не висел).

        Источник истины закрытия: order_events/order_fills.
        """
        if not self._binance:
            return 0
        if not bool(getattr(self.p, "live_cleanup_remaining_orders", True)):
            return 0

        prefix = _sanitize_coid_prefix(str(getattr(self.p, "client_order_id_prefix", "TL") or "TL"))

        # В order_events НЕТ event_time — используем ts_ms/recv_ts
        sql = """
              WITH last_ev AS (SELECT DISTINCT \
              ON (client_order_id)
                  client_order_id,
                  order_id,
                  symbol_id,
                  status,
                  ts_ms,
                  recv_ts
              FROM order_events
              WHERE exchange_id = %(ex)s
                AND account_id = %(acc)s
                AND client_order_id IS NOT NULL
                AND (
                  client_order_id LIKE %(sl_like)s
                 OR client_order_id LIKE %(tp_like)s
                  )
              ORDER BY client_order_id, ts_ms DESC
                  )
              SELECT client_order_id, order_id, symbol_id, status, ts_ms, recv_ts
              FROM last_ev
              WHERE status = 'FILLED'
              ORDER BY ts_ms DESC LIMIT 200; \
              """

        rows = list(
            self.store.query_dict(
                sql,
                {
                    "ex": int(self.exchange_id),
                    "acc": int(self.account_id),
                    "sl_like": f"{prefix}_%_SL",
                    "tp_like": f"{prefix}_%_TP",
                },
            )
        )
        if not rows:
            return 0

        sym_rows = list(
            self.store.query_dict(
                "SELECT symbol_id, symbol FROM symbols WHERE exchange_id=%(ex)s",
                {"ex": int(self.exchange_id)},
            )
        )
        sym_map = {int(r["symbol_id"]): str(r["symbol"]) for r in sym_rows if r.get("symbol_id") is not None}

        canceled = 0

        for r in rows:
            coid = str(r.get("client_order_id") or "")
            if not coid.startswith(prefix + "_"):
                continue
            if not (coid.endswith("_SL") or coid.endswith("_TP")):
                continue

            # TL_<token>_SL  / TL_<token>_TP
            tok = coid[len(prefix) + 1: -3]

            # Resolve real pos_uid via shadow orders table (because clientOrderId stores only token)
            pos_uid = None
            try:
                rr = list(
                    self.store.query_dict(
                        """
                        SELECT pos_uid
                        FROM orders
                        WHERE exchange_id=%(ex)s
                          AND account_id=%(acc)s
                          AND client_order_id=%(coid)s
                        ORDER BY updated_at DESC
                        LIMIT 1
                        """,
                        {"ex": int(self.exchange_id), "acc": int(self.account_id), "coid": coid},
                    )
                )
                if rr and rr[0].get("pos_uid"):
                    pos_uid = str(rr[0]["pos_uid"])
            except Exception:
                pos_uid = None

            symbol_id = int(r.get("symbol_id") or 0)
            symbol = sym_map.get(symbol_id)
            if not symbol:
                continue

            other_suffix = "TP" if coid.endswith("_SL") else "SL"
            other_coid = f"{prefix}_{tok}_{other_suffix}"

            # 1) отменяем второй ордер, если он есть в openOrders
            try:
                canceled_this = False
                # If we already fetched openOrders (async snapshot), reuse it.
                open_all = self._rest_snapshot_get("open_orders_all")
                if isinstance(open_all, list):
                    open_orders = [oo for oo in open_all if str(oo.get("symbol") or "") == symbol]
                else:
                    open_orders = self._binance.open_orders(symbol=symbol)
                for oo in open_orders:
                    if str(oo.get("clientOrderId") or "") == other_coid:
                        self._binance.cancel_order(symbol=symbol, origClientOrderId=other_coid)
                        canceled += 1
                        canceled_this = True
                        log.info(
                            "[trade_liquidation][LIVE] cancel leftover %s (pos_uid=%s symbol=%s)",
                            other_suffix,
                            pos_uid,
                            symbol,
                        )
                        break

                # Binance conditional orders may live in Algo endpoints.
                # If we don't find it in openOrders, try openAlgoOrders.
                if not canceled_this:
                    try:
                        # Production-clean guard: when we cannot resolve pos_uid ownership,
                        # do not aggressively cancel algo leftovers. This prevents valid
                        # closePosition hedge/main protection from being removed by orphan
                        # cleanup races during reconcile/restart windows.
                        if pos_uid is None:
                            pass
                        else:
                            algo_open = self._binance.open_algo_orders(symbol=symbol)
                            for ao in algo_open:
                                if str(ao.get("clientAlgoId") or "") == other_coid:
                                    self._binance.cancel_algo_order(symbol=symbol, clientAlgoId=other_coid)
                                    canceled += 1
                                    canceled_this = True
                                    log.info(
                                        "[trade_liquidation][LIVE] cancel leftover(algo) %s (pos_uid=%s symbol=%s)",
                                        other_suffix,
                                        pos_uid,
                                        symbol,
                                    )
                                    break
                    except Exception:
                        pass
            except Exception:
                log.exception(
                    "[trade_liquidation][LIVE] failed to cancel leftover order (pos_uid=%s symbol=%s)",
                    pos_uid,
                    symbol,
                )

            # 2) закрываем ledger по факту FILLED (идемпотентно)
            try:
                r_fill = list(
                    self.store.query_dict(
                        """
                        SELECT price, qty, ts
                        FROM order_fills
                        WHERE exchange_id = %(ex)s
                          AND account_id = %(acc)s
                          AND order_id = %(oid)s
                        ORDER BY ts DESC LIMIT 1
                        """,
                        {
                            "ex": int(self.exchange_id),
                            "acc": int(self.account_id),
                            "oid": str(r.get("order_id") or ""),
                        },
                    )
                )
                exit_price = float(r_fill[0]["price"]) if r_fill and r_fill[0].get("price") is not None else None

                self.store.execute(
                    """
                    UPDATE position_ledger
                    SET status='CLOSED',
                        closed_at=now(),
                        exit_price=%(exit_price)s,
                        qty_closed=COALESCE(qty_closed, 0) + COALESCE(qty_current, 0),
                        qty_current=0,
                        updated_at=now()
                    WHERE exchange_id = %(ex)s
                      AND account_id = %(acc)s
                      AND pos_uid = %(pos_uid)s
                      AND status = 'OPEN'
                      AND source = 'live'
                      AND strategy_id = %(sid)s
                    """,
                    {
                        "ex": int(self.exchange_id),
                        "acc": int(self.account_id),
                        "pos_uid": str(pos_uid),
                        "exit_price": exit_price,
                        "sid": self.STRATEGY_ID,
                    },
                )
            except Exception:
                log.exception("[trade_liquidation][LIVE] failed to mark ledger CLOSED (pos_uid=%s)", pos_uid)

        return canceled

    def _live_cancel_all_orders_if_flat(self) -> int:
        """If exchange shows position is flat (positionAmt == 0) — cancel all orders.

        Covers manual closes, liquidation, reduce-only closes, etc. If the position
        is already 0 on the exchange, we should not leave TP/SL/trailing orders
        hanging around.

        Uses the current REST snapshot (open_orders_all/open_algo_orders_all/position_risk).
        Returns the number of symbols for which we attempted cleanup.
        """

        if not self._is_live:
            return 0

        pos_risk = self._rest_snapshot_get("position_risk") or []
        if not isinstance(pos_risk, list) or not pos_risk:
            return 0

        open_orders = self._rest_snapshot_get("open_orders_all") or []
        open_algos = self._rest_snapshot_get("open_algo_orders_all") or []

        # Build symbol -> counts
        oo_cnt: Dict[str, int] = {}
        for o in open_orders if isinstance(open_orders, list) else []:
            sym = str(o.get("symbol") or "").strip()
            if sym:
                oo_cnt[sym] = oo_cnt.get(sym, 0) + 1

        ao_cnt: Dict[str, int] = {}
        for o in open_algos if isinstance(open_algos, list) else []:
            sym = str(o.get("symbol") or "").strip()
            if sym:
                ao_cnt[sym] = ao_cnt.get(sym, 0) + 1

        cleaned_syms = 0
        EPS = 1e-12

        # In hedge mode, positionRisk returns one row per (symbol, positionSide).
        # A symbol can have LONG!=0 and SHORT==0 at the same time.
        # We must treat the *symbol* as open if ANY side is non-zero; otherwise we might
        # incorrectly cancel protective orders for a still-open position.
        sym_has_pos: Dict[str, bool] = {}
        for _pr in pos_risk:
            if not isinstance(_pr, dict):
                continue
            _sym = str(_pr.get('symbol') or '').strip()
            if not _sym:
                continue
            try:
                _amt = float(_pr.get('positionAmt') or 0.0)
            except Exception:
                _amt = 0.0
            if abs(_amt) > EPS:
                sym_has_pos[_sym] = True
            else:
                sym_has_pos.setdefault(_sym, False)

        for pr in pos_risk:
            if not isinstance(pr, dict):
                continue
            sym = str(pr.get("symbol") or "").strip()
            if not sym:
                continue
            # If ANY side for this symbol is non-zero, the position is still open -> do not cancel orders.
            if sym_has_pos.get(sym, False):
                continue
            try:
                pos_amt = float(pr.get("positionAmt") or 0.0)
            except Exception:
                pos_amt = 0.0
            if abs(pos_amt) > EPS:
                continue

            if oo_cnt.get(sym, 0) == 0 and ao_cnt.get(sym, 0) == 0:
                continue

            cleaned_syms += 1
            try:
                if oo_cnt.get(sym, 0) > 0:
                    self._binance.cancel_all_open_orders(sym)
                if ao_cnt.get(sym, 0) > 0:
                    self._binance.cancel_all_open_algo_orders(sym)
                self._dlog(
                    "[trade_liquidation][LIVE] flat-position cleanup: symbol=%s openOrders=%s openAlgoOrders=%s",
                    sym,
                    oo_cnt.get(sym, 0),
                    ao_cnt.get(sym, 0),
                )
            except Exception:
                log.exception("[trade_liquidation][LIVE] flat-position cleanup failed for %s", sym)

        return cleaned_syms

    def _qty_step_for_symbol(self, symbol: str):
        """Return Binance LOT_SIZE step for quantity rounding, if known."""
        f = self._symbol_filters_map.get((symbol or "").upper())
        if not f:
            return None
        v = f.get("qty_step")
        if v is None:
            return None
        try:
            return Decimal(str(v))
        except Exception:
            return None

    def _price_tick_for_symbol(self, symbol: str):
        """Return Binance PRICE_FILTER tickSize for price rounding, if known."""
        f = self._symbol_filters_map.get((symbol or "").upper())
        if not f:
            return None
        v = f.get("price_tick")
        if v is None:
            return None
        try:
            return Decimal(str(v))
        except Exception:
            return None

    def _fmt_qty(self, symbol: str, qty: Any) -> str:
        """Format quantity for REST calls.

        Binance Futures is strict about LOT_SIZE precision. We round **down** to the
        symbol's qty_step (if known) and return a fixed-point decimal string.
        """
        q = Decimal(str(_safe_float(qty, default=0.0)))
        step = self._qty_step_for_symbol(symbol) or Decimal("0")
        if step and step > 0:
            q = _round_to_step(q, step, rounding="down")
        return format(q.normalize(), "f")

    def _is_binance_max_position_error(self, exc: Any) -> bool:
        msg = str(exc or "")
        return '"code":-2027' in msg or 'Exceeded the maximum allowable position at current leverage' in msg

    def _entry_reduce_retry_settings(self) -> tuple[bool, int, float]:
        enabled = bool(getattr(self.p, "live_entry_reduce_on_max_position", True))
        steps = max(1, int(getattr(self.p, "live_entry_reduce_steps", 5) or 5))
        factor = float(getattr(self.p, "live_entry_reduce_factor", 0.85) or 0.85)
        if factor <= 0.0 or factor >= 1.0:
            factor = 0.85
        return enabled, steps, factor

    def _reduce_entry_qty_for_retry(self, symbol: str, qty: float, factor: float) -> float:
        try:
            qty_try = float(qty) * float(factor)
        except Exception:
            return 0.0

        step = self._qty_step_for_symbol(symbol)
        if step is not None:
            try:
                qty_try = _as_float(_round_to_step(Decimal(str(qty_try)), step, rounding="down"), 0.0)
            except Exception:
                qty_try = _round_qty_to_step(float(qty_try), float(step), mode="down")

        return max(0.0, float(qty_try))

    def _fmt_price(self, symbol: str, price: Any) -> str:
        """Format price for REST calls.

        Round to symbol's price_tick (if known) and return a fixed-point decimal string.
        """
        p = Decimal(str(_safe_float(price, default=0.0)))
        tick = self._price_tick_for_symbol(symbol) or Decimal("0")
        if tick and tick > 0:
            p = _round_to_step(p, tick, rounding="half_up")
        return format(p.normalize(), "f")

    def _live_cancel_symbol_orders(self, symbol: str) -> int:
        """Cancel ALL open regular+algo orders for a symbol. Returns number of canceled items (best-effort)."""
        symbol = (symbol or "").upper()
        canceled_total = 0

        # Regular open orders
        try:
            resp = self._binance.cancel_all_open_orders(symbol=symbol)
            if isinstance(resp, list):
                canceled_total += len(resp)
            elif isinstance(resp, dict) and resp:
                # Binance may return dict for single cancel responses; count as 1
                canceled_total += 1
            self._dlog("[live] canceled openOrders symbol=%s total=%s", symbol, canceled_total)
        except Exception as e:
            log.warning("[live] cancel_all_open_orders failed symbol=%s: %s", symbol, e)

        # Algo / conditional orders
        try:
            resp = self._binance.cancel_all_open_algo_orders(symbol=symbol)
            if isinstance(resp, list):
                canceled_total += len(resp)
            elif isinstance(resp, dict) and resp:
                canceled_total += 1
            self._dlog("[live] canceled openAlgoOrders symbol=%s total=%s", symbol, canceled_total)
        except Exception as e:
            log.warning("[live] cancel_all_open_algo_orders failed symbol=%s: %s", symbol, e)

        return canceled_total

    def _to_epoch_ms(v: Any) -> Optional[int]:
        if v is None:
            return None
        try:
            import datetime as _dt
            if isinstance(v, (_dt.datetime, _dt.date)):
                if isinstance(v, _dt.date) and not isinstance(v, _dt.datetime):
                    v = _dt.datetime(v.year, v.month, v.day)
                # if tz-naive, assume UTC to avoid local-time surprises
                if getattr(v, "tzinfo", None) is None:
                    v = v.replace(tzinfo=_dt.timezone.utc)
                return int(v.timestamp() * 1000)
        except Exception:
            pass
        if isinstance(v, (int, float)):
            # heuristics: seconds vs ms
            return int(v * 1000) if v < 10_000_000_000 else int(v)
        if isinstance(v, str):
            s = v.strip()
            try:
                import datetime as _dt
                # tolerate 'Z'
                if s.endswith("Z"):
                    s = s[:-1] + "+00:00"
                dt = _dt.datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=_dt.timezone.utc)
                return int(dt.timestamp() * 1000)
            except Exception:
                return None
        return None

    def _upsert_order_shadow(
        self,
        *,
        pos_uid: str,
        order_id: Any,
        client_order_id: str,
        symbol_id: int,
        side: str,
        order_type: str,
        qty: float,
        price: Optional[float],
        reduce_only: bool,
        status: str = "NEW",
    ) -> None:
        """Write/Update row in public.orders to link exchange orders with pos_uid/strategy_id.

        Multi-account safe: first UPDATE by (exchange_id, account_id, client_order_id) for active status=NEW,
        then INSERT; avoids unique violations on the partial unique index for active NEW orders.
        """
        try:
            oid = str(order_id).strip() if order_id is not None else ''
            if not oid:
                oid = f"CID:{client_order_id}"
            sql = """
            WITH upd AS (
                UPDATE orders
                SET
                    order_id = %(oid)s,
                    symbol_id = %(sym)s,
                    side = %(side)s,
                    type = %(type)s,
                    status = %(status)s,
                    qty = %(qty)s,
                    price = %(price)s,
                    reduce_only = %(reduce_only)s,
                    strategy_id = %(strategy_id)s,
                    pos_uid = %(pos_uid)s,
                    updated_at = now()
                WHERE exchange_id = %(ex)s
                  AND account_id = %(acc)s
                  AND client_order_id = %(coid)s
                  AND client_order_id IS NOT NULL
                  AND client_order_id <> ''
                RETURNING 1
            )
            INSERT INTO orders (
                exchange_id, account_id, symbol_id,
                order_id, client_order_id,
                side, type, status,
                qty, price,
                reduce_only,
                strategy_id, pos_uid,
                created_at, updated_at
            )
            SELECT
                %(ex)s, %(acc)s, %(sym)s,
                %(oid)s, %(coid)s,
                %(side)s, %(type)s, %(status)s,
                %(qty)s, %(price)s,
                %(reduce_only)s,
                %(strategy_id)s, %(pos_uid)s, 
                now(), now()
            WHERE NOT EXISTS (SELECT 1 FROM upd)
            ON CONFLICT (exchange_id, account_id, client_order_id)
            WHERE client_order_id IS NOT NULL AND client_order_id <> ''
            DO UPDATE
            SET
                order_id = EXCLUDED.order_id,
                client_order_id = EXCLUDED.client_order_id,
                symbol_id = EXCLUDED.symbol_id,
                side = EXCLUDED.side,
                type = EXCLUDED.type,
                status = EXCLUDED.status,
                qty = EXCLUDED.qty,
                price = EXCLUDED.price,
                reduce_only = EXCLUDED.reduce_only,
                strategy_id = EXCLUDED.strategy_id,
                pos_uid = EXCLUDED.pos_uid,
                updated_at = now();
            """
            self.store.execute(
                sql,
                {
                    "ex": int(self.exchange_id),
                    "acc": int(self.account_id),
                    "sym": int(symbol_id),
                    "oid": oid,
                    "coid": str(client_order_id),
                    "side": str(side),
                    "type": str(order_type),
                    "status": str(status),
                    "qty": float(qty),
                    "price": None if price is None else float(price),
                    "reduce_only": bool(reduce_only),
                    "strategy_id": str(self.STRATEGY_ID),
                    "pos_uid": str(pos_uid),
                },
            )
        except Exception:
            log.exception("[trade_liquidation][LIVE] failed to upsert orders shadow row (order_id=%s)", str(order_id))

    def _upsert_algo_order_shadow(self, resp: Any, *, pos_uid: str, strategy_id: str = "unknown") -> None:
        """Persist a single algo order response into public.algo_orders.

        Used for Binance Futures /fapi/v1/algoOrder responses (STOP_MARKET / TRAILING_STOP_MARKET, etc).
        Best-effort mapping; extra fields are kept in raw_json.
        """
        try:
            if not isinstance(resp, dict):
                return

            symbol = str(resp.get("symbol") or "").upper()
            client_algo_id = str(resp.get("clientAlgoId") or resp.get("client_algo_id") or resp.get("clientOrderId") or "").strip()
            algo_id = resp.get("algoId") or resp.get("algo_id") or resp.get("orderId") or resp.get("order_id")

            # Binance uses triggerPrice for STOP_MARKET, activatePrice for TRAILING_STOP_MARKET
            trig = resp.get("triggerPrice")
            if trig is None:
                trig = resp.get("activatePrice")
            if trig is None:
                trig = resp.get("stopPrice")
            try:
                trig_f = float(trig) if trig is not None else None
            except Exception:
                trig_f = None

            qty = resp.get("quantity")
            try:
                qty_f = float(qty) if qty is not None else None
            except Exception:
                qty_f = None

            row = {
                "exchange_id": int(self.exchange_id),
                "account_id": int(self.account_id),
                "client_algo_id": client_algo_id,
                "algo_id": str(algo_id) if algo_id is not None else None,
                "symbol": symbol,
                "side": resp.get("side"),
                "position_side": resp.get("positionSide") or resp.get("position_side"),
                "type": resp.get("type"),
                "quantity": qty_f,
                "trigger_price": trig_f,
                "working_type": resp.get("workingType") or resp.get("working_type"),
                "status": resp.get("status") or "OPEN",
                "strategy_id": strategy_id or "unknown",
                "pos_uid": pos_uid or None,
                "raw_json": resp,
            }

            # store expects list of dicts
            if hasattr(self, "store") and self.store is not None:
                self.store.upsert_algo_orders([row])
        except Exception:
            # never break the trading loop because of shadow persistence
            return

