# src/platform/traders/trade_liquidation.py
from __future__ import annotations

import json
import logging
import math
import os
import random
import time
import threading
import uuid
import hmac
import hashlib
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from concurrent.futures import ThreadPoolExecutor, Future

import requests

from src.platform.data.storage.postgres.storage import PostgreSQLStorage

log = logging.getLogger("traders.trade_liquidation")


# ==============================================================
# Small utils
# ==============================================================

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default



def _as_bool(v: Any, default: bool = False) -> bool:
    """Best-effort convert config values to bool.

    Accepts: bool, int/float (0/1), str ('true','false','1','0','yes','no','on','off').
    """
    try:
        if isinstance(v, bool):
            return v
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return bool(int(v))
        if isinstance(v, str):
            s = v.strip().lower()
            if s in {"1", "true", "yes", "y", "on"}:
                return True
            if s in {"0", "false", "no", "n", "off"}:
                return False
        return default
    except Exception:
        return default


def _round_step_down(x: float, step: float) -> float:
    """Round down to step (qty_step), to avoid 'too much precision' / 'LOT_SIZE' issues."""
    if step <= 0:
        return x
    return math.floor(x / step) * step


def _pct_to_mult(pct: float) -> float:
    return pct / 100.0


def _sig_side_to_ledger_side(sig_side: str) -> str:
    """
    signals.side: BUY/SELL
    position_ledger.side: LONG/SHORT
    """
    s = (sig_side or "").upper()
    if s == "BUY":
        return "LONG"
    if s == "SELL":
        return "SHORT"
    return ""


# ==============================================================
# Config
# ==============================================================

@dataclass
class TradeLiquidationParams:
    """Params for liquidation-signal trader.

    Supports:
      - paper: emulation (position_ledger + paper_position_risk)
      - live:  real trading on Binance USD-M Futures
    """

    # --- signal source
    exchange_id: int = 1
    account_id: Optional[int] = None
    account_name: str = "BASE"
    screener_name: str = "scr_liquidation_binance"
    allowed_timeframes: Tuple[str, ...] = ("15m", "1h")
    signal_status_new: str = "NEW"

    # --- mode
    mode: str = "paper"  # paper | live

    # --- freshness + cooldown
    max_signal_age_minutes: int = 60
    per_symbol_cooldown_minutes: int = 60

    # --- auto-expire old NEW signals
    expire_old_signals: bool = False
    expire_status: str = "EXPIRED"
    expire_max_age_minutes: Optional[int] = None  # if None -> use max_signal_age_minutes
    expire_batch_limit: int = 5000  # 0 => unlimited

    # --- balance source
    use_exchange_balance: bool = False  # live can pull balance via REST if True

    # --- position limits
    max_open_positions: int = 5
    max_position_notional_pct_wallet: float = 50.0  # anti-overload cap per position

    # --- risk / SLTP
    risk_wallet_pct: float = 1.0
    stop_loss_pct: float = 2.0
    take_profit_pct: float = 5.0
    recalc_sl_tp: bool = True

    # --- orders (LIVE)
    entry_order_type: str = "market"  # market | limit (we use market for live entry now)
    sl_order_mode: str = "stop_market"  # stop_market | stop_limit | market | limit
    tp_order_mode: str = "take_profit_market"  # take_profit_market | take_profit_limit | market | limit
    exit_order_type: str = "market"

    # bracket orders flags
    reduce_only: bool = True
    working_type: str = "MARK_PRICE"  # MARK_PRICE | CONTRACT_PRICE
    time_in_force: str = "GTC"

    # hedge-mode (Binance Dual Side)
    hedge_enabled: bool = False  # true => positionSide LONG/SHORT

    # live cleanup (after closure by FILLED SL/TP via order-events/fills)
    client_order_id_prefix: str = "TL"
    live_cleanup_remaining_orders: bool = True

    # --- Binance REST (LIVE)
    api_key_env: str = "BINANCE_API_KEY"
    api_secret_env: str = "BINANCE_API_SECRET"
    base_url: str = "https://fapi.binance.com"  # testnet: https://testnet.binancefuture.com
    recv_window_ms: int = 5000
    timeout_sec: int = 10

    # Optional: set on entry
    leverage: Optional[int] = None
    margin_type: Optional[str] = None  # ISOLATED | CROSSED

    # --- trailing
    # If mode=live and sl_order_mode=trailing_stop_market, trader will place Binance
    # TRAILING_STOP_MARKET instead of STOP_MARKET for SL.
    trailing_enabled: bool = True
    # Activation threshold (% from entry):
    #  - LONG: activationPrice = entry * (1 + trailing_activation_pct/100)
    #  - SHORT: activationPrice = entry * (1 - trailing_activation_pct/100)
    trailing_activation_pct: float = 1.2
    # Trail distance in percent for Binance callbackRate (0.1..10, where 1 = 1%)
    trailing_trail_pct: float = 0.6

    # --- averaging/levels (paper only for now)
    averaging_enabled: bool = True
    averaging_add_notional_usdt: float = 0.0
    averaging_add_pct_of_position: float = 35.0
    averaging_max_additions: int = 2
    averaging_cooldown_minutes: int = 20

    levels_timeframe: str = "4h"
    levels_lookback_bars: int = 300
    levels_method: str = "volume_profile"
    levels_max_levels: int = 20
    levels_touch_near_pct: float = 0.25

    vp_bins: int = 48
    vp_top_nodes: int = 10
    vp_smooth: int = 1
    vp_min_separation_pct: float = 0.35

    levels_pivot_left: int = 3
    levels_pivot_right: int = 3
    levels_cluster_tolerance_pct: float = 0.25

    # --- debug
    paper_mode: bool = True  # compatibility
    debug: bool = True
    debug_top: int = 10

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "TradeLiquidationParams":
        def _as_bool(x: Any, default: bool) -> bool:
            if x is None:
                return default
            if isinstance(x, bool):
                return x
            if isinstance(x, (int, float)):
                return bool(x)
            s = str(x).strip().lower()
            if s in {"1", "true", "yes", "y", "on"}:
                return True
            if s in {"0", "false", "no", "n", "off"}:
                return False
            return default

        def _as_int(x: Any, default: Optional[int]) -> Optional[int]:
            if x in (None, ""):
                return default
            try:
                return int(x)
            except Exception:
                return default

        def _as_float(x: Any, default: float) -> float:
            try:
                return float(x)
            except Exception:
                return default

        params: Dict[str, Any] = {}

        # 1) direct matches
        for k in cls.__dataclass_fields__.keys():
            if k in d:
                params[k] = d[k]

        # 2) aliases / backward compatibility
        if "paper_mode" in d and "mode" not in d:
            params["mode"] = "paper" if _as_bool(d.get("paper_mode"), True) else "live"

        # old naming for SL/TP
        if "sl_order_type" in d and "sl_order_mode" not in d:
            params["sl_order_mode"] = d.get("sl_order_type")
        if "tp_order_type" in d and "tp_order_mode" not in d:
            params["tp_order_mode"] = d.get("tp_order_type")

        # signal source aliases
        if "signal_source" in d:
            params["screener_name"] = d.get("signal_source")
        if "signals_source" in d:
            params["screener_name"] = d.get("signals_source")
        if "signals_interval" in d:
            tf = str(d.get("signals_interval") or "").strip()
            if tf:
                params["allowed_timeframes"] = (tf,)

        # allowed_timeframes list -> tuple
        atf = params.get("allowed_timeframes")
        if isinstance(atf, list):
            params["allowed_timeframes"] = tuple(str(x) for x in atf if str(x).strip())

        # mode normalization
        m = str(params.get("mode") or "paper").strip().lower()
        params["mode"] = "live" if m in {"live", "real", "trade"} else "paper"

        # types + coercions
        params["exchange_id"] = int(_as_int(params.get("exchange_id"), cls().exchange_id) or cls().exchange_id)
        params["account_id"] = _as_int(params.get("account_id"), None)

        params["max_open_positions"] = int(_as_int(params.get("max_open_positions"), cls().max_open_positions) or cls().max_open_positions)
        params["max_signal_age_minutes"] = int(_as_int(params.get("max_signal_age_minutes"), cls().max_signal_age_minutes) or cls().max_signal_age_minutes)
        params["per_symbol_cooldown_minutes"] = int(_as_int(params.get("per_symbol_cooldown_minutes"), cls().per_symbol_cooldown_minutes) or cls().per_symbol_cooldown_minutes)

        params["expire_old_signals"] = _as_bool(params.get("expire_old_signals", cls().expire_old_signals), cls().expire_old_signals)
        params["expire_batch_limit"] = int(_as_int(params.get("expire_batch_limit"), cls().expire_batch_limit) or cls().expire_batch_limit)

        exp_age = params.get("expire_max_age_minutes", cls().expire_max_age_minutes)
        if exp_age in (None, "", 0, "0"):
            params["expire_max_age_minutes"] = None
        else:
            params["expire_max_age_minutes"] = int(_as_int(exp_age, cls().max_signal_age_minutes) or cls().max_signal_age_minutes)

        params["use_exchange_balance"] = _as_bool(params.get("use_exchange_balance", cls().use_exchange_balance), cls().use_exchange_balance)
        params["recalc_sl_tp"] = _as_bool(params.get("recalc_sl_tp", cls().recalc_sl_tp), cls().recalc_sl_tp)
        params["trailing_enabled"] = _as_bool(params.get("trailing_enabled", cls().trailing_enabled), cls().trailing_enabled)
        params["averaging_enabled"] = _as_bool(params.get("averaging_enabled", cls().averaging_enabled), cls().averaging_enabled)
        params["hedge_enabled"] = _as_bool(params.get("hedge_enabled", cls().hedge_enabled), cls().hedge_enabled)
        # accept hedge_mode too
        if "hedge_mode" in d and "hedge_enabled" not in d:
            params["hedge_enabled"] = _as_bool(d.get("hedge_mode"), cls().hedge_enabled)

        params["risk_wallet_pct"] = _as_float(params.get("risk_wallet_pct", cls().risk_wallet_pct), cls().risk_wallet_pct)
        params["stop_loss_pct"] = _as_float(params.get("stop_loss_pct", cls().stop_loss_pct), cls().stop_loss_pct)
        params["take_profit_pct"] = _as_float(params.get("take_profit_pct", cls().take_profit_pct), cls().take_profit_pct)
        params["max_position_notional_pct_wallet"] = _as_float(
            params.get("max_position_notional_pct_wallet", cls().max_position_notional_pct_wallet),
            cls().max_position_notional_pct_wallet,
        )

        params["recv_window_ms"] = int(_as_int(params.get("recv_window_ms"), cls().recv_window_ms) or cls().recv_window_ms)
        params["timeout_sec"] = int(_as_int(params.get("timeout_sec"), cls().timeout_sec) or cls().timeout_sec)

        lev = params.get("leverage", cls().leverage)
        params["leverage"] = _as_int(lev, None)
        if params["leverage"] is not None and params["leverage"] <= 0:
            params["leverage"] = None

        params["debug"] = _as_bool(params.get("debug", cls().debug), cls().debug)
        params["debug_top"] = int(_as_int(params.get("debug_top"), cls().debug_top) or cls().debug_top)

        return cls(**params)


# ==============================================================
# SQL helpers
# ==============================================================

_DDL_PAPER_RISK = """
CREATE TABLE IF NOT EXISTS paper_position_risk (
    exchange_id SMALLINT NOT NULL,
    account_id SMALLINT NOT NULL,
    symbol_id  INT NOT NULL,
    pos_uid    TEXT NOT NULL,
    strategy_id TEXT NOT NULL,

    timeframe  TEXT,

    sl_price   DOUBLE PRECISION,
    tp_price   DOUBLE PRECISION,

    trailing_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    trail_activation_pct DOUBLE PRECISION,
    trail_pct DOUBLE PRECISION,
    trail_armed BOOLEAN NOT NULL DEFAULT FALSE,
    best_price DOUBLE PRECISION,

    additions_count INT NOT NULL DEFAULT 0,
    last_add_at TIMESTAMPTZ,
    levels_json JSONB,
    used_levels_json JSONB,

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (exchange_id, account_id, symbol_id)
);
"""


def _ensure_paper_tables(store: PostgreSQLStorage) -> None:
    store.exec_ddl(_DDL_PAPER_RISK)


def _has_column(store: PostgreSQLStorage, table: str, column: str) -> bool:
    q = """
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema='public'
      AND table_name=%(t)s
      AND column_name=%(c)s
    LIMIT 1;
    """
    rows = list(store.query_dict(q, {"t": table, "c": column}))
    return bool(rows)


# ==============================================================
# Binance USD-M Futures REST client (minimal)
# ==============================================================

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
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self.recv_window_ms = int(recv_window_ms)
        self.timeout_sec = int(timeout_sec)
        self.debug = bool(debug)

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


    def _sign(self, params: Dict[str, Any]) -> Dict[str, Any]:
        p = {k: v for k, v in params.items() if v is not None}
        p["timestamp"] = int(_utc_now().timestamp() * 1000)
        if self.recv_window_ms:
            p["recvWindow"] = int(self.recv_window_ms)
        query = urllib.parse.urlencode(p, doseq=True)
        sig = hmac.new(self.api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
        p["signature"] = sig
        return p

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
        p = self._sign(params) if signed else {k: v for k, v in params.items() if v is not None}

        # Use per-call timeout override if provided; otherwise use client default.
        _timeout = timeout if timeout is not None else self._timeout

        if self.debug:
            try:
                safe_p = dict(p)
                if "signature" in safe_p:
                    safe_p["signature"] = "<redacted>"
                log.info("[TL][binance_rest] %s %s signed=%s params=%s", method, path, "Y" if signed else "N", json.dumps(safe_p, ensure_ascii=False))
            except Exception:
                pass

        last_exc: Exception | None = None
        attempts = 1 + max(0, int(self.max_retries))
        for i in range(attempts):
            try:
                r = self._session.request(method, url, params=p, headers=headers, timeout=_timeout)
                if self.debug:
                    log.info("[TL][binance_rest] HTTP %s for %s", r.status_code, path)

                if r.status_code >= 400:
                    # include response text (short) for debugging
                    txt = (r.text or "").strip()
                    if len(txt) > 600:
                        txt = txt[:600] + "..."
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


        # Forced debug: show the actual REST call once we are about to send it.
        # IMPORTANT: do not print signature/api secret.
        if self.debug:
            safe_p = dict(p)
            if "signature" in safe_p:
                safe_p["signature"] = "<redacted>"
            log.info(
                "[TL][binance_rest] %s %s signed=%s params=%s",
                str(method).upper(),
                str(path),
                "Y" if signed else "N",
                json.dumps(safe_p, ensure_ascii=False, default=str)[:1200],
            )

        try:
            m = method.upper()
            if m == "GET":
                r = requests.get(url, params=p, headers=headers, timeout=self.timeout_sec)
            elif m == "POST":
                r = requests.post(url, params=p, headers=headers, timeout=self.timeout_sec)
            elif m == "DELETE":
                r = requests.delete(url, params=p, headers=headers, timeout=self.timeout_sec)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
        except Exception as e:
            raise RuntimeError(f"binance request failed: {method} {path}: {e}")

        if self.debug:
            try:
                log.info("[TL][binance_rest] HTTP %s for %s", str(r.status_code), str(path))
            except Exception:
                pass

        if r.status_code >= 400:
            try:
                j = r.json()
            except Exception:
                j = {"status": r.status_code, "text": r.text}
            raise RuntimeError(f"binance error {r.status_code}: {j}")

        return r.json()

    # ---- wrappers
    def change_leverage(self, symbol: str, leverage: int) -> Any:
        return self._request("POST", "/fapi/v1/leverage", symbol=symbol, leverage=int(leverage))

    def change_margin_type(self, symbol: str, marginType: str) -> Any:
        return self._request("POST", "/fapi/v1/marginType", symbol=symbol, marginType=str(marginType))

    def new_order(self, **kwargs: Any) -> Any:
        return self._request("POST", "/fapi/v1/order", **kwargs)

    def open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        res = self._request("GET", "/fapi/v1/openOrders", signed=True, **params)
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

    def balance(self) -> Any:
        return self._request("GET", "/fapi/v2/balance", signed=True)

    def position_risk(self) -> List[Dict[str, Any]]:
        """GET /fapi/v2/positionRisk (signed)."""
        res = self._request("GET", "/fapi/v2/positionRisk", signed=True)
        return res if isinstance(res, list) else []


# ==============================================================
# Core strategy
# ==============================================================

class TradeLiquidation:
    """Trader for scr_liquidation_binance signals (paper + live)."""

    STRATEGY_ID = "trade_liquidation"

    def __init__(self, store: PostgreSQLStorage, params: TradeLiquidationParams | Dict[str, Any]):
        self.store = store
        self.p = params if isinstance(params, TradeLiquidationParams) else TradeLiquidationParams.from_dict(params or {})

        _ensure_paper_tables(self.store)

        # resolve account_id (uses your storage helper)
        self.account_id = self.store._resolve_account_id(
            self.p.exchange_id,
            account=self.p.account_name,
            account_id=self.p.account_id,
        )

        self._pl_has_raw_meta = _has_column(self.store, "position_ledger", "raw_meta")

        self._is_live = str(self.p.mode or "paper").strip().lower() == "live"
        self._binance: Optional[BinanceUMFuturesRest] = None
        if self._is_live:
            api_key = str(os.getenv(self.p.api_key_env) or "").strip()
            api_secret = str(os.getenv(self.p.api_secret_env) or "").strip()
            if not api_key or not api_secret:
                raise RuntimeError(
                    f"LIVE mode requires env {self.p.api_key_env} and {self.p.api_secret_env} to be set"
                )
            self._binance = BinanceUMFuturesRest(
                api_key=api_key,
                api_secret=api_secret,
                base_url=str(self.p.base_url),
                recv_window_ms=int(self.p.recv_window_ms),
                timeout_sec=int(self.p.timeout_sec),
                debug=bool(self.p.debug),
                connect_timeout_sec=float(getattr(self.p, "binance_connect_timeout_sec", 3.0) or 3.0),
                read_timeout_sec=float(getattr(self.p, "binance_read_timeout_sec", self.p.timeout_sec) or self.p.timeout_sec),
                max_retries=int(getattr(self.p, "binance_max_retries", 2) or 2),
                retry_backoff_sec=float(getattr(self.p, "binance_retry_backoff_sec", 1.0) or 1.0),
            )

        # --------------------------------------------------
        # Step 4: Async REST (ThreadPool)
        # --------------------------------------------------
        self._rest_pool: Optional[ThreadPoolExecutor] = None
        self._rest_snapshot_lock = threading.Lock()
        self._rest_snapshot: Dict[str, Any] = {}
        self._rest_snapshot_ts: Optional[datetime] = None

        # cycle counter (used for reconcile/recovery throttling)
        self._cycle_n = 0

        # cached symbol map (symbol_id -> symbol)
        self._symbols_cache: Optional[Dict[int, str]] = None
        self._symbols_cache_ts: Optional[datetime] = None

        if self._is_live and self._binance is not None and bool(getattr(self.p, "async_rest_enabled", False)):
            workers = int(getattr(self.p, "async_rest_workers", 4) or 4)
            workers = max(1, min(16, workers))
            self._rest_pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="tl_rest")
            self._dlog("async REST enabled: workers=%d", workers)

        if self.p.debug:
            self._dlog("position_ledger.raw_meta present=%s", self._pl_has_raw_meta)
            self._dlog("mode=%s account_id=%s hedge=%s", self.p.mode, str(self.account_id), str(self.p.hedge_enabled))

    # ----------------------------------------------------------
    # DB helpers
    # ----------------------------------------------------------

    def _symbols_map(self, cache_ttl_sec: float = 600.0) -> Dict[int, str]:
        """Return {symbol_id: symbol} cache (DB read is cheap but keep it tidy)."""
        try:
            now = _utc_now()
            if self._symbols_cache is not None and self._symbols_cache_ts is not None:
                if 0 <= (now - self._symbols_cache_ts).total_seconds() <= float(cache_ttl_sec or 0.0):
                    return dict(self._symbols_cache)

            sym_rows = list(
                self.store.query_dict(
                    "SELECT symbol_id, symbol FROM symbols WHERE exchange_id=%(ex)s",
                    {"ex": int(self.p.exchange_id)},
                )
            )
            mp = {int(r["symbol_id"]): str(r["symbol"]) for r in sym_rows if r.get("symbol_id") is not None and r.get("symbol")}
            self._symbols_cache = dict(mp)
            self._symbols_cache_ts = now
            return mp
        except Exception:
            return {}

    def _dlog(self, msg: str, *args: Any) -> None:
        if getattr(self.p, "debug", False):
            if args:
                log.info("[TL][debug] " + msg, *args)
            else:
                log.info("[TL][debug] " + msg)

    # ----------------------------------------------------------
    # Step 4: Async REST helpers
    # ----------------------------------------------------------

    def _rest_submit(self, fn: Any, *args: Any, **kwargs: Any) -> Optional[Future]:
        """Submit a REST call into thread-pool if enabled."""
        pool = getattr(self, "_rest_pool", None)
        if pool is None:
            return None
        try:
            return pool.submit(fn, *args, **kwargs)
        except Exception:
            return None

    def _rest_snapshot_set(self, **kv: Any) -> None:
        with self._rest_snapshot_lock:
            self._rest_snapshot.update(kv)
            self._rest_snapshot_ts = _utc_now()

    def _rest_snapshot_get(self, key: str) -> Any:
        with self._rest_snapshot_lock:
            return self._rest_snapshot.get(key)

    def _is_our_position_by_uid(self, pos_uid: str, open_orders_all: Any, raw_meta: Any) -> bool:
        """Heuristic: is this position created/managed by this trader.

        Returns True if:
          - pos_uid is present in any clientOrderId of current open orders (snapshot), or
          - raw_meta contains explicit marker that this trader created the position.
        Conservative by design: if unsure, returns False.
        """
        uid = (pos_uid or "").strip()
        if not uid:
            return False

        # 1) open orders marker (preferred)
        try:
            rows = open_orders_all if isinstance(open_orders_all, list) else []
            for o in rows:
                coid = str((o or {}).get("clientOrderId") or "")
                if uid and uid in coid:
                    return True
        except Exception:
            pass

        # 2) raw_meta marker (fallback)
        try:
            meta = raw_meta
            if isinstance(meta, str) and meta.strip():
                try:
                    meta = json.loads(meta)
                except Exception:
                    meta = None
            if isinstance(meta, dict):
                # explicit strategy/trader markers
                for k in ("strategy_id", "strategy", "trader", "origin", "bot", "source"):
                    v = meta.get(k)
                    if isinstance(v, str) and v.strip().lower() in ("trade_liquidation", "tl", "trade-liquidation"):
                        return True
                # sometimes we store uid inside meta
                for k in ("pos_uid", "uid", "position_uid"):
                    v = meta.get(k)
                    if isinstance(v, str) and v.strip() == uid:
                        return True
        except Exception:
            pass

        return False


    def _maybe_refresh_rest_snapshot(self) -> None:
        """Fetch REST data in parallel (balance / positionRisk / openOrders).

        This reduces total wall time when multiple REST endpoints are needed.
        Even if async is disabled, we still populate snapshot synchronously.
        """
        if not self._is_live or self._binance is None:
            return

        need_open_orders = bool(getattr(self.p, "live_cleanup_remaining_orders", True)) or bool(getattr(self.p, "auto_recovery_enabled", False))
        need_position_risk = bool(getattr(self.p, "reconcile_enabled", False)) or float(getattr(self.p, "portfolio_cap_ratio", 0.0) or 0.0) > 0
        need_balance = bool(getattr(self.p, "use_exchange_balance", True))

        timeout_s = float(getattr(self.p, "async_rest_timeout_sec", 8.0) or 8.0)

        futs: Dict[str, Future] = {}

        # 1) openOrders (all)
        if need_open_orders:
            f = self._rest_submit(self._binance.open_orders)
            if f is not None:
                futs["open_orders_all"] = f

        # 2) positionRisk
        if need_position_risk:
            f = self._rest_submit(self._binance.position_risk)
            if f is not None:
                futs["position_risk"] = f

        # 3) balance (may be cached; this call uses poll interval)
        if need_balance:
            f = self._rest_submit(self._wallet_balance_usdt)
            if f is not None:
                futs["wallet_balance_usdt"] = f

        # If async is disabled, do sync fetches (still useful for caching/avoid duplicates).
        if not futs:
            if need_open_orders:
                try:
                    self._rest_snapshot_set(open_orders_all=self._binance.open_orders())
                except Exception:
                    log.debug("[trade_liquidation] openOrders sync fetch failed", exc_info=True)
            if need_position_risk:
                try:
                    self._rest_snapshot_set(position_risk=self._binance.position_risk())
                except Exception:
                    log.debug("[trade_liquidation] positionRisk sync fetch failed", exc_info=True)
            if need_balance:
                try:
                    self._rest_snapshot_set(wallet_balance_usdt=float(self._wallet_balance_usdt()))
                except Exception:
                    log.debug("[trade_liquidation] balance sync fetch failed", exc_info=True)
            return

        # Gather results
        out: Dict[str, Any] = {}
        for k, f in futs.items():
            try:
                out[k] = f.result(timeout=timeout_s)
            except Exception:
                log.debug("[trade_liquidation] async REST task failed: %s", k, exc_info=True)

        if out:
            self._rest_snapshot_set(**out)

    # ----------------------------------------------------------
    # Public API
    # ----------------------------------------------------------

    def run_once(self) -> Dict[str, Any]:
        started = _utc_now()

        # cycle counter
        try:
            self._cycle_n = int(getattr(self, "_cycle_n", 0) or 0) + 1
        except Exception:
            self._cycle_n = 1

        # Step 4: prefetch REST state (parallel where possible)
        # This populates in-memory snapshot for this cycle.
        self._maybe_refresh_rest_snapshot()

        # LIVE: cleanup leftover bracket order after SL/TP filled (source of truth: order_events/fills)
        canceled_leftovers = 0
        if self._is_live and bool(getattr(self.p, "live_cleanup_remaining_orders", True)):
            canceled_leftovers = int(self._live_cleanup_remaining_brackets() or 0)

        # Step 1: auto-recovery (ensure SL/TP exist) using async snapshot (openOrders/positionRisk)
        recovered = 0
        if self._is_live and bool(getattr(self.p, "auto_recovery_enabled", False)):
            recovered = int(self._auto_recovery_brackets() or 0)

        # Step 2: reconcile ledger ↔ Binance positions using async snapshot (positionRisk)
        reconcile_stats: Dict[str, int] = {}
        if self._is_live and bool(getattr(self.p, "reconcile_enabled", False)):
            reconcile_stats = dict(self._reconcile_ledger_vs_exchange() or {})

        open_positions = self._get_open_positions()
        self._dlog(
            "cycle start: open_positions=%d/%d | signal_tf=%s | sl_mode=%s tp_mode=%s | trailing=%s | avg=%s levels=%s | canceled_leftovers=%d",
            len(open_positions),
            int(self.p.max_open_positions),
            ",".join(self.p.allowed_timeframes) if self.p.allowed_timeframes else "*",
            str(self.p.sl_order_mode),
            str(self.p.tp_order_mode),
            "ON" if bool(self.p.trailing_enabled) else "OFF",
            "ON" if bool(self.p.averaging_enabled) else "OFF",
            str(self.p.levels_method),
            canceled_leftovers,
        )

        expired = 0
        if bool(getattr(self.p, "expire_old_signals", False)):
            expired = int(self._expire_old_new_signals())

        close_stats = self._process_open_positions()
        open_stats = self._process_new_signals()

        elapsed = (_utc_now() - started).total_seconds()
        out = {
            "strategy": self.STRATEGY_ID,
            "paper": not self._is_live,
            "live": self._is_live,
            "expired": expired,
            "closed": close_stats,
            "opened": open_stats,
            "canceled_leftovers": canceled_leftovers,
            "recovered": recovered,
            "reconcile": reconcile_stats,
            "elapsed_s": elapsed,
        }
        self._dlog("cycle end: closed=%s opened=%s elapsed=%.2fs", str(close_stats), str(open_stats), float(elapsed))
        return out

    # ----------------------------------------------------------
    # Step 3: portfolio cap (usedMargin / wallet)
    # ----------------------------------------------------------

    def _portfolio_used_margin_usdt(self, position_risk: Any) -> float:
        """Best-effort estimate of used margin from /fapi/v2/positionRisk.

        Binance returns per-position fields like positionInitialMargin / isolatedMargin.
        We sum absolute margins for all non-zero positions.
        """
        try:
            rows = position_risk if isinstance(position_risk, list) else []
            used = 0.0
            for it in rows:
                amt = _safe_float(it.get("positionAmt"), 0.0)
                if abs(amt) <= 0:
                    continue
                m = _safe_float(it.get("positionInitialMargin"), 0.0)
                if m <= 0:
                    m = _safe_float(it.get("isolatedMargin"), 0.0)
                if m <= 0:
                    m = _safe_float(it.get("initialMargin"), 0.0)
                used += abs(float(m))
            return float(used)
        except Exception:
            return 0.0

    def _portfolio_cap_blocked(self) -> Tuple[bool, Dict[str, float]]:
        """Return (blocked, metrics) where blocked means do not open new positions."""
        ratio = float(getattr(self.p, "portfolio_cap_ratio", 0.0) or 0.0)
        if ratio <= 0:
            return False, {"cap_ratio": ratio, "used_margin": 0.0, "wallet": 0.0, "used_over_wallet": 0.0}

        wallet = self._rest_snapshot_get("wallet_balance_usdt")
        wallet_f = _safe_float(wallet, 0.0)
        if wallet_f <= 0:
            wallet_f = float(self._wallet_balance_usdt() or 0.0)

        pr = self._rest_snapshot_get("position_risk")
        oo_all = self._rest_snapshot_get("open_orders_all")
        used = self._portfolio_used_margin_usdt(pr)
        used_over = (used / wallet_f) if wallet_f > 0 else 0.0
        blocked = bool(wallet_f > 0 and used_over >= ratio)
        return blocked, {"cap_ratio": ratio, "used_margin": float(used), "wallet": float(wallet_f), "used_over_wallet": float(used_over)}

    # ----------------------------------------------------------
    # Step 2: reconcile ledger ↔ Binance positions
    # ----------------------------------------------------------

    def _reconcile_ledger_vs_exchange(self) -> Dict[str, int]:
        """Compare OPEN ledger positions with Binance positions and fix obvious drift.

        Uses snapshot "position_risk" (prefetched in run_once).
        """
        if not self._is_live:
            return {}
        if self._binance is None:
            return {}

        every_n = int(getattr(self.p, "reconcile_every_n_cycles", 1) or 1)
        every_n = max(1, every_n)
        if int(getattr(self, "_cycle_n", 0) or 0) % every_n != 0:
            return {"skipped": 1}

        tol = float(getattr(self.p, "reconcile_qty_tolerance", 1e-8) or 1e-8)
        auto_close = bool(getattr(self.p, "reconcile_auto_close_ledger", True))
        # Optional micro-step: if exchange qty differs from ledger, adjust qty_current to exchange qty.
        # Safe by default: OFF.
        auto_adjust_qty = bool(getattr(self.p, "reconcile_auto_adjust_ledger_qty_current", False))
        adjust_only_our = bool(getattr(self.p, "reconcile_adjust_only_our_positions", True))
        ignore_external = bool(getattr(self.p, "reconcile_ignore_external_positions", True))
        ext_lvl = str(getattr(self.p, "reconcile_external_log_level", "debug") or "debug").strip().lower()
        oo_all = self._rest_snapshot_get("open_orders_all")

        sym_map = self._symbols_map()

        # 1) exchange positions
        pr = self._rest_snapshot_get("position_risk")
        rows = pr if isinstance(pr, list) else []
        ex_pos: Dict[Tuple[str, str], float] = {}
        for it in rows:
            sym = str(it.get("symbol") or "").strip()
            if not sym:
                continue
            amt = _safe_float(it.get("positionAmt"), 0.0)
            if abs(amt) <= 0:
                continue
            # Determine side
            ps = str(it.get("positionSide") or "").upper().strip()
            if ps in {"LONG", "SHORT"}:
                side = ps
            else:
                side = "LONG" if amt > 0 else "SHORT"
            ex_pos[(sym, side)] = abs(float(amt))

        # 2) ledger positions
        led = list(
            self.store.query_dict(
                """
                SELECT symbol_id, pos_uid, side, status, qty_current, opened_at
                FROM position_ledger
                WHERE exchange_id=%(ex)s
                  AND account_id=%(acc)s
                  AND strategy_id=%(sid)s
                  AND status='OPEN'
                ORDER BY opened_at DESC
                """,
                {"ex": int(self.p.exchange_id), "acc": int(self.account_id), "sid": str(self.STRATEGY_ID)},
            )
        )

        missing_on_exchange = 0
        qty_mismatch = 0
        closed_ledger = 0
        external = 0

        for p in led:
            symbol_id = int(p.get("symbol_id") or 0)
            sym = sym_map.get(symbol_id)
            if not sym:
                continue
            side = str(p.get("side") or "").upper().strip()
            if side not in {"LONG", "SHORT"}:
                continue

            led_qty = abs(_safe_float(p.get("qty_current"), 0.0))
            ex_qty = abs(_safe_float(ex_pos.get((sym, side)), 0.0))

            if ex_qty <= 0:
                missing_on_exchange += 1
                if auto_close:
                    try:
                        self.store.execute(
                            """
                            UPDATE position_ledger
                            SET status='CLOSED',
                                closed_at=now(),
                                qty_current=0,
                                updated_at=now(),
                                close_reason='reconcile_missing_on_exchange'
                            WHERE exchange_id=%(ex)s
                              AND account_id=%(acc)s
                              AND pos_uid=%(uid)s
                              AND status='OPEN'
                            """,
                            {"ex": int(self.p.exchange_id), "acc": int(self.account_id), "uid": str(p.get("pos_uid") or "")},
                        )
                        closed_ledger += 1
                    except Exception:
                        log.exception("[trade_liquidation][RECONCILE] failed to auto-close ledger pos_uid=%s", str(p.get("pos_uid") or ""))
                continue

            if abs(ex_qty - led_qty) > tol:
                qty_mismatch += 1
                if bool(getattr(self.p, "reconcile_log_diffs", True)):
                    log.warning(
                        "[trade_liquidation][RECONCILE] qty mismatch %s %s: ledger=%.8f exchange=%.8f (pos_uid=%s)",
                        sym,
                        side,
                        float(led_qty),
                        float(ex_qty),
                        str(p.get("pos_uid") or ""),
                    )

                # Micro-step: optionally auto-adjust ledger qty_current to exchange qty.
                # This is useful if partial close happened on exchange (or reduce-only orders filled)
                # and ledger qty_current became stale.
                if auto_adjust_qty and ex_qty > 0:
                    try:
                        uid = str(p.get("pos_uid") or "")
                        if adjust_only_our and not self._is_our_position_by_uid(uid, oo_all, p.get("raw_meta")):
                            log.info("[trade_liquidation][RECONCILE] skip auto-adjust qty_current for external pos_uid=%s (%s %s)", uid, sym, side)
                            continue
                        if self._pl_has_raw_meta:
                            meta = {"reconcile_adjust": {"old_qty": float(led_qty), "new_qty": float(ex_qty), "ts": _utc_now().isoformat()}}
                            self.store.execute(
                                """
                                UPDATE position_ledger
                                SET qty_current=%(q)s,
                                    updated_at=now(),
                                    raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb
                                WHERE exchange_id=%(ex)s
                                  AND account_id=%(acc)s
                                  AND pos_uid=%(uid)s
                                  AND status='OPEN'
                                """,
                                {"q": float(ex_qty), "meta": json.dumps(meta), "ex": int(self.p.exchange_id), "acc": int(self.account_id), "uid": uid},
                            )
                        else:
                            self.store.execute(
                                """
                                UPDATE position_ledger
                                SET qty_current=%(q)s,
                                    updated_at=now()
                                WHERE exchange_id=%(ex)s
                                  AND account_id=%(acc)s
                                  AND pos_uid=%(uid)s
                                  AND status='OPEN'
                                """,
                                {"q": float(ex_qty), "ex": int(self.p.exchange_id), "acc": int(self.account_id), "uid": uid},
                            )
                        log.info(
                            "[trade_liquidation][RECONCILE] adjusted ledger qty_current to exchange qty: %s %s old=%.8f new=%.8f (pos_uid=%s)",
                            sym,
                            side,
                            float(led_qty),
                            float(ex_qty),
                            uid,
                        )
                    except Exception:
                        log.exception("[trade_liquidation][RECONCILE] failed to auto-adjust ledger qty_current (pos_uid=%s)", str(p.get("pos_uid") or ""))

        # 3) external positions (on exchange, not in ledger)
        led_keys = set()
        for p in led:
            sid = int(p.get("symbol_id") or 0)
            sym = sym_map.get(sid)
            if not sym:
                continue
            side = str(p.get("side") or "").upper().strip()
            if side in {"LONG", "SHORT"}:
                led_keys.add((sym, side))

        for k in ex_pos.keys():
            if k not in led_keys:
                external += 1
                if not ignore_external:
                    log.warning("[trade_liquidation][RECONCILE] external exchange position: %s %s", k[0], k[1])
                else:
                    if ext_lvl == "info":
                        log.info("[trade_liquidation][RECONCILE] external exchange position: %s %s", k[0], k[1])
                    elif ext_lvl == "warning":
                        log.warning("[trade_liquidation][RECONCILE] external exchange position: %s %s", k[0], k[1])
                    else:
                        log.debug("[trade_liquidation][RECONCILE] external exchange position: %s %s", k[0], k[1])

        return {
            "ledger_open": int(len(led)),
            "exchange_open": int(len(ex_pos)),
            "missing_on_exchange": int(missing_on_exchange),
            "qty_mismatch": int(qty_mismatch),
            "closed_ledger": int(closed_ledger),
            "external": int(external),
        }

    # ----------------------------------------------------------
    # Step 1: auto-recovery SL/TP based on openOrders + positions
    # ----------------------------------------------------------

    def _auto_recovery_brackets(self) -> int:
        """Ensure protective orders exist for OPEN ledger positions.

        Uses snapshot "open_orders_all" + "position_risk".
        """
        if not self._is_live or self._binance is None:
            return 0

        sym_map = self._symbols_map()
        prefix = str(getattr(self.p, "client_order_id_prefix", "TL") or "TL").strip() or "TL"

        max_age_h = float(getattr(self.p, "recovery_max_age_hours", 48) or 48)
        place_sl = bool(getattr(self.p, "recovery_place_sl", True))
        place_tp = bool(getattr(self.p, "recovery_place_tp", True))
        hedge_mode = bool(getattr(self.p, "hedge_enabled", False))

        # Open orders snapshot
        open_all = self._rest_snapshot_get("open_orders_all")
        open_rows = open_all if isinstance(open_all, list) else []
        open_by_symbol: Dict[str, set[str]] = {}
        for oo in open_rows:
            sym = str(oo.get("symbol") or "").strip()
            if not sym:
                continue
            open_by_symbol.setdefault(sym, set()).add(str(oo.get("clientOrderId") or ""))

        # Exchange positions snapshot (for positionSide in hedge, and to skip already-closed positions)
        pr = self._rest_snapshot_get("position_risk")
        pr_rows = pr if isinstance(pr, list) else []
        ex_has_pos: set[Tuple[str, str]] = set()
        # Current exchange quantity by (symbol, side) from positionRisk.
        # Used to place trailing stops / qty-based TP when ledger qty_current may be stale (partial close etc.).
        ex_qty_map: Dict[Tuple[str, str], float] = {}
        for it in pr_rows:
            sym = str(it.get("symbol") or "").strip()
            if not sym:
                continue
            amt = _safe_float(it.get("positionAmt"), 0.0)
            if abs(amt) <= 0:
                continue
            ps = str(it.get("positionSide") or "").upper().strip()
            if ps in {"LONG", "SHORT"}:
                side = ps
            else:
                side = "LONG" if amt > 0 else "SHORT"
            ex_has_pos.add((sym, side))
            ex_qty_map[(sym, side)] = abs(float(amt))

        # Ledger OPEN positions
        led = list(
            self.store.query_dict(
                """
                SELECT symbol_id, pos_uid, side, status, qty_current, entry_price, opened_at
                FROM position_ledger
                WHERE exchange_id=%(ex)s
                  AND account_id=%(acc)s
                  AND strategy_id=%(sid)s
                  AND status='OPEN'
                ORDER BY opened_at DESC
                """,
                {"ex": int(self.p.exchange_id), "acc": int(self.account_id), "sid": str(self.STRATEGY_ID)},
            )
        )

        now = _utc_now()
        recovered = 0

        for p in led:
            try:
                opened_at = p.get("opened_at")
                if isinstance(opened_at, datetime):
                    age_h = (now - opened_at.replace(tzinfo=timezone.utc) if opened_at.tzinfo is None else now - opened_at.astimezone(timezone.utc)).total_seconds() / 3600.0
                    if age_h > max_age_h:
                        continue
            except Exception:
                pass

            symbol_id = int(p.get("symbol_id") or 0)
            sym = sym_map.get(symbol_id)
            if not sym:
                continue
            side = str(p.get("side") or "").upper().strip()
            if side not in {"LONG", "SHORT"}:
                continue

            # If exchange says no position - skip recovery (reconcile will handle ledger close)
            if (sym, side) not in ex_has_pos:
                continue

            # Use exchange qty for protective orders (especially important for TRAILING_STOP_MARKET and limit TP),
            # because ledger qty_current can lag after partial closes / external reduce-only fills.
            qty_ex = abs(_safe_float(ex_qty_map.get((sym, side)), 0.0))
            qty_led = abs(_safe_float(p.get("qty_current"), 0.0))
            qty_use = float(qty_ex if qty_ex > 0 else qty_led)
            if qty_use <= 0:
                continue

            pos_uid = str(p.get("pos_uid") or "").strip()
            if not pos_uid:
                continue

            cid_sl = f"{prefix}_{pos_uid}_SL"
            cid_tp = f"{prefix}_{pos_uid}_TP"

            existing = open_by_symbol.get(sym, set())
            has_sl = cid_sl in existing
            has_tp = cid_tp in existing

            # compute protective prices from ledger entry
            entry = _safe_float(p.get("entry_price"), 0.0)
            if entry <= 0:
                continue

            if side == "LONG":
                sl_price = entry * (1.0 - _pct_to_mult(self.p.stop_loss_pct))
                tp_price = entry * (1.0 + _pct_to_mult(self.p.take_profit_pct))
                close_side = "SELL"
                position_side = "LONG"
            else:
                sl_price = entry * (1.0 + _pct_to_mult(self.p.stop_loss_pct))
                tp_price = entry * (1.0 - _pct_to_mult(self.p.take_profit_pct))
                close_side = "BUY"
                position_side = "SHORT"

            # place missing orders
            if place_sl and (not has_sl) and sl_price > 0:
                try:
                    sl_mode = str(getattr(self.p, "sl_order_mode", "stop_market") or "stop_market").strip().lower()
                    if sl_mode in {"trailing_stop_market", "trailing", "tsm"} and bool(getattr(self.p, "trailing_enabled", True)):
                        callback_rate = float(getattr(self.p, "trailing_trail_pct", 0.6) or 0.6)
                        activation_pct = float(getattr(self.p, "trailing_activation_pct", 1.2) or 1.2)
                        if close_side.upper() == "SELL":
                            activation_price = entry * (1.0 + activation_pct / 100.0)
                        else:
                            activation_price = entry * (1.0 - activation_pct / 100.0)
                        params = dict(
                            symbol=sym,
                            side=close_side,
                            type="TRAILING_STOP_MARKET",
                            quantity=float(qty_use),
                            activationPrice=float(activation_price),
                            callbackRate=float(callback_rate),
                            workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                            newClientOrderId=cid_sl,
                            positionSide=position_side if hedge_mode else None,
                        )
                        if (not hedge_mode) and bool(self.p.reduce_only):
                            params["reduceOnly"] = True
                        resp = self._binance.new_order(**params)
                    else:
                        params = dict(
                            symbol=sym,
                            side=close_side,
                            type="STOP_MARKET",
                            stopPrice=float(sl_price),
                            closePosition=True,
                            workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                            newClientOrderId=cid_sl,
                            positionSide=position_side if hedge_mode else None,
                        )
                        if (not hedge_mode) and bool(self.p.reduce_only):
                            params["reduceOnly"] = True
                        resp = self._binance.new_order(**params)

                    if isinstance(resp, dict):
                        self._upsert_order_shadow(
                            pos_uid=pos_uid,
                            order_id=resp.get("orderId") or resp.get("order_id") or "",
                            client_order_id=cid_sl,
                            symbol_id=symbol_id,
                            side=close_side,
                            order_type=str(resp.get("type") or "STOP_MARKET"),
                            qty=float(qty_use),
                            price=None,
                            reduce_only=True,
                            status=str(resp.get("status") or "NEW"),
                        )
                    recovered += 1
                    log.info("[trade_liquidation][RECOVERY] placed missing SL %s %s pos_uid=%s", sym, side, pos_uid)
                except Exception:
                    log.exception("[trade_liquidation][RECOVERY] failed to place SL %s %s pos_uid=%s", sym, side, pos_uid)

            if place_tp and (not has_tp) and tp_price > 0:
                try:
                    tp_mode = str(getattr(self.p, "tp_order_mode", "take_profit_market") or "take_profit_market").strip().lower()
                    if tp_mode in {"take_profit_market", "takeprofit_market", "tp_market", "market"}:
                        params = dict(
                            symbol=sym,
                            side=close_side,
                            type="TAKE_PROFIT_MARKET",
                            stopPrice=float(tp_price),
                            closePosition=True,
                            workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                            newClientOrderId=cid_tp,
                            positionSide=position_side if hedge_mode else None,
                        )
                        if (not hedge_mode) and bool(self.p.reduce_only):
                            params["reduceOnly"] = True
                        resp = self._binance.new_order(**params)
                    else:
                        params = dict(
                            symbol=sym,
                            side=close_side,
                            type="TAKE_PROFIT",
                            timeInForce=str(getattr(self.p, "time_in_force", "GTC")),
                            quantity=float(qty_use),
                            price=float(tp_price),
                            stopPrice=float(tp_price),
                            newClientOrderId=cid_tp,
                            positionSide=position_side if hedge_mode else None,
                        )
                        if (not hedge_mode) and bool(self.p.reduce_only):
                            params["reduceOnly"] = True
                        resp = self._binance.new_order(**params)

                    if isinstance(resp, dict):
                        self._upsert_order_shadow(
                            pos_uid=pos_uid,
                            order_id=resp.get("orderId") or resp.get("order_id") or "",
                            client_order_id=cid_tp,
                            symbol_id=symbol_id,
                            side=close_side,
                            order_type=str(resp.get("type") or "TAKE_PROFIT_MARKET"),
                            qty=float(qty_use),
                            price=None,
                            reduce_only=True,
                            status=str(resp.get("status") or "NEW"),
                        )
                    recovered += 1
                    log.info("[trade_liquidation][RECOVERY] placed missing TP %s %s pos_uid=%s", sym, side, pos_uid)
                except Exception:
                    log.exception("[trade_liquidation][RECOVERY] failed to place TP %s %s pos_uid=%s", sym, side, pos_uid)

        return int(recovered)

    # ----------------------------------------------------------
    # LIVE: cleanup leftover SL/TP based on order-events/fills
    # ----------------------------------------------------------

    def _live_cleanup_remaining_brackets(self) -> int:
        """LIVE: если SL или TP был FILLED (по order_events/order_fills),
        то отменяем второй защитный ордер на бирже (чтобы не висел).

        Источник истины закрытия: order_events/order_fills.
        """
        if not self._binance:
            return 0
        if not bool(getattr(self.p, "live_cleanup_remaining_orders", True)):
            return 0

        prefix = str(getattr(self.p, "client_order_id_prefix", "TL") or "TL").strip() or "TL"

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
                    "ex": int(self.p.exchange_id),
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
                {"ex": int(self.p.exchange_id)},
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

            # TL_{pos_uid}_SL  / TL_{pos_uid}_TP
            pos_uid = coid[len(prefix) + 1: -3]

            symbol_id = int(r.get("symbol_id") or 0)
            symbol = sym_map.get(symbol_id)
            if not symbol:
                continue

            other_suffix = "TP" if coid.endswith("_SL") else "SL"
            other_coid = f"{prefix}_{pos_uid}_{other_suffix}"

            # 1) отменяем второй ордер, если он есть в openOrders
            try:
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
                        log.info(
                            "[trade_liquidation][LIVE] cancel leftover %s (pos_uid=%s symbol=%s)",
                            other_suffix,
                            pos_uid,
                            symbol,
                        )
                        break
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
                            "ex": int(self.p.exchange_id),
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
                        "ex": int(self.p.exchange_id),
                        "acc": int(self.account_id),
                        "pos_uid": str(pos_uid),
                        "exit_price": exit_price,
                        "sid": self.STRATEGY_ID,
                    },
                )
            except Exception:
                log.exception("[trade_liquidation][LIVE] failed to mark ledger CLOSED (pos_uid=%s)", pos_uid)

        return canceled

    # ----------------------------------------------------------
    # Positions
    # ----------------------------------------------------------

    def _get_open_positions(self) -> List[Dict[str, Any]]:
        q = """
        SELECT
          pl.exchange_id, pl.account_id, pl.symbol_id, pl.pos_uid, pl.side,
          pl.avg_price, pl.qty_current, pl.opened_at, pl.strategy_id,
          s.symbol,
          COALESCE(sf.qty_step, 0) AS qty_step
        FROM position_ledger pl
        JOIN symbols s ON s.symbol_id = pl.symbol_id
        LEFT JOIN symbol_filters sf
          ON sf.exchange_id = pl.exchange_id
         AND sf.symbol_id = pl.symbol_id
        WHERE pl.exchange_id=%(ex)s
          AND pl.account_id=%(acc)s
          AND pl.strategy_id=%(sid)s
          AND pl.status='OPEN'
          AND pl.source=%(src)s
        ORDER BY pl.opened_at ASC;
        """
        src = "live" if self._is_live else "paper"
        return list(
            self.store.query_dict(
                q,
                {"ex": int(self.p.exchange_id), "acc": int(self.account_id), "sid": self.STRATEGY_ID, "src": src},
            )
        )

    def _get_last_price(self, symbol_id: int, timeframe: str) -> Optional[float]:
        q = """
        SELECT close
        FROM candles
        WHERE exchange_id=%(ex)s AND symbol_id=%(sym)s AND interval=%(tf)s
        ORDER BY open_time DESC
        LIMIT 1;
        """
        rows = list(self.store.query_dict(q, {"ex": int(self.p.exchange_id), "sym": int(symbol_id), "tf": str(timeframe)}))
        if not rows:
            return None
        return _safe_float(rows[0].get("close"), default=0.0)

    # ----------------------------------------------------------
    # Risk-table helpers (paper)
    # ----------------------------------------------------------

    def _load_risk(self, symbol_id: int) -> Optional[Dict[str, Any]]:
        q = """
        SELECT *
        FROM paper_position_risk
        WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s;
        """
        rows = list(self.store.query_dict(q, {"ex": int(self.p.exchange_id), "acc": int(self.account_id), "sym": int(symbol_id)}))
        return rows[0] if rows else None

    def _update_risk(self, symbol_id: int, **kwargs: Any) -> None:
        if not kwargs:
            return
        sets = [f"{k}=%({k})s" for k in kwargs.keys()]
        kwargs["ex"] = int(self.p.exchange_id)
        kwargs["acc"] = int(self.account_id)
        kwargs["sym"] = int(symbol_id)
        q = f"""
        UPDATE paper_position_risk
        SET {', '.join(sets)}, updated_at=now()
        WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s;
        """
        self.store.execute(q, kwargs)

    def _delete_risk(self, symbol_id: int) -> None:
        self.store.execute(
            "DELETE FROM paper_position_risk WHERE exchange_id=%s AND account_id=%s AND symbol_id=%s",
            (int(self.p.exchange_id), int(self.account_id), int(symbol_id)),
        )

    # ----------------------------------------------------------
    # Close logic (paper only)
    # ----------------------------------------------------------

    def _close_position(self, pos: Dict[str, Any], exit_price: float, reason: str, timeframe: str) -> None:
        side = str(pos.get("side") or "").upper()  # LONG/SHORT
        entry = _safe_float(pos.get("avg_price"), default=0.0)
        qty = _safe_float(pos.get("qty_current"), default=0.0)
        if qty <= 0 or entry <= 0:
            return

        pnl = (exit_price - entry) * qty if side == "LONG" else (entry - exit_price) * qty

        meta = {"paper_exit": {"reason": reason, "timeframe": timeframe, "exit_order_type": self.p.exit_order_type}}

        if self._pl_has_raw_meta:
            q = """
            UPDATE position_ledger
            SET
              status='CLOSED',
              closed_at=now(),
              qty_closed=qty_current,
              qty_current=0,
              exit_price=%(exit)s,
              realized_pnl=%(pnl)s,
              updated_at=now(),
              raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb
            WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s AND pos_uid=%(uid)s;
            """
            params = {
                "exit": float(exit_price),
                "pnl": float(pnl),
                "meta": json.dumps(meta),
                "ex": int(self.p.exchange_id),
                "acc": int(self.account_id),
                "sym": int(pos["symbol_id"]),
                "uid": str(pos["pos_uid"]),
            }
        else:
            q = """
            UPDATE position_ledger
            SET
              status='CLOSED',
              closed_at=now(),
              qty_closed=qty_current,
              qty_current=0,
              exit_price=%(exit)s,
              realized_pnl=%(pnl)s,
              updated_at=now()
            WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s AND pos_uid=%(uid)s;
            """
            params = {
                "exit": float(exit_price),
                "pnl": float(pnl),
                "ex": int(self.p.exchange_id),
                "acc": int(self.account_id),
                "sym": int(pos["symbol_id"]),
                "uid": str(pos["pos_uid"]),
            }

        self.store.execute(q, params)
        self._delete_risk(int(pos["symbol_id"]))

        log.info(
            "[trade_liquidation][PAPER] CLOSE %s %s qty=%.8f entry=%.6f exit=%.6f pnl=%.4f reason=%s",
            pos.get("symbol"),
            side,
            qty,
            entry,
            exit_price,
            pnl,
            reason,
        )

    def _process_open_positions(self) -> Dict[str, int]:
        # Paper: uses candle prices + paper_position_risk
        if self._is_live:
            # LIVE close handled by order-events/fills + cleanup; no candle-based forced close here.
            return {"checked": 0, "closed": 0}

        positions = self._get_open_positions()
        closed = 0
        checked = 0

        if positions:
            names = [str(p.get("symbol") or p.get("symbol_id") or "?") for p in positions][: max(1, int(self.p.debug_top))]
            self._dlog("positions: %d -> %s", len(positions), ", ".join(names))
        else:
            self._dlog("positions: 0")

        for pos in positions:
            checked += 1
            symbol_id = int(pos["symbol_id"])
            risk = self._load_risk(symbol_id)
            if not risk:
                continue

            tf = str(risk.get("timeframe") or "15m")
            price = self._get_last_price(symbol_id, tf)
            if not price or price <= 0:
                continue

            side = str(pos.get("side") or "").upper()
            entry = _safe_float(pos.get("avg_price"), default=0.0)

            sl = _safe_float(risk.get("sl_price"), default=0.0)
            tp = _safe_float(risk.get("tp_price"), default=0.0)

            if self.p.recalc_sl_tp and entry > 0:
                if side == "LONG":
                    sl = entry * (1.0 - _pct_to_mult(self.p.stop_loss_pct))
                    tp = entry * (1.0 + _pct_to_mult(self.p.take_profit_pct))
                else:
                    sl = entry * (1.0 + _pct_to_mult(self.p.stop_loss_pct))
                    tp = entry * (1.0 - _pct_to_mult(self.p.take_profit_pct))
                self._update_risk(symbol_id, sl_price=float(sl), tp_price=float(tp))

            trail_enabled = bool(risk.get("trailing_enabled"))
            trail_armed = bool(risk.get("trail_armed"))
            best = _safe_float(risk.get("best_price"), default=0.0)

            if trail_enabled:
                if best <= 0:
                    best = price
                best = max(best, price) if side == "LONG" else min(best, price)

                act_pct = _safe_float(risk.get("trail_activation_pct"), default=self.p.trailing_activation_pct)
                tr_pct = _safe_float(risk.get("trail_pct"), default=self.p.trailing_trail_pct)

                if not trail_armed and entry > 0:
                    if side == "LONG" and price >= entry * (1.0 + _pct_to_mult(act_pct)):
                        trail_armed = True
                    if side == "SHORT" and price <= entry * (1.0 - _pct_to_mult(act_pct)):
                        trail_armed = True

                self._update_risk(symbol_id, best_price=float(best), trail_armed=bool(trail_armed))

                if trail_armed and best > 0:
                    if side == "LONG":
                        trail_stop = best * (1.0 - _pct_to_mult(tr_pct))
                        if price <= trail_stop:
                            self._close_position(pos, exit_price=price, reason="TRAIL", timeframe=tf)
                            closed += 1
                            continue
                    else:
                        trail_stop = best * (1.0 + _pct_to_mult(tr_pct))
                        if price >= trail_stop:
                            self._close_position(pos, exit_price=price, reason="TRAIL", timeframe=tf)
                            closed += 1
                            continue

            if sl > 0:
                if side == "LONG" and price <= sl:
                    self._close_position(pos, exit_price=price, reason="SL", timeframe=tf)
                    closed += 1
                    continue
                if side == "SHORT" and price >= sl:
                    self._close_position(pos, exit_price=price, reason="SL", timeframe=tf)
                    closed += 1
                    continue

            if tp > 0:
                if side == "LONG" and price >= tp:
                    self._close_position(pos, exit_price=price, reason="TP", timeframe=tf)
                    closed += 1
                    continue
                if side == "SHORT" and price <= tp:
                    self._close_position(pos, exit_price=price, reason="TP", timeframe=tf)
                    closed += 1
                    continue

        return {"checked": checked, "closed": closed}

    # ----------------------------------------------------------
    # Open logic
    # ----------------------------------------------------------

    def _wallet_balance_usdt(self) -> float:
        """Return walletBalance for USDT.

        Robustness:
        - Retries transient Binance REST issues (timeouts / TLS handshakes / temporary network).
        - Keeps a short-lived cache of the last successful wallet balance.
        - Falls back to DB snapshot if exchange fetch fails.
        """
        # local cache (in-memory) to survive transient REST issues
        if not hasattr(self, "_last_wallet_balance_usdt"):
            self._last_wallet_balance_usdt = 0.0
            self._last_wallet_balance_ts = None  # type: ignore[assignment]

        cache_ttl = float(getattr(self.p, "wallet_balance_cache_ttl_sec", 180.0) or 180.0)
        # Poll interval: even if exchange is healthy, do NOT request balance more often than this.
        # This is the main speedup knob when your loop is frequent.
        poll_sec = float(getattr(self.p, "wallet_balance_poll_sec", 10.0) or 10.0)
        now = _utc_now()

        def _cache_ok() -> bool:
            ts = getattr(self, "_last_wallet_balance_ts", None)
            if ts is None:
                return False
            try:
                age = (now - ts).total_seconds()
                return 0 <= age <= cache_ttl and float(getattr(self, "_last_wallet_balance_usdt", 0.0) or 0.0) > 0
            except Exception:
                return False

        # 0) Fast path: reuse recent cached value (even if exchange is healthy)
        ts = getattr(self, "_last_wallet_balance_ts", None)
        if ts is not None:
            try:
                age = (now - ts).total_seconds()
                if 0 <= age <= poll_sec and float(getattr(self, "_last_wallet_balance_usdt", 0.0) or 0.0) > 0:
                    return float(getattr(self, "_last_wallet_balance_usdt", 0.0) or 0.0)
            except Exception:
                pass

        # 1) Exchange (preferred)
        if bool(getattr(self.p, "use_exchange_balance", True)) and self._binance is not None:
            retries = int(getattr(self.p, "wallet_balance_retries", 2) or 2)
            backoff = float(getattr(self.p, "wallet_balance_retry_backoff_sec", 1.0) or 1.0)

            for i in range(max(1, retries + 1)):
                try:
                    rows = self._binance.balance()
                    bal = 0.0
                    if isinstance(rows, list):
                        for it in rows:
                            if str(it.get("asset") or "").upper() == "USDT":
                                # Binance Futures /fapi/v2/balance returns keys like:
                                # balance, availableBalance, crossWalletBalance, etc.
                                for k in ("walletBalance", "crossWalletBalance", "balance", "maxWithdrawAmount"):
                                    bal = _safe_float(it.get(k), default=0.0)
                                    if bal > 0:
                                        break
                                break
                    if bal > 0:
                        self._last_wallet_balance_usdt = float(bal)
                        self._last_wallet_balance_ts = now
                        return float(bal)
                    # if response is weird but request succeeded: do not instantly kill cached value
                    if _cache_ok():
                        log.warning("[trade_liquidation] exchange balance returned 0/invalid -> using cached %.2f USDT", float(self._last_wallet_balance_usdt))
                        return float(self._last_wallet_balance_usdt)
                    return 0.0

                except Exception:
                    if i < max(1, retries + 1):
                        # small exponential backoff
                        delay = backoff * (2 ** max(0, i - 1)) + (random.random() * 0.2)
                        log.warning("[trade_liquidation] exchange balance fetch failed (attempt %d/%d); retry in %.2fs", i, max(1, retries + 1), delay)
                        time.sleep(delay)
                        continue

                    log.exception("[trade_liquidation] failed to fetch exchange balance; fallback to DB")

                    if _cache_ok():
                        log.warning("[trade_liquidation] using cached wallet balance %.2f USDT (age<=%.0fs)", float(self._last_wallet_balance_usdt), cache_ttl)
                        return float(self._last_wallet_balance_usdt)

                    break  # fallback to DB below

        # 2) DB fallback (account_balance_snapshots)
        try:
            row = self.store.fetch_one(
                """
                SELECT wallet_balance AS wallet_balance_usdt
                FROM account_balance_snapshots
                WHERE exchange_id=%(ex)s AND account_id=%(acc)s
                ORDER BY ts DESC
                LIMIT 1
                """,
                {"ex": int(self.p.exchange_id), "acc": int(self.account_id)},
            )
            bal = _safe_float(row.get("wallet_balance_usdt") if isinstance(row, dict) else None, default=0.0)
            if bal > 0:
                self._last_wallet_balance_usdt = float(bal)
                self._last_wallet_balance_ts = now
            return float(bal)
        except Exception:
            log.exception("[trade_liquidation] DB wallet balance fallback failed")
            if _cache_ok():
                log.warning("[trade_liquidation] using cached wallet balance %.2f USDT (db failed)", float(self._last_wallet_balance_usdt))
                return float(self._last_wallet_balance_usdt)
            return 0.0
    def _fetch_new_signals(self, limit: int = 50) -> List[Dict[str, Any]]:
        q = """
        SELECT
          sig.id AS signal_id,
          sig.signal_ts,
          sig.side,
          sig.exchange_id,
          sig.symbol_id,
          sig.timeframe,
          sig.entry_price,
          sig.stop_loss,
          sig.take_profit,
          sym.symbol,
          COALESCE(sf.qty_step, 0) AS qty_step,
          sig.context
        FROM signals sig
        JOIN screeners sc ON sc.screener_id = sig.screener_id
        JOIN symbols sym ON sym.symbol_id = sig.symbol_id
        LEFT JOIN symbol_filters sf
          ON sf.exchange_id = sig.exchange_id
         AND sf.symbol_id = sig.symbol_id
        WHERE sig.exchange_id=%(ex)s
          AND sc.name=%(scr)s
          AND sig.status=%(st)s
          AND sig.signal_ts >= now() - (%(age)s::text || ' minutes')::interval
        ORDER BY sig.signal_ts DESC
        LIMIT %(lim)s;
        """
        return list(
            self.store.query_dict(
                q,
                {
                    "ex": int(self.p.exchange_id),
                    "scr": str(self.p.screener_name),
                    "st": str(self.p.signal_status_new),
                    "age": int(self.p.max_signal_age_minutes),
                    "lim": int(limit),
                },
            )
        )

    def _expire_old_new_signals(self) -> int:
        age = self.p.expire_max_age_minutes if self.p.expire_max_age_minutes not in (None, 0, "0") else self.p.max_signal_age_minutes
        age = int(age) if age else int(self.p.max_signal_age_minutes)
        limit = int(getattr(self.p, "expire_batch_limit", 0) or 0)
        status_exp = str(getattr(self.p, "expire_status", "EXPIRED") or "EXPIRED").strip().upper()

        q = """
        WITH cte AS (
          SELECT sig.id
          FROM signals sig
          JOIN screeners sc ON sc.screener_id = sig.screener_id
          WHERE sig.exchange_id=%(ex)s
            AND sc.name=%(scr)s
            AND sig.status=%(st)s
            AND sig.signal_ts < now() - (%(age)s::text || ' minutes')::interval
          ORDER BY sig.signal_ts ASC
          LIMIT %(lim)s
        )
        UPDATE signals s
        SET status=%(newst)s,
            updated_at=now()
        FROM cte
        WHERE s.id = cte.id
        RETURNING s.id;
        """
        rows = list(
            self.store.query_dict(
                q,
                {
                    "ex": int(self.p.exchange_id),
                    "scr": str(self.p.screener_name),
                    "st": str(self.p.signal_status_new),
                    "age": int(age),
                    "lim": int(limit) if limit > 0 else 1000000000,
                    "newst": status_exp,
                },
            )
        )
        n = len(rows)
        if n > 0:
            log.info(
                "[trade_liquidation] expired old signals: %d (age>%dmin, screener=%s, limit=%s)",
                int(n),
                int(age),
                str(self.p.screener_name),
                str(limit if limit > 0 else "ALL"),
            )
        return n

    def _mark_signal_taken(self, signal_id: int) -> None:
        self.store.execute("UPDATE signals SET status='TAKEN', updated_at=now() WHERE id=%s", (int(signal_id),))

    def _last_open_time_for_symbol(self, symbol_id: int) -> Optional[datetime]:
        q = """
        SELECT opened_at
        FROM position_ledger
        WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s
          AND strategy_id=%(sid)s AND source=%(src)s
        ORDER BY opened_at DESC
        LIMIT 1;
        """
        src = "live" if self._is_live else "paper"
        rows = list(
            self.store.query_dict(
                q,
                {"ex": int(self.p.exchange_id), "acc": int(self.account_id), "sym": int(symbol_id), "sid": self.STRATEGY_ID, "src": src},
            )
        )
        if not rows:
            return None
        v = rows[0].get("opened_at")
        return v if isinstance(v, datetime) else None

    def _is_symbol_in_cooldown(self, symbol_id: int) -> bool:
        last = self._last_open_time_for_symbol(symbol_id)
        if not last:
            return False
        mins = (_utc_now() - last.astimezone(timezone.utc)).total_seconds() / 60.0
        return mins < float(self.p.per_symbol_cooldown_minutes)

    def _process_new_signals(self) -> Dict[str, int]:
        open_positions = self._get_open_positions()
        open_by_symbol = {int(p["symbol_id"]) for p in open_positions}
        open_count = len(open_positions)

        capacity = max(0, int(self.p.max_open_positions) - open_count)
        if capacity <= 0:
            return {"capacity": 0, "considered": 0, "opened": 0, "skipped": 0}

        # Step 3: portfolio cap (LIVE): if usedMargin/wallet is too high, do NOT open new positions.
        if self._is_live:
            blocked, m = self._portfolio_cap_blocked()
            if blocked:
                log.warning(
                    "[trade_liquidation][CAP] blocked: used_margin=%.2f wallet=%.2f used/wallet=%.3f >= cap_ratio=%.3f",
                    float(m.get("used_margin", 0.0)),
                    float(m.get("wallet", 0.0)),
                    float(m.get("used_over_wallet", 0.0)),
                    float(m.get("cap_ratio", 0.0)),
                )
                return {"capacity": capacity, "considered": 0, "opened": 0, "skipped": 0, "blocked_by_cap": 1}

        # Prefer snapshot wallet balance if present (prefetched in run_once)
        wallet = _safe_float(self._rest_snapshot_get("wallet_balance_usdt"), 0.0)
        if wallet <= 0:
            wallet = self._wallet_balance_usdt()
        if wallet <= 0:
            log.warning("[trade_liquidation] wallet_balance(USDT)=0 -> skip opening")
            return {"capacity": capacity, "considered": 0, "opened": 0, "skipped": 0}

        signals = self._fetch_new_signals(limit=50)

        if self.p.debug:
            self._dlog(
                "new signals: fetched=%d capacity=%d open=%d wallet=%.2fUSDT allowed_tfs=%s",
                len(signals),
                capacity,
                open_count,
                wallet,
                list(self.p.allowed_timeframes) if self.p.allowed_timeframes else "ALL",
            )

        opened = 0
        considered = 0
        skipped = 0
        seen_symbols: set[int] = set()

        for sig in signals:
            if opened >= capacity:
                break
            considered += 1

            symbol_id = int(sig["symbol_id"])
            tf = str(sig.get("timeframe") or "")
            if tf and self.p.allowed_timeframes and tf not in self.p.allowed_timeframes:
                skipped += 1
                continue

            if symbol_id in seen_symbols:
                skipped += 1
                continue
            seen_symbols.add(symbol_id)

            if symbol_id in open_by_symbol:
                skipped += 1
                continue

            if self._is_symbol_in_cooldown(symbol_id):
                skipped += 1
                continue

            ok = self._open_from_signal(sig, wallet_balance_usdt=wallet)
            if ok:
                opened += 1
            else:
                skipped += 1

        return {"capacity": capacity, "considered": considered, "opened": opened, "skipped": skipped}

    def _open_from_signal(self, sig: Dict[str, Any], wallet_balance_usdt: float) -> bool:
        signal_id = int(sig["signal_id"])
        symbol_id = int(sig["symbol_id"])
        symbol = str(sig.get("symbol") or f"symbol_id={symbol_id}")

        sig_side = str(sig.get("side") or "").upper()  # BUY/SELL
        ledger_side = _sig_side_to_ledger_side(sig_side)  # LONG/SHORT
        if ledger_side not in ("LONG", "SHORT"):
            return False

        entry = _safe_float(sig.get("entry_price"), default=0.0)
        tf = str(sig.get("timeframe") or "15m")
        if entry <= 0:
            p = self._get_last_price(symbol_id, tf)
            entry = p or 0.0
        if entry <= 0:
            return False

        # size by risk
        risk_usdt = wallet_balance_usdt * _pct_to_mult(self.p.risk_wallet_pct)
        if risk_usdt <= 0:
            return False
        sl_pct = float(self.p.stop_loss_pct)
        if sl_pct <= 0:
            return False

        notional = risk_usdt / _pct_to_mult(sl_pct)
        cap = wallet_balance_usdt * _pct_to_mult(self.p.max_position_notional_pct_wallet)
        if cap > 0:
            notional = min(notional, cap)

        qty = notional / entry
        qty_step = _safe_float(sig.get("qty_step"), default=0.0)
        if qty_step > 0:
            qty = _round_step_down(qty, qty_step)
        if qty <= 0:
            return False

        # SL/TP price
        if self.p.recalc_sl_tp:
            if ledger_side == "LONG":
                sl_price = entry * (1.0 - _pct_to_mult(self.p.stop_loss_pct))
                tp_price = entry * (1.0 + _pct_to_mult(self.p.take_profit_pct))
            else:
                sl_price = entry * (1.0 + _pct_to_mult(self.p.stop_loss_pct))
                tp_price = entry * (1.0 - _pct_to_mult(self.p.take_profit_pct))
        else:
            sl_price = _safe_float(sig.get("stop_loss"), default=0.0)
            tp_price = _safe_float(sig.get("take_profit"), default=0.0)

        # LIVE: place real orders first
        if self._is_live:
            return self._open_live_from_signal(
                signal_id=signal_id,
                symbol=symbol,
                symbol_id=symbol_id,
                sig_side=sig_side,
                ledger_side=ledger_side,
                timeframe=tf,
                qty=float(qty),
                entry_estimate=float(entry),
                sl_price=float(sl_price) if sl_price > 0 else 0.0,
                tp_price=float(tp_price) if tp_price > 0 else 0.0,
                risk_usdt=float(risk_usdt),
                notional_usdt=float(notional),
                qty_step=float(qty_step),
            )

        # PAPER: create position_ledger + paper_position_risk
        pos_uid = str(uuid.uuid4())
        meta = {
            "paper_entry": {
                "signal_id": signal_id,
                "screener": self.p.screener_name,
                "timeframe": tf,
                "signal_side": sig_side,
                "ledger_side": ledger_side,
                "entry_order_type": self.p.entry_order_type,
                "risk_wallet_pct": self.p.risk_wallet_pct,
                "risk_usdt": risk_usdt,
                "notional_usdt": notional,
                "qty_step": qty_step,
            }
        }

        if self._pl_has_raw_meta:
            ins = """
            INSERT INTO position_ledger (
              exchange_id, account_id, symbol_id, pos_uid,
              strategy_id, source,
              side, status,
              opened_at,
              qty_opened, qty_current,
              entry_price, avg_price,
              position_value_usdt,
              scale_in_count,
              raw_meta,
              updated_at
            ) VALUES (
              %(ex)s, %(acc)s, %(sym)s, %(uid)s,
              %(sid)s, 'paper',
              %(side)s, 'OPEN',
              now(),
              %(qty)s, %(qty)s,
              %(entry)s, %(entry)s,
              %(val)s,
              0,
              %(meta)s::jsonb,
              now()
            );
            """
            params = {
                "ex": int(self.p.exchange_id),
                "acc": int(self.account_id),
                "sym": int(symbol_id),
                "uid": pos_uid,
                "sid": self.STRATEGY_ID,
                "side": ledger_side,
                "qty": float(qty),
                "entry": float(entry),
                "val": float(entry * qty),
                "meta": json.dumps(meta),
            }
        else:
            ins = """
            INSERT INTO position_ledger (
              exchange_id, account_id, symbol_id, pos_uid,
              strategy_id, source,
              side, status,
              opened_at,
              qty_opened, qty_current,
              entry_price, avg_price,
              position_value_usdt,
              scale_in_count,
              updated_at
            ) VALUES (
              %(ex)s, %(acc)s, %(sym)s, %(uid)s,
              %(sid)s, 'paper',
              %(side)s, 'OPEN',
              now(),
              %(qty)s, %(qty)s,
              %(entry)s, %(entry)s,
              %(val)s,
              0,
              now()
            );
            """
            params = {
                "ex": int(self.p.exchange_id),
                "acc": int(self.account_id),
                "sym": int(symbol_id),
                "uid": pos_uid,
                "sid": self.STRATEGY_ID,
                "side": ledger_side,
                "qty": float(qty),
                "entry": float(entry),
                "val": float(entry * qty),
            }

        try:
            self.store.execute(ins, params)
        except Exception as e:
            log.exception("[trade_liquidation][PAPER] failed to INSERT position_ledger for %s: %s", symbol, e)
            return False

        self.store.execute(
            """
            INSERT INTO paper_position_risk (
              exchange_id, account_id, symbol_id, pos_uid, strategy_id,
              timeframe,
              sl_price, tp_price,
              trailing_enabled, trail_activation_pct, trail_pct, trail_armed, best_price,
              additions_count
            ) VALUES (
              %(ex)s, %(acc)s, %(sym)s, %(uid)s, %(sid)s,
              %(tf)s,
              %(sl)s, %(tp)s,
              %(ten)s, %(tap)s, %(trp)s, %(armed)s, %(best)s,
              0
            )
            ON CONFLICT (exchange_id, account_id, symbol_id) DO UPDATE SET
              pos_uid=EXCLUDED.pos_uid,
              strategy_id=EXCLUDED.strategy_id,
              timeframe=EXCLUDED.timeframe,
              sl_price=EXCLUDED.sl_price,
              tp_price=EXCLUDED.tp_price,
              trailing_enabled=EXCLUDED.trailing_enabled,
              trail_activation_pct=EXCLUDED.trail_activation_pct,
              trail_pct=EXCLUDED.trail_pct,
              trail_armed=EXCLUDED.trail_armed,
              best_price=EXCLUDED.best_price,
              updated_at=now();
            """,
            {
                "ex": int(self.p.exchange_id),
                "acc": int(self.account_id),
                "sym": int(symbol_id),
                "uid": pos_uid,
                "sid": self.STRATEGY_ID,
                "tf": tf,
                "sl": float(sl_price) if sl_price > 0 else None,
                "tp": float(tp_price) if tp_price > 0 else None,
                "ten": bool(self.p.trailing_enabled),
                "tap": float(self.p.trailing_activation_pct),
                "trp": float(self.p.trailing_trail_pct),
                "armed": False,
                "best": float(entry),
            },
        )

        self._mark_signal_taken(signal_id)
        log.info(
            "[trade_liquidation][PAPER] OPEN %s %s(%s) qty=%.8f entry=%.6f SL=%.6f TP=%.6f (risk=%.2fUSDT notional=%.2fUSDT)",
            symbol,
            sig_side,
            ledger_side,
            qty,
            entry,
            sl_price,
            tp_price,
            risk_usdt,
            notional,
        )
        return True

    # ----------------------------------------------------------
    # LIVE execution (Binance USD-M Futures)
    # ----------------------------------------------------------

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
        """Write/Update row in public.orders to link exchange orders with pos_uid/strategy_id."""
        try:
            oid = str(order_id)
            sql = """
            INSERT INTO orders (
                exchange_id, account_id, symbol_id,
                order_id, client_order_id,
                side, type, status,
                qty, price,
                reduce_only,
                strategy_id, pos_uid,
                created_at, updated_at
            )
            VALUES (
                %(ex)s, %(acc)s, %(sym)s,
                %(oid)s, %(coid)s,
                %(side)s, %(type)s, %(status)s,
                %(qty)s, %(price)s,
                %(reduce_only)s,
                %(strategy_id)s, %(pos_uid)s,
                now(), now()
            )
            ON CONFLICT (exchange_id, account_id, order_id) DO UPDATE
            SET
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
                    "ex": int(self.p.exchange_id),
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

    def _open_live_from_signal(
        self,
        *,
        signal_id: int,
        symbol: str,
        symbol_id: int,
        sig_side: str,
        ledger_side: str,
        timeframe: str,
        qty: float,
        entry_estimate: float,
        sl_price: float,
        tp_price: float,
        risk_usdt: float,
        notional_usdt: float,
        qty_step: float,
    ) -> bool:
        """Open real position + place SL/TP (market entry)."""

        if not self._binance:
            log.error("[trade_liquidation][LIVE] Binance client not initialized (check API key/secret envs)")
            return False

        # entry direction
        entry_side = "BUY" if ledger_side == "LONG" else "SELL"
        close_side = "SELL" if entry_side == "BUY" else "BUY"

        # pos_uid is the join-key for all 3 orders + ledger
        pos_uid = str(uuid.uuid4())
        prefix = str(getattr(self.p, "client_order_id_prefix", "TL") or "TL").strip() or "TL"
        cid_entry = f"{prefix}_{pos_uid}_ENTRY"
        cid_sl = f"{prefix}_{pos_uid}_SL"
        cid_tp = f"{prefix}_{pos_uid}_TP"

        hedge_mode = bool(getattr(self.p, "hedge_enabled", False))
        position_side = "LONG" if entry_side == "BUY" else "SHORT"  # for dual-side

        # 1) optional leverage/margin settings
        try:
            if self.p.margin_type:
                try:
                    self._binance.change_margin_type(symbol=symbol, marginType=str(self.p.margin_type))
                except Exception:
                    pass
            if self.p.leverage is not None and int(self.p.leverage) > 0:
                try:
                    self._binance.change_leverage(symbol=symbol, leverage=int(self.p.leverage))
                except Exception:
                    pass
        except Exception:
            pass

        # 2) MARKET entry
        try:
            resp = self._binance.new_order(
                symbol=symbol,
                side=entry_side,
                type="MARKET",
                quantity=float(qty),
                newClientOrderId=cid_entry,
                positionSide=position_side if hedge_mode else None,
            )
        except Exception as e:
            log.exception("[trade_liquidation][LIVE] MARKET entry failed for %s: %s", symbol, e)
            return False

        # Binance response may include avgPrice or fills; if missing, fallback
        avg_price = _safe_float(resp.get("avgPrice"), default=0.0)
        if avg_price <= 0:
            avg_price = entry_estimate

        # shadow order row (entry)
        try:
            self._upsert_order_shadow(
                pos_uid=pos_uid,
                order_id=resp.get("orderId") or resp.get("order_id") or "",
                client_order_id=cid_entry,
                symbol_id=symbol_id,
                side=entry_side,
                order_type="MARKET",
                qty=float(qty),
                price=None,
                reduce_only=False,
                status=str(resp.get("status") or "NEW"),
            )
        except Exception:
            pass

        # 3) SL/TP brackets
        sl_mode = str(getattr(self.p, "sl_order_mode", "stop_market") or "stop_market").strip().lower()
        tp_mode = str(getattr(self.p, "tp_order_mode", "take_profit_market") or "take_profit_market").strip().lower()

        sl_resp: Optional[dict] = None
        tp_resp: Optional[dict] = None

        def _place_sl() -> Optional[dict]:
            if sl_price <= 0:
                return None

            # LIVE trailing stop (Binance Futures): TRAILING_STOP_MARKET
            # Note: in Hedge Mode Binance forbids reduceOnly param, so we omit it there.
            if sl_mode in {"trailing_stop_market", "trailing", "tsm"}:
                if not bool(getattr(self.p, "trailing_enabled", True)):
                    return None

                callback_rate = float(getattr(self.p, "trailing_trail_pct", 0.6) or 0.6)
                activation_pct = float(getattr(self.p, "trailing_activation_pct", 1.2) or 1.2)

                # For SELL trailing stop: activationPrice must be ABOVE current/latest price.
                # For BUY trailing stop: activationPrice must be BELOW current/latest price.
                if close_side.upper() == "SELL":
                    activation_price = avg_price * (1.0 + activation_pct / 100.0)
                else:
                    activation_price = avg_price * (1.0 - activation_pct / 100.0)

                params = dict(
                    symbol=symbol,
                    side=close_side,
                    type="TRAILING_STOP_MARKET",
                    quantity=float(qty),
                    activationPrice=float(activation_price),
                    callbackRate=float(callback_rate),
                    workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                    newClientOrderId=cid_sl,
                    positionSide=position_side if hedge_mode else None,
                )
                if (not hedge_mode) and bool(self.p.reduce_only):
                    params["reduceOnly"] = True
                return self._binance.new_order(**params)

            if sl_mode in {"stop_market", "stopmarket", "market"}:
                params = dict(
                    symbol=symbol,
                    side=close_side,
                    type="STOP_MARKET",
                    stopPrice=float(sl_price),
                    closePosition=True,
                    workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                    newClientOrderId=cid_sl,
                    positionSide=position_side if hedge_mode else None,
                )
                if (not hedge_mode) and bool(self.p.reduce_only):
                    params["reduceOnly"] = True
                return self._binance.new_order(**params)

            # stop-limit (STOP)
            params = dict(
                symbol=symbol,
                side=close_side,
                type="STOP",
                timeInForce=str(getattr(self.p, "time_in_force", "GTC")),
                quantity=float(qty),
                price=float(sl_price),
                stopPrice=float(sl_price),
                newClientOrderId=cid_sl,
                positionSide=position_side if hedge_mode else None,
            )
            if (not hedge_mode) and bool(self.p.reduce_only):
                params["reduceOnly"] = True
            return self._binance.new_order(**params)

        def _place_tp() -> Optional[dict]:
            if tp_price <= 0:
                return None

            if tp_mode in {"take_profit_market", "takeprofit_market", "tp_market", "market"}:
                params = dict(
                    symbol=symbol,
                    side=close_side,
                    type="TAKE_PROFIT_MARKET",
                    stopPrice=float(tp_price),
                    closePosition=True,
                    workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                    newClientOrderId=cid_tp,
                    positionSide=position_side if hedge_mode else None,
                )
                if (not hedge_mode) and bool(self.p.reduce_only):
                    params["reduceOnly"] = True
                return self._binance.new_order(**params)

            # tp-limit (TAKE_PROFIT)
            params = dict(
                symbol=symbol,
                side=close_side,
                type="TAKE_PROFIT",
                timeInForce=str(getattr(self.p, "time_in_force", "GTC")),
                quantity=float(qty),
                price=float(tp_price),
                stopPrice=float(tp_price),
                newClientOrderId=cid_tp,
                positionSide=position_side if hedge_mode else None,
            )
            if (not hedge_mode) and bool(self.p.reduce_only):
                params["reduceOnly"] = True
            return self._binance.new_order(**params)

        try:
            sl_resp = _place_sl()
        except Exception:
            log.exception("[trade_liquidation][LIVE] failed to place SL for %s", symbol)

        try:
            tp_resp = _place_tp()
        except Exception:
            log.exception("[trade_liquidation][LIVE] failed to place TP for %s", symbol)

        # shadow order rows (SL/TP)
        try:
            if isinstance(sl_resp, dict):
                self._upsert_order_shadow(
                    pos_uid=pos_uid,
                    order_id=sl_resp.get("orderId") or sl_resp.get("order_id") or "",
                    client_order_id=cid_sl,
                    symbol_id=symbol_id,
                    side=close_side,
                    order_type=str(sl_resp.get("type") or "STOP_MARKET"),
                    qty=float(qty),
                    price=None,
                    reduce_only=True,
                    status=str(sl_resp.get("status") or "NEW"),
                )
            if isinstance(tp_resp, dict):
                self._upsert_order_shadow(
                    pos_uid=pos_uid,
                    order_id=tp_resp.get("orderId") or tp_resp.get("order_id") or "",
                    client_order_id=cid_tp,
                    symbol_id=symbol_id,
                    side=close_side,
                    order_type=str(tp_resp.get("type") or "TAKE_PROFIT_MARKET"),
                    qty=float(qty),
                    price=None,
                    reduce_only=True,
                    status=str(tp_resp.get("status") or "NEW"),
                )
        except Exception:
            pass

        # 4) ledger row (journal)
        meta = {
            "live_entry": {
                "signal_id": int(signal_id),
                "screener": str(self.p.screener_name),
                "timeframe": str(timeframe),
                "signal_side": str(sig_side),
                "ledger_side": str(ledger_side),
                "risk_wallet_pct": float(self.p.risk_wallet_pct),
                "risk_usdt": float(risk_usdt),
                "notional_usdt": float(notional_usdt),
                "qty_step": float(qty_step),
                "entry_order": resp,
                "sl_order": sl_resp,
                "tp_order": tp_resp,
            }
        }

        cols = [
            "exchange_id",
            "account_id",
            "symbol_id",
            "pos_uid",
            "strategy_id",
            "strategy_name",
            "side",
            "status",
            "opened_at",
            "qty_opened",
            "qty_current",
            "entry_price",
            "avg_price",
            "position_value_usdt",
            "scale_in_count",
            "updated_at",
            "source",
        ]
        vals = [
            "%(ex)s",
            "%(acc)s",
            "%(sym)s",
            "%(uid)s",
            "%(sid)s",
            "%(sname)s",
            "%(side)s",
            "'OPEN'",
            "now()",
            "%(qty)s",
            "%(qty)s",
            "%(entry)s",
            "%(avg)s",
            "%(val)s",
            "0",
            "now()",
            "'live'",
        ]
        if self._pl_has_raw_meta:
            cols.insert(-2, "raw_meta")
            vals.insert(-2, "%(meta)s::jsonb")

        ins = f"INSERT INTO position_ledger ({', '.join(cols)}) VALUES ({', '.join(vals)});"
        try:
            self.store.execute(
                ins,
                {
                    "ex": int(self.p.exchange_id),
                    "acc": int(self.account_id),
                    "sym": int(symbol_id),
                    "uid": str(pos_uid),
                    "sid": self.STRATEGY_ID,
                    "sname": self.STRATEGY_ID,
                    "side": str(ledger_side),
                    "qty": float(qty),
                    "entry": float(avg_price),
                    "avg": float(avg_price),
                    "val": float(avg_price * qty),
                    "meta": json.dumps(meta),
                },
            )
        except Exception:
            log.exception("[trade_liquidation][LIVE] failed to INSERT position_ledger for %s", symbol)

        # mark signal taken
        self._mark_signal_taken(int(signal_id))

        log.info(
            "[trade_liquidation][LIVE] OPEN %s %s qty=%.8f avg=%.6f SL=%.6f TP=%.6f (sl=%s tp=%s hedge=%s pos_uid=%s)",
            symbol,
            ledger_side,
            float(qty),
            float(avg_price),
            float(sl_price),
            float(tp_price),
            str(sl_mode),
            str(tp_mode),
            str(hedge_mode),
            str(pos_uid),
        )
        return True