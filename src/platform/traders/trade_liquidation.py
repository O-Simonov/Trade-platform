# src/platform/traders/trade_liquidation.py
from __future__ import annotations

import json
import logging
import math
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR, ROUND_HALF_UP, ROUND_UP, InvalidOperation
import os
import re
import random
import time
import threading
import uuid
import hmac
import hashlib
import urllib.parse
from dataclasses import dataclass, field
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


# -------------------------
# precision helpers (Binance)
# -------------------------

def _dec(x: object) -> Decimal:
    try:
        if isinstance(x, Decimal):
            return x
        return Decimal(str(x))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")

def _round_to_step(value: object, step: object, *, rounding="down") -> Decimal:
    """Round `value` to an exchange `step` using a chosen rounding mode.

    `rounding` may be either:
      - a Decimal rounding constant (e.g. ROUND_FLOOR, ROUND_CEILING, ...)
      - a string alias: 'down'/'floor', 'up'/'ceil', 'toward_zero', 'away_zero', 'half_up'
    """
    v = Decimal(str(value))
    s = Decimal(str(step))
    if s == 0:
        return v

    # Allow simple string aliases to avoid leaking Decimal rounding constants across the codebase.
    if isinstance(rounding, str):
        r = rounding.strip().lower()
        if r in ("down", "floor"):
            rounding = ROUND_FLOOR
        elif r in ("up", "ceil", "ceiling"):
            rounding = ROUND_CEILING
        elif r in ("toward_zero", "trunc", "truncate"):
            rounding = ROUND_DOWN
        elif r in ("away_zero",):
            rounding = ROUND_UP
        elif r in ("half_up", "nearest", "round"):
            rounding = ROUND_HALF_UP
        else:
            raise ValueError(f"Unsupported rounding alias: {rounding!r}")

    q = (v / s).to_integral_value(rounding=rounding)
    return q * s


def _round_qty_to_step(qty: float, step: float, mode: str = "down") -> float:
    """Round quantity to exchange LOT_SIZE step. mode: 'down' or 'up'."""
    from decimal import Decimal, ROUND_DOWN, ROUND_UP
    q = Decimal(str(qty))
    s = Decimal(str(step))
    rounding = ROUND_DOWN if str(mode).lower() == "down" else ROUND_UP
    return float((q / s).to_integral_value(rounding=rounding) * s)


def _as_float_clean(d: Decimal) -> float:
    # avoid floats like 55.300000000000004 by going through str
    return float(format(d.normalize(), "f"))


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _ensure_tp_trail_side(level: float, entry: float, tick: float, pos_side: str, *, kind: str) -> float:
    """Ensure TP / trailing activation are on the correct side of the *actual* entry.

    For LONG: level must be > entry.
    For SHORT: level must be < entry.
    If violated, we move it by at least 1 tick.
    """
    try:
        level = float(level)
        entry = float(entry)
        tick = float(tick)
    except Exception:
        return level

    if level <= 0 or entry <= 0 or tick <= 0:
        return level

    s = str(pos_side or "").upper()
    if s == "LONG":
        if level <= entry:
            return entry + tick
    elif s == "SHORT":
        if level >= entry:
            return entry - tick
    return level


# -------------------------
# Binance clientOrderId helpers
# Binance requires: <= 36 chars and [0-9A-Za-z_-]
# We keep deterministic ids based on pos_uid so recovery can recreate them.
# -------------------------

_COID_SAN_RE = re.compile(r"[^0-9A-Za-z_-]+")

def _sanitize_coid_prefix(prefix: str) -> str:
    p = (prefix or "TL").strip()
    p = _COID_SAN_RE.sub("", p)
    if not p:
        p = "TL"
    # keep it short so we always fit into 36 chars with suffixes
    return p[:6]

def _coid_token(pos_uid: str, n: int = 20) -> str:
    try:
        b = (pos_uid or "").encode("utf-8")
    except Exception:
        b = str(pos_uid).encode("utf-8", errors="ignore")
    return hashlib.sha1(b).hexdigest()[: max(8, int(n))]



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


def _round_price_to_tick(price: float, tick: float, *, mode: str = "nearest") -> float:
    """Round price to tick size.

    mode:
      - 'down'    : floor to tick
      - 'up'      : ceil to tick
      - 'nearest' : round to nearest tick
    """
    if tick <= 0:
        return float(price)
    p = float(price)
    t = float(tick)
    if t <= 0:
        return p
    k = p / t
    if mode == "down":
        return math.floor(k) * t
    if mode == "up":
        return math.ceil(k) * t
    return round(k) * t


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

    # Safety buffer to prevent "Order would immediately trigger" on Binance.
    trailing_activation_buffer_pct: float = 0.20
    trailing_place_retries: int = 5

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

    # --- backfill (CLOSED rows enrichment from exchange trades)
    backfill_run_on_startup: bool = True
    backfill_enabled: bool = True
    backfill_interval_sec: int = 600
    backfill_lookback_days: int = 14
    backfill_batch_limit: int = 50
    backfill_fill_fees: bool = True
    backfill_overwrite: bool = False  # If True, refresh CLOSED even if already backfilled
    # --- store unknown config keys here (so getattr(self, key) works)
    extras: Dict[str, Any] = field(default_factory=dict, repr=False)

    def __getattr__(self, name: str) -> Any:
        ex = self.__dict__.get('extras')
        if isinstance(ex, dict) and name in ex:
            return ex[name]
        raise AttributeError(name)


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
        # 3) store unknown keys so strategy can read them via getattr
        known = set(cls.__dataclass_fields__.keys())
        extras = {k: v for k, v in (d or {}).items() if k not in known}
        if extras:
            params['extras'] = extras

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


    def _sign(self, params: Dict[str, Any]) -> Dict[str, Any]:
        p = {k: v for k, v in params.items() if v is not None}
        p["timestamp"] = int(_utc_now().timestamp() * 1000)
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
        p = self._sign(raw) if signed else raw

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

        return self._request("POST", "/fapi/v1/algoOrder", **p)

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


# ==============================================================
# Core strategy
# ==============================================================

class TradeLiquidation:
    """Trader for scr_liquidation_binance signals (paper + live)."""

    STRATEGY_ID = "trade_liquidation"

    def __init__(self, store: PostgreSQLStorage, params: TradeLiquidationParams | Dict[str, Any]):
        self.store = store

        # cache symbol precision filters from DB (price_tick / qty_step)
        self.p = params if isinstance(params, TradeLiquidationParams) else TradeLiquidationParams.from_dict(params or {})
        self._symbol_filters_map: Dict[str, Dict[str, Any]] = self._load_symbol_filters_map()

        # --------------------------------------------------
        # Backward-compatible aliases (some legacy methods use old names)
        # --------------------------------------------------
        self.exchange_id = int(getattr(self.p, "exchange_id", 1) or 1)
        self.params = self.p
        self.mode = getattr(self.p, "mode", "paper")
        self.strategy_id = self.STRATEGY_ID


        _ensure_paper_tables(self.store)

        # resolve account_id (uses your storage helper)
        self.account_id = self.store._resolve_account_id(
            self.p.exchange_id,
            account=self.p.account_name,
            account_id=self.p.account_id,
        )

        self._pl_has_raw_meta = _has_column(self.store, "position_ledger", "raw_meta")

        self._is_live = str(self.p.mode or "paper").strip().lower() == "live"
        # The Binance USD-M Futures REST client instance.
        # (Some helper code paths historically referenced "_binance_rest"; we keep
        # a compatible alias below.)
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
                # let REST client round price/qty using DB filters (best-effort)
                symbol_filters_resolver=lambda _sym: self._symbol_filters_map.get(str(_sym or "").upper()),
            )

        # Backward compatible alias used by some newer helper functions.
        # Prefer self._binance everywhere.
        self._binance_rest = self._binance

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

    def _load_symbol_filters_map(self) -> Dict[str, Dict[str, Any]]:
        """Load per-symbol precision/limits from DB table public.symbol_filters.

        Expected schema (confirmed in your DB):
          exchange_id, symbol_id, price_tick, qty_step, min_qty, max_qty, min_notional, max_leverage, margin_type, updated_at

        Returns dict keyed by symbol name (e.g. 'BTCUSDT') with raw DB values (Decimal/None).
        """
        ex_id = int(getattr(self.p, "exchange_id", 1) or 1)

        sql = """
        SELECT
          s.symbol AS symbol,
          sf.symbol_id AS symbol_id,
          sf.price_tick,
          sf.qty_step,
          sf.min_qty,
          sf.max_qty,
          sf.min_notional,
          sf.max_leverage,
          sf.margin_type,
          sf.updated_at
        FROM public.symbol_filters sf
        JOIN public.symbols s
          ON s.exchange_id = sf.exchange_id
         AND s.symbol_id   = sf.symbol_id
        WHERE sf.exchange_id = %(ex_id)s
        """

        try:
            rows = self.store.query_dict(sql, {"ex_id": ex_id}) or []
        except Exception as e:
            log.warning("[TL] failed to load symbol_filters from DB: %s", e)
            return {}

        out: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            sym = (r.get("symbol") or "").strip()
            if not sym:
                continue
            out[sym] = dict(r)

        log.info("[TL] loaded symbol_filters rows=%s exchange_id=%s", len(out), ex_id)
        return out

    def _dlog(self, msg: str, *args: Any) -> None:
        if getattr(self.p, "debug", False):
            if args:
                log.info("[TL][debug] " + msg, *args)
            else:
                log.info("[TL][debug] " + msg)

    # ----------------------------------------------------------
    # Step 4: Async REST helpers
    # ----------------------------------------------------------


    def _ilog(self, msg: str, *args: Any) -> None:
        """Info-log helper used by some reconciliation/backfill paths.

        Some builds historically referenced self._ilog; to avoid AttributeError we
        provide a thin wrapper around the module logger.
        """
        try:
            if args:
                log.info(msg, *args)
            else:
                log.info(msg)
        except Exception:
            # Never let logging break trading loop
            pass
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


    
    def _get_mark_price(self, symbol: str, position_side: Optional[str] = None) -> float:
        """Compat wrapper: always return a mark price even in hedge mode.

        Binance premiumIndex is per-symbol; for mark we should *not* require positionSide.
        """
        try:
            return float(self._get_mark_price_live(symbol, None))
        except Exception:
            return 0.0

    def _get_mark_price_live(self, symbol: str, position_side: Optional[str] = None) -> float:
        """Best-effort current markPrice for a symbol (LIVE).

        Uses prefetched REST snapshot (position_risk) when available; falls back to direct REST call.
        """
        if not self._is_live or self._binance is None:
            return 0.0
        sym = (symbol or "").upper().strip()
        if not sym:
            return 0.0
        ps_need = (position_side or "").upper().strip() if position_side else ""

        rows = self._rest_snapshot_get("position_risk")
        if not isinstance(rows, list):
            try:
                rows = self._binance.position_risk()
            except Exception:
                rows = []
        for it in rows or []:
            try:
                if str(it.get("symbol") or "").upper().strip() != sym:
                    continue
                ps = str(it.get("positionSide") or "").upper().strip()
                if ps_need and ps and ps != ps_need:
                    continue
                mp = _safe_float(it.get("markPrice"), 0.0)
                if mp > 0:
                    return float(mp)
            except Exception:
                continue
        return 0.0

    @staticmethod
    def _safe_price_above_mark(price: float, mark: float, tick: float, buffer_pct: float) -> float:
        """Force price to be ABOVE current mark by buffer_pct (percent)."""
        p = float(price or 0.0)
        m = float(mark or 0.0)
        buf = max(0.0, float(buffer_pct or 0.0) / 100.0)
        if m > 0 and buf > 0:
            p = max(p, m * (1.0 + buf))
        if tick and tick > 0:
            p = _round_price_to_tick(p, float(tick), mode="up")
        return float(p)

    @staticmethod
    @staticmethod
    def _safe_price_below_mark(price: float, mark: float, tick: float, buffer_pct: float) -> float:
        """Force price to be BELOW current mark by buffer_pct (percent)."""
        p = float(price or 0.0)
        m = float(mark or 0.0)
        buf = max(0.0, float(buffer_pct or 0.0) / 100.0)
        if m > 0 and buf > 0:
            p = min(p, m * (1.0 - buf))
        if tick and tick > 0:
            p = _round_price_to_tick(p, float(tick), mode="down")
        return float(p)

    @staticmethod
    def _is_immediate_trigger_error(e: Exception) -> bool:
        s = str(e) if e is not None else ""
        s_l = s.lower()
        return ("immediately trigger" in s_l) or ("\"code\":-2021" in s_l) or ("code=-2021" in s_l)

    def _is_our_position_by_uid(self, pos_uid: str, open_orders_all: Any, raw_meta: Any) -> bool:
        """Heuristic: is this position created/managed by this trader.

        Returns True if:
          - pos_uid is present in any clientOrderId of current open orders (snapshot), or
          - raw_meta contains explicit marker that this trader created the position.
        Conservative by design: if unsure, returns False.
        """
        pos_uid = (pos_uid or "").strip()
        if not pos_uid:
            return False

        # 1) open orders marker (preferred)
        try:
            rows = open_orders_all if isinstance(open_orders_all, list) else []
            for o in rows:
                coid = str((o or {}).get("clientOrderId") or "")
                if pos_uid and pos_uid in coid:
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
                # sometimes we store pos_uid inside meta
                for k in ("pos_uid", "pos_uid", "position_uid"):
                    v = meta.get(k)
                    if isinstance(v, str) and v.strip() == pos_uid:
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

            # 1b) openAlgoOrders (all) — conditional orders live here
            f2 = self._rest_submit(self._binance.open_algo_orders)
            if f2 is not None:
                futs["open_algo_orders_all"] = f2

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
                    self._rest_snapshot_set(
                        open_orders_all=self._binance.open_orders(),
                        open_algo_orders_all=self._binance.open_algo_orders(),
                    )
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

        # LIVE: hard cleanup — if positionAmt==0 on exchange, cancel *all* orders for symbol
        # (manual close / liquidation / reduce-only close / etc.)
        if self._is_live and bool(getattr(self.p, "live_cancel_orders_when_flat", True)):
            canceled_leftovers += int(self._live_cancel_all_orders_if_flat() or 0)

        # Step 1: auto-recovery (ensure SL/TP exist) using async snapshot (openOrders/positionRisk)
        recovered = 0

        def _tl_safe_trigger_price(*, symbol: str, close_side: str, desired: float, mark: float, tick: float, buffer_pct: float) -> float:
            """Ensure conditional trigger price won't immediately trigger.

            For close_side=SELL (closing LONG): trigger/activation must be ABOVE current mark.
            For close_side=BUY  (closing SHORT): trigger/activation must be BELOW current mark.
            """
            p = float(desired or 0.0)
            m = float(mark or 0.0)
            buf = float(buffer_pct or 0.0) / 100.0
            if m > 0:
                if str(close_side).upper() == "SELL":
                    # must be above mark
                    floor = m * (1.0 + max(buf, 0.0005))
                    p = max(p, floor)
                    # round UP so it stays above
                    if tick and tick > 0:
                        p = _round_price_to_tick(p, tick, mode="up")
                else:
                    # BUY: must be below mark
                    ceil = m * (1.0 - max(buf, 0.0005))
                    p = min(p if p > 0 else ceil, ceil)
                    # round DOWN so it stays below
                    if tick and tick > 0:
                        p = _round_price_to_tick(p, tick, mode="down")
            else:
                # no mark -> just tick rounding in the safe direction
                if tick and tick > 0:
                    p = _round_price_to_tick(p, tick, mode="up" if str(close_side).upper() == "SELL" else "down")
            return float(p)

        def _tl_place_algo_with_retry(place_fn, *, max_tries: int = 3):
            """Retry placing conditional orders when Binance says 'would immediately trigger' (-2021)."""
            last = None
            for i in range(int(max_tries or 1)):
                try:
                    return place_fn(i)
                except Exception as e:
                    last = e
                    s = str(e)
                    if ("-2021" in s) or ("immediately trigger" in s.lower()):
                        continue
                    raise
            if last:
                raise last
            return None

        if self._is_live and bool(getattr(self.p, "auto_recovery_enabled", False)):
            recovered = int(self._auto_recovery_brackets() or 0)

        # Step 2: reconcile ledger ↔ Binance positions using async snapshot (positionRisk)
        reconcile_stats: Dict[str, int] = {}
        if self._is_live and bool(getattr(self.p, "reconcile_enabled", False)):
            reconcile_stats = dict(self._reconcile_ledger_vs_exchange() or {})

        # Step 2.5: backfill exit_price/realized_pnl for already CLOSED ledger rows (best-effort, rate-limited)
        if self._is_live and bool(getattr(self.p, "backfill_enabled", True)):
            try:
                # Run once immediately on startup (then continue on interval inside the function).
                if (not bool(getattr(self, "_backfill_startup_done", False))
                        and bool(getattr(self.p, "backfill_run_on_startup", True))):
                    self._backfill_closed_exit_pnl(force=True, log_empty=True)
                    self._backfill_startup_done = True
                else:
                    self._backfill_closed_exit_pnl()
            except Exception:
                log.debug("[trade_liquidation] backfill_closed_exit_pnl failed", exc_info=True)

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
                SELECT symbol_id, pos_uid, side, status, qty_current, entry_price, avg_price, raw_meta, opened_at
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

            if self._is_flat_qty(sym, ex_qty):
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
                                raw_meta = jsonb_set(
                                    COALESCE(raw_meta, '{}'::jsonb),
                                    '{close_reason}',
                                    to_jsonb(%(reason)s::text),
                                    true
                                )
                            WHERE exchange_id=%(exchange_id)s
                              AND account_id=%(account_id)s
                              AND pos_uid=%(pos_uid)s
                            """,
                            {
                                "exchange_id": ex_id,
                                "account_id": acc_id,
                                "pos_uid": pos_uid,
                                "reason": "reconcile_missing_on_exchange",
                            },
                        )
                        closed_ledger += 1
                        try:
                            c = self._live_cancel_symbol_orders(sym)
                            if c:
                                self._dlog("reconcile: flat on exchange -> canceled %s orders for %s", c, sym)
                        except Exception as _e:
                            self._dlog("reconcile: failed to cancel orders for %s: %s", sym, _e)
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
                        pos_uid = str(p.get("pos_uid") or "")
                        if adjust_only_our and not self._is_our_position_by_uid(pos_uid, oo_all, p.get("raw_meta")):
                            log.info("[trade_liquidation][RECONCILE] skip auto-adjust qty_current for external pos_uid=%s (%s %s)", pos_uid, sym, side)
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
                                  AND pos_uid=%(pos_uid)s
                                  AND status='OPEN'
                                """,
                                {"q": float(ex_qty), "meta": json.dumps(meta), "ex": int(self.p.exchange_id), "acc": int(self.account_id), "pos_uid": pos_uid},
                            )
                        else:
                            self.store.execute(
                                """
                                UPDATE position_ledger
                                SET qty_current=%(q)s,
                                    updated_at=now()
                                WHERE exchange_id=%(ex)s
                                  AND account_id=%(acc)s
                                  AND pos_uid=%(pos_uid)s
                                  AND status='OPEN'
                                """,
                                {"q": float(ex_qty), "ex": int(self.p.exchange_id), "acc": int(self.account_id), "pos_uid": pos_uid},
                            )
                        log.info(
                            "[trade_liquidation][RECONCILE] adjusted ledger qty_current to exchange qty: %s %s old=%.8f new=%.8f (pos_uid=%s)",
                            sym,
                            side,
                            float(led_qty),
                            float(ex_qty),
                            pos_uid,
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
        prefix = _sanitize_coid_prefix(str(getattr(self.p, "client_order_id_prefix", "TL") or "TL"))

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

        # Algo open orders snapshot (conditional orders live here)
        open_algo_all = self._rest_snapshot_get("open_algo_orders_all")
        open_algo_rows = open_algo_all if isinstance(open_algo_all, list) else []
        open_algo_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
        for ao in open_algo_rows:
            sym = str((ao or {}).get("symbol") or "").strip()
            if not sym:
                continue
            open_algo_by_symbol.setdefault(sym, []).append(dict(ao))

        # Algo (conditional) orders snapshot: SL/TP/trailing live here
        algo_all = self._rest_snapshot_get("open_algo_orders_all")
        algo_rows = algo_all if isinstance(algo_all, list) else []
        for ao in algo_rows:
            sym = str(ao.get("symbol") or "").strip()
            if not sym:
                continue
            # Binance returns clientAlgoId for algo orders
            open_by_symbol.setdefault(sym, set()).add(str(ao.get("clientAlgoId") or ""))

        # Exchange positions snapshot (for positionSide in hedge, and to skip already-closed positions)
        pr = self._rest_snapshot_get("position_risk")
        pr_rows = pr if isinstance(pr, list) else []
        ex_has_pos: set[Tuple[str, str]] = set()
        # Current exchange quantity by (symbol, side) from positionRisk.
        # Used to place trailing stops / qty-based TP when ledger qty_current may be stale (partial close etc.).
        ex_qty_map: Dict[Tuple[str, str], float] = {}
        ex_mark_map: Dict[Tuple[str, str], float] = {}
        ex_entry_map: Dict[Tuple[str, str], float] = {}
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
            try:
                ex_mark_map[(sym, side)] = abs(float(it.get('markPrice') or 0.0))
            except Exception:
                pass
            try:
                ex_entry_map[(sym, side)] = abs(float(it.get('entryPrice') or it.get('avgEntryPrice') or it.get('avgPrice') or 0.0))
            except Exception:
                pass

        # Ledger OPEN positions
        led = list(
            self.store.query_dict(
                """
            SELECT symbol_id, pos_uid, side, status, qty_current, entry_price, avg_price, raw_meta, opened_at
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

        def _tl_safe_trigger_price(*, symbol: str, close_side: str, desired: float, mark: float, tick: float, buffer_pct: float) -> float:
            """Ensure conditional trigger price won't immediately trigger.

            For close_side=SELL (closing LONG): trigger/activation must be ABOVE current mark.
            For close_side=BUY  (closing SHORT): trigger/activation must be BELOW current mark.
            """
            p = float(desired or 0.0)
            m = float(mark or 0.0)
            buf = float(buffer_pct or 0.0) / 100.0
            if m > 0:
                if str(close_side).upper() == "SELL":
                    # must be above mark
                    floor = m * (1.0 + max(buf, 0.0005))
                    p = max(p, floor)
                    # round UP so it stays above
                    if tick and tick > 0:
                        p = _round_price_to_tick(p, tick, mode="up")
                else:
                    # BUY: must be below mark
                    ceil = m * (1.0 - max(buf, 0.0005))
                    p = min(p if p > 0 else ceil, ceil)
                    # round DOWN so it stays below
                    if tick and tick > 0:
                        p = _round_price_to_tick(p, tick, mode="down")
            else:
                # no mark -> just tick rounding in the safe direction
                if tick and tick > 0:
                    p = _round_price_to_tick(p, tick, mode="up" if str(close_side).upper() == "SELL" else "down")
            return float(p)

        def _tl_place_algo_with_retry(place_fn, *, max_tries: int = 3):
            """Retry placing conditional orders when Binance says 'would immediately trigger' (-2021)."""
            last = None
            for i in range(int(max_tries or 1)):
                try:
                    return place_fn(i)
                except Exception as e:
                    last = e
                    s = str(e)
                    if ("-2021" in s) or ("immediately trigger" in s.lower()):
                        continue
                    raise
            if last:
                raise last
            return None


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
            avg_price = _safe_float(p.get("avg_price"), 0.0)
            raw_meta = p.get("raw_meta") or {}
            qty_led = abs(_safe_float(p.get("qty_current"), 0.0))
            qty_use = float(qty_ex if qty_ex > 0 else qty_led)
            if qty_use <= 0:
                continue

            pos_uid = str(p.get("pos_uid") or "").strip()
            if not pos_uid:
                continue

            tok = _coid_token(pos_uid, n=20)
            cid_sl = f"{prefix}_{tok}_SL"
            cid_tp = f"{prefix}_{tok}_TP"
            cid_trl = f"{prefix}_{tok}_TRL"

            close_side = "SELL" if side == "LONG" else "BUY"
            position_side = side if hedge_mode else "BOTH"


            
            existing = open_by_symbol.get(sym, set())

            # Also detect *any* existing protective orders on Binance for this symbol/side,
            # even if their client ids are not ours. This prevents Binance error -4130
            # ("An open stop or take profit order with GTE and closePosition ... is existing.")
            def _to_bool(x: Any) -> bool:
                if isinstance(x, bool):
                    return bool(x)
                if x is None:
                    return False
                s = str(x).strip().lower()
                return s in {"1", "true", "yes", "y", "on"}

            def _detect_existing_protection() -> tuple[bool, bool, bool]:
                rows = open_algo_by_symbol.get(sym, []) if isinstance(open_algo_by_symbol, dict) else []
                has_any_sl = False
                has_any_tp = False
                has_any_trl = False
                for o in rows:
                    try:
                        otype = str(o.get("orderType") or o.get("type") or "").upper()
                        oside = str(o.get("side") or "").upper()
                        ops = str(o.get("positionSide") or "").upper()
                        # In hedge mode positionSide is LONG/SHORT; otherwise it can be BOTH/empty.
                        if hedge_mode:
                            if ops and ops not in {position_side, "BOTH"}:
                                continue
                        # Make sure it's the close direction
                        if oside and oside != close_side:
                            continue

                        close_pos = _to_bool(o.get("closePosition"))

                        # TP: closePosition=true TP orders (Binance typically enforces one)
                        if otype in {"TAKE_PROFIT_MARKET", "TAKE_PROFIT"} and close_pos:
                            has_any_tp = True

                        # SL: stop-market/stop with closePosition OR any trailing stop
                        if otype in {"STOP_MARKET", "STOP"} and close_pos:
                            has_any_sl = True
                        if otype == "TRAILING_STOP_MARKET":
                            has_any_trl = True
                    except Exception:
                        continue
                return has_any_sl, has_any_tp, has_any_trl

            any_sl, any_tp, any_trl = _detect_existing_protection()

            has_sl = (cid_sl in existing) or bool(any_sl)
            has_tp = (cid_tp in existing) or bool(any_tp)
            has_trl = (cid_trl in existing) or bool(any_trl)


            # Compute protective prices from the *actual* average entry.
            # Prefer exchange-reported entryPrice (positionRisk) when available.
            entry_ledger = _safe_float(p.get("entry_price"), 0.0)
            entry = float(ex_entry_map.get((sym, side), entry_ledger) or 0.0)
            # price tick for proper rounding (used for TP / trailing activation sanity checks)
            try:
                _tick_dec = self._price_tick_for_symbol(sym)
                tick = float(_tick_dec) if (_tick_dec is not None and float(_tick_dec) > 0) else 0.0
            except Exception:
                tick = 0.0
            if not tick or tick <= 0:
                tick = 1e-8
            if entry <= 0:
                continue

            if side == "LONG":
                sl_price = entry * (1.0 - _pct_to_mult(self.p.stop_loss_pct))
                tp_price = entry * (1.0 + _pct_to_mult(self.p.take_profit_pct))
            else:
                sl_price = entry * (1.0 + _pct_to_mult(self.p.stop_loss_pct))
                tp_price = entry * (1.0 - _pct_to_mult(self.p.take_profit_pct))

            # Hard guarantee: TP must be on the correct side of entry.
            # (Some safety/rounding logic can otherwise push it across.)
            tp_price = _ensure_tp_trail_side(tp_price, entry, tick, side, kind="tp")

            # place missing orders
            
            if place_sl and (not has_sl) and sl_price > 0:
                # Defer STOP_MARKET SL until last averaging add (if enabled)
                defer_sl = bool(getattr(self.p, "defer_stop_loss_until_last_add", False))
                avg_enabled = bool(getattr(self.p, "averaging_enabled", False))
                cfg_max_adds = int(getattr(self.p, "averaging_max_adds", 0) or 0)
                allow_place_sl = True
                if avg_enabled and defer_sl and cfg_max_adds > 0:
                    try:
                        rm = p.get("raw_meta") or {}
                        st = (rm.get("avg_state") or {}) if isinstance(rm, dict) else {}
                        adds_done = int(st.get("adds_done", 0) or 0)
                        max_adds = int(st.get("max_adds", cfg_max_adds) or cfg_max_adds)
                        scale_in = int(p.get("scale_in_count", 0) or 0)
                        if max_adds > 0 and min(adds_done, scale_in) < max_adds:
                            allow_place_sl = False
                    except Exception:
                        allow_place_sl = False
                if allow_place_sl:
                    try:
                        sl_mode = str(getattr(self.p, "sl_order_mode", "stop_market") or "stop_market").strip().lower()
                        if sl_mode in {"trailing_stop_market", "trailing", "tsm"} and bool(getattr(self.p, "trailing_enabled", True)):
                            callback_rate = float(getattr(self.p, "trailing_trail_pct", 0.6) or 0.6)
                            activation_pct = float(getattr(self.p, "trailing_activation_pct", 1.2) or 1.2)
                            buffer_pct = float(getattr(self.p, "trailing_activation_buffer_pct", 0.25) or 0.25)

                            # Get markPrice from exchange snapshot if available (best-effort).
                            mark = _safe_float(ex_mark_map.get((sym, side)), 0.0)

                            # For SELL trailing stop, activation must be ABOVE current mark.
                            # For BUY trailing stop, activation must be BELOW current mark.
                            if close_side.upper() == "SELL":
                                desired_act = entry * (1.0 + activation_pct / 100.0)
                            else:
                                desired_act = entry * (1.0 - activation_pct / 100.0)

                            # Ensure activation is on correct side of the *actual* entry.
                            desired_act = _ensure_tp_trail_side(desired_act, entry, tick, side, kind="trail_activate")

                            tick_dec = self._price_tick_for_symbol(sym)
                            tick = float(tick_dec) if tick_dec is not None else 0.0

                            def _place(i_try: int):
                                # increase buffer a bit on each retry
                                buf = buffer_pct * (1.0 + 0.7 * float(i_try))
                                activation_price = _tl_safe_trigger_price(
                                    symbol=sym,
                                    close_side=close_side,
                                    desired=desired_act,
                                    mark=mark,
                                    tick=tick,
                                    buffer_pct=buf,
                                )

                                # Round and enforce side vs actual entry.
                                activation_price = _round_price_to_tick(
                                    activation_price,
                                    tick,
                                    mode="up" if close_side.upper() == "SELL" else "down",
                                )
                                activation_price = _ensure_tp_trail_side(
                                    activation_price,
                                    entry,
                                    tick,
                                    side,
                                    kind="trail_activate",
                                )
                                params = dict(
                                    symbol=sym,
                                    side=close_side,
                                    type="TRAILING_STOP_MARKET",
                                    quantity=float(qty_use),
                                    activationPrice=float(activation_price),
                                    callbackRate=float(callback_rate),
                                    workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                                    newClientOrderId=cid_trl,
                                    positionSide=position_side if hedge_mode else None,
                                )
                                if (not hedge_mode) and bool(self.p.reduce_only):
                                    params["reduceOnly"] = True
                                return self._binance.new_order(**params)

                            resp = _tl_place_algo_with_retry(_place, max_tries=int(getattr(self.p, "trailing_place_retries", 3) or 3))

                        else:
                            params = dict(
                                symbol=sym,
                                side=close_side,
                                type="STOP_MARKET",
                                stopPrice=float(sl_price),
                                closePosition=True,
                                workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                                newClientOrderId=cid_trl,
                                positionSide=position_side if hedge_mode else None,
                            )
                            if (not hedge_mode) and bool(self.p.reduce_only):
                                params["reduceOnly"] = True
                            def _place_sl2(i: int):
                                p2 = dict(params)
                                try:
                                    px = float(p2.get("stopPrice") or p2.get("triggerPrice") or 0.0)
                                    # Mark-based protection from -2021: force stop further away from current mark.
                                    mark_sl = 0.0
                                    try:
                                        mark_sl = float(self._get_mark_price(symbol) or 0.0)
                                    except Exception:
                                        mark_sl = 0.0
                                    if mark_sl <= 0:
                                        try:
                                            mark_sl = float(self._get_last_price(symbol) or 0.0)
                                        except Exception:
                                            mark_sl = 0.0

                                    buf = float(getattr(self.p, "tp_trigger_buffer_pct", 0.25) or 0.25)
                                    # Increase buffer on retries so we actually move away if Binance says -2021.
                                    eff_buf = buf * (1.0 + float(i))
                                    tick = float(tick_size) if tick_size else 0.0

                                    if mark_sl > 0:
                                        if close_side == "SELL":
                                            # long SL: SELL stop must be BELOW mark
                                            px = min(px, mark_sl * (1.0 - eff_buf / 100.0))
                                            # push one more tick down each retry (if tick known)
                                            if tick > 0:
                                                px = px - tick
                                                px = _round_price_to_tick(px, tick, mode="down")
                                        else:
                                            # short SL: BUY stop must be ABOVE mark
                                            px = max(px, mark_sl * (1.0 + eff_buf / 100.0))
                                            if tick > 0:
                                                px = px + tick
                                                px = _round_price_to_tick(px, tick, mode="up")

                                    # Ensure numeric and set both keys (some wrappers drop stopPrice if triggerPrice is present)
                                    p2["triggerPrice"] = float(px)
                                    p2["stopPrice"] = float(px)
                                except Exception:
                                    pass
                                return self._binance.new_order(**p2)

                            resp = _tl_place_algo_with_retry(_place_sl2, max_tries=int(getattr(self.p, "trailing_place_retries", 3) or 3))

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
                        buffer_pct = float(getattr(self.p, "tp_trigger_buffer_pct", 0.25) or 0.25)
                        mark = _safe_float(ex_mark_map.get((sym, side)), 0.0)
                        tick_dec = self._price_tick_for_symbol(sym)
                        tick = float(tick_dec) if tick_dec is not None else 0.0

                        def _place(i_try: int):
                            buf = buffer_pct * (1.0 + 0.7 * float(i_try))
                            trigger_price = _tl_safe_trigger_price(
                                symbol=sym,
                                close_side=close_side,
                                desired=float(tp_price),
                                mark=mark,
                                tick=tick,
                                buffer_pct=buf,
                            )
                            params = dict(
                                symbol=sym,
                                side=close_side,
                                type="TAKE_PROFIT_MARKET",
                                stopPrice=float(trigger_price),
                                closePosition=True,
                                workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                                newClientOrderId=cid_tp,
                                positionSide=position_side if hedge_mode else None,
                            )
                            if (not hedge_mode) and bool(self.p.reduce_only):
                                params["reduceOnly"] = True
                            return self._binance.new_order(**params)

                        resp = _tl_place_algo_with_retry(_place, max_tries=int(getattr(self.p, "tp_place_retries", 3) or 3))

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
                        def _place_tp2(i: int):
                            p2 = dict(params)
                            try:
                                px = float(p2.get("stopPrice") or p2.get("triggerPrice") or 0.0)
                                if i > 0 and tick_size:
                                    # move one tick away from immediate trigger
                                    if close_side == "SELL":
                                        px = px + float(tick_size) * i
                                        px = _round_price_to_tick(px, float(tick_size), mode="up")
                                    else:
                                        px = px - float(tick_size) * i
                                        px = _round_price_to_tick(px, float(tick_size), mode="down")
                                # Binance uses triggerPrice for algo orders, stopPrice for normal - keep both
                                p2["triggerPrice"] = float(px)
                                p2["stopPrice"] = float(px)
                                p2["triggerPrice"] = float(px)
                            except Exception:
                                pass
                            return self._binance.new_order(**p2)

                        resp = _tl_place_algo_with_retry(_place_tp2, max_tries=int(getattr(self.p, "tp_place_retries", 3) or 3))

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

            # Trailing stop recovery (independent from SL deferral): ensure TRAILING is present if enabled
            try:
                trailing_enabled = bool(getattr(self.p, "trailing_enabled", False))
                if trailing_enabled and (not has_trl):
                    # reference price: before any averaging adds -> entry_price, after adds -> avg_price
                    try:
                        avg_state = (raw_meta or {}).get("avg_state") or {}
                        adds_done = int(avg_state.get("adds_done") or 0)
                    except Exception:
                        adds_done = 0
                    ref_price = float(avg_price or 0) if adds_done > 0 and avg_price else float(entry or 0)
                    if ref_price and qty_use and float(qty_use) > 0:
                        mark = self._get_mark_price(sym, position_side if hedge_mode else None)
                        tick = float(getattr(self, "_symbol_tick", {}).get(sym, 0) or 0) or float(getattr(self, "_symbol_tick_size", {}).get(sym, 0) or 0) or 0.0
                        if tick <= 0:
                            # fallback: reuse ticks from symbol meta if present
                            tick = float((raw_meta or {}).get("tick_size") or 0) or 0.0
                        act_pct = float(getattr(self.p, "trailing_activation_pct", 0.5) or 0.5)
                        act_des = ref_price * (1 + act_pct / 100.0) if side == "LONG" else ref_price * (1 - act_pct / 100.0)
                        act = _round_to_tick(act_des, tick_size=tick, mode="nearest") if tick > 0 else act_des
                        buf = float(getattr(self.p, "trailing_activation_buffer_pct", 0.15) or 0.15)
                        if side == "LONG":
                            act = self._safe_price_above_mark(act, mark, tick, buf)
                        else:
                            act = self._safe_price_below_mark(act, mark, tick, buf)
                        cb = float(getattr(self.p, "trailing_callback_rate", 0.5) or 0.5)
                        def _place_trl(i_try: int):
                            p_trl = dict(
                                symbol=sym,
                                side=close_side,
                                type="TRAILING_STOP_MARKET",
                                quantity=str(qty_use),
                                callbackRate=cb,
                                workingType="MARK_PRICE",
                                positionSide=position_side if hedge_mode else None,
                                algoType="CONDITIONAL",
                                activatePrice=str(act),
                                newClientOrderId=cid_trl,
                            )
                            return self._binance.new_order(**p_trl)
                        resp_trl = _tl_place_algo_with_retry(_place_trl, max_tries=int(getattr(self.p, "trailing_place_retries", 3) or 3))
                        self._upsert_order_shadow(
                            symbol_id=int(symbol_id),
                            order_id=str(resp_trl.get("algoId") or resp_trl.get("orderId") or ""),
                            client_order_id=cid_trl,
                            side=close_side,
                            order_type="TRAILING_STOP_MARKET",
                            status=str(resp_trl.get("algoStatus") or resp_trl.get("status") or "NEW"),
                            qty=float(qty_use),
                            price=float(act) if act is not None else None,
                            reduce_only=True,
                            pos_uid=pos_uid,
                        )
                        recovered += 1
                        log.info("[trade_liquidation][RECOVERY] placed missing TRAILING %s %s pos_uid=%s", sym, side, pos_uid)
            except Exception:
                log.exception("[trade_liquidation][RECOVERY] failed trailing recovery for %s %s pos_uid=%s", sym, side, pos_uid)

            # Averaging ADD recovery: after restart ADD orders may be missing because they were previously
            # placed only at ENTRY time. If averaging is enabled, ensure at least ADD1 exists.
            try:
                averaging_enabled = bool(getattr(self.p, "averaging_enabled", False) or getattr(self.p, "avg_enabled", False))
                if averaging_enabled:
                    self._recovery_ensure_add1(
                        sym=sym,
                        side=side,
                        position_side=position_side,
                        pos_uid=pos_uid,
                        symbol_id=int(symbol_id),
                        entry_price=Decimal(str(avg_price or entry or 0)),
        mark_price=_dec(ex_mark_map.get((sym, side)) or "0"),
                        pos_qty=Decimal(str(qty_use or 0)),
                        existing_client_ids=existing,
                    )
            except Exception:
                log.exception("[trade_liquidation][RECOVERY] failed ADD recovery for %s %s pos_uid=%s", sym, side, pos_uid)

        return int(recovered)

    def _recovery_ensure_add1(
        self,
        *,
        sym: str,
        side: str,
        position_side: str,
        pos_uid: str,
        symbol_id: int,
        entry_price: Decimal,
        mark_price: Decimal | None = None,
        pos_qty: Decimal,
        existing_client_ids: set,
    ) -> None:
        """Ensure the first averaging add order exists.

        If there is no significant level available (we don't have it during recovery), we place a
        fallback add level at least `min_level_distance_pct` away from the average entry.
        Default min is 5%.
        """

        tok = _coid_token(pos_uid)
        sym_u = (sym or "").upper()

        # If any ADD exists, do nothing
        for cid in (existing_client_ids or set()):
            if isinstance(cid, str) and (f"TL_{tok}_ADD" in cid):
                return

        if entry_price is None or entry_price <= 0 or pos_qty is None or pos_qty <= 0:
            return

        # Min distance (default 5%)
        min_pct = Decimal(str(getattr(self.p, "min_level_distance_pct", 5) or 5))
        if min_pct < 0:
            min_pct = Decimal("0")

        if str(side).upper() == "LONG":
            add_price = entry_price * (Decimal("1") - (min_pct / Decimal("100")))
            add_side = "BUY"
        else:
            add_price = entry_price * (Decimal("1") + (min_pct / Decimal("100")))
            add_side = "SELL"

        tick = self._price_tick_for_symbol(sym_u) or Decimal("0")
        if tick and tick > 0:
            # for LONG BUY add we want to be conservative: round down; for SHORT SELL add round up
            # NOTE: _round_to_step uses keyword-only arg `rounding` (not `mode`).
            add_price = _round_to_step(add_price, tick, rounding=("floor" if add_side == "BUY" else "ceiling"))

        # Qty sizing: default = position qty / max_adds, unless averaging_add_qty_pct is specified
        max_adds = int(getattr(self.p, "averaging_max_adds", 1) or 1)
        add_qty_pct = Decimal(str(getattr(self.p, "averaging_add_qty_pct", 0) or 0))
        if add_qty_pct > 0:
            add_qty = pos_qty * (add_qty_pct / Decimal("100"))
        else:
            add_qty = (pos_qty / Decimal(str(max_adds))) if max_adds > 0 else pos_qty

        qty_step = self._qty_step_for_symbol(sym_u) or Decimal("0")
        if qty_step and qty_step > 0:
            add_qty = _round_qty_to_step(add_qty, qty_step, mode="down")

        if add_qty <= 0:
            return

        cid_add = f"TL_{tok}_ADD1"
        # Ensure ADD trigger price is on the correct side of current mark price to avoid immediate-trigger (Binance -2021).
        # For LONG we add with BUY when price falls; for SHORT we add with SELL when price rises.
        try:
            tick = _dec(tick)
        except Exception:
            tick = _dec('0')
        if mark_price is not None and mark_price > 0 and tick and tick > 0:
            if add_side == 'BUY':
                # must be strictly below mark
                if add_price >= mark_price:
                    add_price = mark_price - tick
            else:
                # must be strictly above mark
                if add_price <= mark_price:
                    add_price = mark_price + tick

        # If price already moved beyond the first add level, push trigger further away in steps of min_pct from entry.
        min_pct = _dec(str(getattr(self.p, 'averaging_min_level_pct', getattr(self.p, 'min_level_pct', 5)) or 5)) / _dec('100')
        if entry_price and min_pct > 0 and mark_price is not None and mark_price > 0 and tick and tick > 0:
            for i in range(1, 26):
                if add_side == 'BUY':
                    cand = entry_price * (_dec('1') - (min_pct * _dec(i)))
                    cand = _round_to_step(cand, tick, rounding='down')
                    if cand < (mark_price - tick):
                        add_price = cand
                        break
                else:
                    cand = entry_price * (_dec('1') + (min_pct * _dec(i)))
                    cand = _round_to_step(cand, tick, rounding='up')
                    if cand > (mark_price + tick):
                        add_price = cand
                        break

        params = dict(
            symbol=sym_u,
            side=add_side,
            # Conditional market order for averaging (trigger -> market)
            type='TAKE_PROFIT_MARKET',
            stopPrice=float(add_price),
            quantity=float(add_qty),
            workingType="MARK_PRICE",
            priceProtect=True,
            newClientOrderId=cid_add,
            positionSide=position_side,
        )

        resp = self._binance.new_order(**params)

        # Shadow insert with supported args only
        try:
            if isinstance(resp, dict):
                self._upsert_order_shadow(
                    pos_uid=pos_uid,
                    order_id=str(resp.get("orderId") or resp.get("order_id") or ""),
                    client_order_id=cid_add,
                    symbol_id=int(symbol_id),
                    side=add_side,
                    order_type=params.get("type"),
                    qty=float(add_qty),
                    price=float(add_price),
                    reduce_only=False,
                    status=str(resp.get("status") or "NEW"),
                )
        except Exception:
            pass

        log.info("[trade_liquidation][RECOVERY] placed missing ADD1 %s %s pos_uid=%s price=%s qty=%s", sym_u, side, pos_uid, add_price, add_qty)

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
                        {"ex": int(self.p.exchange_id), "acc": int(self.account_id), "coid": coid},
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

    # ----------------------------------------------------------
    # Positions
    # ----------------------------------------------------------


# ---------------------------------------------------------------------
# Live: helpers for "position became 0" => cancel all orders by symbol
    # ---------------------------------------
    # Helpers for symbol precision / cleanup
    # ---------------------------------------

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

    def _is_flat_qty(self, qty: Any, symbol: str) -> bool:
        """
        True if position amount is effectively zero for the symbol.

        Binance positionAmt can be very small residual; we treat anything below qty_step as flat.
        """
        try:
            q = abs(Decimal(str(qty)))
        except Exception:
            try:
                q = abs(Decimal(qty))
            except Exception:
                return False
        step = self._qty_step_for_symbol(symbol) or Decimal("0")
        if step > 0:
            return q < step
        return q == 0

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
    @staticmethod
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

    def _fetch_exchange_exit_stats(
        self,
        symbol: str,
        pos_side: str,
        opened_at: Any,
        closed_at: Any,
        qty_expected: float,
    ) -> Dict[str, Any]:
        """Fetch best-effort exit_price / realized_pnl / fees / close_time_ms from Binance futures trade history.

        Strategy:
          - Query userTrades in a bounded window [opened_at-60s, closed_at+60s] when closed_at is known.
            This avoids the "last 1000 trades" truncation when the account is active.
          - Paginate forward if Binance returns exactly `limit` trades.
          - Aggregate closing-side fills (SELL for LONG, BUY for SHORT) with matching positionSide (or BOTH).
          - Return fees (USDT + other assets) even if we can't infer exit fills (so we can at least backfill fees/marker).
        """
        ps = str(pos_side or "").upper()
        close_order_side = "SELL" if ps == "LONG" else "BUY"

        # time window
        opened_ms = self._to_epoch_ms(opened_at)
        closed_ms = self._to_epoch_ms(closed_at)
        if opened_ms is None:
            opened_ms = int(time.time() * 1000) - 7 * 24 * 3600 * 1000  # fallback: last 7d
        start_ms = max(0, opened_ms - 60_000)
        end_ms = (closed_ms + 60_000) if closed_ms is not None else None

        limit = 1000
        trades_all: List[Dict[str, Any]] = []
        page_start = start_ms
        # paginate forward (Binance returns trades within [startTime,endTime] up to limit)
        for _ in range(20):  # hard safety cap
            batch = self._binance_rest.user_trades(
                symbol=symbol,
                start_time_ms=page_start,
                end_time_ms=end_ms,
                limit=limit,
            )
            if not batch:
                break
            trades_all.extend(batch)
            if len(batch) < limit:
                break
            # move start to just after last trade time to avoid duplicates
            try:
                last_t = max(int(x.get("time") or 0) for x in batch)
            except Exception:
                break
            if last_t <= 0:
                break
            page_start = last_t + 1
            # if we already passed end_ms, stop
            if end_ms is not None and page_start >= end_ms:
                break

        if not trades_all:
            return {"fees_usdt": 0.0, "fees_other": {}, "exit_qty": 0.0}

        # Aggregate
        qty_target = abs(float(qty_expected or 0.0))
        qty_target_tol = qty_target * 0.995 if qty_target > 0 else 0.0

        exit_qty = 0.0
        exit_notional = 0.0
        realized = 0.0
        fees_usdt = 0.0
        fees_other: Dict[str, float] = {}
        last_time_ms: Optional[int] = None

        try:
            trades_sorted = sorted(trades_all, key=lambda x: int(x.get("time") or 0))
        except Exception:
            trades_sorted = trades_all

        for t in trades_sorted:
            try:
                # fees (collect for ALL trades in window for that positionSide/BOTH)
                if "positionSide" in t and str(t.get("positionSide") or "").upper() not in ("", ps, "BOTH"):
                    continue
                ca = str(t.get("commissionAsset") or "").upper()
                cval = float(t.get("commission") or 0.0)
                if ca == "USDT":
                    fees_usdt += cval
                elif cval:
                    fees_other[ca] = fees_other.get(ca, 0.0) + cval

                # closing side fills for exit VWAP/pnl
                if str(t.get("side") or "").upper() != close_order_side:
                    continue
                q = float(t.get("qty") or 0.0)
                p = float(t.get("price") or 0.0)
                if q <= 0 or p <= 0:
                    continue
                exit_qty += q
                exit_notional += q * p
                realized += float(t.get("realizedPnl") or 0.0)
                last_time_ms = int(t.get("time") or 0) or last_time_ms

                if qty_target_tol > 0 and exit_qty >= qty_target_tol:
                    # we've likely covered the closing quantity; keep collecting fees already done
                    break
            except Exception:
                continue

        out: Dict[str, Any] = {
            "fees_usdt": float(fees_usdt),
            "fees_other": fees_other,
                        "fees_done": True,
                            "fees_done": True,
            "exit_qty": float(exit_qty),
        }
        if exit_qty > 0:
            out.update(
                {
                    "exit_price": (exit_notional / exit_qty) if exit_qty else None,
                    "realized_pnl": float(realized),
                    "close_time_ms": int(last_time_ms) if last_time_ms else None,
                }
            )
        return out

    def _close_position_exchange(
        self,
        pos: Dict[str, Any],
        exit_price: float,
        realized_pnl: float,
        close_time_ms: Optional[int],
        reason: str,
        timeframe: str,
    ) -> None:
        """Close a ledger position using exchange-derived exit price and realized PnL."""
        meta = {
            "exchange_exit": {
                "reason": reason,
                "timeframe": timeframe,
                "exit_price": float(exit_price),
                "realized_pnl": float(realized_pnl),
                "close_time_ms": int(close_time_ms) if close_time_ms else None,
            }
        }

        closed_at_sql = "now()"
        closed_at_param = None
        if close_time_ms:
            closed_at_sql = "to_timestamp(%(closed_ts)s)"
            closed_at_param = float(close_time_ms) / 1000.0

        if self._pl_has_raw_meta:
            q = f"""
            UPDATE position_ledger
            SET
              status='CLOSED',
              closed_at={closed_at_sql},
              qty_closed=qty_current,
              qty_current=0,
              exit_price=%(exit)s,
              realized_pnl=%(pnl)s,
              updated_at=now(),
              raw_meta = COALESCE(raw_meta,'{{}}'::jsonb) || %(meta)s::jsonb
            WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s AND pos_uid=%(pos_uid)s;
            """
            params = {
                "exit": float(exit_price),
                "pnl": float(realized_pnl),
                "meta": json.dumps(meta),
                "ex": int(self.p.exchange_id),
                "acc": int(self.account_id),
                "sym": int(pos["symbol_id"]),
                "pos_uid": str(pos["pos_uid"]),
            }
            if closed_at_param is not None:
                params["closed_ts"] = closed_at_param
        else:
            q = f"""
            UPDATE position_ledger
            SET
              status='CLOSED',
              closed_at={closed_at_sql},
              qty_closed=qty_current,
              qty_current=0,
              exit_price=%(exit)s,
              realized_pnl=%(pnl)s,
              updated_at=now()
            WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s AND pos_uid=%(pos_uid)s;
            """
            params = {
                "exit": float(exit_price),
                "pnl": float(realized_pnl),
                "ex": int(self.p.exchange_id),
                "acc": int(self.account_id),
                "sym": int(pos["symbol_id"]),
                "pos_uid": str(pos["pos_uid"]),
            }
            if closed_at_param is not None:
                params["closed_ts"] = closed_at_param

        self.store.execute(q, params)

        # log in LIVE style (avoid confusing [PAPER])
        self._ilog(
            "[trade_liquidation][LIVE] CLOSE %s %s qty=%.8f entry=%.8f exit=%.8f pnl=%.8f reason=%s",
            str(pos.get("symbol") or "").upper(),
            str(pos.get("side") or "").upper(),
            float(pos.get("qty_current") or 0.0),
            float(pos.get("avg_price") or pos.get("entry_price") or 0.0),
            float(exit_price),
            float(realized_pnl),
            reason,
        )

    def _get_open_positions(self):
        """Возвращает OPEN позиции стратегии.

        В LIVE режиме считаем "открыто" по факту на бирже:
          - если в ledger позиция OPEN, но на Binance qty уже 0 (закрыли руками/по SL/TP/ликвидации),
            то (если включено reconcile_auto_close_ledger) помечаем её CLOSED и снимаем все ордера по символу.
          - для расчёта open_positions в логах возвращаем только те позиции, у которых qty на бирже != 0.

        Это нужно, чтобы open_positions=... отражал реальность и не блокировал новые входы,
        когда позиции уже закрылись на бирже.
        """
        sql = """
        SELECT
          pl.exchange_id,
          pl.account_id,
          pl.pos_uid,
          pl.symbol_id,
          pl.strategy_id,
          pl.strategy_name,
          pl.side,
          pl.status,
          pl.opened_at,
          pl.closed_at,
          pl.entry_price,
          pl.avg_price,
          pl.exit_price,
          pl.qty_opened,
          pl.qty_current,
          pl.qty_closed,
          pl.position_value_usdt,
          pl.scale_in_count,
          pl.realized_pnl,
          pl.fees,
          pl.updated_at,
          pl.source,
          pl.raw_meta,
          s.symbol
        FROM public.position_ledger pl
        JOIN public.symbols s
          ON s.exchange_id = pl.exchange_id
         AND s.symbol_id   = pl.symbol_id
        WHERE pl.exchange_id = %(exchange_id)s
          AND pl.account_id  = %(account_id)s
          AND pl.strategy_id = %(strategy_id)s
          AND pl.status      = 'OPEN'
        ORDER BY pl.opened_at ASC
        """
        pos = self.store.query_dict(sql, dict(
            exchange_id=int(self.exchange_id),
            account_id=int(self.account_id),
            strategy_id=str(self.strategy_id),
        ))

        # PAPER: просто считаем по ledger
        if str(self.mode).lower() != "live":
            return pos

        # LIVE: сверяемся с биржей, чтобы OPEN отражал реальность
        try:
            pr = self._binance.position_risk()
        except Exception as e:
            # если не смогли получить snapshot, не ломаем цикл — вернём ledger как есть
            self._dlog("positionRisk fetch failed in _get_open_positions: %s", e)
            return pos

        # build map (symbol, positionSide) -> abs(positionAmt) and markPrice
        qty_map = {}
        mark_map = {}
        for r in pr or []:
            sym = (r.get("symbol") or "").upper()
            ps = (r.get("positionSide") or "").upper()
            if not sym or not ps:
                continue
            try:
                amt = float(r.get("positionAmt") or 0.0)
            except Exception:
                amt = 0.0
            try:
                mp = float(r.get("markPrice") or 0.0)
            except Exception:
                mp = 0.0
            qty_map[(sym, ps)] = abs(amt)
            mark_map[(sym, ps)] = mp

        tol = float(getattr(self.params, "reconcile_qty_tolerance", 1e-8) or 1e-8)
        auto_close = bool(getattr(self.params, "reconcile_auto_close_ledger", True))
        closed_now = 0
        open_real = []

        for p in pos:
            sym = (p.get("symbol") or "").upper()
            side = (p.get("side") or "").upper()  # LONG/SHORT
            exch_qty = float(qty_map.get((sym, side), 0.0) or 0.0)

            if abs(exch_qty) <= tol:
                # На бирже позиции уже нет, но в ledger ещё OPEN
                if auto_close:
                    try:
                        exit_price = float(mark_map.get((sym, side), 0.0) or 0.0)
                        # Try to get real exit stats from exchange trade history (close price + realized pnl)
                        stats = self._fetch_exchange_exit_stats(
                            symbol=sym,
                            pos_side=side,
                            opened_at=p.get("opened_at"),
                            closed_at=_utc_now(),
                            qty_expected=_safe_float(p.get("qty_current"), default=0.0),
                        )
                        ex_exit = stats.get("exit_price")
                        ex_pnl = stats.get("realized_pnl")
                        ex_close_ms = stats.get("close_time_ms")

                        if ex_exit is None:
                            ex_exit = exit_price
                        if not ex_exit:
                            # fallback: если markPrice не пришёл, используем avg_price/entry_price
                            ex_exit = float(p.get("avg_price") or p.get("entry_price") or 0.0)

                        if ex_pnl is None:
                            # last resort: approximate (still better than missing)
                            entry = _safe_float(p.get("avg_price"), default=_safe_float(p.get("entry_price"), default=0.0))
                            qty = _safe_float(p.get("qty_current"), default=0.0)
                            ex_pnl = (float(ex_exit) - entry) * qty if str(side).upper() == "LONG" else (entry - float(ex_exit)) * qty

                        self._close_position_exchange(
                            p,
                            exit_price=float(ex_exit),
                            realized_pnl=float(ex_pnl),
                            close_time_ms=ex_close_ms,
                            reason="exchange_qty_zero",
                            timeframe="reconcile",
                        )
                        # снимаем все ордера по символу (обычные + algo)
                        self._live_cancel_symbol_orders(sym)
                        closed_now += 1
                    except Exception as e:
                        self._dlog("auto-close ledger failed pos_uid=%s sym=%s side=%s: %s",
                                   p.get("pos_uid"), sym, side, e)
                continue

            # позиция реально открыта
            open_real.append(p)

        if closed_now:
            log.info("[TL] ledger auto-closed (exchange qty=0): closed_now=%s", closed_now)

        return open_real


    def _backfill_closed_exit_pnl(self, force: bool = False, log_empty: bool = False) -> Dict[str, int]:
        """Background backfill for CLOSED ledger rows missing exit_price/realized_pnl.

        Goal: if a position is already CLOSED in DB but exit_price/realized_pnl are NULL (e.g. old runs),
        fetch Binance userTrades to infer close VWAP and realized PnL and write them into position_ledger.

        Runs in LIVE mode only. Rate-limited by backfill_interval_sec (default 600s).
        """
        # rate limit
        interval = float(getattr(self.p, "backfill_interval_sec", 600) or 600)
        now_ts = time.time()
        if not force:
            last_ts = float(getattr(self, "_last_backfill_ts", 0.0) or 0.0)
            if interval > 0 and (now_ts - last_ts) < interval:
                return {"checked": 0, "updated": 0, "skipped": 1}

        self._last_backfill_ts = now_ts

        lookback_days = int(getattr(self.p, "backfill_lookback_days", 14) or 14)
        batch = int(getattr(self.p, "backfill_batch_limit", 50) or 50)

        overwrite = bool(getattr(self.p, 'backfill_overwrite', False))
        if overwrite:
            q = """
                SELECT
                  pl.pos_uid, pl.symbol_id, pl.side, pl.qty_closed,
                  pl.opened_at, pl.closed_at, pl.exit_price, pl.realized_pnl, pl.fees, pl.raw_meta
                FROM public.position_ledger pl
                WHERE pl.exchange_id = %(exchange_id)s
                  AND pl.account_id = %(account_id)s
                  AND pl.strategy_id = %(strategy_id)s
                  AND pl.status = 'CLOSED'
                  AND COALESCE(pl.closed_at, pl.updated_at) >= (now() - (%(lookback_days)s || ' days')::interval)
                  AND (
                        %(overwrite)s
                        OR pl.exit_price IS NULL
                        OR pl.realized_pnl IS NULL
                        OR pl.fees IS NULL
                        OR pl.fees = 0
                        OR NOT (pl.raw_meta ? 'exchange_exit_backfill')
                      )
                ORDER BY pl.updated_at DESC
                LIMIT %(batch)s
            """
        else:
            q = """
                SELECT
                  pl.pos_uid, pl.symbol_id, pl.side, pl.qty_closed,
                  pl.opened_at, pl.closed_at, pl.exit_price, pl.realized_pnl, pl.fees, pl.raw_meta
                FROM public.position_ledger pl
                WHERE pl.exchange_id = %(exchange_id)s
                  AND pl.account_id = %(account_id)s
                  AND pl.strategy_id = %(strategy_id)s
                  AND pl.status = 'CLOSED'
                  AND COALESCE(pl.closed_at, pl.updated_at) >= (now() - (%(lookback_days)s || ' days')::interval)
                  AND (
                        pl.exit_price IS NULL
                     OR pl.realized_pnl IS NULL
                     OR (%(fill_fees)s AND (pl.fees IS NULL OR pl.fees = 0))
                     OR NOT (pl.raw_meta ? 'exchange_exit_backfill')
                  )
                ORDER BY pl.updated_at DESC
                LIMIT %(batch)s
            """
        rows = self.store.query_dict(q, {
            "exchange_id": int(self.p.exchange_id),
            "account_id": int(self.account_id),
            "strategy_id": str(self.STRATEGY_ID),
            "lookback_days": int(lookback_days),
            "batch": int(batch),
            "fill_fees": bool(getattr(self.p, "backfill_fill_fees", True)),
        }) or []

        updated = 0
        checked = 0

        for r in rows:
            checked += 1
            sym = (r.get("symbol") or "").upper()
            side = (r.get("side") or "").upper()  # LONG/SHORT
            qty_expected = _safe_float(r.get("qty_closed"), default=_safe_float(r.get("qty_opened"), default=0.0))
            if not sym or qty_expected <= 0:
                continue

            opened_ms = self._to_epoch_ms(r.get("opened_at"))
            closed_ms = self._to_epoch_ms(r.get("closed_at"))
            start_ms = (opened_ms - 60_000) if opened_ms else None
            end_ms = (closed_ms + 60_000) if closed_ms else None

            if getattr(self.p, "debug", False):
                log.info("[TL][debug] backfill candidate symbol=%s side=%s qty_expected=%.8f opened_ms=%s closed_ms=%s", sym, side, qty_expected, opened_ms, closed_ms)

            try:
                trades = self._binance_rest.user_trades(symbol=sym, start_time_ms=start_ms, end_time_ms=end_ms, limit=1000)
                if getattr(self.p, "debug", False):
                    log.info("[TL][debug] backfill userTrades fetched=%s symbol=%s window_ms=[%s,%s]", len(trades) if trades else 0, sym, start_ms, end_ms)
                # Retry with a wider window if nothing returned (closed_at may be set long after fills)
                if (not trades) and closed_ms:
                    start2 = max(int(closed_ms) - 3 * 24 * 60 * 60 * 1000, 0)
                    end2 = int(time.time() * 1000)
                    trades = self._binance_rest.user_trades(symbol=sym, start_time_ms=start2, end_time_ms=end2, limit=1000)
                    if getattr(self.p, "debug", False):
                        log.info("[TL][debug] backfill userTrades retry fetched=%s symbol=%s window_ms=[%s,%s]", len(trades) if trades else 0, sym, start2, end2)
                ps = str(side or "").upper()
                close_order_side = "SELL" if ps == "LONG" else "BUY"


                trades_sorted = sorted(trades or [], key=lambda t: int(t.get("time") or t.get("timestamp") or t.get("tradeTime") or 0))
                qty_target = abs(float(qty_expected or 0.0))
                qty_target_tol = qty_target * 0.995 if qty_target else 0.0

                # Collect only *closing* fills for this symbol/positionSide within the window.
                # In hedge-mode Binance may return BOTH; we allow BOTH or exact match.
                close_fills = []
                for t in trades_sorted:
                    try:
                        sym_t = str(t.get("symbol") or t.get("s") or "").upper()
                        if sym_t and sym_t != str(sym).upper():
                            continue

                        if "positionSide" in t and str(t.get("positionSide") or "").upper() not in ("", ps, "BOTH"):
                            continue

                        t_side = str(t.get("side") or t.get("order_side") or "").upper()
                        if not t_side or t_side != close_order_side:
                            continue

                        # Extract numeric fields robustly (wrappers sometimes rename keys)
                        def _f(keys, default=0.0):
                            for k in keys:
                                if k in t and t.get(k) not in (None, ""):
                                    try:
                                        return float(t.get(k))
                                    except Exception:
                                        pass
                            return default

                        def _i(keys, default=0):
                            for k in keys:
                                if k in t and t.get(k) not in (None, ""):
                                    try:
                                        return int(t.get(k))
                                    except Exception:
                                        pass
                            return default

                        qty = _f(["qty", "quantity", "executedQty", "executed_qty", "origQty", "orig_qty"], 0.0)
                        price = _f(["price", "avgPrice", "avg_price"], 0.0)

                        # Guard against buggy wrappers where 'qty' accidentally contains trade id / order id
                        if qty_target > 0:
                            too_big = max(qty_target * 10.0, 1000.0)
                            if qty > too_big:
                                # try alternatives
                                found_alt = False
                                for alt_k in ["quantity", "executedQty", "executed_qty", "origQty", "orig_qty"]:
                                    if alt_k in t and t.get(alt_k) not in (None, ""):
                                        try:
                                            alt = float(t.get(alt_k))
                                            if 0 < alt <= too_big:
                                                qty = alt
                                                found_alt = True
                                                break
                                        except Exception:
                                            pass
                                if (not found_alt) and qty > too_big:
                                    # still absurd -> skip this fill
                                    continue

                        if qty <= 0 or price <= 0:
                            continue

                        ca = str(t.get("commissionAsset") or t.get("commission_asset") or "").upper()
                        cv = _f(["commission", "commissionAmount", "commission_amount"], 0.0)

                        order_id = t.get("orderId") or t.get("order_id") or t.get("orderID") or None
                        trade_time = _i(["time", "timestamp", "tradeTime", "trade_time"], 0)

                        close_fills.append({
                            "order_id": str(order_id) if order_id is not None else "",
                            "qty": float(qty),
                            "price": float(price),
                            "realized": _f(["realizedPnl", "realized_pnl"], 0.0),
                            "commission_asset": ca,
                            "commission": float(cv),
                            "time_ms": int(trade_time) if trade_time else 0,
                        })
                    except Exception:
                        continue

                # Aggregate closing fills. If the position was closed by multiple orders, we sum all of them.
                # Additionally, we try to avoid mixing unrelated closes by preferring the most recent orders first
                # until we reach expected qty (if expected qty is known).
                order_agg = {}
                for f in close_fills:
                    key = f["order_id"] or "_no_order_id"
                    a = order_agg.get(key)
                    if not a:
                        a = {
                            "exit_qty": 0.0,
                            "exit_notional": 0.0,
                            "realized": 0.0,
                            "fees_usdt": 0.0,
                            "fees_other": {},
                            "last_time_ms": 0,
                        }
                        order_agg[key] = a
                    a["exit_qty"] += f["qty"]
                    a["exit_notional"] += f["qty"] * f["price"]
                    a["realized"] += f["realized"]
                    a["last_time_ms"] = max(a["last_time_ms"], int(f["time_ms"] or 0))

                    if f["commission"]:
                        if f["commission_asset"] == "USDT":
                            a["fees_usdt"] += f["commission"]
                        elif f["commission_asset"]:
                            a["fees_other"][f["commission_asset"]] = float(a["fees_other"].get(f["commission_asset"], 0.0)) + f["commission"]

                # Choose which order groups to use:
                # - if we know expected qty: take the newest groups until qty >= expected (tolerance)
                # - else: take all groups
                groups = list(order_agg.values())
                groups.sort(key=lambda g: int(g.get("last_time_ms") or 0), reverse=True)

                use_groups = []
                total_qty = 0.0
                if qty_target > 0:
                    for g in groups:
                        use_groups.append(g)
                        total_qty += float(g.get("exit_qty") or 0.0)
                        if total_qty >= qty_target_tol:
                            break
                else:
                    use_groups = groups

                # reset per-position accumulators (IMPORTANT: do not carry between symbols)
                exit_qty = 0.0
                exit_notional = 0.0
                realized = 0.0
                fees_usdt = 0.0
                fees_other = {}
                last_time_ms = 0

                for g in use_groups:
                    exit_qty += float(g.get("exit_qty") or 0.0)
                    exit_notional += float(g.get("exit_notional") or 0.0)
                    realized += float(g.get("realized") or 0.0)
                    fees_usdt += float(g.get("fees_usdt") or 0.0)
                    for k, v in (g.get("fees_other") or {}).items():
                        fees_other[k] = float(fees_other.get(k, 0.0)) + float(v or 0.0)
                    last_time_ms = max(last_time_ms, int(g.get("last_time_ms") or 0))

                have_exit = exit_qty > 0
                exit_price = (exit_notional / exit_qty) if exit_qty else None

                # If we couldn't infer exit fills (e.g. API window/limit issues), we can still backfill fees + marker
                if (not have_exit) or (exit_price is None):
                    # Even if fees are not available, write a marker so we don't re-check forever.
                    meta = {
                        "exchange_exit_backfill": {
                            "note": "no_exit_fills_in_window" if (fees_usdt > 0.0 or fees_other) else "no_exit_fills_or_fees",
                            "fees_usdt": float(fees_usdt),
                            "fees_other": fees_other,
                        }
                    }
                    # fees-only update (do not touch exit_price/realized_pnl)
                    if self._pl_has_raw_meta:
                        uq_fees = """
                        UPDATE position_ledger
                        SET
                          fees = COALESCE(NULLIF(fees, 0), %(fees)s),
                          raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb,
                          updated_at = now()
                        WHERE exchange_id = %(ex)s
                          AND account_id  = %(acc)s
                          AND pos_uid     = %(pos_uid)s
                          AND opened_at   = %(opened_at)s
                        """
                    else:
                        uq_fees = """
                        UPDATE position_ledger
                        SET
                          fees = COALESCE(NULLIF(fees, 0), %(fees)s),
                          updated_at = now()
                        WHERE exchange_id = %(ex)s
                          AND account_id  = %(acc)s
                          AND pos_uid     = %(pos_uid)s
                          AND opened_at   = %(opened_at)s
                        """
                    self.store.execute(
                        uq_fees,
                        dict(
                            ex=int(self.exchange_id),
                            acc=int(self.p.account_id),
                            pos_uid=r.get("pos_uid"),
                            opened_at=r.get("opened_at"),
                            fees=float(fees_usdt),
                            meta=json.dumps(meta) if isinstance(meta, dict) else "{}",
                        ),
                    )
                    updated += 1
                    continue

                # have_exit == True and exit_price is known -> full backfill
                meta = {
                    "exchange_exit_backfill": {
                        "exit_price": float(exit_price),
                        "realized_pnl": float(realized),
                        "close_time_ms": int(last_time_ms) if last_time_ms else None,
                        "exit_qty": float(exit_qty),
                        "fees_usdt": float(fees_usdt),
                        "fees_other": fees_other,
                    }
                }

                closed_at_param = float(last_time_ms) / 1000.0 if last_time_ms else None
                closed_at_sql = ("CASE WHEN %(closed_ts)s IS NOT NULL AND (%(overwrite)s = true OR closed_at IS NULL OR (opened_at IS NOT NULL AND closed_at < opened_at)) "
                               "THEN to_timestamp(%(closed_ts)s) ELSE closed_at END")

                if self._pl_has_raw_meta:
                    uq = f"""
                    UPDATE position_ledger
                    SET
                      exit_price = CASE WHEN %(overwrite)s = true THEN %(exit)s ELSE COALESCE(NULLIF(exit_price, 0), %(exit)s) END,
                      realized_pnl = CASE WHEN %(overwrite)s = true THEN %(pnl)s ELSE COALESCE(NULLIF(realized_pnl, 0), %(pnl)s) END,
                      fees = CASE WHEN %(overwrite)s = true THEN %(fees)s ELSE COALESCE(NULLIF(fees, 0), %(fees)s) END,
                      closed_at = {closed_at_sql},
                      updated_at = now(),
                      raw_meta = COALESCE(raw_meta,'{{}}'::jsonb) || %(meta)s::jsonb
                    WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s AND pos_uid=%(pos_uid)s
                      AND status='CLOSED';
                    """
                    params = {
                        "exit": float(exit_price),
                        "pnl": float(realized),
                        "fees": float(fees_usdt),
                        "overwrite": bool(overwrite),
                        "meta": json.dumps(meta),
                        "ex": int(self.p.exchange_id),
                        "acc": int(self.account_id),
                        "sym": int(r["symbol_id"]),
                        "pos_uid": str(r["pos_uid"]),
                    }
                    params["closed_ts"] = closed_at_param
                else:
                    uq = f"""
                    UPDATE position_ledger
                    SET
                      exit_price = COALESCE(exit_price, %(exit)s),
                      realized_pnl = CASE WHEN %(overwrite)s = true THEN %(pnl)s ELSE COALESCE(NULLIF(realized_pnl, 0), %(pnl)s) END,
                      fees = CASE WHEN %(overwrite)s = true THEN %(fees)s ELSE COALESCE(NULLIF(fees, 0), %(fees)s) END,
                      closed_at = {closed_at_sql},
                      updated_at = now()
                    WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s AND pos_uid=%(pos_uid)s
                      AND status='CLOSED';
                    """
                    params = {
                        "exit": float(exit_price),
                        "pnl": float(realized),
                        "fees": float(fees_usdt),
                        "overwrite": bool(overwrite),
                        "ex": int(self.p.exchange_id),
                        "acc": int(self.account_id),
                        "sym": int(r["symbol_id"]),
                        "pos_uid": str(r["pos_uid"]),
                    }
                    if closed_at_param is not None:
                        params["closed_ts"] = closed_at_param

                self.store.execute(uq, params)
                updated += 1
            except Exception:
                log.info("[TL] backfill failed sym=%s pos_uid=%s (will mark as attempted)", sym, r.get("pos_uid"), exc_info=True)
                try:
                    if self._pl_has_raw_meta:
                        meta = {"exchange_exit_backfill": {"note": "exception", "error": "userTrades_or_update_failed"}}
                        uqm = """
                        UPDATE position_ledger
                        SET raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb
                        WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND opened_at=%(opened_at)s;
                        """
                        self.store.execute(
                            uqm,
                            {
                                "ex": int(r["exchange_id"]),
                                "acc": int(r["account_id"]),
                                "pos_uid": str(r["pos_uid"]),
                                "opened_at": r["opened_at"],
                                "meta": json.dumps(meta),
                            },
                        )
                        updated += 1
                except Exception:
                    pass

        if updated or log_empty:
            log.info("[TL] backfill CLOSED exit/pnl updated=%s checked=%s (lookback_days=%s)", updated, checked, lookback_days)

        return {"checked": checked, "updated": updated, "skipped": 0}

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
            WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s AND pos_uid=%(pos_uid)s;
            """
            params = {
                "exit": float(exit_price),
                "pnl": float(pnl),
                "meta": json.dumps(meta),
                "ex": int(self.p.exchange_id),
                "acc": int(self.account_id),
                "sym": int(pos["symbol_id"]),
                "pos_uid": str(pos["pos_uid"]),
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
            WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s AND pos_uid=%(pos_uid)s;
            """
            params = {
                "exit": float(exit_price),
                "pnl": float(pnl),
                "ex": int(self.p.exchange_id),
                "acc": int(self.account_id),
                "sym": int(pos["symbol_id"]),
                "pos_uid": str(pos["pos_uid"]),
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
          COALESCE(sf.price_tick, 0) AS price_tick,
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
        tick_size = _safe_float(sig.get("price_tick"), default=0.0)
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

        # Round SL/TP to tick size to avoid Binance precision errors (-1111).
        if tick_size > 0:
            if ledger_side == "LONG":
                # SL below entry => round down; TP above => round up
                if sl_price > 0:
                    sl_price = _round_price_to_tick(sl_price, tick_size, mode="down")
                if tp_price > 0:
                    tp_price = _round_price_to_tick(tp_price, tick_size, mode="up")
            else:
                # SHORT: SL above => round up; TP below => round down
                if sl_price > 0:
                    sl_price = _round_price_to_tick(sl_price, tick_size, mode="up")
                if tp_price > 0:
                    tp_price = _round_price_to_tick(tp_price, tick_size, mode="down")

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
                tick_size=float(tick_size),
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
              %(ex)s, %(acc)s, %(sym)s, %(pos_uid)s,
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
                "pos_uid": pos_uid,
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
              %(ex)s, %(acc)s, %(sym)s, %(pos_uid)s,
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
                "pos_uid": pos_uid,
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
              %(ex)s, %(acc)s, %(sym)s, %(pos_uid)s, %(sid)s,
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
                "pos_uid": pos_uid,
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
        tick_size: float,
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
        prefix = _sanitize_coid_prefix(str(getattr(self.p, "client_order_id_prefix", "TL") or "TL"))
        tok = _coid_token(pos_uid, n=20)
        cid_entry = f"{prefix}_{tok}_ENTRY"
        cid_sl = f"{prefix}_{tok}_SL"
        cid_tp = f"{prefix}_{tok}_TP"

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

        # Re-anchor precomputed bracket prices to the *actual* average entry.
        # This keeps the same distance (delta) that was computed from the estimated entry.
        try:
            if avg_price > 0 and entry_estimate > 0 and sl_price > 0:
                sl_price = float(avg_price) + (float(sl_price) - float(entry_estimate))
        except Exception:
            pass
        try:
            if avg_price > 0 and entry_estimate > 0 and tp_price > 0:
                tp_price = float(avg_price) + (float(tp_price) - float(entry_estimate))
        except Exception:
            pass

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
                buffer_pct = float(getattr(self.p, "trailing_activation_buffer_pct", 0.20) or 0.20)
                retries = int(getattr(self.p, "trailing_place_retries", 5) or 5)
                retries = max(1, min(10, retries))

                # Prefer current mark price to avoid immediate-trigger (-2021).
                mark = self._get_mark_price_live(symbol, position_side if hedge_mode else None)
                if mark <= 0:
                    mark = avg_price

                last_err: Exception | None = None
                for k in range(retries):
                    extra = buffer_pct * float(k + 1)

                    # Direction rules for Binance trailing:
                    #  - SELL trailing stop: activationPrice must be ABOVE current mark.
                    #  - BUY  trailing stop: activationPrice must be BELOW current mark.
                    if close_side.upper() == "SELL":
                        base_activation = avg_price * (1.0 + activation_pct / 100.0)
                        activation_price = self._safe_price_above_mark(base_activation, mark, tick_size, extra)
                    else:
                        base_activation = avg_price * (1.0 - activation_pct / 100.0)
                        activation_price = self._safe_price_below_mark(base_activation, mark, tick_size, extra)

                    # Round and enforce side vs actual avg entry (Binance is sensitive to triggers).
                    activation_price = _round_price_to_tick(
                        activation_price,
                        tick_size,
                        mode="up" if close_side.upper() == "SELL" else "down",
                    )
                    activation_price = _ensure_tp_trail_side(
                        activation_price,
                        avg_price,
                        tick_size,
                        side,
                        kind="trail_activate",
                    )

                    params = dict(
                        symbol=symbol,
                        side=close_side,
                        type="TRAILING_STOP_MARKET",
                        quantity=float(qty),
                        activationPrice=float(activation_price),
                        callbackRate=float(callback_rate),
                        workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                        newClientOrderId=cid_trl,
                        positionSide=position_side if hedge_mode else None,
                    )
                    if (not hedge_mode) and bool(self.p.reduce_only):
                        params["reduceOnly"] = True

                    try:
                        return self._binance.new_order(**params)
                    except Exception as e:
                        last_err = e
                        if self._is_immediate_trigger_error(e) and k < retries - 1:
                            continue
                        raise

                if last_err is not None:
                    raise last_err
                return None

            if sl_mode in {"stop_market", "stopmarket", "market"}:
                params = dict(
                    symbol=symbol,
                    side=close_side,
                    type="STOP_MARKET",
                    stopPrice=float(_round_price_to_tick(sl_price, tick_size, mode="down" if ledger_side == "LONG" else "up")),
                    closePosition=True,
                    workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                    newClientOrderId=cid_trl,
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
                newClientOrderId=cid_trl,
                positionSide=position_side if hedge_mode else None,
            )
            if (not hedge_mode) and bool(self.p.reduce_only):
                params["reduceOnly"] = True
            return self._binance.new_order(**params)

        def _place_tp() -> Optional[dict]:
            if tp_price <= 0:
                return None

            # TP must be on correct side of *actual* avg entry
            base_tp = tp_price
            try:
                base_tp = _ensure_tp_trail_side(base_tp, avg_price, tick_size, ledger_side, kind="tp")
            except Exception:
                base_tp = tp_price

            if tp_mode in {"take_profit_market", "takeprofit_market", "tp_market", "market"}:
                buffer_pct = float(getattr(self.p, "trailing_activation_buffer_pct", 0.20) or 0.20)
                retries = int(getattr(self.p, "trailing_place_retries", 5) or 5)
                retries = max(1, min(10, retries))

                mark = self._get_mark_price_live(symbol, position_side if hedge_mode else None)
                if mark <= 0:
                    mark = avg_price

                last_err: Exception | None = None
                for k in range(retries):
                    extra = buffer_pct * float(k + 1)

                    # TP trigger must be on the profitable side of current mark, otherwise Binance returns -2021.
                    if ledger_side == "LONG":
                        trig = self._safe_price_above_mark(base_tp, mark, tick_size, extra)
                    else:
                        trig = self._safe_price_below_mark(base_tp, mark, tick_size, extra)

                    params = dict(
                        symbol=symbol,
                        side=close_side,
                        type="TAKE_PROFIT_MARKET",
                        stopPrice=float(trig),
                        closePosition=True,
                        workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                        newClientOrderId=cid_tp,
                        positionSide=position_side if hedge_mode else None,
                    )
                    if (not hedge_mode) and bool(self.p.reduce_only):
                        params["reduceOnly"] = True

                    try:
                        return self._binance.new_order(**params)
                    except Exception as e:
                        last_err = e
                        if self._is_immediate_trigger_error(e) and k < retries - 1:
                            continue
                        raise

                if last_err is not None:
                    raise last_err
                return None

            # tp-limit (TAKE_PROFIT)
            params = dict(
                symbol=symbol,
                side=close_side,
                type="TAKE_PROFIT",
                timeInForce=str(getattr(self.p, "time_in_force", "GTC")),
                quantity=float(qty),
                price=float(base_tp),
                stopPrice=float(base_tp),
                newClientOrderId=cid_tp,
                positionSide=position_side if hedge_mode else None,
            )
            if (not hedge_mode) and bool(self.p.reduce_only):
                params["reduceOnly"] = True
            return self._binance.new_order(**params)

        try:
            # If averaging (scale-in) is enabled and we defer SL until the last add,
            # do NOT place STOP_MARKET SL on initial entry. Trailing (if enabled) is handled separately.
            avg_enabled = bool(getattr(self.p, "averaging_enabled", False)) and int(getattr(self.p, "averaging_max_adds", 0) or 0) > 0
            defer_sl = bool(getattr(self.p, "defer_stop_loss_until_last_add", False))
            if bool(getattr(self.p, "enable_stop_loss", True)) and (not (avg_enabled and defer_sl)):
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
                    order_id=sl_resp.get("orderId") or sl_resp.get("order_id") or sl_resp.get("algoId") or "",
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
                    order_id=tp_resp.get("orderId") or tp_resp.get("order_id") or tp_resp.get("algoId") or "",
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

        # 3.5) Averaging add (conditional MARKET) right after entry (if enabled)
        add_resp: Optional[dict] = None
        try:
            if bool(getattr(self.p, "averaging_enabled", False)) and int(getattr(self.p, "averaging_max_adds", 0) or 0) > 0:
                mult = float(getattr(self.p, "averaging_add_position_multiplier", 1.0) or 1.0)
                if mult > 1.0:
                    # pick a significant level from candles (support/resistance)
                    lv_tf = str(getattr(self.p, "averaging_levels_tf", timeframe) or timeframe)
                    lookback_h = int(getattr(self.p, "averaging_levels_lookback_hours", 168) or 168)
                    left = int(getattr(self.p, "averaging_pivot_left", 3) or 3)
                    right = int(getattr(self.p, "averaging_pivot_right", 3) or 3)
                    tol_pct = float(getattr(self.p, "averaging_level_tolerance_pct", 0.15) or 0.15)
                    min_dist_pct = float(getattr(self.p, "averaging_min_level_distance_pct", 5.0) or 5.0)
                    dist_limit_pct = max(5.0, float(min_dist_pct))

                    # load candles
                    q = """
                    SELECT open_time, high, low, close, volume
                    FROM candles
                    WHERE exchange_id=%(ex)s AND symbol_id=%(sym)s AND interval=%(tf)s
                      AND open_time >= (NOW() AT TIME ZONE 'UTC') - (%(h)s || ' hours')::interval
                    ORDER BY open_time ASC;
                    """
                    rows = list(self.store.query_dict(q, {"ex": int(self.p.exchange_id), "sym": int(symbol_id), "tf": lv_tf, "h": int(lookback_h)}))
                    lows = [float(r.get("low") or 0) for r in rows]
                    highs = [float(r.get("high") or 0) for r in rows]

                    entry_ref = float(avg_price or entry_estimate or 0)
                    level = 0.0
                    if entry_ref > 0 and len(lows) >= (left + right + 5):
                        piv_lows = []
                        piv_highs = []
                        for i in range(left, len(lows) - right):
                            w = lows[i-left:i+right+1]
                            if lows[i] == min(w):
                                piv_lows.append(lows[i])
                            w2 = highs[i-left:i+right+1]
                            if highs[i] == max(w2):
                                piv_highs.append(highs[i])

                        if ledger_side == "LONG":
                            # nearest support below entry
                            below = [x for x in piv_lows if x > 0 and x < entry_ref * (1.0 - dist_limit_pct/100.0)]
                            if below:
                                level = max(below)
                            else:
                                # fallback: 1% below entry
                                level = entry_ref * (1.0 - (dist_limit_pct / 100.0))
                        else:
                            above = [x for x in piv_highs if x > entry_ref * (1.0 + dist_limit_pct/100.0)]
                            if above:
                                level = min(above)
                            else:
                                level = entry_ref * (1.0 + (dist_limit_pct / 100.0))

                    if level > 0:
                        # compute add qty via multiplier: new_qty = cur_qty * mult => add = cur*(mult-1)
                        add_qty = float(qty) * (mult - 1.0)
                        add_qty = _round_qty_to_step(add_qty, qty_step, mode="down")
                        if add_qty >= float(qty_step):
                            cid_add = f"{prefix}_{tok}_ADD1"
                            # Use TAKE_PROFIT_MARKET as conditional entry: BUY triggers when mark <= stopPrice; SELL triggers when mark >= stopPrice
                            add_side = entry_side
                            add_type = "TAKE_PROFIT_MARKET"
                            stop_px = _round_price_to_tick(level, tick_size, mode="down" if add_side == "BUY" else "up")
                            # Protect from -2021 (would immediately trigger): ensure trigger is on the correct side of mark
                            mark_add = 0.0
                            try:
                                mark_add = float(self._get_mark_price(symbol, position_side if hedge_mode else None) or 0.0)
                            except Exception:
                                mark_add = 0.0
                            extra_buf = float(getattr(self.p, "tp_trigger_buffer_pct", 0.25) or 0.25)
                            if add_side == "BUY":
                                stop_px = self._safe_price_below_mark(float(stop_px), float(mark_add), float(tick_size), extra_buf)
                            else:
                                stop_px = self._safe_price_above_mark(float(stop_px), float(mark_add), float(tick_size), extra_buf)

                            def _place_add(i: int):
                                # On retry, push trigger one more tick away from mark
                                px = float(stop_px)
                                if i > 0 and tick_size:
                                    px = px - float(tick_size) * i if add_side == "BUY" else px + float(tick_size) * i
                                    px = _round_price_to_tick(px, float(tick_size), mode="down" if add_side == "BUY" else "up")
                                return self._binance.new_order(
                                    symbol=symbol,
                                    side=add_side,
                                    type=add_type,
                                    stopPrice=float(px),
                                    quantity=float(add_qty),
                                    workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                                    newClientOrderId=cid_add,
                                    positionSide=position_side if hedge_mode else None,
                                )

                            add_resp = _tl_place_algo_with_retry(_place_add, max_tries=int(getattr(self.p, "tp_place_retries", 3) or 3))

                            try:
                                self._upsert_order_shadow(
                                    pos_uid=pos_uid,
                                    order_id=add_resp.get("orderId") or add_resp.get("algoId") or "",
                                    client_order_id=cid_add,
                                    symbol_id=symbol_id,
                                    side=add_side,
                                    order_type=str(add_resp.get("type") or add_type),
                                    qty=float(add_qty),
                                    price=None,
                                    reduce_only=False,
                                    status=str(add_resp.get("status") or "NEW"),
                                )
                            except Exception:
                                pass
        except Exception:
            log.exception("[trade_liquidation][LIVE] failed to place averaging add for %s", symbol)


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
                "add_order": add_resp,
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
            "%(pos_uid)s",
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
                    "pos_uid": str(pos_uid),
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

### --- TL HOTFIX: list screeners/timeframes + ensure methods bound ---
# This section ensures that list-based config params work reliably and that
# methods exist on the TradeLiquidation class even if an earlier edit moved them
# out of the class scope by accident.
def _tl_norm_list(v):
    if v is None:
        return []
    if isinstance(v, (list, tuple, set)):
        return [str(x).strip() for x in v if str(x).strip()]
    s = str(v).strip()
    return [s] if s else []

def _tl_fetch_new_signals(self, limit: int = 50):
    scr = _tl_norm_list(getattr(self.p, "screener_name", None))
    tfs = _tl_norm_list(getattr(self.p, "allowed_timeframes", None))
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
      COALESCE(sf.price_tick, 0) AS price_tick,
      sig.context
    FROM signals sig
    JOIN screeners sc
      ON sc.screener_id = sig.screener_id
    JOIN symbols sym
      ON sym.exchange_id = sig.exchange_id
     AND sym.symbol_id   = sig.symbol_id
    LEFT JOIN symbol_filters sf
      ON sf.exchange_id = sig.exchange_id
     AND sf.symbol_id   = sig.symbol_id
    WHERE sig.exchange_id=%(ex)s
      AND sc.name = ANY(%(scr)s::text[])
      AND sig.timeframe = ANY(%(tfs)s::text[])
      AND sig.status=%(st)s
      AND sig.signal_ts >= now() - (%(age)s::text || ' minutes')::interval
    ORDER BY sig.signal_ts DESC
    LIMIT %(lim)s;
    """
    return list(self.store.query_dict(q, {
        "ex": int(self.p.exchange_id),
        "scr": scr if scr else [str(getattr(self.p, "screener_name", "")).strip()],
        "tfs": tfs if tfs else [str(x) for x in (self.p.allowed_timeframes or [])],
        "st": str(self.p.signal_status_new),
        "age": int(self.p.max_signal_age_minutes),
        "lim": int(limit),
    }))

def _tl_expire_old_new_signals(self) -> int:
    age = self.p.expire_max_age_minutes if getattr(self.p, "expire_max_age_minutes", None) not in (None, 0, "0") else self.p.max_signal_age_minutes
    age = int(age) if age else int(self.p.max_signal_age_minutes)
    limit = int(getattr(self.p, "expire_batch_limit", 0) or 0)
    status_exp = str(getattr(self.p, "expire_status", "EXPIRED") or "EXPIRED").strip().upper()

    scr = _tl_norm_list(getattr(self.p, "screener_name", None))
    tfs = _tl_norm_list(getattr(self.p, "allowed_timeframes", None))

    q = """
    WITH cte AS (
      SELECT sig.id
      FROM signals sig
      JOIN screeners sc ON sc.screener_id = sig.screener_id
      WHERE sig.exchange_id=%(ex)s
        AND sc.name = ANY(%(scr)s::text[])
        AND sig.timeframe = ANY(%(tfs)s::text[])
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
    rows = list(self.store.query_dict(q, {
        "ex": int(self.p.exchange_id),
        "scr": scr if scr else [str(getattr(self.p, "screener_name", "")).strip()],
        "tfs": tfs if tfs else [str(x) for x in (self.p.allowed_timeframes or [])],
        "st": str(self.p.signal_status_new),
        "age": int(age),
        "lim": int(limit) if limit > 0 else 1000000000,
        "newst": status_exp,
    }))
    n = len(rows)
    if n > 0:
        try:
            log.info(
                "[trade_liquidation] expired old signals: %d (age>%dmin, screeners=%s, limit=%s)",
                int(n), int(age), ",".join(scr), str(limit if limit > 0 else "ALL"),
            )
        except Exception:
            pass
    return int(n)

# Bind / override methods on the class (idempotent).
try:
    TradeLiquidation._norm_list = staticmethod(_tl_norm_list)  # optional helper
except Exception:
    pass
try:
    TradeLiquidation._fetch_new_signals = _tl_fetch_new_signals
except Exception:
    pass
try:
    TradeLiquidation._expire_old_new_signals = _tl_expire_old_new_signals
except Exception:
    pass