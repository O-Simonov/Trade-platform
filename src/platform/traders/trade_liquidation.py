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
def _tl_place_algo_with_retry(place_fn, max_tries: int = 3, base_sleep_sec: float = 0.25, log_obj=None):
    """Retry wrapper for placing Binance conditional/algo orders.

    Some code paths call this helper from different methods. Earlier versions had
    it as a nested function, which caused NameError in other methods.
    """
    import time

    tries = max(1, int(max_tries or 1))
    last_exc = None
    for attempt in range(1, tries + 1):
        try:
            # Backward compatible: some call sites define place_fn(attempt)
            try:
                import inspect
                _sig = inspect.signature(place_fn)
                _nparams = len([p for p in _sig.parameters.values() if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)])
            except Exception:
                _nparams = 0

            if _nparams >= 1:
                return place_fn(attempt)
            return place_fn()
        except Exception as e:  # noqa: BLE001
            last_exc = e
            if log_obj is not None:
                try:
                    log_obj.warning("[TL] algo place retry %s/%s failed: %s", attempt, tries, e)
                except Exception:
                    pass
            if attempt < tries:
                time.sleep(base_sleep_sec * attempt)
    # re-raise last error (keeps stack in logs)
    raise last_exc


log = logging.getLogger("traders.trade_liquidation")

# Match averaging add order ids: ..._ADD1, ..._ADD2, ...
_ADD_RE = re.compile(r"_ADD(\d+)$", re.IGNORECASE)


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

    # Hedge (strategy): if enabled, SL is replaced by opening an opposite hedge position (market),
    # validated by EMA(20) trend on 5m candles.
    hedge_koff: float = 1.2  # hedge qty = abs(main_qty) * hedge_koff
    hedge_ema_window: int = 20
    hedge_ema_interval: str = "5m"
    hedge_ema_confirm_bars: int = 3  # require N consecutive EMA moves
    hedge_ema_min_slope_pct: float = 0.0  # minimal EMA slope (% per bar) to validate direction
    hedge_ema_flip_slope_mult: float = 0.7  # close-on-flip uses min_slope * mult
    hedge_cooldown_sec: int = 180  # cooldown between hedge open/close actions
    hedge_close_on_level: bool = True  # close hedge near averaging significant level
    hedge_close_on_ema_flip: bool = True  # close hedge if EMA trend flips against hedge
    hedge_level_tolerance_pct: float = 0.10  # tolerance around computed level for closing hedge

    hedge_reopen_enabled: bool = True
    hedge_reopen_drawdown_pct: float = 0.0

    all_P_L: float = 0.0  # close both positions when combined unrealized PnL >= this USDT

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
    averaging_max_additions: int = 1
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
        params["hedge_koff"] = float(_as_float(params.get("hedge_koff", cls().hedge_koff), cls().hedge_koff) or cls().hedge_koff)
        params["hedge_ema_window"] = int(_as_int(params.get("hedge_ema_window", cls().hedge_ema_window), cls().hedge_ema_window) or cls().hedge_ema_window)
        params["hedge_ema_interval"] = str(params.get("hedge_ema_interval", cls().hedge_ema_interval) or cls().hedge_ema_interval)
        params["hedge_ema_confirm_bars"] = int(_as_int(params.get("hedge_ema_confirm_bars", cls().hedge_ema_confirm_bars), cls().hedge_ema_confirm_bars) or cls().hedge_ema_confirm_bars)
        params["hedge_ema_min_slope_pct"] = float(_as_float(params.get("hedge_ema_min_slope_pct", cls().hedge_ema_min_slope_pct), cls().hedge_ema_min_slope_pct) or cls().hedge_ema_min_slope_pct)
        params["hedge_ema_flip_slope_mult"] = float(_as_float(params.get("hedge_ema_flip_slope_mult", cls().hedge_ema_flip_slope_mult), cls().hedge_ema_flip_slope_mult) or cls().hedge_ema_flip_slope_mult)
        params["hedge_cooldown_sec"] = int(_as_int(params.get("hedge_cooldown_sec", cls().hedge_cooldown_sec), cls().hedge_cooldown_sec) or cls().hedge_cooldown_sec)
        params["hedge_close_on_level"] = _as_bool(params.get("hedge_close_on_level", cls().hedge_close_on_level), cls().hedge_close_on_level)
        params["hedge_close_on_ema_flip"] = _as_bool(params.get("hedge_close_on_ema_flip", cls().hedge_close_on_ema_flip), cls().hedge_close_on_ema_flip)
        params["hedge_level_tolerance_pct"] = float(_as_float(params.get("hedge_level_tolerance_pct", cls().hedge_level_tolerance_pct), cls().hedge_level_tolerance_pct) or cls().hedge_level_tolerance_pct)
        params["hedge_reopen_enabled"] = _as_bool(params.get("hedge_reopen_enabled", cls().hedge_reopen_enabled), cls().hedge_reopen_enabled)
        params["hedge_reopen_drawdown_pct"] = float(_as_float(params.get("hedge_reopen_drawdown_pct", cls().hedge_reopen_drawdown_pct), cls().hedge_reopen_drawdown_pct) or cls().hedge_reopen_drawdown_pct)
        params["all_P_L"] = float(_as_float(params.get("all_P_L", cls().all_P_L), cls().all_P_L) or cls().all_P_L)
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

        # logger alias (module-level logger is `log`)
        self.log = log

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

        # Runtime caches
        #   pos_uid -> last avg_price that TP/TRAIL were anchored to
        self._avg_anchor_cache: Dict[str, float] = {}
        # throttle bracket order replace (TP/Trailing) to avoid churn
        self._bracket_replace_last_ts: Dict[Tuple[str, str], float] = {}

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
        provide a thin wrapper around the module log.
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



    def _is_our_position_by_symbol_side(
        self,
        symbol: str,
        side: str,
        open_orders_all: Any,
        open_algo_orders_all: Any,
        raw_meta: Any,
    ) -> bool:
        """Decide if position should be treated as 'ours' for reconcile purposes.

        Variant A: adjust ONLY our positions, but detect them reliably even if pos_uid is not embedded
        into client IDs (Binance does not include pos_uid).

        Heuristic (ordered):
          1) raw_meta live_entry.entry_order.clientOrderId starts with our prefix (e.g. 'TL_')
          2) any OPEN regular order for this symbol has clientOrderId starting with prefix
          3) any OPEN algo order for this symbol has clientAlgoId starting with prefix
          4) recent DB orders for this symbol have client_order_id starting with prefix (last 7 days)

        If unsure -> False (treat as external).
        """
        sym = str(symbol or "").strip().upper()
        sd = str(side or "").strip().upper()
        if not sym or sd not in {"LONG", "SHORT"}:
            return False

        prefix = str(getattr(self.p, "client_order_id_prefix", "TL") or "TL").strip()
        if prefix and not prefix.endswith("_"):
            prefix_ = prefix + "_"
        else:
            prefix_ = prefix

        # 1) raw_meta marker
        try:
            meta = raw_meta
            if isinstance(meta, str) and meta.strip():
                try:
                    meta = json.loads(meta)
                except Exception:
                    meta = None
            if isinstance(meta, dict):
                le = meta.get("live_entry") if isinstance(meta.get("live_entry"), dict) else None
                if le:
                    eo = le.get("entry_order") if isinstance(le.get("entry_order"), dict) else None
                    if eo:
                        coid = str(eo.get("clientOrderId") or "")
                        if coid.startswith(prefix_):
                            return True
        except Exception:
            pass

        # 2) open regular orders snapshot
        try:
            rows = open_orders_all if isinstance(open_orders_all, list) else []
            for o in rows:
                if str((o or {}).get("symbol") or "").strip().upper() != sym:
                    continue
                coid = str((o or {}).get("clientOrderId") or "")
                if coid.startswith(prefix_):
                    return True
        except Exception:
            pass

        # 3) open algo orders snapshot
        try:
            rows = open_algo_orders_all if isinstance(open_algo_orders_all, list) else []
            for o in rows:
                if str((o or {}).get("symbol") or "").strip().upper() != sym:
                    continue
                coid = str((o or {}).get("clientAlgoId") or "")
                if coid.startswith(prefix_):
                    return True
        except Exception:
            pass

        # 4) DB fallback (recent orders)
        try:
            row = self.store.query_one(
                """
                SELECT 1
                FROM orders o
                JOIN symbols s ON s.symbol_id=o.symbol_id AND s.exchange_id=o.exchange_id
                WHERE o.exchange_id=%(ex)s
                  AND o.account_id=%(acc)s
                  AND o.strategy_id=%(sid)s
                  AND s.symbol=%(sym)s
                  AND o.client_order_id LIKE %(pfx)s
                  AND o.updated_at >= (now() - interval '7 days')
                LIMIT 1
                """,
                {
                    "ex": int(self.p.exchange_id),
                    "acc": int(self.account_id),
                    "sid": str(self.STRATEGY_ID),
                    "sym": sym,
                    "pfx": prefix_ + "%",
                },
            )
            if row:
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

        # LIVE: averaging adds maintenance.
        # Ensure we have the next ADD order queued, but never exceed max_adds.
        # We place at most ONE missing ADD per cycle to avoid spamming Binance endpoints.
        if self._is_live and bool(getattr(self.p, "averaging_enabled", False)) and self._cfg_max_adds() > 0:
            try:
                placed_any = False
                for p in open_positions or []:
                    if self._live_ensure_next_add_order(pos=p):
                        placed_any = True
                        break
                if placed_any:
                    # refresh snapshot next cycle; current cycle continues
                    pass
            except Exception:
                log.debug("[trade_liquidation][LIVE][AVG] ensure_next_add failed", exc_info=True)
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
        oo_algo_all = self._rest_snapshot_get("open_algo_orders_all")
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
        # New: auto-adjust ledger position_value_usdt (mark-to-market) independently of qty.
        auto_adjust_value = bool(getattr(self.p, "reconcile_auto_adjust_ledger_value_usdt", True))
        # New: refresh value_usdt even when within tolerance (to keep ledger close to exchange mark).
        auto_refresh_value = bool(getattr(self.p, "reconcile_refresh_position_value_usdt", False))
        refresh_min_delta = float(getattr(self.p, "reconcile_value_refresh_min_delta_usdt", 0.1) or 0.1)
        adjust_only_our = bool(getattr(self.p, "reconcile_adjust_only_our_positions", True))
        ignore_external = bool(getattr(self.p, "reconcile_ignore_external_positions", True))
        ext_lvl = str(getattr(self.p, "reconcile_external_log_level", "debug") or "debug").strip().lower()
        oo_all = self._rest_snapshot_get("open_orders_all")
        # Needed for "our position" detection (clientAlgoId TL_...)
        oo_algo_all = self._rest_snapshot_get("open_algo_orders_all")

        sym_map = self._symbols_map()

        # 1) exchange positions
        pr = self._rest_snapshot_get("position_risk")
        rows = pr if isinstance(pr, list) else []
        ex_pos: Dict[Tuple[str, str], float] = {}
        ex_usdt: Dict[Tuple[str, str], float] = {}
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
            try:
                notional = abs(_safe_float(it.get('notional'), 0.0))
            except Exception:
                notional = 0.0
            if notional <= 0:
                mp = _safe_float(it.get('markPrice'), 0.0)
                if mp > 0:
                    notional = abs(float(amt)) * float(mp)
            ex_usdt[(sym, side)] = abs(float(notional))

        # 2) ledger positions
        led = list(
            self.store.query_dict(
                """
                SELECT symbol_id, pos_uid, side, status, qty_current, position_value_usdt, entry_price, avg_price, raw_meta, opened_at
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

            # Qty/Value mismatch checks.
            led_val = float(_safe_float(p.get("position_value_usdt"), 0.0))
            ex_val = float(ex_usdt.get((sym, side), 0.0) or 0.0)
            usdt_tol = float(getattr(self.p, "reconcile_value_tolerance_usdt", 0.5) or 0.5)

            qty_diff = abs(ex_qty - led_qty)
            usdt_diff = abs(ex_val - led_val) if (ex_val > 0 and led_val > 0) else 0.0

            if qty_diff > tol or usdt_diff > usdt_tol:
                if qty_diff > tol:
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
                if usdt_diff > usdt_tol and bool(getattr(self.p, "reconcile_log_diffs", True)):
                    log.warning(
                        "[trade_liquidation][RECONCILE] value mismatch %s %s: ledger_usdt=%.8f exchange_usdt=%.8f diff=%.8f (pos_uid=%s)",
                        sym,
                        side,
                        float(led_val),
                        float(ex_val),
                        float(ex_val - led_val),
                        str(p.get("pos_uid") or ""),
                    )

                
                # Micro-step: optionally auto-adjust ledger qty_current and/or position_value_usdt to exchange values.
                # - qty_current adjustment is OFF by default (reconcile_auto_adjust_ledger_qty_current=False)
                # - value_usdt adjustment is ON by default (reconcile_auto_adjust_ledger_value_usdt=True) but only when diff > tolerance
                # - value_usdt refresh is OFF by default (reconcile_refresh_position_value_usdt=False) and keeps ledger mark-to-market (writes only if delta > reconcile_value_refresh_min_delta_usdt)
                if (auto_adjust_qty or auto_adjust_value or auto_refresh_value) and ex_qty > 0:
                    try:
                        pos_uid = str(p.get("pos_uid") or "")
                        if adjust_only_our and not self._is_our_position_by_symbol_side(sym, side, oo_all, oo_algo_all, p.get("raw_meta")):
                            log.info(
                                "[trade_liquidation][RECONCILE] skip auto-adjust qty/value for external pos_uid=%s (%s %s)",
                                pos_uid,
                                sym,
                                side,
                            )
                            continue

                        do_qty = bool(auto_adjust_qty) and (qty_diff > tol)
                        do_val = False
                        if ex_val > 0:
                            if bool(auto_refresh_value):
                                # refresh even when within tolerance (but avoid excessive writes)
                                if led_val <= 0:
                                    do_val = True
                                elif abs(ex_val - led_val) > float(refresh_min_delta):
                                    do_val = True
                            elif bool(auto_adjust_value):
                                # adjust only when beyond tolerance
                                if usdt_diff > usdt_tol:
                                    do_val = True

                        if not do_qty and not do_val:
                            continue

                        new_qty_f = float(ex_qty) if do_qty else float(led_qty)
                        new_val_d = _dec(abs(ex_val)).quantize(Decimal("0.00000001")) if do_val else _dec(led_val).quantize(Decimal("0.00000001"))

                        if self._pl_has_raw_meta:
                            meta = {
                                "reconcile_adjust": {
                                    "old_qty": float(led_qty),
                                    "new_qty": float(new_qty_f),
                                    "old_usdt": float(led_val),
                                    "new_usdt": float(new_val_d),
                                    "ts": _utc_now().isoformat(),
                                    "mode": ("qty" if do_qty else "") + ("+val" if do_val else ""),
                                }
                            }
                            self.store.execute(
                                """
                                UPDATE position_ledger
                                SET qty_current=%(q)s,
                                    position_value_usdt=%(v)s,
                                    updated_at=now(),
                                    raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb
                                WHERE exchange_id=%(ex)s
                                  AND account_id=%(acc)s
                                  AND pos_uid=%(pos_uid)s
                                  AND status='OPEN'
                                """,
                                {
                                    "q": float(new_qty_f),
                                    "v": new_val_d,
                                    "meta": json.dumps(meta),
                                    "ex": int(self.p.exchange_id),
                                    "acc": int(self.account_id),
                                    "pos_uid": pos_uid,
                                },
                            )
                        else:
                            self.store.execute(
                                """
                                UPDATE position_ledger
                                SET qty_current=%(q)s,
                                    position_value_usdt=%(v)s,
                                    updated_at=now()
                                WHERE exchange_id=%(ex)s
                                  AND account_id=%(acc)s
                                  AND pos_uid=%(pos_uid)s
                                  AND status='OPEN'
                                """,
                                {
                                    "q": float(new_qty_f),
                                    "v": new_val_d,
                                    "ex": int(self.p.exchange_id),
                                    "acc": int(self.account_id),
                                    "pos_uid": pos_uid,
                                },
                            )

                        if bool(getattr(self.p, "reconcile_log_diffs", True)):
                            log.info(
                                "[trade_liquidation][RECONCILE] adjusted ledger %s %s old_qty=%.8f new_qty=%.8f old_usdt=%.8f new_usdt=%.8f (pos_uid=%s)",
                                sym,
                                side,
                                float(led_qty),
                                float(new_qty_f),
                                float(led_val),
                                float(new_val_d),
                                pos_uid,
                            )
                    except Exception:
                        log.exception(
                            "[trade_liquidation][RECONCILE] failed to auto-adjust ledger qty/value (pos_uid=%s)",
                            str(p.get("pos_uid") or ""),
                        )


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

        # --- bracket replace throttling / tolerance knobs ---
        REPLACE_COOLDOWN_SEC = float(getattr(self.p, "replace_cooldown_sec", 60) or 60)
        AVG_EPS_PCT = float(getattr(self.p, "avg_eps_pct", 0.02) or 0.02)  # percent
        PRICE_EPS_TICKS = int(getattr(self.p, "price_eps_ticks", 2) or 2)
        now_ts = time.time()

        max_age_h = float(getattr(self.p, "recovery_max_age_hours", 48) or 48)
        place_sl = bool(getattr(self.p, "recovery_place_sl", True))
        # Hedge strategy: do not auto-place SL brackets (hedge replaces SL)
        if bool(getattr(self.p, "hedge_enabled", False)):
            place_sl = False
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

        # --- Reconcile DB: if TP/TRL/SL were manually canceled on the exchange,
        # the DB may still show them as NEW and the trader will think protection exists.
        # We treat the exchange as source of truth and mark such stale DB rows as CANCELED.
        try:
            active_client_ids: set[str] = set()
            # regular orders (openOrders): open_by_symbol already stores clientOrderId strings
            for _ids in open_by_symbol.values():
                try:
                    active_client_ids.update(str(x) for x in (_ids or []) if x)
                except Exception:
                    pass

            # conditional orders (openAlgoOrders): open_algo_by_symbol stores list[dict]
            for _rows in open_algo_by_symbol.values():
                try:
                    for o in (_rows or []):
                        if isinstance(o, dict):
                            cid = o.get("clientAlgoId") or o.get("clientOrderId") or o.get("newClientOrderId")
                            if cid:
                                active_client_ids.add(str(cid))
                        elif o:
                            # if upstream ever stores ids directly
                            active_client_ids.add(str(o))
                except Exception:
                    pass

            pos_uids = [str(r.get("pos_uid")) for r in led if r.get("pos_uid")]
            if pos_uids:
                like_tp = r"TL\_%\_TP%"
                like_trl = r"TL\_%\_TRL%"
                like_sl = r"TL\_%\_SL%"

                if active_client_ids:
                    self.store.execute(
                        '''
                        UPDATE public.orders
                           SET status = 'CANCELED',
                               updated_at = NOW(),
                               source = 'reconcile_exchange'
                         WHERE strategy_id = 'trade_liquidation'
                           AND pos_uid = ANY(%s)
                           AND status IN ('NEW','PARTIALLY_FILLED')
                           AND (
                                 client_order_id LIKE %s ESCAPE '\\'
                              OR client_order_id LIKE %s ESCAPE '\\'
                              OR client_order_id LIKE %s ESCAPE '\\'
                           )
                           AND NOT (client_order_id = ANY(%s))
                        ''',
                        (pos_uids, like_tp, like_trl, like_sl, list(active_client_ids)),
                    )
                else:
                    # No active TP/TRL/SL on exchange → cancel all DB-protective orders for these positions
                    self.store.execute(
                        '''
                        UPDATE public.orders
                           SET status = 'CANCELED',
                               updated_at = NOW(),
                               source = 'reconcile_exchange'
                         WHERE strategy_id = 'trade_liquidation'
                           AND pos_uid = ANY(%s)
                           AND status IN ('NEW','PARTIALLY_FILLED')
                           AND (
                                 client_order_id LIKE %s ESCAPE '\\'
                              OR client_order_id LIKE %s ESCAPE '\\'
                              OR client_order_id LIKE %s ESCAPE '\\'
                           )
                        ''',
                        (pos_uids, like_tp, like_trl, like_sl),
                    )
        except Exception:
            log.exception("[TL][auto-recovery] reconcile stale TP/TRL/SL in DB failed")

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
            # If our TP exists but trigger price no longer matches (e.g. after averaging fill),
            # cancel it so we can recreate at the new correct level.
            try:
                tp_order = None
                for ao in (open_algo_by_symbol.get(sym) or []):
                    if str(ao.get("clientAlgoId") or "") == cid_tp:
                        tp_order = ao
                        break
                if tp_order is not None:
                    try:
                        tp_trg = float(tp_order.get("triggerPrice") or tp_order.get("stopPrice") or 0.0)
                    except Exception:
                        tp_trg = 0.0
                    tol = max(
                        float(tick or 0.0) * float(PRICE_EPS_TICKS),
                        abs(float(tp_price)) * (float(AVG_EPS_PCT) / 100.0),
                        1e-12,
                    )
                    if tp_trg > 0.0 and abs(tp_trg - float(tp_price)) > tol:
                        # cancel stale TP (throttle to avoid churn)
                        _last = float(self._bracket_replace_last_ts.get((pos_uid, "tp"), 0.0) or 0.0)
                        if (now_ts - _last) >= float(REPLACE_COOLDOWN_SEC):
                            try:
                                self._binance.cancel_algo_order(symbol=sym, clientAlgoId=cid_tp)
                            except Exception:
                                pass
                            has_tp = False
                            self._bracket_replace_last_ts[(pos_uid, "tp")] = now_ts
            except Exception:
                pass

            # Ensure TP exists (and matches current avg/entry-based level)
            if not has_tp:
                try:
                    tp_side = "SELL" if side == "LONG" else "BUY"
                    self._binance.new_algo_order(
                        symbol=sym,
                        side=tp_side,
                        type="TAKE_PROFIT_MARKET",
                        closePosition="true",
                        workingType="MARK_PRICE",
                        positionSide=side,
                        algoType="CONDITIONAL",
                        clientAlgoId=cid_tp,
                        triggerPrice=str(tp_price),
                    )
                    has_tp = True
                except Exception as _e:
                    log.warning("[trade_liquidation][RECOVER] failed to place TP %s %s: %s", sym, side, _e)

            # place missing orders
            
            if place_sl and (not has_sl) and sl_price > 0:
                # Defer STOP_MARKET SL until last averaging add (if enabled)
                defer_sl = bool(getattr(self.p, "defer_stop_loss_until_last_add", False))
                avg_enabled = bool(getattr(self.p, "averaging_enabled", False))
                cfg_max_adds = int(self._cfg_max_adds() or 0)
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
                            # In hedge mode, SL is implemented as a conditional market order that OPENS the opposite hedge leg.
                            if hedge_mode and bool(getattr(self.p, "hedge_enabled", False)):
                                hedge_koff = float(getattr(self.p, "hedge_koff", 1.0) or 1.0)
                                hedge_qty = float(abs(float(qty_use)) * hedge_koff)
                                hedge_ps = "SHORT" if position_side == "LONG" else "LONG"
                                params = dict(
                                    symbol=sym,
                                    side=close_side,
                                    type="STOP_MARKET",
                                    stopPrice=float(sl_price),
                                    quantity=hedge_qty,
                                    workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                                    newClientOrderId=cid_trl,
                                    positionSide=hedge_ps,
                                )
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
                                        mark_sl = float(self._get_mark_price(sym) or 0.0)
                                    except Exception:
                                        mark_sl = 0.0
                                    if mark_sl <= 0:
                                        try:
                                            mark_sl = float(self._get_last_price(sym) or 0.0)
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

            # If TRAILING exists but based on old entry/avg price, cancel so it can be re-placed on the new level.
            # This also covers manual position additions that change avgEntryPrice on the exchange.
            if cid_trl and has_trl:
                try:
                    # Find existing trailing algo order (if any)
                    trl_ao = None
                    for ao in open_algo_by_symbol.get(sym, []) or []:
                        if str(ao.get("clientAlgoId") or "") == str(cid_trl):
                            trl_ao = ao
                            break

                    if trl_ao is not None:
                        # Desired activation price from current exchange entry price
                        mark = float(ex_mark_map.get(sym) or 0.0)
                        activation_pct = float(getattr(self.p, "trailing_activation_pct", 0.0) or 0.0)
                        activation_buf_pct = float(getattr(self.p, "trailing_activation_buffer_pct", 0.0) or 0.0)
                        activation = max(0.0, activation_pct - activation_buf_pct)
                        ref_price = float(entry)

                        if close_side == "SELL":
                            desired_act = ref_price * (1.0 + activation / 100.0)
                            desired_act = _tl_safe_trigger_price(desired_act, mark, "above", tick=tick)
                        else:
                            desired_act = ref_price * (1.0 - activation / 100.0)
                            desired_act = _tl_safe_trigger_price(desired_act, mark, "below", tick=tick)

                        # Compare with current activation price
                        cur_act = trl_ao.get("activatePrice") or trl_ao.get("triggerPrice")
                        try:
                            cur_act_f = float(cur_act) if cur_act is not None else None
                        except Exception:
                            cur_act_f = None

                        # Tolerance: 2 ticks
                        tol = max(
                    float(tick or 0.0) * float(PRICE_EPS_TICKS),
                    abs(float(tr_activate)) * (float(AVG_EPS_PCT) / 100.0),
                    1e-12,
                )
                        if cur_act_f is None or abs(cur_act_f - float(desired_act)) > tol:
                            self.log.info(
                                f"[trade_liquidation][auto-recovery] stale TRL {sym} {pos_side}: "
                                f"cur_activate={cur_act_f} desired_activate={desired_act} -> cancel+recreate"
                            )
                            _last = float(self._bracket_replace_last_ts.get((pos_uid, "trl"), 0.0) or 0.0)
                            if (now_ts - _last) < float(REPLACE_COOLDOWN_SEC):
                                continue
                            self._bracket_replace_last_ts[(pos_uid, "trl")] = now_ts
                            try:
                                self._binance.cancel_algo_order(symbol=sym, clientAlgoId=cid_trl)
                            except Exception:
                                # fallback - might be a regular order
                                try:
                                    self._binance.cancel_order(symbol=sym, origClientOrderId=cid_trl)
                                except Exception:
                                    pass
                            has_trl = False
                except Exception:
                    # Don't break recovery loop on debug/compare errors
                    pass
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
                            # fallback: reuse ticks from sym meta if present
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


        # Respect max additions cap: if configured max_adds < 1, never place ADD1.
        max_adds_cfg = int(self._cfg_max_adds() or 0)
        if max_adds_cfg < 1:
            return

        # If ledger already indicates we reached the cap (e.g. ADD1 filled previously),
        # do not place a new ADD1 during recovery.
        try:
            rows = list(
                self.store.query_dict(
                    """
                    SELECT COALESCE(scale_in_count, 0) AS scale_in_count
                    FROM position_ledger
                    WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s
                      AND status='OPEN' AND source='live' AND strategy_id=%(sid)s
                    LIMIT 1;
                    """,
                    {
                        "ex": int(self.exchange_id),
                        "acc": int(self.account_id),
                        "pos_uid": str(pos_uid),
                        "sid": self.STRATEGY_ID,
                    },
                )
            )
            if rows and int(rows[0].get("scale_in_count") or 0) >= max_adds_cfg:
                return
        except Exception:
            # best-effort, non-fatal
            pass

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
        max_adds = int(self._cfg_max_adds() or 1)
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
    # LIVE: Averaging (scale-in) helpers
    # ----------------------------------------------------------

    def _cfg_max_adds(self) -> int:
        """Read max additions from config with backward-compatible keys.

        IMPORTANT:
          - Many configs still use `averaging_max_adds`.
          - `averaging_max_additions` has a non-zero default in Params, so we must not
            blindly prefer it when the legacy key is explicitly provided in config.
        Supported keys (priority):
          1) averaging_max_adds (legacy, if provided)
          2) averaging_max_additions (new)
          3) avg_max_adds (very old)
        """
        try:
            # If legacy key exists in config/extras, prefer it.
            v_legacy = None
            try:
                v_legacy = getattr(self.p, "averaging_max_adds", None)
            except Exception:
                v_legacy = None

            # New key (may be defaulted)
            v_new = getattr(self.p, "averaging_max_additions", None)

            # Heuristic: if legacy is set (not None) and differs from the new default value,
            # treat legacy as the source of truth.
            if v_legacy is not None:
                try:
                    n_legacy = int(v_legacy)
                except Exception:
                    n_legacy = 0
                try:
                    n_new = int(v_new) if v_new is not None else None
                except Exception:
                    n_new = None

                # If user set legacy and new is missing OR equals Params default (1),
                # prefer legacy.
                if n_new is None or n_new == 1 or n_new != n_legacy:
                    return max(0, int(n_legacy))

            # Otherwise fall back to new key
            if v_new is not None:
                return max(0, int(v_new))

            # Very old fallback
            v_old = getattr(self.p, "avg_max_adds", None)
            return max(0, int(v_old or 0))
        except Exception:
            return 0


    def _cfg_averaging_min_level_distance_pct(self) -> float:
        """Read averaging min level distance percent from config with backward-compatible keys.

        Reason:
          `averaging_min_level_distance_pct` has a non-zero default (15.0). If older configs use
          a legacy key, we must not accidentally ignore it just because the new key is defaulted.

        Supported keys (priority):
          1) averaging_min_level_distance_pct (new)
          2) min_level_distance_pct (legacy / older configs)
          3) avg_min_level_distance_pct (very old)
        """
        DEFAULT = 15.0
        try:
            v_new = getattr(self.p, "averaging_min_level_distance_pct", None)

            v_legacy = None
            try:
                v_legacy = getattr(self.p, "min_level_distance_pct", None)
            except Exception:
                v_legacy = None

            v_old = getattr(self.p, "avg_min_level_distance_pct", None)

            # If user provided legacy key and the new value looks like the default, prefer legacy.
            if v_legacy is not None:
                try:
                    n_legacy = float(v_legacy)
                except Exception:
                    n_legacy = DEFAULT
                try:
                    n_new = float(v_new) if v_new is not None else None
                except Exception:
                    n_new = None

                if n_new is None or abs(n_new - DEFAULT) < 1e-9 or abs(n_new - n_legacy) > 1e-9:
                    return max(0.0, float(n_legacy))

            if v_new is not None:
                return max(0.0, float(v_new))

            if v_old is not None:
                return max(0.0, float(v_old))

            return DEFAULT
        except Exception:
            return DEFAULT

    def _parse_add_n(self, client_id: str) -> int:
        """Extract ADD number from client_order_id (.._ADD1/.._ADD2..)."""
        try:
            m = _ADD_RE.search(str(client_id or ""))
            if not m:
                return 0
            return max(0, int(m.group(1)))
        except Exception:
            return 0

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

        token: str | None = None

        # 1) Prefer active series token (NEW/PARTIALLY_FILLED)
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
              AND upper(coalesce(status,'')) IN ('NEW','PARTIALLY_FILLED')
            ORDER BY updated_at DESC
            LIMIT 50;
            """
            rows = list(
                self.store.query_dict(
                    qtok,
                    {
                        "ex": int(self.exchange_id),
                        "acc": int(self.account_id),
                        "sid": self.STRATEGY_ID,
                        "sym": sym_u,
                        "oside": desired_order_side,
                        "like_any": f"{prefix}_%_ADD%",
                    },
                )
            )
            for r in rows:
                cid = str(r.get("client_order_id") or "")
                m = re.match(rf"^{re.escape(prefix)}_([^_]+)_ADD\d+$", cid)
                if m:
                    token = m.group(1)
                    break
        except Exception:
            token = None

        # 2) Fallback to the most recent completed series token
        if token is None:
            try:
                qtok2 = """
                SELECT client_order_id
                FROM public.orders
                WHERE exchange_id=%(ex)s
                  AND account_id=%(acc)s
                  AND strategy_id=%(sid)s
                  AND upper(symbol)=%(sym)s
                  AND upper(side)=%(oside)s
                  AND client_order_id LIKE %(like_any)s
                  AND upper(coalesce(status,'')) IN ('FILLED','PARTIALLY_FILLED')
                ORDER BY updated_at DESC
                LIMIT 50;
                """
                rows = list(
                    self.store.query_dict(
                        qtok2,
                        {
                            "ex": int(self.exchange_id),
                            "acc": int(self.account_id),
                            "sid": self.STRATEGY_ID,
                            "sym": sym_u,
                            "oside": desired_order_side,
                            "like_any": f"{prefix}_%_ADD%",
                        },
                    )
                )
                for r in rows:
                    cid = str(r.get("client_order_id") or "")
                    m = re.match(rf"^{re.escape(prefix)}_([^_]+)_ADD\d+$", cid)
                    if m:
                        token = m.group(1)
                        break
            except Exception:
                token = None

        # 3) Compute adds_done as MAX(ADDn) among FILLED/PARTIALLY_FILLED for that token
        adds_done = 0
        if token:
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
                ORDER BY updated_at DESC
                LIMIT 1000;
                """
                rows = list(
                    self.store.query_dict(
                        qmax,
                        {
                            "ex": int(self.exchange_id),
                            "acc": int(self.account_id),
                            "sid": self.STRATEGY_ID,
                            "sym": sym_u,
                            "oside": desired_order_side,
                            "like_tok": f"{prefix}_{token}_ADD%",
                        },
                    )
                )
                done: set[int] = set()
                for r in rows:
                    n = self._parse_add_n(str(r.get("client_order_id") or ""))
                    if n > 0:
                        done.add(n)
                adds_done = max(done) if done else 0
            except Exception:
                adds_done = 0
        try:
            self.store.execute(
                """
                UPDATE public.position_ledger
                SET scale_in_count = GREATEST(COALESCE(scale_in_count,0), %(n)s),
                    updated_at = now()
                WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s
                  AND status='OPEN' AND source='live' AND strategy_id=%(sid)s;
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


    def _live_ensure_hedge_after_last_add(
        self,
        *,
        pos: dict,
        sym: str,
        pos_side: str,
        pos_uid: str,
        prefix: str,
        tok: str,
        adds_done: int,
        max_adds: int,
    ) -> bool:
        """After the last allowed add, place a *conditional market* hedge opener.

        This is NOT the main trailing order. We use a dedicated client id:
          - {prefix}_{tok}_HEDGE

        Trigger is derived from the last add fill and `sl_after_last_add_distance_pct`.
        Quantity is derived from the *exchange* position size (positionRisk.positionAmt)
        multiplied by `hedge_koff` and rounded to qty step.

        We also cancel a legacy hedge order that used `{prefix}_{tok}_TRL` with type STOP_MARKET.
        """
        try:
            dist_pct = float(getattr(self.p, "sl_after_last_add_distance_pct", 0.0) or 0.0)
        except Exception:
            dist_pct = 0.0
        if dist_pct <= 0:
            return False

        if int(adds_done) < int(max_adds) or int(max_adds) <= 0:
            return False

        last_n = int(min(int(adds_done), int(max_adds)))
        last_fill = float(self._live_last_add_fill_price(sym=sym, prefix=prefix, tok=tok, add_n=last_n) or 0.0)
        if last_fill <= 0:
            return False

        # Resolve symbol_id for DB filtering (orders/order_fills store symbol_id)
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
        if symbol_id <= 0:
            return False

        # Compare by DB write time (orders.updated_at), not fill time (order_fills.ts).
        last_add_seen_at = None
        hedge_seen_at = None
        try:
            cid_add = f"{prefix}_{tok}_ADD{int(last_n)}"
            row = self.store.query_one(
                """
                SELECT MAX(updated_at)
                  FROM public.orders
                 WHERE exchange_id=%(ex)s
                   AND account_id=%(acc)s
                   AND symbol_id=%(symbol_id)s
                   AND client_order_id=%(cid)s
                   AND status IN ('FILLED','PARTIALLY_FILLED');
                """,
                {"ex": int(self.exchange_id), "acc": int(self.account_id), "symbol_id": int(symbol_id), "cid": cid_add},
            )
            last_add_seen_at = row[0] if row else None
        except Exception:
            last_add_seen_at = None

        cid_hedge = f"{prefix}_{tok}_HEDGE"
        try:
            row = self.store.query_one(
                """
                SELECT MAX(updated_at)
                  FROM public.orders
                 WHERE exchange_id=%(ex)s
                   AND account_id=%(acc)s
                   AND symbol_id=%(symbol_id)s
                   AND client_order_id=%(cid)s;
                """,
                {"ex": int(self.exchange_id), "acc": int(self.account_id), "symbol_id": int(symbol_id), "cid": cid_hedge},
            )
            hedge_seen_at = row[0] if row else None
        except Exception:
            hedge_seen_at = None

        must_refresh = False
        try:
            if last_add_seen_at is not None and (hedge_seen_at is None or hedge_seen_at < last_add_seen_at):
                must_refresh = True
        except Exception:
            must_refresh = False

        # Anti-churn: if we very recently placed after-last-add hedge, don't cancel/recreate it
        # even if DB snapshots lag behind. This uses hedge_cooldown_sec from config.
        try:
            cd_sec = float(getattr(self.p, "hedge_cooldown_sec", 0.0) or 0.0)
        except Exception:
            cd_sec = 0.0
        if cd_sec and cd_sec > 0 and self._pl_has_raw_meta:
            try:
                rm = pos.get("raw_meta") if isinstance(pos.get("raw_meta"), dict) else {}
                ts_s = ((rm.get("live_entry") or {}).get("after_last_add_hedge") or {}).get("ts")
                if ts_s:
                    now_ms = self._to_epoch_ms(_utc_now()) or 0
                    ts_ms = self._to_epoch_ms(ts_s) or 0
                    if now_ms and ts_ms and (now_ms - ts_ms) < int(cd_sec * 1000):
                        # don't touch the hedge too often
                        must_refresh = False
            except Exception:
                pass

        side_u = str(pos_side or "").upper()
        close_side = "SELL" if side_u == "LONG" else "BUY"

        # Trigger price from last add fill
        if side_u == "LONG":
            stop_price = last_fill * (1.0 - dist_pct / 100.0)
        else:
            stop_price = last_fill * (1.0 + dist_pct / 100.0)

        # Tick rounding
        tick = 0.0
        try:
            tick = float(self._price_tick_for_symbol(sym) or 0.0)
        except Exception:
            tick = 0.0
        if tick and tick > 0:
            stop_price = float(_round_price_to_tick(stop_price, tick, mode=("down" if close_side == "SELL" else "up")))

        # Avoid immediate trigger wrt mark
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
            if close_side == "BUY" and stop_price <= mark:
                stop_price = float(_round_price_to_tick(mark + tick, tick, mode="up"))
            if close_side == "SELL" and stop_price >= mark:
                stop_price = float(_round_price_to_tick(mark - tick, tick, mode="down"))

        # Find open algos by clientAlgoId
        open_algos = self._rest_snapshot_get("open_algo_orders_all") or []
        found_hedge = None
        found_legacy = None
        legacy_cid = f"{prefix}_{tok}_TRL"
        for ao in open_algos if isinstance(open_algos, list) else []:
            try:
                if str(ao.get("symbol") or "").upper() != str(sym).upper():
                    continue
                cid = str(ao.get("clientAlgoId") or "")
                otype = str(ao.get("orderType") or ao.get("type") or "").upper()
                if cid == cid_hedge:
                    found_hedge = ao
                if cid == legacy_cid and otype in {"STOP_MARKET", "STOP"}:
                    found_legacy = ao
            except Exception:
                continue

        # If hedge exists and trigger already matches, keep it (ignore must_refresh).
        # DB may lag behind exchange; we prefer exchange snapshot here.
        if found_hedge is not None:
            try:
                cur = found_hedge.get("triggerPrice") or found_hedge.get("stopPrice")
                cur_f = float(cur) if cur is not None else 0.0
                tol = max(float(tick or 0.0) * 2.0, abs(stop_price) * 0.0002, 1e-8)
                if cur_f > 0 and abs(cur_f - float(stop_price)) <= tol:
                    return False
            except Exception:
                pass

        # Cancel existing hedge (best-effort) only if we really want to refresh it.
        if must_refresh or found_hedge is not None:
            for cid in (cid_hedge,):
                try:
                    self._binance.cancel_algo_order(symbol=sym, clientAlgoId=cid)
                except Exception:
                    try:
                        self._binance.cancel_order(symbol=sym, origClientOrderId=cid)
                    except Exception:
                        pass

        # Cancel legacy hedge stored under _TRL if it's STOP_MARKET
        if found_legacy is not None:
            try:
                self._binance.cancel_algo_order(symbol=sym, clientAlgoId=legacy_cid)
            except Exception:
                pass

        # Quantity: must be based on exchange position size
        qty_main = 0.0
        try:
            pr = self._rest_snapshot_get("position_risk") or []
            for r in pr if isinstance(pr, list) else []:
                if str(r.get("symbol") or "").upper() != str(sym).upper():
                    continue
                ps = str(r.get("positionSide") or "").upper()
                if ps in {"LONG", "SHORT"} and ps != side_u:
                    continue
                amt = float(r.get("positionAmt") or 0.0)
                if abs(amt) > 0:
                    qty_main = abs(float(amt))
                    break
        except Exception:
            qty_main = 0.0
        if qty_main <= 0:
            qty_main = _safe_float(pos.get("qty_current") or 0.0, 0.0)
        if qty_main <= 0:
            return False

        hedge_koff = float(getattr(self.p, "hedge_koff", 1.0) or 1.0)
        hedge_qty = float(abs(float(qty_main)) * hedge_koff)

        # qty step rounding
        try:
            qty_step = float(self._qty_step_for_symbol(sym) or 0.0)
        except Exception:
            qty_step = 0.0
        if qty_step and qty_step > 0:
            hedge_qty = float(_round_qty_to_step(hedge_qty, qty_step, mode="down"))
        if hedge_qty <= 0:
            return False

        # Hedge position side is opposite
        hedge_ps = "SHORT" if side_u == "LONG" else "LONG"

        try:
            self._binance.new_order(
                symbol=sym,
                side=close_side,
                type="STOP_MARKET",
                stopPrice=float(stop_price),
                quantity=float(hedge_qty),
                workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                newClientOrderId=cid_hedge,
                positionSide=hedge_ps,
            )
        except Exception:
            return False

        # store meta flag (best-effort)
        try:
            if self._pl_has_raw_meta:
                meta = {
                    "after_last_add_hedge": {
                        "add_n": int(last_n),
                        "fill": float(last_fill),
                        "stop": float(stop_price),
                        "pct": float(dist_pct),
                        "qty_main": float(qty_main),
                        "qty_hedge": float(hedge_qty),
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
            "[trade_liquidation][LIVE][HEDGE] placed after-last-add STOP_MARKET %s %s stop=%.8f (last_add=%.8f add_n=%s pct=%.4f qty=%.8f koff=%.4f)",
            str(sym).upper(),
            str(side_u),
            float(stop_price),
            float(last_fill),
            str(last_n),
            float(dist_pct),
            float(hedge_qty),
            float(hedge_koff),
        )
        return True

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
            eps = float(tick or 0.0) * 2.0 if tick and tick > 0 else 1e-9
            if abs(ex_act - float(activation)) <= max(eps, 1e-9) and abs(ex_cb - float(trail_pct)) <= 1e-9:
                return False

        if (not must_refresh) and existing is not None:
            # already fresh by DB time
            return False

        # Cancel existing TRL (best-effort)
        try:
            self._binance.cancel_algo_order(symbol=sym, clientAlgoId=cid_trl)
        except Exception:
            pass

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

        try:
            self._binance.new_order(
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
        except Exception:
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

        next_n = int(adds_done) + 1
        if next_n > max_adds:
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

        def _iter_open_adds() -> list[dict[str, Any]]:
            out: list[dict[str, Any]] = []
            # Regular openOrders
            for oo in open_all if isinstance(open_all, list) else []:
                try:
                    if str(oo.get("symbol") or "").upper() != sym:
                        continue
                    cid = str(oo.get("clientOrderId") or "")
                    if not cid.startswith(f"{prefix}_{tok}_ADD"):
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
                    if not cid.startswith(f"{prefix}_{tok}_ADD"):
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
                    self._binance.cancel_algo_order(symbol=sym, clientAlgoId=cid)
                else:
                    self._binance.cancel_order(symbol=sym, origClientOrderId=cid)
                return True
            except Exception:
                return False

        # 1) Cleanup: if we already have FILLED adds_done, any older open ADD1/ADD2... that is <= adds_done
        # should be canceled (stale orders after restart / duplicated placement).
        open_adds = _iter_open_adds()

        # Drop obviously invalid numbers
        open_adds = [a for a in open_adds if int(a.get("add_n") or 0) > 0]

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
        open_adds = [a for a in open_adds if int(a.get("add_n") or 0) > 0]

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
        # Reference: entry for 1st add, avg after that.
        min_dist_pct = float(self._cfg_averaging_min_level_distance_pct())
        ref_price = _dec(pos.get("entry_price") or 0) if int(adds_done) == 0 else avg_price
        if ref_price <= 0:
            ref_price = avg_price

        add_side = "BUY" if side == "LONG" else "SELL"

        picked: Optional[float] = None
        try:
            sid = int(pos.get("symbol_id") or 0)
            if sid > 0:
                picked = self._pick_averaging_price_from_candles(
                    symbol_id=sid,
                    side=side,
                    ref_price=float(ref_price),
                    min_dist_pct=min_dist_pct,
                    level_index=next_n,
                )
        except Exception:
            picked = None

        if picked is not None and picked > 0:
            level = _dec(str(picked))
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

        add_pct = _dec(str(getattr(self.p, "averaging_add_pct_of_position", 35.0) or 35.0)) / _dec("100")
        add_qty = pos_qty * add_pct
        if add_qty <= 0:
            return False
        if qty_step and qty_step > 0:
            add_qty = _round_qty_to_step(add_qty, qty_step, mode="down")
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

        try:
            resp = self._binance.new_order(**params)
        except Exception as e:
            log.warning("[trade_liquidation][LIVE][AVG] failed to place ADD%d for %s: %s", int(next_n), sym, e)
            return False

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
    # Indicators helpers (EMA) + levels (used by Hedge mode)
    # ----------------------------------------------------------

    def _ema_direction(
        self,
        symbol_id: int,
        *,
        window: int = 20,
        interval: str = "5m",
        confirm_bars: int = 1,
        min_slope_pct: float = 0.0,
    ) -> Optional[str]:
        """Return EMA direction: 'UP' | 'DOWN' | 'FLAT' | None.

        Anti-chop knobs:
        - confirm_bars: require N consecutive EMA moves in same direction.
        - min_slope_pct: require last-step EMA slope (% of EMA) to be strong enough.

        Uses candles table (close) for given interval.
        """
        try:
            window = int(window)
            if window < 2:
                return None
        except Exception:
            return None

        try:
            confirm_bars = int(confirm_bars)
        except Exception:
            confirm_bars = 1
        confirm_bars = max(1, confirm_bars)

        try:
            min_slope_pct = float(min_slope_pct or 0.0)
        except Exception:
            min_slope_pct = 0.0

        q = """
        SELECT close
        FROM candles
        WHERE exchange_id=%(ex)s AND symbol_id=%(sym)s AND interval=%(tf)s
        ORDER BY open_time ASC
        LIMIT %(n)s;
        """
        # take a bit more history to stabilize EMA
        n = max(window * 4, window + 5)
        rows = list(self.store.query_dict(q, {"ex": int(self.p.exchange_id), "sym": int(symbol_id), "tf": str(interval), "n": int(n)}))
        closes = [float(r.get("close") or 0.0) for r in rows if float(r.get("close") or 0.0) > 0]
        if len(closes) < window + 2:
            return None

        alpha = 2.0 / (float(window) + 1.0)
        # init EMA with SMA(window)
        ema = sum(closes[:window]) / float(window)

        # keep last (confirm_bars + 1) EMA points for direction validation
        keep = max(2, confirm_bars + 1)
        ema_tail: List[float] = [float(ema)]

        for c in closes[window:]:
            ema = (c - ema) * alpha + ema
            ema_tail.append(float(ema))
            if len(ema_tail) > keep:
                ema_tail = ema_tail[-keep:]

        if len(ema_tail) < 2:
            return None

        diffs = [ema_tail[i] - ema_tail[i - 1] for i in range(1, len(ema_tail))]
        if len(diffs) < confirm_bars:
            return None
        last_diffs = diffs[-confirm_bars:]

        ema_prev = float(ema_tail[-2])
        ema_last = float(ema_tail[-1])
        slope_pct = 0.0
        if ema_prev != 0:
            slope_pct = (ema_last - ema_prev) / ema_prev * 100.0

        all_up = all(d > 0 for d in last_diffs)
        all_down = all(d < 0 for d in last_diffs)

        if all_up and slope_pct >= float(min_slope_pct):
            return "UP"
        if all_down and slope_pct <= -float(min_slope_pct):
            return "DOWN"
        return "FLAT"

    def _compute_significant_level(self, symbol_id: int, *, side: str, entry_ref: float, timeframe: str) -> float:
        """Compute a significant support/resistance level using pivot highs/lows (same logic as live ADD1).

        Returns 0.0 if cannot compute.
        """
        try:
            lv_tf = str(getattr(self.p, "averaging_levels_tf", timeframe) or timeframe)
            lookback_h = int(getattr(self.p, "averaging_levels_lookback_hours", 168) or 168)
            left = int(getattr(self.p, "averaging_pivot_left", 3) or 3)
            right = int(getattr(self.p, "averaging_pivot_right", 3) or 3)
            min_dist_pct = float(self._cfg_averaging_min_level_distance_pct())
            dist_limit_pct = max(5.0, float(min_dist_pct))

            if entry_ref <= 0:
                return 0.0

            q = """
            SELECT high, low
            FROM candles
            WHERE exchange_id=%(ex)s AND symbol_id=%(sym)s AND interval=%(tf)s
              AND open_time >= (NOW() AT TIME ZONE 'UTC') - (%(h)s || ' hours')::interval
            ORDER BY open_time ASC;
            """
            rows = list(self.store.query_dict(q, {"ex": int(self.p.exchange_id), "sym": int(symbol_id), "tf": lv_tf, "h": int(lookback_h)}))
            lows = [float(r.get("low") or 0.0) for r in rows]
            highs = [float(r.get("high") or 0.0) for r in rows]
            if len(lows) < (left + right + 5):
                return 0.0

            piv_lows = []
            piv_highs = []
            for i in range(left, len(lows) - right):
                w = lows[i-left:i+right+1]
                if lows[i] > 0 and lows[i] == min(w):
                    piv_lows.append(lows[i])
                w2 = highs[i-left:i+right+1]
                if highs[i] > 0 and highs[i] == max(w2):
                    piv_highs.append(highs[i])

            side_u = str(side or "").upper()
            level = 0.0
            if side_u == "LONG":
                below = [x for x in piv_lows if x > 0 and x < entry_ref * (1.0 - dist_limit_pct/100.0)]
                if below:
                    level = max(below)
                else:
                    level = entry_ref * (1.0 - (dist_limit_pct / 100.0))
            else:
                above = [x for x in piv_highs if x > entry_ref * (1.0 + dist_limit_pct/100.0)]
                if above:
                    level = min(above)
                else:
                    level = entry_ref * (1.0 + (dist_limit_pct / 100.0))

            return float(level) if level and level > 0 else 0.0
        except Exception:
            return 0.0

    def _live_hedge_manage(self) -> Dict[str, int]:
        """Manage hedge positions in LIVE mode when hedge_enabled is True.

        Rule:
          - When SL condition would be met, open an opposite market position sized:
                hedge_qty = abs(main_qty) * hedge_koff
            but only if EMA(hedge_ema_window) on hedge_ema_interval supports hedge direction.
          - Close hedge when:
                a) price reaches significant level (same as averaging level) OR
                b) EMA flips against hedge (if enabled).
        """
        if (not self._is_live) or self._binance is None:
            return {"checked": 0, "opened": 0, "closed": 0, "skipped": 0}

        if not bool(getattr(self.p, "hedge_enabled", False)):
            return {"checked": 0, "opened": 0, "closed": 0, "skipped": 0}

        hedge_koff = float(getattr(self.p, "hedge_koff", 1.2) or 1.2)
        ema_win = int(getattr(self.p, "hedge_ema_window", 20) or 20)
        ema_tf = str(getattr(self.p, "hedge_ema_interval", "5m") or "5m")
        ema_confirm = int(getattr(self.p, "hedge_ema_confirm_bars", 3) or 3)
        ema_min_slope = float(getattr(self.p, "hedge_ema_min_slope_pct", 0.0) or 0.0)
        ema_flip_mult = float(getattr(self.p, "hedge_ema_flip_slope_mult", 0.7) or 0.7)
        hedge_cooldown_sec = float(getattr(self.p, "hedge_cooldown_sec", 180) or 180)
        if ema_flip_mult <= 0:
            ema_flip_mult = 0.7
        ema_close_slope = float(ema_min_slope) * float(ema_flip_mult)

        # Exchange snapshot positions (positionRisk)
        pr = self._rest_snapshot_get("position_risk")
        pr_rows = pr if isinstance(pr, list) else []
        pr_map = {}  # (symbol, positionSide)-> dict
        for r in pr_rows:
            sym = str(r.get("symbol") or "").strip()
            ps = str(r.get("positionSide") or ("BOTH" if not bool(getattr(self.p,"hedge_enabled",False)) else "")).strip().upper()
            if not sym:
                continue
            pr_map[(sym, ps)] = r

        # Ledger OPEN positions for strategy
        led = []
        # NOTE: position_ledger schema can differ between deployments.
        # We only need basic fields + optional raw_meta (for hedge state).
        try:
            sel = "symbol_id, pos_uid, side, qty_current, avg_price, entry_price"
            if getattr(self, "_pl_has_raw_meta", False):
                sel += ", raw_meta"
            led = list(
                self.store.query_dict(
                    f"""
                    SELECT {sel}
                    FROM position_ledger
                    WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND strategy_id=%(sid)s AND status='OPEN' AND source='live'
                    ORDER BY opened_at DESC;
                    """,
                    {"ex": int(self.p.exchange_id), "acc": int(self.account_id), "sid": str(self.STRATEGY_ID)},
                )
            )
        except Exception:
            # fallback without raw_meta
            led = list(
                self.store.query_dict(
                    """
                    SELECT symbol_id, pos_uid, side, qty_current, avg_price, entry_price
                    FROM position_ledger
                    WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND strategy_id=%(sid)s AND status='OPEN' AND source='live'
                    ORDER BY opened_at DESC;
                    """,
                    {"ex": int(self.p.exchange_id), "acc": int(self.account_id), "sid": str(self.STRATEGY_ID)},
                )
            )

        checked = opened = closed = skipped = 0
        sym_map = self._symbols_map()

        for p in led:
            checked += 1
            symbol_id = int(p["symbol_id"])
            symbol = sym_map.get(symbol_id) or ""
            if not symbol:
                skipped += 1
                continue

            main_side = str(p.get("side") or "").upper()
            if main_side not in {"LONG","SHORT"}:
                skipped += 1
                continue

            qty = abs(float(p.get("qty_current") or 0.0))
            if qty <= 0:
                skipped += 1
                continue

            entry = _safe_float(p.get("avg_price"), default=_safe_float(p.get("entry_price"), default=0.0))
            if entry <= 0:
                skipped += 1
                continue

            # Determine hedge direction and required EMA direction
            hedge_side = "SHORT" if main_side == "LONG" else "LONG"
            need_ema = "DOWN" if hedge_side == "SHORT" else "UP"
            # EMA direction for OPEN must be stricter (min slope); for CLOSE we can use a softer threshold
            ema_dir_open = self._ema_direction(
                symbol_id,
                window=ema_win,
                interval=ema_tf,
                confirm_bars=ema_confirm,
                min_slope_pct=ema_min_slope,
            )
            ema_dir_close = self._ema_direction(
                symbol_id,
                window=ema_win,
                interval=ema_tf,
                confirm_bars=ema_confirm,
                min_slope_pct=ema_close_slope,
            )
            hedge_ok = (ema_dir_open == need_ema)

            # Detect existing hedge position on exchange
            # In hedge mode we should have positionSide LONG/SHORT.
            main_ps = main_side
            hedge_ps = hedge_side
            main_pos = pr_map.get((symbol, main_ps))
            hedge_pos = pr_map.get((symbol, hedge_ps))

            hedge_amt = 0.0
            hedge_entry = 0.0
            if hedge_pos:
                try:
                    hedge_amt = abs(float(hedge_pos.get("positionAmt") or 0.0))
                except Exception:
                    hedge_amt = 0.0
                try:
                    hedge_entry = abs(float(hedge_pos.get("entryPrice") or hedge_pos.get("avgEntryPrice") or 0.0))
                except Exception:
                    hedge_entry = 0.0

            # Main position info (for PnL controls) + hedge base qty from ledger raw_meta
            main_amt = 0.0
            main_entry_ex = 0.0
            main_upnl_ex: Optional[float] = None
            if main_pos:
                try:
                    main_amt = abs(float(main_pos.get("positionAmt") or 0.0))
                except Exception:
                    main_amt = 0.0
                try:
                    main_entry_ex = abs(float(main_pos.get("entryPrice") or main_pos.get("avgEntryPrice") or 0.0))
                except Exception:
                    main_entry_ex = 0.0
                try:
                    v = main_pos.get("unRealizedProfit")
                    if v is not None:
                        main_upnl_ex = float(v)
                except Exception:
                    main_upnl_ex = None

            hedge_upnl_ex: Optional[float] = None
            if hedge_pos:
                try:
                    v = hedge_pos.get("unRealizedProfit")
                    if v is not None:
                        hedge_upnl_ex = float(v)
                except Exception:
                    hedge_upnl_ex = None

            hedge_base_qty: Optional[float] = None
            hedge_last_action_ts: float = 0.0
            hedge_last_open_ts: float = 0.0
            hedge_last_close_ts: float = 0.0
            raw_meta = p.get("raw_meta") if isinstance(p, dict) else None
            if raw_meta is not None:
                try:
                    rm = raw_meta
                    if isinstance(rm, str):
                        import json as _json
                        rm = _json.loads(rm) if rm.strip() else {}
                    if isinstance(rm, dict):
                        h = rm.get("hedge") or {}
                        if isinstance(h, dict):
                            if h.get("base_qty") is not None:
                                try:
                                    hedge_base_qty = float(h.get("base_qty"))
                                except Exception:
                                    hedge_base_qty = None
                            try:
                                hedge_last_action_ts = float(h.get("last_action_ts") or 0.0)
                            except Exception:
                                hedge_last_action_ts = 0.0
                            try:
                                hedge_last_open_ts = float(h.get("last_open_ts") or 0.0)
                            except Exception:
                                hedge_last_open_ts = 0.0
                            try:
                                hedge_last_close_ts = float(h.get("last_close_ts") or 0.0)
                            except Exception:
                                hedge_last_close_ts = 0.0
                except Exception:
                    hedge_base_qty = None

            all_pl_limit = float(getattr(self.p, "all_P_L", 0.0) or 0.0)

            mark = 0.0
            try:
                mark = float(self._get_mark_price(symbol, hedge_ps) or self._get_mark_price(symbol, main_ps) or 0.0)
            except Exception:
                mark = 0.0
            if mark <= 0:
                skipped += 1
                continue

            # Compute unrealized PnL (prefer exchange fields; fallback to mark/entry)
            def _calc_upnl(side: str, entry_px: float, amt: float, upnl_ex: Optional[float]) -> float:
                if upnl_ex is not None:
                    try:
                        return float(upnl_ex)
                    except Exception:
                        pass
                if entry_px <= 0 or amt <= 0:
                    return 0.0
                if side == "LONG":
                    return (mark - entry_px) * amt
                return (entry_px - mark) * amt

            main_entry_use = float(main_entry_ex or entry or 0.0)
            main_upnl = _calc_upnl(main_side, main_entry_use, float(main_amt or qty), main_upnl_ex)
            hedge_upnl = 0.0
            if hedge_amt > 0:
                hedge_entry_use = float(hedge_entry or 0.0)
                if hedge_entry_use <= 0 and hedge_pos:
                    try:
                        hedge_entry_use = abs(float(hedge_pos.get("entryPrice") or hedge_pos.get("avgEntryPrice") or 0.0))
                    except Exception:
                        hedge_entry_use = 0.0
                hedge_upnl = _calc_upnl(hedge_side, hedge_entry_use, float(hedge_amt), hedge_upnl_ex)

            combined_upnl = float(main_upnl) + float(hedge_upnl)

            # Close BOTH positions if combined unrealized PnL >= configured limit (USDT)
            if all_pl_limit and all_pl_limit > 0 and combined_upnl >= float(all_pl_limit):
                try:
                    # close hedge first (if exists), then main
                    if hedge_amt > 0:
                        close_side = "BUY" if hedge_side == "SHORT" else "SELL"
                        self._binance.new_order(
                            symbol=symbol,
                            side=close_side,
                            type="MARKET",
                            quantity=float(hedge_amt),
                            positionSide=hedge_ps,
                        )
                        closed += 1
                    if main_amt > 0:
                        close_side = "BUY" if main_side == "SHORT" else "SELL"
                        self._binance.new_order(
                            symbol=symbol,
                            side=close_side,
                            type="MARKET",
                            quantity=float(main_amt),
                            positionSide=main_ps,
                        )
                        closed += 1
                    log.info("[TL][HEDGE] closed ALL %s combined_upnl=%.4f >= %.4f (main=%.4f hedge=%.4f)",
                             symbol, float(combined_upnl), float(all_pl_limit), float(main_upnl), float(hedge_upnl))
                except Exception:
                    log.exception("[TL][HEDGE] failed to close ALL for %s", symbol)
                    skipped += 1
                continue

            # Close hedge if main position is already positive unrealized PnL
            main_positive = (float(main_upnl) > 0.0)

            # Compute would-be SL price (same as default SL)
            if main_side == "LONG":
                sl_price = entry * (1.0 - _pct_to_mult(self.p.stop_loss_pct))
                sl_hit = (mark <= sl_price)
            else:
                sl_price = entry * (1.0 + _pct_to_mult(self.p.stop_loss_pct))
                sl_hit = (mark >= sl_price)

            # 1) Open hedge instead of SL
            # Open hedge instead of SL (or reopen after prior close) when conditions met
            open_trigger = bool(sl_hit)
            if (not open_trigger) and bool(getattr(self.p, "hedge_reopen_enabled", True)):
                # reopen hedge if main is in loss and drawdown beyond configured threshold
                try:
                    dd_thr = float(getattr(self.p, "hedge_reopen_drawdown_pct", 0.0) or 0.0)
                except Exception:
                    dd_thr = 0.0
                dd = 0.0
                if main_entry_use > 0:
                    if main_side == "LONG" and mark < main_entry_use:
                        dd = (main_entry_use - mark) / main_entry_use * 100.0
                    if main_side == "SHORT" and mark > main_entry_use:
                        dd = (mark - main_entry_use) / main_entry_use * 100.0
                if float(main_upnl) < 0.0 and (dd_thr <= 0.0 or dd >= dd_thr):
                    open_trigger = True

            if hedge_amt <= 0 and open_trigger:
                # cooldown check (open)
                if hedge_cooldown_sec and hedge_last_action_ts and (time.time() - float(hedge_last_action_ts)) < float(hedge_cooldown_sec):
                    skipped += 1
                    continue
                if not hedge_ok:
                    skipped += 1
                    continue

                # qty and rounding
                qty_step = float(self._qty_step_for_symbol(symbol) or 0.0) or 0.0
                # hedge qty: use stored base qty if available (same as first open), otherwise compute from current main qty
                hedge_qty = float(hedge_base_qty) if (hedge_base_qty is not None and float(hedge_base_qty) > 0) else (qty * float(hedge_koff))
                if qty_step and qty_step > 0:
                    hedge_qty = _round_qty_to_step(hedge_qty, qty_step, mode="down")
                if hedge_qty <= 0 or (qty_step and hedge_qty < qty_step):
                    skipped += 1
                    continue

                side_ord = "SELL" if hedge_side == "SHORT" else "BUY"
                try:
                    resp = self._binance.new_order(
                        symbol=symbol,
                        side=side_ord,
                        type="MARKET",
                        quantity=float(hedge_qty),
                        positionSide=hedge_ps,
                    )
                    opened += 1
                    # persist hedge base qty (same for reopens) into position_ledger.raw_meta if available
                    try:
                        if getattr(self, "_pl_has_raw_meta", False):
                            import json as _json
                            reason = "SL" if bool(sl_hit) else "REOPEN"
                            meta = {"hedge": {"base_qty": float(hedge_qty), "last_open_reason": str(reason), "last_open_ts": float(time.time()), "last_action_ts": float(time.time())}}
                            uqm = '''
                            UPDATE position_ledger
                            SET raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb,
                                updated_at = now()
                            WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND status='OPEN';
                            '''
                            self.store.execute(
                                uqm,
                                {
                                    "meta": _json.dumps(meta),
                                    "ex": int(self.p.exchange_id),
                                    "acc": int(self.account_id),
                                    "pos_uid": str(p.get("pos_uid")),
                                },
                            )
                    except Exception:
                        pass
                    log.info("[TL][HEDGE] opened %s %s qty=%.8f (main=%s qty=%.8f) mark=%.6f sl=%.6f ema_open=%s ema_close=%s",
                             symbol, hedge_ps, float(hedge_qty), main_side, float(qty), float(mark), float(sl_price), str(ema_dir_open), str(ema_dir_close))
                except Exception as e:
                    log.exception("[TL][HEDGE] failed to open hedge for %s: %s", symbol, e)
                    skipped += 1
                continue  # next position

            # 2) Close hedge if exists and conditions met
            if hedge_amt > 0:
                close_reason = None

                # close hedge if main became profitable
                if main_positive:
                    close_reason = "MAIN_POSITIVE"

                # a) close on significant level
                if bool(getattr(self.p, "hedge_close_on_level", True)):
                    tf_main = str(p.get("timeframe") or "15m")
                    level = self._compute_significant_level(symbol_id, side=main_side, entry_ref=float(entry), timeframe=tf_main)
                    tol = float(getattr(self.p, "hedge_level_tolerance_pct", 0.10) or 0.10) / 100.0
                    if level > 0:
                        if main_side == "LONG":
                            # close SHORT hedge when price reaches support level (or slightly above)
                            if mark <= level * (1.0 + tol):
                                close_reason = "LEVEL"
                        else:
                            # close LONG hedge when price reaches resistance level (or slightly below)
                            if mark >= level * (1.0 - tol):
                                close_reason = "LEVEL"

                # b) close on EMA flip against hedge
                if close_reason is None and bool(getattr(self.p, "hedge_close_on_ema_flip", True)):
                    # use softer slope threshold for close to avoid missing real flips,
                    # but still ignore micro-flips when slope is too small.
                    if ema_dir_close and ema_dir_close in {"UP", "DOWN"}:
                        if ema_dir_close != need_ema:
                            close_reason = "EMA_FLIP"

                if close_reason:
                    # cooldown check (close)
                    if hedge_cooldown_sec and hedge_last_action_ts and (time.time() - float(hedge_last_action_ts)) < float(hedge_cooldown_sec):
                        skipped += 1
                        continue
                    close_side = "BUY" if hedge_side == "SHORT" else "SELL"
                    qty_step = float(self._qty_step_for_symbol(symbol) or 0.0) or 0.0
                    close_qty = float(hedge_amt)
                    if qty_step and qty_step > 0:
                        close_qty = _round_qty_to_step(close_qty, qty_step, mode="down")
                    if close_qty <= 0 or (qty_step and close_qty < qty_step):
                        skipped += 1
                        continue
                    try:
                        resp = self._binance.new_order(
                            symbol=symbol,
                            side=close_side,
                            type="MARKET",
                            quantity=float(close_qty),
                            positionSide=hedge_ps,
                        )
                        closed += 1
                        # persist last hedge close reason into raw_meta if available
                        try:
                            if getattr(self, "_pl_has_raw_meta", False):
                                import json as _json
                                meta = {"hedge": {"base_qty": float(hedge_base_qty) if (hedge_base_qty is not None and float(hedge_base_qty) > 0) else float(hedge_amt), "last_close_reason": str(close_reason), "last_close_ts": float(time.time()), "last_action_ts": float(time.time())}}
                                uqm = '''
                                UPDATE position_ledger
                                SET raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb,
                                    updated_at = now()
                                WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND status='OPEN';
                                '''
                                self.store.execute(
                                    uqm,
                                    {
                                        "meta": _json.dumps(meta),
                                        "ex": int(self.p.exchange_id),
                                        "acc": int(self.account_id),
                                        "pos_uid": str(p.get("pos_uid")),
                                    },
                                )
                        except Exception:
                            pass
                        log.info("[TL][HEDGE] closed %s %s qty=%.8f reason=%s mark=%.6f ema_close=%s ema_open=%s",
                                 symbol, hedge_ps, float(close_qty), str(close_reason), float(mark), str(ema_dir_close), str(ema_dir_open))
                    except Exception as e:
                        log.exception("[TL][HEDGE] failed to close hedge for %s: %s", symbol, e)
                        skipped += 1

        return {"checked": checked, "opened": opened, "closed": closed, "skipped": skipped}
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
            # In LIVE mode SL/TP is usually handled by exchange orders and cleanup.
            # But if hedge_enabled is True, we actively manage hedge open/close here.
            if bool(getattr(self.p, "hedge_enabled", False)):
                hs = self._live_hedge_manage()
                return {"checked": int(hs.get("checked", 0)), "closed": int(hs.get("closed", 0)), "hedge": hs}
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
        """Write/Update row in public.orders to link exchange orders with pos_uid/strategy_id.

        Multi-account safe: first UPDATE by (exchange_id, account_id, client_order_id) for active status=NEW,
        then INSERT; avoids unique violations on the partial unique index for active NEW orders.
        """
        try:
            oid = str(order_id)
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
                  AND status = 'NEW'
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

            # Hedge strategy mode (Binance Futures hedge mode):
            # Instead of a classic stop-loss that closes the current leg, place a *conditional market*
            # order that OPENS the opposite hedge leg when stop price is reached.
            if bool(getattr(self.p, "hedge_enabled", False)):
                if not hedge_mode:
                    return None
                try:
                    tick_size = float(getattr(meta, "tick_size", 0) or 0)
                    stop_px = float(_round_price_to_tick(sl_price, tick_size, mode="down" if ledger_side == "LONG" else "up"))
                    hedge_koff = float(getattr(self.p, "hedge_koff", 1.0) or 1.0)
                    hedge_qty = float(abs(float(qty)) * hedge_koff)
                    if hedge_qty <= 0:
                        return None
                    hedge_ps = "SHORT" if ledger_side == "LONG" else "LONG"
                    params = dict(
                        symbol=symbol,
                        side=close_side,
                        type="STOP_MARKET",
                        stopPrice=stop_px,
                        quantity=hedge_qty,
                        workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                        newClientOrderId=cid_trl,
                        positionSide=hedge_ps,
                    )
                    return self._binance.new_order(**params)
                except Exception:
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
            avg_enabled = bool(getattr(self.p, "averaging_enabled", False)) and int(self._cfg_max_adds() or 0) > 0
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
            if bool(getattr(self.p, "averaging_enabled", False)) and int(self._cfg_max_adds() or 0) > 0:
                mult = float(getattr(self.p, "averaging_add_position_multiplier", 1.0) or 1.0)
                # Safety: respect max additions cap even at ENTRY time (prevents duplicate ADD1 after retries/restarts).
                max_adds_cfg = int(self._cfg_max_adds() or 0)
                if max_adds_cfg < 1:
                    mult = 1.0  # disable add placement
                else:
                    try:
                        rows_sc = list(self.store.query_dict(
                            "SELECT COALESCE(scale_in_count, 0) AS scale_in_count FROM position_ledger WHERE pos_uid=%(u)s LIMIT 1;",
                            {"u": str(pos_uid)},
                        ))
                        if rows_sc and int(rows_sc[0].get("scale_in_count") or 0) >= max_adds_cfg:
                            mult = 1.0  # cap reached -> disable add placement
                    except Exception:
                        pass
                if mult > 1.0:
                    # pick a significant level from candles (support/resistance)
                    lv_tf = str(getattr(self.p, "averaging_levels_tf", timeframe) or timeframe)
                    lookback_h = int(getattr(self.p, "averaging_levels_lookback_hours", 168) or 168)
                    left = int(getattr(self.p, "averaging_pivot_left", 3) or 3)
                    right = int(getattr(self.p, "averaging_pivot_right", 3) or 3)
                    tol_pct = float(getattr(self.p, "averaging_level_tolerance_pct", 0.15) or 0.15)
                    min_dist_pct = float(self._cfg_averaging_min_level_distance_pct())
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