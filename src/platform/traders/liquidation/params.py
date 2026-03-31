from __future__ import annotations

import logging
import math
import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR, ROUND_HALF_UP, ROUND_UP, InvalidOperation
from typing import Any, Dict,  Optional, Tuple

from src.platform.data.storage.postgres.storage import PostgreSQLStorage

log = logging.getLogger("traders.trade_liquidation")

# Match averaging add order ids: ..._ADD1, ..._ADD2, ...
_ADD_RE = re.compile(r"_ADD(\d+)$", re.IGNORECASE)
_COID_SAN_RE = re.compile(r"[^0-9A-Za-z_-]+")

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

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

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

def _as_float(v: Any, default: float = 0.0) -> float:
    return _safe_float(v, default)


def _cap_qty_by_wallet_notional(*, qty: float, wallet_balance_usdt: float, price: float, cap_pct_wallet: float, qty_step: float = 0.0, leverage: float = 1.0) -> float:
    """Cap quantity by wallet notional percentage with optional leverage multiplier.

    Effective max notional = wallet_balance_usdt * leverage * (cap_pct_wallet / 100).
    Returns original qty when cap is disabled.
    """
    q = _safe_float(qty, 0.0)
    wallet = _safe_float(wallet_balance_usdt, 0.0)
    px = _safe_float(price, 0.0)
    cap_pct = _safe_float(cap_pct_wallet, 0.0)
    lev = max(1.0, _safe_float(leverage, 1.0))
    if q <= 0.0 or wallet <= 0.0 or px <= 0.0 or cap_pct <= 0.0:
        return q
    max_notional = wallet * lev * (cap_pct / 100.0)
    if max_notional <= 0.0:
        return q
    max_qty = max_notional / px
    if qty_step and qty_step > 0:
        max_qty = _round_qty_to_step(max_qty, qty_step, mode="down")
    return min(q, max(0.0, float(max_qty)))

def _as_int(v: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if v in (None, ""):
            return default
        return int(v)
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

def _stable_live_signal_pos_uid(*, exchange_id: Any, account_id: Any, strategy_id: Any, signal_id: Any, symbol_id: Any, side: Any) -> str:
    """Deterministic pos_uid for live entries opened from a signal.

    This makes entry/SL/TP/TRAIL client ids idempotent across retries/restarts so the
    same signal cannot create a second independent position with a new uuid4().
    """
    seed = "|".join([
        str(exchange_id),
        str(account_id),
        str(strategy_id),
        str(signal_id),
        str(symbol_id),
        str(side or "").upper(),
    ])
    return f"SIG-{hashlib.sha1(seed.encode('utf-8', errors='ignore')).hexdigest()[:24]}"

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
    screener_name: Union[str, Tuple[str, ...]] = "scr_liquidation_binance"
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
    averaging_add_max_notional_pct_wallet: float = 0.0  # 0=OFF; caps one averaging add notional vs wallet*leverage
    main_position_max_notional_pct_wallet: float = 0.0  # 0=OFF; caps total MAIN leg notional vs wallet*leverage after adds
    hedge_position_max_notional_pct_wallet: float = 0.0  # 0=OFF; caps total HEDGE leg notional vs wallet*leverage

    # --- risk / SLTP
    risk_wallet_pct: float = 1.0
    stop_loss_pct: float = 2.0
    take_profit_pct: float = 5.0
    recalc_sl_tp: bool = True
    defer_stop_loss_until_last_add: bool = False
    sl_after_last_add_distance_pct: float = 0.0

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
    hedge_koff: float = 1.2  # base hedge qty multiplier relative to main qty
    hedge_max_ratio: float = 1.0  # hard cap for hedge qty relative to main qty
    hedge_funding_kof_limit: float = 0.0  # funding threshold in percent to allow boost
    hedge_koff_funding_boost: float = 1.0  # boost multiplier for hedge_koff when funding is favorable
    hedge_trigger_buffer_pct: float = 0.3  # extra buffer (%) for after-last-add hedge trigger placement
    hedge_cooldown_sec: int = 180  # cooldown between hedge open/close actions
    hedge_close_on_main_positive: bool = True  # close hedge if MAIN unrealized PnL becomes positive
    hedge_level_tolerance_pct: float = 0.10  # tolerance around computed level for closing hedge
    hedge_stop_loss_pct: float = 0.0  # 0=OFF. % from hedge entry (SHORT: entry*(1+pct), LONG: entry*(1-pct))
    hedge_stop_loss_working_type: Optional[str] = None  # override workingType for hedge SL (defaults to hedge_trailing_working_type/working_type)
    hedge_trailing_reissue_on_avg_entry_change: bool = True

    # Move hedge stop-loss to break-even (profit-lock) after activation move
    hedge_stop_loss_be_pct: float = 0.0  # 0=OFF. % from hedge entry to place BE stop (SHORT: entry*(1-pct), LONG: entry*(1+pct))
    hedge_stop_loss_be_activation_pct: float = 0.0  # 0=OFF. activate when price moves in favor by this % from entry (SHORT: entry*(1-pct), LONG: entry*(1+pct))
    hedge_stop_loss_be_pct_min: float = 0.0
    hedge_stop_loss_be_pct_max: float = 100.0
    hedge_stop_loss_be_activation_pct_min: float = 0.0
    hedge_stop_loss_be_activation_pct_max: float = 100.0

    hedge_reopen_enabled: bool = True
    hedge_open_only_after_max_adds: bool = True
    hedge_reopen_price_filter_enabled: bool = False
    hedge_reopen_price_move_pct: float = 0.0
    hedge_reopen_anchor_shift_trigger_pct: float = 0.0
    hedge_reopen_anchor_shift_step_pct: float = 0.0

    # Main add when hedge entry is too far from main entry
    main_add_on_hedge_price_diff_enabled: bool = False
    main_add_on_hedge_price_diff_pct: float = 20.0
    main_add_on_hedge_price_diff_koff_lot: float = 2.0

    # If main leg is closed but hedge leg survives, create a new safe LIVE main row
    # from the surviving hedge and stop treating it as a hedge mirror.
    hedge_promote_survivor_to_main_enabled: bool = True

    # Auto recovery
    recovery_place_trailing: bool = True

    # Balanced hedge / profit optimization
    hedge_unwind_enabled: bool = False
    hedge_unwind_step1_pct: float = 0.8
    hedge_unwind_step1_share: float = 0.35
    hedge_unwind_step2_pct: float = 1.6
    hedge_unwind_step2_share: float = 0.35
    hedge_unwind_full_exit_pct: float = 2.4
    hedge_unwind_cooldown_sec: int = 30
    hedge_unwind_min_qty_ratio: float = 0.15
    hedge_adaptive_enabled: bool = True
    hedge_adaptive_trigger_pct: float = 0.8
    hedge_adaptive_step_pct: float = 0.8
    hedge_adaptive_boost_per_step: float = 0.08
    hedge_adaptive_max_extra_koff: float = 0.20

    # v14 profit optimizer
    main_partial_tp_enabled: bool = True
    main_partial_tp1_pct: float = 2.5
    main_partial_tp1_share: float = 0.30
    main_partial_tp2_pct: float = 4.5
    main_partial_tp2_share: float = 0.30
    main_partial_tp_cooldown_sec: int = 30
    main_partial_tp_min_qty_ratio: float = 0.15
    main_trailing_after_tp1_enabled: bool = True
    main_trailing_after_tp1_cb_pct: float = 0.50
    main_trailing_after_tp2_enabled: bool = True
    main_trailing_after_tp2_cb_pct: float = 0.40
    main_exit_small_remainder_enabled: bool = True
    main_exit_small_remainder_qty_ratio: float = 0.20

    hedge_unwind_full_exit_enabled: bool = True
    hedge_funding_guard_enabled: bool = True
    hedge_funding_bad_limit_pct: float = 0.8
    hedge_funding_reduce_step_pct: float = 0.5
    hedge_funding_reduce_share: float = 0.25
    hedge_funding_full_exit_limit_pct: float = 1.2
    hedge_funding_cooldown_sec: int = 300
    hedge_funding_soft_cap_ratio: float = 0.25

    # Close hedge at/near entry after it has first moved into profit by a configured amount.

    # After hedge close, optionally add to the main leg by the currently open main volume.
    # Example: main LONG 0.5 + hedge SHORT 0.5 -> hedge closes -> buy extra 0.5 LONG.

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

    # --- live entry retry on Binance position limits (-2027)
    live_entry_reduce_on_max_position: bool = True
    live_entry_reduce_steps: int = 5
    live_entry_reduce_factor: float = 0.85

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
    averaging_add_position_multiplier: float = 2.0
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
    debug: bool = False
    debug_top: int = 10
    ema_log_enabled: bool = False

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

        # screener_name list -> tuple
        sn = params.get("screener_name", cls().screener_name)
        if isinstance(sn, list):
            params["screener_name"] = tuple(str(x) for x in sn if str(x).strip())
        elif isinstance(sn, tuple):
            params["screener_name"] = tuple(str(x) for x in sn if str(x).strip())
        elif sn is None:
            params["screener_name"] = cls().screener_name
        else:
            params["screener_name"] = str(sn)

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
        params["averaging_add_max_notional_pct_wallet"] = float(_as_float(params.get("averaging_add_max_notional_pct_wallet", cls().averaging_add_max_notional_pct_wallet), cls().averaging_add_max_notional_pct_wallet) or 0.0)
        params["main_position_max_notional_pct_wallet"] = float(_as_float(params.get("main_position_max_notional_pct_wallet", cls().main_position_max_notional_pct_wallet), cls().main_position_max_notional_pct_wallet) or 0.0)
        params["hedge_position_max_notional_pct_wallet"] = float(_as_float(params.get("hedge_position_max_notional_pct_wallet", cls().hedge_position_max_notional_pct_wallet), cls().hedge_position_max_notional_pct_wallet) or 0.0)
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
        params["defer_stop_loss_until_last_add"] = _as_bool(params.get("defer_stop_loss_until_last_add", cls().defer_stop_loss_until_last_add), cls().defer_stop_loss_until_last_add)
        params["sl_after_last_add_distance_pct"] = float(_as_float(params.get("sl_after_last_add_distance_pct", cls().sl_after_last_add_distance_pct), cls().sl_after_last_add_distance_pct) or cls().sl_after_last_add_distance_pct)
        params["trailing_enabled"] = _as_bool(params.get("trailing_enabled", cls().trailing_enabled), cls().trailing_enabled)
        params["averaging_enabled"] = _as_bool(params.get("averaging_enabled", cls().averaging_enabled), cls().averaging_enabled)
        params["hedge_enabled"] = _as_bool(params.get("hedge_enabled", cls().hedge_enabled), cls().hedge_enabled)
        params["hedge_koff"] = float(_as_float(params.get("hedge_koff", cls().hedge_koff), cls().hedge_koff) or cls().hedge_koff)
        params["hedge_max_ratio"] = float(_as_float(params.get("hedge_max_ratio", cls().hedge_max_ratio), cls().hedge_max_ratio) or cls().hedge_max_ratio)
        params["hedge_funding_kof_limit"] = float(_as_float(params.get("hedge_funding_kof_limit", cls().hedge_funding_kof_limit), cls().hedge_funding_kof_limit) or cls().hedge_funding_kof_limit)
        params["hedge_koff_funding_boost"] = float(_as_float(params.get("hedge_koff_funding_boost", cls().hedge_koff_funding_boost), cls().hedge_koff_funding_boost) or cls().hedge_koff_funding_boost)
        params["hedge_cooldown_sec"] = int(_as_int(params.get("hedge_cooldown_sec", cls().hedge_cooldown_sec), cls().hedge_cooldown_sec) or cls().hedge_cooldown_sec)
        params["hedge_close_on_main_positive"] = _as_bool(
            params.get("hedge_close_on_main_positive", cls().hedge_close_on_main_positive),
            cls().hedge_close_on_main_positive,
        )
        params["hedge_level_tolerance_pct"] = float(_as_float(params.get("hedge_level_tolerance_pct", cls().hedge_level_tolerance_pct), cls().hedge_level_tolerance_pct) or cls().hedge_level_tolerance_pct)
        params["hedge_reopen_enabled"] = _as_bool(params.get("hedge_reopen_enabled", cls().hedge_reopen_enabled), cls().hedge_reopen_enabled)
        params["hedge_open_only_after_max_adds"] = _as_bool(params.get("hedge_open_only_after_max_adds", cls().hedge_open_only_after_max_adds), cls().hedge_open_only_after_max_adds)
        params["hedge_reopen_price_filter_enabled"] = _as_bool(params.get("hedge_reopen_price_filter_enabled", cls().hedge_reopen_price_filter_enabled), cls().hedge_reopen_price_filter_enabled)
        params["hedge_reopen_price_move_pct"] = float(_as_float(params.get("hedge_reopen_price_move_pct", cls().hedge_reopen_price_move_pct), cls().hedge_reopen_price_move_pct) or cls().hedge_reopen_price_move_pct)
        params["hedge_reopen_anchor_shift_trigger_pct"] = float(_as_float(params.get("hedge_reopen_anchor_shift_trigger_pct", cls().hedge_reopen_anchor_shift_trigger_pct), cls().hedge_reopen_anchor_shift_trigger_pct) or cls().hedge_reopen_anchor_shift_trigger_pct)
        params["hedge_reopen_anchor_shift_step_pct"] = float(_as_float(params.get("hedge_reopen_anchor_shift_step_pct", cls().hedge_reopen_anchor_shift_step_pct), cls().hedge_reopen_anchor_shift_step_pct) or cls().hedge_reopen_anchor_shift_step_pct)
        params["main_add_on_hedge_price_diff_enabled"] = _as_bool(params.get("main_add_on_hedge_price_diff_enabled", cls().main_add_on_hedge_price_diff_enabled), cls().main_add_on_hedge_price_diff_enabled)
        params["main_add_on_hedge_price_diff_pct"] = float(_as_float(params.get("main_add_on_hedge_price_diff_pct", cls().main_add_on_hedge_price_diff_pct), cls().main_add_on_hedge_price_diff_pct) or cls().main_add_on_hedge_price_diff_pct)
        params["main_add_on_hedge_price_diff_koff_lot"] = float(_as_float(params.get("main_add_on_hedge_price_diff_koff_lot", cls().main_add_on_hedge_price_diff_koff_lot), cls().main_add_on_hedge_price_diff_koff_lot) or cls().main_add_on_hedge_price_diff_koff_lot)
        params["hedge_promote_survivor_to_main_enabled"] = _as_bool(params.get("hedge_promote_survivor_to_main_enabled", cls().hedge_promote_survivor_to_main_enabled), cls().hedge_promote_survivor_to_main_enabled)
        params["hedge_unwind_enabled"] = _as_bool(params.get("hedge_unwind_enabled", cls().hedge_unwind_enabled), cls().hedge_unwind_enabled)
        params["hedge_unwind_step1_pct"] = float(_as_float(params.get("hedge_unwind_step1_pct", cls().hedge_unwind_step1_pct), cls().hedge_unwind_step1_pct) or cls().hedge_unwind_step1_pct)
        params["hedge_unwind_step1_share"] = float(_as_float(params.get("hedge_unwind_step1_share", cls().hedge_unwind_step1_share), cls().hedge_unwind_step1_share) or cls().hedge_unwind_step1_share)
        params["hedge_unwind_step2_pct"] = float(_as_float(params.get("hedge_unwind_step2_pct", cls().hedge_unwind_step2_pct), cls().hedge_unwind_step2_pct) or cls().hedge_unwind_step2_pct)
        params["hedge_unwind_step2_share"] = float(_as_float(params.get("hedge_unwind_step2_share", cls().hedge_unwind_step2_share), cls().hedge_unwind_step2_share) or cls().hedge_unwind_step2_share)
        params["hedge_unwind_full_exit_pct"] = float(_as_float(params.get("hedge_unwind_full_exit_pct", cls().hedge_unwind_full_exit_pct), cls().hedge_unwind_full_exit_pct) or cls().hedge_unwind_full_exit_pct)
        params["hedge_unwind_cooldown_sec"] = int(_as_int(params.get("hedge_unwind_cooldown_sec", cls().hedge_unwind_cooldown_sec), cls().hedge_unwind_cooldown_sec) or cls().hedge_unwind_cooldown_sec)
        params["hedge_unwind_min_qty_ratio"] = float(_as_float(params.get("hedge_unwind_min_qty_ratio", cls().hedge_unwind_min_qty_ratio), cls().hedge_unwind_min_qty_ratio) or cls().hedge_unwind_min_qty_ratio)
        params["main_partial_tp_enabled"] = _as_bool(params.get("main_partial_tp_enabled", cls().main_partial_tp_enabled), cls().main_partial_tp_enabled)
        params["main_partial_tp1_pct"] = float(_as_float(params.get("main_partial_tp1_pct", cls().main_partial_tp1_pct), cls().main_partial_tp1_pct) or cls().main_partial_tp1_pct)
        params["main_partial_tp1_share"] = float(_as_float(params.get("main_partial_tp1_share", cls().main_partial_tp1_share), cls().main_partial_tp1_share) or cls().main_partial_tp1_share)
        params["main_partial_tp2_pct"] = float(_as_float(params.get("main_partial_tp2_pct", cls().main_partial_tp2_pct), cls().main_partial_tp2_pct) or cls().main_partial_tp2_pct)
        params["main_partial_tp2_share"] = float(_as_float(params.get("main_partial_tp2_share", cls().main_partial_tp2_share), cls().main_partial_tp2_share) or cls().main_partial_tp2_share)
        params["main_partial_tp_cooldown_sec"] = int(_as_int(params.get("main_partial_tp_cooldown_sec", cls().main_partial_tp_cooldown_sec), cls().main_partial_tp_cooldown_sec) or cls().main_partial_tp_cooldown_sec)
        params["main_partial_tp_min_qty_ratio"] = float(_as_float(params.get("main_partial_tp_min_qty_ratio", cls().main_partial_tp_min_qty_ratio), cls().main_partial_tp_min_qty_ratio) or cls().main_partial_tp_min_qty_ratio)
        params["main_trailing_after_tp1_enabled"] = _as_bool(params.get("main_trailing_after_tp1_enabled", cls().main_trailing_after_tp1_enabled), cls().main_trailing_after_tp1_enabled)
        params["main_trailing_after_tp1_cb_pct"] = float(_as_float(params.get("main_trailing_after_tp1_cb_pct", cls().main_trailing_after_tp1_cb_pct), cls().main_trailing_after_tp1_cb_pct) or cls().main_trailing_after_tp1_cb_pct)
        params["main_trailing_after_tp2_enabled"] = _as_bool(params.get("main_trailing_after_tp2_enabled", cls().main_trailing_after_tp2_enabled), cls().main_trailing_after_tp2_enabled)
        params["main_trailing_after_tp2_cb_pct"] = float(_as_float(params.get("main_trailing_after_tp2_cb_pct", cls().main_trailing_after_tp2_cb_pct), cls().main_trailing_after_tp2_cb_pct) or cls().main_trailing_after_tp2_cb_pct)
        params["main_exit_small_remainder_enabled"] = _as_bool(params.get("main_exit_small_remainder_enabled", cls().main_exit_small_remainder_enabled), cls().main_exit_small_remainder_enabled)
        params["main_exit_small_remainder_qty_ratio"] = float(_as_float(params.get("main_exit_small_remainder_qty_ratio", cls().main_exit_small_remainder_qty_ratio), cls().main_exit_small_remainder_qty_ratio) or cls().main_exit_small_remainder_qty_ratio)
        params["hedge_unwind_full_exit_enabled"] = _as_bool(params.get("hedge_unwind_full_exit_enabled", cls().hedge_unwind_full_exit_enabled), cls().hedge_unwind_full_exit_enabled)
        params["hedge_funding_guard_enabled"] = _as_bool(params.get("hedge_funding_guard_enabled", cls().hedge_funding_guard_enabled), cls().hedge_funding_guard_enabled)
        params["hedge_funding_bad_limit_pct"] = float(_as_float(params.get("hedge_funding_bad_limit_pct", cls().hedge_funding_bad_limit_pct), cls().hedge_funding_bad_limit_pct) or cls().hedge_funding_bad_limit_pct)
        params["hedge_funding_reduce_step_pct"] = float(_as_float(params.get("hedge_funding_reduce_step_pct", cls().hedge_funding_reduce_step_pct), cls().hedge_funding_reduce_step_pct) or cls().hedge_funding_reduce_step_pct)
        params["hedge_funding_reduce_share"] = float(_as_float(params.get("hedge_funding_reduce_share", cls().hedge_funding_reduce_share), cls().hedge_funding_reduce_share) or cls().hedge_funding_reduce_share)
        params["hedge_funding_full_exit_limit_pct"] = float(_as_float(params.get("hedge_funding_full_exit_limit_pct", cls().hedge_funding_full_exit_limit_pct), cls().hedge_funding_full_exit_limit_pct) or cls().hedge_funding_full_exit_limit_pct)
        params["hedge_funding_cooldown_sec"] = int(_as_int(params.get("hedge_funding_cooldown_sec", cls().hedge_funding_cooldown_sec), cls().hedge_funding_cooldown_sec) or cls().hedge_funding_cooldown_sec)
        params["hedge_funding_soft_cap_ratio"] = float(_as_float(params.get("hedge_funding_soft_cap_ratio", cls().hedge_funding_soft_cap_ratio), cls().hedge_funding_soft_cap_ratio) or cls().hedge_funding_soft_cap_ratio)
        params["hedge_adaptive_enabled"] = _as_bool(params.get("hedge_adaptive_enabled", cls().hedge_adaptive_enabled), cls().hedge_adaptive_enabled)
        params["hedge_adaptive_trigger_pct"] = float(_as_float(params.get("hedge_adaptive_trigger_pct", cls().hedge_adaptive_trigger_pct), cls().hedge_adaptive_trigger_pct) or cls().hedge_adaptive_trigger_pct)
        params["hedge_adaptive_step_pct"] = float(_as_float(params.get("hedge_adaptive_step_pct", cls().hedge_adaptive_step_pct), cls().hedge_adaptive_step_pct) or cls().hedge_adaptive_step_pct)
        params["hedge_adaptive_boost_per_step"] = float(_as_float(params.get("hedge_adaptive_boost_per_step", cls().hedge_adaptive_boost_per_step), cls().hedge_adaptive_boost_per_step) or cls().hedge_adaptive_boost_per_step)
        params["hedge_adaptive_max_extra_koff"] = float(_as_float(params.get("hedge_adaptive_max_extra_koff", cls().hedge_adaptive_max_extra_koff), cls().hedge_adaptive_max_extra_koff) or cls().hedge_adaptive_max_extra_koff)
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

        params["live_entry_reduce_on_max_position"] = _as_bool(
            params.get("live_entry_reduce_on_max_position", cls().live_entry_reduce_on_max_position),
            cls().live_entry_reduce_on_max_position,
        )
        params["live_entry_reduce_steps"] = int(
            _as_int(params.get("live_entry_reduce_steps"), cls().live_entry_reduce_steps) or cls().live_entry_reduce_steps
        )
        params["live_entry_reduce_factor"] = _as_float(
            params.get("live_entry_reduce_factor", cls().live_entry_reduce_factor),
            cls().live_entry_reduce_factor,
        )

        params["debug"] = _as_bool(params.get("debug", cls().debug), cls().debug)
        params["debug_top"] = int(_as_int(params.get("debug_top"), cls().debug_top) or cls().debug_top)
        params["ema_log_enabled"] = _as_bool(params.get("ema_log_enabled", cls().ema_log_enabled), cls().ema_log_enabled)
        # 3) store unknown keys so strategy can read them via getattr
        known = set(cls.__dataclass_fields__.keys())
        extras = {k: v for k, v in (d or {}).items() if k not in known}
        if extras:
            params['extras'] = extras

        return cls(**params)

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

def _tl_norm_list(v):
    if v is None:
        return []
    if isinstance(v, (list, tuple, set)):
        return [str(x).strip() for x in v if str(x).strip()]
    s = str(v).strip()
    return [s] if s else []

__all__ = [
    "TradeLiquidationParams",
    "_ADD_RE",
    "_COID_SAN_RE",
    "_tl_place_algo_with_retry",
    "_utc_now",
    "_dec",
    "_round_to_step",
    "_round_qty_to_step",
    "_as_float_clean",
    "_safe_float",
    "_as_float",
    "_as_int",
    "_ensure_tp_trail_side",
    "_sanitize_coid_prefix",
    "_coid_token",
    "_stable_live_signal_pos_uid",
    "_as_bool",
    "_round_step_down",
    "_round_price_to_tick",
    "_pct_to_mult",
    "_sig_side_to_ledger_side",
    "_ensure_paper_tables",
    "_has_column",
    "_tl_norm_list",
    "_cap_qty_by_wallet_notional",
]
