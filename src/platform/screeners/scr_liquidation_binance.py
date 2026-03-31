# src/platform/screeners/scr_liquidation_binance.py
from __future__ import annotations

import logging
from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

log = logging.getLogger("screeners.scr_liquidation_binance")


# =========================================================
# utils
# =========================================================

def _utc(dt: Any) -> datetime:
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    raise TypeError(f"Expected datetime, got {type(dt)}")

def _ts_key(dt: Any) -> datetime:
    """Normalize timestamps for dict keys (UTC, no microseconds)."""
    d = _utc(dt)
    return d.replace(microsecond=0)


def _to_float(x: Any, default: float = 0.0) -> float:
    if x is None:
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default

def _to_float_opt(x: Any) -> Optional[float]:
    """Return float(x) or None if x is None/invalid (keeps missing data visible)."""
    if x is None:
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None



def _to_int(x: Any, default: int = 0) -> int:
    if x is None:
        return default
    try:
        return int(x)
    except (TypeError, ValueError):
        return default


def _to_bool(x: Any, default: bool = False) -> bool:
    if isinstance(x, bool):
        return x
    if x is None:
        return default
    s = str(x).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off", ""):
        return False
    return default


def _parse_datetime_opt(x: Any) -> Optional[datetime]:
    if x in (None, "", "null", "None"):
        return None
    if isinstance(x, datetime):
        return _utc(x)
    try:
        return _utc(datetime.fromisoformat(str(x)))
    except (TypeError, ValueError):
        return None


def _parse_interval(interval: str) -> timedelta:
    s = str(interval).strip().lower()
    try:
        if s.endswith("m"):
            return timedelta(minutes=int(s[:-1]))
        if s.endswith("h"):
            return timedelta(hours=int(s[:-1]))
        if s.endswith("d"):
            return timedelta(days=int(s[:-1]))
    except (TypeError, ValueError):
        pass
    return timedelta(hours=1)


# =========================================================
# dataclasses
# =========================================================

@dataclass
class ScrParams:
    # -------------------------
    # базовые фильтры
    # -------------------------
    min_price: float = 0.00000001
    max_price: float = 100000.0
    interval: str = "1h"
    as_of_ts: Optional[datetime] = None
    # -------------------------
    # ликвидации + объём
    # -------------------------
    volume_liquid_limit: float = 5_000.0

    # -------------------------
    # фильтр всплеска объёма (peak volume in recent touch window vs avg(volume) before anchor)
    # -------------------------
    volume_avg_window: int = 20          # Сколько свечей ДО anchor для среднего объёма
    volume_spike_k: float = 0.0          # <=0 => отключить; иначе peak recent volume должно быть >= avg * K

    liq_dominance_pct: float = 10.0

    # -------------------------
    # уровни
    # -------------------------
    period_levels: int = 60
    level_tol_pct: float = 0.01

    pivot_left: int = 2
    pivot_right: int = 2
    level_cluster_tol_pct: float = 0.003
    max_level_candidates: int = 14
    min_level_cluster_strength: int = 0
    allow_fallback_levels: bool = False

    # -------------------------
    # ✅ TOUCH/CROSS lookback
    # -------------------------
    touch_lookback_candles: int = 3
    touch_mode: str = "touch_or_cross"  # touch/cross/touch_or_cross

    # -------------------------
    # подтверждение
    # -------------------------
    confirm_lookforward: int = 3
    confirm_body_min_pct: float = 0.0    # min(body/range) * 100 для confirm-candle
    confirm_close_vs_anchor_mid: bool = False

    # -------------------------
    # anchor / реакция от ликвидации
    # -------------------------
    anchor_wick_to_range_min_pct: float = 0.0
    anchor_wick_to_body_min: float = 0.0
    anchor_close_recovery_min_pct: float = 0.0
    anchor_to_level_max_pct: float = 0.0
    followthrough_max_pct: float = 0.0
    followthrough_check_candles: int = 1

    # -------------------------
    # относительная сила ликвидации
    # -------------------------
    liq_relative_window: int = 0
    liq_relative_k: float = 0.0

    # -------------------------
    # уровни / качество зон
    # -------------------------
    level_reaction_lookahead: int = 3
    level_reaction_min_pct: float = 0.25
    level_zone_extra_tol_pct: float = 0.0015
    acceptance_bins: int = 24
    require_htf_level_overlap: bool = False
    htf_level_interval: str = "15m"
    htf_level_tol_pct: float = 0.003

    # -------------------------
    # дополнительные кандидаты / контекст
    # -------------------------
    max_anchor_candidates: int = 1
    trend_ema_span: int = 34
    trend_extension_max_pct: float = 0.0

    # -------------------------
    # опциональные подтверждения
    # -------------------------
    kof_fund: float = 0.5
    enable_funding: bool = False

    # -------------------------
    # entry
    # -------------------------
    entry_price_mode: str = "current"  # current / confirm_close / anchor_close

    # -------------------------
    # SL/TP (pct)
    # -------------------------
    stop_loss_pct: float = 1.0
    take_profit_mode: str = "pct"
    take_profit_pct: float = 2.0

    # -------------------------
    # риск
    # -------------------------
    risk_trade_pct: float = 0.5
    risk_equity_usdt: Optional[float] = None
    risk_account_id: int = 1

    # -------------------------
    # графики/телега
    # -------------------------
    enable_plots: bool = False
    plots_dir: str = "artifacts/screener_plots"
    plot_lookback: int = 120
    plot_lookforward: int = 40

    telegram_enabled: bool = False
    telegram_mode: str = "batch"
    telegram_max_signals: int = 200
    telegram_timezone: str = "Europe/Moscow"
    telegram_extras_enabled: bool = False
    telegram_max_friends: int = 0

    # -------------------------
    # debug
    # -------------------------
    debug: bool = False
    debug_top: int = 20


@dataclass
class ScreenerSignal:
    symbol_id: int
    symbol: str
    timeframe: str
    signal_ts: datetime
    side: str  # "BUY" / "SELL"

    entry_price: float
    exit_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None

    confidence: float = 0.0
    score: float = 0.0
    reason: str = ""
    context: Dict[str, Any] = field(default_factory=dict)


# =========================================================
# Screener
# =========================================================

class ScrLiquidationBinance:
    name = "scr_liquidation_binance"

    def __init__(self) -> None:
        self._runtime_context: Dict[str, Any] = {}
        self._levels_cache: Dict[Tuple[Any, ...], Tuple[float, float, Dict[str, Any]]] = {}

    def set_runtime_context(self, ctx: Optional[Dict[str, Any]]) -> None:
        self._runtime_context = dict(ctx or {})

    def clear_runtime_context(self) -> None:
        self._runtime_context = {}

    """
    BUY только если:
      - TOUCH/CROSS DOWN уровня за последние N свечей (включая anchor)
      - long_liq >= volume_liquid_limit
      - long_liq доминирует над short_liq (liq_dominance_pct)
      - CONFIRM UP: зелёная свеча (close > open) в confirm_window (+ optional FUND)

    SELL только если:
      - TOUCH/CROSS UP уровня за последние N свечей (включая anchor)
      - short_liq >= volume_liquid_limit
      - short_liq доминирует над long_liq
      - CONFIRM DOWN: красная свеча (close < open) в confirm_window (+ optional FUND)

    В context:
      touch_side, touch_ts (datetime), touch_level, touch_age_candles, touch_kind,
      why_not_buy/why_not_sell (list[str]).
    """

    # =========================================================
    # touch/cross
    # =========================================================

    @staticmethod
    def _touch_hit_kind(
        *,
        level: float,
        low: float,
        high: float,
        open_: float,
        close: float,
        tol_pct: float,
        mode: str,
    ) -> Optional[str]:
        if level <= 0:
            return None

        tol = abs(float(level) * float(tol_pct))
        lo = float(level) - tol
        hi = float(level) + tol

        in_band = (float(low) <= hi and float(high) >= lo) or (lo <= float(close) <= hi)

        cross = (
            (float(open_) < lo and float(close) > hi) or
            (float(open_) > hi and float(close) < lo)
        )

        m = str(mode or "").strip().lower()
        if m == "cross":
            return "CROSS" if cross else None
        if m == "touch":
            return "TOUCH" if in_band else None

        if cross:
            return "CROSS"
        if in_band:
            return "TOUCH"
        return None

    def _find_recent_touch_event(
        self,
        *,
        candles: Sequence[Dict[str, Any]],
        idx_anchor: int,
        level: float,
        p: ScrParams,
    ) -> Optional[Tuple[int, datetime, str]]:
        lb = max(1, int(p.touch_lookback_candles))
        start = max(0, idx_anchor - lb + 1)
        end = idx_anchor + 1

        for i in range(end - 1, start - 1, -1):
            c = candles[i]
            kind = self._touch_hit_kind(
                level=float(level),
                low=_to_float(c.get("low")),
                high=_to_float(c.get("high")),
                open_=_to_float(c.get("open")),
                close=_to_float(c.get("close")),
                tol_pct=p.level_tol_pct,
                mode=p.touch_mode,
            )
            if kind:
                return i, _utc(c["ts"]), str(kind)
        return None

    # =========================================================
    # params
    # =========================================================

    @staticmethod
    def _parse_params(*, interval: str, params: Dict[str, Any]) -> ScrParams:
        p = ScrParams()
        p.interval = str(params.get("interval", interval))
        p.as_of_ts = _parse_datetime_opt(params.get("as_of_ts"))

        p.min_price = _to_float(params.get("min_price", p.min_price), p.min_price)
        p.max_price = _to_float(params.get("max_price", p.max_price), p.max_price)

        p.volume_liquid_limit = _to_float(params.get("volume_liquid_limit", p.volume_liquid_limit), p.volume_liquid_limit)
        # --- recent volume spike vs average before anchor ---
        p.volume_avg_window = _to_int(params.get("volume_avg_window", params.get("windows", p.volume_avg_window)), p.volume_avg_window)
        vsk_raw = params.get("volume_spike_k", None)
        if vsk_raw is None:
            vcp_raw = params.get("volume_change_pct", None)
            if vcp_raw is None:
                # backward compatibility: kof_Volume=1.25 => avg * 1.25
                kv = _to_float(params.get("kof_Volume", 0.0), 0.0)
                vsk_raw = kv if kv and kv > 0 else 0.0
            else:
                vcp = _to_float(vcp_raw, 0.0)
                vsk_raw = 1.0 + (vcp / 100.0) if vcp > 0 else 0.0
        p.volume_spike_k = _to_float(vsk_raw, p.volume_spike_k)

        p.liq_dominance_pct = _to_float(params.get("liq_dominance_pct", p.liq_dominance_pct), p.liq_dominance_pct)

        p.period_levels = _to_int(params.get("period_levels", p.period_levels), p.period_levels)
        p.level_tol_pct = _to_float(params.get("level_tol_pct", p.level_tol_pct), p.level_tol_pct)

        p.pivot_left = _to_int(params.get("pivot_left", p.pivot_left), p.pivot_left)
        p.pivot_right = _to_int(params.get("pivot_right", p.pivot_right), p.pivot_right)
        p.level_cluster_tol_pct = _to_float(params.get("level_cluster_tol_pct", p.level_cluster_tol_pct), p.level_cluster_tol_pct)
        p.max_level_candidates = _to_int(params.get("max_level_candidates", p.max_level_candidates), p.max_level_candidates)
        p.min_level_cluster_strength = _to_int(params.get("min_level_cluster_strength", p.min_level_cluster_strength), p.min_level_cluster_strength)
        p.allow_fallback_levels = _to_bool(params.get("allow_fallback_levels", p.allow_fallback_levels), p.allow_fallback_levels)

        tlb = params.get("touch_lookback_candles", None)
        if tlb is None:
            tlb = params.get("cross_lookback_candles", p.touch_lookback_candles)
        p.touch_lookback_candles = _to_int(tlb, p.touch_lookback_candles)

        p.touch_mode = str(params.get("touch_mode", p.touch_mode))

        p.confirm_lookforward = _to_int(params.get("confirm_lookforward", p.confirm_lookforward), p.confirm_lookforward)
        p.confirm_body_min_pct = _to_float(params.get("confirm_body_min_pct", p.confirm_body_min_pct), p.confirm_body_min_pct)
        p.confirm_close_vs_anchor_mid = _to_bool(params.get("confirm_close_vs_anchor_mid", p.confirm_close_vs_anchor_mid), p.confirm_close_vs_anchor_mid)

        p.anchor_wick_to_range_min_pct = _to_float(params.get("anchor_wick_to_range_min_pct", p.anchor_wick_to_range_min_pct), p.anchor_wick_to_range_min_pct)
        p.anchor_wick_to_body_min = _to_float(params.get("anchor_wick_to_body_min", p.anchor_wick_to_body_min), p.anchor_wick_to_body_min)
        p.anchor_close_recovery_min_pct = _to_float(params.get("anchor_close_recovery_min_pct", p.anchor_close_recovery_min_pct), p.anchor_close_recovery_min_pct)
        p.anchor_to_level_max_pct = _to_float(params.get("anchor_to_level_max_pct", p.anchor_to_level_max_pct), p.anchor_to_level_max_pct)
        p.followthrough_max_pct = _to_float(params.get("followthrough_max_pct", p.followthrough_max_pct), p.followthrough_max_pct)
        p.followthrough_check_candles = _to_int(params.get("followthrough_check_candles", p.followthrough_check_candles), p.followthrough_check_candles)

        p.liq_relative_window = _to_int(params.get("liq_relative_window", p.liq_relative_window), p.liq_relative_window)
        p.liq_relative_k = _to_float(params.get("liq_relative_k", p.liq_relative_k), p.liq_relative_k)

        p.level_reaction_lookahead = _to_int(params.get("level_reaction_lookahead", p.level_reaction_lookahead), p.level_reaction_lookahead)
        p.level_reaction_min_pct = _to_float(params.get("level_reaction_min_pct", p.level_reaction_min_pct), p.level_reaction_min_pct)
        p.level_zone_extra_tol_pct = _to_float(params.get("level_zone_extra_tol_pct", p.level_zone_extra_tol_pct), p.level_zone_extra_tol_pct)
        p.acceptance_bins = _to_int(params.get("acceptance_bins", p.acceptance_bins), p.acceptance_bins)
        p.require_htf_level_overlap = _to_bool(params.get("require_htf_level_overlap", p.require_htf_level_overlap), p.require_htf_level_overlap)
        p.htf_level_interval = str(params.get("htf_level_interval", p.htf_level_interval))
        p.htf_level_tol_pct = _to_float(params.get("htf_level_tol_pct", p.htf_level_tol_pct), p.htf_level_tol_pct)

        p.max_anchor_candidates = _to_int(params.get("max_anchor_candidates", p.max_anchor_candidates), p.max_anchor_candidates)
        p.trend_ema_span = _to_int(params.get("trend_ema_span", p.trend_ema_span), p.trend_ema_span)
        p.trend_extension_max_pct = _to_float(params.get("trend_extension_max_pct", p.trend_extension_max_pct), p.trend_extension_max_pct)

        p.kof_fund = _to_float(params.get("kof_fund", p.kof_fund), p.kof_fund)
        p.enable_funding = _to_bool(params.get("enable_funding", p.enable_funding), p.enable_funding)

        p.entry_price_mode = str(params.get("entry_price_mode", p.entry_price_mode))

        p.stop_loss_pct = _to_float(params.get("stop_loss_pct", p.stop_loss_pct), p.stop_loss_pct)
        p.take_profit_mode = "pct"
        p.take_profit_pct = _to_float(params.get("take_profit_pct", p.take_profit_pct), p.take_profit_pct)

        p.risk_trade_pct = _to_float(params.get("risk_trade_pct", p.risk_trade_pct), p.risk_trade_pct)
        risk_equity = params.get("risk_equity_usdt", None)
        if risk_equity in (None, "", 0, "0", "null", "None"):
            p.risk_equity_usdt = None
        else:
            p.risk_equity_usdt = _to_float(risk_equity, 0.0) or None
        p.risk_account_id = _to_int(params.get("risk_account_id", p.risk_account_id), p.risk_account_id)

        p.enable_plots = _to_bool(params.get("enable_plots", p.enable_plots), p.enable_plots)
        p.plots_dir = str(params.get("plots_dir", p.plots_dir))
        p.plot_lookback = _to_int(params.get("plot_lookback", p.plot_lookback), p.plot_lookback)
        p.plot_lookforward = _to_int(params.get("plot_lookforward", p.plot_lookforward), p.plot_lookforward)

        p.telegram_enabled = _to_bool(params.get("telegram_enabled", p.telegram_enabled), p.telegram_enabled)
        p.telegram_mode = str(params.get("telegram_mode", p.telegram_mode))
        p.telegram_max_signals = _to_int(params.get("telegram_max_signals", p.telegram_max_signals), p.telegram_max_signals)
        p.telegram_timezone = str(params.get("telegram_timezone", p.telegram_timezone))
        p.telegram_extras_enabled = _to_bool(params.get("telegram_extras_enabled", p.telegram_extras_enabled), p.telegram_extras_enabled)
        p.telegram_max_friends = _to_int(params.get("telegram_max_friends", p.telegram_max_friends), p.telegram_max_friends)

        p.debug = _to_bool(params.get("debug", False), False)
        p.debug_top = _to_int(params.get("debug_top", 20), 20)

        return p

    # =========================================================
    # entry price
    # =========================================================

    @staticmethod
    def _resolve_entry_price(
        *,
        mode: str,
        current_price: float,
        current_src: str,
        anchor_close: float,
        confirm_close: float,
    ) -> Tuple[float, str]:
        m = str(mode or "").strip().lower()
        if m in ("confirm", "confirm_close", "confirmclose"):
            return float(confirm_close), "confirm_close"
        if m in ("anchor", "anchor_close", "anchorclose"):
            return float(anchor_close), "anchor_close"
        return float(current_price), str(current_src)

    # =========================================================
    # dominance
    # =========================================================

    @staticmethod
    def _liq_dominance_ok(*, side: str, liq_long_usdt: float, liq_short_usdt: float, p: ScrParams) -> bool:
        k = 1.0 + abs(float(p.liq_dominance_pct)) / 100.0
        L = float(liq_long_usdt or 0.0)
        S = float(liq_short_usdt or 0.0)
        side_u = str(side).upper().strip()
        if side_u == "SELL":
            return S >= L * k
        return L >= S * k

    # =========================================================
    # SL/TP
    # =========================================================

    @staticmethod
    def _compute_sl_tp(*, side: str, entry_price: float, p: ScrParams) -> Tuple[Optional[float], Optional[float]]:
        ep = float(entry_price)
        sl_pct = max(0.0, float(p.stop_loss_pct)) / 100.0
        tp_pct = max(0.0, float(p.take_profit_pct)) / 100.0

        if ep <= 0 or sl_pct <= 0:
            return None, None

        side_u = str(side).upper().strip()
        if side_u == "SELL":
            sl = ep * (1.0 + sl_pct)
            tp = ep * (1.0 - tp_pct) if tp_pct > 0 else None
            return float(sl), float(tp) if tp is not None else None

        sl = ep * (1.0 - sl_pct)
        tp = ep * (1.0 + tp_pct) if tp_pct > 0 else None
        return float(sl), float(tp) if tp is not None else None

    # =========================================================
    # equity / position sizing
    # =========================================================

    @staticmethod
    def _resolve_equity_usdt(*, storage: Any, exchange_id: int, p: ScrParams) -> Optional[float]:
        if p.risk_equity_usdt is not None and p.risk_equity_usdt > 0:
            return float(p.risk_equity_usdt)

        q1 = """
        SELECT equity
        FROM public.account_state
        WHERE exchange_id=%s AND account_id=%s
        ORDER BY ts DESC
        LIMIT 1
        """
        try:
            with storage.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(q1, (int(exchange_id), int(p.risk_account_id)))
                    r = cur.fetchone()
                    if r and r[0] is not None:
                        v = _to_float(r[0], default=0.0)
                        return v if v > 0 else None
        except Exception:
            pass

        q2 = """
        SELECT equity
        FROM public.account_balance_snapshots
        WHERE exchange_id=%s AND account_id=%s
        ORDER BY ts DESC
        LIMIT 1
        """
        try:
            with storage.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(q2, (int(exchange_id), int(p.risk_account_id)))
                    r = cur.fetchone()
                    if r and r[0] is not None:
                        v = _to_float(r[0], default=0.0)
                        return v if v > 0 else None
        except Exception:
            pass

        return None

    @staticmethod
    def _compute_position_size(
        *,
        entry_price: float,
        stop_loss: Optional[float],
        equity_usdt: Optional[float],
        p: ScrParams,
    ) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        ep = float(entry_price)
        if ep <= 0 or stop_loss is None:
            return None, None, None
        if equity_usdt is None or float(equity_usdt) <= 0:
            return None, None, None

        risk_pct = max(0.0, float(p.risk_trade_pct)) / 100.0
        risk_usdt = float(equity_usdt) * risk_pct

        dist = abs(ep - float(stop_loss))
        if dist <= 0:
            return None, None, None

        qty = risk_usdt / dist
        notional = qty * ep
        return float(qty), float(notional), float(risk_usdt)

    # =========================================================
    # confirmation helpers
    # =========================================================

    @staticmethod
    def _fund_threshold(p: ScrParams) -> float:
        return abs(float(p.kof_fund)) / 100.0

    @staticmethod
    def _confirm_body_ok(candle: Dict[str, Any], p: ScrParams) -> bool:
        min_pct = max(0.0, float(getattr(p, "confirm_body_min_pct", 0.0)))
        if min_pct <= 0.0:
            return True

        high = _to_float(candle.get("high"))
        low = _to_float(candle.get("low"))
        open_ = _to_float(candle.get("open"))
        close = _to_float(candle.get("close"))
        rng = max(0.0, high - low)
        if rng <= 0.0:
            return False

        body_pct = abs(close - open_) / rng * 100.0
        return body_pct >= min_pct


    @staticmethod
    def _candle_range_metrics(candle: Dict[str, Any]) -> Dict[str, float]:
        high = _to_float(candle.get("high"))
        low = _to_float(candle.get("low"))
        open_ = _to_float(candle.get("open"))
        close = _to_float(candle.get("close"))
        rng = max(0.0, high - low)
        body = abs(close - open_)
        lower_wick = max(0.0, min(open_, close) - low)
        upper_wick = max(0.0, high - max(open_, close))
        close_pos_pct = ((close - low) / rng * 100.0) if rng > 0 else 0.0
        body_pct = (body / rng * 100.0) if rng > 0 else 0.0
        return {
            "range": float(rng),
            "body": float(body),
            "body_pct": float(body_pct),
            "lower_wick": float(lower_wick),
            "upper_wick": float(upper_wick),
            "lower_wick_pct": float((lower_wick / rng * 100.0) if rng > 0 else 0.0),
            "upper_wick_pct": float((upper_wick / rng * 100.0) if rng > 0 else 0.0),
            "close_pos_pct": float(close_pos_pct),
            "close_recovery_from_low_pct": float(close_pos_pct),
            "close_recovery_from_high_pct": float((high - close) / rng * 100.0 if rng > 0 else 0.0),
        }

    def _anchor_wick_ok(self, *, side: str, candle: Dict[str, Any], p: ScrParams) -> Tuple[bool, Dict[str, float], str]:
        m = self._candle_range_metrics(candle)
        rng = float(m["range"])
        if rng <= 0.0:
            return False, m, "anchor_range=0"

        min_wick_pct = max(0.0, float(getattr(p, "anchor_wick_to_range_min_pct", 0.0)))
        min_wick_body = max(0.0, float(getattr(p, "anchor_wick_to_body_min", 0.0)))
        min_recovery_pct = max(0.0, float(getattr(p, "anchor_close_recovery_min_pct", 0.0)))
        body = max(1e-12, float(m["body"]))
        side_u = str(side).upper().strip()

        if side_u == "BUY":
            wick = float(m["lower_wick"])
            wick_pct = float(m["lower_wick_pct"])
            recovery_pct = float(m["close_recovery_from_low_pct"])
        else:
            wick = float(m["upper_wick"])
            wick_pct = float(m["upper_wick_pct"])
            recovery_pct = float(m["close_recovery_from_high_pct"])

        if min_wick_pct > 0.0 and wick_pct < min_wick_pct:
            return False, m, f"wick_pct<{min_wick_pct}"
        if min_wick_body > 0.0 and (wick / body) < min_wick_body:
            return False, m, f"wick_to_body<{min_wick_body}"
        if min_recovery_pct > 0.0 and recovery_pct < min_recovery_pct:
            return False, m, f"close_recovery<{min_recovery_pct}"
        return True, m, "ok"

    @staticmethod
    def _confirm_vs_anchor_mid_ok(*, side: str, confirm_candle: Dict[str, Any], anchor_candle: Dict[str, Any], p: ScrParams) -> bool:
        if not bool(getattr(p, "confirm_close_vs_anchor_mid", False)):
            return True
        anchor_high = _to_float(anchor_candle.get("high"))
        anchor_low = _to_float(anchor_candle.get("low"))
        confirm_close = _to_float(confirm_candle.get("close"))
        anchor_mid = (anchor_high + anchor_low) / 2.0
        if str(side).upper().strip() == "SELL":
            return confirm_close <= anchor_mid
        return confirm_close >= anchor_mid

    @staticmethod
    def _anchor_to_level_distance_pct(*, side: str, anchor_candle: Dict[str, Any], level: float) -> float:
        if level <= 0:
            return 999999.0
        if str(side).upper().strip() == "SELL":
            px = _to_float(anchor_candle.get("high"))
        else:
            px = _to_float(anchor_candle.get("low"))
        return abs(px - float(level)) / float(level) * 100.0

    def _followthrough_ok(
        self,
        *,
        side: str,
        candles: Sequence[Dict[str, Any]],
        anchor_idx: int,
        p: ScrParams,
    ) -> Tuple[bool, float]:
        max_pct = max(0.0, float(getattr(p, "followthrough_max_pct", 0.0)))
        n = max(0, int(getattr(p, "followthrough_check_candles", 1)))
        if max_pct <= 0.0 or n <= 0:
            return True, 0.0
        anchor = candles[anchor_idx]
        a_low = _to_float(anchor.get("low"))
        a_high = _to_float(anchor.get("high"))
        window = candles[anchor_idx + 1: anchor_idx + 1 + n]
        if not window:
            return True, 0.0
        side_u = str(side).upper().strip()
        if side_u == "BUY":
            worst = min(_to_float(c.get("low"), a_low) for c in window)
            ext = max(0.0, (a_low - worst) / max(a_low, 1e-12) * 100.0)
        else:
            worst = max(_to_float(c.get("high"), a_high) for c in window)
            ext = max(0.0, (worst - a_high) / max(a_high, 1e-12) * 100.0)
        return ext <= max_pct, float(ext)

    @staticmethod
    def _ema(values: Sequence[float], span: int) -> float:
        vals = [float(v) for v in values if v is not None]
        if not vals:
            return 0.0
        span = max(1, int(span))
        alpha = 2.0 / (span + 1.0)
        ema = vals[0]
        for v in vals[1:]:
            ema = alpha * v + (1.0 - alpha) * ema
        return float(ema)

    def _trend_extension_ok(self, *, side: str, candles: Sequence[Dict[str, Any]], anchor_idx: int, p: ScrParams) -> Tuple[bool, float, float]:
        max_pct = max(0.0, float(getattr(p, "trend_extension_max_pct", 0.0)))
        span = max(2, int(getattr(p, "trend_ema_span", 34)))
        closes = [_to_float(c.get("close")) for c in candles[max(0, anchor_idx - span * 3): anchor_idx + 1]]
        ema = self._ema(closes, span)
        anchor_close = _to_float(candles[anchor_idx].get("close"))
        ext_pct = abs(anchor_close - ema) / max(abs(ema), 1e-12) * 100.0 if ema > 0 else 0.0
        if max_pct <= 0.0:
            return True, float(ext_pct), float(ema)
        return ext_pct <= max_pct, float(ext_pct), float(ema)

    def _find_confirmation_short(
        self,
        *,
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        symbol: str,
        interval: str,
        p: ScrParams,
        confirm_window: Sequence[Dict[str, Any]],
        anchor_candle: Dict[str, Any],
    ) -> Optional[Tuple[datetime, float, Optional[float], str, Dict[str, Any]]]:
        """Подтверждение для SELL: ищем RED свечу в confirm_window (+ optional funding filter)."""
        if not confirm_window:
            return None

        thr = self._fund_threshold(p)
        anchor_mid = (_to_float(anchor_candle.get("high")) + _to_float(anchor_candle.get("low"))) / 2.0

        for c in confirm_window:
            ts = _utc(c["ts"])
            o = _to_float(c.get("open"))
            close = _to_float(c.get("close"))
            metrics = self._candle_range_metrics(c)

            if close >= o:
                continue
            if not self._confirm_body_ok(c, p):
                continue
            if not self._confirm_vs_anchor_mid_ok(side="SELL", confirm_candle=c, anchor_candle=anchor_candle, p=p):
                continue

            fund_c: Optional[float] = None
            if p.enable_funding:
                fund_c = self._fetch_funding_at(storage, exchange_id, symbol_id, ts)
                if fund_c is None:
                    continue
                if _to_float(fund_c) < thr:
                    continue

            why = "CONFIRM DOWN: red candle with body/reclaim filters"
            return ts, close, (float(fund_c) if fund_c is not None else None), why, {
                "confirm_body_pct": float(metrics.get("body_pct") or 0.0),
                "confirm_close_vs_anchor_mid_pct": float((anchor_mid - close) / max(abs(anchor_mid), 1e-12) * 100.0),
                "confirm_close_pos_pct": float(metrics.get("close_pos_pct") or 0.0),
            }

        return None

    def _find_confirmation_long(
        self,
        *,
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        symbol: str,
        interval: str,
        p: ScrParams,
        confirm_window: Sequence[Dict[str, Any]],
        anchor_candle: Dict[str, Any],
    ) -> Optional[Tuple[datetime, float, Optional[float], str, Dict[str, Any]]]:
        """Подтверждение для BUY: ищем GREEN свечу в confirm_window (+ optional funding filter)."""
        if not confirm_window:
            return None

        thr = self._fund_threshold(p)
        anchor_mid = (_to_float(anchor_candle.get("high")) + _to_float(anchor_candle.get("low"))) / 2.0

        for c in confirm_window:
            ts = _utc(c["ts"])
            o = _to_float(c.get("open"))
            close = _to_float(c.get("close"))
            metrics = self._candle_range_metrics(c)

            if close <= o:
                continue
            if not self._confirm_body_ok(c, p):
                continue
            if not self._confirm_vs_anchor_mid_ok(side="BUY", confirm_candle=c, anchor_candle=anchor_candle, p=p):
                continue

            fund_c: Optional[float] = None
            if p.enable_funding:
                fund_c = self._fetch_funding_at(storage, exchange_id, symbol_id, ts)
                if fund_c is None:
                    continue
                if _to_float(fund_c) > -thr:
                    continue

            why = "CONFIRM UP: green candle with body/reclaim filters"
            return ts, close, (float(fund_c) if fund_c is not None else None), why, {
                "confirm_body_pct": float(metrics.get("body_pct") or 0.0),
                "confirm_close_vs_anchor_mid_pct": float((close - anchor_mid) / max(abs(anchor_mid), 1e-12) * 100.0),
                "confirm_close_pos_pct": float(metrics.get("close_pos_pct") or 0.0),
            }

        return None

    # signal builder
    # =========================================================

    @staticmethod
    def _make_signal(
        *,
        symbol_id: int,
        symbol: str,
        timeframe: str,
        signal_ts: datetime,
        side: str,
        entry_price: float,
        sl: Optional[float],
        tp: Optional[float],
        confidence: float,
        score: float,
        reason: str,
        context: Dict[str, Any],
    ) -> ScreenerSignal:
        return ScreenerSignal(
            symbol_id=int(symbol_id),
            symbol=str(symbol),
            timeframe=str(timeframe),
            signal_ts=_utc(signal_ts),
            side=str(side).upper(),
            entry_price=float(entry_price),
            stop_loss=float(sl) if sl is not None else None,
            take_profit=float(tp) if tp is not None else None,
            confidence=float(confidence),
            score=float(score),
            reason=str(reason),
            context=dict(context or {}),
        )

    # =========================================================
    # levels: pivots + clustering
    # =========================================================

    def _build_levels(
        self,
        *,
        candles: Sequence[Dict[str, Any]],
        ref_price: float,
        p: ScrParams,
    ) -> Tuple[float, float, Dict[str, Any]]:
        meta: Dict[str, Any] = {
            "pivot_highs": 0,
            "pivot_lows": 0,
            "up_strength": 0,
            "down_strength": 0,
            "up_score": 0.0,
            "down_score": 0.0,
            "clusters_up": [],
            "clusters_down": [],
            "fallback": False,
        }

        highs_all = [_to_float(c.get("high")) for c in candles if c.get("high") is not None]
        lows_all = [_to_float(c.get("low")) for c in candles if c.get("low") is not None]
        max_high = max(highs_all) if highs_all else 0.0
        min_low = min(lows_all) if lows_all else 0.0

        if len(candles) < max(20, p.pivot_left + p.pivot_right + 5):
            meta["fallback"] = True
            return float(max_high), float(min_low), meta

        piv_h, piv_l = self._find_pivots(candles=candles, left=p.pivot_left, right=p.pivot_right)
        meta["pivot_highs"] = len(piv_h)
        meta["pivot_lows"] = len(piv_l)

        acceptance_up, acceptance_down = self._find_acceptance_levels(candles=candles, ref_price=ref_price, bins=p.acceptance_bins)
        cl_up = self._cluster_levels([{"price": x, "strength": 1.0, "kind": "pivot"} for x in piv_h] + acceptance_up, tol_pct=p.level_cluster_tol_pct, max_candidates=p.max_level_candidates)
        cl_dn = self._cluster_levels([{"price": x, "strength": 1.0, "kind": "pivot"} for x in piv_l] + acceptance_down, tol_pct=p.level_cluster_tol_pct, max_candidates=p.max_level_candidates)

        meta["clusters_up"] = cl_up
        meta["clusters_down"] = cl_dn

        up_cluster = self._pick_level_above(ref_price, cl_up)
        down_cluster = self._pick_level_below(ref_price, cl_dn)

        up = float((up_cluster or {}).get("price") or 0.0)
        down = float((down_cluster or {}).get("price") or 0.0)

        if up <= 0.0:
            up = max_high
        if down <= 0.0:
            down = min_low

        ref = float(ref_price)
        if down > ref:
            down = min_low
            meta["fallback"] = True
            meta["fallback_down"] = "min_low"
        if up < ref:
            up = max_high
            meta["fallback"] = True
            meta["fallback_up"] = "max_high"

        if up <= 0 or down <= 0 or up <= down:
            meta["fallback"] = True
            return float(max_high), float(min_low), meta

        up_zone = self._build_level_zone(candles=candles, level=up, side="UP", p=p)
        down_zone = self._build_level_zone(candles=candles, level=down, side="DOWN", p=p)

        meta["up_strength"] = int((up_cluster or {}).get("count") or 0)
        meta["down_strength"] = int((down_cluster or {}).get("count") or 0)
        meta["up_score"] = float((up_cluster or {}).get("score") or 0.0)
        meta["down_score"] = float((down_cluster or {}).get("score") or 0.0)
        meta["up_zone"] = up_zone
        meta["down_zone"] = down_zone

        return float(up), float(down), meta

    @staticmethod
    def _find_pivots(
        *,
        candles: Sequence[Dict[str, Any]],
        left: int,
        right: int,
    ) -> Tuple[List[float], List[float]]:
        highs = [_to_float(c.get("high")) for c in candles]
        lows = [_to_float(c.get("low")) for c in candles]

        n = len(candles)
        L = max(1, int(left))
        R = max(1, int(right))

        piv_h: List[float] = []
        piv_l: List[float] = []

        for i in range(L, n - R):
            h = highs[i]
            l = lows[i]
            left_h = highs[i - L:i]
            right_h = highs[i + 1:i + 1 + R]
            left_l = lows[i - L:i]
            right_l = lows[i + 1:i + 1 + R]
            if left_h and right_h and h > max(left_h) and h > max(right_h):
                piv_h.append(h)
            if left_l and right_l and l < min(left_l) and l < min(right_l):
                piv_l.append(l)
        return piv_h, piv_l

    @staticmethod
    def _find_acceptance_levels(*, candles: Sequence[Dict[str, Any]], ref_price: float, bins: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        bins = max(6, int(bins or 0))
        closes = [_to_float(c.get("close")) for c in candles if _to_float(c.get("close")) > 0]
        if len(closes) < 10:
            return [], []
        lo = min(closes)
        hi = max(closes)
        if hi <= lo:
            return [], []
        step = (hi - lo) / bins
        if step <= 0:
            return [], []
        weights: Dict[int, float] = {}
        for c in candles:
            px = _to_float(c.get("close"))
            if px <= 0:
                continue
            idx = min(bins - 1, max(0, int((px - lo) / step)))
            weights[idx] = weights.get(idx, 0.0) + max(1.0, _to_float(c.get("quote_volume") or c.get("volume"), 1.0) ** 0.25)
        ranked = sorted(weights.items(), key=lambda kv: kv[1], reverse=True)[: max(2, bins // 6)]
        up: List[Dict[str, Any]] = []
        down: List[Dict[str, Any]] = []
        for idx, w in ranked:
            price = lo + (idx + 0.5) * step
            item = {"price": float(price), "strength": float(w / max(1.0, len(candles) ** 0.25)), "kind": "acceptance"}
            if price >= ref_price:
                up.append(item)
            if price <= ref_price:
                down.append(item)
        return up, down

    @staticmethod
    def _cluster_levels(
        values: Sequence[Any],
        *,
        tol_pct: float,
        max_candidates: int,
    ) -> List[Dict[str, Any]]:
        vals: List[Dict[str, Any]] = []
        for v in values:
            if isinstance(v, dict):
                price = float(v.get("price") or 0.0)
                if price <= 0:
                    continue
                vals.append({
                    "price": price,
                    "strength": float(v.get("strength") or 1.0),
                    "kind": str(v.get("kind") or "pivot"),
                })
            else:
                price = float(v or 0.0)
                if price <= 0:
                    continue
                vals.append({"price": price, "strength": 1.0, "kind": "pivot"})
        if not vals:
            return []

        vals.sort(key=lambda x: x["price"])
        tol_pct = abs(float(tol_pct))
        clusters: List[List[Dict[str, Any]]] = []
        cur: List[Dict[str, Any]] = [vals[0]]
        for v in vals[1:]:
            base = float(cur[-1]["price"])
            tol = base * tol_pct
            if abs(float(v["price"]) - base) <= tol:
                cur.append(v)
            else:
                clusters.append(cur)
                cur = [v]
        clusters.append(cur)

        out: List[Dict[str, Any]] = []
        for cluster in clusters:
            prices = [float(x["price"]) for x in cluster]
            strengths = [float(x.get("strength") or 1.0) for x in cluster]
            total_strength = sum(strengths)
            price = sum(p * s for p, s in zip(prices, strengths)) / max(total_strength, 1e-12)
            count = len(cluster)
            acceptance_count = sum(1 for x in cluster if x.get("kind") == "acceptance")
            out.append({
                "price": float(price),
                "count": int(count),
                "score": float(count + acceptance_count * 0.75 + total_strength * 0.25),
                "min": float(min(prices)),
                "max": float(max(prices)),
                "acceptance_count": int(acceptance_count),
            })

        out.sort(key=lambda x: x["score"], reverse=True)
        if max_candidates and len(out) > int(max_candidates):
            out = out[: int(max_candidates)]
        out.sort(key=lambda x: x["price"])
        return out

    @staticmethod
    def _pick_level_above(ref_price: float, clusters: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        ref = float(ref_price)
        above = [c for c in clusters if float(c.get("price") or 0.0) >= ref]
        if not above:
            return None
        above.sort(key=lambda x: (float(x["price"]) - ref, -float(x.get("score") or 0.0)))
        return dict(above[0])

    @staticmethod
    def _pick_level_below(ref_price: float, clusters: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        ref = float(ref_price)
        below = [c for c in clusters if float(c.get("price") or 0.0) <= ref]
        if not below:
            return None
        below.sort(key=lambda x: (ref - float(x["price"]), -float(x.get("score") or 0.0)))
        return dict(below[0])

    @staticmethod
    def _cluster_strength(level: float, clusters: Sequence[Dict[str, Any]]) -> int:
        for c in clusters:
            price = float(c.get("price") or 0.0)
            if abs(price - level) <= max(1e-12, level * 1e-6):
                return int(c.get("count") or 0)
        return 0

    def _build_level_zone(self, *, candles: Sequence[Dict[str, Any]], level: float, side: str, p: ScrParams) -> Dict[str, Any]:
        tol_pct = max(abs(float(p.level_tol_pct)), abs(float(getattr(p, "level_zone_extra_tol_pct", 0.0))))
        tol = abs(float(level) * tol_pct)
        hits: List[int] = []
        lows: List[float] = []
        highs: List[float] = []
        reaction_moves: List[float] = []
        lookahead = max(1, int(getattr(p, "level_reaction_lookahead", 3)))
        reaction_min_pct = max(0.0, float(getattr(p, "level_reaction_min_pct", 0.0)))
        side_u = str(side).upper().strip()
        for i, c in enumerate(candles):
            low = _to_float(c.get("low"))
            high = _to_float(c.get("high"))
            if low <= level + tol and high >= level - tol:
                hits.append(i)
                lows.append(low)
                highs.append(high)
                future = candles[i + 1: i + 1 + lookahead]
                if future:
                    if side_u == "UP":
                        reaction = max((_to_float(fc.get("close")) - level) / max(level, 1e-12) * 100.0 for fc in future)
                    else:
                        reaction = max((level - _to_float(fc.get("close"))) / max(level, 1e-12) * 100.0 for fc in future)
                    if reaction >= reaction_min_pct:
                        reaction_moves.append(float(reaction))
        zone_low = min(lows) if lows else float(level)
        zone_high = max(highs) if highs else float(level)
        return {
            "low": float(zone_low),
            "high": float(zone_high),
            "touches": int(len(hits)),
            "successful_reactions": int(len(reaction_moves)),
            "best_reaction_pct": float(max(reaction_moves) if reaction_moves else 0.0),
        }

    @staticmethod
    def _zones_overlap(a: Dict[str, Any], b: Dict[str, Any], tol_pct: float) -> bool:
        alo = float(a.get("low") or a.get("price") or 0.0)
        ahi = float(a.get("high") or a.get("price") or 0.0)
        blo = float(b.get("low") or b.get("price") or 0.0)
        bhi = float(b.get("high") or b.get("price") or 0.0)
        ref = max(abs(ahi), abs(bhi), 1e-12)
        tol = ref * abs(float(tol_pct))
        return max(alo, blo) <= min(ahi, bhi) + tol
    # =========================================================
    # volume helper
    # =========================================================

    @staticmethod
    def _avg_volume_before(*, candles: Sequence[Dict[str, Any]], idx: int, window: int) -> float:
        w = int(window)
        if w <= 1:
            return 0.0
        if idx <= w + 1:
            return 0.0

        part = candles[idx - w: idx]
        vals = [_to_float(c.get("quote_volume") or c.get("volume")) for c in part]
        vals = [v for v in vals if v > 0]
        if not vals:
            return 0.0
        return sum(vals) / float(len(vals))

    # =========================================================
    # DB fetchers / runtime preloads
    # =========================================================

    def _ctx_get(self, key: str, default: Any = None) -> Any:
        return (self._runtime_context or {}).get(key, default)

    def _ctx_key(self, dt: datetime) -> datetime:
        return _ts_key(dt)

    def _slice_prefetched_candles(self, *, symbol_id: int, interval: str, limit: int, as_of_ts: Optional[datetime]) -> Optional[List[Dict[str, Any]]]:
        candles_map = self._ctx_get("candles_by_symbol_interval") or {}
        rows = candles_map.get((int(symbol_id), str(interval)))
        if not rows:
            return None
        if as_of_ts is None:
            return list(rows[-int(limit):])
        key = self._ctx_key(as_of_ts)
        ts_list = self._ctx_get("candle_ts_index") or {}
        idxs = ts_list.get((int(symbol_id), str(interval)))
        if not idxs:
            out = [r for r in rows if self._ctx_key(r["ts"]) <= key]
            return out[-int(limit):]
        pos = bisect_right(idxs, key)
        start = max(0, pos - int(limit))
        return list(rows[start:pos])

    def _prefetched_funding_at(self, *, symbol_id: int, ts: datetime) -> Optional[float]:
        sid = int(symbol_id)
        series = (self._ctx_get("funding_by_symbol") or {}).get(sid)
        if not series:
            return None
        keys_map = self._ctx_get("funding_ts_index") or {}
        keys = keys_map.get(sid)
        if not keys:
            keys = [x[0] for x in series]
        pos = bisect_right(keys, self._ctx_key(ts)) - 1
        if pos < 0:
            return None
        return _to_float_opt(series[pos][1])

    def _prefetched_liq_bucket(self, *, symbol_id: int, candle_open_ts: datetime) -> Optional[Tuple[float, float]]:
        buckets = self._ctx_get("liq_by_symbol_bucket") or {}
        row = buckets.get((int(symbol_id), self._ctx_key(candle_open_ts)))
        if row is None:
            return None
        return float(row.get("long", 0.0)), float(row.get("short", 0.0))

    def _prefetched_liq_history(self, *, symbol_id: int, anchor_ts: datetime, interval: str, lookback: int) -> Optional[List[float]]:
        hist_map = self._ctx_get("liq_history_by_symbol_interval") or {}
        row = hist_map.get((int(symbol_id), str(interval), self._ctx_key(anchor_ts), int(lookback)))
        if row is None:
            return None
        return list(row)

    def _cached_levels(self, *, symbol_id: int, interval: str, anchor_ts: datetime, candles: Sequence[Dict[str, Any]], ref_price: float, p: ScrParams) -> Tuple[float, float, Dict[str, Any]]:
        key = (int(symbol_id), str(interval), self._ctx_key(anchor_ts), len(candles), round(float(ref_price), 8), int(p.pivot_left), int(p.pivot_right), round(float(p.level_cluster_tol_pct), 8), int(p.max_level_candidates), int(p.acceptance_bins), round(float(p.level_tol_pct), 8), round(float(getattr(p, "level_zone_extra_tol_pct", 0.0)), 8), int(getattr(p, "level_reaction_lookahead", 0)), round(float(getattr(p, "level_reaction_min_pct", 0.0)), 8))
        cached = self._levels_cache.get(key)
        if cached is not None:
            up, down, meta = cached
            return float(up), float(down), dict(meta)
        up, down, meta = self._build_levels(candles=candles, ref_price=ref_price, p=p)
        self._levels_cache[key] = (float(up), float(down), dict(meta or {}))
        return float(up), float(down), dict(meta or {})

    def _apply_runtime_mode(self, p: ScrParams) -> ScrParams:
        mode = str(self._ctx_get("run_mode", "full") or "full").lower()
        if mode != "fast":
            return p
        q = ScrParams(**p.__dict__)
        q.acceptance_bins = min(int(q.acceptance_bins), 12)
        q.period_levels = min(int(q.period_levels), 260)
        q.volume_avg_window = min(int(q.volume_avg_window), 120)
        q.max_level_candidates = min(int(q.max_level_candidates), 8)
        q.max_anchor_candidates = 1
        q.require_htf_level_overlap = False
        q.level_reaction_lookahead = min(int(q.level_reaction_lookahead), 3)
        return q

    def _fetch_symbols(self, *, storage: Any, exchange_id: int) -> List[Dict[str, Any]]:
        prefetched = self._ctx_get("symbols")
        if isinstance(prefetched, list) and prefetched:
            return [dict(x) for x in prefetched]
        q = """
        SELECT symbol_id, symbol
        FROM symbols
        WHERE exchange_id = %s AND is_active = true
        ORDER BY symbol_id
        """
        with storage.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (int(exchange_id),))
                rows = cur.fetchall()
                return [{"symbol_id": r[0], "symbol": r[1]} for r in rows]

    @staticmethod
    def _fetch_last_close(storage: Any, exchange_id: int, symbol_id: int, interval: str) -> Optional[float]:
        q = """
        SELECT close
        FROM candles
        WHERE exchange_id=%s AND symbol_id=%s AND interval=%s
        ORDER BY open_time DESC
        LIMIT 1
        """
        with storage.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (int(exchange_id), int(symbol_id), str(interval)))
                r = cur.fetchone()
                return _to_float(r[0]) if r else None

    @staticmethod
    def _fetch_last_candles_db(
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        interval: str,
        limit: int,
        as_of_ts: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        params: List[Any] = [int(exchange_id), int(symbol_id), str(interval)]
        where_as_of = ""
        if as_of_ts is not None:
            where_as_of = " AND open_time <= %s"
            params.append(_utc(as_of_ts))
        q = f"""
        SELECT
            open_time AS ts,
            open, high, low, close,
            volume,
            quote_volume
        FROM candles
        WHERE exchange_id=%s AND symbol_id=%s AND interval=%s{where_as_of}
        ORDER BY open_time DESC
        LIMIT %s
        """
        params.append(int(limit))
        with storage.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, tuple(params))
                rows = cur.fetchall()

        rows = list(rows)[::-1]
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "ts": _utc(r[0]),
                    "open": _to_float(r[1]),
                    "high": _to_float(r[2]),
                    "low": _to_float(r[3]),
                    "close": _to_float(r[4]),
                    "volume": _to_float(r[5]),
                    "quote_volume": _to_float(r[6]),
                }
            )
        return out
    def _fetch_last_candles(
        self,
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        interval: str,
        limit: int,
        sp: Optional[ScrParams] = None,
    ) -> List[Dict[str, Any]]:
        as_of_ts = getattr(sp, "as_of_ts", None) if sp is not None else None
        prefetched = self._slice_prefetched_candles(symbol_id=int(symbol_id), interval=str(interval), limit=int(limit), as_of_ts=as_of_ts)
        if prefetched is not None:
            return prefetched
        return self._fetch_last_candles_db(storage, exchange_id, symbol_id, str(interval), limit=int(limit), as_of_ts=as_of_ts)

    def _fetch_funding_at(self, storage: Any, exchange_id: int, symbol_id: int, ts: datetime) -> Optional[float]:
        prefetched = self._prefetched_funding_at(symbol_id=int(symbol_id), ts=ts)
        if prefetched is not None:
            return prefetched
        q = """
        SELECT funding_rate
        FROM funding
        WHERE exchange_id=%s AND symbol_id=%s AND funding_time <= %s
        ORDER BY funding_time DESC
        LIMIT 1
        """
        with storage.pool.connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(q, (int(exchange_id), int(symbol_id), _utc(ts)))
                    r = cur.fetchone()
                    return _to_float(r[0]) if r else None
                except Exception:
                    return None

    def _fetch_liquidations_for_candle(
        self,
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        candle_open_ts: datetime,
        interval: str,
    ) -> Tuple[float, float]:
        prefetched = self._prefetched_liq_bucket(symbol_id=int(symbol_id), candle_open_ts=candle_open_ts)
        if prefetched is not None:
            return prefetched
        start_ts = _utc(candle_open_ts)
        end_ts = start_ts + _parse_interval(interval)

        q = """
        SELECT
            COALESCE(SUM(long_notional), 0) AS long_usdt,
            COALESCE(SUM(short_notional), 0) AS short_usdt
        FROM liquidation_1m
        WHERE exchange_id=%s
          AND symbol_id=%s
          AND bucket_ts >= %s
          AND bucket_ts < %s
        """
        with storage.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (int(exchange_id), int(symbol_id), start_ts, end_ts))
                r = cur.fetchone()
                if not r:
                    return 0.0, 0.0
                return _to_float(r[0]), _to_float(r[1])


    @staticmethod
    def _floor_ts_to_interval(ts: datetime, interval: str) -> datetime:
        dt = _utc(ts)
        delta = _parse_interval(interval)
        sec = int(max(delta.total_seconds(), 60.0))
        epoch = int(dt.timestamp())
        floored = epoch - (epoch % sec)
        return datetime.fromtimestamp(floored, tz=timezone.utc)

    def _fetch_recent_liquidation_history(
        self,
        *,
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        anchor_ts: datetime,
        interval: str,
        lookback: int,
    ) -> List[float]:
        prefetched = self._prefetched_liq_history(symbol_id=int(symbol_id), anchor_ts=anchor_ts, interval=interval, lookback=lookback)
        if prefetched is not None:
            return prefetched
        lookback = max(0, int(lookback))
        if lookback <= 1:
            return []
        delta = _parse_interval(interval)
        start_ts = _utc(anchor_ts) - delta * lookback
        end_ts = _utc(anchor_ts) + delta
        q = """
        SELECT bucket_ts, COALESCE(long_notional, 0), COALESCE(short_notional, 0)
        FROM liquidation_1m
        WHERE exchange_id=%s
          AND symbol_id=%s
          AND bucket_ts >= %s
          AND bucket_ts < %s
        ORDER BY bucket_ts ASC
        """
        buckets: Dict[datetime, float] = {}
        with storage.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (int(exchange_id), int(symbol_id), start_ts, end_ts))
                for ts, long_n, short_n in cur.fetchall():
                    k = self._floor_ts_to_interval(_utc(ts), interval)
                    buckets[k] = buckets.get(k, 0.0) + _to_float(long_n) + _to_float(short_n)
        out: List[float] = []
        cur_ts = _utc(anchor_ts) - delta * (lookback - 1)
        for _ in range(lookback):
            out.append(float(buckets.get(cur_ts, 0.0)))
            cur_ts = cur_ts + delta
        return out

    def _liquidation_relative_ok(self, *, anchor_total: float, history: Sequence[float], p: ScrParams) -> Tuple[bool, float, float]:
        k = max(0.0, float(getattr(p, "liq_relative_k", 0.0)))
        if k <= 0.0:
            return True, 0.0, 0.0
        vals = [float(v) for v in history[:-1] if float(v) > 0.0]
        avg = sum(vals) / float(len(vals)) if vals else 0.0
        ratio = (float(anchor_total) / avg) if avg > 0 else 0.0
        return ratio >= k, float(ratio), float(avg)

    def _htf_levels_ok(
        self,
        *,
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        ref_price: float,
        local_meta: Dict[str, Any],
        p: ScrParams,
    ) -> Tuple[bool, Dict[str, Any]]:
        if not bool(getattr(p, "require_htf_level_overlap", False)):
            return True, {}
        htf_interval = str(getattr(p, "htf_level_interval", "") or "").strip()
        if not htf_interval or htf_interval == p.interval:
            return True, {}
        need = max(180, int(p.period_levels // 2) + 40)
        candles = self._fetch_last_candles(storage, exchange_id, symbol_id, htf_interval, limit=need, sp=p)
        if len(candles) < 80:
            return False, {"reason": "htf_not_enough_candles"}
        htf_params = ScrParams(**{**p.__dict__, "interval": htf_interval, "require_htf_level_overlap": False})
        up, down, meta = self._cached_levels(symbol_id=symbol_id, interval=htf_interval, anchor_ts=_utc(candles[-1]["ts"]), candles=candles[:-1], ref_price=ref_price, p=htf_params)
        local_up = dict(local_meta.get("up_zone") or {"low": local_meta.get("up_level", 0.0), "high": local_meta.get("up_level", 0.0)})
        local_down = dict(local_meta.get("down_zone") or {"low": local_meta.get("down_level", 0.0), "high": local_meta.get("down_level", 0.0)})
        htf_up = dict(meta.get("up_zone") or {"low": up, "high": up})
        htf_down = dict(meta.get("down_zone") or {"low": down, "high": down})
        ok = self._zones_overlap(local_up, htf_up, p.htf_level_tol_pct) and self._zones_overlap(local_down, htf_down, p.htf_level_tol_pct)
        return ok, {"interval": htf_interval, "up_level": up, "down_level": down, "up_zone": htf_up, "down_zone": htf_down, "fallback": bool(meta.get("fallback"))}

    @staticmethod
    def _score_signal(
        *,
        side: str,
        p: ScrParams,
        liq_side_usdt: float,
        liq_limit: float,
        liq_relative_ratio: float,
        volume_spike_k: float,
        level_distance_pct: float,
        wick_metrics: Dict[str, float],
        confirm_meta: Dict[str, Any],
        level_strength: int,
        level_zone: Dict[str, Any],
        fund_c: Optional[float],
        trend_extension_pct: float,
    ) -> Tuple[float, float, Dict[str, float]]:
        blocks: Dict[str, float] = {}
        blocks["liq_abs"] = min(25.0, 25.0 * (float(liq_side_usdt) / max(float(liq_limit), 1.0)))
        blocks["liq_rel"] = min(15.0, max(0.0, (float(liq_relative_ratio) - 1.0) * 10.0))
        vsk_req = max(float(getattr(p, "volume_spike_k", 0.0)), 1.0)
        blocks["volume"] = min(15.0, 15.0 * max(0.0, float(volume_spike_k)) / max(vsk_req, 1.0))
        dist_max = max(float(getattr(p, "anchor_to_level_max_pct", 0.0)), 0.15)
        blocks["level_proximity"] = max(0.0, 15.0 * (1.0 - float(level_distance_pct) / max(dist_max, 1e-12)))
        wick_key = "lower_wick_pct" if str(side).upper().strip() == "BUY" else "upper_wick_pct"
        blocks["wick"] = min(10.0, float(wick_metrics.get(wick_key) or 0.0) / 5.0)
        blocks["confirm"] = min(10.0, float(confirm_meta.get("confirm_body_pct") or 0.0) / 4.0)
        blocks["level_quality"] = min(10.0, float(level_strength) * 2.0 + float(level_zone.get("successful_reactions") or 0.0) * 1.5)
        blocks["trend"] = max(0.0, 5.0 - max(0.0, float(trend_extension_pct) - 1.0))
        if fund_c is None:
            blocks["funding"] = 2.5
        else:
            align = abs(float(fund_c)) / max(abs(float(getattr(p, "kof_fund", 0.0))) / 100.0, 1e-9)
            blocks["funding"] = min(5.0, align * 2.5)
        score = min(100.0, sum(blocks.values()))
        confidence = max(0.5, min(0.98, 0.5 + score / 200.0))
        return float(score), float(confidence), blocks

    # =========================================================
    # current price
    # =========================================================

    @staticmethod
    def _fetch_ticker_last_price(
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        as_of_ts: Optional[datetime] = None,
    ) -> Optional[float]:
        where_as_of = ""
        params: List[Any] = [int(exchange_id), int(symbol_id)]
        if as_of_ts is not None:
            where_as_of = " AND close_time <= %s"
            params.append(_utc(as_of_ts))
        q = f"""
        SELECT last_price
        FROM public.ticker_24h
        WHERE exchange_id=%s AND symbol_id=%s{where_as_of}
        ORDER BY close_time DESC
        LIMIT 1
        """
        try:
            with storage.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(q, tuple(params))
                    r = cur.fetchone()
                    px = _to_float(r[0]) if r and r[0] is not None else None
                    return px if px and px > 0 else None
        except Exception:
            return None

    def _fetch_current_price(
        self,
        *,
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        interval: str,
        as_of_ts: Optional[datetime] = None,
    ) -> Tuple[Optional[float], str]:
        sid = int(symbol_id)
        prefetched_prices = self._ctx_get("current_price_by_symbol") or {}
        px_pref = prefetched_prices.get(sid)
        if px_pref is not None:
            px = _to_float_opt(px_pref)
            if px is not None and px > 0:
                return float(px), "runtime_context.current_price"

        if as_of_ts is None:
            px = ScrLiquidationBinance._fetch_ticker_last_price(storage, exchange_id, sid)
            if px is not None:
                return float(px), "ticker_24h.last_price"

        candles = self._fetch_last_candles(
            storage,
            exchange_id,
            sid,
            interval,
            limit=1,
            sp=ScrParams(interval=str(interval), as_of_ts=as_of_ts),
        )
        if candles:
            return float(_to_float(candles[-1].get("close"))), "candles.close"

        return None, "none"

    # =========================================================
    # main run
    # =========================================================

    def run(
        self,
        *,
        storage: Any,
        exchange_id: int,
        interval: str,
        params: Dict[str, Any],
    ) -> List[ScreenerSignal]:
        p = self._apply_runtime_mode(self._parse_params(interval=interval, params=params))
        log.info("scr_liquidation_binance params: %s", p)

        symbols = self._fetch_symbols(storage=storage, exchange_id=exchange_id)
        if not symbols:
            log.warning("No symbols in DB for exchange_id=%s", exchange_id)
            return []

        candidate_ids_raw = self._ctx_get("candidate_symbol_ids")
        if candidate_ids_raw:
            try:
                candidate_ids = {int(x) for x in candidate_ids_raw}
                symbols = [row for row in symbols if int(row.get("symbol_id") or 0) in candidate_ids]
            except Exception:
                pass
        if not symbols:
            return []

        out: List[ScreenerSignal] = []
        debug_rows: List[Dict[str, Any]] = []

        need = max(320, p.period_levels + p.volume_avg_window + p.confirm_lookforward + 90)
        equity_usdt = self._resolve_equity_usdt(storage=storage, exchange_id=exchange_id, p=p)
        liq_hist_window = max(0, int(getattr(p, "liq_relative_window", 0)))
        max_anchor_candidates = max(1, int(getattr(p, "max_anchor_candidates", 1)))

        for sym_row in symbols:
            symbol_id = int(sym_row["symbol_id"])
            symbol = str(sym_row["symbol"])

            current_price, price_src = self._fetch_current_price(
                storage=storage,
                exchange_id=exchange_id,
                symbol_id=symbol_id,
                interval=p.interval,
                as_of_ts=p.as_of_ts,
            )
            if current_price is None:
                continue
            if not (p.min_price <= current_price <= p.max_price):
                continue

            candles = self._fetch_last_candles(storage, exchange_id, symbol_id, p.interval, limit=need, sp=p)
            min_need = max(p.period_levels + p.volume_avg_window + p.confirm_lookforward + 20, 160)
            if len(candles) < min_need:
                continue

            lf = max(0, int(p.confirm_lookforward))
            latest_anchor = len(candles) - 1 - lf
            if latest_anchor <= 10 or latest_anchor >= len(candles):
                continue

            liq_history_cache: Dict[datetime, List[float]] = {}
            best_signal: Optional[ScreenerSignal] = None
            best_score = -1.0
            best_debug: Optional[Dict[str, Any]] = None

            start_anchor = max(11, latest_anchor - max_anchor_candidates + 1)
            for anchor_idx in range(latest_anchor, start_anchor - 1, -1):
                anchor = candles[anchor_idx]
                anchor_ts = _utc(anchor["ts"])
                anchor_open = _to_float(anchor["open"])
                anchor_high = _to_float(anchor["high"])
                anchor_low = _to_float(anchor["low"])
                anchor_close = _to_float(anchor["close"])
                confirm_window = candles[anchor_idx + 1: anchor_idx + 1 + lf] if lf > 0 else []

                lvl_hist = candles[max(0, anchor_idx - (p.period_levels + 25)): anchor_idx]
                up_level, down_level, lvl_meta = self._cached_levels(symbol_id=symbol_id, interval=p.interval, anchor_ts=anchor_ts, candles=lvl_hist, ref_price=anchor_close, p=p)
                lvl_meta = dict(lvl_meta or {})
                lvl_meta["up_level"] = float(up_level)
                lvl_meta["down_level"] = float(down_level)
                if up_level <= 0 or down_level <= 0 or up_level <= down_level:
                    continue
                if bool(lvl_meta.get("fallback")) and not bool(getattr(p, "allow_fallback_levels", False)):
                    continue

                up_strength = int(lvl_meta.get("up_strength") or 0)
                down_strength = int(lvl_meta.get("down_strength") or 0)
                min_cluster_strength = max(0, int(getattr(p, "min_level_cluster_strength", 0)))
                if min_cluster_strength > 0 and (up_strength < min_cluster_strength or down_strength < min_cluster_strength):
                    continue

                htf_ok, htf_meta = self._htf_levels_ok(
                    storage=storage,
                    exchange_id=exchange_id,
                    symbol_id=symbol_id,
                    ref_price=anchor_close,
                    local_meta=lvl_meta,
                    p=p,
                )
                if not htf_ok:
                    continue
                if htf_meta:
                    lvl_meta["htf"] = htf_meta

                vol_anchor = _to_float(anchor.get("quote_volume") or anchor.get("volume"))
                avg_vol = self._avg_volume_before(candles=candles, idx=anchor_idx, window=p.volume_avg_window)
                vol_ratio = (vol_anchor / avg_vol) if avg_vol > 0 else 0.0
                touch_lb = max(1, int(p.touch_lookback_candles))
                recent_volume_window = candles[max(0, anchor_idx - touch_lb + 1): anchor_idx + 1]
                recent_volumes = [_to_float(c.get("quote_volume") or c.get("volume")) for c in recent_volume_window]
                recent_volume_peak = max(recent_volumes) if recent_volumes else vol_anchor
                recent_volume_spike_k = (recent_volume_peak / avg_vol) if avg_vol > 0 else 0.0
                vol_change_pct = ((vol_ratio - 1.0) * 100.0) if vol_ratio > 0 else 0.0
                recent_volume_spike_pct = ((recent_volume_spike_k - 1.0) * 100.0) if recent_volume_spike_k > 0 else 0.0
                vol_ok = True if float(p.volume_spike_k) <= 0 else (recent_volume_spike_k >= float(p.volume_spike_k))

                liq_long_usdt, liq_short_usdt = self._fetch_liquidations_for_candle(
                    storage=storage,
                    exchange_id=exchange_id,
                    symbol_id=symbol_id,
                    candle_open_ts=anchor_ts,
                    interval=p.interval,
                )
                if liq_hist_window > 1:
                    liq_hist = liq_history_cache.get(anchor_ts)
                    if liq_hist is None:
                        liq_hist = self._fetch_recent_liquidation_history(
                            storage=storage,
                            exchange_id=exchange_id,
                            symbol_id=symbol_id,
                            anchor_ts=anchor_ts,
                            interval=p.interval,
                            lookback=liq_hist_window,
                        )
                        liq_history_cache[anchor_ts] = liq_hist
                else:
                    liq_hist = []
                liq_total = float(liq_long_usdt + liq_short_usdt)
                liq_rel_ok, liq_rel_ratio, liq_rel_avg = self._liquidation_relative_ok(anchor_total=liq_total, history=liq_hist, p=p)

                dom_sell_ok = self._liq_dominance_ok(side="SELL", liq_long_usdt=liq_long_usdt, liq_short_usdt=liq_short_usdt, p=p)
                dom_buy_ok = self._liq_dominance_ok(side="BUY", liq_long_usdt=liq_long_usdt, liq_short_usdt=liq_short_usdt, p=p)
                touch_up_evt = self._find_recent_touch_event(candles=candles, idx_anchor=anchor_idx, level=up_level, p=p)
                touch_dn_evt = self._find_recent_touch_event(candles=candles, idx_anchor=anchor_idx, level=down_level, p=p)
                touch_up = touch_up_evt is not None
                touch_dn = touch_dn_evt is not None
                sell_liq_ok = liq_short_usdt >= p.volume_liquid_limit
                buy_liq_ok = liq_long_usdt >= p.volume_liquid_limit

                wick_sell_ok, wick_sell_metrics, wick_sell_reason = self._anchor_wick_ok(side="SELL", candle=anchor, p=p)
                wick_buy_ok, wick_buy_metrics, wick_buy_reason = self._anchor_wick_ok(side="BUY", candle=anchor, p=p)
                sell_follow_ok, sell_follow_ext = self._followthrough_ok(side="SELL", candles=candles, anchor_idx=anchor_idx, p=p)
                buy_follow_ok, buy_follow_ext = self._followthrough_ok(side="BUY", candles=candles, anchor_idx=anchor_idx, p=p)
                trend_ok, trend_ext_pct, trend_ema = self._trend_extension_ok(side="BUY", candles=candles, anchor_idx=anchor_idx, p=p)
                sell_level_dist_pct = self._anchor_to_level_distance_pct(side="SELL", anchor_candle=anchor, level=up_level)
                buy_level_dist_pct = self._anchor_to_level_distance_pct(side="BUY", anchor_candle=anchor, level=down_level)
                max_anchor_level_pct = max(0.0, float(getattr(p, "anchor_to_level_max_pct", 0.0)))
                sell_anchor_dist_ok = True if max_anchor_level_pct <= 0 else sell_level_dist_pct <= max_anchor_level_pct
                buy_anchor_dist_ok = True if max_anchor_level_pct <= 0 else buy_level_dist_pct <= max_anchor_level_pct

                sell_ready = bool(touch_up and sell_liq_ok and vol_ok and dom_sell_ok and wick_sell_ok and sell_follow_ok and sell_anchor_dist_ok and liq_rel_ok and trend_ok)
                buy_ready = bool(touch_dn and buy_liq_ok and vol_ok and dom_buy_ok and wick_buy_ok and buy_follow_ok and buy_anchor_dist_ok and liq_rel_ok and trend_ok)

                sell_hit_data = None
                buy_hit_data = None
                if sell_ready:
                    sell_hit_data = self._find_confirmation_short(
                        storage=storage,
                        exchange_id=exchange_id,
                        symbol_id=symbol_id,
                        symbol=symbol,
                        interval=p.interval,
                        p=p,
                        confirm_window=confirm_window,
                        anchor_candle=anchor,
                    )
                if buy_ready:
                    buy_hit_data = self._find_confirmation_long(
                        storage=storage,
                        exchange_id=exchange_id,
                        symbol_id=symbol_id,
                        symbol=symbol,
                        interval=p.interval,
                        p=p,
                        confirm_window=confirm_window,
                        anchor_candle=anchor,
                    )

                score_sell = None
                conf_sell = None
                sell_blocks = None
                if sell_hit_data is not None:
                    _, _, fund_sell, _, sell_meta = sell_hit_data
                    score_sell, conf_sell, sell_blocks = self._score_signal(
                        side="SELL",
                        p=p,
                        liq_side_usdt=liq_short_usdt,
                        liq_limit=p.volume_liquid_limit,
                        liq_relative_ratio=liq_rel_ratio,
                        volume_spike_k=recent_volume_spike_k,
                        level_distance_pct=sell_level_dist_pct,
                        wick_metrics=wick_sell_metrics,
                        confirm_meta=sell_meta,
                        level_strength=up_strength,
                        level_zone=dict(lvl_meta.get("up_zone") or {}),
                        fund_c=fund_sell,
                        trend_extension_pct=trend_ext_pct,
                    )
                score_buy = None
                conf_buy = None
                buy_blocks = None
                if buy_hit_data is not None:
                    _, _, fund_buy, _, buy_meta = buy_hit_data
                    score_buy, conf_buy, buy_blocks = self._score_signal(
                        side="BUY",
                        p=p,
                        liq_side_usdt=liq_long_usdt,
                        liq_limit=p.volume_liquid_limit,
                        liq_relative_ratio=liq_rel_ratio,
                        volume_spike_k=recent_volume_spike_k,
                        level_distance_pct=buy_level_dist_pct,
                        wick_metrics=wick_buy_metrics,
                        confirm_meta=buy_meta,
                        level_strength=down_strength,
                        level_zone=dict(lvl_meta.get("down_zone") or {}),
                        fund_c=fund_buy,
                        trend_extension_pct=trend_ext_pct,
                    )

                chosen: Optional[str] = None
                if sell_hit_data is not None and buy_hit_data is None:
                    chosen = "SELL"
                elif buy_hit_data is not None and sell_hit_data is None:
                    chosen = "BUY"
                elif sell_hit_data is not None and buy_hit_data is not None:
                    chosen = "SELL" if float(score_sell or 0.0) >= float(score_buy or 0.0) else "BUY"

                def why_not_buy() -> List[str]:
                    r: List[str] = []
                    if not touch_dn:
                        r.append(f"нет TOUCH/CROSS DOWN за last {p.touch_lookback_candles}")
                    if not buy_liq_ok:
                        r.append(f"liq_long<{p.volume_liquid_limit:.0f}")
                    if not liq_rel_ok:
                        r.append(f"liq_relative_k<{p.liq_relative_k}")
                    if not vol_ok:
                        r.append(f"recent_volume_spike_k<{p.volume_spike_k}")
                    if not dom_buy_ok:
                        r.append("dominance BUY=NO")
                    if not wick_buy_ok:
                        r.append(f"anchor_wick BUY=NO ({wick_buy_reason})")
                    if not buy_follow_ok:
                        r.append(f"followthrough BUY=NO ({buy_follow_ext:.3f}%>{p.followthrough_max_pct})")
                    if not buy_anchor_dist_ok:
                        r.append(f"anchor_to_level>{max_anchor_level_pct}%")
                    if not trend_ok:
                        r.append(f"trend_extension>{p.trend_extension_max_pct}%")
                    if buy_ready and buy_hit_data is None:
                        r.append("confirm BUY=NO")
                    return r

                def why_not_sell() -> List[str]:
                    r: List[str] = []
                    if not touch_up:
                        r.append(f"нет TOUCH/CROSS UP за last {p.touch_lookback_candles}")
                    if not sell_liq_ok:
                        r.append(f"liq_short<{p.volume_liquid_limit:.0f}")
                    if not liq_rel_ok:
                        r.append(f"liq_relative_k<{p.liq_relative_k}")
                    if not vol_ok:
                        r.append(f"recent_volume_spike_k<{p.volume_spike_k}")
                    if not dom_sell_ok:
                        r.append("dominance SELL=NO")
                    if not wick_sell_ok:
                        r.append(f"anchor_wick SELL=NO ({wick_sell_reason})")
                    if not sell_follow_ok:
                        r.append(f"followthrough SELL=NO ({sell_follow_ext:.3f}%>{p.followthrough_max_pct})")
                    if not sell_anchor_dist_ok:
                        r.append(f"anchor_to_level>{max_anchor_level_pct}%")
                    if not trend_ok:
                        r.append(f"trend_extension>{p.trend_extension_max_pct}%")
                    if sell_ready and sell_hit_data is None:
                        r.append("confirm SELL=NO")
                    return r

                debug_item = {
                    "symbol": symbol,
                    "px": float(current_price),
                    "up": float(up_level),
                    "down": float(down_level),
                    "liqS": float(liq_short_usdt),
                    "liqL": float(liq_long_usdt),
                    "volR": float(vol_ratio),
                    "volCh": float(vol_change_pct),
                    "recentVolK": float(recent_volume_spike_k),
                    "recentVolCh": float(recent_volume_spike_pct),
                    "touch_up": bool(touch_up),
                    "touch_dn": bool(touch_dn),
                    "sell_ready": bool(sell_ready),
                    "buy_ready": bool(buy_ready),
                    "sell_confirm": bool(sell_hit_data is not None),
                    "buy_confirm": bool(buy_hit_data is not None),
                    "why_not_sell": why_not_sell(),
                    "why_not_buy": why_not_buy(),
                }

                if chosen is None:
                    if p.debug:
                        debug_rows.append(debug_item)
                    continue

                if chosen == "SELL":
                    confirm_ts, confirm_close, fund_c, why, confirm_meta = sell_hit_data  # type: ignore[misc]
                    entry_price, entry_src = self._resolve_entry_price(
                        mode=p.entry_price_mode,
                        current_price=current_price,
                        current_src=price_src,
                        anchor_close=anchor_close,
                        confirm_close=confirm_close,
                    )
                    sl, tp = self._compute_sl_tp(side="SELL", entry_price=entry_price, p=p)
                    qty, notional, risk_usdt = self._compute_position_size(entry_price=entry_price, stop_loss=sl, equity_usdt=equity_usdt, p=p)
                    t_idx, t_ts, t_kind = touch_up_evt  # type: ignore[misc]
                    touch_age = int(anchor_idx - int(t_idx))
                    wnb = why_not_buy()
                    if buy_hit_data is not None:
                        wnb.insert(0, f"BUY тоже подтвердился, но выбран SELL по score: sell={float(score_sell or 0.0):.2f} >= buy={float(score_buy or 0.0):.2f}")
                    sig = self._make_signal(
                        symbol_id=symbol_id,
                        symbol=symbol,
                        timeframe=p.interval,
                        signal_ts=_utc(confirm_ts),
                        side="SELL",
                        entry_price=entry_price,
                        sl=sl,
                        tp=tp,
                        confidence=float(conf_sell or 0.83),
                        score=float(score_sell or 0.0),
                        reason=f"{t_kind} UP + SHORT LIQ DOM -> CONFIRM DOWN | {why}",
                        context={
                            "touch_side": "UP",
                            "touch_ts": _utc(t_ts),
                            "touch_level": float(up_level),
                            "touch_age_candles": touch_age,
                            "touch_kind": str(t_kind),
                            "why_not_buy": wnb,
                            "why_not_sell": [],
                            "anchor_candidate_rank": int(latest_anchor - anchor_idx + 1),
                            "anchor_ts": anchor_ts,
                            "confirm_ts": _utc(confirm_ts),
                            "entry_price_source": entry_src,
                            "current_price": float(current_price),
                            "anchor_close": float(anchor_close),
                            "confirm_close": float(confirm_close),
                            "up_level": float(up_level),
                            "down_level": float(down_level),
                            "up_zone": lvl_meta.get("up_zone"),
                            "down_zone": lvl_meta.get("down_zone"),
                            "liq_short_usdt": float(liq_short_usdt),
                            "liq_long_usdt": float(liq_long_usdt),
                            "liq_dominance_pct": float(p.liq_dominance_pct),
                            "liq_relative_ratio": float(liq_rel_ratio),
                            "liq_relative_avg": float(liq_rel_avg),
                            "vol_ratio": float(vol_ratio),
                            "avg_vol": float(avg_vol),
                            "volume": float(vol_anchor),
                            "volume_avg_window": int(getattr(p, "volume_avg_window", 0)),
                            "volume_spike_k": float(getattr(p, "volume_spike_k", 0.0)),
                            "vol_change_pct": float(vol_change_pct),
                            "recent_volume_peak": float(recent_volume_peak),
                            "recent_volume_spike_k": float(recent_volume_spike_k),
                            "recent_volume_spike_pct": float(recent_volume_spike_pct),
                            "anchor_open": float(anchor_open),
                            "anchor_high": float(anchor_high),
                            "anchor_low": float(anchor_low),
                            "anchor_wick_metrics": wick_sell_metrics,
                            "anchor_to_level_distance_pct": float(sell_level_dist_pct),
                            "followthrough_extension_pct": float(sell_follow_ext),
                            "confirm_meta": confirm_meta,
                            "funding_confirm": _to_float(fund_c) if fund_c is not None else None,
                            "levels_meta": lvl_meta,
                            "score_blocks": sell_blocks,
                            "trend_ema": float(trend_ema),
                            "trend_extension_pct": float(trend_ext_pct),
                            "equity_usdt": _to_float(equity_usdt) if equity_usdt is not None else None,
                            "risk_trade_pct": float(p.risk_trade_pct),
                            "risk_trade_usdt": _to_float(risk_usdt) if risk_usdt is not None else None,
                            "stop_loss_pct": float(p.stop_loss_pct),
                            "take_profit_pct": float(p.take_profit_pct),
                            "position_qty": _to_float(qty) if qty is not None else None,
                            "position_notional_usdt": _to_float(notional) if notional is not None else None,
                        },
                    )
                else:
                    confirm_ts, confirm_close, fund_c, why, confirm_meta = buy_hit_data  # type: ignore[misc]
                    entry_price, entry_src = self._resolve_entry_price(
                        mode=p.entry_price_mode,
                        current_price=current_price,
                        current_src=price_src,
                        anchor_close=anchor_close,
                        confirm_close=confirm_close,
                    )
                    sl, tp = self._compute_sl_tp(side="BUY", entry_price=entry_price, p=p)
                    qty, notional, risk_usdt = self._compute_position_size(entry_price=entry_price, stop_loss=sl, equity_usdt=equity_usdt, p=p)
                    t_idx, t_ts, t_kind = touch_dn_evt  # type: ignore[misc]
                    touch_age = int(anchor_idx - int(t_idx))
                    wns = why_not_sell()
                    if sell_hit_data is not None:
                        wns.insert(0, f"SELL тоже подтвердился, но выбран BUY по score: buy={float(score_buy or 0.0):.2f} > sell={float(score_sell or 0.0):.2f}")
                    sig = self._make_signal(
                        symbol_id=symbol_id,
                        symbol=symbol,
                        timeframe=p.interval,
                        signal_ts=_utc(confirm_ts),
                        side="BUY",
                        entry_price=entry_price,
                        sl=sl,
                        tp=tp,
                        confidence=float(conf_buy or 0.83),
                        score=float(score_buy or 0.0),
                        reason=f"{t_kind} DOWN + LONG LIQ DOM -> CONFIRM UP | {why}",
                        context={
                            "touch_side": "DOWN",
                            "touch_ts": _utc(t_ts),
                            "touch_level": float(down_level),
                            "touch_age_candles": touch_age,
                            "touch_kind": str(t_kind),
                            "why_not_buy": [],
                            "why_not_sell": wns,
                            "anchor_candidate_rank": int(latest_anchor - anchor_idx + 1),
                            "anchor_ts": anchor_ts,
                            "confirm_ts": _utc(confirm_ts),
                            "entry_price_source": entry_src,
                            "current_price": float(current_price),
                            "anchor_close": float(anchor_close),
                            "confirm_close": float(confirm_close),
                            "up_level": float(up_level),
                            "down_level": float(down_level),
                            "up_zone": lvl_meta.get("up_zone"),
                            "down_zone": lvl_meta.get("down_zone"),
                            "liq_short_usdt": float(liq_short_usdt),
                            "liq_long_usdt": float(liq_long_usdt),
                            "liq_dominance_pct": float(p.liq_dominance_pct),
                            "liq_relative_ratio": float(liq_rel_ratio),
                            "liq_relative_avg": float(liq_rel_avg),
                            "vol_ratio": float(vol_ratio),
                            "avg_vol": float(avg_vol),
                            "volume": float(vol_anchor),
                            "volume_avg_window": int(getattr(p, "volume_avg_window", 0)),
                            "volume_spike_k": float(getattr(p, "volume_spike_k", 0.0)),
                            "vol_change_pct": float(vol_change_pct),
                            "recent_volume_peak": float(recent_volume_peak),
                            "recent_volume_spike_k": float(recent_volume_spike_k),
                            "recent_volume_spike_pct": float(recent_volume_spike_pct),
                            "anchor_open": float(anchor_open),
                            "anchor_high": float(anchor_high),
                            "anchor_low": float(anchor_low),
                            "anchor_wick_metrics": wick_buy_metrics,
                            "anchor_to_level_distance_pct": float(buy_level_dist_pct),
                            "followthrough_extension_pct": float(buy_follow_ext),
                            "confirm_meta": confirm_meta,
                            "funding_confirm": _to_float(fund_c) if fund_c is not None else None,
                            "levels_meta": lvl_meta,
                            "score_blocks": buy_blocks,
                            "trend_ema": float(trend_ema),
                            "trend_extension_pct": float(trend_ext_pct),
                            "equity_usdt": _to_float(equity_usdt) if equity_usdt is not None else None,
                            "risk_trade_pct": float(p.risk_trade_pct),
                            "risk_trade_usdt": _to_float(risk_usdt) if risk_usdt is not None else None,
                            "stop_loss_pct": float(p.stop_loss_pct),
                            "take_profit_pct": float(p.take_profit_pct),
                            "position_qty": _to_float(qty) if qty is not None else None,
                            "position_notional_usdt": _to_float(notional) if notional is not None else None,
                        },
                    )

                if float(sig.score or 0.0) > best_score:
                    best_score = float(sig.score or 0.0)
                    best_signal = sig
                    best_debug = debug_item

            if p.debug and best_debug is not None:
                debug_rows.append(best_debug)
            if best_signal is not None:
                out.append(best_signal)

        if p.debug and debug_rows:
            debug_rows.sort(key=lambda r: (r["liqS"] + r["liqL"]), reverse=True)
            log.info("=== DEBUG TOP %d interval=%s touch_mode=%s ===", int(p.debug_top), p.interval, p.touch_mode)
            for r in debug_rows[: max(1, int(p.debug_top))]:
                log.info(
                    "%s px=%.6f up=%.6f down=%.6f liqS=%.0f liqL=%.0f volCh=%.1f%% touchUP=%s touchDN=%s readyS=%s confS=%s readyB=%s confB=%s",
                    r["symbol"], r["px"], r["up"], r["down"], r["liqS"], r["liqL"], r["volCh"],
                    r["touch_up"], r["touch_dn"], r["sell_ready"], r["sell_confirm"], r["buy_ready"], r["buy_confirm"],
                )
                if not r["sell_confirm"]:
                    log.info("  why_not_sell: %s", " | ".join(r["why_not_sell"]) if r["why_not_sell"] else "—")
                if not r["buy_confirm"]:
                    log.info("  why_not_buy : %s", " | ".join(r["why_not_buy"]) if r["why_not_buy"] else "—")

        return out
