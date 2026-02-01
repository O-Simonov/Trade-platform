# src/platform/screeners/scr_liquidation_binance.py
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple
from src.platform.core.utils.candles import aggregate_candles

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
    # -------------------------
    # LIVE candles (агрегация 15m -> 1h/4h/1d)
    # -------------------------
    live_candles_enabled: bool = True
    live_prefer_agg: bool = True
    live_base_interval: str = "15m"
    live_extra_base_bars: int = 64


    # -------------------------
    # ликвидации + объём
    # -------------------------
    volume_liquid_limit: float = 5_000.0
    windows: int = 20
    kof_Volume: float = 0.0  # <=0 => отключить vol_ratio фильтр
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

    # -------------------------
    # ✅ TOUCH/CROSS lookback
    # -------------------------
    touch_lookback_candles: int = 3
    touch_mode: str = "touch_or_cross"  # touch/cross/touch_or_cross

    # -------------------------
    # подтверждение
    # -------------------------
    confirm_lookforward: int = 3

    # -------------------------
    # опциональные подтверждения
    # -------------------------
    kof_fund: float = 0.5
    enable_funding: bool = False
    enable_oi: bool = False
    enable_cvd: bool = False

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
    """
    BUY только если:
      - TOUCH/CROSS DOWN уровня за последние N свечей (включая anchor)
      - long_liq >= volume_liquid_limit
      - long_liq доминирует над short_liq (liq_dominance_pct)
      - CONFIRM UP: зелёная свеча (close > open) в confirm_window (+ optional OI/CVD/FUND)

    SELL только если:
      - TOUCH/CROSS UP уровня за последние N свечей (включая anchor)
      - short_liq >= volume_liquid_limit
      - short_liq доминирует над long_liq
      - CONFIRM DOWN: красная свеча (close < open) в confirm_window (+ optional OI/CVD/FUND)

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

        p.min_price = _to_float(params.get("min_price", p.min_price), p.min_price)
        p.max_price = _to_float(params.get("max_price", p.max_price), p.max_price)

        # LIVE candles
        p.live_candles_enabled = _to_bool(params.get("live_candles_enabled", p.live_candles_enabled), p.live_candles_enabled)
        p.live_prefer_agg = _to_bool(params.get("live_prefer_agg", p.live_prefer_agg), p.live_prefer_agg)
        p.live_base_interval = str(params.get("live_base_interval", p.live_base_interval))
        p.live_extra_base_bars = _to_int(params.get("live_extra_base_bars", p.live_extra_base_bars), p.live_extra_base_bars)

        p.volume_liquid_limit = _to_float(params.get("volume_liquid_limit", p.volume_liquid_limit), p.volume_liquid_limit)
        p.windows = _to_int(params.get("windows", p.windows), p.windows)
        p.kof_Volume = _to_float(params.get("kof_Volume", p.kof_Volume), p.kof_Volume)
        p.liq_dominance_pct = _to_float(params.get("liq_dominance_pct", p.liq_dominance_pct), p.liq_dominance_pct)

        p.period_levels = _to_int(params.get("period_levels", p.period_levels), p.period_levels)
        p.level_tol_pct = _to_float(params.get("level_tol_pct", p.level_tol_pct), p.level_tol_pct)

        p.pivot_left = _to_int(params.get("pivot_left", p.pivot_left), p.pivot_left)
        p.pivot_right = _to_int(params.get("pivot_right", p.pivot_right), p.pivot_right)
        p.level_cluster_tol_pct = _to_float(params.get("level_cluster_tol_pct", p.level_cluster_tol_pct), p.level_cluster_tol_pct)
        p.max_level_candidates = _to_int(params.get("max_level_candidates", p.max_level_candidates), p.max_level_candidates)

        tlb = params.get("touch_lookback_candles", None)
        if tlb is None:
            tlb = params.get("cross_lookback_candles", p.touch_lookback_candles)
        p.touch_lookback_candles = _to_int(tlb, p.touch_lookback_candles)

        p.touch_mode = str(params.get("touch_mode", p.touch_mode))

        p.confirm_lookforward = _to_int(params.get("confirm_lookforward", p.confirm_lookforward), p.confirm_lookforward)

        p.kof_fund = _to_float(params.get("kof_fund", p.kof_fund), p.kof_fund)
        p.enable_funding = _to_bool(params.get("enable_funding", p.enable_funding), p.enable_funding)
        p.enable_oi = _to_bool(params.get("enable_oi", p.enable_oi), p.enable_oi)
        p.enable_cvd = _to_bool(params.get("enable_cvd", p.enable_cvd), p.enable_cvd)

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

    def _find_confirmation_short(
        self,
        *,
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        symbol: str,
        interval: str,
        p: ScrParams,
        oi_anchor: Any,
        cvd_anchor: Any,
        confirm_window: Sequence[Dict[str, Any]],
        oi_map: Optional[Dict[datetime, Optional[float]]] = None,
        cvd_map: Optional[Dict[datetime, Optional[float]]] = None,
    ) -> Optional[Tuple[datetime, float, Any, Any, Any, str]]:
        if not confirm_window:
            return None

        thr = self._fund_threshold(p)

        for c in confirm_window:
            ts = _utc(c["ts"])
            ts_key = _ts_key(ts)
            o = _to_float(c.get("open"))
            close = _to_float(c.get("close"))

            if close >= o:  # need RED
                continue

            oi_c = cvd_c = fund_c = None

            if p.enable_oi:
                if oi_map is not None:
                    oi_c = oi_map.get(ts_key)
                else:
                    oi_c = self._fetch_oi_at(storage, exchange_id, symbol_id, interval, ts)
                if oi_anchor is None or oi_c is None:
                    continue
                if _to_float(oi_c) >= _to_float(oi_anchor):
                    continue

            if p.enable_cvd:
                cvd_c = c.get("cvd_quote")
                if cvd_c is None:
                    if cvd_map is not None:
                        cvd_c = cvd_map.get(ts_key)
                    else:
                        cvd_c = self._fetch_cvd_at(storage, exchange_id, symbol_id, interval, ts)
                if cvd_anchor is None or cvd_c is None:
                    continue
                if _to_float(cvd_c) >= _to_float(cvd_anchor):
                    continue

            if p.enable_funding:
                fund_c = self._fetch_funding_at(storage, exchange_id, symbol_id, ts)
                if fund_c is None:
                    continue
                if _to_float(fund_c) < thr:
                    continue

            why = "CONFIRM DOWN: red candle in confirm window (close < open)"
            return ts, close, oi_c, cvd_c, fund_c, why

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
        oi_anchor: Any,
        cvd_anchor: Any,
        confirm_window: Sequence[Dict[str, Any]],
        oi_map: Optional[Dict[datetime, Optional[float]]] = None,
        cvd_map: Optional[Dict[datetime, Optional[float]]] = None,
    ) -> Optional[Tuple[datetime, float, Any, Any, Any, str]]:
        if not confirm_window:
            return None

        thr = self._fund_threshold(p)

        for c in confirm_window:
            ts = _utc(c["ts"])
            ts_key = _ts_key(ts)
            o = _to_float(c.get("open"))
            close = _to_float(c.get("close"))

            if close <= o:  # need GREEN
                continue

            oi_c = cvd_c = fund_c = None

            if p.enable_oi:
                if oi_map is not None:
                    oi_c = oi_map.get(ts_key)
                else:
                    oi_c = self._fetch_oi_at(storage, exchange_id, symbol_id, interval, ts)
                if oi_anchor is None or oi_c is None:
                    continue
                if _to_float(oi_c) >= _to_float(oi_anchor):
                    continue

            if p.enable_cvd:
                cvd_c = c.get("cvd_quote")
                if cvd_c is None:
                    if cvd_map is not None:
                        cvd_c = cvd_map.get(ts_key)
                    else:
                        cvd_c = self._fetch_cvd_at(storage, exchange_id, symbol_id, interval, ts)
                if cvd_anchor is None or cvd_c is None:
                    continue
                if _to_float(cvd_c) <= _to_float(cvd_anchor):
                    continue

            if p.enable_funding:
                fund_c = self._fetch_funding_at(storage, exchange_id, symbol_id, ts)
                if fund_c is None:
                    continue
                if _to_float(fund_c) > -thr:
                    continue

            why = "CONFIRM UP: green candle in confirm window (close > open)"
            return ts, close, oi_c, cvd_c, fund_c, why

        return None

    # =========================================================
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

        if len(piv_h) < 2 or len(piv_l) < 2:
            meta["fallback"] = True
            return float(max_high), float(min_low), meta

        cl_up = self._cluster_levels(piv_h, tol_pct=p.level_cluster_tol_pct, max_candidates=p.max_level_candidates)
        cl_dn = self._cluster_levels(piv_l, tol_pct=p.level_cluster_tol_pct, max_candidates=p.max_level_candidates)

        meta["clusters_up"] = cl_up
        meta["clusters_down"] = cl_dn

        up = self._pick_level_above(ref_price, cl_up)
        down = self._pick_level_below(ref_price, cl_dn)

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

        meta["up_strength"] = self._cluster_strength(up, cl_up)
        meta["down_strength"] = self._cluster_strength(down, cl_dn)

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
    def _cluster_levels(
        values: Sequence[float],
        *,
        tol_pct: float,
        max_candidates: int,
    ) -> List[Tuple[float, int]]:
        vals = [float(v) for v in values if v and v > 0]
        if not vals:
            return []

        vals.sort()
        tol_pct = abs(float(tol_pct))

        clusters: List[List[float]] = []
        cur: List[float] = [vals[0]]

        for v in vals[1:]:
            base = cur[-1]
            tol = base * tol_pct
            if abs(v - base) <= tol:
                cur.append(v)
            else:
                clusters.append(cur)
                cur = [v]

        clusters.append(cur)

        out: List[Tuple[float, int]] = []
        for c in clusters:
            price = sum(c) / float(len(c))
            out.append((float(price), int(len(c))))

        out.sort(key=lambda x: x[1], reverse=True)

        if max_candidates and len(out) > int(max_candidates):
            out = out[: int(max_candidates)]

        out.sort(key=lambda x: x[0])
        return out

    @staticmethod
    def _pick_level_above(ref_price: float, clusters: Sequence[Tuple[float, int]]) -> float:
        ref = float(ref_price)
        above = [(price, cnt) for (price, cnt) in clusters if price >= ref]
        if not above:
            return 0.0
        above.sort(key=lambda x: (x[0] - ref, -x[1]))
        return float(above[0][0])

    @staticmethod
    def _pick_level_below(ref_price: float, clusters: Sequence[Tuple[float, int]]) -> float:
        ref = float(ref_price)
        below = [(price, cnt) for (price, cnt) in clusters if price <= ref]
        if not below:
            return 0.0
        below.sort(key=lambda x: (ref - x[0], -x[1]))
        return float(below[0][0])

    @staticmethod
    def _cluster_strength(level: float, clusters: Sequence[Tuple[float, int]]) -> int:
        for (price, cnt) in clusters:
            if abs(price - level) <= max(1e-12, level * 1e-6):
                return int(cnt)
        return 0

    # =========================================================
    # volume helper
    # =========================================================

    @staticmethod
    def _avg_volume_before(*, candles: Sequence[Dict[str, Any]], idx: int, windows: int) -> float:
        w = int(windows)
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
    # DB fetchers
    # =========================================================

    @staticmethod
    def _fetch_symbols(*, storage: Any, exchange_id: int) -> List[Dict[str, Any]]:
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
        limit: int
    ) -> List[Dict[str, Any]]:
        q = """
        SELECT
            open_time AS ts,
            open, high, low, close,
            volume,
            quote_volume,
            cvd_quote
        FROM candles
        WHERE exchange_id=%s AND symbol_id=%s AND interval=%s
        ORDER BY open_time DESC
        LIMIT %s
        """
        with storage.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (int(exchange_id), int(symbol_id), str(interval), int(limit)))
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
                    "cvd_quote": _to_float_opt(r[7]),
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
        """
        1) Пытаемся взять свечи нужного interval напрямую из БД.
        2) Если включен LIVE-режим и interval != base, то берём base (обычно 15m) и агрегируем
           в interval (1h/4h/1d), чтобы получать текущую формирующуюся свечу без лага.
        """
        interval = str(interval)

        if sp and sp.live_candles_enabled and sp.live_prefer_agg:
            base = str(sp.live_base_interval or "15m")
            if base and base != interval:
                base_td = _parse_interval(base)
                tgt_td = _parse_interval(interval)
                base_sec = int(base_td.total_seconds()) if base_td.total_seconds() > 0 else 0
                tgt_sec = int(tgt_td.total_seconds()) if tgt_td.total_seconds() > 0 else 0

                # поддерживаем только кратные интервалы: 15m -> 1h/4h/1d
                if base_sec > 0 and tgt_sec > 0 and (tgt_sec % base_sec == 0):
                    ratio = max(1, int(tgt_sec // base_sec))
                    base_limit = int(limit) * ratio + max(0, int(sp.live_extra_base_bars))
                    base_rows = self._fetch_last_candles_db(storage, exchange_id, symbol_id, base, limit=base_limit)
                    if base_rows:
                        agg = aggregate_candles(base_rows, target_interval=interval)
                        try:
                            agg = self._inject_cvd_for_agg(base_rows, agg, target_interval=interval)
                        except Exception:
                            pass
                        if len(agg) >= int(limit):
                            return list(agg)[-int(limit):]
                        if agg:
                            return agg

        # fallback: обычные свечи нужного интервала
        return self._fetch_last_candles_db(storage, exchange_id, symbol_id, interval, limit=int(limit))

    @staticmethod
    def _inject_cvd_for_agg(
        base_rows: Sequence[Dict[str, Any]],
        agg_rows: Sequence[Dict[str, Any]],
        target_interval: str,
    ) -> List[Dict[str, Any]]:
        """Заполняет cvd_quote для агрегированных свечей.

        aggregate_candles() обычно агрегирует OHLCV, но cvd_quote (кумулятивный индикатор) нужно "подтянуть" отдельно.
        Правило: CVD на закрытии свечи = последнее НЕ-NULL значение cvd_quote с base-интервала, чьё ts <= close_time.

        base_rows должны быть отсортированы по ts по возрастанию (как у _fetch_last_candles_db()).
        """
        target_td = _parse_interval(str(target_interval))
        if target_td.total_seconds() <= 0:
            target_td = timedelta(hours=1)

        # Берём только валидные (ts, cvd) и только не-NULL cvd
        points: List[Tuple[datetime, float]] = []
        for r in base_rows:
            try:
                ts = _utc(r.get("ts"))
                cvd = r.get("cvd_quote")
                if cvd is None:
                    continue
                cvd_f = float(cvd)
                points.append((ts, cvd_f))
            except Exception:
                continue

        if not points:
            return list(agg_rows)

        points.sort(key=lambda x: x[0])
        times = [t for t, _ in points]
        values = [v for _, v in points]

        from bisect import bisect_right

        out: List[Dict[str, Any]] = []
        for a in agg_rows:
            try:
                open_ts = _utc(a.get("ts"))
                close_ts = open_ts + target_td
                close_eps = close_ts - timedelta(microseconds=1)

                j = bisect_right(times, close_eps) - 1
                if j >= 0:
                    a = dict(a)
                    a["cvd_quote"] = float(values[j])
                out.append(a)
            except Exception:
                out.append(a)

        return out

    # =========================================================
    # batched prefetch (performance)
    # =========================================================

    @staticmethod
    def _prefetch_oi_close_map(
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        interval: str,
        candle_open_times: Sequence[datetime],
    ) -> Dict[datetime, Optional[float]]:
        """Batch OI lookup for a set of candle open times.

        Returns mapping: candle_open_ts (UTC, no microseconds) -> oi_value (float) on candle CLOSE.

        Strategy:
        - For each candle open_ts compute close_ts = open_ts + interval
        - Fetch OI points for two candidate intervals:
            * interval (same as candle TF)
            * '5m' (freshest, near-real-time)
        - For each candle choose the value coming from the freshest point (largest oi_ts) among candidates.

        This reduces DB round-trips (1 per candle) to a couple of queries per symbol.
        """
        if not candle_open_times:
            return {}

        interval = str(interval).strip()
        td = _parse_interval(interval)
        if td.total_seconds() <= 0:
            td = timedelta(hours=1)

        opens = [_ts_key(t) for t in candle_open_times]
        close_ts_list = [o + td for o in opens]
        start_bound = min(opens) - max(td, timedelta(hours=6))
        end_bound = max(close_ts_list)

        ivs: List[str] = [interval]
        if interval != "5m":
            ivs.append("5m")

        # dynamic placeholders for intervals
        ph = ", ".join(["%s"] * len(ivs))

        q_seed = f"""
        SELECT DISTINCT ON (interval) interval, ts, open_interest
        FROM open_interest
        WHERE exchange_id=%s AND symbol_id=%s
          AND interval IN ({ph})
          AND ts <= %s
        ORDER BY interval, ts DESC
        """

        q_range = f"""
        SELECT interval, ts, open_interest
        FROM open_interest
        WHERE exchange_id=%s AND symbol_id=%s
          AND interval IN ({ph})
          AND ts > %s AND ts <= %s
        ORDER BY interval, ts ASC
        """

        points_by_iv: Dict[str, List[Tuple[datetime, float]]] = {iv: [] for iv in ivs}

        try:
            with storage.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(q_seed, (int(exchange_id), int(symbol_id), *ivs, start_bound))
                    for iv, ts, val in cur.fetchall() or []:
                        v = _to_float_opt(val)
                        if v is None:
                            continue
                        points_by_iv[str(iv)].append((_utc(ts), float(v)))

                    cur.execute(q_range, (int(exchange_id), int(symbol_id), *ivs, start_bound, end_bound))
                    for iv, ts, val in cur.fetchall() or []:
                        v = _to_float_opt(val)
                        if v is None:
                            continue
                        points_by_iv[str(iv)].append((_utc(ts), float(v)))
        except Exception:
            # if DB hiccup — return empty map and let fallback per-candle queries handle it
            return {}

        # sort points for bisect
        for iv in ivs:
            points_by_iv[iv].sort(key=lambda x: x[0])

        from bisect import bisect_right

        times_by_iv: Dict[str, List[datetime]] = {
            iv: [t for t, _ in (points_by_iv.get(iv) or [])] for iv in ivs
        }

        out: Dict[datetime, Optional[float]] = {}

        for open_ts, close_ts in zip(opens, close_ts_list):
            best_point_ts: Optional[datetime] = None
            best_val: Optional[float] = None

            for iv in ivs:
                pts = points_by_iv.get(iv) or []
                if not pts:
                    continue
                times = times_by_iv.get(iv) or []
                j = bisect_right(times, close_ts) - 1
                if j < 0:
                    continue
                pt_ts, pt_val = pts[j]
                if best_point_ts is None or pt_ts > best_point_ts:
                    best_point_ts = pt_ts
                    best_val = float(pt_val)

            out[open_ts] = best_val

        return out

    @staticmethod
    def _prefetch_cvd_close_map(
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        interval: str,
        candle_open_times: Sequence[datetime],
    ) -> Dict[datetime, Optional[float]]:
        """Batch CVD lookup for a set of candle open times.

        Returns mapping: candle_open_ts (UTC, no microseconds) -> cvd_quote on candle CLOSE.

        Candidate sources (in order of preference by freshness):
          - interval (same TF, if not NULL)
          - 15m (best coverage in your DB)
          - 5m

        For each candle we pick the freshest point (largest open_time) among candidates.
        """
        if not candle_open_times:
            return {}

        interval = str(interval).strip()
        td = _parse_interval(interval)
        if td.total_seconds() <= 0:
            td = timedelta(hours=1)

        opens = [_ts_key(t) for t in candle_open_times]
        close_ts_list = [o + td for o in opens]
        start_bound = min(opens) - max(td, timedelta(hours=6))
        end_bound = max(close_ts_list)

        # build unique candidates
        ivs: List[str] = []
        for iv in (interval, "15m", "5m"):
            if iv not in ivs:
                ivs.append(iv)

        ph = ", ".join(["%s"] * len(ivs))

        q_seed = f"""
        SELECT DISTINCT ON (interval) interval, open_time, cvd_quote
        FROM candles
        WHERE exchange_id=%s AND symbol_id=%s
          AND interval IN ({ph})
          AND open_time <= %s
          AND cvd_quote IS NOT NULL
        ORDER BY interval, open_time DESC
        """

        q_range = f"""
        SELECT interval, open_time, cvd_quote
        FROM candles
        WHERE exchange_id=%s AND symbol_id=%s
          AND interval IN ({ph})
          AND open_time > %s AND open_time <= %s
          AND cvd_quote IS NOT NULL
        ORDER BY interval, open_time ASC
        """

        points_by_iv: Dict[str, List[Tuple[datetime, float]]] = {iv: [] for iv in ivs}

        try:
            with storage.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(q_seed, (int(exchange_id), int(symbol_id), *ivs, start_bound))
                    for iv, ts, val in cur.fetchall() or []:
                        v = _to_float_opt(val)
                        if v is None:
                            continue
                        points_by_iv[str(iv)].append((_utc(ts), float(v)))

                    cur.execute(q_range, (int(exchange_id), int(symbol_id), *ivs, start_bound, end_bound))
                    for iv, ts, val in cur.fetchall() or []:
                        v = _to_float_opt(val)
                        if v is None:
                            continue
                        points_by_iv[str(iv)].append((_utc(ts), float(v)))
        except Exception:
            return {}

        for iv in ivs:
            points_by_iv[iv].sort(key=lambda x: x[0])

        from bisect import bisect_right

        times_by_iv: Dict[str, List[datetime]] = {
            iv: [t for t, _ in (points_by_iv.get(iv) or [])] for iv in ivs
        }

        out: Dict[datetime, Optional[float]] = {}

        for open_ts, close_ts in zip(opens, close_ts_list):
            best_point_ts: Optional[datetime] = None
            best_val: Optional[float] = None

            for iv in ivs:
                pts = points_by_iv.get(iv) or []
                if not pts:
                    continue
                times = times_by_iv.get(iv) or []
                j = bisect_right(times, close_ts) - 1
                if j < 0:
                    continue
                pt_ts, pt_val = pts[j]
                if best_point_ts is None or pt_ts > best_point_ts:
                    best_point_ts = pt_ts
                    best_val = float(pt_val)

            out[open_ts] = best_val

        return out

    @staticmethod
    def _fetch_oi_at(storage: Any, exchange_id: int, symbol_id: int, interval: str, ts: datetime) -> Optional[float]:
        """Open Interest на закрытии свечи с умным выбором таймфрейма.

        В БД у тебя есть OI и по 1h/4h, но они могут сильно лагать. При этом 5m обновляется почти в реальном времени.
        Поэтому для каждой свечи берём OI как **последнее значение <= close_time свечи** и выбираем самый свежий вариант
        среди:
          - interval (тот же TF, что и анализ)
          - base_interval='5m' (если interval != 5m)

        Это даёт:
          - исторически: будут использоваться 1h/4h (когда 5m ещё нет)
          - в онлайне: будет использоваться 5m, если 1h/4h отстают
        """
        interval = str(interval).strip()
        open_ts = _utc(ts)
        td = _parse_interval(interval)
        if td.total_seconds() <= 0:
            td = timedelta(hours=1)
        close_ts = open_ts + td
        close_eps = close_ts - timedelta(microseconds=1)

        q = """
        SELECT ts, open_interest
        FROM open_interest
        WHERE exchange_id=%s AND symbol_id=%s AND interval=%s AND ts <= %s
        ORDER BY ts DESC
        LIMIT 1
        """

        base_interval = "5m"

        try:
            with storage.pool.connection() as conn:
                with conn.cursor() as cur:
                    best_ts: Optional[datetime] = None
                    best_val: Optional[float] = None
                    best_iv: Optional[str] = None

                    candidates = [interval]
                    if interval != base_interval:
                        candidates.append(base_interval)

                    for iv in candidates:
                        cur.execute(q, (int(exchange_id), int(symbol_id), iv, close_eps))
                        r = cur.fetchone()
                        if not r:
                            continue
                        r_ts = _utc(r[0])
                        r_val = _to_float_opt(r[1])
                        if r_val is None:
                            continue
                        if best_ts is None or r_ts > best_ts:
                            best_ts, best_val, best_iv = r_ts, float(r_val), iv

                    if best_val is not None:
                        if best_iv and best_iv != interval:
                            try:
                                log.debug(
                                    "OI pick fresher interval=%s instead of %s symbol_id=%s close=%s oi_ts=%s",
                                    best_iv, interval, int(symbol_id), close_eps, best_ts
                                )
                            except Exception:
                                pass
                        return float(best_val)
        except Exception:
            return None

        return None

    @staticmethod
    def _fetch_cvd_at(storage: Any, exchange_id: int, symbol_id: int, interval: str, ts: datetime) -> Optional[float]:
        """CVD (candles.cvd_quote) на закрытии свечи.

        По твоей статистике:
          - 15m: cvd_quote заполнен (nulls=0)
          - 4h: много NULL (nulls большие)
        Поэтому работаем так:
          1) пытаемся взять cvd_quote из того же interval (если не NULL)
          2) если NULL/нет — берём самый свежий cvd_quote <= close_time из 15m
          3) если 15m вдруг нет — пробуем 5m

        Важно: считаем cvd_quote кумулятивным, поэтому "последнее значение до конца свечи" = CVD на close.
        """
        interval = str(interval).strip()
        open_ts = _utc(ts)
        td = _parse_interval(interval)
        if td.total_seconds() <= 0:
            td = timedelta(hours=1)
        close_ts = open_ts + td
        close_eps = close_ts - timedelta(microseconds=1)

        q = """
        SELECT open_time, cvd_quote
        FROM candles
        WHERE exchange_id=%s AND symbol_id=%s AND interval=%s
          AND open_time <= %s
          AND cvd_quote IS NOT NULL
        ORDER BY open_time DESC
        LIMIT 1
        """

        # Порядок важен для производительности: 15m обычно намного легче чем 1m,
        # и у тебя 15m уже полностью заполнен.
        fallbacks = ["15m", "5m"]

        try:
            with storage.pool.connection() as conn:
                with conn.cursor() as cur:
                    best_ts: Optional[datetime] = None
                    best_val: Optional[float] = None

                    candidates = [interval] + [x for x in fallbacks if x != interval]

                    for iv in candidates:
                        cur.execute(q, (int(exchange_id), int(symbol_id), iv, close_eps))
                        r = cur.fetchone()
                        if not r:
                            continue
                        r_ts = _utc(r[0])
                        r_val = _to_float_opt(r[1])
                        if r_val is None:
                            continue
                        if best_ts is None or r_ts > best_ts:
                            best_ts, best_val = r_ts, float(r_val)

                    if best_val is not None:
                        return float(best_val)
        except Exception:
            return None

        return None

    @staticmethod
    def _fetch_funding_at(storage: Any, exchange_id: int, symbol_id: int, ts: datetime) -> Optional[float]:
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

    @staticmethod
    def _fetch_liquidations_for_candle(
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        candle_open_ts: datetime,
        interval: str,
    ) -> Tuple[float, float]:
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

    # =========================================================
    # current price
    # =========================================================

    @staticmethod
    def _fetch_ticker_last_price(storage: Any, exchange_id: int, symbol_id: int) -> Optional[float]:
        q = """
        SELECT last_price
        FROM public.ticker_24h
        WHERE exchange_id=%s AND symbol_id=%s
        ORDER BY close_time DESC
        LIMIT 1
        """
        try:
            with storage.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(q, (int(exchange_id), int(symbol_id)))
                    r = cur.fetchone()
                    px = _to_float(r[0]) if r and r[0] is not None else None
                    return px if px and px > 0 else None
        except Exception:
            return None

    @staticmethod
    def _fetch_current_price(
        *,
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        interval: str,
    ) -> Tuple[Optional[float], str]:
        px = ScrLiquidationBinance._fetch_ticker_last_price(storage, exchange_id, symbol_id)
        if px is not None:
            return float(px), "ticker_24h.last_price"

        last_close = ScrLiquidationBinance._fetch_last_close(storage, exchange_id, symbol_id, interval)
        if last_close is not None:
            return float(last_close), "candles.close"

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
        p = self._parse_params(interval=interval, params=params)
        log.info("scr_liquidation_binance params: %s", p)

        symbols = self._fetch_symbols(storage=storage, exchange_id=exchange_id)
        if not symbols:
            log.warning("No symbols in DB for exchange_id=%s", exchange_id)
            return []

        out: List[ScreenerSignal] = []
        debug_rows: List[Dict[str, Any]] = []

        need = max(260, p.period_levels + p.windows + p.confirm_lookforward + 60)
        equity_usdt = self._resolve_equity_usdt(storage=storage, exchange_id=exchange_id, p=p)

        for sym_row in symbols:
            symbol_id = int(sym_row["symbol_id"])
            symbol = str(sym_row["symbol"])

            current_price, price_src = self._fetch_current_price(
                storage=storage,
                exchange_id=exchange_id,
                symbol_id=symbol_id,
                interval=p.interval,
            )
            if current_price is None:
                continue

            if not (p.min_price <= current_price <= p.max_price):
                continue

            candles = self._fetch_last_candles(storage, exchange_id, symbol_id, p.interval, limit=need, sp=p)
            min_need = max(p.period_levels + p.windows + p.confirm_lookforward + 12, 140)
            if len(candles) < min_need:
                continue

            lf = max(0, int(p.confirm_lookforward))
            anchor_idx = len(candles) - 1 - lf
            if anchor_idx <= 10 or anchor_idx >= len(candles):
                continue

            anchor = candles[anchor_idx]
            anchor_ts = _utc(anchor["ts"])
            anchor_open = _to_float(anchor["open"])
            anchor_high = _to_float(anchor["high"])
            anchor_low = _to_float(anchor["low"])
            anchor_close = _to_float(anchor["close"])

            confirm_window = candles[anchor_idx + 1: anchor_idx + 1 + lf] if lf > 0 else []
            # -------------------------
            # batched OI/CVD prefetch for anchor + confirm window (performance)
            # -------------------------
            oi_map: Optional[Dict[datetime, Optional[float]]] = None
            cvd_map: Optional[Dict[datetime, Optional[float]]] = None

            pref_times = [anchor_ts] + [_utc(c["ts"]) for c in confirm_window]

            if p.enable_oi:
                oi_map = self._prefetch_oi_close_map(storage, exchange_id, symbol_id, p.interval, pref_times)

            if p.enable_cvd:
                # If all candles already carry cvd_quote (e.g. from live aggregation), skip DB prefetch.
                need_db_cvd = any((c.get("cvd_quote") is None) for c in ([anchor] + list(confirm_window)))
                if need_db_cvd:
                    cvd_map = self._prefetch_cvd_close_map(storage, exchange_id, symbol_id, p.interval, pref_times)
                else:
                    cvd_map = {}


            lvl_hist = candles[max(0, anchor_idx - (p.period_levels + 25)): anchor_idx]
            up_level, down_level, lvl_meta = self._build_levels(
                candles=lvl_hist,
                ref_price=anchor_close,
                p=p,
            )
            if up_level <= 0 or down_level <= 0 or up_level <= down_level:
                continue

            vol_anchor = _to_float(anchor.get("quote_volume") or anchor.get("volume"))
            avg_vol = self._avg_volume_before(candles=candles, idx=anchor_idx, windows=p.windows)
            vol_ratio = (vol_anchor / avg_vol) if avg_vol > 0 else 0.0

            liq_long_usdt, liq_short_usdt = self._fetch_liquidations_for_candle(
                storage=storage,
                exchange_id=exchange_id,
                symbol_id=symbol_id,
                candle_open_ts=anchor_ts,
                interval=p.interval,
            )

            dom_sell_ok = self._liq_dominance_ok(side="SELL", liq_long_usdt=liq_long_usdt, liq_short_usdt=liq_short_usdt, p=p)
            dom_buy_ok = self._liq_dominance_ok(side="BUY", liq_long_usdt=liq_long_usdt, liq_short_usdt=liq_short_usdt, p=p)

            touch_up_evt = self._find_recent_touch_event(candles=candles, idx_anchor=anchor_idx, level=up_level, p=p)
            touch_dn_evt = self._find_recent_touch_event(candles=candles, idx_anchor=anchor_idx, level=down_level, p=p)

            touch_up = touch_up_evt is not None
            touch_dn = touch_dn_evt is not None

            vol_ok = True if float(p.kof_Volume) <= 0 else (vol_ratio >= float(p.kof_Volume))
            sell_liq_ok = (liq_short_usdt >= p.volume_liquid_limit)
            buy_liq_ok = (liq_long_usdt >= p.volume_liquid_limit)

            sell_ready = bool(touch_up and sell_liq_ok and vol_ok and dom_sell_ok)
            buy_ready = bool(touch_dn and buy_liq_ok and vol_ok and dom_buy_ok)

            oi_anchor = cvd_anchor = None

            if p.enable_oi:
                # Prefer batched map, fallback to per-candle query if missing
                if oi_map is not None:
                    oi_anchor = oi_map.get(_ts_key(anchor_ts))
                if oi_anchor is None:
                    oi_anchor = self._fetch_oi_at(storage, exchange_id, symbol_id, p.interval, anchor_ts)

            if p.enable_cvd:
                # Prefer CVD attached to candle (works for live aggregated candles too).
                cvd_anchor = anchor.get("cvd_quote")
                if cvd_anchor is None and cvd_map is not None:
                    cvd_anchor = cvd_map.get(_ts_key(anchor_ts))
                if cvd_anchor is None:
                    cvd_anchor = self._fetch_cvd_at(storage, exchange_id, symbol_id, p.interval, anchor_ts)

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
                    oi_anchor=oi_anchor,
                    cvd_anchor=cvd_anchor,
                    oi_map=oi_map,
                    cvd_map=cvd_map,
                    confirm_window=confirm_window,
                )

            if buy_ready:
                buy_hit_data = self._find_confirmation_long(
                    storage=storage,
                    exchange_id=exchange_id,
                    symbol_id=symbol_id,
                    symbol=symbol,
                    interval=p.interval,
                    p=p,
                    oi_anchor=oi_anchor,
                    cvd_anchor=cvd_anchor,
                    oi_map=oi_map,
                    cvd_map=cvd_map,
                    confirm_window=confirm_window,
                )

            score_sell = None
            score_buy = None
            if sell_hit_data is not None:
                score_sell = min(100.0, 40.0 + (liq_short_usdt / max(1.0, p.volume_liquid_limit)) * 25.0 + vol_ratio)
            if buy_hit_data is not None:
                score_buy = min(100.0, 40.0 + (liq_long_usdt / max(1.0, p.volume_liquid_limit)) * 25.0 + vol_ratio)

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
                if not vol_ok:
                    r.append(f"vol_ratio<{p.kof_Volume}")
                if not dom_buy_ok:
                    r.append("dominance BUY=NO")
                if buy_ready and buy_hit_data is None:
                    r.append("confirm BUY=NO (нет GREEN свечи / OI/CVD/FUND фильтры)")
                return r

            def why_not_sell() -> List[str]:
                r: List[str] = []
                if not touch_up:
                    r.append(f"нет TOUCH/CROSS UP за last {p.touch_lookback_candles}")
                if not sell_liq_ok:
                    r.append(f"liq_short<{p.volume_liquid_limit:.0f}")
                if not vol_ok:
                    r.append(f"vol_ratio<{p.kof_Volume}")
                if not dom_sell_ok:
                    r.append("dominance SELL=NO")
                if sell_ready and sell_hit_data is None:
                    r.append("confirm SELL=NO (нет RED свечи / OI/CVD/FUND фильтры)")
                return r

            # ========= DEBUG collect =========
            if p.debug:
                debug_rows.append({
                    "symbol": symbol,
                    "px": float(current_price),
                    "up": float(up_level),
                    "down": float(down_level),
                    "liqS": float(liq_short_usdt),
                    "liqL": float(liq_long_usdt),
                    "volR": float(vol_ratio),
                    "touch_up": bool(touch_up),
                    "touch_dn": bool(touch_dn),
                    "sell_ready": bool(sell_ready),
                    "buy_ready": bool(buy_ready),
                    "sell_confirm": bool(sell_hit_data is not None),
                    "buy_confirm": bool(buy_hit_data is not None),
                    "why_not_sell": why_not_sell(),
                    "why_not_buy": why_not_buy(),
                })

            # ========= build signal =========
            if chosen == "SELL":
                confirm_ts, confirm_close, oi_c, cvd_c, fund_c, why = sell_hit_data  # type: ignore[misc]

                entry_price, entry_src = self._resolve_entry_price(
                    mode=p.entry_price_mode,
                    current_price=current_price,
                    current_src=price_src,
                    anchor_close=anchor_close,
                    confirm_close=confirm_close,
                )

                sl, tp = self._compute_sl_tp(side="SELL", entry_price=entry_price, p=p)
                qty, notional, risk_usdt = self._compute_position_size(
                    entry_price=entry_price,
                    stop_loss=sl,
                    equity_usdt=equity_usdt,
                    p=p,
                )

                t_idx, t_ts, t_kind = touch_up_evt  # type: ignore[misc]
                touch_age = int(anchor_idx - int(t_idx))

                wnb = why_not_buy()
                if buy_hit_data is not None:
                    wnb.insert(0, f"BUY тоже подтвердился, но выбран SELL по score: sell={float(score_sell or 0.0):.2f} >= buy={float(score_buy or 0.0):.2f}")

                out.append(
                    self._make_signal(
                        symbol_id=symbol_id,
                        symbol=symbol,
                        timeframe=p.interval,
                        signal_ts=_utc(confirm_ts),
                        side="SELL",
                        entry_price=entry_price,
                        sl=sl,
                        tp=tp,
                        confidence=0.83,
                        score=float(score_sell or 0.0),
                        reason=f"{t_kind} UP (last {p.touch_lookback_candles}) + SHORT LIQ DOM -> CONFIRM DOWN | {why}",
                        context={
                            "touch_side": "UP",
                            "touch_ts": _utc(t_ts),
                            "touch_level": float(up_level),
                            "touch_age_candles": touch_age,
                            "touch_kind": str(t_kind),

                            "why_not_buy": wnb,
                            "why_not_sell": [],

                            "anchor_ts": anchor_ts,
                            "confirm_ts": _utc(confirm_ts),

                            "entry_price_source": entry_src,
                            "current_price": float(current_price),
                            "anchor_close": float(anchor_close),
                            "confirm_close": float(confirm_close),

                            "up_level": float(up_level),
                            "down_level": float(down_level),

                            "liq_short_usdt": float(liq_short_usdt),
                            "liq_long_usdt": float(liq_long_usdt),
                            "liq_dominance_pct": float(p.liq_dominance_pct),

                            "vol_ratio": float(vol_ratio),
                            "avg_vol": float(avg_vol),
                            "volume": float(vol_anchor),

                            "anchor_open": float(anchor_open),
                            "anchor_high": float(anchor_high),
                            "anchor_low": float(anchor_low),

                            "oi_anchor": _to_float(oi_anchor) if oi_anchor is not None else None,
                            "oi_confirm": _to_float(oi_c) if oi_c is not None else None,
                            "cvd_anchor": _to_float(cvd_anchor) if cvd_anchor is not None else None,
                            "cvd_confirm": _to_float(cvd_c) if cvd_c is not None else None,
                            "funding_confirm": _to_float(fund_c) if fund_c is not None else None,

                            "levels_meta": lvl_meta,

                            "equity_usdt": _to_float(equity_usdt) if equity_usdt is not None else None,
                            "risk_trade_pct": float(p.risk_trade_pct),
                            "risk_trade_usdt": _to_float(risk_usdt) if risk_usdt is not None else None,
                            "stop_loss_pct": float(p.stop_loss_pct),
                            "take_profit_pct": float(p.take_profit_pct),
                            "position_qty": _to_float(qty) if qty is not None else None,
                            "position_notional_usdt": _to_float(notional) if notional is not None else None,
                        },
                    )
                )

            elif chosen == "BUY":
                confirm_ts, confirm_close, oi_c, cvd_c, fund_c, why = buy_hit_data  # type: ignore[misc]

                entry_price, entry_src = self._resolve_entry_price(
                    mode=p.entry_price_mode,
                    current_price=current_price,
                    current_src=price_src,
                    anchor_close=anchor_close,
                    confirm_close=confirm_close,
                )

                sl, tp = self._compute_sl_tp(side="BUY", entry_price=entry_price, p=p)
                qty, notional, risk_usdt = self._compute_position_size(
                    entry_price=entry_price,
                    stop_loss=sl,
                    equity_usdt=equity_usdt,
                    p=p,
                )

                t_idx, t_ts, t_kind = touch_dn_evt  # type: ignore[misc]
                touch_age = int(anchor_idx - int(t_idx))

                wns = why_not_sell()
                if sell_hit_data is not None:
                    wns.insert(0, f"SELL тоже подтвердился, но выбран BUY по score: buy={float(score_buy or 0.0):.2f} > sell={float(score_sell or 0.0):.2f}")

                out.append(
                    self._make_signal(
                        symbol_id=symbol_id,
                        symbol=symbol,
                        timeframe=p.interval,
                        signal_ts=_utc(confirm_ts),
                        side="BUY",
                        entry_price=entry_price,
                        sl=sl,
                        tp=tp,
                        confidence=0.83,
                        score=float(score_buy or 0.0),
                        reason=f"{t_kind} DOWN (last {p.touch_lookback_candles}) + LONG LIQ DOM -> CONFIRM UP | {why}",
                        context={
                            "touch_side": "DOWN",
                            "touch_ts": _utc(t_ts),
                            "touch_level": float(down_level),
                            "touch_age_candles": touch_age,
                            "touch_kind": str(t_kind),

                            "why_not_buy": [],
                            "why_not_sell": wns,

                            "anchor_ts": anchor_ts,
                            "confirm_ts": _utc(confirm_ts),

                            "entry_price_source": entry_src,
                            "current_price": float(current_price),
                            "anchor_close": float(anchor_close),
                            "confirm_close": float(confirm_close),

                            "up_level": float(up_level),
                            "down_level": float(down_level),

                            "liq_short_usdt": float(liq_short_usdt),
                            "liq_long_usdt": float(liq_long_usdt),
                            "liq_dominance_pct": float(p.liq_dominance_pct),

                            "vol_ratio": float(vol_ratio),
                            "avg_vol": float(avg_vol),
                            "volume": float(vol_anchor),

                            "anchor_open": float(anchor_open),
                            "anchor_high": float(anchor_high),
                            "anchor_low": float(anchor_low),

                            "oi_anchor": _to_float(oi_anchor) if oi_anchor is not None else None,
                            "oi_confirm": _to_float(oi_c) if oi_c is not None else None,
                            "cvd_anchor": _to_float(cvd_anchor) if cvd_anchor is not None else None,
                            "cvd_confirm": _to_float(cvd_c) if cvd_c is not None else None,
                            "funding_confirm": _to_float(fund_c) if fund_c is not None else None,

                            "levels_meta": lvl_meta,

                            "equity_usdt": _to_float(equity_usdt) if equity_usdt is not None else None,
                            "risk_trade_pct": float(p.risk_trade_pct),
                            "risk_trade_usdt": _to_float(risk_usdt) if risk_usdt is not None else None,
                            "stop_loss_pct": float(p.stop_loss_pct),
                            "take_profit_pct": float(p.take_profit_pct),
                            "position_qty": _to_float(qty) if qty is not None else None,
                            "position_notional_usdt": _to_float(notional) if notional is not None else None,
                        },
                    )
                )

        # ========= DEBUG print =========
        if p.debug and debug_rows:
            debug_rows.sort(key=lambda r: (r["liqS"] + r["liqL"]), reverse=True)
            log.info("=== DEBUG TOP %d interval=%s touch_mode=%s ===", int(p.debug_top), p.interval, p.touch_mode)

            for r in debug_rows[: max(1, int(p.debug_top))]:
                log.info(
                    "%s px=%.6f up=%.6f down=%.6f liqS=%.0f liqL=%.0f volR=%.2f "
                    "touchUP=%s touchDN=%s readyS=%s confS=%s readyB=%s confB=%s",
                    r["symbol"], r["px"], r["up"], r["down"], r["liqS"], r["liqL"], r["volR"],
                    r["touch_up"], r["touch_dn"], r["sell_ready"], r["sell_confirm"], r["buy_ready"], r["buy_confirm"],
                )

                if not r["sell_confirm"]:
                    log.info("  why_not_sell: %s", " | ".join(r["why_not_sell"]) if r["why_not_sell"] else "—")
                if not r["buy_confirm"]:
                    log.info("  why_not_buy : %s", " | ".join(r["why_not_buy"]) if r["why_not_buy"] else "—")

        return out
