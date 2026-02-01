# src/platform/screeners/scr_pump_binance.py
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from src.platform.data.storage.postgres.storage import PostgreSQLStorage
from src.platform.core.utils.candles import interval_to_timedelta

log = logging.getLogger("screeners.scr_pump_binance")


# -------------------------
# small utils
# -------------------------

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_dt(v: Any) -> Optional[datetime]:
    """
    Приводит ts к datetime (aware UTC), если возможно.
    Поддерживает:
      - datetime (naive -> считаем UTC)
      - str (isoformat)
    """
    if isinstance(v, datetime):
        return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v.astimezone(timezone.utc)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s)
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
        except Exception:
            return None
    return None


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _to_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def _clamp_int(x: Any, lo: int, hi: int, default: int) -> int:
    v = _to_int(x, default)
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def _pct_change(start: float, end: float) -> float:
    if start is None or end is None:
        return 0.0
    if start == 0:
        return 0.0
    return (float(end) / float(start) - 1.0) * 100.0


def _safe_mean(vals: List[float]) -> Optional[float]:
    try:
        xs = [float(v) for v in vals if v is not None and float(v) > 0]
        if not xs:
            return None
        return sum(xs) / float(len(xs))
    except Exception:
        return None


def _median(xs: List[float]) -> Optional[float]:
    try:
        ys = [float(x) for x in xs if x is not None]
        if not ys:
            return None
        ys.sort()
        n = len(ys)
        mid = n // 2
        if n % 2 == 1:
            return ys[mid]
        return 0.5 * (ys[mid - 1] + ys[mid])
    except Exception:
        return None


def _linreg_slope(y: List[float]) -> Optional[float]:
    """
    Наклон линейной регрессии y по x=0..n-1.
    Возвращает slope в "единицах y на 1 бар".
    """
    try:
        n = len(y)
        if n < 2:
            return None

        x_mean = (n - 1) / 2.0
        y_mean = sum(float(v) for v in y) / float(n)

        var = 0.0
        cov = 0.0
        for i, v in enumerate(y):
            dx = float(i) - x_mean
            dy = float(v) - y_mean
            var += dx * dx
            cov += dx * dy

        if var <= 0:
            return None
        return cov / var
    except Exception:
        return None


def _sort_series_by_ts(series: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Стабильная сортировка series по ts.
    Если ts битый/непарсится — элемент улетит в начало.
    """
    try:
        def _key(r: Dict[str, Any]) -> datetime:
            dt = _as_dt(r.get("ts"))
            return dt if dt is not None else datetime.min.replace(tzinfo=timezone.utc)

        return sorted(series, key=_key)
    except Exception:
        return series


def _pick_series_bounds(
    series: List[Dict[str, Any]],
    *,
    start_ts: datetime,
    end_ts: datetime,
    value_key: str,
) -> Optional[Tuple[float, float]]:
    """
    Берём значение ближе всего к start_ts (<=) и end_ts (<=).
    series должен быть отсортирован по ts.
    """
    if not series:
        return None

    start_ts2 = _as_dt(start_ts) or start_ts
    end_ts2 = _as_dt(end_ts) or end_ts

    start_val = None
    end_val = None

    for r in series:
        ts = _as_dt(r.get("ts"))
        if ts is None:
            continue
        if ts <= start_ts2:
            start_val = r.get(value_key)
        if ts <= end_ts2:
            end_val = r.get(value_key)
        else:
            break

    if start_val is None:
        start_val = series[0].get(value_key)
    if end_val is None:
        end_val = series[-1].get(value_key)

    try:
        return float(start_val), float(end_val)
    except Exception:
        return None


def _last_n_values(
    series: List[Dict[str, Any]],
    *,
    end_ts: datetime,
    value_key: str,
    n: int,
) -> Optional[List[float]]:
    """
    Берём последние n значений (по ts) <= end_ts.
    series желательно уже отсортирован по ts.
    """
    if not series or n <= 0:
        return None

    end_ts2 = _as_dt(end_ts) or end_ts

    out: List[float] = []
    for r in reversed(series):
        ts = _as_dt(r.get("ts"))
        if ts is None:
            continue
        if ts <= end_ts2:
            try:
                out.append(float(r.get(value_key)))
            except Exception:
                continue
            if len(out) >= n:
                break

    if len(out) < n:
        return None

    out.reverse()
    return out


def _trend_steps_ok(
    values: List[float],
    *,
    direction: str,
    eps: float = 0.0,
    strict: bool = True,
) -> bool:
    """
    values: [v0, v1, v2, ...] -> проверяем шаги v1-v0, v2-v1, ...
    direction:
      - "up": шаги > eps (strict) или >= eps (not strict)
      - "down": шаги < -eps (strict) или <= -eps (not strict)
    """
    if not values or len(values) < 2:
        return False

    for i in range(1, len(values)):
        d = float(values[i]) - float(values[i - 1])

        if direction == "up":
            if strict:
                if not (d > eps):
                    return False
            else:
                if not (d >= eps):
                    return False

        elif direction == "down":
            if strict:
                if not (d < -eps):
                    return False
            else:
                if not (d <= -eps):
                    return False
        else:
            return False

    return True


def _base_avg_before_window(
    candles: List[Dict[str, Any]],
    *,
    end_exclusive_idx: int,
    lookback_bars: int,
) -> Optional[float]:
    """
    Средняя цена (close) по свечам ДО окна.
    end_exclusive_idx: индекс (не включительно), до которого берём базу.
    """
    if not candles:
        return None

    lb = max(20, int(lookback_bars))
    end_i = max(0, min(int(end_exclusive_idx), len(candles)))
    start_i = max(0, end_i - lb)

    base_slice = candles[start_i:end_i]
    if len(base_slice) < 10:
        base_slice = candles[-min(lb, len(candles)):]

    base_prices = [_to_float(c.get("close"), 0.0) for c in base_slice]
    return _safe_mean(base_prices)


# -------------------------
# data model
# -------------------------

@dataclass
class ScreenerSignal:
    symbol_id: int
    symbol: str
    signal_ts: datetime
    side: str  # BUY / SELL
    entry_price: float
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    exit_price: Optional[float] = None
    confidence: Optional[float] = None
    score: Optional[float] = None
    reason: Optional[str] = None
    context: Optional[Dict[str, Any]] = None


@dataclass
class ScrParams:
    # general
    intervals: List[str] = None
    min_price: float = 1e-7
    max_price: float = 100000.0

    # режим работы (both|buy|sell)
    signal_mode: str = "both"

    # BUY: from AVG baseline
    buy_window_minutes: int = 2
    buy_price_change_pct: float = 2.0
    buy_base_lookback_bars: int = 300

    # SELL: from AVG baseline + red confirm
    sell_window_minutes: int = 15
    sell_pump_pct: float = 10.0
    sell_base_lookback_bars: int = 300
    sell_confirm_lookforward: int = 3  # 1..N

    # confirmations
    enable_oi: bool = True
    enable_cvd: bool = True

    # thresholds (prev-candle delta)
    buy_min_oi_delta: float = 0.0
    buy_min_cvd_delta: float = 0.0
    sell_max_oi_delta: float = 0.0  # expecting <= 0
    sell_max_cvd_delta: float = 0.0  # expecting <= 0

    # intervals for OI/CVD (empty -> use candle interval)
    oi_interval: Optional[str] = None
    cvd_interval: Optional[str] = None

    # -------------------------
    # TREND FILTER
    # -------------------------
    # trend_method: off|steps|slope|both
    trend_method: str = "off"

    # trend_mode: none|oi|cvd|both (применяется к steps/slope)
    trend_mode: str = "both"

    # steps-trend: последние N шагов (N=2 -> 3 точки)
    confirm_trend_bars: int = 2
    trend_strict: bool = True
    oi_eps: float = 0.0
    cvd_eps: float = 0.0

    # slope-trend: линейная регрессия на последних slope_bars точках
    slope_bars: int = 9
    slope_strict: bool = True

    # BACKWARD-COMPAT (старые симметричные пороги)
    oi_slope_min_bps_per_bar: float = 0.0
    cvd_slope_min_k: float = 0.0

    # NEW: раздельные пороги BUY/SELL
    buy_oi_slope_min_bps: Optional[float] = None
    buy_cvd_slope_norm_min: Optional[float] = None
    sell_oi_slope_max_bps: Optional[float] = None
    sell_cvd_slope_norm_max: Optional[float] = None

    # funding filter
    enable_funding: bool = False
    buy_funding_max_pct: Optional[float] = None   # отрицательное (например -0.01)
    sell_funding_min_pct: Optional[float] = None  # положительное (например 0.01)

    # risk / tp/sl
    stop_loss_pct: Optional[float] = None
    take_profit_pct: Optional[float] = None

    # scan
    max_symbols: int = 0
    scan_lookback_bars: int = 200

    # debug
    debug: bool = False
    debug_top: int = 3


def _parse_params(params: Dict[str, Any]) -> ScrParams:
    p = ScrParams()

    p.intervals = [str(x).strip() for x in (params.get("intervals") or []) if str(x).strip()] or [
        str(params.get("interval") or "1m").strip()
    ]

    p.min_price = _to_float(params.get("min_price"), p.min_price)
    p.max_price = _to_float(params.get("max_price"), p.max_price)

    # mode
    mode = str(params.get("signal_mode", "both")).strip().lower()
    if mode not in ("both", "buy", "sell"):
        mode = "both"
    p.signal_mode = mode

    # BUY
    p.buy_window_minutes = max(1, _to_int(params.get("buy_window_minutes"), p.buy_window_minutes))
    p.buy_price_change_pct = _to_float(params.get("buy_price_change_pct"), p.buy_price_change_pct)
    p.buy_base_lookback_bars = max(20, _to_int(params.get("buy_base_lookback_bars"), p.buy_base_lookback_bars))

    # SELL
    p.sell_window_minutes = max(1, _to_int(params.get("sell_window_minutes"), p.sell_window_minutes))
    p.sell_pump_pct = _to_float(params.get("sell_pump_pct"), p.sell_pump_pct)
    p.sell_base_lookback_bars = max(20, _to_int(params.get("sell_base_lookback_bars"), p.sell_base_lookback_bars))

    p.sell_confirm_lookforward = _clamp_int(
        params.get("sell_confirm_lookforward"),
        1,
        10,
        p.sell_confirm_lookforward,
    )

    # confirmations enabled
    p.enable_oi = bool(params.get("enable_oi", p.enable_oi))
    p.enable_cvd = bool(params.get("enable_cvd", p.enable_cvd))

    # thresholds
    p.buy_min_oi_delta = _to_float(params.get("buy_min_oi_delta"), p.buy_min_oi_delta)
    p.buy_min_cvd_delta = _to_float(params.get("buy_min_cvd_delta"), p.buy_min_cvd_delta)
    p.sell_max_oi_delta = _to_float(params.get("sell_max_oi_delta"), p.sell_max_oi_delta)
    p.sell_max_cvd_delta = _to_float(params.get("sell_max_cvd_delta"), p.sell_max_cvd_delta)

    # data intervals
    p.oi_interval = str(params.get("oi_interval") or "").strip() or None
    p.cvd_interval = str(params.get("cvd_interval") or "").strip() or None

    # -------------------------
    # TREND: method
    # -------------------------
    tm = str(params.get("trend_method") or "").strip().lower()
    if tm in ("off", "none", "false", "0", ""):
        tm = "off"
    elif tm not in ("steps", "slope", "both"):
        tm = "off"
    p.trend_method = tm

    # legacy require_2step_oi_cvd -> steps
    legacy_require = params.get("require_trend")
    if legacy_require is None:
        legacy_require = params.get("require_2step_oi_cvd", None)

    legacy_bars = params.get("confirm_trend_bars")
    if legacy_bars is None:
        legacy_bars = params.get("oi_cvd_trend_bars", None)

    if p.trend_method == "off" and legacy_require is not None:
        if bool(legacy_require):
            p.trend_method = "steps"

    if legacy_bars not in (None, "", "null"):
        try:
            p.confirm_trend_bars = _clamp_int(legacy_bars, 0, 50, p.confirm_trend_bars)
        except Exception:
            pass

    # trend_mode
    tmode = str(params.get("trend_mode") or "both").strip().lower()
    if tmode in ("off", "none", "false", "0", ""):
        tmode = "none"
    if tmode not in ("none", "oi", "cvd", "both"):
        tmode = "both"
    p.trend_mode = tmode

    # strict + eps
    p.trend_strict = bool(params.get("trend_strict", p.trend_strict))
    p.oi_eps = _to_float(params.get("oi_eps"), p.oi_eps)
    p.cvd_eps = _to_float(params.get("cvd_eps"), p.cvd_eps)

    # slope params: alias trend_slope_points -> slope_bars
    if "slope_bars" in params:
        p.slope_bars = _clamp_int(params.get("slope_bars"), 2, 200, p.slope_bars)
    else:
        alias_pts = params.get("trend_slope_points")
        if alias_pts not in (None, "", "null"):
            p.slope_bars = _clamp_int(alias_pts, 2, 200, p.slope_bars)

    p.slope_strict = bool(params.get("slope_strict", p.slope_strict))

    # backward-compat symmetric thresholds
    p.oi_slope_min_bps_per_bar = _to_float(params.get("oi_slope_min_bps_per_bar"), p.oi_slope_min_bps_per_bar)
    p.cvd_slope_min_k = _to_float(params.get("cvd_slope_min_k"), p.cvd_slope_min_k)

    # NEW: separate thresholds
    b1 = params.get("buy_oi_slope_min_bps")
    b2 = params.get("buy_cvd_slope_norm_min")
    s1 = params.get("sell_oi_slope_max_bps")
    s2 = params.get("sell_cvd_slope_norm_max")

    p.buy_oi_slope_min_bps = None if b1 in (None, "", "null") else _to_float(b1, None)
    p.buy_cvd_slope_norm_min = None if b2 in (None, "", "null") else _to_float(b2, None)
    p.sell_oi_slope_max_bps = None if s1 in (None, "", "null") else _to_float(s1, None)
    p.sell_cvd_slope_norm_max = None if s2 in (None, "", "null") else _to_float(s2, None)

    # safeguards on signs
    if p.buy_oi_slope_min_bps is not None:
        p.buy_oi_slope_min_bps = abs(float(p.buy_oi_slope_min_bps))
    if p.buy_cvd_slope_norm_min is not None:
        p.buy_cvd_slope_norm_min = abs(float(p.buy_cvd_slope_norm_min))

    if p.sell_oi_slope_max_bps is not None:
        p.sell_oi_slope_max_bps = -abs(float(p.sell_oi_slope_max_bps))
    if p.sell_cvd_slope_norm_max is not None:
        p.sell_cvd_slope_norm_max = -abs(float(p.sell_cvd_slope_norm_max))

    # funding
    p.enable_funding = bool(params.get("enable_funding", False))
    bf = params.get("buy_funding_max_pct")
    sf = params.get("sell_funding_min_pct")
    p.buy_funding_max_pct = None if bf in (None, "", "null") else _to_float(bf, None)
    p.sell_funding_min_pct = None if sf in (None, "", "null") else _to_float(sf, None)

    # защита от случайно перепутанного знака
    if p.buy_funding_max_pct is not None and p.buy_funding_max_pct > 0:
        p.buy_funding_max_pct = -abs(float(p.buy_funding_max_pct))
    if p.sell_funding_min_pct is not None and p.sell_funding_min_pct < 0:
        p.sell_funding_min_pct = abs(float(p.sell_funding_min_pct))

    # risk
    slp = params.get("stop_loss_pct")
    tpp = params.get("take_profit_pct")
    p.stop_loss_pct = None if slp in (None, "", "null") else _to_float(slp, None)
    p.take_profit_pct = None if tpp in (None, "", "null") else _to_float(tpp, None)

    # scan
    p.max_symbols = _to_int(params.get("max_symbols"), p.max_symbols)
    p.scan_lookback_bars = max(50, _to_int(params.get("scan_lookback_bars"), p.scan_lookback_bars))

    # debug
    p.debug = bool(params.get("debug", p.debug))
    p.debug_top = max(0, _to_int(params.get("debug_top"), p.debug_top))

    return p


class ScrPumpBinance:
    """PUMP screener (Binance)."""

    name = "scr_pump_binance"
    version = "0.1"

    # -------------------------
    # funding helper
    # -------------------------

    def _funding_pass(self, *, side: str, funding_pct: Optional[float], sp: ScrParams) -> bool:
        """
        BUY: funding должен быть < 0 и <= buy_funding_max_pct (если задан)
        SELL: funding должен быть > 0 и >= sell_funding_min_pct (если задан)

        funding_pct ожидается в ПРОЦЕНТАХ (т.к. в run мы делаем rate * 100).
        """
        if not sp.enable_funding:
            return True

        s = str(side or "").upper()

        if funding_pct is None:
            return False

        if s == "BUY":
            if funding_pct >= 0.0:
                return False
            if sp.buy_funding_max_pct is None:
                return True
            return funding_pct <= float(sp.buy_funding_max_pct)

        if s == "SELL":
            if funding_pct <= 0.0:
                return False
            if sp.sell_funding_min_pct is None:
                return True
            return funding_pct >= float(sp.sell_funding_min_pct)

        return True

    # -------------------------
    # OI / CVD helpers
    # -------------------------

    def _oi_fallbacks(self, *, sp: ScrParams, candle_interval: str) -> List[str]:
        base = sp.oi_interval or candle_interval

        if sp.trend_method in ("steps", "slope", "both"):
            fb = [base]
            if candle_interval != base:
                fb.append(candle_interval)
            return fb

        return [base, "5m", "15m", "1h", "4h"]

    def _cvd_fallbacks(self, *, sp: ScrParams, candle_interval: str) -> List[str]:
        base = sp.cvd_interval or candle_interval

        if sp.trend_method in ("steps", "slope", "both"):
            fb = [base]
            if candle_interval != base:
                fb.append(candle_interval)
            return fb

        return [base, "5m", "15m", "1h", "4h", candle_interval]

    def _need_points(self, *, sp: ScrParams) -> int:
        steps_pts = max(0, int(sp.confirm_trend_bars)) + 1 if sp.trend_method in ("steps", "both") else 0
        slope_pts = int(sp.slope_bars) if sp.trend_method in ("slope", "both") else 0
        return max(2, steps_pts, slope_pts)

    def _calc_oi_info(
        self,
        *,
        storage: PostgreSQLStorage,
        exchange_id: int,
        symbol_id: int,
        candle_interval: str,
        start_ts: datetime,
        end_ts: datetime,
        sp: ScrParams,
    ) -> Tuple[Optional[float], Optional[List[float]], Optional[str]]:
        fallbacks = self._oi_fallbacks(sp=sp, candle_interval=candle_interval)

        end_dt = _as_dt(end_ts) or end_ts
        start_dt = _as_dt(start_ts) or start_ts

        need_pts = self._need_points(sp=sp)

        for itv in fallbacks:
            try:
                td = interval_to_timedelta(itv)
                if td.total_seconds() <= 0:
                    continue

                span_sec = max(0.0, (end_dt - start_dt).total_seconds())
                bars = int(span_sec // td.total_seconds()) + 20
                bars = max(30, min(1200, bars))
                bars = max(bars, need_pts + 30)

                series = storage.fetch_open_interest_window(
                    exchange_id=int(exchange_id),
                    symbol_id=int(symbol_id),
                    interval=str(itv),
                    center_ts=end_dt,
                    lookback=bars,
                    lookforward=0,
                ) or []
                if not series:
                    continue

                series = _sort_series_by_ts(series)

                bounds = _pick_series_bounds(series, start_ts=start_dt, end_ts=end_dt, value_key="open_interest")
                points = _last_n_values(series, end_ts=end_dt, value_key="open_interest", n=need_pts)

                delta = None
                if bounds is not None:
                    a, b = bounds
                    delta = float(b - a)

                if delta is not None or points is not None:
                    return delta, points, str(itv)

            except Exception:
                continue

        return None, None, None

    def _calc_cvd_info(
        self,
        *,
        storage: PostgreSQLStorage,
        exchange_id: int,
        symbol_id: int,
        candle_interval: str,
        start_ts: datetime,
        end_ts: datetime,
        sp: ScrParams,
    ) -> Tuple[Optional[float], Optional[List[float]], Optional[str]]:
        fallbacks = self._cvd_fallbacks(sp=sp, candle_interval=candle_interval)

        end_dt = _as_dt(end_ts) or end_ts
        start_dt = _as_dt(start_ts) or start_ts

        need_pts = self._need_points(sp=sp)

        for itv in fallbacks:
            try:
                td = interval_to_timedelta(itv)
                if td.total_seconds() <= 0:
                    continue

                span_sec = max(0.0, (end_dt - start_dt).total_seconds())
                bars = int(span_sec // td.total_seconds()) + 20
                bars = max(30, min(1200, bars))
                bars = max(bars, need_pts + 30)

                series = storage.fetch_cvd_window(
                    exchange_id=int(exchange_id),
                    symbol_id=int(symbol_id),
                    interval=str(itv),
                    center_ts=end_dt,
                    lookback=bars,
                    lookforward=0,
                ) or []
                if not series:
                    continue

                series = _sort_series_by_ts(series)

                bounds = _pick_series_bounds(series, start_ts=start_dt, end_ts=end_dt, value_key="cvd_quote")
                points = _last_n_values(series, end_ts=end_dt, value_key="cvd_quote", n=need_pts)

                delta = None
                if bounds is not None:
                    a, b = bounds
                    delta = float(b - a)

                if delta is not None or points is not None:
                    return delta, points, str(itv)

            except Exception:
                continue

        return None, None, None

    # -------------------------
    # TREND checks
    # -------------------------

    def _use_oi_trend(self, sp: ScrParams) -> bool:
        if sp.trend_mode == "none":
            return False
        return sp.enable_oi and sp.trend_mode in ("oi", "both")

    def _use_cvd_trend(self, sp: ScrParams) -> bool:
        if sp.trend_mode == "none":
            return False
        return sp.enable_cvd and sp.trend_mode in ("cvd", "both")

    def _trend_steps_pass(
        self,
        *,
        side: str,
        oi_points: Optional[List[float]],
        cvd_points: Optional[List[float]],
        sp: ScrParams,
    ) -> bool:
        if sp.trend_method not in ("steps", "both"):
            return True
        if sp.trend_mode == "none":
            return True

        steps = int(sp.confirm_trend_bars)
        if steps <= 0:
            return True

        need = steps + 1
        strict = bool(sp.trend_strict)
        s = str(side or "").upper()

        use_oi = self._use_oi_trend(sp)
        use_cvd = self._use_cvd_trend(sp)

        # если по trend_mode вообще ничего не проверяем
        if not use_oi and not use_cvd:
            return True

        if use_oi:
            if oi_points is None or len(oi_points) < need:
                return False
            if s == "BUY":
                if not _trend_steps_ok(oi_points[-need:], direction="up", eps=float(sp.oi_eps), strict=strict):
                    return False
            elif s == "SELL":
                if not _trend_steps_ok(oi_points[-need:], direction="down", eps=float(sp.oi_eps), strict=strict):
                    return False

        if use_cvd:
            if cvd_points is None or len(cvd_points) < need:
                return False
            if s == "BUY":
                if not _trend_steps_ok(cvd_points[-need:], direction="up", eps=float(sp.cvd_eps), strict=strict):
                    return False
            elif s == "SELL":
                if not _trend_steps_ok(cvd_points[-need:], direction="down", eps=float(sp.cvd_eps), strict=strict):
                    return False

        return True

    def _trend_slope_pass(
        self,
        *,
        side: str,
        oi_points: Optional[List[float]],
        cvd_points: Optional[List[float]],
        sp: ScrParams,
    ) -> Tuple[bool, Optional[float], Optional[float], Optional[float]]:
        """
        Возвращает:
          (pass, oi_slope_bps_per_bar, cvd_slope_norm, cvd_med_abs_delta)
        """
        if sp.trend_method not in ("slope", "both"):
            return True, None, None, None
        if sp.trend_mode == "none":
            return True, None, None, None

        n = int(sp.slope_bars)
        if n < 2:
            return True, None, None, None

        s = str(side or "").upper()
        strict = bool(sp.slope_strict)

        oi_bps = None
        cvd_norm = None
        cvd_med = None

        use_oi = self._use_oi_trend(sp)
        use_cvd = self._use_cvd_trend(sp)

        if not use_oi and not use_cvd:
            return True, None, None, None

        # --- OI slope (log) ---
        if use_oi:
            if not oi_points or len(oi_points) < n:
                return False, None, None, None

            tail = oi_points[-n:]
            ys = []
            for v in tail:
                vv = float(v)
                if vv <= 0:
                    return False, None, None, None
                ys.append(math.log(vv))

            slope = _linreg_slope(ys)
            if slope is None:
                return False, None, None, None

            oi_bps = float(slope) * 10000.0  # bps/бар

            if s == "BUY":
                thr = sp.buy_oi_slope_min_bps
                if thr is None:
                    thr = float(sp.oi_slope_min_bps_per_bar)  # backward compat
                thr = float(thr or 0.0)

                if thr > 0:
                    if oi_bps < thr:
                        return False, oi_bps, None, None
                else:
                    if strict:
                        if not (oi_bps > 0.0):
                            return False, oi_bps, None, None
                    else:
                        if not (oi_bps >= 0.0):
                            return False, oi_bps, None, None

            elif s == "SELL":
                thr_max = sp.sell_oi_slope_max_bps
                if thr_max is not None:
                    thr_max = float(thr_max)  # уже отрицательный
                    if oi_bps > thr_max:
                        return False, oi_bps, None, None
                else:
                    # backward compat: симметрия от oi_slope_min_bps_per_bar
                    mag = float(sp.oi_slope_min_bps_per_bar or 0.0)
                    if mag > 0:
                        if oi_bps > -mag:
                            return False, oi_bps, None, None
                    else:
                        if strict:
                            if not (oi_bps < 0.0):
                                return False, oi_bps, None, None
                        else:
                            if not (oi_bps <= 0.0):
                                return False, oi_bps, None, None

        # --- CVD slope (normalized by median abs delta) ---
        if use_cvd:
            if not cvd_points or len(cvd_points) < n:
                return False, oi_bps, None, None

            tail = [float(x) for x in cvd_points[-n:]]
            slope = _linreg_slope(tail)
            if slope is None:
                return False, oi_bps, None, None

            deltas = [abs(tail[i] - tail[i - 1]) for i in range(1, len(tail))]
            cvd_med = _median(deltas) or 0.0
            denom = max(1e-12, float(cvd_med))
            cvd_norm = float(slope) / denom

            if s == "BUY":
                thr = sp.buy_cvd_slope_norm_min
                if thr is None:
                    thr = float(sp.cvd_slope_min_k)  # backward compat
                thr = float(thr or 0.0)

                if thr > 0:
                    if cvd_norm < thr:
                        return False, oi_bps, cvd_norm, cvd_med
                else:
                    if strict:
                        if not (cvd_norm > 0.0):
                            return False, oi_bps, cvd_norm, cvd_med
                    else:
                        if not (cvd_norm >= 0.0):
                            return False, oi_bps, cvd_norm, cvd_med

            elif s == "SELL":
                thr_max = sp.sell_cvd_slope_norm_max
                if thr_max is not None:
                    thr_max = float(thr_max)  # уже отрицательный
                    if cvd_norm > thr_max:
                        return False, oi_bps, cvd_norm, cvd_med
                else:
                    # backward compat: симметрия от cvd_slope_min_k
                    mag = float(sp.cvd_slope_min_k or 0.0)
                    if mag > 0:
                        if cvd_norm > -mag:
                            return False, oi_bps, cvd_norm, cvd_med
                    else:
                        if strict:
                            if not (cvd_norm < 0.0):
                                return False, oi_bps, cvd_norm, cvd_med
                        else:
                            if not (cvd_norm <= 0.0):
                                return False, oi_bps, cvd_norm, cvd_med

        return True, oi_bps, cvd_norm, cvd_med

    def _trend_pass(
        self,
        *,
        side: str,
        oi_points: Optional[List[float]],
        cvd_points: Optional[List[float]],
        sp: ScrParams,
    ) -> Tuple[bool, Dict[str, Any]]:
        diag: Dict[str, Any] = {
            "trend_method": sp.trend_method,
            "trend_mode": sp.trend_mode,

            "steps_bars": int(sp.confirm_trend_bars),
            "steps_strict": bool(sp.trend_strict),
            "oi_eps": float(sp.oi_eps),
            "cvd_eps": float(sp.cvd_eps),

            "slope_bars": int(sp.slope_bars),
            "slope_strict": bool(sp.slope_strict),

            "sym_oi_slope_min_bps_per_bar": float(sp.oi_slope_min_bps_per_bar),
            "sym_cvd_slope_min_k": float(sp.cvd_slope_min_k),

            "buy_oi_slope_min_bps": sp.buy_oi_slope_min_bps,
            "buy_cvd_slope_norm_min": sp.buy_cvd_slope_norm_min,
            "sell_oi_slope_max_bps": sp.sell_oi_slope_max_bps,
            "sell_cvd_slope_norm_max": sp.sell_cvd_slope_norm_max,
        }

        if sp.trend_method == "off":
            return True, diag
        if sp.trend_mode == "none":
            diag["steps_ok"] = True
            diag["slope_ok"] = True
            return True, diag

        ok_steps = self._trend_steps_pass(side=side, oi_points=oi_points, cvd_points=cvd_points, sp=sp)
        diag["steps_ok"] = bool(ok_steps)
        if sp.trend_method == "steps" and not ok_steps:
            return False, diag

        ok_slope, oi_bps, cvd_norm, cvd_med = self._trend_slope_pass(
            side=side, oi_points=oi_points, cvd_points=cvd_points, sp=sp
        )
        diag["slope_ok"] = bool(ok_slope)
        diag["oi_slope_bps_per_bar"] = oi_bps
        diag["cvd_slope_norm"] = cvd_norm
        diag["cvd_med_abs_delta"] = cvd_med

        if sp.trend_method == "slope" and not ok_slope:
            return False, diag

        if sp.trend_method == "both":
            return (ok_steps and ok_slope), diag

        return True, diag

    # -------------------------
    # main
    # -------------------------

    def run(
        self,
        *,
        storage: PostgreSQLStorage,
        exchange_id: int,
        interval: str,
        params: Dict[str, Any],
    ) -> List[ScreenerSignal]:
        sp = _parse_params(params)

        interval = str(interval or sp.intervals[0]).strip()
        td = interval_to_timedelta(interval)
        if td.total_seconds() <= 0:
            raise ValueError(f"Invalid interval: {interval}")

        symbols_map = storage.fetch_symbols_map(exchange_id=int(exchange_id), only_active=True)
        symbols = list(symbols_map.items())
        if sp.max_symbols and sp.max_symbols > 0:
            symbols = symbols[: int(sp.max_symbols)]

        now_ts = _utc_now()

        out: List[ScreenerSignal] = []
        dbg_rows: List[Tuple[str, float, str]] = []

        mode = (sp.signal_mode or "both").strip().lower()
        dbg_window_minutes = sp.buy_window_minutes if mode != "sell" else sp.sell_window_minutes

        # funding bulk
        funding_map: Dict[int, Dict[str, Any]] = {}
        if sp.enable_funding and hasattr(storage, "fetch_next_funding_bulk"):
            try:
                symbol_ids = [int(sid) for _, sid in symbols]
                funding_map = storage.fetch_next_funding_bulk(
                    exchange_id=int(exchange_id),
                    symbol_ids=symbol_ids,
                    as_of=now_ts,
                ) or {}
            except Exception:
                funding_map = {}
                log.warning("[PUMP] funding snapshot load failed (fetch_next_funding_bulk)", exc_info=True)
        elif sp.enable_funding:
            log.warning("[PUMP] enable_funding=true, but storage has no fetch_next_funding_bulk()")

        for symbol, symbol_id in symbols:
            symbol = str(symbol or "").strip()
            if not symbol:
                continue
            try:
                symbol_id = int(symbol_id)
            except Exception:
                continue

            # funding snapshot for symbol
            f = funding_map.get(int(symbol_id), {}) if funding_map else {}
            f_rate = f.get("funding_rate")
            f_time = f.get("funding_time")
            f_is_next = bool(f.get("is_next", False))
            f_is_est = bool(f.get("is_estimated", False))

            f_pct: Optional[float] = None
            try:
                if f_rate is not None:
                    f_pct = float(f_rate) * 100.0
            except Exception:
                f_pct = None

            candles = storage.fetch_candles_window(
                exchange_id=int(exchange_id),
                symbol_id=int(symbol_id),
                interval=interval,
                center_ts=now_ts,
                lookback=int(sp.scan_lookback_bars),
                lookforward=0,
            )
            if not candles or len(candles) < 5:
                continue

            last_close = _to_float(candles[-1].get("close"))
            if last_close < sp.min_price or last_close > sp.max_price:
                continue

            if mode == "buy":
                buy_sig = self._check_buy(
                    storage=storage,
                    exchange_id=int(exchange_id),
                    symbol_id=int(symbol_id),
                    symbol=symbol,
                    interval=interval,
                    candles=candles,
                    sp=sp,
                    td=td,
                    funding_pct=f_pct,
                    funding_rate=f_rate,
                    funding_time=f_time,
                    funding_is_next=f_is_next,
                    funding_is_estimated=f_is_est,
                )
                if buy_sig is not None:
                    out.append(buy_sig)
                else:
                    if sp.debug and sp.debug_top > 0:
                        pct = self._debug_pct(candles=candles, td=td, window_minutes=dbg_window_minutes)
                        dbg_rows.append((symbol, pct, f"no-signal mode=buy pct={pct:.2f}%"))
                continue

            if mode == "sell":
                sell_sig = self._check_sell(
                    storage=storage,
                    exchange_id=int(exchange_id),
                    symbol_id=int(symbol_id),
                    symbol=symbol,
                    interval=interval,
                    candles=candles,
                    sp=sp,
                    td=td,
                    funding_pct=f_pct,
                    funding_rate=f_rate,
                    funding_time=f_time,
                    funding_is_next=f_is_next,
                    funding_is_estimated=f_is_est,
                )
                if sell_sig is not None:
                    out.append(sell_sig)
                else:
                    if sp.debug and sp.debug_top > 0:
                        pct = self._debug_pct(candles=candles, td=td, window_minutes=dbg_window_minutes)
                        dbg_rows.append((symbol, pct, f"no-signal mode=sell pct={pct:.2f}%"))
                continue

            # BOTH
            buy_sig = self._check_buy(
                storage=storage,
                exchange_id=int(exchange_id),
                symbol_id=int(symbol_id),
                symbol=symbol,
                interval=interval,
                candles=candles,
                sp=sp,
                td=td,
                funding_pct=f_pct,
                funding_rate=f_rate,
                funding_time=f_time,
                funding_is_next=f_is_next,
                funding_is_estimated=f_is_est,
            )
            if buy_sig is not None:
                out.append(buy_sig)
                continue

            sell_sig = self._check_sell(
                storage=storage,
                exchange_id=int(exchange_id),
                symbol_id=int(symbol_id),
                symbol=symbol,
                interval=interval,
                candles=candles,
                sp=sp,
                td=td,
                funding_pct=f_pct,
                funding_rate=f_rate,
                funding_time=f_time,
                funding_is_next=f_is_next,
                funding_is_estimated=f_is_est,
            )
            if sell_sig is not None:
                out.append(sell_sig)
                continue

            if sp.debug and sp.debug_top > 0:
                pct = self._debug_pct(candles=candles, td=td, window_minutes=sp.buy_window_minutes)
                dbg_rows.append((symbol, pct, f"no-signal mode=both pct={pct:.2f}%"))

        if sp.debug and sp.debug_top > 0 and dbg_rows:
            dbg_rows.sort(key=lambda x: x[1], reverse=True)
            log.info("=== PUMP DEBUG TOP %d interval=%s mode=%s ===", sp.debug_top, interval, mode)
            for sym, sc, why in dbg_rows[: int(sp.debug_top)]:
                log.info("%s score=%.2f %s", sym, sc, why)

        return out

    def _debug_pct(self, *, candles: List[Dict[str, Any]], td: timedelta, window_minutes: int) -> float:
        try:
            if not candles:
                return 0.0
            bar_sec = max(1.0, td.total_seconds())
            n = max(1, int(round(float(window_minutes) * 60.0 / bar_sec)))
            n = min(n, len(candles))
            start_open = _to_float(candles[-n].get("open"))
            last_close = _to_float(candles[-1].get("close"))
            return _pct_change(start_open, last_close)
        except Exception:
            return 0.0

    # -------------------------
    # BUY
    # -------------------------

    def _check_buy(
        self,
        *,
        storage: PostgreSQLStorage,
        exchange_id: int,
        symbol_id: int,
        symbol: str,
        interval: str,
        candles: List[Dict[str, Any]],
        sp: ScrParams,
        td: timedelta,
        funding_pct: Optional[float],
        funding_rate: Any,
        funding_time: Any,
        funding_is_next: bool,
        funding_is_estimated: bool,
    ) -> Optional[ScreenerSignal]:
        bar_sec = max(1.0, td.total_seconds())

        window_bars = int((sp.buy_window_minutes * 60 + bar_sec - 1) // bar_sec)
        window_bars = max(1, min(window_bars, len(candles)))
        window_minutes_real = int(round(window_bars * td.total_seconds() / 60.0))

        start_c = candles[-window_bars]
        end_c = candles[-1]

        start_open = _to_float(start_c.get("open"))
        end_close = _to_float(end_c.get("close"))

        start_ts = _as_dt(start_c.get("ts"))
        end_ts = _as_dt(end_c.get("ts"))
        if start_ts is None or end_ts is None:
            return None

        end_base_idx = max(0, len(candles) - window_bars)
        base_avg = _base_avg_before_window(
            candles,
            end_exclusive_idx=end_base_idx,
            lookback_bars=int(sp.buy_base_lookback_bars),
        )
        if base_avg is None or base_avg <= 0:
            base_avg = start_open if start_open > 0 else end_close
            if base_avg <= 0:
                return None

        threshold = float(base_avg) * (1.0 + float(sp.buy_price_change_pct) / 100.0)

        # must cross within window
        if not (start_open < threshold <= end_close):
            return None

        # funding filter (BUY)
        if not self._funding_pass(side="BUY", funding_pct=funding_pct, sp=sp):
            return None

        pct_from_avg = _pct_change(float(base_avg), float(end_close))
        pct_in_window = _pct_change(float(start_open), float(end_close))

        prev_ts = _as_dt(candles[-2].get("ts")) if len(candles) >= 2 else None
        if prev_ts is None:
            prev_ts = start_ts

        oi_delta = None
        cvd_delta = None
        oi_points = None
        cvd_points = None
        oi_used_itv = None
        cvd_used_itv = None

        if sp.enable_oi:
            oi_delta, oi_points, oi_used_itv = self._calc_oi_info(
                storage=storage,
                exchange_id=exchange_id,
                symbol_id=symbol_id,
                candle_interval=interval,
                start_ts=prev_ts,
                end_ts=end_ts,
                sp=sp,
            )
            if oi_delta is None or oi_delta < sp.buy_min_oi_delta:
                return None

        if sp.enable_cvd:
            cvd_delta, cvd_points, cvd_used_itv = self._calc_cvd_info(
                storage=storage,
                exchange_id=exchange_id,
                symbol_id=symbol_id,
                candle_interval=interval,
                start_ts=prev_ts,
                end_ts=end_ts,
                sp=sp,
            )
            if cvd_delta is None or cvd_delta < sp.buy_min_cvd_delta:
                return None

        ok_trend, trend_diag = self._trend_pass(side="BUY", oi_points=oi_points, cvd_points=cvd_points, sp=sp)
        if not ok_trend:
            return None

        signal_ts = end_ts + td
        sl, tp = self._calc_sl_tp("BUY", end_close, sp)

        ctx: Dict[str, Any] = {
            "pattern": "BUY_FROM_AVG_CROSS",
            "interval": interval,
            "signal_mode": "buy",
            "buy_window_minutes": int(sp.buy_window_minutes),
            "window_minutes": int(window_minutes_real),

            "buy_base_lookback_bars": int(sp.buy_base_lookback_bars),
            "base_lookback_bars": int(sp.buy_base_lookback_bars),
            "base_avg_price": float(base_avg),
            "threshold_price": float(threshold),

            "pump_pct": float(pct_from_avg),
            "pct_in_window": float(pct_in_window),

            "confirm_ts": end_ts,
            "price_change_pct": float(pct_from_avg),
            "pump_start_ts": start_ts,
            "pump_end_ts": end_ts,

            "oi_delta": oi_delta,
            "cvd_delta": cvd_delta,
            "oi_interval_used": oi_used_itv,
            "cvd_interval_used": cvd_used_itv,

            "oi_points": oi_points,
            "cvd_points": cvd_points,
            "trend": trend_diag,

            "funding_rate": funding_rate,
            "funding_pct": funding_pct,
            "funding_time": funding_time,
            "funding_is_next": bool(funding_is_next),
            "funding_is_estimated": bool(funding_is_estimated),
        }

        reason_parts = [
            f"cross avg({int(sp.buy_base_lookback_bars)}) +{sp.buy_price_change_pct:.2f}% in ~{window_minutes_real}m",
            f"avg={base_avg:.6g}->close={end_close:.6g} ({pct_from_avg:.2f}%)",
        ]
        if sp.trend_method != "off":
            reason_parts.append(f"trend={sp.trend_method}/{sp.trend_mode}")

        conf = min(1.0, max(0.0, float(pct_from_avg) / max(1e-9, float(sp.buy_price_change_pct))))

        return ScreenerSignal(
            symbol_id=symbol_id,
            symbol=symbol,
            signal_ts=signal_ts,
            side="BUY",
            entry_price=end_close,
            stop_loss=sl,
            take_profit=tp,
            confidence=conf,
            score=float(pct_from_avg),
            reason=" | ".join(reason_parts),
            context=ctx,
        )

    # -------------------------
    # SELL
    # -------------------------

    def _check_sell(
        self,
        *,
        storage: PostgreSQLStorage,
        exchange_id: int,
        symbol_id: int,
        symbol: str,
        interval: str,
        candles: List[Dict[str, Any]],
        sp: ScrParams,
        td: timedelta,
        funding_pct: Optional[float],
        funding_rate: Any,
        funding_time: Any,
        funding_is_next: bool,
        funding_is_estimated: bool,
    ) -> Optional[ScreenerSignal]:
        bar_sec = max(1.0, td.total_seconds())

        pump_bars = int((sp.sell_window_minutes * 60 + bar_sec - 1) // bar_sec)
        pump_bars = max(1, pump_bars)
        confirm_n = max(1, int(sp.sell_confirm_lookforward))
        pump_minutes_real = int(round(pump_bars * td.total_seconds() / 60.0))

        if len(candles) < (pump_bars + confirm_n + 2):
            return None

        last_idx = len(candles) - 1

        for pump_end_idx in range(last_idx - confirm_n, pump_bars - 1, -1):
            pump_start_idx = pump_end_idx - pump_bars + 1
            if pump_start_idx < 0:
                break

            pump_start = candles[pump_start_idx]
            pump_end = candles[pump_end_idx]

            pump_start_open = _to_float(pump_start.get("open"))
            pump_end_close = _to_float(pump_end.get("close"))

            base_avg = _base_avg_before_window(
                candles,
                end_exclusive_idx=pump_start_idx,
                lookback_bars=int(sp.sell_base_lookback_bars),
            )
            if base_avg is None or base_avg <= 0:
                base_avg = pump_start_open if pump_start_open > 0 else pump_end_close
                if base_avg <= 0:
                    continue

            threshold = float(base_avg) * (1.0 + float(sp.sell_pump_pct) / 100.0)
            if not (pump_start_open < threshold <= pump_end_close):
                continue

            if not self._funding_pass(side="SELL", funding_pct=funding_pct, sp=sp):
                continue

            pct_from_avg = _pct_change(float(base_avg), float(pump_end_close))
            pct_in_window = _pct_change(float(pump_start_open), float(pump_end_close))

            conf_slice = candles[pump_end_idx + 1: pump_end_idx + 1 + confirm_n]

            best_red = None
            best_close = None
            best_ts = None
            best_idx = None

            for j, c in enumerate(conf_slice):
                oo = _to_float(c.get("open"))
                cc = _to_float(c.get("close"))
                ts = _as_dt(c.get("ts"))

                if cc < oo:
                    abs_idx = pump_end_idx + 1 + j

                    if best_red is None:
                        best_red = c
                        best_close = cc
                        best_ts = ts
                        best_idx = abs_idx
                        continue

                    if best_close is None or cc > float(best_close):
                        best_red = c
                        best_close = cc
                        best_ts = ts
                        best_idx = abs_idx
                        continue

                    if best_close is not None and abs(cc - float(best_close)) < 1e-12 and ts is not None and best_ts is not None:
                        if ts < best_ts:
                            best_red = c
                            best_close = cc
                            best_ts = ts
                            best_idx = abs_idx

            if best_red is None or best_idx is None:
                continue

            red_candle = best_red
            red_idx = int(best_idx)

            pump_start_ts = _as_dt(pump_start.get("ts"))
            pump_end_ts = _as_dt(pump_end.get("ts"))
            red_ts = _as_dt(red_candle.get("ts"))
            if pump_start_ts is None or pump_end_ts is None or red_ts is None:
                continue

            prev_ts = _as_dt(candles[red_idx - 1].get("ts")) if red_idx - 1 >= 0 else None
            if prev_ts is None:
                prev_ts = pump_end_ts

            oi_delta = None
            cvd_delta = None
            oi_points = None
            cvd_points = None
            oi_used_itv = None
            cvd_used_itv = None

            if sp.enable_oi:
                oi_delta, oi_points, oi_used_itv = self._calc_oi_info(
                    storage=storage,
                    exchange_id=exchange_id,
                    symbol_id=symbol_id,
                    candle_interval=interval,
                    start_ts=prev_ts,
                    end_ts=red_ts,
                    sp=sp,
                )
                if oi_delta is None or oi_delta > sp.sell_max_oi_delta:
                    continue

            if sp.enable_cvd:
                cvd_delta, cvd_points, cvd_used_itv = self._calc_cvd_info(
                    storage=storage,
                    exchange_id=exchange_id,
                    symbol_id=symbol_id,
                    candle_interval=interval,
                    start_ts=prev_ts,
                    end_ts=red_ts,
                    sp=sp,
                )
                if cvd_delta is None or cvd_delta > sp.sell_max_cvd_delta:
                    continue

            ok_trend, trend_diag = self._trend_pass(side="SELL", oi_points=oi_points, cvd_points=cvd_points, sp=sp)
            if not ok_trend:
                continue

            signal_ts = red_ts + td
            entry_price = _to_float(red_candle.get("close"))
            sl, tp = self._calc_sl_tp("SELL", entry_price, sp)

            ctx: Dict[str, Any] = {
                "pattern": "SELL_FROM_AVG_PUMP_REVERSAL",
                "interval": interval,
                "signal_mode": "sell",

                "sell_window_minutes": int(sp.sell_window_minutes),
                "window_minutes": int(pump_minutes_real),

                "sell_base_lookback_bars": int(sp.sell_base_lookback_bars),
                "base_lookback_bars": int(sp.sell_base_lookback_bars),
                "base_avg_price": float(base_avg),
                "threshold_price": float(threshold),

                "pump_pct": float(pct_from_avg),
                "pct_in_window": float(pct_in_window),

                "confirm_ts": red_ts,
                "sell_pump_pct": float(sp.sell_pump_pct),
                "sell_confirm_lookforward": int(confirm_n),

                "price_change_pct": float(pct_from_avg),
                "pump_start_ts": pump_start_ts,
                "pump_end_ts": pump_end_ts,

                "confirm_red_ts": red_ts,
                "confirm_red_close": float(entry_price) if entry_price is not None else None,

                "oi_delta": oi_delta,
                "cvd_delta": cvd_delta,
                "oi_interval_used": oi_used_itv,
                "cvd_interval_used": cvd_used_itv,

                "oi_points": oi_points,
                "cvd_points": cvd_points,
                "trend": trend_diag,

                "funding_rate": funding_rate,
                "funding_pct": funding_pct,
                "funding_time": funding_time,
                "funding_is_next": bool(funding_is_next),
                "funding_is_estimated": bool(funding_is_estimated),
            }

            reason_parts = [
                f"pump vs avg({int(sp.sell_base_lookback_bars)}) +{sp.sell_pump_pct:.2f}% in ~{pump_minutes_real}m",
                f"avg={base_avg:.6g}->pump_close={pump_end_close:.6g} ({pct_from_avg:.2f}%)",
                f"red within {confirm_n} (max close)",
            ]
            if sp.trend_method != "off":
                reason_parts.append(f"trend={sp.trend_method}/{sp.trend_mode}")

            conf = min(1.0, max(0.0, float(pct_from_avg) / max(1e-9, float(sp.sell_pump_pct))))

            return ScreenerSignal(
                symbol_id=symbol_id,
                symbol=symbol,
                signal_ts=signal_ts,
                side="SELL",
                entry_price=entry_price,
                stop_loss=sl,
                take_profit=tp,
                confidence=conf,
                score=float(pct_from_avg),
                reason=" | ".join(reason_parts),
                context=ctx,
            )

        return None

    # -------------------------
    # SL / TP
    # -------------------------

    def _calc_sl_tp(self, side: str, entry: float, sp: ScrParams) -> Tuple[Optional[float], Optional[float]]:
        sl = None
        tp = None

        if entry and sp.stop_loss_pct and sp.stop_loss_pct > 0:
            if side.upper() == "BUY":
                sl = float(entry) * (1.0 - float(sp.stop_loss_pct) / 100.0)
            else:
                sl = float(entry) * (1.0 + float(sp.stop_loss_pct) / 100.0)

        if entry and sp.take_profit_pct and sp.take_profit_pct > 0:
            if side.upper() == "BUY":
                tp = float(entry) * (1.0 + float(sp.take_profit_pct) / 100.0)
            else:
                tp = float(entry) * (1.0 - float(sp.take_profit_pct) / 100.0)

        return sl, tp
