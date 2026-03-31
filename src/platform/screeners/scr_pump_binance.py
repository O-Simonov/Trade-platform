from __future__ import annotations

import copy
import logging
import math
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from src.platform.core.utils.candles import interval_to_timedelta
from src.platform.data.storage.postgres.storage import PostgreSQLStorage

log = logging.getLogger("screeners.scr_pump_binance")


# -------------------------
# small utils
# -------------------------

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_dt(v: Any) -> Optional[datetime]:
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


def _to_opt_float(x: Any) -> Optional[float]:
    if x in (None, "", "null"):
        return None
    try:
        return float(x)
    except Exception:
        return None


def _to_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(x)
    except Exception:
        return default


def _pct_change(start: float, end: float) -> float:
    if start is None or end is None or float(start) == 0.0:
        return 0.0
    return (float(end) / float(start) - 1.0) * 100.0


def _safe_mean(vals: List[float]) -> Optional[float]:
    xs = [float(v) for v in vals if v is not None]
    if not xs:
        return None
    return sum(xs) / float(len(xs))


def _median(xs: List[float]) -> Optional[float]:
    ys = [float(x) for x in xs if x is not None]
    if not ys:
        return None
    ys.sort()
    n = len(ys)
    mid = n // 2
    if n % 2 == 1:
        return ys[mid]
    return 0.5 * (ys[mid - 1] + ys[mid])


def _linreg_slope(y: List[float]) -> Optional[float]:
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
    try:
        def _key(r: Dict[str, Any]) -> datetime:
            dt = _as_dt(r.get("ts"))
            return dt if dt is not None else datetime.min.replace(tzinfo=timezone.utc)
        return sorted(series, key=_key)
    except Exception:
        return series


def _pick_series_bounds(series: List[Dict[str, Any]], *, start_ts: datetime, end_ts: datetime, value_key: str) -> Optional[Tuple[float, float]]:
    if not series:
        return None
    start_val = None
    end_val = None
    start_ts2 = _as_dt(start_ts) or start_ts
    end_ts2 = _as_dt(end_ts) or end_ts
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


def _last_n_values(series: List[Dict[str, Any]], *, end_ts: datetime, value_key: str, n: int) -> Optional[List[float]]:
    if not series or n <= 0:
        return None
    end_ts2 = _as_dt(end_ts) or end_ts
    out: List[float] = []
    for r in reversed(series):
        ts = _as_dt(r.get("ts"))
        if ts is None or ts > end_ts2:
            continue
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


def _trend_steps_ok(values: List[float], *, direction: str, eps: float = 0.0, strict: bool = True) -> bool:
    if not values or len(values) < 2:
        return False
    for i in range(1, len(values)):
        d = float(values[i]) - float(values[i - 1])
        if direction == "up":
            if strict and not (d > eps):
                return False
            if not strict and not (d >= eps):
                return False
        elif direction == "down":
            if strict and not (d < -eps):
                return False
            if not strict and not (d <= -eps):
                return False
    return True


def _true_range(curr: Dict[str, Any], prev_close: Optional[float]) -> float:
    h = _to_float(curr.get("high"))
    l = _to_float(curr.get("low"))
    c0 = prev_close if prev_close is not None else _to_float(curr.get("open"))
    return max(h - l, abs(h - c0), abs(l - c0))


def _calc_atr(candles: List[Dict[str, Any]], lookback: int) -> Optional[float]:
    if not candles or len(candles) < 2:
        return None
    trs: List[float] = []
    start = max(1, len(candles) - max(2, int(lookback)))
    prev_close = _to_float(candles[start - 1].get("close"))
    for i in range(start, len(candles)):
        c = candles[i]
        tr = _true_range(c, prev_close)
        if tr > 0:
            trs.append(tr)
        prev_close = _to_float(c.get("close"))
    return _safe_mean(trs)


def _deep_merge_dict(base: dict, override: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge_dict(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def _dataclass_to_dict(obj: Any) -> Any:
    if is_dataclass(obj):
        return {f.name: _dataclass_to_dict(getattr(obj, f.name)) for f in fields(obj)}
    if isinstance(obj, dict):
        return {k: _dataclass_to_dict(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_dataclass_to_dict(v) for v in obj]
    return copy.deepcopy(obj)


def _update_dataclass_from_dict(obj: Any, patch: dict) -> Any:
    for f in fields(obj):
        if f.name not in patch:
            continue
        current = getattr(obj, f.name)
        val = patch[f.name]
        if is_dataclass(current) and isinstance(val, dict):
            _update_dataclass_from_dict(current, val)
        else:
            setattr(obj, f.name, copy.deepcopy(val))
    return obj


def _bar_quote_volume(c: Dict[str, Any]) -> float:
    for key in ("quote_volume", "volume_quote", "turnover", "quote_turnover"):
        if key in c and c.get(key) is not None:
            return _to_float(c.get(key), 0.0)
    close = _to_float(c.get("close"), 0.0)
    vol = _to_float(c.get("volume"), 0.0)
    return close * vol


def _bars_per_day(interval: Optional[str]) -> int:
    try:
        td = interval_to_timedelta(str(interval or '5m'))
        sec = max(60.0, float(td.total_seconds()))
        return max(1, int(round(86400.0 / sec)))
    except Exception:
        return 288


def _base_avg_before_window(candles: List[Dict[str, Any]], *, end_exclusive_idx: int, lookback_bars: int) -> Optional[float]:
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


def _fetch_batch_candles(
    *,
    storage: PostgreSQLStorage,
    exchange_id: int,
    symbol_ids: List[int],
    interval: str,
    start_ts: datetime,
    end_ts: datetime,
) -> Dict[int, List[Dict[str, Any]]]:
    ids = [int(x) for x in symbol_ids if x is not None]
    if not ids:
        return {}
    q = """
        SELECT symbol_id, open_time AS ts, open, high, low, close, volume, quote_volume
        FROM candles
        WHERE exchange_id = %s
          AND interval = %s
          AND symbol_id = ANY(%s)
          AND open_time >= %s
          AND open_time <= %s
        ORDER BY symbol_id, open_time
    """
    out: Dict[int, List[Dict[str, Any]]] = {}
    with storage.pool.connection() as con:
        with con.cursor() as cur:
            cur.execute(q, (int(exchange_id), str(interval), ids, start_ts, end_ts))
            rows = cur.fetchall() or []
            cols = [d[0] for d in cur.description]
    for row in rows:
        rec = dict(zip(cols, row))
        sid = _to_int(rec.get('symbol_id'), 0)
        ts = _as_dt(rec.get('ts')) or _as_dt(rec.get('open_time'))
        if ts is not None:
            rec['ts'] = ts
        out.setdefault(sid, []).append(rec)
    return out


def _fetch_batch_open_interest_latest(
    *,
    storage: PostgreSQLStorage,
    exchange_id: int,
    symbol_ids: List[int],
    interval: str,
    as_of: datetime,
) -> Dict[int, Optional[float]]:
    ids = [int(x) for x in symbol_ids if x is not None]
    if not ids:
        return {}
    q = """
        WITH ranked AS (
            SELECT
                symbol_id,
                open_interest,
                ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY ts DESC) AS rn
            FROM open_interest
            WHERE exchange_id = %s
              AND interval = %s
              AND symbol_id = ANY(%s)
              AND ts <= %s
        )
        SELECT symbol_id, open_interest
        FROM ranked
        WHERE rn = 1
    """
    out: Dict[int, Optional[float]] = {int(sid): None for sid in ids}
    with storage.pool.connection() as con:
        with con.cursor() as cur:
            cur.execute(q, (int(exchange_id), str(interval), ids, as_of))
            for symbol_id, open_interest in cur.fetchall() or []:
                out[int(symbol_id)] = _to_opt_float(open_interest)
    return out


# -------------------------
# data model
# -------------------------

@dataclass
class ScreenerSignal:
    symbol_id: int
    symbol: str
    signal_ts: datetime
    side: str
    entry_price: float
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    exit_price: Optional[float] = None
    confidence: Optional[float] = None
    score: Optional[float] = None
    reason: Optional[str] = None
    context: Optional[Dict[str, Any]] = None


@dataclass
class PumpUniverseConfig:
    enabled: bool = True
    quote_asset: str = "USDT"
    futures_only: bool = True
    active_only: bool = True
    max_symbols: int = 0
    symbols_whitelist: List[str] = field(default_factory=list)
    symbols_blacklist: List[str] = field(default_factory=list)
    min_price: float = 1e-8
    max_price: float = 100000.0
    min_tick_size: float = 0.0


@dataclass
class PumpRuntimeConfig:
    signal_mode: str = "both"
    max_signal_age_minutes: int = 30
    cooldown_minutes: int = 30
    max_signals_per_cycle: int = 20
    sort_by: str = "score"
    descending: bool = True
    require_fresh_last_bar: bool = False
    use_closed_bar_only: bool = True
    enable_debug_logs: bool = False
    debug_top: int = 3


@dataclass
class PumpWindowsConfig:
    scan_lookback_bars: int = 200
    base_lookback_bars: int = 24
    confirm_lookback_bars: int = 6
    oi_lookback_bars: int = 12
    cvd_lookback_bars: int = 12
    funding_lookback_points: int = 1
    atr_lookback_bars: int = 14


@dataclass
class PumpLiquidityConfig:
    min_quote_volume_24h: float = 0.0
    min_base_quote_volume: float = 0.0
    min_last_bar_quote_volume: float = 0.0
    min_notional_oi: float = 0.0
    max_spread_bps: float = 0.0
    skip_low_liquidity: bool = False


@dataclass
class PumpBaseQualityConfig:
    enabled: bool = False
    base_range_max_pct: float = 100.0
    base_noise_max_pct: float = 100.0
    base_trend_abs_max_pct: float = 100.0
    base_volume_max_mult: float = 999.0
    base_green_red_imbalance_max: float = 1.0
    require_calm_base: bool = False


@dataclass
class PumpVolumeFiltersConfig:
    enabled: bool = True
    min_volume_mult: float = 1.0
    min_volume_spike_mult: float = 1.0
    min_volume_acceleration: float = 1.0
    require_volume_confirmation: bool = False
    use_median_base_volume: bool = True
    max_anomaly_volume_mult: float = 999.0


@dataclass
class PumpBuyConfig:
    enabled: bool = True
    enable_early: bool = True
    enable_confirmed: bool = True
    window_minutes: int = 5
    min_price_change_pct: float = 2.0
    min_price_change_atr: float = 0.7
    max_price_extension_pct: float = 10.0
    min_green_candles: int = 1
    require_last_bar_green: bool = False
    require_close_near_high: bool = False
    min_volume_mult: float = 1.0
    min_volume_spike_mult: float = 1.0
    min_volume_acceleration: float = 1.0
    min_oi_delta_pct: float = 0.0
    min_oi_value_delta_pct: float = 0.0
    min_cvd_delta_pct: float = 0.0
    require_oi_confirmation: bool = False
    require_cvd_confirmation: bool = False
    oi_up_ratio_min: float = 0.0
    oi_drawdown_from_peak_min_pct: float = -100.0
    breakout_lookback_bars: int = 20
    require_breakout: bool = False
    breakout_buffer_pct: float = 0.0
    allow_retest_entry: bool = True
    min_signal_score: float = 0.0
    early_min_signal_score: float = 0.0
    confirmed_min_signal_score: float = 0.0


@dataclass
class PumpSellConfig:
    enabled: bool = True
    enable_early: bool = True
    enable_confirmed: bool = True
    window_minutes: int = 15
    min_pump_pct: float = 2.5
    min_pump_atr: float = 0.7
    confirm_lookforward_bars: int = 3
    reversal_from_peak_pct: float = 0.2
    min_red_body_pct: float = 0.1
    min_upper_wick_pct: float = 0.0
    require_red_confirm: bool = True
    require_lower_high: bool = False
    require_close_below_prev_close: bool = False
    confirm_close_position_max: float = 1.0
    min_volume_mult: float = 1.0
    min_volume_spike_mult: float = 1.0
    require_volume_confirmation: bool = False
    max_oi_delta_pct: float = 0.0
    max_cvd_delta_pct: float = 0.0
    allow_flat_oi: bool = True
    require_oi_exhaustion: bool = False
    require_cvd_weakness: bool = False
    min_signal_score: float = 0.0
    early_min_signal_score: float = 0.0
    confirmed_min_signal_score: float = 0.0


@dataclass
class PumpTrendFiltersConfig:
    enabled: bool = False
    trend_method: str = "off"
    trend_mode: str = "both"
    confirm_trend_bars: int = 3
    slope_bars: int = 4
    oi_eps_pct: float = 0.0
    cvd_eps_pct: float = 0.0
    buy_oi_slope_min_bps: float = 0.0
    buy_cvd_slope_min_bps: float = 0.0
    sell_oi_slope_max_bps: float = 0.0
    sell_cvd_slope_max_bps: float = 0.0


@dataclass
class PumpFundingConfig:
    enabled: bool = False
    mode: str = "soft"
    buy_max_funding_pct: Optional[float] = None
    sell_min_funding_pct: Optional[float] = None
    sell_prefer_positive_funding: bool = True
    extreme_positive_funding_pct: Optional[float] = None
    extreme_negative_funding_pct: Optional[float] = None


@dataclass
class PumpScoringConfig:
    enabled: bool = True
    clamp_each_component_0_100: bool = True
    normalize_total_to_100: bool = True
    weight_price: float = 0.25
    weight_volume: float = 0.20
    weight_oi: float = 0.20
    weight_cvd: float = 0.10
    weight_structure: float = 0.15
    weight_funding: float = 0.05
    weight_base_quality: float = 0.05
    weak_score_threshold: float = 35.0
    medium_score_threshold: float = 50.0
    strong_score_threshold: float = 65.0
    very_strong_score_threshold: float = 80.0


@dataclass
class PumpOverheatingConfig:
    enabled: bool = False
    max_price_extension_pct: float = 999.0
    max_oi_spike_pct: float = 999.0
    max_volume_spike_mult: float = 999.0
    reject_vertical_moves: bool = False
    vertical_move_atr_threshold: float = 999.0


@dataclass
class PumpAntiNoiseConfig:
    enabled: bool = False
    min_body_to_range_ratio: float = 0.0
    max_wick_to_body_ratio: float = 999.0
    max_gap_pct: float = 999.0
    reject_erratic_series: bool = False
    erratic_range_threshold_pct: float = 999.0


@dataclass
class PumpAlertsConfig:
    enabled: bool = True
    channel: str = "telegram"
    send_only_new: bool = True
    include_score: bool = True
    include_context_stats: bool = True
    include_interval: bool = True
    include_profile: bool = True
    include_debug_metrics: bool = False
    max_alerts_per_cycle: int = 10


@dataclass
class PumpPlotsConfig:
    enabled: bool = True
    plot_only_for_sent_signals: bool = True
    plot_only_top_scored: bool = True
    plot_top_n: int = 8
    include_oi_panel: bool = True
    include_cvd_panel: bool = True
    include_volume_panel: bool = True
    include_funding_annotation: bool = True
    image_format: str = "png"
    dpi: int = 120
    compress_images: bool = True


@dataclass
class PumpStorageConfig:
    enabled: bool = True
    store_candidates: bool = False
    store_rejections: bool = False
    store_metrics: bool = True
    deduplicate_before_insert: bool = True


@dataclass
class PumpOptimizerConfig:
    enabled: bool = False
    profile_name: str = "pump_default"
    store_metrics: bool = True
    objective: str = "profit_factor"
    min_trades: int = 20
    min_win_rate_pct: float = 35.0
    min_avg_pnl_pct: float = 0.1
    penalty_for_low_sample: bool = True


@dataclass
class PumpCompatibilityConfig:
    strict_mode: bool = False
    allow_unused_fields: bool = True
    fallback_to_legacy_logic: bool = False


@dataclass
class PumpConfig:
    name: str = "scr_pump_binance"
    exchange: str = "binance"
    market_type: str = "perpetual"
    intervals: List[str] = field(default_factory=lambda: ["5m"])
    universe: PumpUniverseConfig = field(default_factory=PumpUniverseConfig)
    runtime: PumpRuntimeConfig = field(default_factory=PumpRuntimeConfig)
    windows: PumpWindowsConfig = field(default_factory=PumpWindowsConfig)
    liquidity: PumpLiquidityConfig = field(default_factory=PumpLiquidityConfig)
    base_quality: PumpBaseQualityConfig = field(default_factory=PumpBaseQualityConfig)
    volume_filters: PumpVolumeFiltersConfig = field(default_factory=PumpVolumeFiltersConfig)
    buy: PumpBuyConfig = field(default_factory=PumpBuyConfig)
    sell: PumpSellConfig = field(default_factory=PumpSellConfig)
    trend_filters: PumpTrendFiltersConfig = field(default_factory=PumpTrendFiltersConfig)
    funding: PumpFundingConfig = field(default_factory=PumpFundingConfig)
    scoring: PumpScoringConfig = field(default_factory=PumpScoringConfig)
    overheating: PumpOverheatingConfig = field(default_factory=PumpOverheatingConfig)
    anti_noise: PumpAntiNoiseConfig = field(default_factory=PumpAntiNoiseConfig)
    alerts: PumpAlertsConfig = field(default_factory=PumpAlertsConfig)
    plots: PumpPlotsConfig = field(default_factory=PumpPlotsConfig)
    storage: PumpStorageConfig = field(default_factory=PumpStorageConfig)
    optimizer: PumpOptimizerConfig = field(default_factory=PumpOptimizerConfig)
    compatibility: PumpCompatibilityConfig = field(default_factory=PumpCompatibilityConfig)
    per_interval_overrides: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    exchange_id: int = 1
    stop_loss_pct: Optional[float] = None
    take_profit_pct: Optional[float] = None
    keep_plots_days: int = 0
    telegram_enabled: bool = True
    telegram_timezone: str = "Europe/Moscow"
    telegram_extras_enabled: bool = True
    telegram_max_friends: int = 5
    telegram_send_text: bool = True
    telegram_mode: str = "batch"
    telegram_max_signals: int = 50
    telegram_send_plots: bool = True
    telegram_plot_mode: str = "photo"
    telegram_max_plot_signals: int = 30
    telegram_send_plot_followup_text: bool = False
    plots_dir: str = "artifacts/screener_plots"
    plot_lookback: int = 180
    plot_lookforward: int = 30
    debug: bool = False
    debug_top: int = 3
    interval: Optional[str] = None

    @classmethod
    def from_cfg(cls, raw: Dict[str, Any]) -> "PumpConfig":
        raw = copy.deepcopy(raw or {})
        cfg = cls()

        if "params" in raw and isinstance(raw.get("params"), dict):
            raw = _deep_merge_dict({k: v for k, v in raw.items() if k != "params"}, raw["params"])

        # top-level nested blocks
        for name in [
            "universe", "runtime", "windows", "liquidity", "base_quality", "volume_filters",
            "buy", "sell", "trend_filters", "funding", "scoring", "overheating", "anti_noise",
            "alerts", "plots", "storage", "optimizer", "compatibility",
        ]:
            if isinstance(raw.get(name), dict):
                _update_dataclass_from_dict(getattr(cfg, name), raw[name])

        if isinstance(raw.get("intervals"), list) and raw.get("intervals"):
            cfg.intervals = [str(x).strip() for x in raw.get("intervals") if str(x).strip()]
        elif raw.get("interval"):
            cfg.intervals = [str(raw.get("interval")).strip()]

        for k in [
            "name", "exchange", "market_type", "exchange_id", "stop_loss_pct", "take_profit_pct", "keep_plots_days",
            "telegram_enabled", "telegram_timezone", "telegram_extras_enabled", "telegram_max_friends", "telegram_send_text",
            "telegram_mode", "telegram_max_signals", "telegram_send_plots", "telegram_plot_mode", "telegram_max_plot_signals",
            "telegram_send_plot_followup_text", "plots_dir", "plot_lookback", "plot_lookforward", "debug", "debug_top", "interval",
        ]:
            if k in raw:
                setattr(cfg, k, copy.deepcopy(raw[k]))

        if isinstance(raw.get("per_interval_overrides"), dict):
            cfg.per_interval_overrides = copy.deepcopy(raw.get("per_interval_overrides") or {})

        # legacy flat mapping
        cfg.universe.max_symbols = _to_int(raw.get("max_symbols", cfg.universe.max_symbols), cfg.universe.max_symbols)
        cfg.universe.min_price = _to_float(raw.get("min_price", cfg.universe.min_price), cfg.universe.min_price)
        cfg.universe.max_price = _to_float(raw.get("max_price", cfg.universe.max_price), cfg.universe.max_price)
        cfg.runtime.signal_mode = str(raw.get("signal_mode", cfg.runtime.signal_mode) or "both").lower()
        cfg.runtime.max_signal_age_minutes = _to_int(raw.get("max_signal_age_minutes", cfg.runtime.max_signal_age_minutes), cfg.runtime.max_signal_age_minutes)
        cfg.runtime.enable_debug_logs = bool(raw.get("debug", raw.get("enable_debug_logs", cfg.runtime.enable_debug_logs)))
        cfg.runtime.debug_top = _to_int(raw.get("debug_top", cfg.runtime.debug_top), cfg.runtime.debug_top)
        cfg.debug = bool(raw.get("debug", cfg.debug))
        cfg.debug_top = _to_int(raw.get("debug_top", cfg.debug_top), cfg.debug_top)

        cfg.windows.scan_lookback_bars = max(50, _to_int(raw.get("scan_lookback_bars", cfg.windows.scan_lookback_bars), cfg.windows.scan_lookback_bars))
        cfg.windows.base_lookback_bars = max(10, _to_int(raw.get("base_lookback_bars", cfg.windows.base_lookback_bars), cfg.windows.base_lookback_bars))

        # legacy buy/sell fields
        cfg.buy.window_minutes = _to_int(raw.get("buy_window_minutes", cfg.buy.window_minutes), cfg.buy.window_minutes)
        cfg.buy.min_price_change_pct = _to_float(raw.get("buy_price_change_pct", cfg.buy.min_price_change_pct), cfg.buy.min_price_change_pct)
        cfg.buy.min_oi_delta_pct = _to_float(raw.get("buy_min_oi_delta", cfg.buy.min_oi_delta_pct), cfg.buy.min_oi_delta_pct)
        cfg.buy.min_cvd_delta_pct = _to_float(raw.get("buy_min_cvd_delta", cfg.buy.min_cvd_delta_pct), cfg.buy.min_cvd_delta_pct)
        cfg.buy.require_oi_confirmation = bool(raw.get("enable_oi", cfg.buy.require_oi_confirmation))
        cfg.buy.require_cvd_confirmation = bool(raw.get("enable_cvd", cfg.buy.require_cvd_confirmation))

        cfg.sell.window_minutes = _to_int(raw.get("sell_window_minutes", cfg.sell.window_minutes), cfg.sell.window_minutes)
        cfg.sell.min_pump_pct = _to_float(raw.get("sell_pump_pct", cfg.sell.min_pump_pct), cfg.sell.min_pump_pct)
        cfg.sell.confirm_lookforward_bars = _to_int(raw.get("sell_confirm_lookforward", cfg.sell.confirm_lookforward_bars), cfg.sell.confirm_lookforward_bars)
        cfg.sell.max_oi_delta_pct = _to_float(raw.get("sell_max_oi_delta", cfg.sell.max_oi_delta_pct), cfg.sell.max_oi_delta_pct)
        cfg.sell.max_cvd_delta_pct = _to_float(raw.get("sell_max_cvd_delta", cfg.sell.max_cvd_delta_pct), cfg.sell.max_cvd_delta_pct)

        cfg.trend_filters.enabled = cfg.trend_filters.enabled or bool(raw.get("require_trend") or raw.get("require_2step_oi_cvd"))
        if raw.get("trend_method") is not None:
            cfg.trend_filters.trend_method = str(raw.get("trend_method") or "off").lower()
        if raw.get("trend_mode") is not None:
            cfg.trend_filters.trend_mode = str(raw.get("trend_mode") or "both").lower()
        cfg.trend_filters.confirm_trend_bars = _to_int(raw.get("confirm_trend_bars", cfg.trend_filters.confirm_trend_bars), cfg.trend_filters.confirm_trend_bars)
        cfg.trend_filters.slope_bars = _to_int(raw.get("slope_bars", raw.get("trend_slope_points", cfg.trend_filters.slope_bars)), cfg.trend_filters.slope_bars)
        cfg.trend_filters.oi_eps_pct = _to_float(raw.get("oi_eps", cfg.trend_filters.oi_eps_pct), cfg.trend_filters.oi_eps_pct)
        cfg.trend_filters.cvd_eps_pct = _to_float(raw.get("cvd_eps", cfg.trend_filters.cvd_eps_pct), cfg.trend_filters.cvd_eps_pct)
        cfg.trend_filters.buy_oi_slope_min_bps = _to_float(raw.get("buy_oi_slope_min_bps", cfg.trend_filters.buy_oi_slope_min_bps), cfg.trend_filters.buy_oi_slope_min_bps)
        cfg.trend_filters.buy_cvd_slope_min_bps = _to_float(raw.get("buy_cvd_slope_norm_min", cfg.trend_filters.buy_cvd_slope_min_bps), cfg.trend_filters.buy_cvd_slope_min_bps)
        cfg.trend_filters.sell_oi_slope_max_bps = _to_float(raw.get("sell_oi_slope_max_bps", cfg.trend_filters.sell_oi_slope_max_bps), cfg.trend_filters.sell_oi_slope_max_bps)
        cfg.trend_filters.sell_cvd_slope_max_bps = _to_float(raw.get("sell_cvd_slope_norm_max", cfg.trend_filters.sell_cvd_slope_max_bps), cfg.trend_filters.sell_cvd_slope_max_bps)

        # Compatibility for real-world YAML configs:
        # trend_method: ratio -> closest strict implementation is steps
        tm = str(cfg.trend_filters.trend_method or 'off').lower().strip()
        if tm == 'ratio':
            cfg.trend_filters.trend_method = 'steps'
        elif tm not in ('off', 'none', 'steps', 'slope', 'both'):
            cfg.trend_filters.trend_method = 'both' if cfg.trend_filters.enabled else 'off'

        # trend_mode supports two legacy families of values:
        #   soft/hard/off  -> enforcement mode, both OI and CVD are evaluated
        #   oi/cvd/both    -> legacy source selector, enforced as hard filter
        tmode = str(cfg.trend_filters.trend_mode or 'soft').lower().strip()
        if tmode == 'off':
            cfg.trend_filters.enabled = False
            cfg.trend_filters.trend_mode = 'off'
        elif tmode not in ('soft', 'hard', 'oi', 'cvd', 'both'):
            cfg.trend_filters.trend_mode = 'soft' if cfg.trend_filters.enabled else 'off'

        cfg.funding.enabled = bool(raw.get("enable_funding", cfg.funding.enabled or cfg.funding.mode != "off"))
        cfg.funding.buy_max_funding_pct = _to_opt_float(raw.get("buy_funding_max_pct")) if raw.get("buy_funding_max_pct") is not None else cfg.funding.buy_max_funding_pct
        cfg.funding.sell_min_funding_pct = _to_opt_float(raw.get("sell_funding_min_pct")) if raw.get("sell_funding_min_pct") is not None else cfg.funding.sell_min_funding_pct

        return cfg

    def for_interval(self, interval: str) -> "PumpConfig":
        clone = copy.deepcopy(self)
        ov = clone.per_interval_overrides.get(interval) or clone.per_interval_overrides.get(str(interval)) or {}
        if isinstance(ov, dict) and ov:
            _update_dataclass_from_dict(clone, ov)
            for k, v in ov.items():
                if hasattr(clone, k) and not is_dataclass(getattr(clone, k)):
                    setattr(clone, k, copy.deepcopy(v))
        clone.interval = str(interval)
        clone.intervals = [str(interval)]
        clone.debug = bool(clone.debug or clone.runtime.enable_debug_logs)
        clone.debug_top = max(int(clone.debug_top or 0), int(clone.runtime.debug_top or 0))
        return clone

    def as_flat_params(self) -> Dict[str, Any]:
        d = _dataclass_to_dict(self)
        d["interval"] = self.interval or (self.intervals[0] if self.intervals else None)
        d["signal_mode"] = self.runtime.signal_mode
        d["max_signal_age_minutes"] = self.runtime.max_signal_age_minutes
        d["max_symbols"] = self.universe.max_symbols
        d["min_price"] = self.universe.min_price
        d["max_price"] = self.universe.max_price
        d["enable_plots"] = self.plots.enabled
        d["enable_funding"] = self.funding.enabled
        d["enable_oi"] = self.buy.require_oi_confirmation or self.sell.require_oi_exhaustion or self.trend_filters.enabled
        d["enable_cvd"] = self.buy.require_cvd_confirmation or self.sell.require_cvd_weakness or self.trend_filters.enabled
        d["debug"] = self.debug or self.runtime.enable_debug_logs
        d["debug_top"] = max(int(self.debug_top or 0), int(self.runtime.debug_top or 0))
        return d


@dataclass
class PumpCandidate:
    symbol_id: int
    symbol: str
    interval: str
    side: str
    signal_ts: datetime
    entry_price: float
    price_change_pct: Optional[float] = None
    price_change_atr: Optional[float] = None
    pump_pct: Optional[float] = None
    reversal_from_peak_pct: Optional[float] = None
    base_range_pct: Optional[float] = None
    base_noise_pct: Optional[float] = None
    base_trend_pct: Optional[float] = None
    base_volume_mult: Optional[float] = None
    volume_mult: Optional[float] = None
    volume_spike_mult: Optional[float] = None
    volume_acceleration: Optional[float] = None
    oi_delta_pct: Optional[float] = None
    oi_value_delta_pct: Optional[float] = None
    cvd_delta_pct: Optional[float] = None
    funding_pct: Optional[float] = None
    oi_slope_bps: Optional[float] = None
    cvd_slope_bps: Optional[float] = None
    profile: Optional[str] = None
    score: Optional[float] = None
    score_parts: Dict[str, float] = field(default_factory=dict)
    reject_stage: Optional[str] = None
    reject_reason: Optional[str] = None
    context: Dict[str, Any] = field(default_factory=dict)


class ScrPumpBinance:
    name = "scr_pump_binance"
    version = "0.2"

    def _funding_pass(self, *, side: str, funding_pct: Optional[float], cfg: PumpConfig) -> bool:
        mode = str(cfg.funding.mode or "off").lower()
        if not cfg.funding.enabled or mode == "off":
            return True
        if mode != "hard":
            return True
        if funding_pct is None:
            return False
        side = str(side).upper()
        if side == "BUY":
            thr = cfg.funding.buy_max_funding_pct
            if thr is None:
                return True
            return float(funding_pct) <= float(thr)
        if side == "SELL":
            thr = cfg.funding.sell_min_funding_pct
            if thr is None:
                return True
            return float(funding_pct) >= float(thr)
        return True

    def _resolve_signal_ts(self, *, bar_ts: datetime, td: timedelta, cfg: PumpConfig) -> datetime:
        ts = _as_dt(bar_ts) or bar_ts
        if bool(cfg.runtime.use_closed_bar_only):
            return ts + td
        return ts

    def _is_fresh_signal_ts(self, *, signal_ts: datetime, cfg: PumpConfig, now_ts: Optional[datetime] = None) -> bool:
        now_ts2 = _as_dt(now_ts) or _utc_now()
        age_cutoff = now_ts2 - timedelta(minutes=max(1, int(cfg.runtime.max_signal_age_minutes or 1)))
        return signal_ts >= age_cutoff

    def _need_points(self, cfg: PumpConfig) -> int:
        tf = cfg.trend_filters
        steps_pts = max(0, int(tf.confirm_trend_bars)) + 1 if tf.trend_method in ("steps", "both") else 0
        slope_pts = int(tf.slope_bars) if tf.trend_method in ("slope", "both") else 0
        return max(2, steps_pts, slope_pts)

    def _calc_flow_info(self, storage: PostgreSQLStorage, exchange_id: int, symbol_id: int, interval: str, start_ts: datetime, end_ts: datetime, cfg: PumpConfig, kind: str) -> Tuple[Optional[float], Optional[List[float]], Optional[str]]:
        """
        Read OI/CVD strictly in the same interval as the signal candles.

        Important: do not fallback to 5m/15m/1h here. Mixing intervals makes
        both calculations and diagnostics inconsistent with the candle timeframe.
        If data for the requested interval is absent in DB, we return no flow data
        and let higher-level logic decide how strict to be.
        """
        need_pts = self._need_points(cfg)
        try:
            td = interval_to_timedelta(interval)
            span_sec = max(0.0, (end_ts - start_ts).total_seconds())
            bars = max(30, min(1200, int(span_sec // max(1.0, td.total_seconds())) + 30))
            bars = max(bars, need_pts + 20)
            if kind == "oi":
                series = storage.fetch_open_interest_window(
                    exchange_id=int(exchange_id),
                    symbol_id=int(symbol_id),
                    interval=str(interval),
                    center_ts=end_ts,
                    lookback=bars,
                    lookforward=0,
                ) or []
                value_key = "open_interest"
            else:
                series = storage.fetch_cvd_window(
                    exchange_id=int(exchange_id),
                    symbol_id=int(symbol_id),
                    interval=str(interval),
                    center_ts=end_ts,
                    lookback=bars,
                    lookforward=0,
                ) or []
                value_key = "cvd_quote"
            if not series:
                return None, None, str(interval)
            series = _sort_series_by_ts(series)
            bounds = _pick_series_bounds(series, start_ts=start_ts, end_ts=end_ts, value_key=value_key)
            pts = _last_n_values(series, end_ts=end_ts, value_key=value_key, n=need_pts)
            delta_pct = None
            if bounds is not None:
                a, b = bounds
                delta_pct = _pct_change(a, b)
            return delta_pct, pts, str(interval)
        except Exception:
            return None, None, str(interval)

    def _trend_pass(self, *, side: str, oi_points: Optional[List[float]], cvd_points: Optional[List[float]], cfg: PumpConfig) -> Tuple[bool, Dict[str, Any]]:
        tf = cfg.trend_filters
        trend_mode = str(tf.trend_mode or "soft").lower().strip()
        diag: Dict[str, Any] = {"trend_method": tf.trend_method, "trend_mode": trend_mode}
        if not tf.enabled or tf.trend_method in ("off", "none") or trend_mode == "off":
            diag["trend_enabled"] = False
            return True, diag

        side = str(side).upper()
        if trend_mode in ("oi", "cvd", "both"):
            source_mode = trend_mode
            enforcement = "hard"
        else:
            source_mode = "both"
            enforcement = "soft" if trend_mode == "soft" else "hard"
        diag["trend_source_mode"] = source_mode
        diag["trend_enforcement"] = enforcement

        use_oi = source_mode in ("oi", "both")
        use_cvd = source_mode in ("cvd", "both")
        failures: List[str] = []

        def _mark_failure(reason: str) -> None:
            if reason not in failures:
                failures.append(reason)

        if tf.trend_method in ("steps", "both"):
            need = max(2, int(tf.confirm_trend_bars) + 1)
            diag["trend_steps_need"] = need
            if use_oi:
                if not oi_points or len(oi_points) < need:
                    _mark_failure("oi_steps_insufficient")
                elif not _trend_steps_ok(oi_points[-need:], direction="up" if side == "BUY" else "down", eps=float(tf.oi_eps_pct), strict=True):
                    _mark_failure("oi_steps")
            if use_cvd:
                if not cvd_points or len(cvd_points) < need:
                    _mark_failure("cvd_steps_insufficient")
                elif not _trend_steps_ok(cvd_points[-need:], direction="up" if side == "BUY" else "down", eps=float(tf.cvd_eps_pct), strict=True):
                    _mark_failure("cvd_steps")

        if tf.trend_method in ("slope", "both"):
            n = max(2, int(tf.slope_bars))
            diag["trend_slope_need"] = n
            if use_oi:
                if not oi_points or len(oi_points) < n:
                    _mark_failure("oi_slope_insufficient")
                else:
                    ys = []
                    bad_oi = False
                    for v in oi_points[-n:]:
                        if float(v) <= 0:
                            bad_oi = True
                            break
                        ys.append(math.log(float(v)))
                    if bad_oi:
                        _mark_failure("oi_slope_nonpositive")
                    else:
                        slope = _linreg_slope(ys)
                        oi_bps = None if slope is None else float(slope) * 10000.0
                        diag["oi_slope_bps"] = oi_bps
                        if oi_bps is None:
                            _mark_failure("oi_slope_missing")
                        elif side == "BUY" and oi_bps < float(tf.buy_oi_slope_min_bps or 0.0):
                            _mark_failure("oi_slope_buy")
                        elif side == "SELL" and float(tf.sell_oi_slope_max_bps or 0.0) and oi_bps > float(tf.sell_oi_slope_max_bps):
                            _mark_failure("oi_slope_sell")
            if use_cvd:
                if not cvd_points or len(cvd_points) < n:
                    _mark_failure("cvd_slope_insufficient")
                else:
                    slope = _linreg_slope([float(x) for x in cvd_points[-n:]])
                    diag["cvd_slope_bps"] = slope
                    if slope is None:
                        _mark_failure("cvd_slope_missing")
                    elif side == "BUY" and slope < float(tf.buy_cvd_slope_min_bps or 0.0):
                        _mark_failure("cvd_slope_buy")
                    elif side == "SELL" and float(tf.sell_cvd_slope_max_bps or 0.0) and slope > float(tf.sell_cvd_slope_max_bps):
                        _mark_failure("cvd_slope_sell")

        diag["trend_failures"] = failures
        diag["trend_passed"] = not failures
        if failures and enforcement == "soft":
            diag["soft_failed"] = True
            return True, diag
        return not failures, diag

    def _collect_market_snapshot(self, *, storage: PostgreSQLStorage, exchange_id: int, interval: str, cfg: PumpConfig, now_ts: datetime) -> List[Dict[str, Any]]:
        symbols_map = storage.fetch_symbols_map(exchange_id=int(exchange_id), only_active=bool(cfg.universe.active_only)) or {}
        items = list(symbols_map.items())
        if cfg.universe.symbols_whitelist:
            wl = {str(x).upper() for x in cfg.universe.symbols_whitelist}
            items = [(sym, sid) for sym, sid in items if str(sym).upper() in wl]
        if cfg.universe.symbols_blacklist:
            bl = {str(x).upper() for x in cfg.universe.symbols_blacklist}
            items = [(sym, sid) for sym, sid in items if str(sym).upper() not in bl]
        if cfg.universe.quote_asset:
            suffix = str(cfg.universe.quote_asset).upper()
            items = [(sym, sid) for sym, sid in items if str(sym).upper().endswith(suffix)]
        if cfg.universe.max_symbols and cfg.universe.max_symbols > 0:
            items = items[: int(cfg.universe.max_symbols)]
        if not items:
            return []

        symbol_ids = [int(sid) for _, sid in items]
        td = interval_to_timedelta(interval)
        start_ts = now_ts - td * int(cfg.windows.scan_lookback_bars)
        end_ts = now_ts

        candles_map = _fetch_batch_candles(
            storage=storage,
            exchange_id=int(exchange_id),
            symbol_ids=symbol_ids,
            interval=interval,
            start_ts=start_ts,
            end_ts=end_ts,
        )

        funding_map: Dict[int, Dict[str, Any]] = {}
        if cfg.funding.enabled and hasattr(storage, 'fetch_next_funding_bulk'):
            try:
                funding_map = storage.fetch_next_funding_bulk(
                    exchange_id=int(exchange_id),
                    symbol_ids=symbol_ids,
                    as_of=now_ts,
                ) or {}
            except Exception:
                log.warning('[PUMP] funding snapshot load failed', exc_info=True)
                funding_map = {}

        latest_oi_map: Dict[int, Optional[float]] = {}
        if hasattr(storage, 'pool'):
            try:
                latest_oi_map = _fetch_batch_open_interest_latest(
                    storage=storage,
                    exchange_id=int(exchange_id),
                    symbol_ids=symbol_ids,
                    interval=interval,
                    as_of=now_ts,
                )
            except Exception:
                latest_oi_map = {}

        out: List[Dict[str, Any]] = []
        for symbol, symbol_id in items:
            sid = int(symbol_id)
            candles = candles_map.get(sid) or []
            if not candles:
                continue
            out.append({
                'symbol': str(symbol),
                'symbol_id': sid,
                'candles': candles,
                'funding': funding_map.get(sid) or {},
                'latest_open_interest': latest_oi_map.get(sid),
            })
        return out


    def _calc_base_metrics(self, candles: List[Dict[str, Any]], end_exclusive_idx: int, cfg: PumpConfig) -> Dict[str, Optional[float]]:
        lb = max(5, int(cfg.windows.base_lookback_bars))
        start = max(0, end_exclusive_idx - lb)
        base = candles[start:end_exclusive_idx]
        if len(base) < 3:
            return {}
        highs = [_to_float(c.get("high")) for c in base]
        lows = [_to_float(c.get("low")) for c in base]
        closes = [_to_float(c.get("close")) for c in base]
        qvols = [_bar_quote_volume(c) for c in base]
        base_ref = max(1e-12, _safe_mean(closes) or closes[-1])
        range_pct = (max(highs) - min(lows)) / base_ref * 100.0
        body_noises = [abs(_to_float(c.get("close")) - _to_float(c.get("open"))) / max(1e-12, base_ref) * 100.0 for c in base]
        trend_pct = _pct_change(closes[0], closes[-1]) if len(closes) >= 2 else 0.0
        greens = sum(1 for c in base if _to_float(c.get("close")) > _to_float(c.get("open")))
        reds = sum(1 for c in base if _to_float(c.get("close")) < _to_float(c.get("open")))
        imbalance = abs(greens - reds) / max(1.0, float(len(base)))
        return {
            "base_avg_price": base_ref,
            "base_range_pct": range_pct,
            "base_noise_pct": _safe_mean(body_noises) or 0.0,
            "base_trend_pct": trend_pct,
            "base_volume_median": _median(qvols) or 0.0,
            "base_volume_mean": _safe_mean(qvols) or 0.0,
            "base_green_red_imbalance": imbalance,
        }

    def _liquidity_pass(self, candles: List[Dict[str, Any]], funding: Dict[str, Any], latest_open_interest: Optional[float], cfg: PumpConfig) -> Tuple[bool, Dict[str, Any], str]:
        liq = cfg.liquidity
        bars_day = _bars_per_day(cfg.interval)
        qvols = [_bar_quote_volume(c) for c in candles[-min(max(bars_day, 24), len(candles)):]]
        last_qv = qvols[-1] if qvols else 0.0
        qv_window_sum = sum(qvols) if qvols else 0.0
        if qvols:
            scale = float(bars_day) / float(len(qvols))
            quote_volume_24h_est = qv_window_sum * max(1.0, scale)
        else:
            quote_volume_24h_est = 0.0
        avg_qv = _safe_mean(qvols) or 0.0

        oi_notional = None
        try:
            if (funding or {}).get("open_interest_value") is not None:
                oi_notional = abs(float((funding or {}).get("open_interest_value") or 0.0))
            elif latest_open_interest is not None:
                mark_price = _to_opt_float((funding or {}).get("mark_price"))
                if mark_price is None and candles:
                    mark_price = _to_opt_float(candles[-1].get("close"))
                if mark_price is not None:
                    oi_notional = abs(float(latest_open_interest) * float(mark_price))
        except Exception:
            oi_notional = None

        metrics = {
            "avg_quote_volume": avg_qv,
            "last_bar_quote_volume": last_qv,
            "quote_volume_24h_est": quote_volume_24h_est,
            "oi_notional": oi_notional,
        }
        if liq.skip_low_liquidity:
            min_qv24 = float(liq.min_quote_volume_24h or 0.0)
            min_last = float(liq.min_last_bar_quote_volume or 0.0)
            min_oi_notional = float(liq.min_notional_oi or 0.0)
            qv24_bad = min_qv24 > 0.0 and quote_volume_24h_est < min_qv24
            last_bad = min_last > 0.0 and last_qv < min_last
            oi_bad = min_oi_notional > 0.0 and oi_notional is not None and oi_notional < min_oi_notional
            # A weak 24h estimate alone is too noisy on sparse/young markets.
            # Reject only when 24h turnover is weak together with last-bar liquidity,
            # or when a reliable OI-notional check also looks bad.
            if qv24_bad and (last_bad or oi_bad):
                return False, metrics, "quote_volume_24h"
            if last_bad and qv24_bad:
                return False, metrics, "last_bar_quote_volume"
            if oi_bad and (qv24_bad or last_bad):
                return False, metrics, "notional_oi"
        return True, metrics, ""

    def _base_quality_pass(self, candidate: PumpCandidate, cfg: PumpConfig) -> Tuple[bool, str]:
        bq = cfg.base_quality
        if not bq.enabled:
            return True, ""
        # Base quality is valuable, but the raw thresholds are often too hard for fast crypto moves.
        # Treat borderline violations as acceptable and only reject clearly unstable bases.
        if candidate.base_range_pct is not None and candidate.base_range_pct > float(bq.base_range_max_pct) * 1.75:
            return False, "base_range"
        if candidate.base_noise_pct is not None and candidate.base_noise_pct > float(bq.base_noise_max_pct) * 1.75:
            return False, "base_noise"
        if candidate.base_trend_pct is not None and abs(float(candidate.base_trend_pct)) > float(bq.base_trend_abs_max_pct) * 1.6:
            return False, "base_trend"
        if candidate.base_volume_mult is not None and candidate.base_volume_mult > max(3.0, float(bq.base_volume_max_mult) * 1.5):
            return False, "base_volume_hot"
        return True, ""

    def _volume_pass(self, candidate: PumpCandidate, cfg: PumpConfig, side: str) -> Tuple[bool, str]:
        vf = cfg.volume_filters
        prof = cfg.buy if side == "BUY" else cfg.sell
        if not vf.enabled:
            return True, ""
        min_vm = max(float(vf.min_volume_mult), float(getattr(prof, "min_volume_mult", 1.0)))
        min_spike = max(float(vf.min_volume_spike_mult), float(getattr(prof, "min_volume_spike_mult", 1.0)))
        min_acc = float(getattr(prof, "min_volume_acceleration", vf.min_volume_acceleration))
        if bool(vf.require_volume_confirmation) or bool(getattr(prof, "require_volume_confirmation", False)):
            if (candidate.volume_mult or 0.0) < (min_vm * 0.92):
                return False, "volume_mult"
            if (candidate.volume_spike_mult or 0.0) < (min_spike * 0.92):
                return False, "volume_spike"
        # acceleration is noisy on the last bar; allow a small tolerance band
        if (candidate.volume_acceleration or 0.0) < (min_acc * 0.95):
            return False, "volume_acceleration"
        return True, ""

    def _anti_noise_pass(self, candles: List[Dict[str, Any]], idx: int, cfg: PumpConfig, side: str) -> Tuple[bool, str]:
        an = cfg.anti_noise
        if not an.enabled or idx < 0 or idx >= len(candles):
            return True, ""
        c = candles[idx]
        o = _to_float(c.get("open"))
        h = _to_float(c.get("high"))
        l = _to_float(c.get("low"))
        cl = _to_float(c.get("close"))
        rng = max(1e-12, h - l)
        body = abs(cl - o)
        upper_wick = max(0.0, h - max(o, cl))
        lower_wick = max(0.0, min(o, cl) - l)
        wick = max(upper_wick, lower_wick)
        if body / rng < float(an.min_body_to_range_ratio or 0.0):
            return False, "body_to_range"
        if body > 0 and wick / body > float(an.max_wick_to_body_ratio or 999.0):
            return False, "wick_to_body"
        if an.reject_erratic_series:
            tail = candles[max(0, idx - 4): idx + 1]
            refs = [_pct_change(_to_float(x.get("low")) or 1e-12, _to_float(x.get("high"))) for x in tail]
            if refs and max(refs) > float(an.erratic_range_threshold_pct or 999.0):
                return False, "erratic_range"
        return True, ""

    def _overheating_pass(self, candidate: PumpCandidate, cfg: PumpConfig) -> Tuple[bool, str]:
        oh = cfg.overheating
        if not oh.enabled:
            return True, ""
        ext = candidate.price_change_pct if candidate.side == "BUY" else candidate.pump_pct
        if ext is not None and ext > float(oh.max_price_extension_pct or 999.0):
            return False, "price_extension"
        if candidate.volume_spike_mult is not None and candidate.volume_spike_mult > float(oh.max_volume_spike_mult or 999.0):
            return False, "volume_spike_overheated"
        if oh.reject_vertical_moves and candidate.price_change_atr is not None and candidate.price_change_atr > float(oh.vertical_move_atr_threshold or 999.0):
            return False, "vertical_move"
        return True, ""

    def _score_component(self, value: Optional[float], weak: float, strong: float) -> float:
        if value is None:
            return 0.0
        if strong <= weak:
            return 100.0 if value >= strong else 0.0
        x = (float(value) - weak) / max(1e-12, (strong - weak))
        return max(0.0, min(100.0, x * 100.0))

    def calc_pump_score(self, c: PumpCandidate, cfg: PumpConfig) -> Tuple[float, Dict[str, float]]:
        sc = cfg.scoring
        side = c.side.upper()
        prof = cfg.buy if side == "BUY" else cfg.sell
        price_basis = c.price_change_pct if side == "BUY" else c.pump_pct
        price = self._score_component(price_basis, getattr(prof, "min_price_change_pct", getattr(prof, "min_pump_pct", 1.0)), getattr(prof, "max_price_extension_pct", float(getattr(prof, "min_pump_pct", 1.0)) * 2.0))
        volume = self._score_component(c.volume_mult, 1.0, max(1.2, float(getattr(prof, "min_volume_mult", 1.5)) * 1.5))
        oi = self._score_component(c.oi_delta_pct, 0.0 if side == "BUY" else -0.2, max(0.4, float(getattr(prof, "min_oi_delta_pct", 0.4)) * 2.0) if side == "BUY" else 0.0)
        cvd = self._score_component(c.cvd_delta_pct, 0.0 if side == "BUY" else -0.2, max(0.2, float(getattr(prof, "min_cvd_delta_pct", 0.2)) * 2.0) if side == "BUY" else 0.0)
        structure_raw = 100.0
        if c.base_range_pct is not None:
            structure_raw = max(0.0, 100.0 - max(0.0, c.base_range_pct - float(cfg.base_quality.base_range_max_pct or 100.0)) * 5.0)
        funding_raw = 50.0
        if c.funding_pct is not None:
            if side == "BUY":
                funding_raw = 100.0 if c.funding_pct <= 0 else max(0.0, 100.0 - c.funding_pct * 5000.0)
            else:
                funding_raw = 100.0 if c.funding_pct >= 0 else max(0.0, 100.0 + c.funding_pct * 5000.0)
        base_quality = max(0.0, 100.0 - max(0.0, abs(c.base_trend_pct or 0.0) * 8.0) - max(0.0, c.base_noise_pct or 0.0) * 5.0)
        parts = {
            "price": price,
            "volume": volume,
            "oi": oi,
            "cvd": cvd,
            "structure": max(0.0, min(100.0, structure_raw)),
            "funding": max(0.0, min(100.0, funding_raw)),
            "base_quality": max(0.0, min(100.0, base_quality)),
        }
        total = (
            parts["price"] * float(sc.weight_price) +
            parts["volume"] * float(sc.weight_volume) +
            parts["oi"] * float(sc.weight_oi) +
            parts["cvd"] * float(sc.weight_cvd) +
            parts["structure"] * float(sc.weight_structure) +
            parts["funding"] * float(sc.weight_funding) +
            parts["base_quality"] * float(sc.weight_base_quality)
        )
        denom = float(sc.weight_price + sc.weight_volume + sc.weight_oi + sc.weight_cvd + sc.weight_structure + sc.weight_funding + sc.weight_base_quality)
        if sc.normalize_total_to_100 and denom > 0:
            total = total / denom
        return max(0.0, min(100.0, total)), parts

    def _calc_sl_tp(self, side: str, entry: float, cfg: PumpConfig) -> Tuple[Optional[float], Optional[float]]:
        sl = None
        tp = None
        if entry and cfg.stop_loss_pct and cfg.stop_loss_pct > 0:
            sl = float(entry) * (1.0 - float(cfg.stop_loss_pct) / 100.0) if side.upper() == "BUY" else float(entry) * (1.0 + float(cfg.stop_loss_pct) / 100.0)
        if entry and cfg.take_profit_pct and cfg.take_profit_pct > 0:
            tp = float(entry) * (1.0 + float(cfg.take_profit_pct) / 100.0) if side.upper() == "BUY" else float(entry) * (1.0 - float(cfg.take_profit_pct) / 100.0)
        return sl, tp

    def _build_buy_candidate(self, *, storage: PostgreSQLStorage, exchange_id: int, symbol_id: int, symbol: str, interval: str, candles: List[Dict[str, Any]], funding: Dict[str, Any], cfg: PumpConfig, td: timedelta) -> Tuple[Optional[PumpCandidate], Optional[str]]:
        if not cfg.buy.enabled:
            return None, "buy_disabled"
        bar_sec = max(1.0, td.total_seconds())
        window_bars = max(int(cfg.buy.min_green_candles or 1), min(len(candles), int((cfg.buy.window_minutes * 60 + bar_sec - 1) // bar_sec)))
        end_idx = len(candles) - 1
        start_idx = max(0, len(candles) - window_bars)
        start_c = candles[start_idx]
        end_c = candles[end_idx]
        start_ts = _as_dt(start_c.get("ts"))
        end_ts = _as_dt(end_c.get("ts"))
        if start_ts is None or end_ts is None:
            return None, "bad_ts"
        start_open = _to_float(start_c.get("open"))
        end_close = _to_float(end_c.get("close"))
        atr = _calc_atr(candles[:-1] if len(candles) > 1 else candles, cfg.windows.atr_lookback_bars) or 0.0
        base_metrics = self._calc_base_metrics(candles, start_idx, cfg)
        base_avg = float(base_metrics.get("base_avg_price") or _base_avg_before_window(candles, end_exclusive_idx=start_idx, lookback_bars=cfg.windows.base_lookback_bars) or start_open)
        price_change_pct = _pct_change(base_avg, end_close)
        pct_in_window = _pct_change(start_open, end_close)
        price_change_atr = 0.0 if atr <= 0 else (end_close - start_open) / atr
        window = candles[start_idx: end_idx + 1]
        green_candles = sum(1 for c in window if _to_float(c.get("close")) > _to_float(c.get("open")))
        qvols = [_bar_quote_volume(c) for c in window]
        base_vol = base_metrics.get("base_volume_median") if cfg.volume_filters.use_median_base_volume else base_metrics.get("base_volume_mean")
        base_vol = float(base_vol or 0.0)
        last_qv = qvols[-1] if qvols else 0.0
        prev_qv = qvols[-2] if len(qvols) >= 2 else base_vol
        volume_mult = last_qv / max(1e-12, base_vol) if base_vol > 0 else 0.0
        volume_spike_mult = last_qv / max(1e-12, _safe_mean(qvols[:-1]) or base_vol or 1.0)
        volume_acc = last_qv / max(1e-12, prev_qv or 1.0)
        f_rate = (funding or {}).get("funding_rate")
        f_pct = None if f_rate is None else float(f_rate) * 100.0
        if not self._funding_pass(side="BUY", funding_pct=f_pct, cfg=cfg):
            return None, "funding"
        threshold_pct = max(price_change_pct, pct_in_window)
        if threshold_pct < float(cfg.buy.min_price_change_pct):
            return None, "price_change"
        if price_change_atr < float(cfg.buy.min_price_change_atr):
            return None, "price_change_atr"
        if cfg.buy.require_last_bar_green and not (_to_float(end_c.get("close")) > _to_float(end_c.get("open"))):
            return None, "last_bar_green"
        if green_candles < int(cfg.buy.min_green_candles):
            return None, "green_candles"
        if cfg.buy.require_close_near_high:
            h = _to_float(end_c.get("high")); l = _to_float(end_c.get("low"))
            if h > l and (h - end_close) / max(1e-12, h - l) > 0.35:
                return None, "close_near_high"
        signal_ts = self._resolve_signal_ts(bar_ts=end_ts, td=td, cfg=cfg)
        cand = PumpCandidate(symbol_id=symbol_id, symbol=symbol, interval=interval, side="BUY", signal_ts=signal_ts, entry_price=end_close)
        cand.price_change_pct = max(price_change_pct, pct_in_window)
        cand.pump_pct = cand.price_change_pct
        cand.price_change_atr = price_change_atr
        cand.base_range_pct = _to_opt_float(base_metrics.get("base_range_pct"))
        cand.base_noise_pct = _to_opt_float(base_metrics.get("base_noise_pct"))
        cand.base_trend_pct = _to_opt_float(base_metrics.get("base_trend_pct"))
        cand.base_volume_mult = (base_vol / max(1e-12, _safe_mean(qvols) or base_vol)) if base_vol > 0 else None
        cand.volume_mult = volume_mult
        cand.volume_spike_mult = volume_spike_mult
        cand.volume_acceleration = volume_acc
        oi_delta_pct, oi_points, oi_itv = self._calc_flow_info(storage, exchange_id, symbol_id, interval, start_ts, end_ts, cfg, "oi")
        cvd_delta_pct, cvd_points, cvd_itv = self._calc_flow_info(storage, exchange_id, symbol_id, interval, start_ts, end_ts, cfg, "cvd")
        cand.oi_delta_pct = oi_delta_pct
        cand.oi_value_delta_pct = oi_delta_pct
        cand.cvd_delta_pct = cvd_delta_pct
        cand.funding_pct = f_pct
        trend_ok, trend_diag = self._trend_pass(side="BUY", oi_points=oi_points, cvd_points=cvd_points, cfg=cfg)
        if not trend_ok:
            return None, "trend"
        cand.context.update({
            "pattern": "BUY_PUMP",
            "interval": interval,
            "window_minutes": int(cfg.buy.window_minutes),
            "base_lookback_bars": int(cfg.windows.base_lookback_bars),
            "base_avg_price": base_avg,
            "threshold_price": start_open * (1.0 + float(cfg.buy.min_price_change_pct) / 100.0),
            "pct_in_window": pct_in_window,
            "confirm_ts": end_ts,
            "signal_ts": signal_ts,
            "price_change_pct": cand.price_change_pct,
            "pump_pct": cand.pump_pct,
            "pump_start_ts": start_ts,
            "pump_end_ts": end_ts,
            "oi_delta": oi_delta_pct,
            "cvd_delta": cvd_delta_pct,
            "oi_interval_used": oi_itv,
            "cvd_interval_used": cvd_itv,
            "oi_points": oi_points,
            "cvd_points": cvd_points,
            "funding_rate": f_rate,
            "funding_pct": f_pct,
            "funding_time": (funding or {}).get("funding_time"),
            "funding_is_next": bool((funding or {}).get("is_next", False)),
            "funding_is_estimated": bool((funding or {}).get("is_estimated", False)),
            "trend": trend_diag,
        })
        return cand, None

    def _build_sell_candidate(self, *, storage: PostgreSQLStorage, exchange_id: int, symbol_id: int, symbol: str, interval: str, candles: List[Dict[str, Any]], funding: Dict[str, Any], cfg: PumpConfig, td: timedelta) -> Tuple[Optional[PumpCandidate], Optional[str]]:
        if not cfg.sell.enabled:
            return None, "sell_disabled"
        bar_sec = max(1.0, td.total_seconds())
        pump_bars = max(1, int((cfg.sell.window_minutes * 60 + bar_sec - 1) // bar_sec))
        confirm_n = max(1, int(cfg.sell.confirm_lookforward_bars))
        if len(candles) < pump_bars + confirm_n + 2:
            return None, "not_enough_bars"
        last_idx = len(candles) - 1
        atr = _calc_atr(candles[:-1] if len(candles) > 1 else candles, cfg.windows.atr_lookback_bars) or 0.0
        f_rate = (funding or {}).get("funding_rate")
        f_pct = None if f_rate is None else float(f_rate) * 100.0
        if not self._funding_pass(side="SELL", funding_pct=f_pct, cfg=cfg):
            return None, "funding"
        for pump_end_idx in range(last_idx - confirm_n, pump_bars - 1, -1):
            pump_start_idx = pump_end_idx - pump_bars + 1
            pump_start = candles[pump_start_idx]
            pump_end = candles[pump_end_idx]
            pump_start_open = _to_float(pump_start.get("open"))
            pump_end_close = _to_float(pump_end.get("close"))
            base_metrics = self._calc_base_metrics(candles, pump_start_idx, cfg)
            base_avg = float(base_metrics.get("base_avg_price") or _base_avg_before_window(candles, end_exclusive_idx=pump_start_idx, lookback_bars=cfg.windows.base_lookback_bars) or pump_start_open)
            pump_pct = _pct_change(base_avg, pump_end_close)
            pct_in_window = _pct_change(pump_start_open, pump_end_close)
            pump_atr = 0.0 if atr <= 0 else (pump_end_close - pump_start_open) / atr
            if pump_pct < float(cfg.sell.min_pump_pct):
                continue
            if pump_atr < float(cfg.sell.min_pump_atr):
                continue
            conf_slice = candles[pump_end_idx + 1: pump_end_idx + 1 + confirm_n]
            best = None
            best_idx = None
            for j, c in enumerate(conf_slice):
                o = _to_float(c.get("open")); cl = _to_float(c.get("close")); h = _to_float(c.get("high")); l = _to_float(c.get("low"))
                if cfg.sell.require_red_confirm and not (cl < o):
                    continue
                body_pct = abs(cl - o) / max(1e-12, o) * 100.0
                if body_pct < float(cfg.sell.min_red_body_pct):
                    continue
                upper_wick_pct = max(0.0, h - max(o, cl)) / max(1e-12, o) * 100.0
                if upper_wick_pct < float(cfg.sell.min_upper_wick_pct):
                    continue
                reversal_from_peak_pct = _pct_change(max(_to_float(pump_end.get("high")), pump_end_close), cl)
                if abs(reversal_from_peak_pct) < float(cfg.sell.reversal_from_peak_pct):
                    continue
                abs_idx = pump_end_idx + 1 + j
                if cfg.sell.require_close_below_prev_close and cl >= _to_float(candles[abs_idx - 1].get("close")):
                    continue
                if cfg.sell.confirm_close_position_max < 1.0 and h > l and (cl - l) / max(1e-12, h - l) > float(cfg.sell.confirm_close_position_max):
                    continue
                if best is None or cl > _to_float(best.get("close")):
                    best = c
                    best_idx = abs_idx
            if best is None or best_idx is None:
                continue
            pump_start_ts = _as_dt(pump_start.get("ts")); pump_end_ts = _as_dt(pump_end.get("ts")); red_ts = _as_dt(best.get("ts"))
            if pump_start_ts is None or pump_end_ts is None or red_ts is None:
                continue
            entry_price = _to_float(best.get("close"))
            last_qv = _bar_quote_volume(best)
            prev_qv = _bar_quote_volume(candles[best_idx - 1]) if best_idx > 0 else last_qv
            base_vol = float(base_metrics.get("base_volume_median") or 0.0)
            signal_ts = self._resolve_signal_ts(bar_ts=red_ts, td=td, cfg=cfg)
            if not self._is_fresh_signal_ts(signal_ts=signal_ts, cfg=cfg):
                continue
            cand = PumpCandidate(symbol_id=symbol_id, symbol=symbol, interval=interval, side="SELL", signal_ts=signal_ts, entry_price=entry_price)
            cand.price_change_pct = pump_pct
            cand.pump_pct = pump_pct
            cand.price_change_atr = pump_atr
            cand.reversal_from_peak_pct = abs(_pct_change(max(_to_float(pump_end.get("high")), pump_end_close), entry_price))
            cand.base_range_pct = _to_opt_float(base_metrics.get("base_range_pct"))
            cand.base_noise_pct = _to_opt_float(base_metrics.get("base_noise_pct"))
            cand.base_trend_pct = _to_opt_float(base_metrics.get("base_trend_pct"))
            cand.base_volume_mult = (base_vol / max(1e-12, _safe_mean([_bar_quote_volume(x) for x in conf_slice]) or base_vol)) if base_vol > 0 else None
            cand.volume_mult = last_qv / max(1e-12, base_vol or 1.0)
            cand.volume_spike_mult = last_qv / max(1e-12, prev_qv or 1.0)
            cand.volume_acceleration = cand.volume_spike_mult
            oi_delta_pct, oi_points, oi_itv = self._calc_flow_info(storage, exchange_id, symbol_id, interval, pump_end_ts, red_ts, cfg, "oi")
            cvd_delta_pct, cvd_points, cvd_itv = self._calc_flow_info(storage, exchange_id, symbol_id, interval, pump_end_ts, red_ts, cfg, "cvd")
            cand.oi_delta_pct = oi_delta_pct
            cand.oi_value_delta_pct = oi_delta_pct
            cand.cvd_delta_pct = cvd_delta_pct
            cand.funding_pct = f_pct
            trend_ok, trend_diag = self._trend_pass(side="SELL", oi_points=oi_points, cvd_points=cvd_points, cfg=cfg)
            if not trend_ok:
                continue
            cand.context.update({
                "pattern": "SELL_PUMP_REVERSAL",
                "interval": interval,
                "window_minutes": int(cfg.sell.window_minutes),
                "base_lookback_bars": int(cfg.windows.base_lookback_bars),
                "base_avg_price": base_avg,
                "threshold_price": base_avg * (1.0 + float(cfg.sell.min_pump_pct) / 100.0),
                "pct_in_window": pct_in_window,
                "confirm_ts": red_ts,
                "price_change_pct": pump_pct,
                "pump_pct": pump_pct,
                "pump_start_ts": pump_start_ts,
                "pump_end_ts": pump_end_ts,
                "confirm_red_ts": red_ts,
                "signal_ts": signal_ts,
                "confirm_red_close": entry_price,
                "oi_delta": oi_delta_pct,
                "cvd_delta": cvd_delta_pct,
                "oi_interval_used": oi_itv,
                "cvd_interval_used": cvd_itv,
                "oi_points": oi_points,
                "cvd_points": cvd_points,
                "funding_rate": f_rate,
                "funding_pct": f_pct,
                "funding_time": (funding or {}).get("funding_time"),
                "funding_is_next": bool((funding or {}).get("is_next", False)),
                "funding_is_estimated": bool((funding or {}).get("is_estimated", False)),
                "trend": trend_diag,
            })
            return cand, None
        return None, "no_reversal"

    def _profile_and_score(self, candidate: PumpCandidate, cfg: PumpConfig) -> Tuple[bool, str]:
        side = candidate.side.upper()
        prof = cfg.buy if side == "BUY" else cfg.sell
        score, parts = self.calc_pump_score(candidate, cfg)
        candidate.score = score
        candidate.score_parts = parts
        ctx = candidate.context if isinstance(candidate.context, dict) else {}
        candidate.context = ctx

        if side == "BUY":
            min_oi = float(prof.min_oi_delta_pct or 0.0)
            min_cvd = float(prof.min_cvd_delta_pct or 0.0)
            confirmed_thr = float(prof.confirmed_min_signal_score or prof.min_signal_score)
            early_thr = float(prof.early_min_signal_score or prof.min_signal_score)
            oi_val = float(candidate.oi_delta_pct or 0.0)
            cvd_val = float(candidate.cvd_delta_pct or 0.0)
            hard_oi = oi_val >= min_oi
            hard_cvd = cvd_val >= min_cvd
            full_flow = ((not prof.require_oi_confirmation) or hard_oi) and ((not prof.require_cvd_confirmation) or hard_cvd)
            partial_flow = hard_oi or hard_cvd or ((not prof.require_oi_confirmation) and (not prof.require_cvd_confirmation))

            ctx["flow_confirmation"] = "full" if full_flow else ("partial" if partial_flow else "none")
            ctx["flow_metrics"] = {
                "oi_delta_pct": oi_val,
                "cvd_delta_pct": cvd_val,
                "min_oi_delta_pct": min_oi,
                "min_cvd_delta_pct": min_cvd,
                "score": float(score or 0.0),
                "early_threshold": early_thr,
                "confirmed_threshold": confirmed_thr,
                "hard_oi": hard_oi,
                "hard_cvd": hard_cvd,
            }

            if prof.enable_confirmed and score >= confirmed_thr and full_flow:
                candidate.profile = "confirmed"
                return True, ""

            if prof.enable_early and score >= early_thr and partial_flow:
                candidate.profile = "early"
                return True, ""

            if prof.require_oi_confirmation and prof.require_cvd_confirmation and not partial_flow:
                return False, "oi_cvd"
            if prof.require_oi_confirmation and not hard_oi and not hard_cvd:
                return False, "oi"
            if prof.require_cvd_confirmation and not hard_cvd and not hard_oi:
                return False, "cvd"
            if score < min(early_thr, confirmed_thr):
                return False, "score"
            return False, "flow_partial"

        oi_ok = candidate.oi_delta_pct is None or (candidate.oi_delta_pct <= float(prof.max_oi_delta_pct) if not prof.allow_flat_oi else candidate.oi_delta_pct <= max(0.0, float(prof.max_oi_delta_pct)))
        cvd_ok = candidate.cvd_delta_pct is None or candidate.cvd_delta_pct <= float(prof.max_cvd_delta_pct)
        if prof.require_oi_exhaustion and not oi_ok:
            return False, "oi"
        if prof.require_cvd_weakness and not cvd_ok:
            return False, "cvd"
        if prof.enable_confirmed and score >= float(prof.confirmed_min_signal_score or prof.min_signal_score):
            candidate.profile = "confirmed"
            return True, ""
        if prof.enable_early and score >= float(prof.early_min_signal_score or prof.min_signal_score):
            candidate.profile = "early"
            return True, ""
        return False, "score"

    def _candidate_to_signal(self, c: PumpCandidate, cfg: PumpConfig) -> ScreenerSignal:
        sl, tp = self._calc_sl_tp(c.side, c.entry_price, cfg)
        ctx = dict(c.context or {})
        ctx.update({
            "profile": c.profile,
            "score_parts": c.score_parts,
            "base_range_pct": c.base_range_pct,
            "base_noise_pct": c.base_noise_pct,
            "base_trend_pct": c.base_trend_pct,
            "volume_mult": c.volume_mult,
            "volume_spike_mult": c.volume_spike_mult,
            "volume_acceleration": c.volume_acceleration,
            "oi_delta_pct": c.oi_delta_pct,
            "oi_value_delta_pct": c.oi_value_delta_pct,
            "cvd_delta_pct": c.cvd_delta_pct,
            "reversal_from_peak_pct": c.reversal_from_peak_pct,
            "price_change_atr": c.price_change_atr,
        })
        conf = max(0.0, min(1.0, float(c.score or 0.0) / 100.0))
        reason = f"{c.side} {c.profile or 'signal'} score={float(c.score or 0.0):.1f}"
        return ScreenerSignal(symbol_id=c.symbol_id, symbol=c.symbol, signal_ts=c.signal_ts, side=c.side, entry_price=c.entry_price, stop_loss=sl, take_profit=tp, confidence=conf, score=c.score, reason=reason, context=ctx)

    def run(self, *, storage: PostgreSQLStorage, exchange_id: int, interval: str, params: Dict[str, Any]) -> List[ScreenerSignal]:
        cfg = PumpConfig.from_cfg(params).for_interval(str(interval))
        td = interval_to_timedelta(interval)
        if td.total_seconds() <= 0:
            raise ValueError(f"Invalid interval: {interval}")
        now_ts = _utc_now()
        stats = {"symbols_total": 0, "candles_ok": 0, "liq_ok": 0, "base_ok": 0, "trigger_ok": 0, "volume_ok": 0, "flow_ok": 0, "anti_noise_ok": 0, "overheating_ok": 0, "score_ok": 0, "signals": 0}
        fail_samples: Dict[str, List[str]] = {}
        snapshot = self._collect_market_snapshot(storage=storage, exchange_id=int(exchange_id), interval=interval, cfg=cfg, now_ts=now_ts)
        stats["symbols_total"] = len(snapshot)
        candidates: List[PumpCandidate] = []
        for item in snapshot:
            symbol = item["symbol"]
            symbol_id = item["symbol_id"]
            candles = item["candles"]
            funding = item["funding"]
            if not candles or len(candles) < 5:
                continue
            stats["candles_ok"] += 1
            last_close = _to_float(candles[-1].get("close"))
            if last_close < float(cfg.universe.min_price) or last_close > float(cfg.universe.max_price):
                fail_samples.setdefault("price_universe", []).append(symbol)
                continue
            liq_ok, _liq_metrics, liq_reason = self._liquidity_pass(candles, funding, item.get("latest_open_interest"), cfg)
            if not liq_ok:
                fail_samples.setdefault(f"liq:{liq_reason}", []).append(symbol)
                continue
            stats["liq_ok"] += 1
            mode = str(cfg.runtime.signal_mode or "both").lower()
            to_try = ["BUY", "SELL"] if mode == "both" else [mode.upper()]
            accepted_any = False
            for side in to_try:
                if side == "BUY":
                    cand, reason = self._build_buy_candidate(storage=storage, exchange_id=int(exchange_id), symbol_id=int(symbol_id), symbol=symbol, interval=interval, candles=candles, funding=funding, cfg=cfg, td=td)
                else:
                    cand, reason = self._build_sell_candidate(storage=storage, exchange_id=int(exchange_id), symbol_id=int(symbol_id), symbol=symbol, interval=interval, candles=candles, funding=funding, cfg=cfg, td=td)
                if cand is None:
                    fail_samples.setdefault(f"{side.lower()}:{reason or 'trigger'}", []).append(symbol)
                    continue
                stats["trigger_ok"] += 1
                ok, why = self._base_quality_pass(cand, cfg)
                if not ok:
                    fail_samples.setdefault(f"{side.lower()}:base:{why}", []).append(symbol)
                    continue
                stats["base_ok"] += 1
                ok, why = self._volume_pass(cand, cfg, side)
                if not ok:
                    fail_samples.setdefault(f"{side.lower()}:volume:{why}", []).append(symbol)
                    continue
                stats["volume_ok"] += 1
                ok, why = self._overheating_pass(cand, cfg)
                if not ok:
                    fail_samples.setdefault(f"{side.lower()}:overheat:{why}", []).append(symbol)
                    continue
                stats["overheating_ok"] += 1
                signal_idx = len(candles) - 1 if side == "BUY" else max(0, len(candles) - 1)
                ok, why = self._anti_noise_pass(candles, signal_idx, cfg, side)
                if not ok:
                    fail_samples.setdefault(f"{side.lower()}:noise:{why}", []).append(symbol)
                    continue
                stats["anti_noise_ok"] += 1
                ok, why = self._profile_and_score(cand, cfg)
                if not ok:
                    fail_samples.setdefault(f"{side.lower()}:flow:{why}", []).append(symbol)
                    if cfg.debug or cfg.runtime.enable_debug_logs:
                        flow_metrics = (cand.context or {}).get("flow_metrics") or {}
                        trend_diag = (cand.context or {}).get("trend") or {}
                        log.info("[PUMP][FLOW_FAIL] %s side=%s reason=%s score=%.2f oi=%.4f/%.4f cvd=%.4f/%.4f flow=%s trend_failures=%s", symbol, side, why, float(cand.score or 0.0), float(flow_metrics.get("oi_delta_pct") or 0.0), float(flow_metrics.get("min_oi_delta_pct") or 0.0), float(flow_metrics.get("cvd_delta_pct") or 0.0), float(flow_metrics.get("min_cvd_delta_pct") or 0.0), flow_metrics.get("flow_confirmation" ) or (cand.context or {}).get("flow_confirmation"), trend_diag.get("trend_failures"))
                    continue
                stats["flow_ok"] += 1
                stats["score_ok"] += 1
                candidates.append(cand)
                accepted_any = True
                if mode != "both":
                    break
            if not accepted_any and cfg.debug and cfg.debug_top > 0:
                pass
        sort_by = str(cfg.runtime.sort_by or "score")
        descending = bool(cfg.runtime.descending)
        def _sort_key(c: PumpCandidate) -> float:
            if sort_by == "pct":
                return float(c.pump_pct or c.price_change_pct or 0.0)
            if sort_by == "volume":
                return float(c.volume_mult or 0.0)
            if sort_by == "oi":
                return float(c.oi_delta_pct or 0.0)
            return float(c.score or 0.0)
        candidates.sort(key=_sort_key, reverse=descending)
        candidates = candidates[: max(1, int(cfg.runtime.max_signals_per_cycle or 20))]
        out = [self._candidate_to_signal(c, cfg) for c in candidates]
        stats["signals"] = len(out)
        if cfg.debug or cfg.runtime.enable_debug_logs:
            log.info("[PUMP] stats interval=%s symbols=%d candles_ok=%d liq_ok=%d base_ok=%d trigger_ok=%d volume_ok=%d flow_ok=%d anti_noise_ok=%d overheating_ok=%d score_ok=%d signals=%d", interval, stats["symbols_total"], stats["candles_ok"], stats["liq_ok"], stats["base_ok"], stats["trigger_ok"], stats["volume_ok"], stats["flow_ok"], stats["anti_noise_ok"], stats["overheating_ok"], stats["score_ok"], stats["signals"])
            shown = 0
            for key in sorted(fail_samples.keys()):
                vals = fail_samples[key][:3]
                if vals:
                    log.info("[PUMP] fail(%s) sample=%s", key, vals)
                    shown += 1
                    if shown >= max(3, int(cfg.debug_top or cfg.runtime.debug_top or 3)):
                        break
        return out
