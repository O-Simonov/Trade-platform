# src/platform/screeners/scr_oi_binance.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from src.platform.core.utils.candles import interval_to_timedelta
from src.platform.data.storage.postgres.storage import PostgreSQLStorage

log = logging.getLogger("screeners.scr_oi_binance")


# =============================================================================
# small utils (safe conversions)
# =============================================================================

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
            d = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if d.tzinfo is None:
                d = d.replace(tzinfo=timezone.utc)
            return d.astimezone(timezone.utc)
        except Exception:
            return None
    return None


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return float(default)
        x = float(v)
        if x != x:  # NaN
            return float(default)
        return x
    except Exception:
        return float(default)


def _to_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return int(default)
        return int(v)
    except Exception:
        return int(default)


def _to_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return bool(default)
    if isinstance(v, bool):
        return bool(v)
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return bool(default)


def _safe_mean(x: List[float]) -> Optional[float]:
    if not x:
        return None
    try:
        return float(sum(x)) / float(len(x))
    except Exception:
        return None


def _safe_median(x: List[float]) -> Optional[float]:
    """Robust median (useful for volume baselines)."""
    if not x:
        return None
    try:
        xs = sorted(float(v) for v in x)
        n = len(xs)
        if n == 0:
            return None
        mid = n // 2
        if n % 2 == 1:
            return xs[mid]
        return (xs[mid - 1] + xs[mid]) / 2.0
    except Exception:
        return None


def _percentile(x: List[float], p: float) -> Optional[float]:
    """Linear-interpolated percentile for a list of floats."""
    if not x:
        return None
    try:
        xs = sorted(float(v) for v in x)
        n = len(xs)
        if n == 0:
            return None
        if p <= 0:
            return xs[0]
        if p >= 100:
            return xs[-1]
        k = (n - 1) * (float(p) / 100.0)
        f = int(k)
        c = min(f + 1, n - 1)
        if f == c:
            return xs[f]
        d = k - f
        return xs[f] * (1.0 - d) + xs[c] * d
    except Exception:
        return None


def _pct(a: float, b: float) -> Optional[float]:
    """
    % change: (b/a - 1) * 100
    """
    try:
        aa = float(a)
        bb = float(b)
        if aa == 0.0:
            return None
        return (bb / aa - 1.0) * 100.0
    except Exception:
        return None


def _range_pct(values: List[float]) -> Optional[float]:
    """
    (max - min) / mean * 100
    """
    if not values:
        return None
    try:
        mn = min(values)
        mx = max(values)
        m = _safe_mean(values)
        if m is None or m == 0.0:
            return None
        return (mx - mn) / m * 100.0
    except Exception:
        return None


def _pick_last_closed_idx(candles: List[Dict[str, Any]], *, interval: str, now_ts: datetime) -> int:
    """
    Возвращает индекс последней «закрытой» свечи.
    Если в БД есть текущая формирующаяся свеча, её close_time будет в будущем —
    тогда берём предпоследнюю.
    """
    if not candles:
        return -1

    td = interval_to_timedelta(interval)
    if td.total_seconds() <= 0:
        return len(candles) - 1

    last_i = len(candles) - 1
    last_ts = _as_dt(candles[last_i].get("ts")) or _as_dt(candles[last_i].get("open_time")) or None
    if last_ts is None:
        return last_i

    last_close = last_ts + td
    if last_close > (now_ts + td * 0.10):
        return max(0, last_i - 1)

    return last_i


def _candle_quote_volume(c: Dict[str, Any]) -> float:
    """
    В базе candles могут быть:
      - quote_volume (предпочтительно)
      - volume (base) -> умножаем на close, чтобы получить прибл. quote
    """
    qv = _to_float(c.get("quote_volume"), 0.0)
    if qv > 0:
        return qv
    v = _to_float(c.get("volume"), 0.0)
    cl = _to_float(c.get("close"), 0.0)
    if v > 0 and cl > 0:
        return v * cl
    return 0.0


def _align_series_to_candles(
    *,
    candles: List[Dict[str, Any]],
    series: List[Dict[str, Any]],
    value_key: str,
) -> List[Optional[float]]:
    """
    Приводим series (ts, value_key) к длине candles:
      - свечи берём как master timeline (c["ts"])
      - значение series берём как последний ts <= candle_ts (forward-fill)
    """
    if not candles:
        return []
    if not series:
        return [None for _ in candles]

    s = []
    for r in series:
        ts = _as_dt(r.get("ts"))
        if ts is None:
            continue
        s.append({"ts": ts, "v": r.get(value_key)})
    s.sort(key=lambda x: x["ts"])

    out: List[Optional[float]] = []
    j = 0
    last_v: Optional[float] = None

    for c in candles:
        ct = _as_dt(c.get("ts"))
        if ct is None:
            out.append(last_v)
            continue

        while j < len(s) and s[j]["ts"] <= ct:
            last_v = _to_float(s[j]["v"], last_v if last_v is not None else 0.0)
            j += 1

        out.append(last_v)

    return out


def _fetch_oi_series(
    *,
    storage: PostgreSQLStorage,
    exchange_id: int,
    symbol_id: int,
    interval: str,
    start_ts: datetime,
    end_ts: datetime,
) -> List[Dict[str, Any]]:
    """
    В open_interest есть open_interest и open_interest_value.
    Возвращаем оба, чтобы можно было выбрать что рисовать/считать.
    """
    q = """
        SELECT ts, open_interest, open_interest_value
        FROM open_interest
        WHERE exchange_id = %s
          AND symbol_id = %s
          AND interval = %s
          AND ts >= %s
          AND ts <= %s
        ORDER BY ts
    """
    with storage.pool.connection() as con:
        with con.cursor() as cur:
            cur.execute(q, (int(exchange_id), int(symbol_id), str(interval), start_ts, end_ts))
            rows = cur.fetchall() or []
            cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]


def _fetch_last_funding_two(
    *,
    storage: PostgreSQLStorage,
    exchange_id: int,
    symbol_id: int,
    as_of: datetime,
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Берём 2 последних funding записи <= as_of.
    Возвращаем (last, prev).
    """
    q = """
        SELECT funding_time AS ts, funding_rate
        FROM funding
        WHERE exchange_id = %s
          AND symbol_id = %s
          AND funding_time <= %s
        ORDER BY funding_time DESC
        LIMIT 2
    """
    with storage.pool.connection() as con:
        with con.cursor() as cur:
            cur.execute(q, (int(exchange_id), int(symbol_id), as_of))
            rows = cur.fetchall() or []
            cols = [d[0] for d in cur.description]
    out = [dict(zip(cols, r)) for r in rows]
    last = out[0] if len(out) >= 1 else None
    prev = out[1] if len(out) >= 2 else None
    return last, prev


# =============================================================================
# data model
# =============================================================================

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
    interval: str = "5m"
    min_price: float = 1e-7
    max_price: float = 100000.0
    signal_mode: str = "both"  # both|buy|sell
    max_symbols: int = 0

    # windows
    base_minutes: int = 240     # база (флэт/тренд)
    trigger_minutes: int = 20   # триггер (всплеск OI/volume)

    # BASE volume calmness
    # If set, we treat the base window as "calm" only when the largest base candle volume
    # is not much larger than the median base volume. This helps detect "quiet" consolidation.
    base_volume_max_mult: Optional[float] = 2.0

    # PRICE base regime
    price_sideways_max_pct: float = 1.2          # max range% in base to treat as боковик
    price_downtrend_min_pct: float = 1.0         # min drop% in base to treat as downtrend

    # BUY trigger (5m)
    buy_oi_rise_pct: float = 3.0                 # OI % за trigger_minutes
    # Additional confirmation: open_interest_value must also rise by at least this percent.
    # This keeps signals aligned with "notional" (USDT) growth, but we still compute the core OI on contracts.
    oi_value_confirm_pct: Optional[float] = None
    buy_volume_mult: float = 3.0                 # trigger avg volume multiplier vs base median
    buy_volume_spike_mult: float = 3.0          # trigger max candle volume multiplier vs base median
    buy_price_rise_pct: float = 0.3              # price % за trigger_minutes
    buy_min_green_candles: int = 2               # сколько зелёных свечей в trigger
    buy_require_last_green: bool = True      # последняя свеча в trigger должна быть зелёной

    # SELL trigger
    sell_oi_drop_one_candle_pct: float = 1.0     # OI % падение за 1 свечу (|<= -x|)
    sell_oi_drop_window_pct: float = 1.5         # доп. критерий: OI % за trigger_minutes
    sell_price_drop_last_pct: float = 0.5        # последняя свеча должна упасть хотя бы на x%
    sell_volume_spike_mult: float = 2.5          # в trigger был пик >= base_avg * mult
    sell_volume_drop_ratio: float = 0.45         # после пика: last_vol <= max_vol * ratio
    sell_reversal_from_max_pct: float = 0.7      # падение от максимума в trigger до последнего close
    # OI — ключи (раздельно: расчёт vs график)
    #   - Для расчёта сигналов лучше использовать open_interest (контракты),
    #     чтобы рост цены сам по себе не «раздувал» OI в USDT.
    #   - Для графика часто удобнее open_interest_value (USDT).
    oi_calc_key: str = "open_interest"            # open_interest (контракты) | open_interest_value (USDT)
    oi_plot_key: str = "open_interest_value"      # open_interest_value (USDT) | open_interest (контракты)

    # Funding filter (для BUY)
    enable_funding: bool = True
    buy_funding_max_pct: Optional[float] = -0.01  # funding_pct = rate * 100 ; для BUY ожидаем <= -0.01
    buy_require_funding_decreasing: bool = False  # если true: last <= prev (становится более отрицательным)

    # Funding filter (для SELL)
    # Для SELL по ТЗ: funding должен быть всегда положительный.
    sell_enable_funding: bool = True
    sell_funding_min_pct: Optional[float] = 0.0  # funding_pct = rate*100 ; для SELL ожидаем > 0.0

    # SL/TP
    stop_loss_pct: Optional[float] = 2.0
    take_profit_pct: Optional[float] = 5.0

    # misc
    debug: bool = False
    debug_top: int = 5


def _parse_params(params: Dict[str, Any]) -> ScrParams:
    p = ScrParams()

    p.interval = str(params.get("interval", p.interval)).strip() or p.interval

    p.min_price = _to_float(params.get("min_price"), p.min_price)
    p.max_price = _to_float(params.get("max_price"), p.max_price)

    mode = str(params.get("signal_mode", p.signal_mode)).strip().lower()
    if mode not in ("both", "buy", "sell"):
        mode = p.signal_mode
    p.signal_mode = mode

    p.max_symbols = max(0, _to_int(params.get("max_symbols"), p.max_symbols))

    p.base_minutes = max(60, _to_int(params.get("base_minutes"), p.base_minutes))
    p.trigger_minutes = max(5, _to_int(params.get("trigger_minutes"), p.trigger_minutes))

    bvm = params.get("base_volume_max_mult", p.base_volume_max_mult)
    p.base_volume_max_mult = None if bvm is None else _to_float(bvm, 2.0)

    p.price_sideways_max_pct = _to_float(params.get("price_sideways_max_pct"), p.price_sideways_max_pct)
    p.price_downtrend_min_pct = _to_float(params.get("price_downtrend_min_pct"), p.price_downtrend_min_pct)

    p.buy_oi_rise_pct = _to_float(params.get("buy_oi_rise_pct"), p.buy_oi_rise_pct)
    oivc = params.get("oi_value_confirm_pct", p.oi_value_confirm_pct)
    p.oi_value_confirm_pct = None if oivc is None else _to_float(oivc, 0.0)
    p.buy_volume_mult = _to_float(params.get("buy_volume_mult"), p.buy_volume_mult)
    p.buy_price_rise_pct = _to_float(params.get("buy_price_rise_pct"), p.buy_price_rise_pct)
    p.buy_min_green_candles = max(0, _to_int(params.get("buy_min_green_candles"), p.buy_min_green_candles))

    p.sell_oi_drop_one_candle_pct = _to_float(params.get("sell_oi_drop_one_candle_pct"), p.sell_oi_drop_one_candle_pct)
    p.sell_oi_drop_window_pct = _to_float(params.get("sell_oi_drop_window_pct"), p.sell_oi_drop_window_pct)
    p.sell_price_drop_last_pct = _to_float(params.get("sell_price_drop_last_pct"), p.sell_price_drop_last_pct)
    p.sell_volume_spike_mult = _to_float(params.get("sell_volume_spike_mult"), p.sell_volume_spike_mult)
    p.sell_volume_drop_ratio = _to_float(params.get("sell_volume_drop_ratio"), p.sell_volume_drop_ratio)
    p.sell_reversal_from_max_pct = _to_float(params.get("sell_reversal_from_max_pct"), p.sell_reversal_from_max_pct)
    # OI keys (calc vs plot)
    # Backward-compat: if old oi_value_key is provided and new keys are missing,
    # we use it for both calc and plot.
    raw_calc = params.get("oi_calc_key", None)
    raw_plot = params.get("oi_plot_key", None)
    raw_legacy = params.get("oi_value_key", None)

    if raw_calc is None and raw_plot is None and raw_legacy is not None:
        raw_calc = raw_legacy
        raw_plot = raw_legacy

    p.oi_calc_key = str(raw_calc if raw_calc is not None else p.oi_calc_key).strip() or p.oi_calc_key
    p.oi_plot_key = str(raw_plot if raw_plot is not None else p.oi_plot_key).strip() or p.oi_plot_key

    if p.oi_calc_key not in ("open_interest_value", "open_interest"):
        p.oi_calc_key = "open_interest"
    if p.oi_plot_key not in ("open_interest_value", "open_interest"):
        p.oi_plot_key = "open_interest_value"
    p.enable_funding = _to_bool(params.get("enable_funding"), p.enable_funding)
    bf = params.get("buy_funding_max_pct", p.buy_funding_max_pct)
    p.buy_funding_max_pct = None if bf is None else float(_to_float(bf, -0.01))
    p.buy_require_funding_decreasing = _to_bool(params.get("buy_require_funding_decreasing"), p.buy_require_funding_decreasing)
    p.sell_enable_funding = _to_bool(params.get("sell_enable_funding"), p.sell_enable_funding)
    sf = params.get("sell_funding_min_pct", p.sell_funding_min_pct)
    p.sell_funding_min_pct = None if sf is None else float(_to_float(sf, 0.0))

    sl = params.get("stop_loss_pct", p.stop_loss_pct)
    tp = params.get("take_profit_pct", p.take_profit_pct)
    p.stop_loss_pct = None if sl is None else _to_float(sl, 0.0)
    p.take_profit_pct = None if tp is None else _to_float(tp, 0.0)

    p.debug = _to_bool(params.get("debug"), p.debug)
    p.debug_top = max(0, _to_int(params.get("debug_top"), p.debug_top))

    return p


# =============================================================================
# main screener
# =============================================================================

class ScrOiBinance:
    """Open Interest screener (Binance)."""

    name = "scr_oi_binance"
    version = "0.1"

    def run(
        self,
        *,
        storage: PostgreSQLStorage,
        exchange_id: int,
        interval: str,
        params: Dict[str, Any],
    ) -> List[ScreenerSignal]:
        sp = _parse_params(params)
        interval = str(interval or sp.interval).strip() or sp.interval

        td = interval_to_timedelta(interval)
        if td.total_seconds() <= 0:
            raise ValueError(f"Invalid interval: {interval}")

        base_bars = max(5, int(sp.base_minutes * 60 // td.total_seconds()))
        trig_bars = max(1, int(sp.trigger_minutes * 60 // td.total_seconds()))
        need_bars = base_bars + trig_bars + 10  # запас

        symbols_map = storage.fetch_symbols_map(exchange_id=int(exchange_id), only_active=True)
        symbols = list(symbols_map.items())
        if sp.max_symbols and sp.max_symbols > 0:
            symbols = symbols[: int(sp.max_symbols)]

        now_ts = _utc_now()

        out: List[ScreenerSignal] = []
        dbg: List[Tuple[str, float, str]] = []

        stats = {
            "symbols_total": len(symbols),
            "candles_ok": 0,
            "oi_ok": 0,
            "vol_ok": 0,
            "base_ok": 0,
            "buy_oi_ok": 0,
            "buy_oi_value_ok": 0,
            "buy_vol_ok": 0,
            "buy_price_ok": 0,
            "buy_funding_ok": 0,
            "buy_signals": 0,
            "sell_price_ok": 0,
            "sell_rev_ok": 0,
            "sell_oi_ok": 0,
            "sell_vol_ok": 0,
            "sell_funding_ok": 0,
            "sell_signals": 0,
        }
        candidates: List[Dict[str, Any]] = []
        buy_fail_price: List[Dict[str, Any]] = []
        buy_fail_funding: List[Dict[str, Any]] = []
        funding_filter_disabled = False

        # Distribution helpers (for tuning thresholds)
        dist_all: Dict[str, List[float]] = {
            "oi_trig": [],
            "oi_value_trig": [],
            "vol_mult": [],
            "vol_mult_max": [], "price_trig": [],
            "base_range": [],
            "base_ret": [],
            "base_vol_ratio": [],
        }
        dist_base: Dict[str, List[float]] = {
            "oi_trig": [],
            "oi_value_trig": [],
            "vol_mult": [],
            "vol_mult_max": [], "price_trig": [],
            "base_range": [],
            "base_ret": [],
            "base_vol_ratio": [],
        }

        for symbol, symbol_id in symbols:
            symbol = str(symbol or "").strip().upper()
            if not symbol:
                continue
            try:
                symbol_id = int(symbol_id)
            except Exception:
                continue

            try:
                candles = storage.fetch_candles_window(
                    exchange_id=int(exchange_id),
                    symbol_id=int(symbol_id),
                    interval=str(interval),
                    center_ts=now_ts,
                    lookback=int(need_bars),
                    lookforward=0,
                ) or []
            except Exception:
                continue

            if len(candles) < (base_bars + trig_bars + 2):
                continue

            # normalize candle ts
            for c in candles:
                ts = _as_dt(c.get("ts"))
                if ts is None:
                    ts = _as_dt(c.get("open_time"))
                if ts is not None:
                    c["ts"] = ts

            candles.sort(key=lambda x: x.get("ts") or now_ts)

            last_closed_idx = _pick_last_closed_idx(candles, interval=interval, now_ts=now_ts)
            if last_closed_idx < 0:
                continue

            candles = candles[: last_closed_idx + 1]
            if len(candles) < (base_bars + trig_bars + 2):
                continue

            stats["candles_ok"] += 1

            base_slice = candles[-(base_bars + trig_bars):-trig_bars]
            trig_slice = candles[-trig_bars:]

            if len(base_slice) < max(10, base_bars // 2) or len(trig_slice) < max(1, trig_bars):
                continue

            last_close = _to_float(trig_slice[-1].get("close"), 0.0)
            if last_close <= 0:
                continue
            if last_close < sp.min_price or last_close > sp.max_price:
                continue

            # time bounds (для OI)
            start_ts = (candles[0].get("ts") or now_ts) - td * 2
            end_ts = (candles[-1].get("ts") or now_ts) + td * 2

            try:
                oi_series = _fetch_oi_series(
                    storage=storage,
                    exchange_id=int(exchange_id),
                    symbol_id=int(symbol_id),
                    interval=str(interval),
                    start_ts=start_ts,
                    end_ts=end_ts,
                )
            except Exception:
                oi_series = []

            # Align OI (calc key) and OI value (USDT) separately.
            oi_vals_calc = _align_series_to_candles(candles=candles, series=oi_series, value_key=sp.oi_calc_key)
            oi_vals_value = _align_series_to_candles(candles=candles, series=oi_series, value_key="open_interest_value")
            if not oi_vals_calc or oi_vals_calc[-1] is None:
                continue

            stats["oi_ok"] += 1

            base_oi = oi_vals_calc[-(base_bars + trig_bars):-trig_bars]
            trig_oi = oi_vals_calc[-trig_bars:]

            trig_oi_value = oi_vals_value[-trig_bars:] if oi_vals_value else []

            # volume
            base_vol = [_candle_quote_volume(c) for c in base_slice]
            trig_vol = [_candle_quote_volume(c) for c in trig_slice]

            base_vol_num = [float(v) for v in base_vol if v is not None and float(v) > 0]
            trig_vol_num = [float(v) for v in trig_vol if v is not None and float(v) > 0]

            base_vol_avg = _safe_mean(base_vol_num)
            base_vol_med = _safe_median(base_vol_num)
            base_vol_max = max(base_vol_num) if base_vol_num else None
            trig_vol_avg = _safe_mean(trig_vol_num)

            trig_vol_max = max(trig_vol_num) if trig_vol_num else None
            if base_vol_avg is None or base_vol_avg <= 0 or trig_vol_avg is None:
                continue

            stats["vol_ok"] += 1

            # Use median as a more robust baseline (less sensitive to a few spikes in the base window)
            base_vol_ref = base_vol_med if (base_vol_med is not None and base_vol_med > 0) else base_vol_avg
            trig_vol_mult = (trig_vol_avg / base_vol_ref) if (base_vol_ref is not None and base_vol_ref > 0) else None

            trig_vol_mult_max = (float(trig_vol_max) / float(base_vol_ref)) if (trig_vol_max is not None and base_vol_ref is not None and float(base_vol_ref) > 0) else None
            # base price regime
            base_hi = [_to_float(c.get("high"), 0.0) for c in base_slice]
            base_lo = [_to_float(c.get("low"), 0.0) for c in base_slice]
            base_cl = [_to_float(c.get("close"), 0.0) for c in base_slice]

            base_vals = [x for x in base_hi + base_lo + base_cl if x > 0]
            price_range_pct = _range_pct(base_vals)
            price_base_ret = _pct(base_cl[0], base_cl[-1]) if base_cl and base_cl[0] > 0 and base_cl[-1] > 0 else None

            # Collect base-window distributions
            if price_range_pct is not None:
                dist_all["base_range"].append(float(price_range_pct))
            if price_base_ret is not None:
                dist_all["base_ret"].append(float(price_base_ret))
            base_vol_ratio = None
            if base_vol_med is not None and base_vol_med > 0 and base_vol_max is not None:
                base_vol_ratio = float(base_vol_max) / float(base_vol_med)
                dist_all["base_vol_ratio"].append(float(base_vol_ratio))

            is_sideways = (price_range_pct is not None) and (price_range_pct <= float(sp.price_sideways_max_pct))
            is_downtrend = (price_base_ret is not None) and (price_base_ret <= -abs(float(sp.price_downtrend_min_pct)))

            # Base volume calmness (optional)
            base_vol_calm = True
            if sp.base_volume_max_mult is not None and base_vol_med is not None and base_vol_med > 0 and base_vol_max is not None:
                base_vol_calm = float(base_vol_max) <= float(base_vol_med) * float(sp.base_volume_max_mult)

            base_ok = (is_sideways or is_downtrend) and base_vol_calm
            if base_ok:
                stats["base_ok"] += 1
                if price_range_pct is not None:
                    dist_base["base_range"].append(float(price_range_pct))
                if price_base_ret is not None:
                    dist_base["base_ret"].append(float(price_base_ret))
                if base_vol_ratio is not None:
                    dist_base["base_vol_ratio"].append(float(base_vol_ratio))

            # trigger price
            trig_op = [_to_float(c.get("open"), 0.0) for c in trig_slice]
            trig_cl = [_to_float(c.get("close"), 0.0) for c in trig_slice]

            price_trig_ret = _pct(trig_cl[0], trig_cl[-1]) if trig_cl and trig_cl[0] > 0 and trig_cl[-1] > 0 else None
            green_cnt = sum(1 for i in range(len(trig_slice)) if trig_cl[i] >= trig_op[i])

            last_green = bool(trig_cl) and bool(trig_op) and (trig_cl[-1] >= trig_op[-1])
            # OI stats
            base_oi_f = [float(x) for x in base_oi if x is not None]
            trig_oi_f = [float(x) for x in trig_oi if x is not None]

            if len(trig_oi_f) < max(2, trig_bars - 1):
                continue

            oi_base_range_pct = _range_pct(base_oi_f) if base_oi_f else None
            oi_trig_ret_last = _pct(trig_oi_f[0], trig_oi_f[-1]) if trig_oi_f and trig_oi_f[0] != 0 else None
            oi_trig_ret = _pct(trig_oi_f[0], max(trig_oi_f)) if trig_oi_f and trig_oi_f[0] != 0 else None

            trig_oi_value_f = [float(x) for x in trig_oi_value if x is not None]
            oi_value_trig_ret_last = _pct(trig_oi_value_f[0], trig_oi_value_f[-1]) if trig_oi_value_f and trig_oi_value_f[0] != 0 else None
            oi_value_trig_ret = _pct(trig_oi_value_f[0], max(trig_oi_value_f)) if trig_oi_value_f and trig_oi_value_f[0] != 0 else None

            # Collect distributions (for threshold tuning)
            if oi_trig_ret is not None:
                dist_all["oi_trig"].append(float(oi_trig_ret))
            if oi_value_trig_ret is not None:
                dist_all["oi_value_trig"].append(float(oi_value_trig_ret))
            if trig_vol_mult is not None:
                dist_all["vol_mult"].append(float(trig_vol_mult))
            if trig_vol_mult_max is not None:
                dist_all["vol_mult_max"].append(float(trig_vol_mult_max))
            if price_trig_ret is not None:
                dist_all["price_trig"].append(float(price_trig_ret))
            if base_ok:
                if oi_trig_ret is not None:
                    dist_base["oi_trig"].append(float(oi_trig_ret))
                if oi_value_trig_ret is not None:
                    dist_base["oi_value_trig"].append(float(oi_value_trig_ret))
                if trig_vol_mult is not None:
                    dist_base["vol_mult"].append(float(trig_vol_mult))
                if trig_vol_mult_max is not None:
                    dist_base["vol_mult_max"].append(float(trig_vol_mult_max))
                if price_trig_ret is not None:
                    dist_base["price_trig"].append(float(price_trig_ret))

            oi_last = trig_oi_f[-1]
            oi_prev = trig_oi_f[-2] if len(trig_oi_f) >= 2 else None
            oi_drop_one = _pct(oi_prev, oi_last) if (oi_prev is not None and oi_prev != 0) else None

            # funding (для BUY)
            funding_last = None
            funding_prev = None
            funding_pct = None
            funding_prev_pct = None
            funding_ok_buy = True
            funding_ok_sell = True

            if (sp.enable_funding or sp.sell_enable_funding):
                try:
                    funding_last, funding_prev = _fetch_last_funding_two(
                        storage=storage,
                        exchange_id=int(exchange_id),
                        symbol_id=int(symbol_id),
                        as_of=end_ts,
                    )
                except Exception as e:
                    msg = str(e).lower()
                    # If funding table is missing (e.g. you removed it), gracefully disable the funding filter
                    if ("funding" in msg) and ("does not exist" in msg or "undefined table" in msg):
                        if not funding_filter_disabled:
                            log.warning("[OI] funding table not available; disable funding filter for this run")
                            funding_filter_disabled = True
                        sp.enable_funding = False
                        sp.sell_enable_funding = False
                        funding_last, funding_prev = None, None
                        funding_ok_buy = True
                    else:
                        funding_last, funding_prev = None, None


                if funding_last is None:
                    funding_ok_buy = False
                else:
                    fr = funding_last.get("funding_rate")
                    funding_pct = (float(fr) * 100.0) if fr is not None else None
                    if funding_prev is not None:
                        fr2 = funding_prev.get("funding_rate")
                        funding_prev_pct = (float(fr2) * 100.0) if fr2 is not None else None

                    if funding_pct is None or funding_pct >= 0.0:
                        funding_ok_buy = False
                    if funding_ok_buy and (sp.buy_funding_max_pct is not None):
                        funding_ok_buy = funding_pct <= float(sp.buy_funding_max_pct)

                    if funding_ok_buy and sp.buy_require_funding_decreasing:
                        if funding_prev_pct is None:
                            funding_ok_buy = False
                        else:
                            funding_ok_buy = funding_pct <= funding_prev_pct


            # funding (для SELL)
            if sp.sell_enable_funding:
                if funding_pct is None:
                    funding_ok_sell = False
                else:
                    min_pct = float(sp.sell_funding_min_pct or 0.0)
                    # для SELL ожидаем положительный funding
                    if not (float(funding_pct) > min_pct):
                        funding_ok_sell = False

            if sp.debug:
                candidates.append({
                    "symbol": symbol,
                    "price_range_pct": price_range_pct,
                    "price_base_ret_pct": price_base_ret,
                    "price_trig_ret_pct": price_trig_ret,
                    "green_cnt": int(green_cnt),
                    "oi_trig_ret_pct": oi_trig_ret,
                    "oi_value_trig_ret_pct": oi_value_trig_ret,
                    "oi_drop_one_pct": oi_drop_one,
                    "vol_mult": trig_vol_mult,
                    "vol_mult_max": trig_vol_mult_max,
                    "funding_pct": funding_pct,
                    "base_ok": bool(base_ok),
                    "base_vol_calm": bool(base_vol_calm),
                })

            mode = sp.signal_mode

            # -------- BUY --------
            if mode in ("both", "buy"):
                buy_ok = True

                if not base_ok:
                    buy_ok = False

                if buy_ok:
                    if oi_trig_ret is None or oi_trig_ret < float(sp.buy_oi_rise_pct):
                        buy_ok = False
                    else:
                        stats["buy_oi_ok"] += 1

                # Confirm via open_interest_value (USDT)
                if buy_ok and (sp.oi_value_confirm_pct is not None):
                    if oi_value_trig_ret is None or oi_value_trig_ret < float(sp.oi_value_confirm_pct):
                        buy_ok = False
                    else:
                        stats["buy_oi_value_ok"] += 1

                if buy_ok:
                    if trig_vol_mult is None or trig_vol_mult < float(sp.buy_volume_mult):
                        buy_ok = False
                    else:
                        stats["buy_vol_ok"] += 1

                if buy_ok:
                    buy_ok_before_price = True
                    if price_trig_ret is None or price_trig_ret < float(sp.buy_price_rise_pct):
                        buy_ok = False
                    if green_cnt < int(sp.buy_min_green_candles):
                        buy_ok = False
                    if sp.buy_require_last_green and not last_green:
                        buy_ok = False
                    if buy_ok_before_price and (not buy_ok) and sp.debug:
                        buy_fail_price.append({
                            "symbol": symbol,
                            "oi_pct": oi_trig_ret,
                            "oi_value_pct": oi_value_trig_ret,
                            "volx": trig_vol_mult,
                            "volmaxx": trig_vol_mult_max,
                            "price_pct": price_trig_ret,
                            "green_cnt": int(green_cnt),
                            "last_green": bool(last_green),
                            "funding": funding_pct,
                        })

                if buy_ok:
                    stats["buy_price_ok"] += 1
                if buy_ok and sp.enable_funding:
                    if not funding_ok_buy:
                        if sp.debug:
                            buy_fail_funding.append({
                                "symbol": symbol,
                                "oi_pct": oi_trig_ret,
                                "oi_value_pct": oi_value_trig_ret,
                                "volx": trig_vol_mult,
                                "volmaxx": trig_vol_mult_max,
                                "price_pct": price_trig_ret,
                                "green_cnt": int(green_cnt),
                                "last_green": bool(last_green),
                                "funding": funding_pct,
                            })
                        buy_ok = False
                    else:
                        stats["buy_funding_ok"] += 1

                if buy_ok:
                    entry_price = float(trig_cl[-1])
                    sl, tp = self._calc_sl_tp(side="BUY", entry=entry_price, sp=sp)

                    last_ts = _as_dt(trig_slice[-1].get("ts")) or now_ts
                    signal_ts = last_ts + td
                    if signal_ts > (now_ts + td * 0.10):
                        signal_ts = last_ts

                    oi_factor = min(1.0, float(oi_trig_ret) / max(1e-9, float(sp.buy_oi_rise_pct))) if oi_trig_ret is not None else 0.0
                    vol_factor = min(1.0, float(trig_vol_mult) / max(1e-9, float(sp.buy_volume_mult))) if trig_vol_mult is not None else 0.0
                    pr_factor = min(1.0, float(price_trig_ret) / max(1e-9, float(sp.buy_price_rise_pct))) if price_trig_ret is not None else 0.0
                    conf = max(0.0, min(1.0, (oi_factor + vol_factor + pr_factor) / 3.0))

                    reason_parts = [
                        f"base={'sideways' if is_sideways else 'downtrend'}",
                        f"OI {oi_trig_ret:.2f}%/{sp.trigger_minutes}m",
                        (f"OI$ {oi_value_trig_ret:.2f}%" if oi_value_trig_ret is not None else "OI$ n/a"),
                        f"Vol x{trig_vol_mult:.2f}",
                        f"Price {price_trig_ret:.2f}%/{sp.trigger_minutes}m",
                    ]
                    if funding_pct is not None:
                        reason_parts.append(f"Funding {funding_pct:+.4f}%")

                    ctx = {
                        "interval": interval,
                        "base_minutes": int(sp.base_minutes),
                        "trigger_minutes": int(sp.trigger_minutes),
                        "price_range_pct": price_range_pct,
                        "price_base_ret_pct": price_base_ret,
                        "price_trigger_ret_pct": price_trig_ret,
                        "green_candles": int(green_cnt),
                        "oi_calc_key": sp.oi_calc_key,
                        "oi_plot_key": sp.oi_plot_key,
                        "oi_base_range_pct": oi_base_range_pct,
                        "oi_trigger_ret_pct": oi_trig_ret,
                        "oi_value_trigger_ret_pct": oi_value_trig_ret,
                        "oi_value_confirm_pct": sp.oi_value_confirm_pct,
                        "base_vol_avg": float(base_vol_avg),
                        "base_vol_median": float(base_vol_med) if base_vol_med is not None else None,
                        "base_vol_calm": bool(base_vol_calm),
                        "trigger_vol_avg": float(trig_vol_avg),
                        "trigger_vol_mult": float(trig_vol_mult),
                        "funding_pct": funding_pct,
                        "funding_prev_pct": funding_prev_pct,
                        "funding_ts": funding_last.get("ts") if funding_last else None,
                        "funding_prev_ts": funding_prev.get("ts") if funding_prev else None,
                    }

                    stats["buy_signals"] += 1

                    out.append(
                        ScreenerSignal(
                            symbol_id=int(symbol_id),
                            symbol=symbol,
                            signal_ts=signal_ts,
                            side="BUY",
                            entry_price=entry_price,
                            stop_loss=sl,
                            take_profit=tp,
                            confidence=conf,
                            score=float(oi_trig_ret or 0.0),
                            reason=" | ".join(reason_parts),
                            context=ctx,
                        )
                    )

                    if sp.debug:
                        dbg.append((symbol, float(oi_trig_ret or 0.0), "BUY"))

            # -------- SELL --------
            if mode in ("both", "sell"):
                sell_ok = True

                if not base_ok:
                    sell_ok = False

                if sell_ok:
                    if trig_cl[-1] >= trig_op[-1]:
                        sell_ok = False
                    prev_close = trig_cl[-2] if len(trig_cl) >= 2 else trig_cl[-1]
                    last_drop_pct = _pct(prev_close, trig_cl[-1]) if prev_close > 0 else None
                    if last_drop_pct is None or last_drop_pct > -abs(float(sp.sell_price_drop_last_pct)):
                        sell_ok = False
                    else:
                        stats["sell_price_ok"] += 1

                if sell_ok:
                    mx = max(trig_cl) if trig_cl else None
                    if mx is None or mx <= 0:
                        sell_ok = False
                    else:
                        drop_from_max = _pct(mx, trig_cl[-1])
                        if drop_from_max is None or abs(drop_from_max) < abs(float(sp.sell_reversal_from_max_pct)):
                            sell_ok = False
                        else:
                            stats["sell_rev_ok"] += 1

                if sell_ok:
                    ok_oi = False
                    if oi_drop_one is not None and oi_drop_one <= -abs(float(sp.sell_oi_drop_one_candle_pct)):
                        ok_oi = True
                    if (not ok_oi) and (oi_trig_ret is not None) and (oi_trig_ret <= -abs(float(sp.sell_oi_drop_window_pct))):
                        ok_oi = True
                    if ok_oi:
                        stats["sell_oi_ok"] += 1
                    else:
                        sell_ok = False

                if sell_ok:
                    mv = max(trig_vol) if trig_vol else 0.0
                    last_v = trig_vol[-1] if trig_vol else 0.0
                    if mv <= 0:
                        sell_ok = False
                    else:
                        if mv < base_vol_avg * float(sp.sell_volume_spike_mult):
                            sell_ok = False
                        if last_v > mv * float(sp.sell_volume_drop_ratio):
                            sell_ok = False
                        if sell_ok:
                            stats["sell_vol_ok"] += 1

                if sell_ok:
                    if sp.sell_enable_funding:
                        if not funding_ok_sell:
                            sell_ok = False
                        else:
                            stats["sell_funding_ok"] += 1

                if sell_ok:
                    entry_price = float(trig_cl[-1])
                    sl, tp = self._calc_sl_tp(side="SELL", entry=entry_price, sp=sp)

                    last_ts = _as_dt(trig_slice[-1].get("ts")) or now_ts
                    signal_ts = last_ts + td
                    if signal_ts > (now_ts + td * 0.10):
                        signal_ts = last_ts

                    oi_strength = abs(float(oi_drop_one or oi_trig_ret or 0.0))
                    oi_factor = min(1.0, oi_strength / max(1e-9, float(sp.sell_oi_drop_one_candle_pct)))
                    vol_factor = 0.0
                    if trig_vol_mult is not None:
                        vol_factor = min(1.0, float(trig_vol_mult) / max(1e-9, float(sp.sell_volume_spike_mult)))
                    prev_close = trig_cl[-2] if len(trig_cl) >= 2 else trig_cl[-1]
                    last_drop_abs = abs(_pct(prev_close, trig_cl[-1]) or 0.0)
                    pr_factor = min(1.0, last_drop_abs / max(1e-9, float(sp.sell_price_drop_last_pct)))
                    conf = max(0.0, min(1.0, (oi_factor + vol_factor + pr_factor) / 3.0))

                    reason_parts = [
                        f"base={'sideways' if is_sideways else 'downtrend'}",
                        f"OI drop {oi_drop_one:.2f}%/1bar" if oi_drop_one is not None else f"OI {oi_trig_ret:.2f}%/{sp.trigger_minutes}m",
                        f"Vol peak->fade (x{trig_vol_mult:.2f})",
                    ]

                    ctx = {
                        "interval": interval,
                        "base_minutes": int(sp.base_minutes),
                        "trigger_minutes": int(sp.trigger_minutes),
                        "price_range_pct": price_range_pct,
                        "price_base_ret_pct": price_base_ret,
                        "price_trigger_ret_pct": price_trig_ret,
                        "oi_calc_key": sp.oi_calc_key,
                        "oi_plot_key": sp.oi_plot_key,
                        "oi_base_range_pct": oi_base_range_pct,
                        "oi_trigger_ret_pct": oi_trig_ret,
                        "oi_drop_one_pct": oi_drop_one,
                        "base_vol_avg": float(base_vol_avg),
                        "trigger_vol_avg": float(trig_vol_avg),
                        "trigger_vol_mult": float(trig_vol_mult or 0.0),
                        "funding_pct": funding_pct,
                        "funding_prev_pct": funding_prev_pct,
                        "funding_ts": funding_last.get("ts") if funding_last else None,
                        "funding_prev_ts": funding_prev.get("ts") if funding_prev else None,
                    }

                    stats["sell_signals"] += 1

                    out.append(
                        ScreenerSignal(
                            symbol_id=int(symbol_id),
                            symbol=symbol,
                            signal_ts=signal_ts,
                            side="SELL",
                            entry_price=entry_price,
                            stop_loss=sl,
                            take_profit=tp,
                            confidence=conf,
                            score=oi_strength,
                            reason=" | ".join(reason_parts),
                            context=ctx,
                        )
                    )

                    if sp.debug:
                        dbg.append((symbol, oi_strength, "SELL"))


        # Summary / diagnostics
        if sp.debug or not out:
            log.info(
                "[OI] stats: symbols=%d candles_ok=%d oi_ok=%d vol_ok=%d base_ok=%d | "
                "BUY: oi_ok=%d oi_value_ok=%d vol_ok=%d price_ok=%d funding_ok=%d signals=%d | "
                "SELL: price_ok=%d rev_ok=%d oi_ok=%d vol_ok=%d funding_ok=%d signals=%d",
                int(stats.get("symbols_total", 0)),
                int(stats.get("candles_ok", 0)),
                int(stats.get("oi_ok", 0)),
                int(stats.get("vol_ok", 0)),
                int(stats.get("base_ok", 0)),
                int(stats.get("buy_oi_ok", 0)),
                int(stats.get("buy_oi_value_ok", 0)),
                int(stats.get("buy_vol_ok", 0)),
                int(stats.get("buy_price_ok", 0)),
                int(stats.get("buy_funding_ok", 0)),
                int(stats.get("buy_signals", 0)),
                int(stats.get("sell_price_ok", 0)),
                int(stats.get("sell_rev_ok", 0)),
                int(stats.get("sell_oi_ok", 0)),
                int(stats.get("sell_vol_ok", 0)),
                int(stats.get("sell_funding_ok", 0)),
                int(stats.get("sell_signals", 0)),
            )

            # Percentile distributions to tune thresholds quickly
            def _dist_line(xs: List[float]) -> str:
                p50 = _percentile(xs, 50)
                p90 = _percentile(xs, 90)
                p99 = _percentile(xs, 99)
                if p50 is None:
                    return "n/a"
                return f"p50={p50:.2f} p90={p90:.2f} p99={p99:.2f}"

            def _dist_line_ret(xs: List[float]) -> str:
                # For returns it's useful to see downside tail too
                p50 = _percentile(xs, 50)
                p10 = _percentile(xs, 10)
                p1 = _percentile(xs, 1)
                if p50 is None:
                    return "n/a"
                return f"p50={p50:.2f} p10={p10:.2f} p1={p1:.2f}"

            log.info(
                "[OI] dist(all): oi_trig(%s) oi_value(%s) volx(%s) volmaxx(%s) price(%s)",
                _dist_line(dist_all["oi_trig"]),
                _dist_line(dist_all["oi_value_trig"]),
                _dist_line(dist_all["vol_mult"]),
                _dist_line(dist_all["vol_mult_max"]),
                _dist_line(dist_all["price_trig"]),
            )
            log.info(
                "[OI] base(all): range(%s) ret(%s) vol_ratio(%s)",
                _dist_line(dist_all["base_range"]),
                _dist_line_ret(dist_all["base_ret"]),
                _dist_line(dist_all["base_vol_ratio"]),
            )
            log.info(
                "[OI] dist(base_ok): oi_trig(%s) oi_value(%s) volx(%s) volmaxx(%s) price(%s)",
                _dist_line(dist_base["oi_trig"]),
                _dist_line(dist_base["oi_value_trig"]),
                _dist_line(dist_base["vol_mult"]),
                _dist_line(dist_base["vol_mult_max"]),
                _dist_line(dist_base["price_trig"]),
            )
            log.info(
                "[OI] base(base_ok): range(%s) ret(%s) vol_ratio(%s)",
                _dist_line(dist_base["base_range"]),
                _dist_line_ret(dist_base["base_ret"]),
                _dist_line(dist_base["base_vol_ratio"]),
            )

        if sp.debug:
            # Show top candidates even if no signals
            if candidates:
                # top by OI change
                buy_c = [c for c in candidates if c.get("oi_trig_ret_pct") is not None]
                buy_c.sort(key=lambda x: float(x.get("oi_trig_ret_pct") or 0.0), reverse=True)
                log.info("[OI] top OI change (trigger window):")
                for c in buy_c[: int(sp.debug_top)]:
                    log.info(
                        "  %s oi=%.2f%% volx=%.2f volmaxx=%.2f price=%.2f%% base_ok=%s funding=%s",
                        c.get("symbol"),
                        float(c.get("oi_trig_ret_pct") or 0.0),
                        float(c.get("vol_mult") or 0.0),
                        float(c.get("vol_mult_max") or 0.0),
                        float(c.get("price_trig_ret_pct") or 0.0),
                        str(bool(c.get("base_ok"))),
                        str(c.get("funding_pct")),
                    )

                # top by volume multiplier
                vol_c = [c for c in candidates if (c.get("vol_mult") is not None) or (c.get("vol_mult_max") is not None)]
                vol_c.sort(key=lambda x: max(float(x.get("vol_mult") or 0.0), float(x.get("vol_mult_max") or 0.0)), reverse=True)
                log.info("[OI] top volume mult (trigger/base):")
                for c in vol_c[: int(sp.debug_top)]:
                    log.info(
                        "  %s volx=%.2f volmaxx=%.2f oi=%.2f%% price=%.2f%% base_ok=%s funding=%s",
                        c.get("symbol"),
                        float(c.get("vol_mult") or 0.0),
                        float(c.get("vol_mult_max") or 0.0),
                        float(c.get("oi_trig_ret_pct") or 0.0),
                        float(c.get("price_trig_ret_pct") or 0.0),
                        str(bool(c.get("base_ok"))),
                        str(c.get("funding_pct")),
                    )

        return out

    def _calc_sl_tp(self, *, side: str, entry: float, sp: ScrParams) -> Tuple[Optional[float], Optional[float]]:
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
