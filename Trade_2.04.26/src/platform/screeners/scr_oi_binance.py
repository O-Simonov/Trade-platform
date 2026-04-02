# src/platform/screeners/scr_oi_binance.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

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


def _fetch_batch_candles(
    *,
    storage: PostgreSQLStorage,
    exchange_id: int,
    symbol_ids: Iterable[int],
    interval: str,
    start_ts: datetime,
    end_ts: datetime,
) -> Dict[int, List[Dict[str, Any]]]:
    ids = [int(x) for x in symbol_ids]
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
        sid = _to_int(rec.get("symbol_id"), 0)
        ts = _as_dt(rec.get("ts")) or _as_dt(rec.get("open_time"))
        if ts is not None:
            rec["ts"] = ts
        out.setdefault(sid, []).append(rec)
    return out


def _fetch_batch_oi_series(
    *,
    storage: PostgreSQLStorage,
    exchange_id: int,
    symbol_ids: Iterable[int],
    interval: str,
    start_ts: datetime,
    end_ts: datetime,
) -> Dict[int, List[Dict[str, Any]]]:
    ids = [int(x) for x in symbol_ids]
    if not ids:
        return {}
    q = """
        SELECT symbol_id, ts, open_interest, open_interest_value
        FROM open_interest
        WHERE exchange_id = %s
          AND interval = %s
          AND symbol_id = ANY(%s)
          AND ts >= %s
          AND ts <= %s
        ORDER BY symbol_id, ts
    """
    out: Dict[int, List[Dict[str, Any]]] = {}
    with storage.pool.connection() as con:
        with con.cursor() as cur:
            cur.execute(q, (int(exchange_id), str(interval), ids, start_ts, end_ts))
            rows = cur.fetchall() or []
            cols = [d[0] for d in cur.description]
    for row in rows:
        rec = dict(zip(cols, row))
        sid = _to_int(rec.get("symbol_id"), 0)
        ts = _as_dt(rec.get("ts"))
        if ts is not None:
            rec["ts"] = ts
        out.setdefault(sid, []).append(rec)
    return out


def _fetch_batch_last_funding_two(
    *,
    storage: PostgreSQLStorage,
    exchange_id: int,
    symbol_ids: Iterable[int],
    as_of: datetime,
) -> Dict[int, Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]]:
    ids = [int(x) for x in symbol_ids]
    if not ids:
        return {}
    q = """
        WITH ranked AS (
            SELECT
                symbol_id,
                funding_time AS ts,
                funding_rate,
                ROW_NUMBER() OVER (PARTITION BY symbol_id ORDER BY funding_time DESC) AS rn
            FROM funding
            WHERE exchange_id = %s
              AND symbol_id = ANY(%s)
              AND funding_time <= %s
        )
        SELECT symbol_id, ts, funding_rate, rn
        FROM ranked
        WHERE rn <= 2
        ORDER BY symbol_id, rn
    """
    out: Dict[int, Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]] = {int(sid): (None, None) for sid in ids}
    with storage.pool.connection() as con:
        with con.cursor() as cur:
            cur.execute(q, (int(exchange_id), ids, as_of))
            rows = cur.fetchall() or []
            cols = [d[0] for d in cur.description]
    grouped: Dict[int, List[Dict[str, Any]]] = {}
    for row in rows:
        rec = dict(zip(cols, row))
        sid = _to_int(rec.get("symbol_id"), 0)
        ts = _as_dt(rec.get("ts"))
        if ts is not None:
            rec["ts"] = ts
        grouped.setdefault(sid, []).append(rec)
    for sid, items in grouped.items():
        items.sort(key=lambda x: _to_int(x.get("rn"), 999))
        last = {"ts": items[0].get("ts"), "funding_rate": items[0].get("funding_rate")} if len(items) >= 1 else None
        prev = {"ts": items[1].get("ts"), "funding_rate": items[1].get("funding_rate")} if len(items) >= 2 else None
        out[int(sid)] = (last, prev)
    return out


def _true_range_pct(prev_close: float, high: float, low: float) -> Optional[float]:
    vals = [v for v in (prev_close, high, low) if v is not None and float(v) > 0]
    if len(vals) < 3:
        return None
    tr = max(float(high) - float(low), abs(float(high) - float(prev_close)), abs(float(low) - float(prev_close)))
    if float(prev_close) <= 0:
        return None
    return (tr / float(prev_close)) * 100.0


def _atr_pct(candles: List[Dict[str, Any]], period: int = 14) -> Optional[float]:
    if len(candles) < max(3, period + 1):
        return None
    trs: List[float] = []
    for i in range(1, len(candles)):
        prev_close = _to_float(candles[i - 1].get("close"), 0.0)
        high = _to_float(candles[i].get("high"), 0.0)
        low = _to_float(candles[i].get("low"), 0.0)
        trp = _true_range_pct(prev_close, high, low)
        if trp is not None:
            trs.append(float(trp))
    if not trs:
        return None
    tail = trs[-max(1, int(period)):]
    return _safe_mean(tail)


def _ratio_or_none(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None:
        return None
    bb = float(b)
    if bb == 0.0:
        return None
    return float(a) / bb


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
    signal_profile: Optional[str] = None


@dataclass
class ScrParams:
    # general
    interval: str = "5m"
    min_price: float = 1e-7
    max_price: float = 100000.0
    signal_mode: str = "both"  # both|buy|sell
    max_symbols: int = 0

    # liquidity / quality
    min_base_quote_volume: Optional[float] = None
    min_oi_value: Optional[float] = None
    buy_min_oi_last_rise_pct: Optional[float] = None
    max_oi_drawdown_from_peak_pct: Optional[float] = None
    min_signal_score: float = 0.0

    # volatility normalization
    use_atr_normalization: bool = False
    atr_period: int = 14
    buy_price_rise_atr_mult: Optional[float] = None
    buy_price_max_rise_atr_mult: Optional[float] = None

    # windows
    base_minutes: int = 240
    trigger_minutes: int = 20

    # BASE volume calmness
    base_volume_max_mult: Optional[float] = 2.0

    # PRICE base regime
    price_sideways_max_pct: float = 1.2
    price_downtrend_min_pct: float = 1.0

    # replay / freshness
    max_signal_age_minutes: int = 30

    # BUY base regime tuning
    buy_allow_downtrend_base: bool = False

    # BUY trigger (5m)
    buy_oi_rise_pct: float = 3.0
    oi_value_confirm_pct: Optional[float] = None
    buy_volume_mult: float = 3.0
    buy_volume_spike_mult: float = 3.0
    buy_require_volume_spike: bool = True
    buy_price_rise_pct: float = 0.3
    buy_price_max_rise_pct: Optional[float] = 3.0
    buy_min_green_candles: int = 2
    buy_require_last_green: bool = True
    enable_early_buy: bool = True
    enable_confirmed_buy: bool = True
    early_buy_min_green_candles: Optional[int] = None
    early_buy_require_last_green: bool = False
    early_buy_volume_mult_ratio: float = 0.85
    early_buy_volume_spike_ratio: float = 0.85
    early_buy_price_rise_pct_ratio: float = 0.65
    early_buy_price_rise_atr_mult_ratio: float = 0.65
    early_buy_min_signal_score: Optional[float] = None

    # SELL trigger
    sell_oi_drop_one_candle_pct: float = 1.0
    sell_oi_drop_window_pct: float = 1.5
    sell_price_drop_last_pct: float = 0.5
    sell_volume_spike_mult: float = 2.5
    sell_volume_drop_ratio: float = 0.45
    sell_reversal_from_max_pct: float = 0.7

    # OI keys
    oi_calc_key: str = "open_interest"
    oi_plot_key: str = "open_interest_value"

    # Funding filter (BUY)
    enable_funding: bool = True
    buy_funding_max_pct: Optional[float] = -0.01
    buy_require_funding_decreasing: bool = False

    # Funding filter (SELL)
    sell_enable_funding: bool = True
    sell_funding_min_pct: Optional[float] = 0.0

    # SL/TP
    stop_loss_pct: Optional[float] = 2.0
    take_profit_pct: Optional[float] = 5.0

    # replay / historical generation
    replay_as_of_ts: Optional[datetime] = None

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

    mbqv = params.get("min_base_quote_volume", p.min_base_quote_volume)
    p.min_base_quote_volume = None if mbqv is None else _to_float(mbqv, 0.0)
    moi = params.get("min_oi_value", p.min_oi_value)
    p.min_oi_value = None if moi is None else _to_float(moi, 0.0)
    moil = params.get("buy_min_oi_last_rise_pct", p.buy_min_oi_last_rise_pct)
    p.buy_min_oi_last_rise_pct = None if moil is None else _to_float(moil, 0.0)
    modp = params.get("max_oi_drawdown_from_peak_pct", p.max_oi_drawdown_from_peak_pct)
    p.max_oi_drawdown_from_peak_pct = None if modp is None else _to_float(modp, 0.0)
    p.min_signal_score = max(0.0, _to_float(params.get("min_signal_score"), p.min_signal_score))

    p.use_atr_normalization = _to_bool(params.get("use_atr_normalization"), p.use_atr_normalization)
    p.atr_period = max(2, _to_int(params.get("atr_period"), p.atr_period))
    bparm = params.get("buy_price_rise_atr_mult", p.buy_price_rise_atr_mult)
    p.buy_price_rise_atr_mult = None if bparm is None else _to_float(bparm, 0.0)
    bparmx = params.get("buy_price_max_rise_atr_mult", p.buy_price_max_rise_atr_mult)
    p.buy_price_max_rise_atr_mult = None if bparmx is None else _to_float(bparmx, 0.0)

    p.base_minutes = max(60, _to_int(params.get("base_minutes"), p.base_minutes))
    p.trigger_minutes = max(5, _to_int(params.get("trigger_minutes"), p.trigger_minutes))
    p.max_signal_age_minutes = max(1, _to_int(params.get("max_signal_age_minutes"), 30))

    bvm = params.get("base_volume_max_mult", p.base_volume_max_mult)
    p.base_volume_max_mult = None if bvm is None else _to_float(bvm, 2.0)

    p.price_sideways_max_pct = _to_float(params.get("price_sideways_max_pct"), p.price_sideways_max_pct)
    p.price_downtrend_min_pct = _to_float(params.get("price_downtrend_min_pct"), p.price_downtrend_min_pct)

    p.buy_allow_downtrend_base = _to_bool(
        params.get("buy_allow_downtrend_base"),
        p.buy_allow_downtrend_base,
    )

    p.buy_oi_rise_pct = _to_float(params.get("buy_oi_rise_pct"), p.buy_oi_rise_pct)
    oivc = params.get("oi_value_confirm_pct", p.oi_value_confirm_pct)
    p.oi_value_confirm_pct = None if oivc is None else _to_float(oivc, 0.0)

    p.buy_volume_mult = _to_float(params.get("buy_volume_mult"), p.buy_volume_mult)
    p.buy_volume_spike_mult = _to_float(params.get("buy_volume_spike_mult"), p.buy_volume_spike_mult)
    p.buy_require_volume_spike = _to_bool(
        params.get("buy_require_volume_spike"),
        p.buy_require_volume_spike,
    )

    p.buy_price_rise_pct = _to_float(params.get("buy_price_rise_pct"), p.buy_price_rise_pct)

    bmx = params.get("buy_price_max_rise_pct", p.buy_price_max_rise_pct)
    p.buy_price_max_rise_pct = None if bmx is None else _to_float(bmx, 3.0)

    p.buy_min_green_candles = max(0, _to_int(params.get("buy_min_green_candles"), p.buy_min_green_candles))
    p.buy_require_last_green = _to_bool(params.get("buy_require_last_green"), p.buy_require_last_green)
    p.enable_early_buy = _to_bool(params.get("enable_early_buy"), p.enable_early_buy)
    p.enable_confirmed_buy = _to_bool(params.get("enable_confirmed_buy"), p.enable_confirmed_buy)
    ebmg = params.get("early_buy_min_green_candles", p.early_buy_min_green_candles)
    p.early_buy_min_green_candles = None if ebmg is None else max(0, _to_int(ebmg, 0))
    p.early_buy_require_last_green = _to_bool(params.get("early_buy_require_last_green"), p.early_buy_require_last_green)
    p.early_buy_volume_mult_ratio = max(0.1, _to_float(params.get("early_buy_volume_mult_ratio"), p.early_buy_volume_mult_ratio))
    p.early_buy_volume_spike_ratio = max(0.1, _to_float(params.get("early_buy_volume_spike_ratio"), p.early_buy_volume_spike_ratio))
    p.early_buy_price_rise_pct_ratio = max(0.1, _to_float(params.get("early_buy_price_rise_pct_ratio"), p.early_buy_price_rise_pct_ratio))
    p.early_buy_price_rise_atr_mult_ratio = max(0.1, _to_float(params.get("early_buy_price_rise_atr_mult_ratio"), p.early_buy_price_rise_atr_mult_ratio))
    ebss = params.get("early_buy_min_signal_score", p.early_buy_min_signal_score)
    p.early_buy_min_signal_score = None if ebss is None else max(0.0, _to_float(ebss, 0.0))

    p.sell_oi_drop_one_candle_pct = _to_float(params.get("sell_oi_drop_one_candle_pct"), p.sell_oi_drop_one_candle_pct)
    p.sell_oi_drop_window_pct = _to_float(params.get("sell_oi_drop_window_pct"), p.sell_oi_drop_window_pct)
    p.sell_price_drop_last_pct = _to_float(params.get("sell_price_drop_last_pct"), p.sell_price_drop_last_pct)
    p.sell_volume_spike_mult = _to_float(params.get("sell_volume_spike_mult"), p.sell_volume_spike_mult)
    p.sell_volume_drop_ratio = _to_float(params.get("sell_volume_drop_ratio"), p.sell_volume_drop_ratio)
    p.sell_reversal_from_max_pct = _to_float(params.get("sell_reversal_from_max_pct"), p.sell_reversal_from_max_pct)

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
    p.buy_require_funding_decreasing = _to_bool(
        params.get("buy_require_funding_decreasing"),
        p.buy_require_funding_decreasing,
    )

    p.sell_enable_funding = _to_bool(params.get("sell_enable_funding"), p.sell_enable_funding)
    sf = params.get("sell_funding_min_pct", p.sell_funding_min_pct)
    p.sell_funding_min_pct = None if sf is None else float(_to_float(sf, 0.0))

    sl = params.get("stop_loss_pct", p.stop_loss_pct)
    tp = params.get("take_profit_pct", p.take_profit_pct)
    p.stop_loss_pct = None if sl is None else _to_float(sl, 0.0)
    p.take_profit_pct = None if tp is None else _to_float(tp, 0.0)

    replay_as_of_raw = params.get("replay_as_of_ts", params.get("as_of_ts"))
    p.replay_as_of_ts = _as_dt(replay_as_of_raw) if replay_as_of_raw is not None else None

    p.debug = _to_bool(params.get("debug"), p.debug)
    p.debug_top = max(0, _to_int(params.get("debug_top"), p.debug_top))

    return p


# =============================================================================
# main screener
# =============================================================================

class ScrOiBinance:
    """Open Interest screener (Binance)."""

    name = "scr_oi_binance"
    version = "0.2"

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
        need_bars = base_bars + trig_bars + max(10, sp.atr_period + 2)

        symbols_map = storage.fetch_symbols_map(exchange_id=int(exchange_id), only_active=True)
        symbols = list(symbols_map.items())
        if sp.max_symbols and sp.max_symbols > 0:
            symbols = symbols[: int(sp.max_symbols)]
        symbol_rows: List[Tuple[str, int]] = []
        symbol_ids: List[int] = []
        for symbol, symbol_id in symbols:
            symbol = str(symbol or "").strip().upper()
            if not symbol:
                continue
            try:
                sid = int(symbol_id)
            except Exception:
                continue
            symbol_rows.append((symbol, sid))
            symbol_ids.append(sid)

        now_ts = sp.replay_as_of_ts or _utc_now()
        fetch_start_ts = now_ts - td * (need_bars + 2)
        fetch_end_ts = now_ts + td * 2

        candles_map = _fetch_batch_candles(
            storage=storage,
            exchange_id=int(exchange_id),
            symbol_ids=symbol_ids,
            interval=str(interval),
            start_ts=fetch_start_ts,
            end_ts=fetch_end_ts,
        )
        oi_map = _fetch_batch_oi_series(
            storage=storage,
            exchange_id=int(exchange_id),
            symbol_ids=symbol_ids,
            interval=str(interval),
            start_ts=fetch_start_ts - td * 2,
            end_ts=fetch_end_ts + td * 2,
        )

        funding_map: Dict[int, Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]] = {}
        funding_filter_disabled = False
        if sp.enable_funding or sp.sell_enable_funding:
            try:
                funding_map = _fetch_batch_last_funding_two(
                    storage=storage,
                    exchange_id=int(exchange_id),
                    symbol_ids=symbol_ids,
                    as_of=fetch_end_ts + td * 2,
                )
            except Exception as e:
                msg = str(e).lower()
                if ("funding" in msg) and ("does not exist" in msg or "undefined table" in msg):
                    log.warning("[OI] funding table not available; disable funding filter for this run")
                    funding_filter_disabled = True
                    sp.enable_funding = False
                    sp.sell_enable_funding = False
                else:
                    raise

        out: List[ScreenerSignal] = []
        dbg: List[Tuple[str, float, str]] = []
        stats = {
            "symbols_total": len(symbol_rows),
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
            "liquidity_ok": 0,
        }
        candidates: List[Dict[str, Any]] = []
        buy_fail_price: List[Dict[str, Any]] = []
        buy_fail_funding: List[Dict[str, Any]] = []

        dist_all: Dict[str, List[float]] = {
            "oi_trig": [], "oi_value_trig": [], "vol_mult": [], "vol_mult_max": [],
            "price_trig": [], "base_range": [], "base_ret": [], "base_vol_ratio": [],
        }
        dist_base: Dict[str, List[float]] = {k: [] for k in dist_all.keys()}

        for symbol, symbol_id in symbol_rows:
            candles = list(candles_map.get(int(symbol_id), []) or [])
            if len(candles) < (base_bars + trig_bars + 2):
                continue
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
            if last_close <= 0 or last_close < sp.min_price or last_close > sp.max_price:
                continue

            oi_series = oi_map.get(int(symbol_id), []) or []
            oi_vals_calc = _align_series_to_candles(candles=candles, series=oi_series, value_key=sp.oi_calc_key)
            oi_vals_value = _align_series_to_candles(candles=candles, series=oi_series, value_key="open_interest_value")
            if not oi_vals_calc or oi_vals_calc[-1] is None:
                continue
            stats["oi_ok"] += 1

            base_oi = oi_vals_calc[-(base_bars + trig_bars):-trig_bars]
            trig_oi = oi_vals_calc[-trig_bars:]
            trig_oi_value = oi_vals_value[-trig_bars:] if oi_vals_value else []

            base_vol = [_candle_quote_volume(c) for c in base_slice]
            trig_vol = [_candle_quote_volume(c) for c in trig_slice]
            base_vol_num = [float(v) for v in base_vol if float(v) > 0]
            trig_vol_num = [float(v) for v in trig_vol if float(v) > 0]
            base_vol_avg = _safe_mean(base_vol_num)
            base_vol_med = _safe_median(base_vol_num)
            base_vol_max = max(base_vol_num) if base_vol_num else None
            trig_vol_avg = _safe_mean(trig_vol_num)
            trig_vol_max = max(trig_vol_num) if trig_vol_num else None
            if base_vol_avg is None or base_vol_avg <= 0 or trig_vol_avg is None:
                continue
            stats["vol_ok"] += 1

            base_vol_ref = base_vol_med if (base_vol_med is not None and base_vol_med > 0) else base_vol_avg
            trig_vol_mult = _ratio_or_none(trig_vol_avg, base_vol_ref)
            trig_vol_mult_max = _ratio_or_none(trig_vol_max, base_vol_ref)

            trig_oi_f = [float(x) for x in trig_oi if x is not None]
            if len(trig_oi_f) < max(2, trig_bars - 1):
                continue
            trig_oi_value_f = [float(x) for x in trig_oi_value if x is not None]
            oi_last_value = trig_oi_value_f[-1] if trig_oi_value_f else None
            if sp.min_base_quote_volume is not None and (base_vol_med is None or base_vol_med < float(sp.min_base_quote_volume)):
                continue
            if sp.min_oi_value is not None and (oi_last_value is None or oi_last_value < float(sp.min_oi_value)):
                continue
            stats["liquidity_ok"] += 1

            base_hi = [_to_float(c.get("high"), 0.0) for c in base_slice]
            base_lo = [_to_float(c.get("low"), 0.0) for c in base_slice]
            base_cl = [_to_float(c.get("close"), 0.0) for c in base_slice]
            base_vals = [x for x in base_hi + base_lo + base_cl if x > 0]
            price_range_pct = _range_pct(base_vals)
            price_base_ret = _pct(base_cl[0], base_cl[-1]) if base_cl and base_cl[0] > 0 and base_cl[-1] > 0 else None
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
            base_vol_calm = True
            if sp.base_volume_max_mult is not None and base_vol_med is not None and base_vol_med > 0 and base_vol_max is not None:
                base_vol_calm = float(base_vol_max) <= float(base_vol_med) * float(sp.base_volume_max_mult)
            base_ok_buy = (is_sideways or (sp.buy_allow_downtrend_base and is_downtrend)) and base_vol_calm
            base_ok_sell = (is_sideways or is_downtrend) and base_vol_calm
            if base_ok_buy or base_ok_sell:
                stats["base_ok"] += 1
                if price_range_pct is not None:
                    dist_base["base_range"].append(float(price_range_pct))
                if price_base_ret is not None:
                    dist_base["base_ret"].append(float(price_base_ret))
                if base_vol_ratio is not None:
                    dist_base["base_vol_ratio"].append(float(base_vol_ratio))

            trig_op = [_to_float(c.get("open"), 0.0) for c in trig_slice]
            trig_cl = [_to_float(c.get("close"), 0.0) for c in trig_slice]
            trig_hi = [_to_float(c.get("high"), 0.0) for c in trig_slice]
            price_trig_ret = _pct(trig_cl[0], trig_cl[-1]) if trig_cl and trig_cl[0] > 0 and trig_cl[-1] > 0 else None
            green_cnt = sum(1 for i in range(len(trig_slice)) if trig_cl[i] >= trig_op[i])
            last_green = bool(trig_cl) and bool(trig_op) and (trig_cl[-1] >= trig_op[-1])

            base_oi_f = [float(x) for x in base_oi if x is not None]
            oi_base_range_pct = _range_pct(base_oi_f) if base_oi_f else None
            oi_trig_ret_last = _pct(trig_oi_f[0], trig_oi_f[-1]) if trig_oi_f[0] != 0 else None
            oi_trig_ret = _pct(trig_oi_f[0], max(trig_oi_f)) if trig_oi_f[0] != 0 else None
            oi_peak = max(trig_oi_f) if trig_oi_f else None
            oi_drawdown_from_peak_pct = _pct(oi_peak, trig_oi_f[-1]) if oi_peak and oi_peak > 0 else None
            oi_up_bars = sum(1 for i in range(1, len(trig_oi_f)) if trig_oi_f[i] >= trig_oi_f[i - 1])
            oi_up_ratio = float(oi_up_bars) / float(max(1, len(trig_oi_f) - 1))

            oi_value_trig_ret_last = _pct(trig_oi_value_f[0], trig_oi_value_f[-1]) if trig_oi_value_f and trig_oi_value_f[0] != 0 else None
            oi_value_trig_ret = _pct(trig_oi_value_f[0], max(trig_oi_value_f)) if trig_oi_value_f and trig_oi_value_f[0] != 0 else None

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
            if base_ok_buy or base_ok_sell:
                for k, v in (("oi_trig", oi_trig_ret), ("oi_value_trig", oi_value_trig_ret), ("vol_mult", trig_vol_mult), ("vol_mult_max", trig_vol_mult_max), ("price_trig", price_trig_ret)):
                    if v is not None:
                        dist_base[k].append(float(v))

            atr_pct = _atr_pct(candles[-max(base_bars, sp.atr_period + trig_bars + 2):], period=sp.atr_period)
            price_move_atr = _ratio_or_none(price_trig_ret, atr_pct) if price_trig_ret is not None else None
            oi_last = trig_oi_f[-1]
            oi_prev = trig_oi_f[-2] if len(trig_oi_f) >= 2 else None
            oi_drop_one = _pct(oi_prev, oi_last) if (oi_prev is not None and oi_prev != 0) else None

            funding_last, funding_prev = funding_map.get(int(symbol_id), (None, None))
            funding_pct = None
            funding_prev_pct = None
            funding_ok_buy = True
            funding_ok_sell = True
            if (sp.enable_funding or sp.sell_enable_funding) and not funding_filter_disabled:
                if funding_last is None:
                    funding_ok_buy = False
                    funding_ok_sell = False
                else:
                    fr = funding_last.get("funding_rate")
                    funding_pct = (float(fr) * 100.0) if fr is not None else None
                    if funding_prev is not None:
                        fr2 = funding_prev.get("funding_rate")
                        funding_prev_pct = (float(fr2) * 100.0) if fr2 is not None else None
                    funding_ok_buy = funding_pct is not None and funding_pct < 0.0
                    if funding_ok_buy and (sp.buy_funding_max_pct is not None):
                        funding_ok_buy = funding_pct <= float(sp.buy_funding_max_pct)
                    if funding_ok_buy and sp.buy_require_funding_decreasing:
                        funding_ok_buy = funding_prev_pct is not None and funding_pct <= funding_prev_pct
                    if sp.sell_enable_funding:
                        funding_ok_sell = funding_pct is not None and float(funding_pct) > float(sp.sell_funding_min_pct or 0.0)

            funding_buy_bonus = 0.0
            funding_sell_bonus = 0.0
            if funding_pct is not None:
                funding_buy_bonus = min(1.0, abs(min(0.0, funding_pct)) / max(1e-9, abs(float(sp.buy_funding_max_pct or -0.01))))
                funding_sell_bonus = min(1.0, max(0.0, funding_pct) / max(1e-9, abs(float(sp.sell_funding_min_pct or 0.01))))

            if sp.debug:
                candidates.append({
                    "symbol": symbol, "price_range_pct": price_range_pct, "price_base_ret_pct": price_base_ret,
                    "price_trig_ret_pct": price_trig_ret, "price_move_atr": price_move_atr, "green_cnt": int(green_cnt),
                    "oi_trig_ret_pct": oi_trig_ret, "oi_trig_ret_last_pct": oi_trig_ret_last,
                    "oi_drawdown_from_peak_pct": oi_drawdown_from_peak_pct, "oi_up_ratio": oi_up_ratio,
                    "oi_value_trig_ret_pct": oi_value_trig_ret, "oi_drop_one_pct": oi_drop_one,
                    "vol_mult": trig_vol_mult, "vol_mult_max": trig_vol_mult_max, "funding_pct": funding_pct,
                    "base_ok_buy": bool(base_ok_buy), "base_ok_sell": bool(base_ok_sell),
                    "base_vol_calm": bool(base_vol_calm), "base_vol_median": base_vol_med, "oi_last_value": oi_last_value,
                })

            mode = sp.signal_mode

            if mode in ("both", "buy"):
                confirmed_ok = base_ok_buy
                early_ok = base_ok_buy

                if confirmed_ok or early_ok:
                    oi_strong = (oi_trig_ret is not None) and (oi_trig_ret >= float(sp.buy_oi_rise_pct))
                    oi_last_min = float(sp.buy_min_oi_last_rise_pct or sp.buy_oi_rise_pct)
                    oi_hold_ok = (oi_trig_ret_last is not None) and (oi_trig_ret_last >= oi_last_min)
                    oi_dd_ok = (sp.max_oi_drawdown_from_peak_pct is None) or (oi_drawdown_from_peak_pct is not None and oi_drawdown_from_peak_pct <= float(sp.max_oi_drawdown_from_peak_pct))
                    if oi_strong and oi_hold_ok and oi_dd_ok:
                        stats["buy_oi_ok"] += 1
                    else:
                        confirmed_ok = False
                    early_oi_hold_min = min(float(sp.buy_oi_rise_pct), oi_last_min) * 0.75
                    early_oi_dd_max = float(sp.max_oi_drawdown_from_peak_pct or 1.5) * 1.35
                    if not (oi_strong and (oi_trig_ret_last is not None) and (oi_trig_ret_last >= early_oi_hold_min) and (oi_drawdown_from_peak_pct is not None) and (oi_drawdown_from_peak_pct <= early_oi_dd_max)):
                        early_ok = False

                if confirmed_ok or early_ok:
                    oi_value_ok = True
                    if sp.oi_value_confirm_pct is not None:
                        oi_value_ok = (oi_value_trig_ret is not None) and (oi_value_trig_ret >= float(sp.oi_value_confirm_pct))
                    if oi_value_ok:
                        stats["buy_oi_value_ok"] += 1
                    else:
                        confirmed_ok = False
                    early_oi_value_req = None if sp.oi_value_confirm_pct is None else float(sp.oi_value_confirm_pct) * 0.75
                    if early_oi_value_req is not None and not ((oi_value_trig_ret_last is not None and oi_value_trig_ret_last >= early_oi_value_req) or (oi_value_trig_ret is not None and oi_value_trig_ret >= early_oi_value_req)):
                        early_ok = False

                if confirmed_ok or early_ok:
                    confirmed_vol_ok = (trig_vol_mult is not None and trig_vol_mult >= float(sp.buy_volume_mult))
                    if confirmed_vol_ok and (not sp.buy_require_volume_spike or (trig_vol_mult_max is not None and trig_vol_mult_max >= float(sp.buy_volume_spike_mult))):
                        stats["buy_vol_ok"] += 1
                    else:
                        confirmed_ok = False
                    early_vol_mult_req = float(sp.buy_volume_mult) * float(sp.early_buy_volume_mult_ratio)
                    early_spike_req = float(sp.buy_volume_spike_mult) * float(sp.early_buy_volume_spike_ratio)
                    early_spike_ok = True
                    if sp.buy_require_volume_spike:
                        early_spike_ok = trig_vol_mult_max is not None and trig_vol_mult_max >= early_spike_req
                    if not (trig_vol_mult is not None and trig_vol_mult >= early_vol_mult_req and early_spike_ok):
                        early_ok = False

                if confirmed_ok or early_ok:
                    price_ok = False
                    price_overheat = False
                    if sp.use_atr_normalization and sp.buy_price_rise_atr_mult is not None:
                        price_ok = price_move_atr is not None and price_move_atr >= float(sp.buy_price_rise_atr_mult)
                        if sp.buy_price_max_rise_atr_mult is not None and price_move_atr is not None:
                            price_overheat = price_move_atr > float(sp.buy_price_max_rise_atr_mult)
                    else:
                        price_ok = price_trig_ret is not None and price_trig_ret >= float(sp.buy_price_rise_pct)
                        if sp.buy_price_max_rise_pct is not None and price_trig_ret is not None:
                            price_overheat = price_trig_ret > float(sp.buy_price_max_rise_pct)
                    if price_ok and not price_overheat and green_cnt >= int(sp.buy_min_green_candles) and ((not sp.buy_require_last_green) or last_green):
                        stats["buy_price_ok"] += 1
                    else:
                        if price_trig_ret is not None:
                            buy_fail_price.append({
                                "symbol": symbol,
                                "price_trig_ret_pct": round(float(price_trig_ret), 4),
                                "green_cnt": int(green_cnt),
                                "last_green": bool(last_green),
                                "price_move_atr": round(float(price_move_atr), 4) if price_move_atr is not None else None,
                            })
                        confirmed_ok = False

                    early_green_req = int(sp.early_buy_min_green_candles if sp.early_buy_min_green_candles is not None else max(1, int(sp.buy_min_green_candles) - 1))
                    early_price_ok = False
                    early_price_overheat = False
                    if sp.use_atr_normalization and sp.buy_price_rise_atr_mult is not None:
                        early_req = float(sp.buy_price_rise_atr_mult) * float(sp.early_buy_price_rise_atr_mult_ratio)
                        early_price_ok = price_move_atr is not None and price_move_atr >= early_req
                        if sp.buy_price_max_rise_atr_mult is not None and price_move_atr is not None:
                            early_price_overheat = price_move_atr > float(sp.buy_price_max_rise_atr_mult)
                    else:
                        early_req = float(sp.buy_price_rise_pct) * float(sp.early_buy_price_rise_pct_ratio)
                        early_price_ok = price_trig_ret is not None and price_trig_ret >= early_req
                        if sp.buy_price_max_rise_pct is not None and price_trig_ret is not None:
                            early_price_overheat = price_trig_ret > float(sp.buy_price_max_rise_pct)
                    if not (early_price_ok and not early_price_overheat and green_cnt >= early_green_req and ((not sp.early_buy_require_last_green) or last_green)):
                        early_ok = False

                if confirmed_ok or early_ok:
                    if sp.enable_funding:
                        if funding_ok_buy:
                            stats["buy_funding_ok"] += 1
                        else:
                            confirmed_ok = False
                            early_ok = False
                            if funding_pct is not None:
                                buy_fail_funding.append({"symbol": symbol, "funding_pct": round(float(funding_pct), 6)})

                signal_profile = None
                if confirmed_ok and bool(sp.enable_confirmed_buy):
                    signal_profile = "CONFIRMED"
                elif early_ok and bool(sp.enable_early_buy):
                    signal_profile = "EARLY"

                if signal_profile is not None:
                    entry_price = float(trig_cl[-1])
                    sl, tp = self._calc_sl_tp(side="BUY", entry=entry_price, sp=sp)
                    last_ts = _as_dt(trig_slice[-1].get("ts")) or now_ts
                    signal_ts = last_ts + td
                    if signal_ts > (now_ts + td * 0.10):
                        signal_ts = last_ts
                    signal_age_min = (now_ts - signal_ts).total_seconds() / 60.0
                    if signal_age_min > float(sp.max_signal_age_minutes):
                        signal_profile = None

                if signal_profile is not None:
                    oi_factor = min(1.0, float(oi_trig_ret or 0.0) / max(1e-9, float(sp.buy_oi_rise_pct)))
                    oi_hold_ref = float(sp.buy_min_oi_last_rise_pct or sp.buy_oi_rise_pct)
                    oi_hold_factor = min(1.0, float(max(0.0, oi_trig_ret_last or 0.0)) / max(1e-9, oi_hold_ref))
                    oi_quality = min(1.0, max(0.0, 0.6 * oi_factor + 0.25 * oi_hold_factor + 0.15 * oi_up_ratio))
                    vol_quality = min(1.0, max(0.0, 0.65 * float(trig_vol_mult or 0.0) / max(1e-9, float(sp.buy_volume_mult)) + 0.35 * float(trig_vol_mult_max or 0.0) / max(1e-9, float(sp.buy_volume_spike_mult))))
                    if sp.use_atr_normalization and sp.buy_price_rise_atr_mult is not None:
                        price_quality = min(1.0, max(0.0, float(price_move_atr or 0.0) / max(1e-9, float(sp.buy_price_rise_atr_mult))))
                    else:
                        price_quality = min(1.0, max(0.0, float(price_trig_ret or 0.0) / max(1e-9, float(sp.buy_price_rise_pct))))
                    liquidity_quality = min(1.0, max(0.0, 0.5 * float(base_vol_med or 0.0) / max(1e-9, float(sp.min_base_quote_volume or max(1.0, float(base_vol_med or 1.0)))) + 0.5 * float(oi_last_value or 0.0) / max(1e-9, float(sp.min_oi_value or max(1.0, float(oi_last_value or 1.0))))))
                    structure_bonus = 1.0 - min(1.0, max(0.0, float(oi_drawdown_from_peak_pct or 0.0)) / max(1e-9, float(sp.max_oi_drawdown_from_peak_pct or 5.0)))
                    base_conf = max(0.0, min(1.0, 0.35 * oi_quality + 0.25 * vol_quality + 0.20 * price_quality + 0.10 * max(0.0, funding_buy_bonus) + 0.10 * liquidity_quality))
                    base_score = max(0.0, min(100.0, 100.0 * (0.50 * base_conf + 0.20 * structure_bonus + 0.30 * min(1.0, oi_up_ratio))))
                    if signal_profile == "EARLY":
                        conf = max(0.0, min(1.0, base_conf * 0.94))
                        score = max(0.0, min(100.0, base_score * 0.93))
                        score_floor = float(sp.early_buy_min_signal_score if sp.early_buy_min_signal_score is not None else max(0.0, float(sp.min_signal_score) * 0.92))
                    else:
                        conf = base_conf
                        score = min(100.0, base_score * 1.02)
                        score_floor = float(sp.min_signal_score)
                    if score < score_floor:
                        signal_profile = None

                if signal_profile is not None:
                    reason_parts = [
                        f"profile={signal_profile.lower()}",
                        f"base={'sideways' if is_sideways else 'downtrend'}",
                        f"OI {oi_trig_ret:.2f}%/{sp.trigger_minutes}m",
                        f"OI(last) {oi_trig_ret_last:.2f}%" if oi_trig_ret_last is not None else "OI(last) n/a",
                        (f"OI dd {oi_drawdown_from_peak_pct:.2f}%" if oi_drawdown_from_peak_pct is not None else "OI dd n/a"),
                        (f"OI$ {oi_value_trig_ret:.2f}%" if oi_value_trig_ret is not None else "OI$ n/a"),
                        f"Vol x{trig_vol_mult:.2f}",
                        (f"VolPeak x{trig_vol_mult_max:.2f}" if trig_vol_mult_max is not None else "VolPeak n/a"),
                        (f"Price ATRx {price_move_atr:.2f}" if price_move_atr is not None and sp.use_atr_normalization else f"Price {price_trig_ret:.2f}%/{sp.trigger_minutes}m"),
                    ]
                    if funding_pct is not None:
                        reason_parts.append(f"Funding {funding_pct:+.4f}%")
                    ctx = {
                        "interval": interval, "base_minutes": int(sp.base_minutes), "trigger_minutes": int(sp.trigger_minutes),
                        "signal_profile": signal_profile, "signal_label": f"BUY_{signal_profile}",
                        "price_range_pct": price_range_pct, "price_base_ret_pct": price_base_ret,
                        "price_trigger_ret_pct": price_trig_ret, "price_move_atr": price_move_atr, "atr_pct": atr_pct,
                        "green_candles": int(green_cnt), "oi_calc_key": sp.oi_calc_key, "oi_plot_key": sp.oi_plot_key,
                        "oi_base_range_pct": oi_base_range_pct, "oi_trigger_ret_pct": oi_trig_ret,
                        "oi_trigger_ret_last_pct": oi_trig_ret_last, "oi_drawdown_from_peak_pct": oi_drawdown_from_peak_pct,
                        "oi_up_ratio": oi_up_ratio, "oi_value_trigger_ret_pct": oi_value_trig_ret,
                        "oi_value_trigger_ret_last_pct": oi_value_trig_ret_last, "oi_value_confirm_pct": sp.oi_value_confirm_pct,
                        "base_vol_avg": float(base_vol_avg), "base_vol_median": float(base_vol_med) if base_vol_med is not None else None,
                        "base_vol_calm": bool(base_vol_calm), "trigger_vol_avg": float(trig_vol_avg),
                        "trigger_vol_mult": float(trig_vol_mult), "trigger_vol_mult_max": float(trig_vol_mult_max) if trig_vol_mult_max is not None else None,
                        "min_base_quote_volume": sp.min_base_quote_volume, "min_oi_value": sp.min_oi_value,
                        "oi_last_value": oi_last_value, "funding_pct": funding_pct, "funding_prev_pct": funding_prev_pct,
                        "funding_ts": funding_last.get("ts") if funding_last else None,
                        "funding_prev_ts": funding_prev.get("ts") if funding_prev else None,
                        "signal_age_minutes": signal_age_min, "score_components": {
                            "oi_quality": oi_quality, "vol_quality": vol_quality, "price_quality": price_quality,
                            "funding_bonus": funding_buy_bonus, "liquidity_quality": liquidity_quality, "structure_bonus": structure_bonus,
                        },
                    }
                    stats["buy_signals"] += 1
                    out.append(ScreenerSignal(
                        symbol_id=int(symbol_id), symbol=symbol, signal_ts=signal_ts, side="BUY",
                        entry_price=entry_price, stop_loss=sl, take_profit=tp, confidence=conf, score=score,
                        reason=" | ".join(reason_parts), context=ctx, signal_profile=signal_profile,
                    ))
                    if sp.debug:
                        dbg.append((symbol, float(score), f"BUY_{signal_profile}"))

            if mode in ("both", "sell"):
                sell_ok = base_ok_sell
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
                    mx = max(trig_hi) if trig_hi else None
                    drop_from_max = _pct(mx, trig_cl[-1]) if mx and mx > 0 else None
                    if drop_from_max is None or abs(drop_from_max) < abs(float(sp.sell_reversal_from_max_pct)):
                        sell_ok = False
                    else:
                        stats["sell_rev_ok"] += 1
                if sell_ok:
                    ok_oi = False
                    if oi_drop_one is not None and oi_drop_one <= -abs(float(sp.sell_oi_drop_one_candle_pct)):
                        ok_oi = True
                    if (not ok_oi) and (oi_trig_ret_last is not None) and (oi_trig_ret_last <= -abs(float(sp.sell_oi_drop_window_pct))):
                        ok_oi = True
                    if ok_oi:
                        stats["sell_oi_ok"] += 1
                    else:
                        sell_ok = False
                if sell_ok:
                    mv = max(trig_vol) if trig_vol else 0.0
                    last_v = trig_vol[-1] if trig_vol else 0.0
                    if mv <= 0 or mv < base_vol_avg * float(sp.sell_volume_spike_mult) or last_v > mv * float(sp.sell_volume_drop_ratio):
                        sell_ok = False
                    else:
                        stats["sell_vol_ok"] += 1
                if sell_ok and sp.sell_enable_funding:
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
                    oi_strength = abs(float(oi_drop_one or oi_trig_ret_last or 0.0))
                    oi_factor = min(1.0, oi_strength / max(1e-9, float(sp.sell_oi_drop_one_candle_pct)))
                    vol_factor = min(1.0, float(trig_vol_mult or 0.0) / max(1e-9, float(sp.sell_volume_spike_mult)))
                    prev_close = trig_cl[-2] if len(trig_cl) >= 2 else trig_cl[-1]
                    last_drop_abs = abs(_pct(prev_close, trig_cl[-1]) or 0.0)
                    pr_factor = min(1.0, last_drop_abs / max(1e-9, float(sp.sell_price_drop_last_pct)))
                    conf = max(0.0, min(1.0, 0.4 * oi_factor + 0.3 * vol_factor + 0.2 * pr_factor + 0.1 * funding_sell_bonus))
                    score = max(0.0, min(100.0, 100.0 * conf))
                    reason_parts = [
                        f"base={'sideways' if is_sideways else 'downtrend'}",
                        f"OI drop {oi_drop_one:.2f}%/1bar" if oi_drop_one is not None else f"OI {oi_trig_ret_last:.2f}%/{sp.trigger_minutes}m",
                        f"Vol peak->fade (x{trig_vol_mult:.2f})",
                    ]
                    ctx = {
                        "interval": interval, "base_minutes": int(sp.base_minutes), "trigger_minutes": int(sp.trigger_minutes),
                        "price_range_pct": price_range_pct, "price_base_ret_pct": price_base_ret, "price_trigger_ret_pct": price_trig_ret,
                        "oi_calc_key": sp.oi_calc_key, "oi_plot_key": sp.oi_plot_key, "oi_base_range_pct": oi_base_range_pct,
                        "oi_trigger_ret_pct": oi_trig_ret, "oi_trigger_ret_last_pct": oi_trig_ret_last, "oi_drop_one_pct": oi_drop_one,
                        "base_vol_avg": float(base_vol_avg), "trigger_vol_avg": float(trig_vol_avg),
                        "trigger_vol_mult": float(trig_vol_mult or 0.0), "funding_pct": funding_pct, "funding_prev_pct": funding_prev_pct,
                        "funding_ts": funding_last.get("ts") if funding_last else None,
                        "funding_prev_ts": funding_prev.get("ts") if funding_prev else None,
                    }
                    stats["sell_signals"] += 1
                    out.append(ScreenerSignal(
                        symbol_id=int(symbol_id), symbol=symbol, signal_ts=signal_ts, side="SELL",
                        entry_price=entry_price, stop_loss=sl, take_profit=tp, confidence=conf, score=score,
                        reason=" | ".join(reason_parts), context=ctx,
                    ))
                    if sp.debug:
                        dbg.append((symbol, score, "SELL"))

        if sp.debug or not out:
            log.info(
                "[OI] stats: symbols=%d candles_ok=%d oi_ok=%d vol_ok=%d liquidity_ok=%d base_ok=%d | "
                "BUY: oi_ok=%d oi_value_ok=%d vol_ok=%d price_ok=%d funding_ok=%d signals=%d | "
                "SELL: price_ok=%d rev_ok=%d oi_ok=%d vol_ok=%d funding_ok=%d signals=%d",
                int(stats.get("symbols_total", 0)), int(stats.get("candles_ok", 0)), int(stats.get("oi_ok", 0)),
                int(stats.get("vol_ok", 0)), int(stats.get("liquidity_ok", 0)), int(stats.get("base_ok", 0)),
                int(stats.get("buy_oi_ok", 0)), int(stats.get("buy_oi_value_ok", 0)), int(stats.get("buy_vol_ok", 0)),
                int(stats.get("buy_price_ok", 0)), int(stats.get("buy_funding_ok", 0)), int(stats.get("buy_signals", 0)),
                int(stats.get("sell_price_ok", 0)), int(stats.get("sell_rev_ok", 0)), int(stats.get("sell_oi_ok", 0)),
                int(stats.get("sell_vol_ok", 0)), int(stats.get("sell_funding_ok", 0)), int(stats.get("sell_signals", 0)),
            )
            if funding_filter_disabled:
                log.info("[OI] funding filter disabled for current run")

        if sp.debug and candidates:
            topn = max(1, int(sp.debug_top))
            def _top(items: List[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
                vals = [x for x in items if isinstance(x.get(key), (int, float))]
                vals.sort(key=lambda x: float(x.get(key) or 0.0), reverse=True)
                return vals[:topn]
            for key in ("oi_trig_ret_pct", "oi_trig_ret_last_pct", "price_trig_ret_pct", "vol_mult", "vol_mult_max"):
                top = _top(candidates, key)
                if top:
                    log.info("[OI] top %s: %s", key, top)
            if buy_fail_price:
                log.info("[OI] buy fail(price) sample: %s", buy_fail_price[:topn])
            if buy_fail_funding:
                log.info("[OI] buy fail(funding) sample: %s", buy_fail_funding[:topn])
            for label, dist in (("all", dist_all), ("base_ok", dist_base)):
                parts = []
                for k, vals in dist.items():
                    if not vals:
                        continue
                    p50 = _percentile(vals, 50)
                    p80 = _percentile(vals, 80)
                    p95 = _percentile(vals, 95)
                    parts.append(f"{k}: p50={p50:.2f} p80={p80:.2f} p95={p95:.2f}")
                if parts:
                    log.info("[OI] dist %s | %s", label, " | ".join(parts))

        out.sort(key=lambda s: ((s.signal_ts or now_ts), float(s.score or 0.0)), reverse=True)
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