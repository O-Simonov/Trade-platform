# src/platform/screeners/plotting_oi.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from datetime import datetime, timezone

# ======================================================================
# HARD-SAFE matplotlib setup (Windows Server / no GUI / no crashes)
# ======================================================================

os.environ.setdefault("MPLBACKEND", "Agg")

try:
    _ROOT = Path(__file__).resolve().parents[3]  # .../Trade-platform
except Exception:
    _ROOT = Path.cwd()

_MPL_CACHE = _ROOT / ".cache" / "matplotlib"
_MPL_CACHE.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(_MPL_CACHE))

import matplotlib  # noqa
matplotlib.use("Agg", force=True)

from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas

import matplotlib.dates as mdates
from matplotlib.patches import Rectangle


# ======================================================================
# Helpers
# ======================================================================

def _utc(dt: Any) -> Optional[datetime]:
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return None


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return float(default)
        v = float(x)
        if v != v:
            return float(default)
        return v
    except Exception:
        return float(default)


def _candle_qv(c: Dict[str, Any]) -> float:
    qv = _safe_float(c.get("quote_volume"), 0.0)
    if qv > 0:
        return qv
    v = _safe_float(c.get("volume"), 0.0)
    cl = _safe_float(c.get("close"), 0.0)
    if v > 0 and cl > 0:
        return v * cl
    return 0.0


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _find_nearest_index(times: List[datetime], target: datetime) -> int:
    if not times:
        return 0
    # times are sorted
    best_i = 0
    best_d = None
    for i, t in enumerate(times):
        d = abs((t - target).total_seconds())
        if best_d is None or d < best_d:
            best_d = d
            best_i = i
    return best_i


def _align_series_to_times(
    *,
    times: List[datetime],
    series: List[Dict[str, Any]],
    value_key: str = "value",
) -> List[Optional[float]]:
    """
    Forward-fill: для каждого time берём последнее значение series.ts <= time.
    series: [{"ts":..., value_key:...}, ...]
    """
    if not times:
        return []
    if not series:
        return [None for _ in times]

    s = []
    for r in series:
        ts = _utc(r.get("ts"))
        if ts is None:
            continue
        s.append({"ts": ts, "v": r.get(value_key)})
    s.sort(key=lambda x: x["ts"])

    out: List[Optional[float]] = []
    j = 0
    last_v: Optional[float] = None

    for t in times:
        while j < len(s) and s[j]["ts"] <= t:
            last_v = _safe_float(s[j]["v"], last_v if last_v is not None else 0.0)
            j += 1
        out.append(last_v)

    return out


# ======================================================================
# Main plot function
# ======================================================================

def save_oi_signal_plot(
    *,
    out_path: Path,
    symbol: str,
    timeframe: str,
    candles: List[Dict[str, Any]],
    signal_ts: datetime,
    side: str,
    entry_price: float,
    stop_loss: Optional[float],
    take_profit: Optional[float],
    oi_series: Optional[List[Dict[str, Any]]] = None,
    oi_value_key: str = "value",
    title_extra: str = "",
) -> None:
    """
    Рисует:
      - верх: свечи
      - середина: объём
      - низ: OI (линия)

    oi_series: список вида [{"ts": datetime, "value": ...}] (или ключ value_key)
    """
    candles = candles or []
    if not candles:
        return

    # normalize & sort
    cc = []
    for c in candles:
        ts = _utc(c.get("ts")) or _utc(c.get("open_time"))
        if ts is None:
            continue
        cc.append({**c, "ts": ts})
    cc.sort(key=lambda x: x["ts"])

    times = [c["ts"] for c in cc]
    x = mdates.date2num(times)

    # candle components
    o = [_safe_float(c.get("open"), 0.0) for c in cc]
    h = [_safe_float(c.get("high"), 0.0) for c in cc]
    l = [_safe_float(c.get("low"), 0.0) for c in cc]
    cl = [_safe_float(c.get("close"), 0.0) for c in cc]
    vol = [_candle_qv(c) for c in cc]

    # oi aligned
    oi_vals = _align_series_to_times(times=times, series=oi_series or [], value_key=oi_value_key)

    # figure
    fig = Figure(figsize=(14, 9), dpi=150)
    FigureCanvas(fig)

    # layout: 3 rows
    gs = fig.add_gridspec(3, 1, height_ratios=[3.2, 1.2, 1.6], hspace=0.05)
    ax_p = fig.add_subplot(gs[0, 0])
    ax_v = fig.add_subplot(gs[1, 0], sharex=ax_p)
    ax_oi = fig.add_subplot(gs[2, 0], sharex=ax_p)

    # candle width heuristic
    if len(x) >= 2:
        dx = (x[1] - x[0])
        w = dx * 0.65
    else:
        w = 0.02

    # draw candles
    for i in range(len(cc)):
        # wick
        ax_p.vlines(x[i], l[i], h[i], linewidth=1)

        # body
        body_low = min(o[i], cl[i])
        body_h = abs(cl[i] - o[i])
        if body_h == 0:
            body_h = 1e-9

        ax_p.add_patch(
            Rectangle(
                (x[i] - w / 2, body_low),
                w,
                body_h,
                linewidth=1,
                fill=False,
            )
        )

    # volume bars
    ax_v.bar(x, vol, width=w, align="center")

    # OI line
    if any(v is not None for v in oi_vals):
        y = [(_safe_float(v, 0.0) if v is not None else float("nan")) for v in oi_vals]
        ax_oi.plot_date(x, y, fmt="-", linewidth=1.2)


    # signal markers / levels
    signal_ts = _utc(signal_ts) or times[-1]
    si = _find_nearest_index(times, signal_ts)

    side_u = (side or "").upper().strip()
    is_buy = side_u.startswith("B")
    side_color = "green" if is_buy else "red"
    entry_marker = "^" if is_buy else "v"

    # Entry marker (triangle) + guides
    ax_p.scatter([x[si]], [entry_price], marker=entry_marker, s=110, color=side_color, zorder=6)

    # Vertical line at entry time across all panels
    for ax in (ax_p, ax_v, ax_oi):
        ax.axvline(x[si], linewidth=1, color=side_color, alpha=0.6)

    # Horizontal line at entry price on price chart
    ax_p.axhline(entry_price, linewidth=1, color=side_color, alpha=0.7, linestyle="--")

    # Entry annotation (direction + price)
    entry_txt = f"{side_u} {entry_price:.6g}"
    entry_off_y = 10 if is_buy else -12
    entry_va = "bottom" if is_buy else "top"
    ax_p.annotate(
        entry_txt,
        xy=(x[si], entry_price),
        xytext=(0, entry_off_y),
        textcoords="offset points",
        ha="center",
        va=entry_va,
        color=side_color,
        fontweight="bold",
        bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.7, linewidth=0),
    )

    # Stop-loss / Take-profit lines + labels (with prices)
    if stop_loss is not None and stop_loss > 0:
        sl = float(stop_loss)
        ax_p.axhline(sl, linewidth=1.2, color="red", linestyle="-")
        ax_p.text(
            0.90,
            sl,
            f"SL {sl:.6g}",
            transform=ax_p.get_yaxis_transform(),  # x in axes, y in data
            ha="right",
            va="center",
            color="red",
            bbox=dict(boxstyle="round,pad=0.15", facecolor="white", alpha=0.7, linewidth=0),
        )

    if take_profit is not None and take_profit > 0:
        tp = float(take_profit)
        ax_p.axhline(tp, linewidth=1.2, color="green", linestyle="-")
        ax_p.text(
            0.90,
            tp,
            f"TP {tp:.6g}",
            transform=ax_p.get_yaxis_transform(),
            ha="right",
            va="center",
            color="green",
            bbox=dict(boxstyle="round,pad=0.15", facecolor="white", alpha=0.7, linewidth=0),
        )

    # formatting

    ax_p.set_title(f"{symbol}  {timeframe}  {side}  entry={entry_price:.6g}" + (f"  {title_extra}" if title_extra else ""))
    ax_p.grid(True)
    ax_v.grid(True)
    ax_oi.grid(True)

    ax_v.set_ylabel("volume")
    ax_oi.set_ylabel("open interest")

    ax_oi.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    fig.autofmt_xdate(rotation=0)

    # hide x labels on top axes
    ax_p.tick_params(labelbottom=False)
    ax_v.tick_params(labelbottom=False)

    _ensure_dir(Path(out_path).parent)
    fig.savefig(str(out_path), bbox_inches="tight")
    fig.clear()
