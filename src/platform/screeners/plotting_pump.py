# src/platform/screeners/plotting_pump.py
from __future__ import annotations

import os
import warnings
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

def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _utc(dt: Any) -> Optional[datetime]:
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    if isinstance(dt, str):
        s = dt.strip()
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


def _x(dt: Any) -> Optional[float]:
    d = _utc(dt)
    return mdates.date2num(d) if d else None


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        v = float(x)
        if v != v:  # NaN
            return default
        return v
    except Exception:
        return default


def _fmt_pct(x: Any) -> str:
    try:
        if x is None:
            return "0"
        v = float(x)
        return f"{v:.2f}".rstrip("0").rstrip(".")
    except Exception:
        return "0"


def _fmt_price(x: Any) -> str:
    try:
        if x is None:
            return "—"
        v = float(x)
        s = f"{v:.8f}".rstrip("0").rstrip(".")
        return s if s else str(v)
    except Exception:
        return "—"


def _nice_time_axis(ax) -> None:
    try:
        locator = mdates.AutoDateLocator(minticks=4, maxticks=10)
        formatter = mdates.ConciseDateFormatter(locator)
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(formatter)
    except Exception:
        pass


def _candlestick_width_days(x_dt: List[datetime]) -> float:
    if len(x_dt) < 3:
        return 0.02
    try:
        x_num = mdates.date2num(x_dt)
        deltas = [abs(x_num[i] - x_num[i - 1]) for i in range(1, len(x_num))]
        deltas = [d for d in deltas if d > 0]
        if not deltas:
            return 0.02
        deltas.sort()
        median = deltas[len(deltas) // 2]
        return max(0.0005, median * 0.65)
    except Exception:
        return 0.02


def _extract_candles(
    candles: List[Dict[str, Any]],
) -> tuple[List[datetime], List[float], List[float], List[float], List[float]]:
    x: List[datetime] = []
    o: List[float] = []
    h: List[float] = []
    l: List[float] = []
    c: List[float] = []

    for cc in candles or []:
        ts = _utc(cc.get("ts"))
        if ts is None:
            continue
        x.append(ts)
        o.append(_safe_float(cc.get("open"), 0.0))
        h.append(_safe_float(cc.get("high"), 0.0))
        l.append(_safe_float(cc.get("low"), 0.0))
        c.append(_safe_float(cc.get("close"), 0.0))

    return x, o, h, l, c


def _plot_candles(
    ax,
    *,
    x_dt: List[datetime],
    o: List[float],
    h: List[float],
    l: List[float],
    c: List[float],
) -> float:
    if not x_dt:
        return 0.02

    x_num = mdates.date2num(x_dt)
    w = _candlestick_width_days(x_dt)

    for i in range(len(x_num)):
        x = x_num[i]
        oo = o[i]
        hh = h[i]
        ll = l[i]
        cc = c[i]

        if hh <= 0 and ll <= 0 and oo <= 0 and cc <= 0:
            continue

        up = cc >= oo
        col = "green" if up else "red"

        ax.vlines(x, ll, hh, linewidth=1.0, alpha=0.9)

        body_low = min(oo, cc)
        body_h = abs(cc - oo)
        if body_h < 1e-12:
            body_h = 1e-12

        rect = Rectangle(
            (x - w / 2.0, body_low),
            w,
            body_h,
            facecolor=col,
            edgecolor=col,
            alpha=0.65,
        )
        ax.add_patch(rect)

    ax.set_xlim(x_num[0] - w * 2, x_num[-1] + w * 2)
    return w


def _find_nearest_index(x: List[datetime], target: Any) -> Optional[int]:
    t = _utc(target)
    if not x or t is None:
        return None
    try:
        best_i = None
        best_d = None
        for i, dt in enumerate(x):
            d = abs((dt - t).total_seconds())
            if best_d is None or d < best_d:
                best_d = d
                best_i = i
        return best_i
    except Exception:
        return None


def _normalize_side(side: str) -> str:
    s = str(side or "").strip().upper()
    if s == "LONG":
        s = "BUY"
    if s == "SHORT":
        s = "SELL"
    return s


def _safe_tight_layout(fig: Figure) -> None:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            fig.tight_layout()
    except Exception:
        pass


def _expand_price_ylim(ax, values: List[Optional[float]]) -> None:
    """
    ✅ Гарантируем, что AVG/SL/TP/Entry попадают в кадр.
    """
    try:
        ymin, ymax = ax.get_ylim()
        candidates = [v for v in values if v is not None and isinstance(v, (int, float)) and float(v) > 0]
        if not candidates:
            return

        new_min = min([ymin] + candidates)
        new_max = max([ymax] + candidates)
        if new_max <= new_min:
            return

        pad = (new_max - new_min) * 0.03
        ax.set_ylim(new_min - pad, new_max + pad)
    except Exception:
        return


def _place_left_label(ax, *, x_left: float, y: float, text: str, color: str, used_ys: List[float]) -> None:
    """
    ✅ Ставим подпись слева и автоматически раздвигаем, если уровни близко (чтобы не перекрывались).
    """
    try:
        ymin, ymax = ax.get_ylim()
        yr = max(1e-12, ymax - ymin)
        min_sep = yr * 0.02  # 2% высоты графика

        yy = float(y)
        # если слишком близко к уже поставленным — сдвигаем вверх
        tries = 0
        while any(abs(yy - u) < min_sep for u in used_ys) and tries < 20:
            yy += min_sep
            tries += 1

        used_ys.append(yy)

        ax.text(
            x_left,
            yy,
            text,
            color=color,
            va="bottom",
            ha="left",
            bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.65),
            clip_on=True,
            zorder=8,
        )
    except Exception:
        # fallback
        ax.text(x_left, y, text, color=color, va="bottom", ha="left", clip_on=True, zorder=8)


def _draw_entry(ax, *, signal_ts: Any, entry_price: Any, side: str) -> None:
    """
    Точка входа:
      - вертикальная линия по signal_ts (цвет по side)
      - маркер-треугольник: BUY '^' зелёный, SELL 'v' красный
      - подпись, которая не вылезает за правую границу
    """
    xs = _x(signal_ts)
    if xs is None:
        return

    try:
        ep = float(entry_price) if entry_price is not None else None
    except Exception:
        ep = None

    side_u = _normalize_side(side)

    if side_u == "BUY":
        color = "green"
        marker = "^"
    elif side_u == "SELL":
        color = "red"
        marker = "v"
    else:
        color = "blue"
        marker = "o"

    # vertical line
    ax.axvline(xs, linewidth=1.4, color=color, alpha=0.9)

    if ep is None or ep <= 0:
        return

    # marker
    ax.scatter(
        [xs],
        [ep],
        marker=marker,
        s=160,
        zorder=9,
        color=color,
        edgecolors="white",
        linewidths=1.2,
    )

    # label placement (auto flip if near right edge)
    try:
        x0, x1 = ax.get_xlim()
        span = max(1e-12, x1 - x0)
        near_right = xs > (x1 - span * 0.08)  # последние 8% по оси X

        if near_right:
            xytext = (-8, 8)
            ha = "right"
        else:
            xytext = (8, 8)
            ha = "left"

        ax.annotate(
            f"{side_u} @ {_fmt_price(ep)}",
            (xs, ep),
            textcoords="offset points",
            xytext=xytext,
            ha=ha,
            va="bottom",
            color=color,
            bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.65),
            clip_on=True,
            annotation_clip=True,
            zorder=10,
        )
    except Exception:
        ax.annotate(
            f"{side_u} @ {_fmt_price(ep)}",
            (xs, ep),
            textcoords="offset points",
            xytext=(8, 8),
            ha="left",
            va="bottom",
            color=color,
            clip_on=True,
            annotation_clip=True,
            zorder=10,
        )


# ======================================================================
# Public API
# ======================================================================

def save_pump_plot(
    *,
    out_path: Path,
    symbol: str,
    timeframe: str,
    candles: List[Dict[str, Any]],
    pump_start_ts: Optional[datetime],
    pump_end_ts: Optional[datetime],
    confirm_ts: Optional[datetime],
    side: str,
    price_change_pct: Optional[float],
    oi_delta: Optional[float],
    cvd_delta: Optional[float],
    oi_series: Optional[List[Dict[str, Any]]] = None,
    cvd_series: Optional[List[Dict[str, Any]]] = None,
    # entry marker
    signal_ts: Optional[datetime] = None,
    entry_price: Optional[float] = None,
    # SL/TP
    stop_loss: Optional[float] = None,
    take_profit: Optional[float] = None,
    # AVG
    base_avg_price: Optional[float] = None,
    base_lookback_bars: Optional[int] = None,
) -> None:
    if not candles:
        return

    out_path = Path(out_path)
    _ensure_dir(out_path.parent)

    show_oi = oi_series is not None
    show_cvd = cvd_series is not None
    rows = 1 + (1 if show_oi else 0) + (1 if show_cvd else 0)

    fig = Figure(figsize=(12, 7 if rows == 1 else 9), dpi=140)
    _ = FigureCanvas(fig)

    ratios = [3.0]
    if show_oi:
        ratios.append(1.0)
    if show_cvd:
        ratios.append(1.0)

    gs = fig.add_gridspec(rows, 1, hspace=0.12, height_ratios=ratios)
    ax_price = fig.add_subplot(gs[0, 0])

    ax_oi = None
    ax_cvd = None
    idx = 1
    if show_oi:
        ax_oi = fig.add_subplot(gs[idx, 0], sharex=ax_price)
        idx += 1
    if show_cvd:
        ax_cvd = fig.add_subplot(gs[idx, 0], sharex=ax_price)

    # -------------------------
    # PRICE
    # -------------------------
    x, o, h, l, c_ = _extract_candles(candles)
    if not x:
        return

    _plot_candles(ax_price, x_dt=x, o=o, h=h, l=l, c=c_)

    # shaded pump window
    x0 = _x(pump_start_ts)
    x1 = _x(pump_end_ts)
    if x0 is not None and x1 is not None:
        if x1 < x0:
            x0, x1 = x1, x0
        ax_price.axvspan(x0, x1, alpha=0.12)

    # Parse avg/sl/tp/entry for ylim expansion
    avg = None
    try:
        if base_avg_price is not None:
            avg = float(base_avg_price)
    except Exception:
        avg = None

    slv = None
    try:
        if stop_loss is not None:
            v = float(stop_loss)
            slv = v if v > 0 else None
    except Exception:
        slv = None

    tpv = None
    try:
        if take_profit is not None:
            v = float(take_profit)
            tpv = v if v > 0 else None
    except Exception:
        tpv = None

    ep = None
    try:
        if entry_price is not None:
            v = float(entry_price)
            ep = v if v > 0 else None
    except Exception:
        ep = None

    idx_entry = None
    if signal_ts is not None:
        idx_entry = _find_nearest_index(x, signal_ts)

    if ep is None and idx_entry is not None:
        ep2 = _safe_float(c_[idx_entry], 0.0)
        ep = ep2 if ep2 > 0 else None

    # expand ylim so everything visible
    _expand_price_ylim(ax_price, [avg, slv, tpv, ep])

    x_left = _x(x[0])
    used_ys: List[float] = []

    # AVG line + label
    if avg is not None and avg > 0:
        ax_price.axhline(avg, linestyle="--", color="orange", alpha=0.85, linewidth=1.2)
        if x_left is not None:
            n = None
            try:
                n = int(base_lookback_bars) if base_lookback_bars is not None else None
            except Exception:
                n = None
            label = f"AVG({n})={_fmt_price(avg)}" if n and n > 0 else f"AVG={_fmt_price(avg)}"
            _place_left_label(ax_price, x_left=x_left, y=avg, text=label, color="orange", used_ys=used_ys)

    # SL/TP lines + labels
    if slv is not None:
        ax_price.axhline(slv, linestyle=":", color="red", alpha=0.8, linewidth=1.2)
        if x_left is not None:
            _place_left_label(ax_price, x_left=x_left, y=slv, text=f"SL={_fmt_price(slv)}", color="red", used_ys=used_ys)

    if tpv is not None:
        ax_price.axhline(tpv, linestyle=":", color="green", alpha=0.8, linewidth=1.2)
        if x_left is not None:
            _place_left_label(ax_price, x_left=x_left, y=tpv, text=f"TP={_fmt_price(tpv)}", color="green", used_ys=used_ys)

    # ✅ confirm marker: only for SELL (для BUY не рисуем)
    side_u = _normalize_side(side)
    if side_u == "SELL":
        xc = _x(confirm_ts)
        xsig = _x(signal_ts)
        if xc is not None:
            same_as_entry = False
            if xsig is not None:
                try:
                    same_as_entry = abs(float(xc) - float(xsig)) < 1e-9
                except Exception:
                    same_as_entry = False

            if not same_as_entry:
                ax_price.axvline(xc, linestyle="--", linewidth=1.2, color="gray", alpha=0.9)
                y_top = ax_price.get_ylim()[1]
                ax_price.annotate(
                    "confirm",
                    (xc, y_top),
                    textcoords="offset points",
                    xytext=(4, -6),
                    ha="left",
                    va="top",
                    color="gray",
                    bbox=dict(boxstyle="round,pad=0.12", fc="white", ec="none", alpha=0.6),
                    clip_on=True,
                    annotation_clip=True,
                    zorder=8,
                )

    # entry marker
    if signal_ts is not None:
        x_e = x[idx_entry] if idx_entry is not None else _utc(signal_ts)
        if x_e is not None:
            _draw_entry(ax_price, signal_ts=x_e, entry_price=ep, side=side)

    ax_price.set_title(f"{symbol} | {timeframe} | {side_u} | ΔP={_fmt_pct(price_change_pct)}%")
    ax_price.grid(True, alpha=0.15)

    # -------------------------
    # OI
    # -------------------------
    if ax_oi is not None and oi_series is not None:
        xs_dt: List[datetime] = []
        ys: List[float] = []
        for r in oi_series:
            ts = _utc(r.get("ts"))
            if ts is None:
                continue
            xs_dt.append(ts)
            ys.append(_safe_float(r.get("open_interest"), 0.0))

        if xs_dt and ys and len(xs_dt) == len(ys):
            ax_oi.plot(mdates.date2num(xs_dt), ys)
            title = f"OI Δ={_safe_float(oi_delta, 0.0):.4g}" if oi_delta is not None else "OI"
        else:
            title = "OI (no data)"

        ax_oi.set_title(title)
        ax_oi.grid(True, alpha=0.15)

    # -------------------------
    # CVD
    # -------------------------
    if ax_cvd is not None and cvd_series is not None:
        xs_dt: List[datetime] = []
        ys: List[float] = []
        for r in cvd_series:
            ts = _utc(r.get("ts"))
            if ts is None:
                continue
            xs_dt.append(ts)
            ys.append(_safe_float(r.get("cvd_quote"), 0.0))

        if xs_dt and ys and len(xs_dt) == len(ys):
            ax_cvd.plot(mdates.date2num(xs_dt), ys)
            title = f"CVD Δ={_safe_float(cvd_delta, 0.0):.4g}" if cvd_delta is not None else "CVD"
        else:
            title = "CVD (no data)"

        ax_cvd.set_title(title)
        ax_cvd.grid(True, alpha=0.15)

    # X-axis formatting
    axes = [ax for ax in [ax_price, ax_oi, ax_cvd] if ax is not None]
    if axes:
        for ax in axes[:-1]:
            ax.tick_params(axis="x", labelbottom=False)
        _nice_time_axis(axes[-1])

    _safe_tight_layout(fig)
    fig.savefig(str(out_path), bbox_inches="tight")


def save_pump_signal_plot(
    *,
    out_path: Path,
    symbol: str,
    timeframe: str,
    candles: List[Dict[str, Any]],
    signal_ts: Optional[datetime] = None,
    side: str = "BUY",
    entry_price: float = 0.0,
    stop_loss: Optional[float] = None,
    take_profit: Optional[float] = None,
    pump_start_ts: Optional[datetime] = None,
    pump_end_ts: Optional[datetime] = None,
    confirm_ts: Optional[datetime] = None,
    pump_pct: Optional[float] = None,
    window_minutes: Optional[int] = None,
    oi_delta: Optional[float] = None,
    cvd_delta: Optional[float] = None,
    oi_series: Optional[List[Dict[str, Any]]] = None,
    cvd_series: Optional[List[Dict[str, Any]]] = None,
    base_avg_price: Optional[float] = None,
    base_lookback_bars: Optional[int] = None,
) -> None:
    save_pump_plot(
        out_path=out_path,
        symbol=symbol,
        timeframe=timeframe,
        candles=candles,
        pump_start_ts=pump_start_ts,
        pump_end_ts=pump_end_ts,
        confirm_ts=confirm_ts,
        side=side,
        price_change_pct=pump_pct,
        oi_delta=oi_delta,
        cvd_delta=cvd_delta,
        oi_series=oi_series,
        cvd_series=cvd_series,
        signal_ts=signal_ts,
        entry_price=entry_price,
        stop_loss=stop_loss,
        take_profit=take_profit,
        base_avg_price=base_avg_price,
        base_lookback_bars=base_lookback_bars,
    )
