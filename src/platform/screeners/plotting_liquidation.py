# src/platform/screeners/plotting_liquidation.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Sequence, Dict, Any
from datetime import timedelta

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
from matplotlib import transforms


# ======================================================================
# Helpers
# ======================================================================

def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


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


def _safe_ts_list(candles: Sequence[Dict[str, Any]]) -> list:
    try:
        return [c["ts"] for c in candles]
    except Exception:
        return []


def _find_nearest_index(x: Sequence[Any], target: Any) -> int | None:
    if not x or target is None:
        return None
    try:
        best_i = None
        best_d = None
        for i, t in enumerate(x):
            if t is None:
                continue
            d = abs((t - target).total_seconds())
            if best_d is None or d < best_d:
                best_d = d
                best_i = i
        return best_i
    except Exception:
        return None


def _parse_interval_td(interval: str) -> timedelta:
    s = str(interval).strip().lower()
    if s.endswith("m"):
        return timedelta(minutes=int(s[:-1]))
    if s.endswith("h"):
        return timedelta(hours=int(s[:-1]))
    if s.endswith("d"):
        return timedelta(days=int(s[:-1]))
    return timedelta(hours=1)


def _nice_time_axis(ax) -> None:
    try:
        locator = mdates.AutoDateLocator(minticks=4, maxticks=10)
        formatter = mdates.ConciseDateFormatter(locator)
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(formatter)
    except Exception:
        pass


def _candlestick_width_days(x_num: list[float]) -> float:
    if len(x_num) < 3:
        return 0.02
    try:
        deltas = [abs(x_num[i] - x_num[i - 1]) for i in range(1, len(x_num))]
        deltas = [d for d in deltas if d > 0]
        if not deltas:
            return 0.02
        deltas.sort()
        median = deltas[len(deltas) // 2]
        return max(0.0005, median * 0.65)
    except Exception:
        return 0.02


def _pct_to_level(level: Optional[float], base: float) -> Optional[float]:
    try:
        if level is None:
            return None
        lv = float(level)
        if base <= 0:
            return None
        return (lv / float(base) - 1.0) * 100.0
    except Exception:
        return None

def _fmt_compact(v: float) -> str:
    """Human-friendly number formatting: 1.23K / 4.56M / 7.89B"""
    try:
        x = float(v)
        ax = abs(x)
        if ax >= 1e9:
            return f"{x/1e9:.2f}B"
        if ax >= 1e6:
            return f"{x/1e6:.2f}M"
        if ax >= 1e3:
            return f"{x/1e3:.2f}K"
        return f"{x:.0f}"
    except Exception:
        return str(v)


def _label_bbox() -> dict:
    # белая полупрозрачная подложка, чтобы текст не терялся на свечах
    return dict(boxstyle="round,pad=0.20", fc="white", ec="none", alpha=0.72)


def _place_side_labels(ax, fig: Figure, labels: list[dict]) -> None:
    """
    Размещает подписи уровней (UP/DOWN/TP/SL/ENTRY) так, чтобы они не перекрывались:
    - x фиксирован (слева или справа) в координатах осей (0..1)
    - y в координатах данных
    - конфликт решаем вертикальными смещениями в пикселях -> переводим в points
    """
    if not labels:
        return

    dpi = getattr(fig, "dpi", 100) or 100

    def _do_group(group: list[dict], x_ax: float, ha: str) -> None:
        if not group:
            return
        blended = transforms.blended_transform_factory(ax.transAxes, ax.transData)
        # сортируем по y в экранных координатах
        items = []
        for it in group:
            try:
                y = float(it["y"])
            except Exception:
                continue
            y_disp = ax.transData.transform((0.0, y))[1]
            items.append((y_disp, y, it))
        items.sort(key=lambda t: t[0])

        min_px = 14.0  # минимальный вертикальный зазор между подписями
        prev = None
        for y_disp, y, it in items:
            y_adj = y_disp if prev is None else max(y_disp, prev + min_px)
            dy_px = y_adj - y_disp
            dy_pt = dy_px * 72.0 / float(dpi)

            ax.annotate(
                it["text"],
                xy=(x_ax, y),
                xycoords=blended,
                xytext=(0, dy_pt),
                textcoords="offset points",
                ha=ha,
                va="center",
                color=it.get("color", "black"),
                fontsize=9,
                bbox=_label_bbox(),
                clip_on=True,
            )
            prev = y_adj

    left = [it for it in labels if it.get("side", "left") == "left"]
    right = [it for it in labels if it.get("side") == "right"]

    _do_group(left, x_ax=0.01, ha="left")
    _do_group(right, x_ax=0.99, ha="right")



def _plot_candles(ax, *, x_dt: list, o: list[float], h: list[float], l: list[float], c: list[float]) -> float:
    if not x_dt:
        return 0.02

    x_num = mdates.date2num(x_dt)
    w = _candlestick_width_days(list(x_num))

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


def _aggregate_liq_to_candles(
    *,
    candles: Sequence[Dict[str, Any]],
    liq: Sequence[Dict[str, Any]],
    interval: str,
) -> tuple[list, list, list]:
    """
    Возвращает:
      x_c      список datetime свечей
      long_y   long ликвидации (положительные)
      short_y  short ликвидации (отрицательные)
    """
    if not candles or not liq:
        return [], [], []

    x_c = [c["ts"] for c in candles]
    dt = _parse_interval_td(interval)
    if dt.total_seconds() <= 0:
        return [], [], []

    try:
        liq_sorted = sorted(liq, key=lambda r: r.get("ts") or x_c[0])
    except Exception:
        liq_sorted = list(liq)

    long_y = [0.0] * len(x_c)
    short_y = [0.0] * len(x_c)

    i = 0
    for r in liq_sorted:
        ts = r.get("ts")
        if ts is None:
            continue

        while i < len(x_c) - 1 and ts >= (x_c[i] + dt):
            i += 1

        if i < 0 or i >= len(x_c):
            continue

        if not (x_c[i] <= ts < (x_c[i] + dt)):
            continue

        long_y[i] += _safe_float(r.get("long_usdt"), 0.0)
        short_y[i] += _safe_float(r.get("short_usdt"), 0.0)

    short_y = [-abs(v) for v in short_y]
    return x_c, long_y, short_y


def _fallback_liq_series(
    *,
    candles: Sequence[Dict[str, Any]],
    idx: int | None,
    liq_long_usdt: float | None,
    liq_short_usdt: float | None,
) -> tuple[list, list, list]:
    """
    Если liquidation_series нет, рисуем 1 точку по ctx (liq_long_usdt/liq_short_usdt)
    """
    if not candles:
        return [], [], []

    x = [c["ts"] for c in candles]
    long_y = [0.0] * len(x)
    short_y = [0.0] * len(x)

    if idx is None or idx < 0 or idx >= len(x):
        return [], [], []

    if liq_long_usdt is not None:
        long_y[idx] = _safe_float(liq_long_usdt, 0.0)
    if liq_short_usdt is not None:
        short_y[idx] = -abs(_safe_float(liq_short_usdt, 0.0))

    return x, long_y, short_y


def _draw_level_zone(
    ax,
    *,
    level: float,
    zone_pct: float,
    label: str,
    kind: str,
    pct_from_entry: Optional[float] = None,
) -> Optional[Dict[str, Any]]:
    """Рисует линию/зону уровня и возвращает данные для подписи."""
    lv = _safe_float(level, 0.0)
    if lv <= 0:
        return None

    z = abs(float(zone_pct)) if zone_pct else 0.003
    if z <= 0:
        z = 0.003

    y0 = lv * (1.0 - z)
    y1 = lv * (1.0 + z)

    if kind == "up":
        c = "green"
        alpha = 0.10
    else:
        c = "blue"
        alpha = 0.10

    ax.axhspan(y0, y1, xmin=0.0, xmax=1.0, alpha=alpha)
    ax.axhline(lv, linestyle="--", color=c, alpha=0.85)

    if pct_from_entry is None:
        txt = f"{label}={lv:.6f}"
    else:
        txt = f"{label}={lv:.6f} ({pct_from_entry:+.2f}%)"

    return {"y": lv, "text": txt, "color": c}



# ======================================================================
# Plot API (3 panels)
# ======================================================================

def save_signal_plot(
    *,
    out_path: Path,
    symbol: str,
    timeframe: str,
    candles: Sequence[Dict[str, Any]],
    entry_ts: Any,
    touch_ts: Any = None,
    side: str,
    entry_price: float,
    up_level: Optional[float] = None,
    down_level: Optional[float] = None,
    stop_loss: Optional[float] = None,
    take_profit: Optional[float] = None,
    liquidation_series: Optional[Sequence[Dict[str, Any]]] = None,
    liq_short_usdt: Optional[float] = None,
    liq_long_usdt: Optional[float] = None,
    funding_series: Optional[Sequence[Dict[str, Any]]] = None,
    funding_rate: Optional[float] = None,

    # -------------------------
    # Volume info (для отображения среднего и порога)
    # -------------------------
    volume_avg: Optional[float] = None,         # avg volume (как в расчёте сигнала)
    volume_threshold: Optional[float] = None,   # avg * (1 + volume_change_pct/100)
    volume_anchor: Optional[float] = None,      # volume(anchor candle)
    volume_change_pct: Optional[float] = None,  # требуемый рост (в %)
    anchor_ts: Any = None,                      # timestamp anchor candle (если есть)
    level_zone_pct: float = 0.003,  # 0.3%
    **_: Any,
) -> None:
    if not candles:
        return

    out_path = Path(out_path)
    x = _safe_ts_list(candles)
    if not x:
        return

    o = [_safe_float(c.get("open"), 0.0) for c in candles]
    h = [_safe_float(c.get("high"), 0.0) for c in candles]
    l = [_safe_float(c.get("low"), 0.0) for c in candles]
    c_ = [_safe_float(c.get("close"), 0.0) for c in candles]

    # volume: quote_volume preferred
    vol = []
    for cc in candles:
        qv = cc.get("quote_volume")
        vv = cc.get("volume")
        v = _safe_float(qv, 0.0)
        if v <= 0:
            v = _safe_float(vv, 0.0)
        vol.append(v)

    title_side = str(side or "").upper().strip()
    if title_side == "LONG":
        title_side = "BUY"
    if title_side == "SHORT":
        title_side = "SELL"

    entry_p = _safe_float(entry_price, 0.0)

    up_pct = _pct_to_level(up_level, entry_p)
    dn_pct = _pct_to_level(down_level, entry_p)

    extra_pct = ""
    if up_pct is not None:
        extra_pct += f" | UP {up_pct:+.2f}%"
    if dn_pct is not None:
        extra_pct += f" | DOWN {dn_pct:+.2f}%"

    fig = Figure(figsize=(14, 9.5), dpi=110)
    _ = FigureCanvas(fig)

    ax_price, ax_vol, ax_liq = fig.subplots(3, 1, sharex=True, height_ratios=[3.1, 1.0, 1.6])

    # =========================================================
    # PRICE (Candles)
    # =========================================================
    candle_w = _plot_candles(ax_price, x_dt=x, o=o, h=h, l=l, c=c_)
    ax_price.set_title(f"{symbol} [{timeframe}] {title_side} | entry={entry_p:.6f}{extra_pct}")

    # уровни зонами + подписи (без наложений)
    level_labels: list[dict] = []

    if up_level is not None:
        info = _draw_level_zone(
            ax_price,
            level=float(up_level),
            zone_pct=level_zone_pct,
            label="UP",
            kind="up",
            pct_from_entry=up_pct,
        )
        if info:
            info["side"] = "left"
            level_labels.append(info)

    if down_level is not None:
        info = _draw_level_zone(
            ax_price,
            level=float(down_level),
            zone_pct=level_zone_pct,
            label="DOWN",
            kind="down",
            pct_from_entry=dn_pct,
        )
        if info:
            info["side"] = "left"
            level_labels.append(info)

    # ENTRY (горизонтальная линия и подпись)
    if entry_p > 0:
        ax_price.axhline(entry_p, linestyle="-", color="gray", alpha=0.30, linewidth=1.0)
        level_labels.append({"y": entry_p, "text": f"ENTRY={entry_p:.6f}", "color": "black", "side": "right"})

    # SL / TP
    slv = _safe_float(stop_loss, 0.0) if stop_loss is not None else 0.0
    tpv = _safe_float(take_profit, 0.0) if take_profit is not None else 0.0

    if slv > 0:
        ax_price.axhline(slv, linestyle=":", color="red", alpha=0.7)
        level_labels.append({"y": slv, "text": f"SL={slv:.6f}", "color": "red", "side": "left"})

    if tpv > 0:
        ax_price.axhline(tpv, linestyle=":", color="orange", alpha=0.7)
        level_labels.append({"y": tpv, "text": f"TP={tpv:.6f}", "color": "orange", "side": "left"})

    # размещаем подписи уровней так, чтобы не перекрывались (UP/DOWN/TP/SL/ENTRY)
    _place_side_labels(ax_price, fig, level_labels)

    idx_entry = _find_nearest_index(x, entry_ts)
    idx_touch = _find_nearest_index(x, touch_ts) if touch_ts is not None else None

    # эвристика "правого края"
    near_right_cut = max(0, int(len(x) * 0.94))

    # TOUCH marker
    if idx_touch is not None:
        try:
            x_t = x[idx_touch]
            if title_side == "SELL":
                y_t = h[idx_touch] if h[idx_touch] > 0 else c_[idx_touch]
            elif title_side == "BUY":
                y_t = l[idx_touch] if l[idx_touch] > 0 else c_[idx_touch]
            else:
                y_t = c_[idx_touch]

            ax_price.axvline(x_t, linestyle=":", color="purple", alpha=0.9)
            ax_price.scatter([x_t], [y_t], marker="x", color="purple")

            # если рядом confirm — разводим по вертикали
            close_to_entry = (idx_entry is not None) and (abs(int(idx_touch) - int(idx_entry)) <= 2)
            near_right_touch = int(idx_touch) >= near_right_cut
            near_right_entry = (idx_entry is not None) and (int(idx_entry) >= near_right_cut)

            if near_right_touch or (near_right_entry and close_to_entry):
                dx_t, ha_t = -55, "right"
            else:
                dx_t, ha_t = -55, "right"  # touch почти всегда лучше слева

            dy_t = -14 if close_to_entry else 10
            va_t = "top" if dy_t < 0 else "bottom"

            ax_price.annotate(
                "TOUCH",
                xy=(x_t, y_t),
                xytext=(dx_t, dy_t),
                textcoords="offset points",
                ha=ha_t,
                va=va_t,
                color="purple",
                fontsize=10,
                bbox=_label_bbox(),
                clip_on=True,
            )
        except Exception:
            pass

    # CONFIRM / ENTRY marker
    if idx_entry is not None:
        try:
            x_e = x[idx_entry]
            y_e = entry_p
            marker = "^" if title_side == "BUY" else "v" if title_side == "SELL" else "o"

            ax_price.axvline(x_e, linestyle="--", color="blue", alpha=0.9)
            ax_price.scatter([x_e], [y_e], marker=marker, color="blue")

            close_to_touch = (idx_touch is not None) and (abs(int(idx_entry) - int(idx_touch)) <= 2)
            near_right_entry = int(idx_entry) >= near_right_cut

            if near_right_entry:
                dx_e, ha_e = -55, "right"
            else:
                dx_e, ha_e = 55, "left"

            dy_e = 14 if close_to_touch else 10
            ax_price.annotate(
                f"{title_side}",
                xy=(x_e, y_e),
                xytext=(dx_e, dy_e),
                textcoords="offset points",
                ha=ha_e,
                va="bottom",
                color="blue",
                fontsize=10,
                bbox=_label_bbox(),
                clip_on=True,
            )
        except Exception:
            pass


    ax_price.grid(True, alpha=0.15)
    _nice_time_axis(ax_price)

    # =========================================================
    # VOLUME panel (цветные)
    # =========================================================
    try:
        ax_vol.set_title("Volume")
        x_num = mdates.date2num(x)
        w = candle_w * 0.85

        vol_colors = ["green" if c_[i] >= o[i] else "red" for i in range(len(c_))]

        ax_vol.bar(
            x_num,
            vol,
            width=w,
            align="center",
            alpha=0.55,
            color=vol_colors,
            edgecolor=vol_colors,
            linewidth=0.2,
        )

        # --- AVG / THR / ANCH (как в логике сигнала) ---
        avg_v = _safe_float(volume_avg, 0.0)
        thr_v = _safe_float(volume_threshold, 0.0)
        req_pct = _safe_float(volume_change_pct, 0.0)
        if thr_v <= 0.0 and avg_v > 0.0 and req_pct > 0.0:
            thr_v = avg_v * (1.0 + req_pct / 100.0)
        anch_v = _safe_float(volume_anchor, 0.0)

        if avg_v > 0.0:
            ax_vol.axhline(avg_v, linestyle="--", color="black", alpha=0.25, linewidth=1.0)
        if thr_v > 0.0:
            ax_vol.axhline(thr_v, linestyle=":", color="orange", alpha=0.75, linewidth=1.2)

        # отметим anchor свечу вертикальной линией (если передан anchor_ts)
        try:
            idx_a = _find_nearest_index(x, anchor_ts) if anchor_ts is not None else None
            if idx_a is not None:
                ax_vol.axvline(x[idx_a], linestyle=":", color="black", alpha=0.12)
        except Exception:
            pass

        # подпись сверху слева
        if avg_v > 0.0 or thr_v > 0.0 or anch_v > 0.0:
            parts = []
            if avg_v > 0.0:
                parts.append(f"AVG={_fmt_compact(avg_v)}")
            if thr_v > 0.0:
                if req_pct > 0.0:
                    parts.append(f"THR={_fmt_compact(thr_v)} (REQ=+{req_pct:.0f}%)")
                else:
                    parts.append(f"THR={_fmt_compact(thr_v)}")
            if anch_v > 0.0:
                if avg_v > 0.0:
                    ratio = anch_v / avg_v
                    delta = (ratio - 1.0) * 100.0
                    parts.append(f"ANCH={_fmt_compact(anch_v)} | x{ratio:.2f} (Δ={delta:+.0f}%)")
                else:
                    parts.append(f"ANCH={_fmt_compact(anch_v)}")

            ax_vol.text(
                0.01,
                0.97,
                " | ".join(parts),
                transform=ax_vol.transAxes,
                va="top",
                ha="left",
                fontsize=9,
                bbox=_label_bbox(),
            )

        ax_vol.grid(True, alpha=0.15)
        ax_vol.set_ylabel("Vol")
    except Exception:
        pass


    # =========================================================
    # LIQUIDATIONS panel (СТОЛБИКИ ✅)
    # =========================================================
    ax_liq.set_title("Liquidations (USDT)  |  LONG (+)  /  SHORT (-)")
    ax_liq.axhline(0.0, linestyle="--", color="black", alpha=0.4)

    plotted = False

    # 1) серия из БД -> агрегируем по свечам
    lx, long_y, short_y = [], [], []
    if liquidation_series:
        try:
            lx, long_y, short_y = _aggregate_liq_to_candles(
                candles=candles,
                liq=liquidation_series,
                interval=timeframe,
            )
        except Exception:
            lx, long_y, short_y = [], [], []

    if lx and (len(lx) == len(long_y) == len(short_y)):
        try:
            x_num_l = mdates.date2num(lx)
            w = candle_w * 0.85

            # LONG вверх (зелёные)
            ax_liq.bar(
                x_num_l,
                long_y,
                width=w,
                align="center",
                alpha=0.55,
                color="green",
                edgecolor="green",
                linewidth=0.2,
                label="LONG liq (+)",
            )

            # SHORT вниз (красные)
            ax_liq.bar(
                x_num_l,
                short_y,
                width=w,
                align="center",
                alpha=0.55,
                color="red",
                edgecolor="red",
                linewidth=0.2,
                label="SHORT liq (-)",
            )

            plotted = True
        except Exception:
            plotted = False

    # 2) fallback: если нет liquidation_series — рисуем одну свечу
    if not plotted:
        base_idx = idx_touch if idx_touch is not None else idx_entry
        fx, fy_long, fy_short = _fallback_liq_series(
            candles=candles,
            idx=base_idx,
            liq_long_usdt=liq_long_usdt,
            liq_short_usdt=liq_short_usdt,
        )
        if fx:
            try:
                x_num_f = mdates.date2num(fx)
                w = candle_w * 0.85

                ax_liq.bar(
                    x_num_f,
                    fy_long,
                    width=w,
                    align="center",
                    alpha=0.55,
                    color="green",
                    edgecolor="green",
                    linewidth=0.2,
                    label="LONG liq (+)",
                )
                ax_liq.bar(
                    x_num_f,
                    fy_short,
                    width=w,
                    align="center",
                    alpha=0.55,
                    color="red",
                    edgecolor="red",
                    linewidth=0.2,
                    label="SHORT liq (-)",
                )
            except Exception:
                pass

    # вертикальные линии touch/entry на панели ликвидаций
    if idx_touch is not None:
        try:
            ax_liq.axvline(x[idx_touch], linestyle=":", color="purple", alpha=0.8)
        except Exception:
            pass

    if idx_entry is not None:
        try:
            ax_liq.axvline(x[idx_entry], linestyle="--", color="blue", alpha=0.8)
        except Exception:
            pass

    ax_liq.grid(True, alpha=0.15)
    ax_liq.legend(loc="upper left")
    _nice_time_axis(ax_liq)

    try:
        fig.tight_layout()
    except Exception:
        pass

    _ensure_dir(out_path.parent)
    fig.savefig(out_path, dpi=140, bbox_inches="tight")
