# src/platform/screeners/plotting.py
from __future__ import annotations

from pathlib import Path
from typing import Optional, Sequence, Dict, Any

# ✅ IMPORTANT: fix Windows/Server crash (no GUI backend)
# Must be set BEFORE importing pyplot
import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def save_signal_plot(
    *,
    out_path: Path,
    symbol: str,
    timeframe: str,
    candles: Sequence[Dict[str, Any]],
    signal_ts: Any,
    side: str,
    entry_price: float,
    up_level: Optional[float] = None,
    down_level: Optional[float] = None,
    liq_short_usdt: Optional[float] = None,
    liq_long_usdt: Optional[float] = None,
    enable_oi: bool = False,
    oi_series: Optional[Sequence[Dict[str, Any]]] = None,
    enable_cvd: bool = False,
    cvd_series: Optional[Sequence[Dict[str, Any]]] = None,
    enable_funding: bool = False,
    funding_series: Optional[Sequence[Dict[str, Any]]] = None,
) -> None:
    """
    Надёжный PNG-плот по сигналу (без GUI, безопасно для Windows Server).

    Рисуем:
      - Close price (line)
      - UP/DOWN level
      - Entry marker
      - Volume
      - OI / CVD / Funding (опционально)
    """

    if not candles:
        return

    # --- prepare arrays ---
    try:
        x = [c["ts"] for c in candles]
    except Exception:
        return

    close = [_safe_float(c.get("close"), 0.0) for c in candles]
    vol = [_safe_float(c.get("quote_volume") or c.get("volume"), 0.0) for c in candles]

    # Panels:
    # 0 price
    # 1 volume
    panels = 2
    if enable_oi:
        panels += 1
    if enable_cvd:
        panels += 1
    if enable_funding:
        panels += 1

    fig_h = max(6, panels * 2.2)

    fig, axes = plt.subplots(panels, 1, figsize=(14, fig_h), sharex=True)
    if panels == 1:
        axes = [axes]

    ax_price = axes[0]
    ax_vol = axes[1]

    # --- PRICE ---
    ax_price.plot(x, close)
    ax_price.set_title(f"{symbol} [{timeframe}] {side} | entry={entry_price}")

    if up_level is not None:
        ax_price.axhline(_safe_float(up_level), linestyle="--")
        ax_price.text(x[0], _safe_float(up_level), f"UP={up_level}", va="bottom")

    if down_level is not None:
        ax_price.axhline(_safe_float(down_level), linestyle="--")
        ax_price.text(x[0], _safe_float(down_level), f"DOWN={down_level}", va="bottom")

    # Entry marker
    ax_price.axhline(_safe_float(entry_price), linestyle=":")
    ax_price.text(x[-1], _safe_float(entry_price), f"ENTRY={entry_price}", ha="right", va="bottom")

    # Info text
    info_lines = []
    if liq_short_usdt is not None:
        info_lines.append(f"liq_short_usdt={_safe_float(liq_short_usdt):,.0f}")
    if liq_long_usdt is not None:
        info_lines.append(f"liq_long_usdt={_safe_float(liq_long_usdt):,.0f}")
    if info_lines:
        ax_price.text(
            0.01,
            0.98,
            "\n".join(info_lines),
            transform=ax_price.transAxes,
            ha="left",
            va="top",
        )

    # --- VOLUME ---
    ax_vol.plot(x, vol)
    ax_vol.set_title("Volume (quote_volume)")

    idx = 2

    # --- OI ---
    if enable_oi:
        ax = axes[idx]
        idx += 1
        if oi_series:
            xx = [r["ts"] for r in oi_series]
            yy = [_safe_float(r.get("open_interest") or r.get("oi"), 0.0) for r in oi_series]
            ax.plot(xx, yy)
        ax.set_title("Open Interest")

    # --- CVD ---
    if enable_cvd:
        ax = axes[idx]
        idx += 1
        if cvd_series:
            xx = [r["ts"] for r in cvd_series]
            yy = [_safe_float(r.get("cvd_quote") or r.get("cvd"), 0.0) for r in cvd_series]
            ax.plot(xx, yy)
        ax.set_title("CVD")

    # --- FUNDING ---
    if enable_funding:
        ax = axes[idx]
        idx += 1
        if funding_series:
            xx = [r["ts"] for r in funding_series]
            yy = [_safe_float(r.get("funding_rate") or r.get("rate"), 0.0) for r in funding_series]
            ax.plot(xx, yy)
        ax.set_title("Funding Rate")

    fig.tight_layout()

    _ensure_dir(out_path.parent)
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
