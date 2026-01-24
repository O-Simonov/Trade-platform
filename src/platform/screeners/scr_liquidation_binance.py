# src/platform/screeners/scr_liquidation_binance.py
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

log = logging.getLogger("screeners.scr_liquidation_binance")


# -------------------------
# utils
# -------------------------

def _utc(dt: Any) -> datetime:
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    raise TypeError(f"Expected datetime, got {type(dt)}")


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _parse_interval(interval: str) -> timedelta:
    s = str(interval).strip().lower()
    if s.endswith("m"):
        return timedelta(minutes=int(s[:-1]))
    if s.endswith("h"):
        return timedelta(hours=int(s[:-1]))
    if s.endswith("d"):
        return timedelta(days=int(s[:-1]))
    return timedelta(hours=1)


# -------------------------
# dataclasses
# -------------------------

@dataclass
class ScrParams:
    min_price: float = 0.00000001
    max_price: float = 100000.0

    interval: str = "1h"

    volume_liquid_limit: float = 5_000.0

    period_levels: int = 60
    windows: int = 20
    kof_Volume: float = 10.0

    # kof_fund трактуем как % (0.5 = 0.5% = 0.005)
    kof_fund: float = 0.5
    enable_funding: bool = True
    enable_oi: bool = True
    enable_cvd: bool = True

    level_tol_pct: float = 0.01

    pivot_left: int = 2
    pivot_right: int = 2
    level_cluster_tol_pct: float = 0.003
    max_level_candidates: int = 14

    confirm_lookforward: int = 5

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


# -------------------------
# Screener
# -------------------------

class ScrLiquidationBinance:
    """
    scr_liquidation_binance

    SELL (SHORT):
      - touch UP
      - short_liq >= limit
      - vol spike >= kof_Volume * avg_vol
      - в одной из след. N свечей:
          цена падает (close < anchor_close)
          OI падает (oi_confirm < oi_anchor) [если enable_oi]
          CVD падает (cvd_confirm < cvd_anchor) [если enable_cvd]
          funding положительный и >= threshold [если enable_funding]

    BUY (LONG):
      - touch DOWN
      - long_liq >= limit
      - vol spike >= kof_Volume * avg_vol
      - в одной из след. N свечей:
          цена растет (close > anchor_close)
          OI падает (oi_confirm < oi_anchor) [если enable_oi]
          CVD растет (cvd_confirm > cvd_anchor) [если enable_cvd]
          funding отрицательный и <= -threshold [если enable_funding]
    """

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

        need = max(260, p.period_levels + p.windows + p.confirm_lookforward + 40)

        for sym in symbols:
            symbol_id = int(sym["symbol_id"])
            symbol = str(sym["symbol"])

            last_close = self._fetch_last_close(storage, exchange_id, symbol_id, p.interval)
            if last_close is None:
                continue
            if not (p.min_price <= last_close <= p.max_price):
                continue

            candles = self._fetch_last_candles(storage, exchange_id, symbol_id, p.interval, limit=need)
            if len(candles) < max(p.period_levels + p.windows + p.confirm_lookforward + 10, 120):
                continue

            lf = max(0, int(p.confirm_lookforward))
            anchor_idx = len(candles) - 1 - lf
            if anchor_idx <= 10 or anchor_idx >= len(candles):
                continue

            anchor = candles[anchor_idx]
            anchor_ts = _utc(anchor["ts"])
            anchor_close = _to_float(anchor["close"])
            anchor_high = _to_float(anchor["high"])
            anchor_low = _to_float(anchor["low"])

            confirm_window = candles[anchor_idx + 1: anchor_idx + 1 + lf] if lf > 0 else []

            lvl_hist = candles[max(0, anchor_idx - (p.period_levels + 20)): anchor_idx]
            up_level, down_level, lvl_meta = self._build_levels(
                candles=lvl_hist,
                ref_price=anchor_close,
                p=p,
            )
            if up_level <= 0 or down_level <= 0:
                continue

            vol_anchor = _to_float(anchor.get("quote_volume") or anchor.get("volume"))
            avg_vol = self._avg_volume_before(candles=candles, idx=anchor_idx, windows=p.windows)
            if avg_vol <= 0:
                continue
            vol_ratio = vol_anchor / avg_vol

            liq_long_usdt, liq_short_usdt = self._fetch_liquidations_for_candle(
                storage=storage,
                exchange_id=exchange_id,
                symbol_id=symbol_id,
                candle_open_ts=anchor_ts,
                interval=p.interval,
            )

            touch_up = self._touch_up(high=anchor_high, level=up_level, tol_pct=p.level_tol_pct)
            touch_down = self._touch_down(low=anchor_low, level=down_level, tol_pct=p.level_tol_pct)

            oi_anchor = cvd_anchor = fund_anchor = None
            if p.enable_oi:
                oi_anchor = self._fetch_oi_at(storage, exchange_id, symbol_id, p.interval, anchor_ts)
            if p.enable_cvd:
                cvd_anchor = self._fetch_cvd_at(storage, exchange_id, symbol_id, p.interval, anchor_ts)
            if p.enable_funding:
                fund_anchor = self._fetch_funding_at(storage, exchange_id, symbol_id, anchor_ts)

            if p.debug:
                debug_rows.append(
                    {
                        "symbol": symbol,
                        "close": float(last_close),
                        "up": float(up_level),
                        "down": float(down_level),
                        "touch_up": bool(touch_up),
                        "touch_down": bool(touch_down),
                        "liq_short": float(liq_short_usdt),
                        "liq_long": float(liq_long_usdt),
                        "vol_ratio": float(vol_ratio),
                        "oi_anchor": _to_float(oi_anchor) if oi_anchor is not None else None,
                        "cvd_anchor": _to_float(cvd_anchor) if cvd_anchor is not None else None,
                        "fund_anchor": _to_float(fund_anchor) if fund_anchor is not None else None,
                        "lvl_strength_up": lvl_meta.get("up_strength"),
                        "lvl_strength_down": lvl_meta.get("down_strength"),
                    }
                )

            # =========================================================
            # SELL (SHORT)
            # =========================================================
            if touch_up and (liq_short_usdt >= p.volume_liquid_limit) and (vol_ratio >= p.kof_Volume):
                hit = self._find_confirmation_short(
                    storage=storage,
                    exchange_id=exchange_id,
                    symbol_id=symbol_id,
                    interval=p.interval,
                    p=p,
                    anchor_ts=anchor_ts,
                    anchor_close=anchor_close,
                    oi_anchor=oi_anchor,
                    cvd_anchor=cvd_anchor,
                    confirm_window=confirm_window,
                )

                if hit is not None:
                    confirm_ts, entry_price, oi_c, cvd_c, fund_c, why = hit

                    sl = up_level * (1.0 + 0.003)
                    tp = entry_price - (sl - entry_price) * 2.0 if entry_price < sl else None

                    out.append(
                        ScreenerSignal(
                            symbol_id=symbol_id,
                            symbol=symbol,
                            timeframe=p.interval,
                            signal_ts=confirm_ts,
                            side="SELL",
                            entry_price=float(entry_price),
                            stop_loss=float(sl),
                            take_profit=float(tp) if tp else None,
                            confidence=0.82,
                            score=min(100.0, 40.0 + (liq_short_usdt / p.volume_liquid_limit) * 25.0 + vol_ratio),
                            reason=why,
                            context={
                                "touch_ts": anchor_ts,
                                "confirm_ts": confirm_ts,
                                "up_level": float(up_level),
                                "down_level": float(down_level),
                                "liq_short_usdt": float(liq_short_usdt),
                                "liq_long_usdt": float(liq_long_usdt),
                                "vol_ratio": float(vol_ratio),
                                "avg_vol": float(avg_vol),
                                "volume": float(vol_anchor),
                                "oi_anchor": _to_float(oi_anchor) if oi_anchor is not None else None,
                                "oi_confirm": _to_float(oi_c) if oi_c is not None else None,
                                "cvd_anchor": _to_float(cvd_anchor) if cvd_anchor is not None else None,
                                "cvd_confirm": _to_float(cvd_c) if cvd_c is not None else None,
                                "funding_confirm": _to_float(fund_c) if fund_c is not None else None,
                                "levels_meta": lvl_meta,
                            },
                        )
                    )

            # =========================================================
            # BUY (LONG)
            # =========================================================
            if touch_down and (liq_long_usdt >= p.volume_liquid_limit) and (vol_ratio >= p.kof_Volume):
                hit = self._find_confirmation_long(
                    storage=storage,
                    exchange_id=exchange_id,
                    symbol_id=symbol_id,
                    interval=p.interval,
                    p=p,
                    anchor_ts=anchor_ts,
                    anchor_close=anchor_close,
                    oi_anchor=oi_anchor,
                    cvd_anchor=cvd_anchor,
                    confirm_window=confirm_window,
                )

                if hit is not None:
                    confirm_ts, entry_price, oi_c, cvd_c, fund_c, why = hit

                    sl = down_level * (1.0 - 0.003)
                    tp = entry_price + (entry_price - sl) * 2.0 if entry_price > sl else None

                    out.append(
                        ScreenerSignal(
                            symbol_id=symbol_id,
                            symbol=symbol,
                            timeframe=p.interval,
                            signal_ts=confirm_ts,
                            side="BUY",
                            entry_price=float(entry_price),
                            stop_loss=float(sl),
                            take_profit=float(tp) if tp else None,
                            confidence=0.82,
                            score=min(100.0, 40.0 + (liq_long_usdt / p.volume_liquid_limit) * 25.0 + vol_ratio),
                            reason=why,
                            context={
                                "touch_ts": anchor_ts,
                                "confirm_ts": confirm_ts,
                                "up_level": float(up_level),
                                "down_level": float(down_level),
                                "liq_short_usdt": float(liq_short_usdt),
                                "liq_long_usdt": float(liq_long_usdt),
                                "vol_ratio": float(vol_ratio),
                                "avg_vol": float(avg_vol),
                                "volume": float(vol_anchor),
                                "oi_anchor": _to_float(oi_anchor) if oi_anchor is not None else None,
                                "oi_confirm": _to_float(oi_c) if oi_c is not None else None,
                                "cvd_anchor": _to_float(cvd_anchor) if cvd_anchor is not None else None,
                                "cvd_confirm": _to_float(cvd_c) if cvd_c is not None else None,
                                "funding_confirm": _to_float(fund_c) if fund_c is not None else None,
                                "levels_meta": lvl_meta,
                            },
                        )
                    )

        if p.debug and debug_rows:
            debug_rows.sort(key=lambda r: (r["liq_short"] + r["liq_long"]), reverse=True)
            log.info("=== DEBUG TOP %d BY (liq_short+liq_long) interval=%s ===", p.debug_top, p.interval)
            for r in debug_rows[: max(1, p.debug_top)]:
                log.info(
                    "%s close=%.6f up=%.6f down=%.6f liqS=%.0f liqL=%.0f volR=%.2f touchUP=%s touchDN=%s fund=%s lvlU=%s lvlD=%s",
                    r["symbol"], r["close"], r["up"], r["down"],
                    r["liq_short"], r["liq_long"], r["vol_ratio"],
                    r["touch_up"], r["touch_down"],
                    r["fund_anchor"],
                    r["lvl_strength_up"], r["lvl_strength_down"]
                )

        return out

    # -------------------------
    # params
    # -------------------------

    def _parse_params(self, *, interval: str, params: Dict[str, Any]) -> ScrParams:
        p = ScrParams()
        p.interval = str(params.get("interval", interval))

        p.min_price = float(params.get("min_price", p.min_price))
        p.max_price = float(params.get("max_price", p.max_price))

        p.volume_liquid_limit = float(params.get("volume_liquid_limit", p.volume_liquid_limit))

        p.period_levels = int(params.get("period_levels", p.period_levels))
        p.windows = int(params.get("windows", p.windows))
        p.kof_Volume = float(params.get("kof_Volume", p.kof_Volume))

        p.kof_fund = float(params.get("kof_fund", p.kof_fund))

        p.enable_funding = bool(params.get("enable_funding", p.enable_funding))
        p.enable_oi = bool(params.get("enable_oi", p.enable_oi))
        p.enable_cvd = bool(params.get("enable_cvd", p.enable_cvd))

        p.level_tol_pct = float(params.get("level_tol_pct", p.level_tol_pct))

        p.pivot_left = int(params.get("pivot_left", p.pivot_left))
        p.pivot_right = int(params.get("pivot_right", p.pivot_right))
        p.level_cluster_tol_pct = float(params.get("level_cluster_tol_pct", p.level_cluster_tol_pct))
        p.max_level_candidates = int(params.get("max_level_candidates", p.max_level_candidates))

        p.confirm_lookforward = int(params.get("confirm_lookforward", p.confirm_lookforward))

        p.debug = bool(params.get("debug", False))
        p.debug_top = int(params.get("debug_top", 20))
        return p

    def _fund_threshold(self, p: ScrParams) -> float:
        return abs(float(p.kof_fund)) / 100.0

    # -------------------------
    # confirmation: next N candles
    # -------------------------

    def _find_confirmation_short(
        self,
        *,
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        interval: str,
        p: ScrParams,
        anchor_ts: datetime,
        anchor_close: float,
        oi_anchor: Any,
        cvd_anchor: Any,
        confirm_window: Sequence[Dict[str, Any]],
    ) -> Optional[Tuple[datetime, float, Any, Any, Any, str]]:
        if not confirm_window:
            return None

        thr = self._fund_threshold(p)

        for c in confirm_window:
            ts = _utc(c["ts"])
            close = _to_float(c["close"])

            if close >= anchor_close:
                continue

            oi_c = cvd_c = fund_c = None

            if p.enable_oi:
                oi_c = self._fetch_oi_at(storage, exchange_id, symbol_id, interval, ts)
                if oi_anchor is None or oi_c is None:
                    continue
                if _to_float(oi_c) >= _to_float(oi_anchor):
                    continue

            if p.enable_cvd:
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

            why = "CONFIRM SHORT: price↓ within next candles + OI↓/CVD↓ + funding+"
            return ts, close, oi_c, cvd_c, fund_c, why

        return None

    def _find_confirmation_long(
        self,
        *,
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        interval: str,
        p: ScrParams,
        anchor_ts: datetime,
        anchor_close: float,
        oi_anchor: Any,
        cvd_anchor: Any,
        confirm_window: Sequence[Dict[str, Any]],
    ) -> Optional[Tuple[datetime, float, Any, Any, Any, str]]:
        if not confirm_window:
            return None

        thr = self._fund_threshold(p)

        for c in confirm_window:
            ts = _utc(c["ts"])
            close = _to_float(c["close"])

            if close <= anchor_close:
                continue

            oi_c = cvd_c = fund_c = None

            if p.enable_oi:
                oi_c = self._fetch_oi_at(storage, exchange_id, symbol_id, interval, ts)
                if oi_anchor is None or oi_c is None:
                    continue
                if _to_float(oi_c) >= _to_float(oi_anchor):
                    continue

            if p.enable_cvd:
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

            why = "CONFIRM LONG: price↑ within next candles + OI↓/CVD↑ + funding-"
            return ts, close, oi_c, cvd_c, fund_c, why

        return None

    # -------------------------
    # smarter levels: pivots + clustering
    # -------------------------

    def _build_levels(
        self,
        *,
        candles: Sequence[Dict[str, Any]],
        ref_price: float,
        p: ScrParams,
    ) -> Tuple[float, float, Dict[str, Any]]:
        """
        FIX: гарантируем корректную геометрию уровней:
          DOWN <= ref_price <= UP
        иначе fallback:
          DOWN = min_low, UP = max_high
        """
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
            return (max_high, min_low, meta)

        piv_h, piv_l = self._find_pivots(candles=candles, left=p.pivot_left, right=p.pivot_right)
        meta["pivot_highs"] = len(piv_h)
        meta["pivot_lows"] = len(piv_l)

        if len(piv_h) < 2 or len(piv_l) < 2:
            meta["fallback"] = True
            return (max_high, min_low, meta)

        cl_up = self._cluster_levels(piv_h, tol_pct=p.level_cluster_tol_pct, max_candidates=p.max_level_candidates)
        cl_dn = self._cluster_levels(piv_l, tol_pct=p.level_cluster_tol_pct, max_candidates=p.max_level_candidates)

        meta["clusters_up"] = cl_up
        meta["clusters_down"] = cl_dn

        up = self._pick_level_above(ref_price, cl_up)
        down = self._pick_level_below(ref_price, cl_dn)

        # --- FIX: если не найден нормальный уровень, берём реальные экстремумы ---
        if up <= 0.0:
            up = max_high
        if down <= 0.0:
            down = min_low

        # --- FIX: если уровни оказались "по неправильную сторону" цены ---
        # Например ref_price ниже всех pivot low -> down получится выше цены -> это ломает логику BUY.
        ref = float(ref_price)
        if down > ref:
            down = min_low
            meta["fallback"] = True
            meta["fallback_down"] = "min_low"
        if up < ref:
            up = max_high
            meta["fallback"] = True
            meta["fallback_up"] = "max_high"

        # если вдруг всё ещё криво
        if up <= 0 or down <= 0 or up <= down:
            meta["fallback"] = True
            return (max_high, min_low, meta)

        meta["up_strength"] = self._cluster_strength(up, cl_up)
        meta["down_strength"] = self._cluster_strength(down, cl_dn)

        return float(up), float(down), meta

    def _find_pivots(
        self,
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

            if h > max(left_h) and h > max(right_h):
                piv_h.append(h)

            if l < min(left_l) and l < min(right_l):
                piv_l.append(l)

        return piv_h, piv_l

    def _cluster_levels(
        self,
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

    def _pick_level_above(self, ref_price: float, clusters: Sequence[Tuple[float, int]]) -> float:
        ref = float(ref_price)
        above = [(price, cnt) for (price, cnt) in clusters if price >= ref]
        if not above:
            return 0.0
        above.sort(key=lambda x: (x[0] - ref, -x[1]))
        return float(above[0][0])

    def _pick_level_below(self, ref_price: float, clusters: Sequence[Tuple[float, int]]) -> float:
        ref = float(ref_price)
        below = [(price, cnt) for (price, cnt) in clusters if price <= ref]
        if not below:
            return 0.0
        below.sort(key=lambda x: (ref - x[0], -x[1]))
        return float(below[0][0])

    def _cluster_strength(self, level: float, clusters: Sequence[Tuple[float, int]]) -> int:
        for (price, cnt) in clusters:
            if abs(price - level) <= max(1e-12, level * 1e-6):
                return int(cnt)
        return 0

    # -------------------------
    # math helpers
    # -------------------------

    def _avg_volume_before(self, *, candles: Sequence[Dict[str, Any]], idx: int, windows: int) -> float:
        windows = int(windows)
        if windows <= 1:
            return 0.0
        if idx <= windows + 1:
            return 0.0

        part = candles[idx - windows: idx]
        vals = [_to_float(c.get("quote_volume") or c.get("volume")) for c in part]
        vals = [v for v in vals if v > 0]
        if not vals:
            return 0.0
        return sum(vals) / float(len(vals))

    def _touch_up(self, *, high: float, level: float, tol_pct: float) -> bool:
        if level <= 0:
            return False
        tol = level * float(tol_pct)
        return float(high) >= (float(level) - tol)

    def _touch_down(self, *, low: float, level: float, tol_pct: float) -> bool:
        if level <= 0:
            return False
        tol = level * float(tol_pct)
        return float(low) <= (float(level) + tol)

    # -------------------------
    # DB fetchers
    # -------------------------

    def _fetch_symbols(self, *, storage: Any, exchange_id: int) -> List[Dict[str, Any]]:
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

    def _fetch_last_close(
        self,
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        interval: str,
    ) -> Optional[float]:
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

    def _fetch_last_candles(
        self,
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        interval: str,
        limit: int,
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
                    "cvd_quote": _to_float(r[7]),
                }
            )
        return out

    def _fetch_oi_at(
        self,
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        interval: str,
        ts: datetime,
    ) -> Optional[float]:
        q = """
        SELECT open_interest
        FROM open_interest
        WHERE exchange_id=%s AND symbol_id=%s AND interval=%s AND ts <= %s
        ORDER BY ts DESC
        LIMIT 1
        """
        with storage.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (int(exchange_id), int(symbol_id), str(interval), _utc(ts)))
                r = cur.fetchone()
                return _to_float(r[0]) if r else None

    def _fetch_cvd_at(
        self,
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        interval: str,
        ts: datetime,
    ) -> Optional[float]:
        q = """
        SELECT cvd_quote
        FROM candles
        WHERE exchange_id=%s AND symbol_id=%s AND interval=%s AND open_time <= %s
        ORDER BY open_time DESC
        LIMIT 1
        """
        with storage.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (int(exchange_id), int(symbol_id), str(interval), _utc(ts)))
                r = cur.fetchone()
                return _to_float(r[0]) if r else None

    def _fetch_funding_at(
        self,
        storage: Any,
        exchange_id: int,
        symbol_id: int,
        ts: datetime,
    ) -> Optional[float]:
        q = """
        SELECT funding_rate
        FROM funding
        WHERE exchange_id=%s AND symbol_id=%s AND funding_time <= %s
        ORDER BY funding_time DESC
        LIMIT 1
        """
        with storage.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (int(exchange_id), int(symbol_id), _utc(ts)))
                r = cur.fetchone()
                return _to_float(r[0]) if r else None

    def _fetch_liquidations_for_candle(
        self,
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
