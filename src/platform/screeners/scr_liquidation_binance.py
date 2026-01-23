# src/platform/screeners/scr_liquidation_binance.py
from __future__ import annotations

import logging
from dataclasses import dataclass
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


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


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
    # price filter
    min_price: float = 0.000001
    max_price: float = 10.0

    # main interval
    interval: str = "1h"

    # liquidation per candle (USDT)
    volume_liquid_limit: float = 20_000.0

    # levels & volume spike
    period_levels: int = 60
    windows: int = 20
    kof_Volume: float = 20.0

    # confirmations
    # kof_fund трактуем как % (0.5 = 0.5% = 0.005)
    kof_fund: float = 0.5
    enable_funding: bool = True
    enable_oi: bool = True
    enable_cvd: bool = True

    # level touch tolerance (%)
    level_tol_pct: float = 0.0015  # 0.15%

    # debug
    debug: bool = False
    debug_top: int = 20


@dataclass
class ScreenerSignal:
    symbol_id: int
    symbol: str
    timeframe: str
    signal_ts: datetime
    side: str  # "Buy" / "Sell"

    entry_price: float
    exit_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None

    confidence: float = 0.0
    score: float = 0.0
    reason: str = ""
    context: Dict[str, Any] = None


# -------------------------
# Screener
# -------------------------

class ScrLiquidationBinance:
    """
    scr_liquidation_binance

    Логика (за 1 свечу):
      - строим уровни UP/DOWN по history (period_levels)
      - текущая свеча касается уровня
      - ликвидации за эту свечу >= лимита
      - объем свечи >= kof_Volume * avg(volume over windows)
      - подтверждение на 1 свечу (prev -> cur):
          * OI падает
          * CVD падает (для SHORT) / растет (для LONG)
          * funding: + для SHORT, - для LONG (порог в %)
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

        for sym in symbols:
            symbol_id = int(sym["symbol_id"])
            symbol = str(sym["symbol"])

            # --- last close (price filter) ---
            last_close = self._fetch_last_close(storage, exchange_id, symbol_id, p.interval)
            if last_close is None:
                continue

            if not (p.min_price <= last_close <= p.max_price):
                continue

            # --- candles ---
            need = max(220, p.period_levels + p.windows + 20)
            candles = self._fetch_last_candles(storage, exchange_id, symbol_id, p.interval, limit=need)
            if len(candles) < max(p.period_levels + 5, p.windows + 5):
                continue

            cur_c = candles[-1]
            prev_c = candles[-2]

            cur_ts = _utc(cur_c["ts"])
            prev_ts = _utc(prev_c["ts"])

            cur_close = _to_float(cur_c["close"])
            cur_high = _to_float(cur_c["high"])
            cur_low = _to_float(cur_c["low"])

            # --- levels from history (exclude current candle) ---
            lvl_slice = candles[-(p.period_levels + 1):-1]
            up_level = max(_to_float(c["high"]) for c in lvl_slice)
            down_level = min(_to_float(c["low"]) for c in lvl_slice)

            # --- volume spike ---
            vol_cur = _to_float(cur_c.get("quote_volume") or cur_c.get("volume"))
            avg_vol = self._avg_volume(candles=candles, windows=p.windows)
            if avg_vol <= 0:
                continue

            vol_ratio = vol_cur / avg_vol

            # --- liquidations for candle (USDT) ---
            liq_long_usdt, liq_short_usdt = self._fetch_liquidations_for_candle(
                storage=storage,
                exchange_id=exchange_id,
                symbol_id=symbol_id,
                candle_open_ts=cur_ts,
                interval=p.interval,
            )

            # --- touch logic ---
            touch_up = self._touch_up(high=cur_high, level=up_level, tol_pct=p.level_tol_pct)
            touch_down = self._touch_down(low=cur_low, level=down_level, tol_pct=p.level_tol_pct)

            # --- confirmations (prev -> cur) ---
            oi_prev = oi_cur = None
            cvd_prev = cvd_cur = None
            fund_cur = None

            if p.enable_oi:
                oi_prev = self._fetch_oi_at(storage, exchange_id, symbol_id, p.interval, prev_ts)
                oi_cur = self._fetch_oi_at(storage, exchange_id, symbol_id, p.interval, cur_ts)

            if p.enable_cvd:
                # cvd_quote stored in candles
                cvd_prev = self._fetch_cvd_at(storage, exchange_id, symbol_id, p.interval, prev_ts)
                cvd_cur = self._fetch_cvd_at(storage, exchange_id, symbol_id, p.interval, cur_ts)

            if p.enable_funding:
                fund_cur = self._fetch_funding_at(storage, exchange_id, symbol_id, cur_ts)

            # --- debug accumulate ---
            if p.debug:
                debug_rows.append(
                    {
                        "symbol": symbol,
                        "close": float(last_close),
                        "liq_short": float(liq_short_usdt),
                        "liq_long": float(liq_long_usdt),
                        "vol_ratio": float(vol_ratio),
                        "touch_up": bool(touch_up),
                        "touch_down": bool(touch_down),
                        "up": float(up_level),
                        "down": float(down_level),
                        "oi_prev": _to_float(oi_prev) if oi_prev is not None else None,
                        "oi_cur": _to_float(oi_cur) if oi_cur is not None else None,
                        "cvd_prev": _to_float(cvd_prev) if cvd_prev is not None else None,
                        "cvd_cur": _to_float(cvd_cur) if cvd_cur is not None else None,
                        "fund": _to_float(fund_cur) if fund_cur is not None else None,
                    }
                )

            # =========================================================
            # SELL (SHORT)
            # =========================================================
            if touch_up and (liq_short_usdt >= p.volume_liquid_limit) and (vol_ratio >= p.kof_Volume):
                if self._confirm_short(p, oi_prev, oi_cur, cvd_prev, cvd_cur, fund_cur):
                    entry = cur_close
                    sl = up_level * (1.0 + 0.003)  # +0.3%
                    tp = entry - (sl - entry) * 2.0 if entry < sl else None

                    out.append(
                        ScreenerSignal(
                            symbol_id=symbol_id,
                            symbol=symbol,
                            timeframe=p.interval,
                            signal_ts=cur_ts,
                            side="Sell",
                            entry_price=float(entry),
                            stop_loss=float(sl),
                            take_profit=float(tp) if tp else None,
                            confidence=0.80,
                            score=min(100.0, 40.0 + (liq_short_usdt / p.volume_liquid_limit) * 20.0 + vol_ratio),
                            reason="UP touch + short_liq + vol spike + confirm(OI↓/CVD↓/fund+)",
                            context={
                                "up_level": float(up_level),
                                "down_level": float(down_level),
                                "liq_short_usdt": float(liq_short_usdt),
                                "liq_long_usdt": float(liq_long_usdt),
                                "vol_ratio": float(vol_ratio),
                                "avg_vol": float(avg_vol),
                                "volume": float(vol_cur),
                                "oi_prev": _to_float(oi_prev) if oi_prev is not None else None,
                                "oi_cur": _to_float(oi_cur) if oi_cur is not None else None,
                                "cvd_prev": _to_float(cvd_prev) if cvd_prev is not None else None,
                                "cvd_cur": _to_float(cvd_cur) if cvd_cur is not None else None,
                                "funding": _to_float(fund_cur) if fund_cur is not None else None,
                            },
                        )
                    )

            # =========================================================
            # BUY (LONG)
            # =========================================================
            if touch_down and (liq_long_usdt >= p.volume_liquid_limit) and (vol_ratio >= p.kof_Volume):
                if self._confirm_long(p, oi_prev, oi_cur, cvd_prev, cvd_cur, fund_cur):
                    entry = cur_close
                    sl = down_level * (1.0 - 0.003)  # -0.3%
                    tp = entry + (entry - sl) * 2.0 if entry > sl else None

                    out.append(
                        ScreenerSignal(
                            symbol_id=symbol_id,
                            symbol=symbol,
                            timeframe=p.interval,
                            signal_ts=cur_ts,
                            side="Buy",
                            entry_price=float(entry),
                            stop_loss=float(sl),
                            take_profit=float(tp) if tp else None,
                            confidence=0.80,
                            score=min(100.0, 40.0 + (liq_long_usdt / p.volume_liquid_limit) * 20.0 + vol_ratio),
                            reason="DOWN touch + long_liq + vol spike + confirm(OI↓/CVD↑/fund-)",
                            context={
                                "up_level": float(up_level),
                                "down_level": float(down_level),
                                "liq_short_usdt": float(liq_short_usdt),
                                "liq_long_usdt": float(liq_long_usdt),
                                "vol_ratio": float(vol_ratio),
                                "avg_vol": float(avg_vol),
                                "volume": float(vol_cur),
                                "oi_prev": _to_float(oi_prev) if oi_prev is not None else None,
                                "oi_cur": _to_float(oi_cur) if oi_cur is not None else None,
                                "cvd_prev": _to_float(cvd_prev) if cvd_prev is not None else None,
                                "cvd_cur": _to_float(cvd_cur) if cvd_cur is not None else None,
                                "funding": _to_float(fund_cur) if fund_cur is not None else None,
                            },
                        )
                    )

        # --- DEBUG OUTPUT ---
        if p.debug and debug_rows:
            # top by total liquidation
            debug_rows.sort(key=lambda r: (r["liq_short"] + r["liq_long"]), reverse=True)
            log.info("=== DEBUG TOP %d BY (liq_short+liq_long) interval=%s ===", p.debug_top, p.interval)
            for r in debug_rows[: max(1, p.debug_top)]:
                log.info(
                    "%s close=%.6f liqS=%.0f liqL=%.0f volR=%.2f touchUP=%s touchDN=%s fund=%s",
                    r["symbol"],
                    r["close"],
                    r["liq_short"],
                    r["liq_long"],
                    r["vol_ratio"],
                    r["touch_up"],
                    r["touch_down"],
                    r["fund"],
                )

            # top by volume ratio
            debug_rows.sort(key=lambda r: r["vol_ratio"], reverse=True)
            log.info("=== DEBUG TOP %d BY VOL_RATIO interval=%s ===", p.debug_top, p.interval)
            for r in debug_rows[: max(1, p.debug_top)]:
                log.info(
                    "%s volR=%.2f liqS=%.0f liqL=%.0f close=%.6f",
                    r["symbol"],
                    r["vol_ratio"],
                    r["liq_short"],
                    r["liq_long"],
                    r["close"],
                )

        return out

    # -------------------------
    # params / confirms
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

        p.debug = bool(params.get("debug", False))
        p.debug_top = int(params.get("debug_top", 20))

        return p

    def _fund_threshold(self, p: ScrParams) -> float:
        # kof_fund задаем в процентах: 0.5 => 0.5% => 0.005
        return abs(float(p.kof_fund)) / 100.0

    def _confirm_short(
        self,
        p: ScrParams,
        oi_prev: Any,
        oi_cur: Any,
        cvd_prev: Any,
        cvd_cur: Any,
        fund_cur: Any,
    ) -> bool:
        # OI must drop
        if p.enable_oi:
            if oi_prev is None or oi_cur is None:
                return False
            if _to_float(oi_cur) >= _to_float(oi_prev):
                return False

        # CVD must drop
        if p.enable_cvd:
            if cvd_prev is None or cvd_cur is None:
                return False
            if _to_float(cvd_cur) >= _to_float(cvd_prev):
                return False

        # funding must be positive and above threshold
        if p.enable_funding:
            if fund_cur is None:
                return False
            thr = self._fund_threshold(p)
            if _to_float(fund_cur) < thr:
                return False

        return True

    def _confirm_long(
        self,
        p: ScrParams,
        oi_prev: Any,
        oi_cur: Any,
        cvd_prev: Any,
        cvd_cur: Any,
        fund_cur: Any,
    ) -> bool:
        # OI must drop
        if p.enable_oi:
            if oi_prev is None or oi_cur is None:
                return False
            if _to_float(oi_cur) >= _to_float(oi_prev):
                return False

        # CVD must rise
        if p.enable_cvd:
            if cvd_prev is None or cvd_cur is None:
                return False
            if _to_float(cvd_cur) <= _to_float(cvd_prev):
                return False

        # funding must be negative and below -threshold
        if p.enable_funding:
            if fund_cur is None:
                return False
            thr = self._fund_threshold(p)
            if _to_float(fund_cur) > -thr:
                return False

        return True

    # -------------------------
    # math helpers
    # -------------------------

    def _avg_volume(self, *, candles: Sequence[Dict[str, Any]], windows: int) -> float:
        if len(candles) < windows + 2:
            return 0.0
        # exclude current candle
        part = candles[-(windows + 1):-1]
        vals = [_to_float(c.get("quote_volume") or c.get("volume")) for c in part]
        vals = [v for v in vals if v > 0]
        if not vals:
            return 0.0
        return sum(vals) / float(len(vals))

    def _touch_up(self, *, high: float, level: float, tol_pct: float) -> bool:
        if level <= 0:
            return False
        tol = level * tol_pct
        return high >= (level - tol)

    def _touch_down(self, *, low: float, level: float, tol_pct: float) -> bool:
        if level <= 0:
            return False
        tol = level * tol_pct
        return low <= (level + tol)

    # -------------------------
    # DB fetchers (schema-safe)
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
        # ✅ candles: open_time
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
        # ✅ open_time AS ts (для унификации)
        q = """
        SELECT
            open_time AS ts,
            open, high, low, close,
            volume,
            quote_volume,
            cvd_quote
        FROM candles
        WHERE exchange_id=%s AND symbol_id=%s AND interval=%s
        ORDER BY open_time ASC
        LIMIT %s
        """
        with storage.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (int(exchange_id), int(symbol_id), str(interval), int(limit)))
                rows = cur.fetchall()

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
        # cvd_quote in candles
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
        # ✅ funding_time
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
        """
        liquidation_1m:
          long_notional  = ликвидации LONG
          short_notional = ликвидации SHORT

        Возвращаем: (liq_long_usdt, liq_short_usdt)
        """
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
