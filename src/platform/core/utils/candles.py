from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

# =========================================================
# UTC helpers
# =========================================================

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def utc(dt: Any) -> datetime:
    """Безопасно приводит к timezone-aware UTC datetime."""
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    raise TypeError(f"Expected datetime, got {type(dt)}")


def interval_to_timedelta(interval: str) -> timedelta:
    """'1m','5m','15m','1h','4h','1d' -> timedelta."""
    s = str(interval or "").strip().lower()
    if not s:
        return timedelta(hours=1)
    try:
        if s.endswith("m"):
            return timedelta(minutes=int(s[:-1]))
        if s.endswith("h"):
            return timedelta(hours=int(s[:-1]))
        if s.endswith("d"):
            return timedelta(days=int(s[:-1]))
    except Exception:
        return timedelta(hours=1)
    return timedelta(hours=1)


def floor_time(ts: datetime, interval: str) -> datetime:
    """Аналог date_bin(..., ts, epoch) для UTC."""
    ts = utc(ts)
    td = interval_to_timedelta(interval)
    step = td.total_seconds()
    if step <= 0:
        return ts
    off = (ts - _EPOCH).total_seconds()
    bucket = int(off // step) * step
    return _EPOCH + timedelta(seconds=bucket)


def aggregate_candles(
    candles: Sequence[Dict[str, Any]],
    *,
    target_interval: str,
) -> List[Dict[str, Any]]:
    """
    Агрегирует свечи (обычно 15m) в более крупный интервал (1h/4h/1d).

    Ожидаемые поля у входной свечи:
      ts, open, high, low, close, volume, quote_volume, cvd_quote (опц.)
    Выход:
      список свечей с ts = open_time бакета target_interval.
    """
    if not candles:
        return []

    # сортировка по времени на всякий случай
    src = sorted((dict(c) for c in candles if c and c.get("ts") is not None), key=lambda x: utc(x["ts"]))

    out: List[Dict[str, Any]] = []
    cur_bucket: Optional[datetime] = None
    acc: Optional[Dict[str, Any]] = None

    for c in src:
        ts = utc(c["ts"])
        b = floor_time(ts, target_interval)

        o = float(c.get("open") or 0.0)
        h = float(c.get("high") or 0.0)
        l = float(c.get("low") or 0.0)
        cl = float(c.get("close") or 0.0)
        v = float(c.get("volume") or 0.0)
        qv = float(c.get("quote_volume") or 0.0)
        cvd = c.get("cvd_quote")
        cvd_f = float(cvd) if cvd is not None else 0.0

        if cur_bucket is None or b != cur_bucket:
            # push previous
            if acc is not None:
                out.append(acc)

            cur_bucket = b
            acc = {
                "ts": b,
                "open": o,
                "high": h,
                "low": l,
                "close": cl,
                "volume": v,
                "quote_volume": qv,
                "cvd_quote": cvd_f,
                "_parts": 1,
            }
            continue

        # same bucket
        assert acc is not None
        acc["high"] = max(float(acc.get("high") or 0.0), h)
        acc["low"] = min(float(acc.get("low") or 0.0), l) if float(acc.get("low") or 0.0) else l
        acc["close"] = cl
        acc["volume"] = float(acc.get("volume") or 0.0) + v
        acc["quote_volume"] = float(acc.get("quote_volume") or 0.0) + qv
        acc["cvd_quote"] = float(acc.get("cvd_quote") or 0.0) + cvd_f
        acc["_parts"] = int(acc.get("_parts") or 0) + 1

    if acc is not None:
        out.append(acc)

    # убрать служебное
    for r in out:
        r.pop("_parts", None)

    return out


def slice_window(
    candles: Sequence[Dict[str, Any]],
    *,
    center_ts: datetime,
    interval: str,
    lookback: int,
    lookforward: int,
) -> List[Dict[str, Any]]:
    """Берёт окно свечей вокруг center_ts по open_time."""
    if not candles:
        return []
    center_ts = utc(center_ts)
    td = interval_to_timedelta(interval)
    start_ts = center_ts - td * int(lookback)
    end_ts = center_ts + td * int(lookforward)

    out = []
    for c in candles:
        ts = c.get("ts")
        if ts is None:
            continue
        t = utc(ts)
        if start_ts <= t <= end_ts:
            cc = dict(c)
            cc["ts"] = t
            out.append(cc)
    out.sort(key=lambda x: x["ts"])
    return out
