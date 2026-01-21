# src/platform/exchanges/binance/collector_open_interest.py
from __future__ import annotations

import logging
import random
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

log = logging.getLogger("src.platform.exchanges.binance.collector_open_interest")

# Binance Open Interest Hist periods:
#  "5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"
_INTERVAL_SEC: Dict[str, int] = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "6h": 21600,
    "12h": 43200,
    "1d": 86400,
}


# -------------------------
# helpers
# -------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _dt_from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)


def _safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def _floor_ts(dt: datetime, interval: str) -> datetime:
    """
    Floor datetime to candle boundary (UTC).
    Works for supported intervals above.
    """
    sec = _INTERVAL_SEC.get(str(interval))
    if not sec:
        return dt.replace(second=0, microsecond=0)

    ts = int(dt.timestamp())
    floored = (ts // sec) * sec
    return datetime.fromtimestamp(floored, tz=timezone.utc)


def _chunks(xs: Sequence[int], n: int) -> List[List[int]]:
    n = max(1, int(n))
    out: List[List[int]] = []
    cur: List[int] = []
    for x in xs:
        cur.append(int(x))
        if len(cur) >= n:
            out.append(cur)
            cur = []
    if cur:
        out.append(cur)
    return out


def _normalize_wm_value(v: Any) -> Optional[datetime]:
    """
    Приводит watermark значение к datetime(UTC), если возможно.
    Поддерживает:
      - datetime
      - int/float (ms or sec)
      - str ISO
    """
    if v is None:
        return None

    if isinstance(v, datetime):
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)

    if isinstance(v, (int, float)):
        x = float(v)
        # heuristic: ms vs sec
        if x > 10_000_000_000:
            return _dt_from_ms(int(x))
        return datetime.fromtimestamp(int(x), tz=timezone.utc)

    if isinstance(v, str):
        try:
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    return None


def _jitter(sec: float) -> float:
    s = float(max(0.0, sec))
    if s <= 0:
        return 0.0
    return random.uniform(0.0, s)


# -------------------------
# config
# -------------------------

@dataclass
class OIDynamicBudget:
    enabled: bool = False
    safety_factor: float = 0.9
    endpoint_weight: float = 2.0
    min_symbols_per_tick: int = 5
    max_symbols_per_tick_cap: int = 80


@dataclass
class OIAdaptive:
    enabled: bool = False
    penalty_step: int = 5
    recovery_step: int = 1
    extra_sleep_penalty_sec: float = 0.05
    extra_sleep_recovery_sec: float = 0.01
    extra_sleep_max_sec: float = 0.5
    post_tick_sleep_max_sec: float = 15.0


@dataclass
class OIConfig:
    enabled: bool = True

    # ✅ правильный default для dataclass
    intervals: List[str] = field(default_factory=list)  # ["5m","15m","1h","4h","1d"]

    seed_on_start: bool = True
    seed_days: int = 30
    seed_limit: int = 500
    seed_max_pages_per_symbol: int = 8
    seed_symbols_per_cycle: int = 5

    per_request_sleep_sec: float = 0.20

    # legacy (kept for backward compatibility)
    poll_sec: float = 60.0
    batch_size: int = 10

    overlap_minutes: int = 60
    safety_lag_sec: int = 15

    refresh_symbols_sec: int = 3600
    log_each_cycle: bool = True

    stall_hits_to_blacklist: int = 3
    blacklist_hours: int = 6
    blacklist_log_every_sec: int = 120

    # ✅ schedule всегда dict, а не None
    schedule: Dict[str, int] = field(default_factory=dict)
    schedule_jitter_sec: float = 1.0

    # ✅ default_factory чтобы не было None
    dynamic_budget: OIDynamicBudget = field(default_factory=OIDynamicBudget)
    adaptive: OIAdaptive = field(default_factory=OIAdaptive)

    @staticmethod
    def from_cfg(cfg: dict) -> "OIConfig":
        c = OIConfig()

        c.enabled = bool(cfg.get("enabled", True))

        c.intervals = list(cfg.get("intervals") or ["5m", "15m", "1h", "4h", "1d"])

        c.seed_on_start = bool(cfg.get("seed_on_start", True))
        c.seed_days = int(cfg.get("seed_days", 30))
        c.seed_limit = int(cfg.get("seed_limit", 500))
        c.seed_max_pages_per_symbol = int(cfg.get("seed_max_pages_per_symbol", 8))
        c.seed_symbols_per_cycle = int(cfg.get("seed_symbols_per_cycle", 5))

        c.per_request_sleep_sec = float(cfg.get("per_request_sleep_sec", 0.20))

        c.poll_sec = float(cfg.get("poll_sec", 60.0))
        c.batch_size = int(cfg.get("batch_size", 10))

        c.overlap_minutes = int(cfg.get("overlap_minutes", 60))
        c.safety_lag_sec = int(cfg.get("safety_lag_sec", 15))

        c.refresh_symbols_sec = int(cfg.get("refresh_symbols_sec", 3600))
        c.log_each_cycle = bool(cfg.get("log_each_cycle", True))

        c.stall_hits_to_blacklist = int(cfg.get("stall_hits_to_blacklist", 3))
        c.blacklist_hours = int(cfg.get("blacklist_hours", 6))
        c.blacklist_log_every_sec = int(cfg.get("blacklist_log_every_sec", 120))

        raw_schedule = cfg.get("schedule") or {}
        c.schedule = {str(k): int(v) for k, v in raw_schedule.items()}
        c.schedule_jitter_sec = float(cfg.get("schedule_jitter_sec", 1.0))

        db_cfg = cfg.get("dynamic_budget") or {}
        c.dynamic_budget = OIDynamicBudget(
            enabled=bool(db_cfg.get("enabled", False)),
            safety_factor=float(db_cfg.get("safety_factor", 0.9)),
            endpoint_weight=float(db_cfg.get("endpoint_weight", 2.0)),
            min_symbols_per_tick=int(db_cfg.get("min_symbols_per_tick", 5)),
            max_symbols_per_tick_cap=int(db_cfg.get("max_symbols_per_tick_cap", 80)),
        )

        ad_cfg = cfg.get("adaptive") or {}
        c.adaptive = OIAdaptive(
            enabled=bool(ad_cfg.get("enabled", False)),
            penalty_step=int(ad_cfg.get("penalty_step", 5)),
            recovery_step=int(ad_cfg.get("recovery_step", 1)),
            extra_sleep_penalty_sec=float(ad_cfg.get("extra_sleep_penalty_sec", 0.05)),
            extra_sleep_recovery_sec=float(ad_cfg.get("extra_sleep_recovery_sec", 0.01)),
            extra_sleep_max_sec=float(ad_cfg.get("extra_sleep_max_sec", 0.5)),
            post_tick_sleep_max_sec=float(ad_cfg.get("post_tick_sleep_max_sec", 15.0)),
        )

        # ✅ ensure schedule defaults for provided intervals
        if not c.schedule:
            c.schedule = {iv: int(c.poll_sec) for iv in c.intervals}
        else:
            for iv in c.intervals:
                if iv not in c.schedule:
                    c.schedule[iv] = int(c.poll_sec)

        return c


# -------------------------
# collector
# -------------------------

class BinanceOpenInterestCollector:
    """
    Open Interest collector (USDⓈ-M Futures).

    Storage methods expected:
      - upsert_open_interest(rows: list[dict]) -> int
      - get_open_interest_watermarks_bulk(exchange_id:int, intervals:list[str]) -> dict[interval]->dict[symbol_id]->datetime
        (fallback: get_open_interest_watermarks(exchange_id, interval) -> dict[symbol_id]->datetime)

    IMPORTANT:
      Чтобы не было [OI] no symbol name for symbol_id=...,
      передавай symbol_id_to_symbol из run_market_data.
    """

    def __init__(
        self,
        *,
        exchange_id: int,
        symbol_ids: Sequence[int],
        storage: Any,
        rest: Any,
        cfg: OIConfig,
        stop_event: threading.Event,
        logger: logging.Logger | None = None,
        symbol_id_to_symbol: Optional[Dict[int, str]] = None,
        watermarks: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.exchange_id = int(exchange_id)
        self.storage = storage
        self.rest = rest
        self.cfg = cfg
        self.stop_event = stop_event
        self.logger = logger or log

        self._symbol_ids: List[int] = list(map(int, symbol_ids))
        self._sym_refresh_deadline = time.time() + self.cfg.refresh_symbols_sec

        # id -> symbol
        self._symbol_id_to_symbol: Dict[int, str] = {}
        if symbol_id_to_symbol:
            for sid, sym in symbol_id_to_symbol.items():
                try:
                    self._symbol_id_to_symbol[int(sid)] = str(sym).upper()
                except Exception:
                    continue

        self._wm: Dict[str, Dict[int, datetime]] = {iv: {} for iv in self.cfg.intervals}
        self._black_until: Dict[str, Dict[int, float]] = {iv: {} for iv in self.cfg.intervals}
        self._stall_hits: Dict[str, Dict[int, int]] = {iv: {} for iv in self.cfg.intervals}
        self._last_blacklist_log: float = 0.0

        self._next_tick: Dict[str, float] = {}
        self._rr_cursor: Dict[str, int] = {iv: 0 for iv in self.cfg.intervals}

        self._extra_sleep_sec: float = 0.0
        self._budget_penalty: Dict[str, int] = {iv: 0 for iv in self.cfg.intervals}

        self._apply_external_watermarks_if_any(watermarks)

    # ---------------------------
    # watermarks
    # ---------------------------

    def _apply_external_watermarks_if_any(self, watermarks: Optional[Dict[str, Any]]) -> None:
        if not watermarks:
            return

        try:
            for iv, mp in watermarks.items():
                if iv not in self._wm:
                    continue
                if not isinstance(mp, dict):
                    continue

                out: Dict[int, datetime] = {}
                for sid, v in mp.items():
                    try:
                        sid_i = int(sid)
                    except Exception:
                        continue
                    dt = _normalize_wm_value(v)
                    if dt is not None:
                        out[sid_i] = dt

                if out:
                    self._wm[iv].update(out)
        except Exception:
            self.logger.exception("[OI] failed to apply external watermarks")

    def _load_watermarks(self) -> None:
        """
        Берём watermarks (MAX(ts)) из БД. Это главная защита от дублей и рестартов.
        """
        try:
            if hasattr(self.storage, "get_open_interest_watermarks_bulk"):
                bulk = self.storage.get_open_interest_watermarks_bulk(self.exchange_id, list(self.cfg.intervals)) or {}
                for iv in self.cfg.intervals:
                    mp = bulk.get(iv) or {}
                    self._wm[iv] = {int(k): v for k, v in mp.items() if v is not None}
                self.logger.info(
                    "[OI] loaded watermarks BULK intervals=%s total=%d",
                    ",".join(self.cfg.intervals),
                    sum(len(self._wm[iv]) for iv in self.cfg.intervals),
                )
                return
        except Exception:
            self.logger.exception("[OI] failed to load watermarks via BULK, fallback")

        for iv in self.cfg.intervals:
            try:
                if hasattr(self.storage, "get_open_interest_watermarks"):
                    wm = self.storage.get_open_interest_watermarks(self.exchange_id, iv) or {}
                    self._wm[iv] = {int(k): v for k, v in wm.items() if v is not None}
                else:
                    self._wm[iv] = {}
                self.logger.info("[OI] loaded watermarks interval=%s count=%d", iv, len(self._wm[iv]))
            except Exception:
                self.logger.exception("[OI] failed to load watermarks interval=%s", iv)
                self._wm[iv] = {}

    # ---------------------------
    # DB helpers
    # ---------------------------

    def _upsert_rows(self, rows: List[Dict[str, Any]]) -> int:
        if not rows:
            return 0
        try:
            return int(self.storage.upsert_open_interest(rows))
        except Exception:
            self.logger.exception("[OI] upsert_open_interest failed rows=%d", len(rows))
            return 0

    # ---------------------------
    # REST helpers
    # ---------------------------

    def _fetch_hist(
        self,
        *,
        symbol: str,
        interval: str,
        limit: int,
        end_time_ms: Optional[int] = None,
        start_time_ms: Optional[int] = None,
    ) -> List[dict]:
        if hasattr(self.rest, "get_open_interest_hist"):
            return self.rest.get_open_interest_hist(
                symbol=symbol,
                period=interval,
                limit=int(limit),
                endTime=end_time_ms,
                startTime=start_time_ms,
            ) or []

        if hasattr(self.rest, "open_interest_hist"):
            return self.rest.open_interest_hist(
                symbol=symbol,
                period=interval,
                limit=int(limit),
                endTime=end_time_ms,
                startTime=start_time_ms,
            ) or []

        if hasattr(self.rest, "open_interest") and "period" in getattr(self.rest.open_interest, "__code__", ()).co_varnames:
            return self.rest.open_interest(
                symbol=symbol,
                period=interval,
                limit=int(limit),
                endTime=end_time_ms,
                startTime=start_time_ms,
            ) or []

        raise RuntimeError("REST has no open interest history method")

    def _symbol_name(self, symbol_id: int) -> Optional[str]:
        sym = self._symbol_id_to_symbol.get(int(symbol_id))
        if sym:
            return sym

        try:
            if hasattr(self.storage, "list_active_symbols_map"):
                smap = self.storage.list_active_symbols_map(self.exchange_id) or {}

                sym2 = None
                if smap:
                    any_val = next(iter(smap.values()))
                    if isinstance(any_val, int):
                        # {symbol: id}
                        for k, v in smap.items():
                            if int(v) == int(symbol_id):
                                sym2 = str(k)
                                break
                    else:
                        # {id: symbol}
                        sym2 = smap.get(int(symbol_id))

                if sym2:
                    sym2 = str(sym2).upper()
                    self._symbol_id_to_symbol[int(symbol_id)] = sym2
                    return sym2

            if hasattr(self.storage, "symbol_name"):
                sym2 = self.storage.symbol_name(symbol_id)
                if sym2:
                    sym2 = str(sym2).upper()
                    self._symbol_id_to_symbol[int(symbol_id)] = sym2
                    return sym2
        except Exception:
            pass

        return None

    # ---------------------------
    # blacklist / stall
    # ---------------------------

    def _is_blacklisted(self, symbol_id: int, interval: str) -> bool:
        until = self._black_until.get(interval, {}).get(int(symbol_id))
        if until is None:
            return False
        if time.time() >= float(until):
            try:
                del self._black_until[interval][int(symbol_id)]
            except Exception:
                pass
            return False
        return True

    def _note_success(self, symbol_id: int, interval: str) -> None:
        self._stall_hits.setdefault(interval, {}).pop(int(symbol_id), None)
        if self.cfg.adaptive.enabled:
            self._extra_sleep_sec = max(0.0, self._extra_sleep_sec - self.cfg.adaptive.extra_sleep_recovery_sec)
            self._budget_penalty[interval] = max(0, int(self._budget_penalty.get(interval, 0)) - self.cfg.adaptive.recovery_step)

    def _note_rate_limit(self, symbol_id: int, interval: str, reason: str) -> None:
        if self.cfg.adaptive.enabled:
            self._extra_sleep_sec = min(
                float(self.cfg.adaptive.extra_sleep_max_sec),
                float(self._extra_sleep_sec) + float(self.cfg.adaptive.extra_sleep_penalty_sec),
            )
            self._budget_penalty[interval] = int(self._budget_penalty.get(interval, 0)) + int(self.cfg.adaptive.penalty_step)

        now = time.time()
        self._black_until.setdefault(interval, {})[int(symbol_id)] = now + 30.0

        self.logger.warning(
            "[OI] rate-limit interval=%s sym_id=%d reason=%s extra_sleep=%.3f budget_penalty=%d",
            interval,
            int(symbol_id),
            reason,
            self._extra_sleep_sec,
            self._budget_penalty.get(interval, 0),
        )

    def _note_stall(self, *, symbol: str, symbol_id: int, interval: str, best_ms: int, last_ms: int) -> None:
        hits = int(self._stall_hits.setdefault(interval, {}).get(int(symbol_id), 0)) + 1
        self._stall_hits[interval][int(symbol_id)] = hits

        if hits >= int(self.cfg.stall_hits_to_blacklist):
            until = time.time() + float(self.cfg.blacklist_hours) * 3600.0
            self._black_until.setdefault(interval, {})[int(symbol_id)] = until
            self._stall_hits[interval].pop(int(symbol_id), None)

            if time.time() - self._last_blacklist_log > float(self.cfg.blacklist_log_every_sec):
                self._last_blacklist_log = time.time()
                self.logger.warning(
                    "[OI] BLACKLIST interval=%s symbol=%s (%d) hits=%d last=%s best=%s for %.1fh",
                    interval,
                    symbol,
                    int(symbol_id),
                    hits,
                    _dt_from_ms(int(last_ms)).isoformat(),
                    _dt_from_ms(int(best_ms)).isoformat(),
                    float(self.cfg.blacklist_hours),
                )

    # ---------------------------
    # seed
    # ---------------------------

    def _seed_symbol_interval(self, *, symbol_id: int, interval: str) -> Tuple[int, int]:
        sym = self._symbol_name(symbol_id)
        if not sym:
            return 0, 0

        end_dt = _floor_ts(_utcnow() - timedelta(seconds=self.cfg.safety_lag_sec), interval)
        end_ms = _ms(end_dt)

        start_dt = end_dt - timedelta(days=int(self.cfg.seed_days))
        start_ms = _ms(start_dt)

        current_end_ms = end_ms
        pages = 0
        upserts_total = 0

        while not self.stop_event.is_set():
            if pages >= int(self.cfg.seed_max_pages_per_symbol):
                break

            try:
                data = self._fetch_hist(
                    symbol=sym,
                    interval=interval,
                    limit=int(self.cfg.seed_limit),
                    end_time_ms=current_end_ms,
                )
            except Exception as e:
                self.logger.exception("[OISeed] REST error sym=%s interval=%s", sym, interval)
                if "429" in str(e) or "-1003" in str(e):
                    self._note_rate_limit(symbol_id, interval, reason=str(e)[:120])
                break

            pages += 1
            if not data:
                break

            items: List[Dict[str, Any]] = []
            for it in data:
                ts_ms = int(it.get("timestamp") or it.get("time") or 0)
                if not ts_ms:
                    continue
                if ts_ms < start_ms or ts_ms > end_ms:
                    continue

                items.append(
                    {
                        "exchange_id": self.exchange_id,
                        "symbol_id": int(symbol_id),
                        "interval": str(interval),
                        "ts": _dt_from_ms(ts_ms),
                        "open_interest": _safe_float(it.get("sumOpenInterest") or it.get("openInterest")),
                        "open_interest_value": _safe_float(it.get("sumOpenInterestValue") or it.get("openInterestValue")),
                        "source": "rest_seed",
                    }
                )

            if items:
                items.sort(key=lambda x: x["ts"])
                up = self._upsert_rows(items)
                upserts_total += up

                best_ts = items[-1]["ts"]
                prev = self._wm.setdefault(interval, {}).get(symbol_id)
                if (prev is None) or (best_ts > prev):
                    self._wm[interval][symbol_id] = best_ts

                self._note_success(symbol_id, interval)

            all_ts_ms = [int(it.get("timestamp") or it.get("time") or 0) for it in data if (it.get("timestamp") or it.get("time"))]
            if not all_ts_ms:
                break

            oldest_ms = min(all_ts_ms)
            if oldest_ms >= current_end_ms:
                self._note_stall(symbol=sym, symbol_id=symbol_id, interval=interval, best_ms=oldest_ms, last_ms=current_end_ms)
                break

            current_end_ms = oldest_ms - 1
            time.sleep(max(0.0, self.cfg.per_request_sleep_sec + self._extra_sleep_sec))

        return upserts_total, pages

    def _seed_interval(self, interval: str) -> None:
        symbols = list(self._symbol_ids)
        n = max(1, int(self.cfg.seed_symbols_per_cycle))
        for batch in _chunks(symbols, n):
            if self.stop_event.is_set():
                break
            for sid in batch:
                if self.stop_event.is_set():
                    break
                up, pages = self._seed_symbol_interval(symbol_id=sid, interval=interval)
                if up > 0:
                    self.logger.info("[OISeed] interval=%s sym_id=%d upserts=%d pages=%d", interval, sid, up, pages)
                time.sleep(max(0.0, self.cfg.per_request_sleep_sec + self._extra_sleep_sec))

    # ---------------------------
    # poll
    # ---------------------------

    def _poll_symbol_interval(self, *, symbol_id: int, interval: str) -> int:
        if self._is_blacklisted(symbol_id, interval):
            return 0

        sym = self._symbol_name(symbol_id)
        if not sym:
            return 0

        now = _utcnow()
        end_dt = _floor_ts(now - timedelta(seconds=self.cfg.safety_lag_sec), interval)
        end_ms = _ms(end_dt)

        try:
            data = self._fetch_hist(symbol=sym, interval=interval, limit=2, end_time_ms=end_ms)
        except Exception as e:
            s = str(e)
            if "429" in s or "-1003" in s:
                self._note_rate_limit(symbol_id, interval, reason=s[:120])
            else:
                self.logger.exception("[OI] REST poll error sym=%s interval=%s", sym, interval)
            return 0

        if not data:
            return 0

        rows: List[Dict[str, Any]] = []
        for it in data:
            ts_ms = int(it.get("timestamp") or it.get("time") or 0)
            if not ts_ms:
                continue
            ts = _dt_from_ms(ts_ms)
            if ts > end_dt:
                continue

            rows.append(
                {
                    "exchange_id": self.exchange_id,
                    "symbol_id": int(symbol_id),
                    "interval": str(interval),
                    "ts": ts,
                    "open_interest": _safe_float(it.get("sumOpenInterest") or it.get("openInterest")),
                    "open_interest_value": _safe_float(it.get("sumOpenInterestValue") or it.get("openInterestValue")),
                    "source": "rest_poll",
                }
            )

        if not rows:
            return 0

        rows.sort(key=lambda x: x["ts"])

        prev = self._wm.setdefault(interval, {}).get(symbol_id)
        if prev is not None:
            try:
                rows = [r for r in rows if r["ts"] > prev]
            except Exception:
                self._wm[interval].pop(symbol_id, None)

        if not rows:
            return 0

        up = self._upsert_rows(rows)
        if up > 0:
            best = rows[-1]["ts"]
            prev2 = self._wm[interval].get(symbol_id)
            if (prev2 is None) or (best > prev2):
                self._wm[interval][symbol_id] = best
            self._note_success(symbol_id, interval)

        return up

    # ---------------------------
    # scheduling / budgeting
    # ---------------------------

    def _schedule_sec(self, interval: str) -> float:
        try:
            return float(self.cfg.schedule.get(interval, self.cfg.poll_sec))
        except Exception:
            return float(self.cfg.poll_sec)

    def _pick_symbols_for_tick(self, interval: str) -> List[int]:
        symbols = list(self._symbol_ids)
        if not symbols:
            return []

        target = len(symbols)

        if self.cfg.dynamic_budget.enabled:
            sched = max(1.0, self._schedule_sec(interval))
            approx = int(
                (sched / max(0.02, self.cfg.per_request_sleep_sec))
                * self.cfg.dynamic_budget.safety_factor
                / max(0.1, self.cfg.dynamic_budget.endpoint_weight)
            )
            target = min(target, max(self.cfg.dynamic_budget.min_symbols_per_tick, approx))
            target = min(target, int(self.cfg.dynamic_budget.max_symbols_per_tick_cap))

        if self.cfg.adaptive.enabled:
            target = max(1, int(target) - int(self._budget_penalty.get(interval, 0)))

        if target >= len(symbols):
            return symbols

        cur = int(self._rr_cursor.get(interval, 0)) % len(symbols)
        out: List[int] = []
        for _ in range(target):
            out.append(symbols[cur])
            cur = (cur + 1) % len(symbols)
        self._rr_cursor[interval] = cur
        return out

    def _tick_interval(self, interval: str) -> int:
        selected = self._pick_symbols_for_tick(interval)
        if not selected:
            return 0

        upserts = 0

        for batch in _chunks(selected, self.cfg.batch_size):
            if self.stop_event.is_set():
                break
            for sid in batch:
                if self.stop_event.is_set():
                    break
                upserts += self._poll_symbol_interval(symbol_id=sid, interval=interval)
                time.sleep(max(0.0, self.cfg.per_request_sleep_sec + self._extra_sleep_sec))

        if self.cfg.adaptive.enabled and self._extra_sleep_sec > 0:
            time.sleep(min(float(self.cfg.adaptive.post_tick_sleep_max_sec), float(self._extra_sleep_sec)))

        return upserts

    # ---------------------------
    # symbol refresh
    # ---------------------------

    def _refresh_symbols_if_needed(self) -> None:
        if time.time() < self._sym_refresh_deadline:
            return
        self._sym_refresh_deadline = time.time() + self.cfg.refresh_symbols_sec

        try:
            if hasattr(self.storage, "list_active_symbol_ids"):
                new_ids = list(map(int, self.storage.list_active_symbol_ids(self.exchange_id)))
                if new_ids:
                    self._symbol_ids = new_ids

            if hasattr(self.storage, "list_active_symbols_map"):
                smap = self.storage.list_active_symbols_map(self.exchange_id) or {}
                for sym, sid in smap.items():
                    try:
                        self._symbol_id_to_symbol[int(sid)] = str(sym).upper()
                    except Exception:
                        pass

            self.logger.info("[OI] symbols refreshed ids=%d map=%d", len(self._symbol_ids), len(self._symbol_id_to_symbol))
        except Exception:
            self.logger.exception("[OI] failed to refresh symbols")

    # ---------------------------
    # main
    # ---------------------------

    def run(self) -> None:
        self.logger.info(
            "[OI] started intervals=%s symbols=%d seed=%s schedule=%s batch=%d sleep=%.2f lag=%ds dynamic_budget=%s adaptive=%s",
            ",".join(self.cfg.intervals),
            len(self._symbol_ids),
            self.cfg.seed_on_start,
            ",".join([f"{k}={self._schedule_sec(k):.0f}s" for k in self.cfg.intervals]),
            self.cfg.batch_size,
            self.cfg.per_request_sleep_sec,
            self.cfg.safety_lag_sec,
            self.cfg.dynamic_budget.enabled,
            self.cfg.adaptive.enabled,
        )

        self._load_watermarks()

        if self.cfg.seed_on_start:
            for interval in self.cfg.intervals:
                if self.stop_event.is_set():
                    break

                self.logger.info(
                    "[OISeed] start interval=%s seed_days=%d symbols=%d limit=%d max_pages=%d sym_per_cycle=%d",
                    interval,
                    self.cfg.seed_days,
                    len(self._symbol_ids),
                    self.cfg.seed_limit,
                    self.cfg.seed_max_pages_per_symbol,
                    self.cfg.seed_symbols_per_cycle,
                )
                self._seed_interval(interval)

        now = time.time()
        for iv in self.cfg.intervals:
            self._next_tick[iv] = now + _jitter(self.cfg.schedule_jitter_sec)

        while not self.stop_event.is_set():
            self._refresh_symbols_if_needed()

            t = time.time()
            did_any = False
            cycle_up = 0

            for interval in self.cfg.intervals:
                if self.stop_event.is_set():
                    break

                due = t >= float(self._next_tick.get(interval, 0.0))
                if not due:
                    continue

                did_any = True
                up = self._tick_interval(interval)
                cycle_up += up

                self._next_tick[interval] = time.time() + self._schedule_sec(interval) + _jitter(self.cfg.schedule_jitter_sec)

            if self.cfg.log_each_cycle and did_any:
                self.logger.info(
                    "[OI] tick ok upserts=%d symbols=%d intervals=%s extra_sleep=%.3f",
                    cycle_up,
                    len(self._symbol_ids),
                    ",".join(self.cfg.intervals),
                    self._extra_sleep_sec,
                )

            if not did_any:
                time.sleep(0.20)

        self.logger.info("[OI] stopped")


# -------------------------
# public starter
# -------------------------

def start_open_interest_collector(
    *,
    cfg: dict,
    exchange_id: int,
    symbol_ids: Sequence[int],
    storage: Any,
    rest: Any,
    stop_event: threading.Event,
    symbol_id_to_symbol: Optional[Dict[int, str]] = None,
    watermarks: Optional[Dict[str, Any]] = None,
) -> threading.Thread:
    """
    Run_market_data.py может передавать watermarks=...
    Аргумент принимается и НЕ ломает запуск.
    """
    oi_cfg = OIConfig.from_cfg(cfg or {})
    if not oi_cfg.enabled:
        t = threading.Thread(target=lambda: None, name="BinanceOpenInterestCollector(disabled)", daemon=True)
        t.start()
        return t

    collector = BinanceOpenInterestCollector(
        exchange_id=exchange_id,
        symbol_ids=symbol_ids,
        storage=storage,
        rest=rest,
        cfg=oi_cfg,
        stop_event=stop_event,
        symbol_id_to_symbol=symbol_id_to_symbol,
        watermarks=watermarks,
    )
    th = threading.Thread(target=collector.run, name="BinanceOpenInterestCollector", daemon=True)
    th.start()
    return th
