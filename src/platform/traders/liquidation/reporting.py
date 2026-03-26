from __future__ import annotations

import hashlib
import logging
import time

from typing import Any, Iterable, List, Sequence, TypeVar

log = logging.getLogger("traders.trade_liquidation")
T = TypeVar("T")

class TradeLiquidationReportingMixin:
    def _dlog(self, msg: str, *args: Any) -> None:
        if getattr(self.p, "debug", False):
            if args:
                log.info("[TL][debug] " + msg, *args)
            else:
                log.info("[TL][debug] " + msg)

    def _ilog(self, msg: str, *args: Any) -> None:
        try:
            if args:
                log.info(msg, *args)
            else:
                log.info(msg)
        except Exception:
            pass

    def _log_rate_limit_sec(self, key: str, default: float = 0.0) -> float:
        try:
            mp = getattr(self.p, 'log_rate_limit_sec', None)
            if isinstance(mp, dict):
                v = mp.get(key, default)
            else:
                v = default
            return max(0.0, float(v or 0.0))
        except Exception:
            return float(default or 0.0)

    def _rate_limited_log(self, level: int, throttle_key: str, cache_key: str, msg: str, *args: Any) -> bool:
        try:
            cache = getattr(self, '_rl_log_cache', None)
            if not isinstance(cache, dict):
                cache = {}
                self._rl_log_cache = cache
            now = time.time()
            min_sec = self._log_rate_limit_sec(throttle_key, 0.0)
            prev = cache.get(cache_key)
            if isinstance(prev, (int, float)) and min_sec > 0 and (now - float(prev)) < min_sec:
                return False
            cache[cache_key] = now
            log.log(level, msg, *args)
            return True
        except Exception:
            try:
                log.log(level, msg, *args)
                return True
            except Exception:
                return False

    def _stateful_info(self, state_key: str, state: Any, msg: str, *args: Any) -> bool:
        try:
            cache = getattr(self, '_state_log_cache', None)
            if not isinstance(cache, dict):
                cache = {}
                self._state_log_cache = cache
            prev = cache.get(state_key, object())
            if prev == state:
                return False
            cache[state_key] = state
            log.info(msg, *args)
            return True
        except Exception:
            try:
                log.info(msg, *args)
                return True
            except Exception:
                return False

    def _cycle_subset(self, items: Sequence[T], bucket: str, limit: int) -> List[T]:
        try:
            seq = list(items or [])
        except Exception:
            return []
        n = len(seq)
        try:
            lim = int(limit or 0)
        except Exception:
            lim = 0
        if lim <= 0 or n <= lim:
            return seq
        try:
            offsets = getattr(self, '_cycle_offsets', None)
            if not isinstance(offsets, dict):
                offsets = {}
                self._cycle_offsets = offsets
            off = int(offsets.get(bucket, 0) or 0) % n
            out = [seq[(off + i) % n] for i in range(min(lim, n))]
            offsets[bucket] = (off + lim) % n
            return out
        except Exception:
            return seq[:lim]
