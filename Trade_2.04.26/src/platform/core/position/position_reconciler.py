# src/platform/core/position/position_reconciler.py
from __future__ import annotations

import time
import logging
from typing import Any, List, Dict, Tuple, Set
from collections import defaultdict

from src.platform.core.position.aggregate import PositionAggregate
from src.platform.core.models.enums import Side


class PositionReconciler:
    """
    K6.5 Position Reconciler (TRADE-SAFE)

    Principles:
      • OMS/WS is primary
      • REST is corrective only
      • Never heal immediately after own trades
      • CLOSE → FLAT is NORMAL

    Also:
      • Hard throttle REST polling
      • Backoff on 429 / -1003
    """

    # --- timings ---
    DESYNC_HOLD_SEC = 12.0          # after trade event, do not heal
    HEAL_COOLDOWN_SEC = 10.0        # do not heal same symbol too often

    # reconcile logic
    DESYNC_CNT_QTY = 3
    MISSING_IN_REST_THRESHOLD = 3

    # eps
    EPS_QTY_ABS = 1e-8
    EPS_QTY_REL = 0.01
    EPS_PRICE = 1e-10

    # --- REST throttling/backoff ---
    MIN_REST_POLL_SEC = 30.0        # IMPORTANT: do not call REST more often than this
    BACKOFF_INIT_SEC = 30.0
    BACKOFF_MAX_SEC = 300.0

    def __init__(
        self,
        *,
        exchange: Any,
        position_manager: Any,
        storage: Any,
        exchange_id: int,
        account_id: int,
        account: str,
        symbol_ids: dict[str, int],
        logger: logging.Logger | None = None,
    ) -> None:
        self.exchange = exchange
        self.storage = storage

        self.exchange_id = int(exchange_id)
        self.account_id = int(account_id)
        self.account = str(account)

        self.symbol_ids = symbol_ids
        self._sid_to_symbol = {v: k for k, v in symbol_ids.items()}

        self.pm = position_manager
        self.logger = logger or logging.getLogger("positions.reconciler")

        self._desync_cnt: Dict[Tuple[int, int, int], int] = defaultdict(int)
        self._missing_cnt: Dict[Tuple[int, int, int], int] = defaultdict(int)
        self._last_heal_ts: Dict[Tuple[int, int, int], float] = defaultdict(float)

        # updated by WS fills
        self._last_event_ts: Dict[Tuple[int, int, int], float] = defaultdict(float)

        # REST control
        self._rate_limit_until = 0.0
        self._backoff_sec = float(self.BACKOFF_INIT_SEC)
        self._last_rest_poll_ts = 0.0

    # ---------------------------------------------------------
    # public hook (call this from WS fill handler)
    # ---------------------------------------------------------
    def on_trade_event(self, *, exchange_id: int, account_id: int, symbol_id: int, ts: float | None = None) -> None:
        key = (int(exchange_id), int(account_id), int(symbol_id))
        self._last_event_ts[key] = float(ts or time.time())

    # ---------------------------------------------------------
    # helpers
    # ---------------------------------------------------------
    @staticmethod
    def _get(obj: Any, key: str, default: Any = None) -> Any:
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

    @staticmethod
    def _f(v: Any, default: float = 0.0) -> float:
        try:
            if v is None or v == "":
                return float(default)
            return float(v)
        except Exception:
            return float(default)

    @staticmethod
    def _signed_qty_from_agg(agg: PositionAggregate) -> float:
        q = float(getattr(agg, "qty", 0.0) or 0.0)
        s = getattr(agg, "side", None)
        if s == Side.LONG:
            return abs(q)
        if s == Side.SHORT:
            return -abs(q)
        return 0.0

    @staticmethod
    def _signed_qty_from_rest(side: Any, qty_abs: float) -> float:
        s = str(side or "").upper()
        if s == "SHORT":
            return -abs(qty_abs)
        if s == "LONG":
            return abs(qty_abs)
        return qty_abs

    @staticmethod
    def _is_rate_limit_error(msg: str) -> bool:
        m = (msg or "").lower()
        return (
            "http 429" in m
            or "too many requests" in m
            or "-1003" in m
            or "rate limit" in m
        )

    # ---------------------------------------------------------

    def run_once(self) -> List[PositionAggregate]:
        now = time.time()

        # --- hard throttle REST ---
        if now - self._last_rest_poll_ts < self.MIN_REST_POLL_SEC:
            return []

        # --- backoff window ---
        if now < self._rate_limit_until:
            return []

        self._last_rest_poll_ts = now

        try:
            rest_positions = self.exchange.fetch_positions(account=self.account)
            # success -> reset backoff
            self._backoff_sec = float(self.BACKOFF_INIT_SEC)
        except Exception as e:
            msg = str(e)
            if self._is_rate_limit_error(msg):
                self._rate_limit_until = time.time() + self._backoff_sec
                self.logger.warning(
                    "[RECONCILE] REST rate-limited. Backoff %.1fs (next at %.0f)",
                    self._backoff_sec,
                    self._rate_limit_until,
                )
                self._backoff_sec = min(self._backoff_sec * 2.0, float(self.BACKOFF_MAX_SEC))
            else:
                self.logger.exception("[RECONCILE] REST fetch failed")
            return []

        updated: List[PositionAggregate] = []
        seen_keys: Set[Tuple[int, int, int]] = set()
        now = time.time()

        # ==============================
        # REST PRESENT
        # ==============================
        for p in rest_positions or []:
            symbol = str(self._get(p, "symbol", "") or "").upper()
            if not symbol:
                continue

            symbol_id = int(self.symbol_ids.get(symbol, 0) or 0)
            if symbol_id <= 0:
                continue

            key = (self.exchange_id, self.account_id, symbol_id)
            seen_keys.add(key)

            qty_rest_abs = abs(self._f(self._get(p, "qty", 0.0)))
            entry_rest = self._f(self._get(p, "entry_price", 0.0))
            upnl_rest = self._f(self._get(p, "unrealized_pnl", 0.0))

            rest_side = self._get(p, "side", None)
            qty_rest_signed = self._signed_qty_from_rest(rest_side, qty_rest_abs)

            mark_raw = self._get(p, "mark_price", None)
            mark_rest = self._f(mark_raw) if mark_raw is not None else None

            agg = self.pm.get_or_create(*key)

            # refresh pnl/mark (safe)
            try:
                agg.apply_rest_snapshot(unrealized_pnl=upnl_rest, mark_price=mark_rest)
            except Exception:
                pass

            agg_signed = self._signed_qty_from_agg(agg)

            diff = abs(abs(agg_signed) - abs(qty_rest_signed))
            eps = max(self.EPS_QTY_ABS, abs(qty_rest_signed) * self.EPS_QTY_REL)
            sign_mismatch = (agg_signed > 0) != (qty_rest_signed > 0) and (abs(qty_rest_signed) > eps)

            if diff <= eps and not sign_mismatch:
                self._desync_cnt[key] = 0
                self._missing_cnt[key] = 0
                continue

            # do not heal right after fills
            if now - self._last_event_ts.get(key, 0.0) < self.DESYNC_HOLD_SEC:
                continue

            self._desync_cnt[key] += 1
            self.logger.warning(
                "[DESYNC] %s rest=%.8f agg=%.8f sign_mismatch=%s cnt=%d/%d",
                symbol,
                qty_rest_signed,
                agg_signed,
                sign_mismatch,
                self._desync_cnt[key],
                self.DESYNC_CNT_QTY,
            )

            if self._desync_cnt[key] < self.DESYNC_CNT_QTY:
                continue

            if now - self._last_heal_ts.get(key, 0.0) < self.HEAL_COOLDOWN_SEC:
                continue

            self.logger.warning("[HEAL] apply REST snapshot %s", symbol)

            try:
                ok = agg.apply_rest_snapshot(
                    qty=abs(qty_rest_signed),
                    side=("LONG" if qty_rest_signed > 0 else "SHORT") if abs(qty_rest_signed) > self.EPS_QTY_ABS else "FLAT",
                    entry_price=entry_rest if entry_rest > self.EPS_PRICE else None,
                    unrealized_pnl=upnl_rest,
                    mark_price=mark_rest,
                )
            except Exception:
                ok = False

            if ok:
                updated.append(agg)

            self._last_heal_ts[key] = now
            self._desync_cnt[key] = 0

        # ==============================
        # MISSING IN REST → FLAT
        # ==============================
        for agg in self.pm.iter_positions(include_flat=False):
            symbol_id = int(getattr(agg, "symbol_id", 0) or 0)
            if symbol_id <= 0:
                continue

            key = (self.exchange_id, self.account_id, symbol_id)
            if key in seen_keys:
                self._missing_cnt[key] = 0
                continue

            if now - self._last_event_ts.get(key, 0.0) < self.DESYNC_HOLD_SEC:
                continue

            self._missing_cnt[key] += 1
            if self._missing_cnt[key] < self.MISSING_IN_REST_THRESHOLD:
                continue

            sym = self._sid_to_symbol.get(symbol_id, "?")
            self.logger.info("[CLOSE->FLAT] %s confirmed missing in REST", sym)

            try:
                ok = agg.apply_rest_snapshot(
                    side="FLAT",
                    qty=0.0,
                    entry_price=None,
                    unrealized_pnl=0.0,
                    mark_price=None,
                )
            except Exception:
                ok = False

            if ok:
                updated.append(agg)

            self._missing_cnt[key] = 0

        return updated
