from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from src.platform.core.oms.events import TradeEvent
from src.platform.core.position.aggregate import PositionAggregate


@dataclass
class LifecycleState:
    """
    Lifecycle state per (exchange_id, account_id, symbol_id).

    We store this OUTSIDE PositionAggregate because PositionAggregate is slots=True
    and cannot accept dynamic attrs like opened_at/closed_at/pos_uid.
    """
    opened_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None
    pos_uid: str = ""
    last_side: str = "FLAT"  # LONG/SHORT/FLAT

    # baselines to compute per-lifecycle DELTA for ledger
    realized_at_open: float = 0.0
    fees_at_open: float = 0.0


class PositionManager:
    """
    In-memory position aggregates built strictly from TradeEvent stream (authoritative).
    K6.7: qty>=0 + side (LONG/SHORT/FLAT)

    Key points:
      - WS fills (TradeEvent) are authoritative for side/qty/entry/realized/fees.
      - mark price ticks update only mark/uPnL.
      - We keep lifecycle (opened_at/closed_at/pos_uid) in a separate cache (LifecycleState)
        because PositionAggregate is dataclass(slots=True).
    """

    MAX_SEEN_TRADE_IDS = 200_000

    def __init__(
        self,
        *,
        storage: Any,
        exchange_id: int,
        account_id: int,
        logger: logging.Logger | None = None,
    ) -> None:
        self.storage = storage
        self.exchange_id = int(exchange_id)
        self.account_id = int(account_id)
        self.logger = logger or logging.getLogger(__name__)

        # (exchange_id, account_id, symbol_id) -> PositionAggregate
        self._aggs: Dict[Tuple[int, int, int], PositionAggregate] = {}

        # de-dup executed trades
        self._seen_trade_ids: set[str] = set()

        # lifecycle cache
        self._life: Dict[Tuple[int, int, int], LifecycleState] = {}

        # reconciler protection (optional)
        self._last_event_ts: Dict[Tuple[int, int, int], float] = {}

    # ------------------------------------------------------------
    @staticmethod
    def _key(exchange_id: int, account_id: int, symbol_id: int) -> tuple[int, int, int]:
        return int(exchange_id), int(account_id), int(symbol_id)

    @staticmethod
    def _dt_from_ts_ms(ts_ms: int) -> datetime:
        return datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=timezone.utc)

    @staticmethod
    def _pos_uid(exchange_id: int, account_id: int, symbol_id: int, opened_at: datetime) -> str:
        opened_ms = int(opened_at.timestamp() * 1000)
        return f"{exchange_id}:{account_id}:{symbol_id}:{opened_ms}"

    def get(self, exchange_id: int, account_id: int, symbol_id: int) -> Optional[PositionAggregate]:
        return self._aggs.get(self._key(exchange_id, account_id, symbol_id))

    def get_or_create(self, exchange_id: int, account_id: int, symbol_id: int) -> PositionAggregate:
        key = self._key(exchange_id, account_id, symbol_id)
        agg = self._aggs.get(key)
        if agg is None:
            agg = PositionAggregate.empty(exchange_id=exchange_id, account_id=account_id, symbol_id=symbol_id)
            self._aggs[key] = agg
        return agg

    # ------------------------------------------------------------
    def _dedup_trade_id(self, trade_id: str) -> bool:
        if not trade_id:
            return True
        if trade_id in self._seen_trade_ids:
            return False

        self._seen_trade_ids.add(trade_id)

        if len(self._seen_trade_ids) > self.MAX_SEEN_TRADE_IDS:
            self.logger.warning(
                "[PositionManager] seen_trade_ids overflow (%d) -> reset cache",
                len(self._seen_trade_ids),
            )
            self._seen_trade_ids.clear()
            self._seen_trade_ids.add(trade_id)

        return True

    def reset_trade_id_cache(self) -> None:
        self._seen_trade_ids.clear()

    # ------------------------------------------------------------
    def get_lifecycle(self, exchange_id: int, account_id: int, symbol_id: int) -> Optional[LifecycleState]:
        return self._life.get(self._key(exchange_id, account_id, symbol_id))

    def _life_get_or_create(self, key: Tuple[int, int, int]) -> LifecycleState:
        st = self._life.get(key)
        if st is None:
            st = LifecycleState()
            self._life[key] = st
        return st

    # ------------------------------------------------------------
    def on_trade_event(self, evt: TradeEvent) -> Optional[PositionAggregate]:
        """
        Authoritative update from WS fills (TradeEvent).
        Also updates lifecycle cache (opened_at/closed_at/pos_uid and baselines).
        """
        if evt is None:
            return None

        symbol_id = int(getattr(evt, "symbol_id", 0) or 0)
        if symbol_id <= 0:
            return None

        ex_id = int(getattr(evt, "exchange_id", 0) or self.exchange_id or 0)
        acc_id = int(getattr(evt, "account_id", 0) or self.account_id or 0)
        if ex_id <= 0 or acc_id <= 0:
            return None

        trade_id = str(getattr(evt, "trade_id", "") or "").strip()
        if not trade_id:
            # без trade_id высок риск дублей
            return None

        if not self._dedup_trade_id(trade_id):
            return None

        key = self._key(ex_id, acc_id, symbol_id)

        agg = self.get_or_create(ex_id, acc_id, symbol_id)

        # snapshot BEFORE apply_trade (для переходов open<->flat)
        was_open = bool(getattr(agg, "is_open")() if hasattr(agg, "is_open") else False)
        was_side = str(getattr(agg, "side_str")() if hasattr(agg, "side_str") else "FLAT").upper()

        ok = agg.apply_trade(evt)
        if not ok:
            return None

        # snapshot AFTER apply_trade
        is_open = bool(getattr(agg, "is_open")() if hasattr(agg, "is_open") else False)
        now_side = str(getattr(agg, "side_str")() if hasattr(agg, "side_str") else "FLAT").upper()

        ts_ms = int(getattr(evt, "ts_ms", 0) or 0) or int(getattr(agg, "last_ts_ms", 0) or 0)
        if ts_ms <= 0:
            ts_ms = int(time.time() * 1000)
        ts_dt = self._dt_from_ts_ms(ts_ms)

        st = self._life_get_or_create(key)

        # ---- transition: FLAT -> OPEN ----
        if (not was_open) and is_open:
            st.opened_at = ts_dt
            st.closed_at = None
            st.pos_uid = self._pos_uid(ex_id, acc_id, symbol_id, st.opened_at)
            st.last_side = now_side if now_side in ("LONG", "SHORT") else "FLAT"

            # baselines at open (after apply_trade)
            st.realized_at_open = float(getattr(agg, "realized_pnl", 0.0) or 0.0)
            st.fees_at_open = float(getattr(agg, "fees", 0.0) or 0.0)

        # ---- transition: OPEN -> FLAT ----
        if was_open and (not is_open):
            # закрываем lifecycle
            if st.opened_at is None:
                # если вдруг пропустили открытие (редко), хотя бы не оставляем NULL
                st.opened_at = ts_dt
                st.pos_uid = self._pos_uid(ex_id, acc_id, symbol_id, st.opened_at)
                st.realized_at_open = float(getattr(agg, "realized_pnl", 0.0) or 0.0)
                st.fees_at_open = float(getattr(agg, "fees", 0.0) or 0.0)

            st.closed_at = ts_dt
            st.last_side = was_side if was_side in ("LONG", "SHORT") else st.last_side

        # ---- still open ----
        if is_open and now_side in ("LONG", "SHORT"):
            st.last_side = now_side

        # reconciler protection timestamp
        self._last_event_ts[key] = time.time()

        return agg

    # ------------------------------------------------------------
    def on_mark_price(
        self,
        *,
        symbol_id: int,
        price: float,
        create_if_missing: bool = False,
    ) -> Optional[PositionAggregate]:
        """
        Mark price updates (WS markPrice@1s).
        """
        symbol_id = int(symbol_id or 0)
        price = float(price or 0.0)
        if symbol_id <= 0 or price <= 0:
            return None

        key = (self.exchange_id, self.account_id, symbol_id)
        agg = self._aggs.get(key)

        if agg is None:
            if not create_if_missing:
                return None
            agg = self.get_or_create(self.exchange_id, self.account_id, symbol_id)

        if agg.apply_mark_price(price):
            return agg
        return None

    # ------------------------------------------------------------
    def snapshot(self, *, include_flat: bool = True) -> List[PositionAggregate]:
        aggs = list(self._aggs.values())
        if include_flat:
            return aggs
        return [a for a in aggs if a.is_open()]

    def all_positions(self) -> List[PositionAggregate]:
        return self.snapshot(include_flat=True)

    def iter_dirty_positions(self) -> Iterable[PositionAggregate]:
        for agg in self._aggs.values():
            if agg.is_dirty():
                yield agg

    def iter_positions(self, *, include_flat: bool = True) -> Iterable[PositionAggregate]:
        for agg in self._aggs.values():
            if include_flat or agg.is_open():
                yield agg
