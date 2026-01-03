# src/platform/core/position/position_manager.py
from __future__ import annotations

import logging
from typing import Dict, Tuple, Optional, List, Any, Iterable

from src.platform.core.oms.events import TradeEvent
from src.platform.core.position.aggregate import PositionAggregate


class PositionManager:
    """
    In-memory position aggregates built strictly from TradeEvent stream (authoritative).
    K6.7: qty>=0 + side (LONG/SHORT/FLAT)
    """

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

        self._aggs: Dict[Tuple[int, int, int], PositionAggregate] = {}
        self._seen_trade_ids: set[str] = set()

    # ------------------------------------------------------------
    @staticmethod
    def _key(exchange_id: int, account_id: int, symbol_id: int) -> tuple[int, int, int]:
        return int(exchange_id), int(account_id), int(symbol_id)

    def get_or_create(self, exchange_id: int, account_id: int, symbol_id: int) -> PositionAggregate:
        key = self._key(exchange_id, account_id, symbol_id)
        agg = self._aggs.get(key)
        if agg is None:
            agg = PositionAggregate.empty(
                exchange_id=exchange_id,
                account_id=account_id,
                symbol_id=symbol_id,
            )
            self._aggs[key] = agg
        return agg

    # ------------------------------------------------------------
    def on_trade_event(self, evt: TradeEvent) -> Optional[PositionAggregate]:
        if not evt:
            return None

        trade_id = str(getattr(evt, "trade_id", "") or "")
        if trade_id:
            if trade_id in self._seen_trade_ids:
                return None
            self._seen_trade_ids.add(trade_id)

        symbol_id = int(getattr(evt, "symbol_id", 0) or 0)
        if symbol_id <= 0:
            return None

        agg = self.get_or_create(
            int(getattr(evt, "exchange_id", 0) or self.exchange_id),
            int(getattr(evt, "account_id", 0) or self.account_id),
            symbol_id,
        )

        if not agg.apply_trade(evt):
            return None

        return agg

    # ------------------------------------------------------------
    def on_mark_price(self, *, symbol_id: int, price: float) -> Optional[PositionAggregate]:
        symbol_id = int(symbol_id or 0)
        price = float(price or 0.0)
        if symbol_id <= 0 or price <= 0:
            return None

        key = (self.exchange_id, self.account_id, symbol_id)
        agg = self._aggs.get(key)
        if not agg:
            return None

        if agg.apply_mark_price(price):
            return agg
        return None

    # ------------------------------------------------------------
    def snapshot(self, *, include_flat: bool = True) -> List[PositionAggregate]:
        aggs = list(self._aggs.values())
        if include_flat:
            return aggs
        return [a for a in aggs if a.is_open()]

    def iter_dirty_positions(self):
        """
        Include dirty FLAT positions too (important for close lifecycle).
        """
        for agg in self._aggs.values():
            if agg.is_dirty():
                yield agg

    def iter_positions(self, *, include_flat: bool = True) -> Iterable[PositionAggregate]:
        for agg in self._aggs.values():
            if include_flat or agg.is_open():
                yield agg
