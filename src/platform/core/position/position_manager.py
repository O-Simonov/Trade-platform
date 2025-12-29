# src/platform/core/position/position_manager.py
from __future__ import annotations

import logging
from typing import Dict, Tuple, Optional

from src.platform.core.oms.events import TradeEvent
from src.platform.core.position.aggregate import PositionAggregate


class PositionManager:
    """
    In-memory position aggregates built strictly from TradeEvent stream.

    Responsibilities:
      ✔ deduplicate trades
      ✔ maintain PositionAggregate per (exchange, account, symbol)
      ✔ return updated aggregate when position changes
      ✖ NO persistence
    """

    def __init__(
        self,
        *,
        storage,
        exchange_id: int,
        account_id: int,
        logger: logging.Logger | None = None,
    ) -> None:
        self.storage = storage
        self.exchange_id = int(exchange_id)
        self.account_id = int(account_id)
        self.logger = logger or logging.getLogger(__name__)

        # key = (exchange_id, account_id, symbol_id)
        self._aggs: Dict[Tuple[int, int, int], PositionAggregate] = {}

        # trade_id deduplication
        self._seen_trade_ids: set[str] = set()

    # ------------------------------------------------------------
    @staticmethod
    def _key(exchange_id: int, account_id: int, symbol_id: int) -> tuple[int, int, int]:
        return int(exchange_id), int(account_id), int(symbol_id)

    def get_or_create(
        self,
        exchange_id: int,
        account_id: int,
        symbol_id: int,
    ) -> PositionAggregate:
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
        """
        Apply TradeEvent to position aggregate.

        Returns:
          PositionAggregate if position changed,
          None otherwise.
        """

        trade_id = str(getattr(evt, "trade_id", "") or "")
        if trade_id:
            if trade_id in self._seen_trade_ids:
                return None
            self._seen_trade_ids.add(trade_id)

        agg = self.get_or_create(
            int(evt.exchange_id or self.exchange_id),
            int(evt.account_id or self.account_id),
            int(evt.symbol_id),
        )

        changed = agg.apply_trade(evt)
        if not changed:
            return None

        return agg

    # ------------------------------------------------------------
    def rebuild_from_trades(
        self,
        *,
        since_ts_ms: int = 0,
        limit: int = 200_000,
    ) -> int:
        """
        STEP H.3

        Rebuild in-memory positions from trades table.
        Persistence is done by caller (OMS / Engine).
        """

        if not hasattr(self.storage, "fetch_trades"):
            self.logger.warning(
                "[POSITIONS][REBUILD] storage.fetch_trades missing -> skip"
            )
            return 0

        rows = self.storage.fetch_trades(
            exchange_id=self.exchange_id,
            account_id=self.account_id,
            since_ts_ms=int(since_ts_ms or 0),
            limit=int(limit or 200_000),
        )

        if not rows:
            self.logger.info("[POSITIONS][REBUILD] no trades found")
            return 0

        for r in rows:
            evt = TradeEvent(
                exchange="",
                account="",
                symbol="",

                exchange_id=int(r.get("exchange_id") or self.exchange_id),
                account_id=int(r.get("account_id") or self.account_id),
                symbol_id=int(r["symbol_id"]),

                trade_id=str(r.get("trade_id") or ""),
                order_id=str(r.get("order_id") or ""),
                side=str(r.get("side") or ""),
                qty=float(r.get("qty") or 0.0),
                price=float(r.get("price") or 0.0),
                realized_pnl=float(r.get("realized_pnl") or 0.0),
                fee=float(r.get("fee") or 0.0),
                ts_ms=int(r.get("ts_ms") or 0),
                raw=r.get("raw_json"),
            )

            self.on_trade_event(evt)

        self.logger.info(
            "[POSITIONS][REBUILD] rebuilt %d positions from %d trades",
            len(self._aggs),
            len(rows),
        )
        return len(self._aggs)
