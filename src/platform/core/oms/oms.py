# src/platform/core/oms/oms.py
from __future__ import annotations

import time
import logging
from dataclasses import dataclass
from typing import Optional

from src.platform.core.models.order import OrderIntent
from src.platform.core.oms.events import OrderEvent, TradeEvent
from src.platform.core.oms.preflight import preflight_intent
from src.platform.core.oms.writer import OmsWriter
from src.platform.core.oms.aggregate import OrderAggregate
from src.platform.core.position.position_manager import PositionManager


_FINAL_STATUSES = {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}

# ----------------------------------------------------------------------
# Pending submit placeholder (idempotency / WS gaps)
# ----------------------------------------------------------------------
@dataclass(slots=True)
class PendingSubmit:
    client_order_id: str
    symbol_id: int
    strategy_id: str
    pos_uid: str | None
    intent: OrderIntent
    created_ts: float


class OrderManager:
    """
    STEP G.2 / H.1 OrderManager

    Responsibilities:
      ✔ route OrderEvent / TradeEvent
      ✔ append immutable events (order_events / trades)
      ✔ maintain in-memory OrderAggregate (FSM)
      ✔ persist order snapshot
      ✔ maintain PositionAggregate via trades
      ✔ minimal submit-side idempotency
    """

    # ------------------------------------------------------------------
    # init
    # ------------------------------------------------------------------
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

        # persistence only
        self.writer = OmsWriter(
            storage=self.storage,
            logger=self.logger,
        )

        # submit-side idempotency
        self._pending: dict[str, PendingSubmit] = {}
        self._final_seen: dict[str, float] = {}

        # order aggregates (FSM)
        self._orders: dict[str, OrderAggregate] = {}

        # position aggregates (from trades)
        self.position_manager = PositionManager(
            storage=self.storage,
            exchange_id=self.exchange_id,
            account_id=self.account_id,
            logger=self.logger,
        )

    # ------------------------------------------------------------------
    # submit-side helpers
    # ------------------------------------------------------------------
    def should_submit(self, client_order_id: str) -> bool:
        if not client_order_id:
            return True
        if client_order_id in self._pending:
            return False
        if client_order_id in self._final_seen:
            return False
        return True

    def record_pending_submit(
        self,
        *,
        client_order_id: str,
        symbol_id: int,
        strategy_id: str,
        pos_uid: str | None,
        intent: OrderIntent,
    ) -> None:
        if not client_order_id:
            return

        self._pending[client_order_id] = PendingSubmit(
            client_order_id=client_order_id,
            symbol_id=int(symbol_id),
            strategy_id=str(strategy_id or "unknown"),
            pos_uid=str(pos_uid) if pos_uid else None,
            intent=intent,
            created_ts=time.time(),
        )

    def reconcile_pending_timeouts(self, timeout_sec: float) -> int:
        now = time.time()
        timeout = float(timeout_sec or 20)

        expired: list[str] = []
        for cid, p in list(self._pending.items()):
            if now - p.created_ts >= timeout:
                expired.append(cid)

        for cid in expired:
            self._pending.pop(cid, None)
            self._final_seen[cid] = now

        return len(expired)

    # ------------------------------------------------------------------
    # event ingestion
    # ------------------------------------------------------------------
    def apply_event(self, ev: object) -> None:
        if isinstance(ev, OrderEvent):
            self.on_order_event(ev)
        elif isinstance(ev, TradeEvent):
            self.on_trade_event(ev)
        else:
            self.logger.debug("[OMS] ignored unknown event=%s", type(ev))

    # ------------------------------------------------------------------
    # order events
    # ------------------------------------------------------------------
    def on_order_event(self, evt: OrderEvent) -> None:
        """
        OrderEvent flow (STEP G.2):

          1) normalize routing ids
          2) persist immutable event
          3) apply to OrderAggregate (FSM)
          4) persist order snapshot
        """

        # normalize routing ids
        evt.exchange_id = int(evt.exchange_id or self.exchange_id)
        evt.account_id = int(evt.account_id or self.account_id)

        # 1️⃣ immutable log
        self.writer.append_order_event(evt)

        oid = str(evt.order_id)

        # 2️⃣ aggregate / FSM
        agg = self._orders.get(oid)
        if agg is None:
            agg = OrderAggregate.from_event(evt)
            self._orders[oid] = agg
        else:
            changed = agg.apply(evt)
            if not changed:
                return

        # mark final for idempotency
        if agg.status in _FINAL_STATUSES and agg.client_order_id:
            self._final_seen[agg.client_order_id] = time.time()
            self._pending.pop(agg.client_order_id, None)

        # 3️⃣ snapshot
        self.writer.upsert_order_snapshot(agg)

    # ------------------------------------------------------------------
    # trade events (fills)
    # ------------------------------------------------------------------
    def on_trade_event(self, evt: TradeEvent) -> None:
        """
        TradeEvent flow (STEP H).

        1) normalize routing ids
        2) persist immutable trade
        3) apply to PositionAggregate
        4) persist position snapshot if changed.
        """

        # normalize routing ids
        evt.exchange_id = int(evt.exchange_id or self.exchange_id)
        evt.account_id = int(evt.account_id or self.account_id)

        # immutable trade log
        self.writer.write_trade(evt)

        # update in-memory position
        pos = self.position_manager.on_trade_event(evt)

        if pos is not None:
            # explicitly tell type checker what pos is
            assert pos.symbol_id is not None

            try:
                self.storage.upsert_positions([pos.to_row()])
            except Exception:
                self.logger.exception(
                    "[POS][SNAPSHOT] upsert failed symbol_id=%s",
                    pos.symbol_id,
                )

        return None

    # ------------------------------------------------------------------
    # submit preflight
    # ------------------------------------------------------------------
    def preflight(
        self,
        *,
        symbol: str,
        symbol_id: int,
        intent: OrderIntent,
        last_price: float,
        logger=None,
    ) -> Optional[OrderIntent]:
        """
        Normalize intent (qty/price rounding, minNotional, etc.).
        """
        try:
            filters = self.storage.get_symbol_filters(
                exchange_id=self.exchange_id,
                symbol_id=int(symbol_id),
            )
        except Exception:
            self.logger.exception(
                "[OMS][PREFLIGHT] get_symbol_filters failed symbol=%s",
                symbol,
            )
            return None

        return preflight_intent(
            intent=intent,
            last_price=float(last_price or 0.0),
            filters=filters,
            logger=logger or self.logger,
        )
