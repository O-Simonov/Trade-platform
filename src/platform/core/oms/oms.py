# src/platform/core/oms/oms.py
from __future__ import annotations

import time
import logging
from dataclasses import dataclass
from typing import Optional, Iterable

from src.platform.core.models.order import OrderIntent
from src.platform.core.oms.events import OrderEvent, TradeEvent
from src.platform.core.oms.preflight import preflight_intent
from src.platform.core.oms.writer import OmsWriter


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
    """Minimal OMS layer.

    STEP G.1 goal:
      - accept WS user events (OrderEvent / TradeEvent)
      - persist events into DB (order_events + trades)
      - keep a tiny idempotency gate for client_order_id

    Full aggregate/FSM (STEP G.2) will be added later.
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

        self.writer = OmsWriter(storage=self.storage, oms=self, logger=self.logger)

        # cid -> PendingSubmit
        self._pending: dict[str, PendingSubmit] = {}

        # cid -> last seen status (debug)
        self._last_status: dict[str, str] = {}

        # cid -> ts when we saw final status (idempotency)
        self._final_seen: dict[str, float] = {}

    # ------------------------------------------------------------------
    # submit-side helpers
    # ------------------------------------------------------------------
    def should_submit(self, client_order_id: str) -> bool:
        """Simple idempotency: don't submit same CID twice."""
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
        """Drop pending orders that never received WS/REST confirmation."""
        now = time.time()
        timeout = float(timeout_sec or 20)

        expired: list[str] = []
        for cid, p in list(self._pending.items()):
            if now - p.created_ts >= timeout:
                expired.append(cid)

        for cid in expired:
            self._pending.pop(cid, None)
            self._final_seen[cid] = now
            self._last_status[cid] = "TIMEOUT"

        return len(expired)

    # ------------------------------------------------------------------
    # event ingestion
    # ------------------------------------------------------------------
    def apply_event(self, ev: object) -> None:
        if isinstance(ev, OrderEvent):
            self.on_order_event(ev)
            return
        if isinstance(ev, TradeEvent):
            self.on_trade_event(ev)
            return
        self.logger.debug("[OMS] ignored unknown event=%s", type(ev))

    def on_order_event(self, evt: OrderEvent) -> None:
        """Consume order status update."""
        # ensure ids match current instance (safety)
        evt.exchange_id = int(evt.exchange_id or self.exchange_id)
        evt.account_id = int(evt.account_id or self.account_id)

        self.writer.write_order(evt)

        cid = str(evt.client_order_id or "")
        status = str(evt.status or "").upper()
        if cid:
            self._last_status[cid] = status
            if status in _FINAL_STATUSES:
                self._pending.pop(cid, None)
                self._final_seen[cid] = time.time()

    def on_trade_event(self, evt: TradeEvent) -> None:
        """Consume trade(fill) event."""
        evt.exchange_id = int(evt.exchange_id or self.exchange_id)
        evt.account_id = int(evt.account_id or self.account_id)
        self.writer.write_trade(evt)

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
        """Normalize intent (qty/price rounding, minNotional, etc)."""
        try:
            filters = self.storage.get_symbol_filters(
                exchange_id=self.exchange_id,
                symbol_id=int(symbol_id),
            )
        except Exception:
            self.logger.exception("[OMS][Preflight] get_symbol_filters failed symbol=%s", symbol)
            return None

        return preflight_intent(
            symbol=symbol,
            intent=intent,
            last_price=float(last_price or 0.0),
            filters=filters,
            logger=logger or self.logger,
        )
