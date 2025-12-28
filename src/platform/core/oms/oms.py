# src/platform/core/oms/oms.py
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Optional

from src.platform.core.models.order import OrderIntent
from .events import OrderEvent, TradeEvent
from .preflight import preflight_intent


_FINAL_STATUSES = {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}


@dataclass
class PendingSubmit:
    client_order_id: str
    symbol_id: int
    strategy_id: str
    pos_uid: str
    intent: OrderIntent
    created_ts: float


class OrderManager:
    """
    STEP E.2 — OMS State Machine (minimal, but correct)

    Responsibilities:
      - preflight (symbol_filters)
      - idempotency gating (should_submit)
      - pending placeholders (record_pending_submit)
      - consume user events (on_order_event / on_trade_event)
      - timeouts for stuck pending
    """

    def __init__(self, *, storage, exchange_id: int, account_id: int):
        self.storage = storage
        self.exchange_id = int(exchange_id)
        self.account_id = int(account_id)

        self._pending: dict[str, PendingSubmit] = {}      # cid -> pending
        self._final_seen: dict[str, float] = {}           # cid -> ts
        self._last_status: dict[str, str] = {}            # cid -> status

    # ------------------------------------------------------------------
    # STEP D — PREFLIGHT
    # ------------------------------------------------------------------

    def preflight_intent(
        self,
        *,
        symbol: str,
        symbol_id: int,
        intent: OrderIntent,
        last_price: float,
        logger=None,
    ) -> Optional[OrderIntent]:
        try:
            filters = self.storage.get_symbol_filters(
                exchange_id=self.exchange_id,
                symbol_id=int(symbol_id),
            )
        except KeyError:
            if logger:
                logger.warning("[OMS][PREFLIGHT] no symbol_filters for %s -> skip", symbol)
            return None
        except Exception as e:
            if logger:
                logger.warning("[OMS][PREFLIGHT] get_symbol_filters error for %s: %s", symbol, e)
            return None

        return preflight_intent(intent, filters=filters, last_price=float(last_price), logger=logger)

    # ------------------------------------------------------------------
    # Idempotency
    # ------------------------------------------------------------------

    def should_submit(self, client_order_id: str) -> bool:
        if not client_order_id:
            return False
        if client_order_id in self._pending:
            return False
        if client_order_id in self._final_seen:
            # already finished earlier (idempotency)
            return False
        return True

    def record_pending_submit(
        self,
        *,
        client_order_id: str,
        symbol_id: int,
        strategy_id: str,
        pos_uid: str,
        intent: OrderIntent,
    ) -> None:
        self._pending[client_order_id] = PendingSubmit(
            client_order_id=client_order_id,
            symbol_id=int(symbol_id),
            strategy_id=str(strategy_id),
            pos_uid=str(pos_uid),
            intent=intent,
            created_ts=time.time(),
        )

    def reconcile_pending_timeouts(self, timeout_sec: int) -> int:
        """
        Drop pending placeholders that never got an order update from WS/REST.
        """
        now = time.time()
        timeout = float(timeout_sec or 20)
        drop: list[str] = []
        for cid, p in self._pending.items():
            if now - p.created_ts >= timeout:
                drop.append(cid)

        for cid in drop:
            self._pending.pop(cid, None)
            self._final_seen[cid] = now
            self._last_status[cid] = "TIMEOUT"

        return len(drop)

    # ------------------------------------------------------------------
    # STEP E.2 — consume events
    # ------------------------------------------------------------------

    def on_order_event(self, evt: OrderEvent) -> None:
        cid = evt.client_order_id or ""
        if not cid:
            return

        st = (evt.status or "").upper() if evt.status else ""
        if st:
            self._last_status[cid] = st

        # If we see any order update for cid -> it is no longer "unknown"
        if cid in self._pending and st in _FINAL_STATUSES:
            self._pending.pop(cid, None)
            self._final_seen[cid] = time.time()
            return

        # Also treat final even if we didn't have pending (restart case)
        if st in _FINAL_STATUSES:
            self._final_seen[cid] = time.time()

    def on_trade_event(self, evt: TradeEvent) -> None:
        # Trades usually come after order updates, but can be used as a signal too
        cid = evt.client_order_id or ""
        if not cid:
            return
        # If we got a trade - at least "executing happened"
        # Do not force-finalize here; finalization comes from order status FILLED.
        return
