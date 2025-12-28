from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

from src.platform.core.models.order import OrderIntent
from src.platform.core.oms.events import OrderEvent, TradeEvent
from src.platform.core.oms.preflight import preflight_intent


_FINAL_STATUSES = {"FILLED", "CANCELED", "REJECTED", "EXPIRED"}


# ----------------------------------------------------------------------
# Pending submit placeholder
# ----------------------------------------------------------------------

@dataclass
class PendingSubmit:
    client_order_id: str
    symbol_id: int
    strategy_id: str
    pos_uid: str
    intent: OrderIntent
    created_ts: float


# ----------------------------------------------------------------------
# OMS
# ----------------------------------------------------------------------

class OrderManager:
    """
    OMS — Order State Manager

    STEP E.2 responsibilities:
      - preflight (symbol_filters + normalize)
      - idempotency gating
      - pending placeholders
      - consume WS order/trade events
      - pending timeout cleanup
    """

    def __init__(self, *, storage, exchange_id: int, account_id: int):
        self.storage = storage
        self.exchange_id = int(exchange_id)
        self.account_id = int(account_id)

        # cid -> PendingSubmit
        self._pending: dict[str, PendingSubmit] = {}

        # cid -> ts (final status seen)
        self._final_seen: dict[str, float] = {}

        # cid -> last known status
        self._last_status: dict[str, str] = {}

    # ------------------------------------------------------------------
    # STEP D — PREFLIGHT
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
        Normalize qty/price using symbol_filters.
        Return normalized intent or None (skip).
        """
        try:
            filters = self.storage.get_symbol_filters(
                exchange_id=self.exchange_id,
                symbol_id=int(symbol_id),
            )
        except KeyError:
            if logger:
                logger.warning("[OMS][PREFLIGHT] no symbol_filters for %s → skip", symbol)
            return None
        except Exception as e:
            if logger:
                logger.warning("[OMS][PREFLIGHT] error loading filters for %s: %s", symbol, e)
            return None

        return preflight_intent(
            intent,
            filters=filters,
            last_price=float(last_price),
            logger=logger,
        )

    # ------------------------------------------------------------------
    # Idempotency
    # ------------------------------------------------------------------

    def should_submit(self, client_order_id: str) -> bool:
        """
        Decide whether we are allowed to submit this CID.
        """
        if not client_order_id:
            return False

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
        pos_uid: str,
        intent: OrderIntent,
    ) -> None:
        """
        Register placeholder before sending order to exchange.
        """
        self._pending[client_order_id] = PendingSubmit(
            client_order_id=client_order_id,
            symbol_id=int(symbol_id),
            strategy_id=str(strategy_id),
            pos_uid=str(pos_uid),
            intent=intent,
            created_ts=time.time(),
        )

    def reconcile_pending_timeouts(self, timeout_sec: float) -> int:
        """
        Drop pending orders that never received WS/REST confirmation.
        """
        now = time.time()
        timeout = float(timeout_sec or 20)

        expired: list[str] = []
        for cid, p in self._pending.items():
            if now - p.created_ts >= timeout:
                expired.append(cid)

        for cid in expired:
            self._pending.pop(cid, None)
            self._final_seen[cid] = now
            self._last_status[cid] = "TIMEOUT"

        return len(expired)

    # ------------------------------------------------------------------
    # STEP E.2 — consume events
    # ------------------------------------------------------------------

    def on_order_event(self, evt: OrderEvent) -> None:
        """
        Consume ORDER_TRADE_UPDATE (order part).
        """
        cid = evt.client_order_id or ""
        if not cid:
            return

        status = (evt.status or "").upper()
        if status:
            self._last_status[cid] = status

        # Final status closes pending
        if status in _FINAL_STATUSES:
            self._pending.pop(cid, None)
            self._final_seen[cid] = time.time()

    def on_trade_event(self, evt: TradeEvent) -> None:
        """
        Consume execution (trade).
        Trades do NOT finalize order — only status does.
        """
        # For now we only record via storage layer
        # (PnL / positions will be STEP H)
        return
