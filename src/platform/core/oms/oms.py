# src/platform/core/oms/oms.py
from __future__ import annotations

import logging
import time
from typing import Dict, Optional, Any

from src.platform.core.models.order import OrderIntent
from src.platform.core.oms.decisions import resolve_order
from src.platform.core.oms.events import OrderEvent
from src.platform.core.position.position_manager import PositionManager
from src.platform.core.models.enums import Side


class OrderManager:
    """
    K6.8 OMS

    Responsibilities:
      ✔ order lifecycle FSM (pending submit tracking)
      ✔ Order ↔ Position decision (OPEN / REDUCE)
      ✔ reduceOnly enforcement
      ✖ NO exchange I/O
    """

    def __init__(
        self,
        *,
        storage: Any,
        exchange_id: int,
        account_id: int,
        position_manager: PositionManager,
        logger: logging.Logger | None = None,
    ) -> None:
        self.storage = storage
        self.exchange_id = int(exchange_id)
        self.account_id = int(account_id)
        self.pm = position_manager
        self.logger = logger or logging.getLogger("oms")

        self.logger.info("[OMS] position_manager_id=%s", id(self.pm))

        # client_order_id -> ts
        self._pending: Dict[str, float] = {}

    # ------------------------------------------------------------
    def should_submit(self, client_order_id: str) -> bool:
        return client_order_id not in self._pending

    def record_pending_submit(self, *, client_order_id: str) -> None:
        self._pending[str(client_order_id)] = time.time()

    def reconcile_pending_timeouts(self, timeout_sec: float) -> None:
        now = time.time()
        expired = [cid for cid, ts in self._pending.items() if now - ts >= timeout_sec]
        for cid in expired:
            self.logger.warning("[OMS] pending timeout cid=%s", cid)
            self._pending.pop(cid, None)

    # ------------------------------------------------------------
    def apply_event(self, event: OrderEvent) -> None:
        """
        Apply exchange events (ACK / FILL / CANCEL).
        """
        cid = getattr(event, "client_order_id", None)
        if cid and str(cid) in self._pending:
            self._pending.pop(str(cid), None)

    # ------------------------------------------------------------
    def _is_close_intent(self, intent: OrderIntent) -> bool:
        it = getattr(intent, "intent_type", None)
        if it is None:
            return False
        s = str(it).upper()
        return "CLOSE" in s or "REDUCE" in s

    def resolve_intent(self, intent: OrderIntent) -> Optional[OrderIntent]:
        """
        K6.8 core logic:
        OrderIntent → OrderDecision → final OrderIntent
        """

        symbol_id = int(getattr(intent, "symbol_id", 0) or 0)
        if symbol_id <= 0:
            self.logger.warning("[OMS] missing/invalid symbol_id intent=%s", intent)
            return None

        agg = self.pm.get_or_create(
            self.exchange_id,
            self.account_id,
            symbol_id,
        )

        # CRITICAL SAFETY:
        # CLOSE intent must NOT become OPEN when position is not yet in memory.
        if self._is_close_intent(intent):
            ps = getattr(agg, "side", None)
            pq = float(getattr(agg, "qty", 0.0) or 0.0)
            if pq <= 0.0 or ps is None:
                self.logger.warning(
                    "[OMS][BLOCK][CLOSE] %s %s qty=%.6f reason=position_is_flat_in_memory",
                    intent.symbol,
                    getattr(intent.side, "name", str(intent.side)),
                    float(intent.qty or 0.0),
                )
                return None

        decision = resolve_order(
            pos_side=getattr(agg, "side", None),
            pos_qty=float(getattr(agg, "qty", 0.0) or 0.0),
            order_side=intent.side,           # Side.LONG / Side.SHORT
            order_qty=float(intent.qty),
        )

        if not decision.allowed:
            self.logger.warning(
                "[OMS][BLOCK] %s %s qty=%.6f reason=%s",
                intent.symbol,
                getattr(intent.side, "name", str(intent.side)),
                float(intent.qty),
                decision.reason,
            )
            return None

        # Apply decision
        intent.qty = float(decision.qty)
        intent.reduce_only = bool(decision.reduce_only)

        # Extra safety: reduceOnly must be True for REDUCE mode.
        if decision.mode == "REDUCE":
            intent.reduce_only = True

        self.logger.info(
            "[OMS][%s] %s %s qty=%.6f reduce_only=%s",
            decision.mode,
            intent.symbol,
            getattr(intent.side, "name", str(intent.side)),
            float(intent.qty),
            bool(intent.reduce_only),
        )

        return intent
