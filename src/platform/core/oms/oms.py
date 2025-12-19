# src/platform/core/oms/oms.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from src.platform.core.models.order import OrderIntent
from src.platform.core.models.enums import OrderState
from src.platform.data.storage.base import Storage


class OrderManager:
    def __init__(self, *, storage: Storage, exchange_id: int, account_id: int):
        self.storage = storage
        self.exchange_id = int(exchange_id)
        self.account_id = int(account_id)

    # ------------------------------------------------------------------
    # IDENTITY / IDEMPOTENCY
    # ------------------------------------------------------------------

    def should_submit(self, client_order_id: str) -> bool:
        # FIX: было self.db -> должно быть self.storage
        return not self.storage.client_order_exists(
            exchange_id=self.exchange_id,
            account_id=self.account_id,
            client_order_id=client_order_id,
        )

    # ------------------------------------------------------------------
    # PLACEHOLDER BEFORE SUBMIT
    # ------------------------------------------------------------------

    def record_pending_submit(
        self,
        *,
        client_order_id: str,
        symbol_id: int,
        strategy_id: str,
        pos_uid: str | None,
        intent: OrderIntent,
    ) -> None:
        now = datetime.now(timezone.utc)

        # FIX: было self.db -> должно быть self.storage
        self.storage.upsert_order_placeholder(
            {
                "exchange_id": self.exchange_id,
                "account_id": self.account_id,
                "order_id": f"CLIENT:{client_order_id}",  # placeholder
                "symbol_id": int(symbol_id),
                "strategy_id": str(strategy_id),
                "pos_uid": pos_uid,
                "client_order_id": client_order_id,
                "side": getattr(intent.side, "value", str(intent.side)),
                "type": intent.order_type.value,
                "reduce_only": bool(intent.reduce_only),
                "price": float(intent.price) if intent.price is not None else None,
                "qty": float(intent.qty),
                "filled_qty": 0.0,
                "status": OrderState.PENDING_SUBMIT.value,
                "created_at": now,
                "updated_at": now,
                "source": "oms",
            }
        )

    # ------------------------------------------------------------------
    # RECONCILE
    # ------------------------------------------------------------------

    def reconcile_pending_timeouts(self, timeout_sec: int) -> int:
        # FIX: было self.db -> должно быть self.storage
        return int(
            self.storage.expire_stuck_pending(
                exchange_id=self.exchange_id,
                account_id=self.account_id,
                timeout_sec=int(timeout_sec),
            )
        )

    def reconcile_open_orders(self, open_orders: list[dict[str, Any]]) -> int:
        # FIX: было self.db -> должно быть self.storage
        return int(
            self.storage.resolve_placeholders_with_open_orders(
                exchange_id=self.exchange_id,
                account_id=self.account_id,
                open_orders=open_orders,
            )
        )

    # ------------------------------------------------------------------
    # LOOKUP PENDING METADATA (for WS fills)
    # ------------------------------------------------------------------

    def get_pending_by_client_id(self, client_order_id: str) -> dict | None:
        return self.storage.get_pending_order_by_client_id(
            exchange_id=self.exchange_id,
            account_id=self.account_id,
            client_order_id=client_order_id,
        )