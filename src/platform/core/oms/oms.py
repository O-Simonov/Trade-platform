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

        # ВАЖНО: orders.order_id в БД = text PK (exchange_id, account_id, order_id)
        # Для placeholder используем "PH::" чтобы потом легко резолвить.
        row = {
            "exchange_id": self.exchange_id,
            "account_id": self.account_id,
            "order_id": f"PH::{client_order_id}",
            "symbol_id": int(symbol_id),
            "strategy_id": str(strategy_id or "unknown"),
            "pos_uid": pos_uid,
            "client_order_id": client_order_id,
            "side": getattr(intent.side, "value", str(intent.side)),
            "type": getattr(intent.order_type, "value", str(intent.order_type)),
            "reduce_only": bool(getattr(intent, "reduce_only", False)),
            "price": float(intent.price) if getattr(intent, "price", None) is not None else None,
            "qty": float(intent.qty),
            "filled_qty": 0.0,
            "status": OrderState.PENDING_SUBMIT.value,
            "created_at": now,
            "updated_at": now,
            "source": "oms",
            "ts_ms": int(now.timestamp() * 1000),
            "raw_json": None,
        }

        self.storage.upsert_order_placeholder(row)

    # ------------------------------------------------------------------
    # RECONCILE
    # ------------------------------------------------------------------

    def reconcile_pending_timeouts(self, timeout_sec: int) -> int:
        return int(
            self.storage.expire_stuck_pending(
                exchange_id=self.exchange_id,
                account_id=self.account_id,
                timeout_sec=int(timeout_sec),
            )
        )

    def reconcile_open_orders(self, open_orders: list[dict[str, Any]]) -> int:
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
