from __future__ import annotations
from datetime import datetime, timezone
from platform.core.models.order import OrderIntent
from platform.core.models.enums import OrderState

class OrderManager:
    def __init__(self, *, storage, exchange_id: int, account_id: int):
        self.db = storage
        self.exchange_id = exchange_id
        self.account_id = account_id

    def should_submit(self, client_order_id: str) -> bool:
        return not self.db.client_order_exists(self.exchange_id, self.account_id, client_order_id)

    def record_pending_submit(self, *, client_order_id: str, symbol_id: int, strategy_id: str, pos_uid: str | None, intent: OrderIntent):
        now = datetime.now(timezone.utc)
        self.db.upsert_order_placeholder({
            "exchange_id": self.exchange_id,
            "account_id": self.account_id,
            "order_id": f"CLIENT:{client_order_id}",
            "symbol_id": symbol_id,
            "strategy_id": strategy_id,
            "pos_uid": pos_uid,
            "client_order_id": client_order_id,
            "side": getattr(intent.side, "value", str(intent.side)),
            "type": intent.order_type.value,
            "reduce_only": intent.reduce_only,
            "price": intent.price,
            "qty": intent.qty,
            "filled_qty": 0.0,
            "status": OrderState.PENDING_SUBMIT.value,
            "created_at": now,
            "updated_at": now,
            "source": "oms",
        })

    def reconcile_pending_timeouts(self, timeout_sec: int) -> int:
        return self.db.expire_stuck_pending(self.exchange_id, self.account_id, timeout_sec)

    def reconcile_open_orders(self, open_orders: list[dict]) -> int:
        return self.db.resolve_placeholders_with_open_orders(self.exchange_id, self.account_id, open_orders)
