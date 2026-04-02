# src/platform/core/oms/rebuild.py
from __future__ import annotations

from src.platform.core.oms.events import OrderEvent
from src.platform.core.oms.aggregate import OrderAggregate


def rebuild_orders(
    storage,
    *,
    exchange_id: int,
    account_id: int,
) -> dict[str, OrderAggregate]:
    """
    Rebuild OMS OrderAggregates from persisted order_events.
    """

    rows = storage.fetch_order_events(
        exchange_id=int(exchange_id),
        account_id=int(account_id),
    )

    aggs: dict[str, OrderAggregate] = {}

    for row in rows:
        # --------------------------------------------------
        # ВАЖНО: OrderEvent требует exchange/account/symbol
        # --------------------------------------------------
        evt = OrderEvent(
            exchange=str(row.get("exchange") or ""),
            account=str(row.get("account") or ""),
            symbol=str(row.get("symbol") or ""),

            exchange_id=row.get("exchange_id"),
            account_id=row.get("account_id"),
            symbol_id=row.get("symbol_id"),

            order_id=str(row.get("order_id") or ""),
            client_order_id=row.get("client_order_id"),

            status=row.get("status"),
            side=row.get("side"),
            type=row.get("type"),
            reduce_only=bool(row.get("reduce_only") or False),

            price=row.get("price"),
            qty=row.get("qty"),
            filled_qty=row.get("filled_qty"),

            ts_ms=int(row.get("ts_ms") or 0),
            source=row.get("source"),

            raw_json=row.get("raw_json"),
        )

        oid = evt.order_id
        if not oid:
            continue

        if oid not in aggs:
            aggs[oid] = OrderAggregate.from_event(evt)
        else:
            aggs[oid].apply(evt)

    return aggs
