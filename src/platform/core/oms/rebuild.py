# src/platform/core/oms/rebuild.py
from src.platform.core.oms.events import OrderEvent
from src.platform.core.oms.aggregate import OrderAggregate


def rebuild_orders(storage, *, exchange_id: int, account_id: int):
    rows = storage.fetch_order_events(
        exchange_id=exchange_id,
        account_id=account_id,
    )

    aggs: dict[str, OrderAggregate] = {}

    for r in rows:
        evt = OrderEvent(
            # 🔹 ОБЯЗАТЕЛЬНЫЕ строковые поля
            exchange=r.get("exchange") or "",
            account=r.get("account") or "",
            symbol=r.get("symbol") or "",

            # 🔹 ID-поля
            exchange_id=r["exchange_id"],
            account_id=r["account_id"],
            symbol_id=r["symbol_id"],

            # 🔹 Ордер
            order_id=r["order_id"],
            client_order_id=r.get("client_order_id"),

            side=r.get("side"),
            type=r.get("type"),
            reduce_only=bool(r.get("reduce_only")),

            price=r.get("price"),
            qty=r.get("qty"),
            filled_qty=r.get("filled_qty"),

            status=r.get("status"),
            ts_ms=r.get("ts_ms"),

            source=r.get("source"),
            raw=r.get("raw_json"),
        )

        oid = evt.order_id

        if oid not in aggs:
            aggs[oid] = OrderAggregate.from_event(evt)
        else:
            aggs[oid].apply(evt)

    return aggs
