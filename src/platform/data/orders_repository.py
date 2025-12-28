# src/platform/data/orders_repository.py
from datetime import datetime
from typing import Iterable, Optional

from src.platform.core.orders.models import Order


class OrdersRepository:
    def __init__(self, conn):
        self._conn = conn

    def upsert(self, order: Order) -> None:
        sql = """
        INSERT INTO orders (
            order_id, exchange, account_id, symbol,
            side, type, status,
            price, stop_price,
            qty, filled_qty, avg_price,
            reduce_only, position_side,
            client_order_id, strategy_id,
            created_ts, updated_ts, last_trade_id
        )
        VALUES (
            %(order_id)s, %(exchange)s, %(account_id)s, %(symbol)s,
            %(side)s, %(type)s, %(status)s,
            %(price)s, %(stop_price)s,
            %(qty)s, %(filled_qty)s, %(avg_price)s,
            %(reduce_only)s, %(position_side)s,
            %(client_order_id)s, %(strategy_id)s,
            %(created_ts)s, %(updated_ts)s, %(last_trade_id)s
        )
        ON CONFLICT (order_id) DO UPDATE SET
            status        = EXCLUDED.status,
            filled_qty    = EXCLUDED.filled_qty,
            avg_price     = EXCLUDED.avg_price,
            updated_ts    = EXCLUDED.updated_ts,
            last_trade_id = EXCLUDED.last_trade_id
        """
        self._conn.execute(sql, vars(order))

    def get(self, order_id: int) -> Optional[Order]:
        row = self._conn.fetch_one(
            "SELECT * FROM orders WHERE order_id = %s",
            (order_id,)
        )
        return Order(**row) if row else None

    def get_non_terminal(self) -> list[Order]:
        rows = self._conn.fetch_all(
            """
            SELECT * FROM orders
            WHERE status NOT IN ('FILLED', 'CANCELED', 'REJECTED', 'EXPIRED')
            """
        )
        return [Order(**r) for r in rows]
