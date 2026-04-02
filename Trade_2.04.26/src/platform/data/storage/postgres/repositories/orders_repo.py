from __future__ import annotations

import json
import time
import logging
from datetime import datetime, timezone
from typing import Any, Iterable
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)
log = logging.getLogger("storage.sql")

from .base import _utcnow

class OrdersRepositoryMixin:
    def client_order_exists(self, *, exchange_id: int, account_id: int, client_order_id: str) -> bool:
        row = self._exec_one(
            """
            SELECT 1 FROM orders
            WHERE exchange_id = %s AND account_id = %s AND client_order_id = %s
            LIMIT 1
            """,
            (exchange_id, account_id, client_order_id),
        )
        return bool(row)

    def get_order(self, *, exchange_id: int, account_id: int, order_id: str) -> dict | None:
        query = """
            SELECT exchange_id,
                   account_id,
                   order_id,
                   symbol_id,
                   strategy_id,
                   pos_uid,
                   client_order_id,
                   side,
                   type,
                   reduce_only,
                   price,
                   qty,
                   filled_qty,
                   status,
                   created_at,
                   updated_at,
                   source,
                   ts_ms,
                   raw_json
            FROM orders
            WHERE exchange_id = %s
              AND account_id = %s
              AND order_id = %s
            LIMIT 1
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(exchange_id), int(account_id), str(order_id)))
                row = cur.fetchone()
                if not row:
                    return None
                cols = [d[0] for d in cur.description]
                return dict(zip(cols, row))

    def expire_stuck_pending(self, *, exchange_id: int, account_id: int, timeout_sec: int = 60) -> int:
        """
        Expire PENDING_SUBMIT / PENDING placeholders older than timeout_sec.
        """
        query = """
            UPDATE orders
            SET status = 'EXPIRED', updated_at = NOW()
            WHERE exchange_id = %s
              AND account_id = %s
              AND status IN ('PENDING', 'PENDING_SUBMIT')
              AND updated_at < NOW() - (INTERVAL '1 second' * %s)
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(exchange_id), int(account_id), int(timeout_sec)))
                n = cur.rowcount
            conn.commit()
        return int(n or 0)

    def upsert_orders(self, orders: list[dict]) -> None:
        """
        Upsert order snapshots.
        Status, qty, filled_qty are already resolved by OrderAggregate (FSM).
        """
        if not orders:
            return


        rows: list[dict] = []
        for o in orders:
            rows.append(
                {
                    "exchange_id": int(o["exchange_id"]),
                    "account_id": int(o["account_id"]),
                    "symbol_id": int(o["symbol_id"]),
                    "order_id": str(o["order_id"]),
                    "client_order_id": o.get("client_order_id"),
                    "side": str(o.get("side") or ""),
                    "type": str(o.get("type") or ""),
                    "reduce_only": bool(o.get("reduce_only") or False),
                    "price": o.get("price"),
                    "qty": float(o.get("qty") or 0.0),
                    "filled_qty": float(o.get("filled_qty") or 0.0),
                    "status": str(o.get("status") or "NEW"),
                    "strategy_id": o.get("strategy_id"),
                    "pos_uid": o.get("pos_uid"),
                    "ts_ms": int(o.get("ts_ms") or 0),
                    "raw_json": self._to_jsonb(o.get("raw") or o.get("raw_json") or {}),

                }
            )

        # Split to avoid duplicate client_order_id (we have partial unique index on it)
        def _has_cid(o: dict) -> bool:
            cid = o.get("client_order_id")
            if cid is None:
                return False
            cid = str(cid).strip()
            return cid != ""

        with_cid = [r for r in rows if _has_cid(r)]
        no_cid = [r for r in rows if not _has_cid(r)]

        if with_cid:
            query_cid = """
                INSERT INTO orders (
                    exchange_id,
                    account_id, 
                    order_id,
                    client_order_id,
                    symbol_id,
                    side,
                    type,
                    reduce_only,
                    price,
                    qty,
                    filled_qty,
                    status, 
                    strategy_id,
                    pos_uid,
                    ts_ms,
                    raw_json,
                    updated_at
                )
                VALUES (
                    %(exchange_id)s,
                    %(account_id)s,
                    %(order_id)s,
                    %(client_order_id)s,
                    %(symbol_id)s,
                    %(side)s,
                    %(type)s,
                    %(reduce_only)s,
                    %(price)s, 
                    %(qty)s,
                    %(filled_qty)s,
                    %(status)s,
                    %(strategy_id)s,
                    %(pos_uid)s,
                    %(ts_ms)s,
                    %(raw_json)s,
                    NOW()
                )
                ON CONFLICT (exchange_id, account_id, client_order_id)
                WHERE client_order_id IS NOT NULL AND client_order_id <> ''
                DO UPDATE SET
                    order_id = EXCLUDED.order_id,
                    symbol_id = EXCLUDED.symbol_id,
                    side = EXCLUDED.side,
                    type = EXCLUDED.type,
                    reduce_only = EXCLUDED.reduce_only, 
                    price = EXCLUDED.price,
                    qty = EXCLUDED.qty,
                    filled_qty = EXCLUDED.filled_qty,
                    status = EXCLUDED.status,
                    strategy_id = COALESCE(orders.strategy_id, EXCLUDED.strategy_id), 
                    pos_uid = COALESCE(orders.pos_uid, EXCLUDED.pos_uid),
                    ts_ms = GREATEST(COALESCE(orders.ts_ms, 0), COALESCE(EXCLUDED.ts_ms, 0)),
                    raw_json = COALESCE(EXCLUDED.raw_json, orders.raw_json),
                    updated_at = NOW()
            """
            self._exec_many(query_cid, with_cid)

        if no_cid:
            query_oid = """
                INSERT INTO orders (
                    exchange_id,
                    account_id, 
                    order_id,
                    client_order_id,
                    symbol_id,
                    side,
                    type,
                    reduce_only,
                    price,
                    qty,
                    filled_qty,
                    status, 
                    strategy_id,
                    pos_uid,
                    ts_ms,
                    raw_json,
                    updated_at
                )
                VALUES (
                    %(exchange_id)s,
                    %(account_id)s,
                    %(order_id)s,
                    %(client_order_id)s,
                    %(symbol_id)s,
                    %(side)s,
                    %(type)s,
                    %(reduce_only)s,
                    %(price)s, 
                    %(qty)s,
                    %(filled_qty)s,
                    %(status)s,
                    %(strategy_id)s,
                    %(pos_uid)s,
                    %(ts_ms)s,
                    %(raw_json)s,
                    NOW()
                )
                ON CONFLICT (exchange_id, account_id, order_id)
                DO UPDATE SET
                    client_order_id = COALESCE(EXCLUDED.client_order_id, orders.client_order_id),
                    symbol_id = EXCLUDED.symbol_id,
                    side = EXCLUDED.side,
                    type = EXCLUDED.type,
                    reduce_only = EXCLUDED.reduce_only, 
                    price = EXCLUDED.price,
                    qty = EXCLUDED.qty,
                    filled_qty = EXCLUDED.filled_qty,
                    status = EXCLUDED.status,
                    strategy_id = COALESCE(orders.strategy_id, EXCLUDED.strategy_id), 
                    pos_uid = COALESCE(orders.pos_uid, EXCLUDED.pos_uid),
                    ts_ms = GREATEST(COALESCE(orders.ts_ms, 0), COALESCE(EXCLUDED.ts_ms, 0)),
                    raw_json = COALESCE(EXCLUDED.raw_json, orders.raw_json),
                    updated_at = NOW()
            """
            self._exec_many(query_oid, no_cid)

    def upsert_algo_orders(self, rows: list[dict]) -> None:
        """Upsert algo order snapshots (e.g. Binance /fapi/v1/openAlgoOrders).

        Expected keys in each row (best-effort; extra keys ignored):
          exchange_id, account_id, client_algo_id, algo_id, symbol,
          side, position_side, type, quantity, trigger_price, working_type,
          status, strategy_id, pos_uid, raw_json
        """
        if not rows:
            return

        # psycopg3 doesn't adapt plain dict -> jsonb automatically in executemany();
        # wrap payloads explicitly.
        prepared: list[dict] = []
        for r in rows:
            d = dict(r)
            if "raw_json" in d and d["raw_json"] is not None and not isinstance(d["raw_json"], Jsonb):
                if isinstance(d["raw_json"], (dict, list)):
                    d["raw_json"] = Jsonb(d["raw_json"])
            prepared.append(d)

        query = """
            INSERT INTO algo_orders (
                exchange_id,
                account_id,
                client_algo_id,
                algo_id,
                symbol,
                side,
                position_side,
                type,
                quantity,
                trigger_price,
                working_type,
                status,
                strategy_id,
                pos_uid,
                raw_json,
                updated_at
            )
            VALUES (
                %(exchange_id)s,
                %(account_id)s,
                %(client_algo_id)s,
                %(algo_id)s,
                %(symbol)s,
                %(side)s,
                %(position_side)s,
                %(type)s,
                %(quantity)s,
                %(trigger_price)s,
                %(working_type)s,
                %(status)s,
                %(strategy_id)s,
                %(pos_uid)s,
                %(raw_json)s,
                NOW()
            )
            ON CONFLICT (exchange_id, account_id, client_algo_id)
            DO UPDATE SET
                algo_id        = COALESCE(EXCLUDED.algo_id, algo_orders.algo_id),
                symbol         = COALESCE(EXCLUDED.symbol, algo_orders.symbol),
                side           = COALESCE(EXCLUDED.side, algo_orders.side),
                position_side  = COALESCE(EXCLUDED.position_side, algo_orders.position_side),
                type           = COALESCE(EXCLUDED.type, algo_orders.type),
                quantity       = COALESCE(EXCLUDED.quantity, algo_orders.quantity),
                trigger_price  = COALESCE(EXCLUDED.trigger_price, algo_orders.trigger_price),
                working_type   = COALESCE(EXCLUDED.working_type, algo_orders.working_type),
                status         = COALESCE(EXCLUDED.status, algo_orders.status),
                strategy_id    = COALESCE(EXCLUDED.strategy_id, algo_orders.strategy_id),
                pos_uid        = COALESCE(EXCLUDED.pos_uid, algo_orders.pos_uid),
                raw_json       = COALESCE(EXCLUDED.raw_json, algo_orders.raw_json),
                updated_at     = NOW()
        """
        self._exec_many(query, prepared)

    def get_algo_order(self, *, exchange_id: int, account_id: int, client_algo_id: str) -> dict | None:
        """Fetch algo order row by PK."""
        sql = """
            SELECT
              exchange_id, account_id, client_algo_id,
              algo_id, symbol, side, position_side, type,
              quantity, trigger_price, working_type,
              status, strategy_id, pos_uid,
              created_at, updated_at, raw_json
            FROM algo_orders
            WHERE exchange_id=%(exchange_id)s
              AND account_id=%(account_id)s
              AND client_algo_id=%(client_algo_id)s
            LIMIT 1
        """
        return self.fetch_one(sql, {
            'exchange_id': int(exchange_id),
            'account_id': int(account_id),
            'client_algo_id': str(client_algo_id),
        })

    def set_algo_order_status(
        self,
        *,
        exchange_id: int,
        account_id: int,
        client_algo_id: str,
        status: str,
        raw_json: dict | None = None,
    ) -> int:
        """Update algo order status by client_algo_id (best-effort)."""
        if raw_json is not None and not isinstance(raw_json, Jsonb):
            if isinstance(raw_json, (dict, list)):
                raw_json = Jsonb(raw_json)

        query = """
            UPDATE algo_orders
            SET status = %s,
                raw_json = COALESCE(%s::jsonb, raw_json),
                updated_at = NOW()
            WHERE exchange_id = %s
              AND account_id = %s
              AND client_algo_id = %s
        """
        return self.execute(
            query,
            (
                str(status),
                raw_json,
                int(exchange_id),
                int(account_id),
                str(client_algo_id),
            ),
        )

    def sync_open_algo_orders_not_found(
        self,
        *,
        exchange_id: int,
        account_id: int,
        active_client_algo_ids: list[str],
        prefix: str = "TL_",
        not_found_status: str = "CANCELED",
        cancel_reason: str = "sync_not_in_openAlgoOrders",
        cancel_source: str = "sync_openAlgoOrders",
    ) -> int:
        """Mark OPEN algo orders as *not found* (defaults to CANCELED) when they are no longer present on the exchange.

        This is a best-effort reconciliation helper. Caller should pass the current list of openAlgoOrders
        client IDs from the exchange. When an OPEN record is marked, we also annotate raw_json with
        cancel_reason / cancel_source so it is clear this status came from reconciliation (vs bot-driven cancel).
        """

        active = [str(x).strip() for x in (active_client_algo_ids or []) if str(x).strip()]

        meta = {"cancel_reason": str(cancel_reason), "cancel_source": str(cancel_source)}
        meta_jsonb = Jsonb(meta)

        # If exchange returned nothing, mark all OPEN prefixed orders as not-found.
        if not active:
            query = """
                UPDATE algo_orders
                SET status = %s,
                    raw_json = raw_json || %s,
                    updated_at = NOW()
                WHERE exchange_id = %s
                  AND account_id = %s
                  AND status = 'OPEN'
                  AND client_algo_id LIKE %s
              AND client_algo_id NOT LIKE '%_HEDGE_TRL'
              AND client_algo_id NOT LIKE '%_HEDGE_SL'
                  AND client_algo_id NOT LIKE '%_HEDGE_TRL'
                  AND client_algo_id NOT LIKE '%_HEDGE_SL'
            """
            return int(
                self.execute(
                    query,
                    (
                        str(not_found_status),
                        meta_jsonb,
                        int(exchange_id),
                        int(account_id),
                        str(prefix) + "%",
                    ),
                )
                or 0
            )

        query = """
            UPDATE algo_orders
            SET status = %s,
                raw_json = raw_json || %s,
                updated_at = NOW()
            WHERE exchange_id = %s
              AND account_id = %s
              AND status = 'OPEN'
              AND client_algo_id LIKE %s
              AND NOT (client_algo_id = ANY(%s))
        """
        return int(
            self.execute(
                query,
                (
                    str(not_found_status),
                    meta_jsonb,
                    int(exchange_id),
                    int(account_id),
                    str(prefix) + "%",
                    active,
                ),
            )
            or 0
        )

    def set_order_status(self, *, exchange_id: int, account_id: int, order_id: str, status: str) -> int:
        query = """
            UPDATE orders
            SET status = %s,
                updated_at = NOW()
            WHERE exchange_id = %s
              AND account_id = %s
              AND order_id = %s
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (str(status), int(exchange_id), int(account_id), str(order_id)))
                n = cur.rowcount
            conn.commit()
        return int(n or 0)

    def upsert_order_placeholder(self, row: dict) -> None:
        """
        row keys expected:
          exchange_id, account_id, order_id, symbol_id,
          strategy_id, pos_uid, client_order_id,
          side, type, reduce_only, price, qty,
          filled_qty, status, source, ts_ms,
          created_at, updated_at, raw_json
        """
        exchange_id = int(row["exchange_id"])
        account_id = int(row["account_id"])
        order_id = str(row["order_id"])
        symbol_id = int(row["symbol_id"])

        strategy_id = str(row.get("strategy_id") or "unknown")
        pos_uid = row.get("pos_uid")
        client_order_id = row.get("client_order_id")

        side = row.get("side")
        order_type = row.get("type")
        reduce_only = bool(row.get("reduce_only", False))
        price = row.get("price", None)
        qty = row.get("qty", None)
        filled_qty = row.get("filled_qty", 0.0)
        status = row.get("status", "PENDING")
        source = row.get("source", "oms")
        ts_ms = int(row.get("ts_ms") or int(time.time() * 1000))
        raw_json = row.get("raw_json")
        cid = ""
        if client_order_id is not None:
            cid = str(client_order_id).strip()

        if cid:
            query = """
                INSERT INTO orders (
                    exchange_id,
                    account_id,
                    order_id,
                    symbol_id,
                    strategy_id,
                    pos_uid,
                    client_order_id,
                    side,
                    type,
                    reduce_only,
                    price,
                    qty,
                    filled_qty, 
                    status,
                    source,
                    ts_ms,
                    created_at,
                    updated_at,
                    raw_json
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s,
                    %s,
                    %s,
                    COALESCE(%s, NOW()),
                    COALESCE(%s, NOW()),
                    %s
                )
                ON CONFLICT (exchange_id, account_id, client_order_id)
                WHERE client_order_id IS NOT NULL AND client_order_id <> ''
                DO UPDATE SET
                    order_id = EXCLUDED.order_id,
                    symbol_id = EXCLUDED.symbol_id,
                    strategy_id = EXCLUDED.strategy_id,
                    pos_uid = EXCLUDED.pos_uid,
                    side = EXCLUDED.side,
                    type = EXCLUDED.type,
                    reduce_only = EXCLUDED.reduce_only,
                    price = EXCLUDED.price,
                    qty = EXCLUDED.qty,
                    filled_qty = EXCLUDED.filled_qty,
                    status = EXCLUDED.status, 
                    source = EXCLUDED.source,
                    ts_ms = EXCLUDED.ts_ms,
                    updated_at = NOW(),
                    raw_json = COALESCE(EXCLUDED.raw_json, orders.raw_json)
            """
        else:
            query = """
                INSERT INTO orders (
                    exchange_id,
                    account_id,
                    order_id,
                    symbol_id,
                    strategy_id,
                    pos_uid,
                    client_order_id,
                    side,
                    type,
                    reduce_only,
                    price,
                    qty,
                    filled_qty, 
                    status,
                    source,
                    ts_ms,
                    created_at,
                    updated_at,
                    raw_json
                )
                VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s,
                    %s,
                    %s,
                    COALESCE(%s, NOW()),
                    COALESCE(%s, NOW()),
                    %s
                )
                ON CONFLICT (exchange_id, account_id, order_id)
                DO UPDATE SET
                    symbol_id = EXCLUDED.symbol_id,
                    strategy_id = EXCLUDED.strategy_id,
                    pos_uid = EXCLUDED.pos_uid,
                    client_order_id = EXCLUDED.client_order_id,
                    side = EXCLUDED.side,
                    type = EXCLUDED.type,
                    reduce_only = EXCLUDED.reduce_only,
                    price = EXCLUDED.price,
                    qty = EXCLUDED.qty,
                    filled_qty = EXCLUDED.filled_qty,
                    status = EXCLUDED.status, 
                    source = EXCLUDED.source,
                    ts_ms = EXCLUDED.ts_ms,
                    updated_at = NOW(),
                    raw_json = COALESCE(EXCLUDED.raw_json, orders.raw_json)
            """


        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query,
                    (
                        exchange_id,
                        account_id,
                        order_id,
                        symbol_id,
                        strategy_id,
                        pos_uid,
                        client_order_id,
                        side,
                        order_type,
                        reduce_only,
                        price,
                        qty,
                        float(filled_qty or 0.0),
                        status,
                        source,
                        ts_ms,
                        row.get("created_at"),
                        row.get("updated_at"),
                        raw_json,
                    ),
                )
            conn.commit()

    def append_order_events(self, rows: Iterable[dict]) -> None:
        """Append immutable order events (idempotent)."""
        rows = list(rows or [])
        if not rows:
            return

        query = """
            INSERT INTO order_events (
                exchange_id, account_id, order_id, symbol_id,
                client_order_id, status, side, type, reduce_only,
                price, qty, filled_qty,
                source, ts_ms, recv_ts, raw_json
            )
            VALUES (
                %(exchange_id)s, %(account_id)s, %(order_id)s, %(symbol_id)s,
                %(client_order_id)s, %(status)s, %(side)s, %(type)s, %(reduce_only)s,
                %(price)s, %(qty)s, %(filled_qty)s,
                %(source)s, %(ts_ms)s, %(recv_ts)s, %(raw_json)s
            )
            ON CONFLICT DO NOTHING
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, rows)
            conn.commit()

    def fetch_order_events(
        self,
        *,
        exchange_id: int,
        account_id: int,
        since_ts_ms: int | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """
        Fetch order_events ordered by ts_ms ASC.
        Used for OMS rebuild.
        """
        sql = """
            SELECT exchange_id,
                   account_id,
                   order_id,
                   symbol_id,
                   client_order_id,
                   status,
                   side,
                   type,
                   reduce_only,
                   price,
                   qty,
                   filled_qty,
                   source,
                   ts_ms,
                   recv_ts,
                   raw_json
            FROM order_events
            WHERE exchange_id = %(exchange_id)s
              AND account_id = %(account_id)s
        """

        params = {"exchange_id": int(exchange_id), "account_id": int(account_id)}

        if since_ts_ms is not None:
            sql += " AND ts_ms >= %(since_ts_ms)s"
            params["since_ts_ms"] = int(since_ts_ms)

        sql += " ORDER BY ts_ms ASC"

        if limit is not None:
            sql += " LIMIT %(limit)s"
            params["limit"] = int(limit)

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                cols = [c.name for c in cur.description]

        return [dict(zip(cols, r)) for r in rows]

    def list_non_terminal_orders(self, *, exchange_id: int, account_id: int, symbol_ids=None) -> list[dict]:
        base = """
            SELECT exchange_id,
                   account_id,
                   order_id,
                   symbol_id,
                   strategy_id,
                   pos_uid,
                   client_order_id,
                   side,
                   type,
                   reduce_only,
                   price,
                   qty,
                   filled_qty,
                   status,
                   created_at,
                   updated_at,
                   source,
                   ts_ms,
                   raw_json
            FROM orders
            WHERE exchange_id = %s
              AND account_id = %s
              AND status NOT IN ('FILLED', 'CANCELED', 'REJECTED', 'EXPIRED')
        """
        params = [int(exchange_id), int(account_id)]
        if symbol_ids:
            base += " AND symbol_id = ANY(%s)"
            params.append(list(map(int, symbol_ids)))
        base += " ORDER BY updated_at DESC LIMIT 500"

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(base, tuple(params))
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in rows]

    def get_order_by_client_order_id(
        self,
        *,
        exchange_id: int,
        account_id: int,
        client_order_id: str,
        prefer_placeholder: bool = True,
    ) -> dict | None:
        if not client_order_id:
            return None

        q_ph = """
            SELECT *
            FROM orders
            WHERE exchange_id = %s
              AND account_id = %s
              AND client_order_id = %s
              AND order_id LIKE 'PH::%%'
            ORDER BY updated_at DESC
            LIMIT 1
        """

        q_any = """
            SELECT *
            FROM orders
            WHERE exchange_id = %s
              AND account_id = %s
              AND client_order_id = %s
            ORDER BY updated_at DESC
            LIMIT 1
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                if prefer_placeholder:
                    cur.execute(q_ph, (int(exchange_id), int(account_id), str(client_order_id)))
                    row = cur.fetchone()
                    if row:
                        cols = [d[0] for d in cur.description]
                        return dict(zip(cols, row))

                cur.execute(q_any, (int(exchange_id), int(account_id), str(client_order_id)))
                row = cur.fetchone()
                if not row:
                    return None
                cols = [d[0] for d in cur.description]
                return dict(zip(cols, row))

    def fetch_orders_metrics_window(self, *, exchange_id: int, account_id: int, since_ts_ms: int) -> list[dict]:
        query = """
            SELECT client_order_id, order_id, status, source, ts_ms
            FROM orders
            WHERE exchange_id = %s
              AND account_id = %s
              AND ts_ms >= %s
              AND client_order_id IS NOT NULL
            ORDER BY ts_ms ASC
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(exchange_id), int(account_id), int(since_ts_ms)))
                rows = cur.fetchall()

        out: list[dict] = []
        for r in rows:
            out.append(
                {
                    "client_order_id": r[0],
                    "order_id": r[1],
                    "status": r[2],
                    "source": r[3],
                    "ts_ms": r[4],
                }
            )
        return out

    def upsert_order_event(self, ev: Any) -> None:
        # side normalization
        side = ev.side
        if hasattr(side, "name"):
            side = side.name
        elif isinstance(side, str):
            side = side.upper()
        else:
            side = None

        status = ev.status.name if hasattr(ev.status, "name") else ev.status
        otype = ev.type.name if hasattr(ev.type, "name") else ev.type

        raw = getattr(ev, "raw_json", None) or getattr(ev, "payload", None)
        if raw is None:
            raw = {}

        if isinstance(raw, str):
            raw_json = raw
        else:
            try:
                raw_json = json.dumps(raw, ensure_ascii=False, default=str)
            except Exception:
                raw_json = "{}"

        recv_ts = getattr(ev, "recv_ts", None)
        if isinstance(recv_ts, (int, float)):
            recv_ts = datetime.fromtimestamp(recv_ts, tz=timezone.utc)
        elif recv_ts is None:
            recv_ts = _utcnow()

        if (
            ev.exchange_id is None
            or ev.account_id is None
            or ev.symbol_id is None
            or ev.order_id is None
            or ev.ts_ms is None
        ):
            return

        strategy_id = getattr(ev, "strategy_id", None) or "unknown"
        pos_uid = getattr(ev, "pos_uid", None)

        row = {
            "exchange_id": int(ev.exchange_id),
            "account_id": int(ev.account_id),
            "order_id": str(ev.order_id),
            "symbol_id": int(ev.symbol_id),

            "client_order_id": getattr(ev, "client_order_id", None),
            "status": str(status or ""),
            "side": side,
            "type": otype,
            "reduce_only": bool(getattr(ev, "reduce_only", False)),
            "price": getattr(ev, "price", None),
            "qty": getattr(ev, "qty", None),
            "filled_qty": getattr(ev, "filled_qty", None),

            "strategy_id": str(strategy_id),
            "pos_uid": str(pos_uid) if pos_uid else None,

            "source": getattr(ev, "source", None) or "ws_user",
            "ts_ms": int(ev.ts_ms),

            "recv_ts": recv_ts,
            "raw_json": raw_json,
        }

        query = """
            INSERT INTO order_events (
                exchange_id, account_id, order_id, symbol_id,
                client_order_id, status, side, type,
                reduce_only, price, qty, filled_qty,
                strategy_id, pos_uid,
                source, ts_ms, recv_ts, raw_json
            )
            VALUES (
                %(exchange_id)s, %(account_id)s, %(order_id)s, %(symbol_id)s,
                %(client_order_id)s, %(status)s, %(side)s, %(type)s,
                %(reduce_only)s, %(price)s, %(qty)s, %(filled_qty)s,
                %(strategy_id)s, %(pos_uid)s,
                %(source)s, %(ts_ms)s, %(recv_ts)s, %(raw_json)s
            )
            ON CONFLICT (exchange_id, account_id, order_id, ts_ms, status, filled_qty)
            DO NOTHING
        """
        self._exec_many(query, [row])

    def exists_order_event(self, *, exchange_id: int, account_id: int, order_id: str, status: str) -> bool:
        sql = """
            SELECT 1
            FROM order_events
            WHERE exchange_id = %s
              AND account_id = %s
              AND order_id = %s
              AND status = %s
            LIMIT 1
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(exchange_id), int(account_id), str(order_id), str(status)))
                return cur.fetchone() is not None

    def get_order_by_id(self, *, exchange_id: int, account_id: int, order_id: str) -> dict | None:
        query = """
            SELECT *
            FROM orders
            WHERE exchange_id = %s
              AND account_id = %s
              AND order_id = %s
            LIMIT 1
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(exchange_id), int(account_id), str(order_id)))
                row = cur.fetchone()
                if row:
                    cols = [d[0] for d in cur.description]
                    return dict(zip(cols, row))
        return None

