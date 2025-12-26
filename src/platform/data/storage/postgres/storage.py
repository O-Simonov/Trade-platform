# src/platform/data/storage/postgres/storage.py
from __future__ import annotations

import json
import logging
from typing import Dict, Iterable, Sequence, Any
from datetime import datetime, timezone

from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


class PostgreSQLStorage:
    """
    PostgreSQL storage — OMS / Orders / Trades / Positions / Equity
    """

    def __init__(self, pool: ConnectionPool):
        self.pool = pool

    # ======================================================================
    # HELPERS
    # ======================================================================

    def _exec_many(self, query: str, rows: list[dict]) -> int:
        if not rows:
            return 0
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, rows)
            conn.commit()
        return len(rows)

    def _exec_one(self, query: str, params: tuple) -> Any:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchone()

    def _fetchone(self, query: str, params: tuple):
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                row = cur.fetchone()
                if not row:
                    return None
                cols = [d[0] for d in cur.description]
                return dict(zip(cols, row))

    # ======================================================================
    # POSITIONS
    # ======================================================================

    def upsert_position(self, position: Any) -> int:
        return self.upsert_positions([position])

    def upsert_positions(self, positions: Sequence[Any]) -> int:
        if not positions:
            return 0

        query = """
        INSERT INTO positions (
            exchange_id,
            account_id,
            symbol_id,
            side,
            qty,
            entry_price,
            realized_pnl,
            unrealized_pnl,
            exchange_realized_pnl,
            fees,
            mark_price,
            last_trade_id,
            status,
            last_ts,
            updated_at,
            source
        )
        VALUES (
            %(exchange_id)s,
            %(account_id)s,
            %(symbol_id)s,
            %(side)s,
            %(qty)s,
            %(entry_price)s,
            %(realized_pnl)s,
            %(unrealized_pnl)s,
            %(exchange_realized_pnl)s,
            %(fees)s,
            %(mark_price)s,
            %(last_trade_id)s,
            %(status)s,
            %(last_ts)s,
            %(updated_at)s,
            %(source)s
        )
        ON CONFLICT (exchange_id, account_id, symbol_id)
        DO UPDATE SET
            side = EXCLUDED.side,
            qty = EXCLUDED.qty,
            entry_price = EXCLUDED.entry_price,
            realized_pnl = EXCLUDED.realized_pnl,
            unrealized_pnl = EXCLUDED.unrealized_pnl,
            exchange_realized_pnl = EXCLUDED.exchange_realized_pnl,
            fees = EXCLUDED.fees,
            mark_price = EXCLUDED.mark_price,
            last_trade_id = EXCLUDED.last_trade_id,
            status = EXCLUDED.status,
            last_ts = EXCLUDED.last_ts,
            updated_at = EXCLUDED.updated_at,
            source = EXCLUDED.source;
        """

        now = _utcnow()

        def get(obj: Any, key: str, default=None):
            return obj.get(key, default) if isinstance(obj, dict) else getattr(obj, key, default)

        rows = []
        for p in positions:
            rows.append({
                "exchange_id": int(get(p, "exchange_id")),
                "account_id": int(get(p, "account_id")),
                "symbol_id": int(get(p, "symbol_id")),
                "side": get(p, "side"),
                "qty": float(get(p, "qty", 0.0) or 0.0),
                "entry_price": float(get(p, "entry_price", 0.0) or 0.0),
                "realized_pnl": float(get(p, "realized_pnl", 0.0) or 0.0),
                "unrealized_pnl": float(get(p, "unrealized_pnl", 0.0) or 0.0),
                "exchange_realized_pnl": float(get(p, "exchange_realized_pnl", 0.0) or 0.0),
                "fees": float(get(p, "fees", 0.0) or 0.0),
                "mark_price": float(get(p, "mark_price", 0.0) or 0.0),
                "last_trade_id": get(p, "last_trade_id"),
                "status": get(p, "status"),
                "last_ts": get(p, "last_ts"),
                "updated_at": get(p, "updated_at") or now,
                "source": get(p, "source") or "snapshot",
            })

        return self._exec_many(query, rows)

    # ======================================================================
    # EQUITY / BALANCE
    # ======================================================================

    def insert_account_balance_snapshots(self, rows: Iterable[dict]) -> int:
        rows = list(rows)
        if not rows:
            return 0

        query = """
        INSERT INTO account_balance_snapshots (
            exchange_id,
            account_id,
            ts,
            wallet_balance,
            equity,
            available_balance,
            margin_used,
            unrealized_pnl,
            source
        )
        VALUES (
            %(exchange_id)s,
            %(account_id)s,
            %(ts)s,
            %(wallet_balance)s,
            %(equity)s,
            %(available_balance)s,
            %(margin_used)s,
            %(unrealized_pnl)s,
            %(source)s
        )
        """

        return self._exec_many(query, rows)

    # ======================================================================
    # ORDERS / TRADES
    # ======================================================================

    def upsert_orders(self, orders: list[dict]) -> None:
        if not orders:
            return

        query = """
        INSERT INTO orders (
            exchange_id, account_id, order_id, client_order_id, symbol_id,
            side, type, status, qty, price, reduce_only, ts_ms, raw_json, updated_at
        )
        VALUES (
            %(exchange_id)s, %(account_id)s, %(order_id)s, %(client_order_id)s, %(symbol_id)s,
            %(side)s, %(type)s, %(status)s, %(qty)s, %(price)s, %(reduce_only)s,
            %(ts_ms)s, %(raw_json)s, NOW()
        )
        ON CONFLICT (exchange_id, account_id, order_id)
        DO UPDATE SET
            status = EXCLUDED.status,
            qty = EXCLUDED.qty,
            price = EXCLUDED.price,
            raw_json = EXCLUDED.raw_json,
            updated_at = NOW();
        """

        rows = []
        for o in orders:
            rows.append({
                "exchange_id": int(o["exchange_id"]),
                "account_id": int(o["account_id"]),
                "order_id": str(o.get("order_id") or ""),
                "client_order_id": str(o.get("client_order_id") or ""),
                "symbol_id": int(o["symbol_id"]),
                "side": str(o.get("side") or ""),
                "type": str(o.get("type") or ""),
                "status": str(o.get("status") or ""),
                "qty": float(o.get("qty") or 0.0),
                "price": float(o.get("price") or 0.0),
                "reduce_only": bool(o.get("reduce_only") or False),
                "ts_ms": int(o.get("ts_ms") or 0),
                "raw_json": json.dumps(o, ensure_ascii=False),
            })

        self._exec_many(query, rows)

    def upsert_trades(self, trades: list[dict]) -> None:
        if not trades:
            return

        query = """
        INSERT INTO trades (
            exchange_id, account_id, trade_id, order_id, symbol_id,
            strategy_id, pos_uid, side, price, qty, fee,
            realized_pnl, ts, source, raw_json
        )
        VALUES (
            %(exchange_id)s, %(account_id)s, %(trade_id)s, %(order_id)s, %(symbol_id)s,
            %(strategy_id)s, %(pos_uid)s, %(side)s, %(price)s, %(qty)s, %(fee)s,
            %(realized_pnl)s, %(ts)s, %(source)s, %(raw_json)s
        )
        ON CONFLICT (exchange_id, account_id, trade_id)
        DO UPDATE SET
            realized_pnl = EXCLUDED.realized_pnl,
            ts = EXCLUDED.ts,
            raw_json = EXCLUDED.raw_json;
        """

        rows = []
        for t in trades:
            rows.append({
                "exchange_id": int(t["exchange_id"]),
                "account_id": int(t["account_id"]),
                "trade_id": str(t.get("trade_id") or ""),
                "order_id": str(t.get("order_id") or ""),
                "symbol_id": int(t["symbol_id"]),
                "strategy_id": str(t.get("strategy_id") or "unknown"),
                "pos_uid": str(t.get("pos_uid") or ""),
                "side": str(t.get("side") or ""),
                "price": float(t.get("price") or 0.0),
                "qty": float(t.get("qty") or 0.0),
                "fee": float(t.get("fee") or 0.0),
                "realized_pnl": float(t.get("realized_pnl") or 0.0),
                "ts": t["ts"],
                "source": str(t.get("source") or "ws"),
                "raw_json": json.dumps(t, ensure_ascii=False),
            })

        self._exec_many(query, rows)

    # ======================================================================
    # STATS
    # ======================================================================

    def get_today_realized_pnl(self, exchange_id: int, account_id: int) -> float:
        query = """
        SELECT COALESCE(SUM(realized_pnl), 0)
        FROM trades
        WHERE exchange_id = %s
          AND account_id = %s
          AND ts >= date_trunc('day', now())
        """
        row = self._exec_one(query, (exchange_id, account_id))
        return float(row[0] or 0.0)

    # ======================================================================
    # IDS / REGISTRY
    # ======================================================================

    def ensure_exchange_account_symbol(
        self,
        *,
        exchange: str,
        account: str,
        symbols: list[str],
    ) -> dict:
        """
        Ensures exchange/account/symbols exist in DB and returns ids mapping.

        Real schema:
          exchanges(exchange_id, name)
          accounts(account_id, exchange_id, account_name, role, is_active)
          symbols(symbol_id, exchange_id, symbol)
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:

                # --- exchange ---
                cur.execute(
                    """
                    INSERT INTO exchanges (name)
                    VALUES (%s)
                    ON CONFLICT (name)
                    DO UPDATE SET name = EXCLUDED.name
                    RETURNING exchange_id
                    """,
                    (exchange,),
                )
                exchange_id = cur.fetchone()[0]

                # --- account ---
                cur.execute(
                    """
                    INSERT INTO accounts (exchange_id, account_name)
                    VALUES (%s, %s)
                    ON CONFLICT (exchange_id, account_name)
                    DO UPDATE SET account_name = EXCLUDED.account_name
                    RETURNING account_id
                    """,
                    (exchange_id, account),
                )
                account_id = cur.fetchone()[0]

                # --- symbols ---
                symbol_ids: dict[str, int] = {}
                for sym in symbols:
                    cur.execute(
                        """
                        INSERT INTO symbols (exchange_id, symbol)
                        VALUES (%s, %s)
                        ON CONFLICT (exchange_id, symbol)
                        DO UPDATE SET symbol = EXCLUDED.symbol
                        RETURNING symbol_id
                        """,
                        (exchange_id, sym),
                    )
                    symbol_ids[sym] = cur.fetchone()[0]

            conn.commit()

        ids = {
            "_exchange_id": exchange_id,
            "_account_id": account_id,
        }
        ids.update(symbol_ids)
        return ids
