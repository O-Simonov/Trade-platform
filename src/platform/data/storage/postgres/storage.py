# src/platform/data/storage/postgres/storage.py
from __future__ import annotations

import json
import time
import logging
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


class PostgreSQLStorage:

    """
    PostgreSQL storage — registry, market state, OMS helpers, retention, exchangeInfo,
    positions, balances snapshots, orders, trades, stats.
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

    def _exec_one(self, query: str, params: tuple):
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.fetchone()

    # ======================================================================
    # IDS / REGISTRY (used by run_instances.py)
    # ======================================================================

    def ensure_exchange_account_symbol(
        self,
        *,
        exchange: str,
        account: str,
        symbols: list[str],
    ) -> dict:
        r"""
        Ensures exchange/account/symbols exist in DB and returns ids mapping.

        Real schema (confirmed by your \d):
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

        ids = {"_exchange_id": exchange_id, "_account_id": account_id}
        ids.update(symbol_ids)
        return ids

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
            exchange_id, account_id, symbol_id,
            side, qty, entry_price,
            realized_pnl, unrealized_pnl, exchange_realized_pnl,
            fees, mark_price,
            last_trade_id, status, last_ts,
            updated_at, source
        )
        VALUES (
            %(exchange_id)s, %(account_id)s, %(symbol_id)s,
            %(side)s, %(qty)s, %(entry_price)s,
            %(realized_pnl)s, %(unrealized_pnl)s, %(exchange_realized_pnl)s,
            %(fees)s, %(mark_price)s,
            %(last_trade_id)s, %(status)s, %(last_ts)s,
            %(updated_at)s, %(source)s
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

        rows: list[dict] = []
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
    # BALANCE / EQUITY SNAPSHOTS
    # ======================================================================

    def insert_account_balance_snapshots(self, rows: Iterable[dict]) -> int:
        rows = list(rows)
        if not rows:
            return 0

        query = """
        INSERT INTO account_balance_snapshots (
            exchange_id, account_id, ts,
            wallet_balance, equity, available_balance, margin_used,
            unrealized_pnl, source
        )
        VALUES (
            %(exchange_id)s, %(account_id)s, %(ts)s,
            %(wallet_balance)s, %(equity)s, %(available_balance)s, %(margin_used)s,
            %(unrealized_pnl)s, %(source)s
        )
        """
        return self._exec_many(query, rows)

    # ======================================================================
    # MARKET STATE (backward compatible kwargs)
    # ======================================================================

    @staticmethod
    def _resolve_ids(exchange, account, exchange_id, account_id):
        return exchange_id or exchange, account_id or account

    def get_latest_positions(self, *, exchange=None, account=None, exchange_id=None, account_id=None) -> dict:
        ex_id = self._resolve_exchange_id(exchange, exchange_id)
        acc_id = self._resolve_account_id(ex_id, account, account_id)
        if ex_id is None or acc_id is None:
            return {}

        query = """
                SELECT s.symbol, \
                       p.symbol_id, \
                       p.qty, \
                       p.entry_price, \
                       p.unrealized_pnl, \
                       p.mark_price, \
                       p.status, \
                       p.updated_at
                FROM positions p
                         JOIN symbols s ON s.symbol_id = p.symbol_id
                WHERE p.exchange_id = %s \
                  AND p.account_id = %s \
                """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (ex_id, acc_id))
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]

        out = {}
        for row in rows:
            r = dict(zip(cols, row))
            sym = r.pop("symbol")
            out[sym] = r
        return out

    def get_latest_balances(self, *, exchange=None, account=None, exchange_id=None, account_id=None) -> dict:
        ex_id = self._resolve_exchange_id(exchange, exchange_id)
        acc_id = self._resolve_account_id(ex_id, account, account_id)
        if ex_id is None or acc_id is None:
            return {}

        query = """
                SELECT wallet_balance, available_balance, margin_used, equity, unrealized_pnl
                FROM account_balance_snapshots
                WHERE exchange_id = %s \
                  AND account_id = %s
                ORDER BY ts DESC LIMIT 1 \
                """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (ex_id, acc_id))
                row = cur.fetchone()
                if not row:
                    return {}
                cols = [d[0] for d in cur.description]
                return dict(zip(cols, row))

    # ======================================================================
    # OMS / WS HELPERS
    # ======================================================================

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

    def expire_stuck_pending(
        self,
        *,
        exchange_id: int,
        account_id: int,
        timeout_sec: int = 60,
    ) -> int:
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
    # ======================================================================
    # RETENTION
    # ======================================================================
    def cleanup_candles(
            self,
            *,
            exchange_id: int,
            interval: str,
            keep_days: int,
    ) -> int:
        """
        Delete old candles from public.candles.
        """
        query = """
                DELETE \
                FROM candles
                WHERE exchange_id = %s
                  AND interval = %s
                  AND open_time \
                    < NOW() - (INTERVAL '1 day' * %s) \
                """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (exchange_id, interval, int(keep_days)))
                n = cur.rowcount
            conn.commit()

        return int(n or 0)

    # ======================================================================
    # EXCHANGE INFO (filters)
    # ======================================================================
    def upsert_symbol_filters(
            self,
            *,
            exchange_id: int,
            symbol_id: int,
            price_tick,
            qty_step,
            min_qty=None,
            max_qty=None,
            min_notional=None,
            max_leverage=None,
            margin_type=None,
    ) -> None:
        query = """
                INSERT INTO symbol_filters (exchange_id, \
                                            symbol_id, \
                                            price_tick, \
                                            qty_step, \
                                            min_qty, \
                                            max_qty, \
                                            min_notional, \
                                            max_leverage, \
                                            margin_type, \
                                            updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()) ON CONFLICT (exchange_id, symbol_id) DO \
                UPDATE SET
                    price_tick = EXCLUDED.price_tick, \
                    qty_step = EXCLUDED.qty_step, \
                    min_qty = EXCLUDED.min_qty, \
                    max_qty = EXCLUDED.max_qty, \
                    min_notional = EXCLUDED.min_notional, \
                    max_leverage = EXCLUDED.max_leverage, \
                    margin_type = EXCLUDED.margin_type, \
                    updated_at = NOW() \
                """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    query,
                    (
                        exchange_id,
                        symbol_id,
                        price_tick,
                        qty_step,
                        min_qty,
                        max_qty,
                        min_notional,
                        max_leverage,
                        margin_type,
                    ),
                )
            conn.commit()

    # ======================================================================
    # ORDERS / TRADES (optional: keep if used elsewhere)
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
                INSERT INTO trades (exchange_id, account_id, trade_id, order_id, symbol_id, \
                                    strategy_id, pos_uid, side, price, qty, fee, \
                                    realized_pnl, ts, source, raw_json)
                VALUES (%(exchange_id)s, %(account_id)s, %(trade_id)s, %(order_id)s, %(symbol_id)s, \
                        %(strategy_id)s, %(pos_uid)s, %(side)s, %(price)s, %(qty)s, %(fee)s, \
                        %(realized_pnl)s, %(ts)s, %(source)s, %(raw_json)s) ON CONFLICT (exchange_id, account_id, trade_id)
        DO \
                UPDATE SET
                    realized_pnl = EXCLUDED.realized_pnl, \
                    ts = EXCLUDED.ts, \
                    raw_json = EXCLUDED.raw_json; \
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

                # ✔ datetime остаётся datetime (для PostgreSQL)
                "ts": t["ts"],

                "source": str(t.get("source") or "ws"),

                # ✔ datetime внутри raw_json → безопасно сериализуется
                "raw_json": json.dumps(t, ensure_ascii=False, default=str),
            })

        self._exec_many(query, rows)

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
    # ID RESOLVERS
    # ======================================================================
    def _resolve_exchange_id(self, exchange, exchange_id):
        if exchange_id is not None:
            return int(exchange_id)
        if isinstance(exchange, int):
            return int(exchange)
        if isinstance(exchange, str):
            row = self._exec_one("SELECT exchange_id FROM exchanges WHERE name = %s", (exchange,))
            return int(row[0]) if row else None
        return None

    def _resolve_account_id(self, exchange_id: int, account, account_id):
        if account_id is not None:
            return int(account_id)
        if isinstance(account, int):
            return int(account)
        if isinstance(account, str) and exchange_id is not None:
            row = self._exec_one(
                "SELECT account_id FROM accounts WHERE exchange_id=%s AND account_name=%s",
                (exchange_id, account),
            )
            return int(row[0]) if row else None
        return None


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

        query = """
                INSERT INTO orders (exchange_id, \
                                    account_id, \
                                    order_id, \
                                    symbol_id, \
                                    strategy_id, \
                                    pos_uid, \
                                    client_order_id, \
                                    side, \
                                    type, \
                                    reduce_only, \
                                    price, \
                                    qty, \
                                    filled_qty, \
                                    status, \
                                    source, \
                                    ts_ms, \
                                    created_at, \
                                    updated_at, \
                                    raw_json)
                VALUES (%s, %s, %s, %s, \
                        %s, %s, %s, \
                        %s, %s, %s, \
                        %s, %s, %s, \
                        %s, \
                        %s, \
                        %s, \
                        COALESCE(%s, NOW()), \
                        COALESCE(%s, NOW()), \
                        %s) ON CONFLICT (exchange_id, account_id, order_id)
        DO \
                UPDATE SET
                    symbol_id = EXCLUDED.symbol_id, \
                    strategy_id = EXCLUDED.strategy_id, \
                    pos_uid = EXCLUDED.pos_uid, \
                    client_order_id = EXCLUDED.client_order_id, \
                    side = EXCLUDED.side, \
                    type = EXCLUDED.type, \
                    reduce_only = EXCLUDED.reduce_only, \
                    price = EXCLUDED.price, \
                    qty = EXCLUDED.qty, \
                    filled_qty = EXCLUDED.filled_qty, \
                    status = EXCLUDED.status, \
                    source = EXCLUDED.source, \
                    ts_ms = EXCLUDED.ts_ms, \
                    updated_at = NOW(), \
                    raw_json = COALESCE (EXCLUDED.raw_json, orders.raw_json) \
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

    def upsert_candles(self, rows: list[dict]) -> int:
        """
        Bulk upsert candles into public.candles.
        """
        if not rows:
            return 0

        query = """
                INSERT INTO candles (exchange_id, \
                                     symbol_id, \
                                     interval, \
                                     open_time, \
                                     open, \
                                     high, \
                                     low, \
                                     close, \
                                     volume, \
                                     source)
                VALUES (%(exchange_id)s, \
                        %(symbol_id)s, \
                        %(interval)s, \
                        %(open_time)s, \
                        %(open)s, \
                        %(high)s, \
                        %(low)s, \
                        %(close)s, \
                        %(volume)s, \
                        %(source)s) ON CONFLICT (exchange_id, symbol_id, interval, open_time)
        DO \
                UPDATE SET \
                    open = EXCLUDED.open, \
                    high = EXCLUDED.high, \
                    low = EXCLUDED.low, \
                    close = EXCLUDED.close, \
                    volume = EXCLUDED.volume \
                """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, rows)
                n = cur.rowcount
            conn.commit()

        return int(n or 0)


    def get_symbol_filters(self, *, exchange_id: int, symbol_id: int) -> dict:
        """
        Load trading filters for symbol.
        Used by exchange to normalize qty/price before submit.
        """

        query = """
            SELECT price_tick,
                   qty_step,
                   min_qty,
                   max_qty,
                   min_notional,
                   max_leverage,
                   margin_type
            FROM symbol_filters
            WHERE exchange_id = %s
              AND symbol_id = %s
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(exchange_id), int(symbol_id)))
                row = cur.fetchone()

        if row is None:
            raise KeyError(
                f"symbol_filters not found "
                f"(exchange_id={exchange_id}, symbol_id={symbol_id})"
            )

        return {
            "price_tick": float(row[0]),
            "qty_step": float(row[1]),
            "min_qty": float(row[2]) if row[2] is not None else None,
            "max_qty": float(row[3]) if row[3] is not None else None,
            "min_notional": float(row[4]) if row[4] is not None else None,
            "max_leverage": int(row[5]) if row[5] is not None else None,
            "margin_type": row[6],
        }
