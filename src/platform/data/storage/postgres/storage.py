# src/platform/data/storage/postgres/storage.py
from __future__ import annotations

import json
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Iterable, Sequence, Mapping, Any, List, Tuple, Dict


from psycopg import sql as _sql

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

    def upsert_symbols(
            self,
            *,
            exchange_id: int,
            symbols: list[str],
            deactivate_missing: bool = True,
    ) -> dict[str, int]:
        symbols_norm = sorted({str(s).upper().strip() for s in (symbols or []) if str(s).strip()})
        if not symbols_norm:
            return {}

        sql_upsert = """
                     WITH incoming AS (SELECT UNNEST(%(symbols)s::text[]) AS symbol)
                     INSERT
                     INTO symbols (exchange_id, symbol, is_active, status, last_seen_at)
                     SELECT %(exchange_id)s, symbol, TRUE, 'TRADING', NOW()
                     FROM incoming ON CONFLICT (exchange_id, symbol)
                     DO \
                     UPDATE SET
                         is_active = TRUE, \
                         status = EXCLUDED.status, \
                         last_seen_at = EXCLUDED.last_seen_at \
                         RETURNING symbol, symbol_id;
                     """

        sql_deactivate_missing = """
                                 UPDATE symbols
                                 SET is_active = FALSE
                                 WHERE exchange_id = %(exchange_id)s
                                   AND NOT (symbol = ANY (%(symbols)s::text[]));
                                 """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_upsert, {"exchange_id": int(exchange_id), "symbols": symbols_norm})
                rows = cur.fetchall() or []

                if deactivate_missing:
                    # помечаем отсутствующие как неактивные
                    cur.execute(sql_deactivate_missing, {"exchange_id": int(exchange_id), "symbols": symbols_norm})

            conn.commit()

        return {str(sym).upper(): int(sid) for (sym, sid) in rows}

    def list_symbols(self, *, exchange_id: int) -> dict[str, int]:
        """Return {SYMBOL: symbol_id} for all symbols known to DB for the exchange."""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol, symbol_id
                    FROM symbols
                    WHERE exchange_id = %s
                    """,
                    (int(exchange_id),),
                )
                rows = cur.fetchall() or []
        return {str(sym).upper(): int(sid) for (sym, sid) in rows}

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

    def get_order(self, *, exchange_id: int, account_id: int, order_id: str) -> dict | None:
        query = """
                SELECT exchange_id, \
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
                       created_at, \
                       updated_at, \
                       source, \
                       ts_ms, \
                       raw_json
                FROM orders
                WHERE exchange_id = %s \
                  AND account_id = %s \
                  AND order_id = %s LIMIT 1 \
                """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(exchange_id), int(account_id), str(order_id)))
                row = cur.fetchone()
                if not row:
                    return None
                cols = [d[0] for d in cur.description]
                return dict(zip(cols, row))


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


    def upsert_symbol_filters(self, rows: Sequence[Mapping[str, Any]]) -> int:
        """
        Upsert symbol trading filters.

        Expected keys in each row dict:
          exchange_id, symbol_id,
          price_tick, qty_step,
          min_qty, max_qty,
          min_notional,
          max_leverage,
          margin_type,
          updated_at
        """
        if not rows:
            return 0

        sql = """
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
              VALUES (%(exchange_id)s, \
                      %(symbol_id)s, \
                      %(price_tick)s, \
                      %(qty_step)s, \
                      %(min_qty)s, \
                      %(max_qty)s, \
                      %(min_notional)s, \
                      %(max_leverage)s, \
                      %(margin_type)s, \
                      %(updated_at)s) ON CONFLICT (exchange_id, symbol_id)
            DO \
              UPDATE SET
                  price_tick = EXCLUDED.price_tick, \
                  qty_step = EXCLUDED.qty_step, \
                  min_qty = EXCLUDED.min_qty, \
                  max_qty = EXCLUDED.max_qty, \
                  min_notional = EXCLUDED.min_notional, \
                  max_leverage = EXCLUDED.max_leverage, \
                  margin_type = EXCLUDED.margin_type, \
                  updated_at = EXCLUDED.updated_at \
              """

        # гарантируем наличие updated_at, и приводим типы аккуратно
        now = datetime.now(timezone.utc)
        prepared: list[dict[str, Any]] = []

        for r in rows:
            d = dict(r)

            if "updated_at" not in d or d["updated_at"] is None:
                d["updated_at"] = now

            d["exchange_id"] = int(d["exchange_id"])
            d["symbol_id"] = int(d["symbol_id"])

            # float columns
            for k in ("price_tick", "qty_step", "min_qty", "max_qty", "min_notional"):
                if k in d and d[k] is not None:
                    d[k] = float(d[k])

            # int / str columns
            if d.get("max_leverage") is not None:
                d["max_leverage"] = int(d["max_leverage"])
            if d.get("margin_type") is not None:
                d["margin_type"] = str(d["margin_type"])

            prepared.append(d)

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, prepared)
            conn.commit()

        return len(prepared)

    # ======================================================================
    # ORDERS / TRADES (optional: keep if used elsewhere)
    # ======================================================================

    def upsert_orders(self, orders: list[dict]) -> None:
        """
        Upsert order snapshots.
        Status, qty, filled_qty are already resolved by OrderAggregate (FSM).
        DB does NOT compute lifecycle logic.
        """
        if not orders:
            return

        query = """
                INSERT INTO orders (exchange_id, \
                                    account_id, \
                                    order_id, \
                                    client_order_id, \
                                    symbol_id, \
                                    side, \
                                    type, \
                                    reduce_only, \
                                    price, \
                                    qty, \
                                    filled_qty, \
                                    status, \
                                    strategy_id, \
                                    pos_uid, \
                                    ts_ms, \
                                    raw_json, \
                                    updated_at)
                VALUES (%(exchange_id)s, \
                        %(account_id)s, \
                        %(order_id)s, \
                        %(client_order_id)s, \
                        %(symbol_id)s, \
                        %(side)s, \
                        %(type)s, \
                        %(reduce_only)s, \
                        %(price)s, \
                        %(qty)s, \
                        %(filled_qty)s, \
                        %(status)s, \
                        %(strategy_id)s, \
                        %(pos_uid)s, \
                        %(ts_ms)s, \
                        %(raw_json)s, \
                        NOW()) ON CONFLICT (exchange_id, account_id, order_id)
        DO \
                UPDATE SET
                    client_order_id = COALESCE (EXCLUDED.client_order_id, orders.client_order_id), \
                    symbol_id = EXCLUDED.symbol_id, \

                    side = EXCLUDED.side, \
                    type = EXCLUDED.type, \
                    reduce_only = EXCLUDED.reduce_only, \

                    price = EXCLUDED.price, \
                    qty = EXCLUDED.qty, \
                    filled_qty = EXCLUDED.filled_qty, \

                    status = EXCLUDED.status, \

                    strategy_id = COALESCE (orders.strategy_id, EXCLUDED.strategy_id), \
                    pos_uid = COALESCE (orders.pos_uid, EXCLUDED.pos_uid), \

                    ts_ms = GREATEST(COALESCE (orders.ts_ms, 0), COALESCE (EXCLUDED.ts_ms, 0)), \
                    raw_json = COALESCE (EXCLUDED.raw_json, orders.raw_json), \
                    updated_at = NOW() \
                """

        rows: list[dict] = []
        for o in orders:
            rows.append({
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
                "raw_json": json.dumps(
                    o.get("raw") or o.get("raw_json") or {},
                    ensure_ascii=False,
                ),
            })

        self._exec_many(query, rows)

    def set_order_status(
            self,
            *,
            exchange_id: int,
            account_id: int,
            order_id: str,
            status: str,
    ) -> int:
        query = """
                UPDATE orders
                SET status     = %s,
                    updated_at = NOW()
                WHERE exchange_id = %s
                  AND account_id = %s
                  AND order_id = %s \
                """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (status, int(exchange_id), int(account_id), str(order_id)))
                return int(cur.rowcount or 0)


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
              SELECT exchange_id, \
                     account_id, \
                     order_id, \
                     symbol_id, \
                     client_order_id, \
                     status, \
                     side, \
                     type, \
                     reduce_only, \
                     price, \
                     qty, \
                     filled_qty, \
                     source, \
                     ts_ms, \
                     recv_ts, \
                     raw_json
              FROM order_events
              WHERE exchange_id = %(exchange_id)s
                AND account_id = %(account_id)s \
              """

        params = {
            "exchange_id": int(exchange_id),
            "account_id": int(account_id),
        }

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


    def get_candles_watermarks(self, *, exchange_id: int, intervals: List[str]) -> Dict[Tuple[int, str], datetime]:
        """
        Возвращает watermarks для candles:
          (symbol_id, interval) -> MAX(open_time)

        Нужно для collector_candles.py чтобы продолжать догрузку после рестарта.
        """
        wm: Dict[Tuple[int, str], datetime] = {}
        intervals = [str(x).strip() for x in (intervals or []) if str(x).strip()]
        if not intervals:
            return wm

        sql = """
              SELECT symbol_id, MAX(open_time) AS last_open_time
              FROM candles
              WHERE exchange_id = %s \
                AND interval = %s
              GROUP BY symbol_id \
              """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                for itv in intervals:
                    cur.execute(sql, (int(exchange_id), str(itv)))
                    for sid, last_dt in (cur.fetchall() or []):
                        if sid is None or last_dt is None:
                            continue
                        wm[(int(sid), str(itv))] = last_dt

        return wm

    def list_non_terminal_orders(self, *, exchange_id: int, account_id: int, symbol_ids=None) -> list[dict]:
        base = """
               SELECT exchange_id, \
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
                      created_at, \
                      updated_at, \
                      source, \
                      ts_ms, \
                      raw_json
               FROM orders
               WHERE exchange_id = %s \
                 AND account_id = %s
                 AND status NOT IN ('FILLED', 'CANCELED', 'REJECTED', 'EXPIRED') \
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
               WHERE exchange_id = %s \
                 AND account_id = %s \
                 AND client_order_id = %s
                 AND order_id LIKE 'PH::%%'
               ORDER BY updated_at DESC LIMIT 1 \
               """
        q_any = """
                SELECT *
                FROM orders
                WHERE exchange_id = %s \
                  AND account_id = %s \
                  AND client_order_id = %s
                ORDER BY updated_at DESC LIMIT 1 \
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


    def get_symbol_filters(
            self,
            *,
            exchange_id: int,
            symbol_id: int,
    ) -> dict | None:
        """
        Load trading filters for symbol.

        Used by OMS to normalize qty/price before submit.
        Returns None if filters not found.
        """

        query = """
                SELECT price_tick, \
                       qty_step, \
                       min_qty, \
                       max_qty, \
                       min_notional, \
                       max_leverage, \
                       margin_type
                FROM symbol_filters
                WHERE exchange_id = %s
                  AND symbol_id = %s \
                """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(exchange_id), int(symbol_id)))
                row = cur.fetchone()

        if row is None:
            return None

        price_tick = float(row[0]) if row[0] is not None else None
        qty_step = float(row[1]) if row[1] is not None else None

        return {
            "price_tick": price_tick,
            "qty_step": qty_step,
            "min_qty": float(row[2]) if row[2] is not None else None,
            "max_qty": float(row[3]) if row[3] is not None else None,
            "min_notional": float(row[4]) if row[4] is not None else None,
            "max_leverage": int(row[5]) if row[5] is not None else None,
            "margin_type": row[6],
        }

    def fetch_orders_metrics_window(self, *, exchange_id: int, account_id: int, since_ts_ms: int) -> list[dict]:
        """
        STEP 7: get orders rows for lifecycle metrics inside time window.
        We only need: client_order_id, order_id, status, source, ts_ms.
        """
        query = """
                SELECT client_order_id, order_id, status, source, ts_ms
                FROM orders
                WHERE exchange_id = %s
                  AND account_id = %s
                  AND ts_ms >= %s
                  AND client_order_id IS NOT NULL
                ORDER BY ts_ms ASC \
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


    def upsert_account_state(
            self,
            *,
            exchange_id: int,
            account_id: int,
            state: dict,
    ) -> None:
        sql = """
              INSERT INTO account_state (exchange_id, \
                                         account_id, \
                                         ts, \
                                         wallet_balance, \
                                         equity, \
                                         available_balance, \
                                         unrealized_pnl)
              VALUES (%s, %s, NOW(), %s, %s, %s, %s) \
              """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        exchange_id,
                        account_id,
                        state.get("wallet_balance"),
                        state.get("equity"),
                        state.get("available_balance"),
                        state.get("unrealized_pnl"),
                    ),
                )

    def exec_ddl(self, sql: str) -> None:
        """Execute DDL SQL that may contain multiple statements."""
        if not sql:
            return

        # very simple splitter by ';'
        statements = [s.strip() for s in sql.split(";") if s.strip()]

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                for stmt in statements:
                    cur.execute(stmt)
            conn.commit()

    def fetch_trades(
            self,
            *,
            exchange_id: int,
            account_id: int,
            since_ts_ms: int = 0,
            limit: int = 200_000,
    ) -> list[dict]:

        query = """
                SELECT exchange_id, \
                       account_id, \
                       symbol_id, \
                       trade_id, \
                       order_id, \
                       side, \
                       qty, \
                       price, \
                       realized_pnl, \
                       fee, \
                       (EXTRACT(EPOCH FROM ts) * 1000)::BIGINT AS ts_ms, raw_json
                FROM trades
                WHERE exchange_id = %(exchange_id)s
                  AND account_id = %(account_id)s
                  AND (
                    %(since_ts_ms)s = 0
                        OR ts >= to_timestamp(%(since_ts_ms)s / 1000.0)
                    )
                ORDER BY ts ASC
                    LIMIT %(limit)s \
                """

        params = {
            "exchange_id": int(exchange_id),
            "account_id": int(account_id),
            "since_ts_ms": int(since_ts_ms or 0),
            "limit": int(limit or 200_000),
        }

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
                cols = [c.name for c in cur.description]
            conn.commit()

        return [dict(zip(cols, r)) for r in rows]


    def upsert_position_snapshots(self, rows):
        if not rows:
            return 0

        sql = """
              INSERT INTO position_snapshots (exchange_id, account_id, symbol_id, \
                                              side, qty, entry_price, mark_price, \
                                              position_value, unrealized_pnl, realized_pnl, fees, \
                                              last_ts, updated_at, source)
              VALUES (%(exchange_id)s, %(account_id)s, %(symbol_id)s, \
                      %(side)s, %(qty)s, %(entry_price)s, %(mark_price)s, \
                      %(position_value)s, %(unrealized_pnl)s, %(realized_pnl)s, %(fees)s, \
                      %(last_ts)s, %(updated_at)s, %(source)s) ON CONFLICT (exchange_id, account_id, symbol_id)
        DO \
              UPDATE SET
                  side=EXCLUDED.side, \
                  qty=EXCLUDED.qty, \
                  entry_price=EXCLUDED.entry_price, \
                  mark_price=EXCLUDED.mark_price, \
                  position_value=EXCLUDED.position_value, \
                  unrealized_pnl=EXCLUDED.unrealized_pnl, \
                  realized_pnl=EXCLUDED.realized_pnl, \
                  fees=EXCLUDED.fees, \
                  last_ts=EXCLUDED.last_ts, \
                  updated_at=EXCLUDED.updated_at, \
                  source =EXCLUDED.source
              ; \
              """
        return self._exec_many(sql, rows)


    # ------------------------------------------------------------
    def execute_raw(self, query: str, *, log: bool = False) -> None:
        """
        Execute raw SQL (DDL / maintenance / cleanup).
        SAFE: uses connection pool.
        """
        if not query:
            return

        if log:
            logger.info("[DB][RAW] %s", query)

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_sql.SQL(query))
            conn.commit()


    def list_active_symbols(self, *, exchange_id: int, active_ttl_sec: int = 1800) -> List[str]:
        """
        Active symbols = symbols with OPEN positions OR NEW orders recently updated.
        Returns list of SYMBOL strings (e.g. 'LTCUSDT').
        """
        exchange_id = int(exchange_id)
        ttl = int(active_ttl_sec)

        sql = """
              WITH active_symbol_ids AS (SELECT p.symbol_id \
                                         FROM positions p \
                                         WHERE p.exchange_id = %s \
                                           AND abs(p.qty) > 0 \
                                           AND p.status = 'OPEN' \
                                           AND p.closed_at IS NULL \

                                         UNION ALL \

                                         SELECT o.symbol_id \
                                         FROM orders o \
                                         WHERE o.exchange_id = %s \
                                           AND o.status = 'NEW' \
                                           AND o.updated_at >= (now() - (%s || ' seconds')::interval))
              SELECT DISTINCT s.symbol
              FROM symbols s
                       JOIN active_symbol_ids a ON a.symbol_id = s.symbol_id
              WHERE s.exchange_id = %s
              ORDER BY s.symbol; \
              """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (exchange_id, exchange_id, ttl, exchange_id))
                return [str(r[0]) for r in (cur.fetchall() or []) if r and r[0]]

    def get_symbol_id(self, *, exchange_id: int, symbol: str) -> int | None:
        """
        Resolve symbol_id for (exchange_id, symbol).
        Returns None if symbol not found.
        """
        symbol = str(symbol).upper().strip()
        if not symbol:
            return None

        sql = "SELECT symbol_id FROM symbols WHERE exchange_id=%s AND symbol=%s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(exchange_id), symbol))
                row = cur.fetchone()
        return int(row[0]) if row else None

    def fetch_symbols_map(self, *, exchange_id: int, only_active: bool = True) -> dict[str, int]:
        where = "WHERE exchange_id=%s"
        params = [int(exchange_id)]
        if only_active:
            where += " AND is_active = TRUE"

        sql = f"""
            SELECT symbol, symbol_id
            FROM symbols
            {where}
            ORDER BY symbol
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()

        return {str(sym).upper(): int(sid) for (sym, sid) in rows}

    def deactivate_missing_symbols(self, *, exchange_id: int, active_symbols: Iterable[str]) -> int:
        """
        Marks symbols NOT in active_symbols as inactive.
        Returns number of rows updated.
        """
        active = [str(s).upper() for s in (active_symbols or [])]
        if not active:
            return 0

        sql = """
        UPDATE symbols
        SET is_active = FALSE
        WHERE exchange_id = %s
          AND is_active = TRUE
          AND NOT (symbol = ANY(%s))
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(exchange_id), active))
                n = cur.rowcount
            conn.commit()

        return int(n or 0)


    def upsert_funding(self, rows: list[dict]) -> int:
        if not rows:
            return 0

        query = """
            INSERT INTO funding (
                exchange_id, symbol_id, funding_time,
                funding_rate, mark_price, source
            )
            VALUES (
                %(exchange_id)s, %(symbol_id)s, %(funding_time)s,
                %(funding_rate)s, %(mark_price)s, %(source)s
            )
            ON CONFLICT (exchange_id, symbol_id, funding_time)
            DO UPDATE SET
                funding_rate = EXCLUDED.funding_rate,
                mark_price   = EXCLUDED.mark_price,
                source       = EXCLUDED.source
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, rows)
                n = cur.rowcount
            conn.commit()
        return int(n or 0)

    def upsert_open_interest(self, rows: list[dict]) -> int:
        if not rows:
            return 0

        query = """
            INSERT INTO open_interest (
                exchange_id, symbol_id, interval, ts,
                open_interest, open_interest_value, source
            )
            VALUES (
                %(exchange_id)s, %(symbol_id)s, %(interval)s, %(ts)s,
                %(open_interest)s, %(open_interest_value)s, %(source)s
            )
            ON CONFLICT (exchange_id, symbol_id, interval, ts)
            DO UPDATE SET
                open_interest       = EXCLUDED.open_interest,
                open_interest_value = EXCLUDED.open_interest_value,
                source              = EXCLUDED.source
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, rows)
                n = cur.rowcount
            conn.commit()
        return int(n or 0)

    def cleanup_funding(self, *, exchange_id: int, keep_days: int) -> int:
        keep_days = int(keep_days)
        if keep_days <= 0:
            return 0

        query = """
            DELETE FROM funding
            WHERE exchange_id = %s
              AND funding_time < (NOW() - (%s || ' days')::interval)
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(exchange_id), keep_days))
                n = cur.rowcount
            conn.commit()
        return int(n or 0)

    def cleanup_open_interest(self, *, exchange_id: int, interval: str, keep_days: int) -> int:
        keep_days = int(keep_days)
        if keep_days <= 0:
            return 0

        query = """
            DELETE FROM open_interest
            WHERE exchange_id = %s
              AND interval = %s
              AND ts < (NOW() - (%s || ' days')::interval)
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (int(exchange_id), str(interval), keep_days))
                n = cur.rowcount
            conn.commit()
        return int(n or 0)


    def upsert_ticker_24h(self, rows: Iterable[dict]) -> int:
        rows = list(rows)
        if not rows:
            return 0

        query = """
                INSERT INTO ticker_24h (exchange_id, symbol_id, \
                                        open_time, close_time, \
                                        open_price, high_price, low_price, last_price, \
                                        volume, quote_volume, \
                                        price_change, price_change_percent, weighted_avg_price, \
                                        trades, source)
                VALUES (%(exchange_id)s, %(symbol_id)s, \
                        %(open_time)s, %(close_time)s, \
                        %(open_price)s, %(high_price)s, %(low_price)s, %(last_price)s, \
                        %(volume)s, %(quote_volume)s, \
                        %(price_change)s, %(price_change_percent)s, %(weighted_avg_price)s, \
                        %(trades)s, %(source)s) ON CONFLICT (exchange_id, symbol_id, close_time)
            DO \
                UPDATE SET
                    open_time = EXCLUDED.open_time, \
                    open_price = EXCLUDED.open_price, \
                    high_price = EXCLUDED.high_price, \
                    low_price = EXCLUDED.low_price, \
                    last_price = EXCLUDED.last_price, \
                    volume = EXCLUDED.volume, \
                    quote_volume = EXCLUDED.quote_volume, \
                    price_change = EXCLUDED.price_change, \
                    price_change_percent = EXCLUDED.price_change_percent, \
                    weighted_avg_price = EXCLUDED.weighted_avg_price, \
                    trades = EXCLUDED.trades, \
                    source = EXCLUDED.source
                ; \
                """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, rows)
                n = cur.rowcount
            conn.commit()

        # rowcount у executemany иногда бывает -1 в некоторых драйверах,
        # но psycopg обычно возвращает число. На всякий:
        return int(n if n is not None and n >= 0 else len(rows))

    def cleanup_ticker_24h(self, exchange_id: int, keep_days: int) -> int:
        """
        Удаляет записи ticker_24h старше keep_days (по close_time).
        Возвращает количество удалённых строк.
        """
        exchange_id = int(exchange_id)
        keep_days = int(keep_days)

        if keep_days <= 0:
            return 0

        cutoff = datetime.now(timezone.utc) - timedelta(days=keep_days)

        sql = """
              DELETE \
              FROM public.ticker_24h
              WHERE exchange_id = %s
                AND close_time < %s \
              """

        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (exchange_id, cutoff))
                    deleted = cur.rowcount or 0
                conn.commit()

            logger.info(
                "[Retention] ticker_24h cleaned exchange_id=%s keep_days=%s deleted=%s",
                exchange_id, keep_days, deleted
            )
            return int(deleted)

        except Exception:
            logger.exception("cleanup_ticker_24h failed exchange_id=%s keep_days=%s", exchange_id, keep_days)
            return 0
