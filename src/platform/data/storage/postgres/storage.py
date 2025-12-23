# src/platform/data/storage/postgres/storage.py
from __future__ import annotations

import json
import logging
from typing import Dict, Iterable,Optional, Any
from datetime import datetime, timezone

from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row


from src.platform.market_state import (
    AccountBalanceSnapshot,
    OpenInterestPoint,
)

logger = logging.getLogger(__name__)


class PostgreSQLStorage:
    """
    PostgreSQL storage — OMS / Orders / Fills / PnL (v9)
    """

    def __init__(self, pool):
        self.pool = pool

    # ------------------------------------------------------------------
    # INTERNAL SQL HELPERS
    # ------------------------------------------------------------------

    def _fetchone(self, sql: str, params: tuple):
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                if not row:
                    return None
                cols = [desc[0] for desc in cur.description]
                return dict(zip(cols, row))

    def _fetchall(self, sql: str, params: tuple = ()):
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description]
                return [dict(zip(cols, r)) for r in rows]

    # =========================================================================
    # REGISTRY
    # =========================================================================

    def ensure_exchange_account_symbol(
        self,
        exchange: str,
        account: str,
        symbols: list[str],
    ) -> Dict[str, int]:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO exchanges (name)
                    VALUES (%s)
                    ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                    RETURNING exchange_id
                    """,
                    (exchange,),
                )
                exchange_id = cur.fetchone()[0]

                cur.execute(
                    """
                    INSERT INTO exchange_accounts (exchange_id, name)
                    VALUES (%s, %s)
                    ON CONFLICT (exchange_id, name)
                    DO UPDATE SET name = EXCLUDED.name
                    RETURNING account_id
                    """,
                    (exchange_id, account),
                )
                account_id = cur.fetchone()[0]

                ids = {
                    "_exchange_id": exchange_id,
                    "_account_id": account_id,
                }

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
                    ids[sym] = cur.fetchone()[0]

            conn.commit()
            return ids

    # ---------------------------------------------------------------------
    # OMS: single wrappers (compat)
    # ---------------------------------------------------------------------

    def upsert_order(self, order: dict) -> None:
        """Single-order upsert (compat for WS/OMS)."""
        self.upsert_orders([order])

    def insert_order(self, order: dict) -> None:
        """Backward compat (some code calls insert_order)."""
        self.upsert_order(order)

    def upsert_trade(self, trade: dict) -> None:
        """Single-trade upsert (compat for WS/OMS)."""
        self.upsert_trades([trade])

    def insert_trade(self, trade: dict) -> None:
        """Backward compat."""
        self.upsert_trade(trade)

    def insert_order_fill(self, fill: dict) -> None:
        """Single fill insert (compat)."""
        self.upsert_order_fills([fill])

    # ---------------------------------------------------------------------
    # OMS: bulk writers
    # ---------------------------------------------------------------------

    def upsert_orders(self, orders: list[dict]) -> None:
        if not orders:
            return

        sql = """
              INSERT INTO orders (exchange_id, account_id, order_id, client_order_id, symbol_id, \
                                  side, type, status, qty, price, reduce_only, ts_ms, raw_json, updated_at) \
              VALUES (%(exchange_id)s, %(account_id)s, %(order_id)s, %(client_order_id)s, %(symbol_id)s, \
                      %(side)s, %(type)s, %(status)s, %(qty)s, %(price)s, %(reduce_only)s, %(ts_ms)s, %(raw_json)s, \
                      NOW()) ON CONFLICT (exchange_id, account_id, order_id)
        DO \
              UPDATE SET
                  client_order_id = EXCLUDED.client_order_id, \
                  symbol_id = EXCLUDED.symbol_id, \
                  side = EXCLUDED.side, \
                  type = EXCLUDED.type, \
                  status = EXCLUDED.status, \
                  qty = EXCLUDED.qty, \
                  price = EXCLUDED.price, \
                  reduce_only = EXCLUDED.reduce_only, \
                  ts_ms = EXCLUDED.ts_ms, \
                  raw_json = EXCLUDED.raw_json, \
                  updated_at = NOW()
              ; \
              """

        rows = []
        for o in orders:
            rows.append({
                "exchange_id": int(o["exchange_id"]),
                "account_id": int(o["account_id"]),
                "order_id": str(o.get("order_id") or o.get("orderId") or ""),
                "client_order_id": str(o.get("client_order_id") or o.get("clientOrderId") or ""),
                "symbol_id": int(o["symbol_id"]),
                "side": str(o.get("side") or ""),
                "type": str(o.get("type") or ""),
                "status": str(o.get("status") or ""),
                "qty": float(o.get("qty") or 0.0),
                "price": float(o.get("price") or 0.0),
                "reduce_only": bool(o.get("reduce_only") or o.get("reduceOnly") or False),
                "ts_ms": int(o.get("ts_ms") or o.get("ts") or 0),
                "raw_json": json.dumps(o.get("raw") or o, ensure_ascii=False),
            })

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()


    def upsert_trades(self, trades: list[dict]) -> None:
        if not trades:
            return

        sql = """
              INSERT INTO trades (exchange_id, account_id, trade_id, order_id, symbol_id, \
                                  strategy_id, pos_uid, side, price, qty, fee, fee_asset, \
                                  realized_pnl, ts, source, raw_json) \
              VALUES (%(exchange_id)s, %(account_id)s, %(trade_id)s, %(order_id)s, %(symbol_id)s, \
                      %(strategy_id)s, %(pos_uid)s, %(side)s, %(price)s, %(qty)s, %(fee)s, %(fee_asset)s, \
                      %(realized_pnl)s, %(ts)s, %(source)s, %(raw_json)s) ON CONFLICT (exchange_id, account_id, trade_id)
        DO \
              UPDATE SET
                  order_id = EXCLUDED.order_id, \
                  symbol_id = EXCLUDED.symbol_id, \
                  strategy_id = EXCLUDED.strategy_id, \
                  pos_uid = EXCLUDED.pos_uid, \
                  side = EXCLUDED.side, \
                  price = EXCLUDED.price, \
                  qty = EXCLUDED.qty, \
                  fee = EXCLUDED.fee, \
                  fee_asset = EXCLUDED.fee_asset, \
                  realized_pnl = EXCLUDED.realized_pnl, \
                  ts = EXCLUDED.ts, \
                  source = EXCLUDED.source, \
                  raw_json = EXCLUDED.raw_json
              ; \
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
                "fee_asset": str(t.get("fee_asset") or ""),
                "realized_pnl": float(t.get("realized_pnl") or 0.0),
                "ts": t["ts"],  # ✅ datetime
                "source": str(t.get("source") or "ws_user"),
                "raw_json": json.dumps(t.get("raw") or t, ensure_ascii=False),
            })

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()

    def upsert_order_fills(self, fills: list[dict]) -> None:
        if not fills:
            return

        sql = """
              INSERT INTO order_fills (exchange_id, account_id, fill_uid, symbol_id, order_id, trade_id, \
                                       client_order_id, price, qty, realized_pnl, ts, source) \
              VALUES (%(exchange_id)s, %(account_id)s, %(fill_uid)s, %(symbol_id)s, %(order_id)s, %(trade_id)s, \
                      %(client_order_id)s, %(price)s, %(qty)s, %(realized_pnl)s, %(ts)s, %(source)s) ON CONFLICT (exchange_id, account_id, fill_uid)
        DO NOTHING; \
              """

        rows = []
        for f in fills:
            rows.append({
                "exchange_id": int(f["exchange_id"]),
                "account_id": int(f["account_id"]),
                "fill_uid": str(f.get("fill_uid") or f.get("uid") or ""),
                "symbol_id": int(f["symbol_id"]),
                "order_id": str(f.get("order_id") or ""),
                "trade_id": str(f.get("trade_id") or ""),
                "client_order_id": str(f.get("client_order_id") or ""),
                "price": float(f.get("price") or 0.0),
                "qty": float(f.get("qty") or 0.0),
                "realized_pnl": float(f.get("realized_pnl") or 0.0),
                "ts": int(f.get("ts") or f.get("ts_ms") or 0),
                "source": str(f.get("source") or "ws"),
            })

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()

    # =========================================================================
    # OMS / ORDERS
    # =========================================================================


    def client_order_exists(
        self,
        exchange_id: int,
        account_id: int,
        client_order_id: str,
    ) -> bool:
        sql = """
        SELECT 1 FROM orders
        WHERE exchange_id=%s AND account_id=%s AND client_order_id=%s
        LIMIT 1
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (exchange_id, account_id, client_order_id))
                return cur.fetchone() is not None


    def get_today_realized_pnl(
            self,
            exchange_id: int,
            account_id: int,
    ) -> float:
        """
        Sum today's realized PnL from trades.
        """
        sql = """
              SELECT COALESCE(SUM(realized_pnl), 0)
              FROM trades
              WHERE exchange_id = %s
                AND account_id = %s
                AND ts >= date_trunc('day', now()) \
              """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (exchange_id, account_id))
                return float(cur.fetchone()[0] or 0.0)



    # --- SYMBOL FILTERS --------------------------------------------------------

    def upsert_symbol_filters(
            self,
            *,
            exchange_id: int,
            symbol_id: int,
            qty_step: float,
            min_qty: float,
            max_qty: float | None,
            price_tick: float,
            min_notional: float | None,
    ) -> None:
        """
        Insert / update symbol trading filters from exchangeInfo
        """
        sql = """
              INSERT INTO symbol_filters (exchange_id, \
                                          symbol_id, \
                                          qty_step, \
                                          min_qty, \
                                          max_qty, \
                                          price_tick, \
                                          min_notional, \
                                          updated_at)
              VALUES (%(exchange_id)s, \
                      %(symbol_id)s, \
                      %(qty_step)s, \
                      %(min_qty)s, \
                      %(max_qty)s, \
                      %(price_tick)s, \
                      %(min_notional)s, \
                      now()) ON CONFLICT (exchange_id, symbol_id)
        DO \
              UPDATE SET
                  qty_step = EXCLUDED.qty_step, \
                  min_qty = EXCLUDED.min_qty, \
                  max_qty = EXCLUDED.max_qty, \
                  price_tick = EXCLUDED.price_tick, \
                  min_notional = EXCLUDED.min_notional, \
                  updated_at = now() \
              """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {
                    "exchange_id": exchange_id,
                    "symbol_id": symbol_id,
                    "qty_step": qty_step,
                    "min_qty": min_qty,
                    "max_qty": max_qty,
                    "price_tick": price_tick,
                    "min_notional": min_notional,
                })
            conn.commit()

    def get_symbol_filters(
            self,
            *,
            exchange_id: int,
            symbol_id: int,
    ) -> dict:
        """
        Return normalized symbol filters for order normalization
        """
        sql = """
              SELECT qty_step, \
                     min_qty, \
                     max_qty, \
                     price_tick, \
                     min_notional
              FROM symbol_filters
              WHERE exchange_id = %s
                AND symbol_id = %s \
              """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (exchange_id, symbol_id))
                row = cur.fetchone()

        if not row:
            raise RuntimeError(
                f"Symbol filters not found: exchange_id={exchange_id} symbol_id={symbol_id}"
            )

        return {
            "qty_step": float(row[0]),
            "min_qty": float(row[1]),
            "max_qty": float(row[2]) if row[2] is not None else None,
            "price_tick": float(row[3]),
            "min_notional": float(row[4]) if row[4] is not None else None,
        }

    def cleanup_candles(self, *, exchange_id: int, interval: str, keep_days: int) -> int:
        sql = """
              DELETE \
              FROM candles
              WHERE exchange_id = %s
                AND interval = %s
                AND open_time \
                  < NOW() - (%s || ' days'):: interval \
              """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (exchange_id, interval, keep_days))
                return cur.rowcount


    # =========================================================================
    # PNL / BALANCE / OI (STUBS)
    # =========================================================================

    def insert_account_balance_snapshots(
        self,
        rows: Iterable[dict],
    ) -> int:
        rows = list(rows)
        if not rows:
            return 0

        sql = """
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

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()

        return len(rows)


    def upsert_open_interest(
        self,
        rows: Iterable[dict],
    ) -> int:
        rows = list(rows)
        if not rows:
            return 0

        sql = """
        INSERT INTO open_interest (
            exchange_id,
            symbol_id,
            interval,
            ts,
            open_interest,
            open_interest_value,
            source
        )
        VALUES (
            %(exchange_id)s,
            %(symbol_id)s,
            %(interval)s,
            %(ts)s,
            %(open_interest)s,
            %(open_interest_value)s,
            %(source)s
        )
        ON CONFLICT (exchange_id, symbol_id, interval, ts)
        DO UPDATE SET
            open_interest = EXCLUDED.open_interest,
            open_interest_value = EXCLUDED.open_interest_value
        """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()

        return len(rows)


    def get_pending_order_by_client_id(
        self,
        *,
        exchange_id: int,
        account_id: int,
        client_order_id: str,
    ) -> dict | None:
        sql = """
        SELECT
            symbol_id,
            strategy_id,
            pos_uid
        FROM orders
        WHERE exchange_id = %s
          AND account_id = %s
          AND client_order_id = %s
          AND status = 'PENDING_SUBMIT'
        ORDER BY created_at DESC
        LIMIT 1
        """
        row = self._fetchone(sql, (exchange_id, account_id, client_order_id))
        if not row:
            return None
        return dict(row)

    def upsert_candles(self, rows: list[dict]) -> None:
        if not rows:
            return

        sql = """
              INSERT INTO candles (exchange_id, \
                                   symbol_id, \
                                   interval, \
                                   open_time, \
                                   open, \
                                   high, \
                                   low, \
                                   close, \
                                   volume, \
                                   source) \
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
                  volume = EXCLUDED.volume, \
                  source = EXCLUDED.source; \
              """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)

    # =========================================================================
    # MARKET STATE (STUBS)
    # =========================================================================

    def get_latest_positions(self, *, exchange: str, account: str) -> dict:
        return {}

    def get_latest_balances(self, *, exchange: str, account: str) -> dict:
        return {
            "wallet_balance": 0.0,
            "available_balance": 0.0,
            "margin_balance": 0.0,
        }


    def upsert_order_placeholder(self, order: dict) -> None:
        """
        WS-safe stub: принимает ОДИН dict
        """
        return None

    def expire_stuck_pending(
        self,
        exchange_id: int,
        account_id: int,
        timeout_sec: int,
    ) -> int:
        return 0