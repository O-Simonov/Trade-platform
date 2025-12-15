from __future__ import annotations
from typing import Iterable
from psycopg_pool import ConnectionPool
from platform.data.storage.base import Storage

class PostgreSQLStorage(Storage):
    def __init__(self, pool: ConnectionPool):
        self.pool = pool

    def exec_ddl(self, ddl_sql: str) -> None:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl_sql)
            conn.commit()

    def ensure_exchange_account_symbol(self, exchange: str, account: str, symbols: list[str]) -> dict[str, int]:
        exchange = exchange.lower()
        symbols = [s.upper() for s in symbols]
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO exchanges(name) VALUES (%s) ON CONFLICT(name) DO NOTHING", (exchange,))
                cur.execute("SELECT exchange_id FROM exchanges WHERE name=%s", (exchange,))
                exchange_id = cur.fetchone()[0]

                cur.execute(
                    "INSERT INTO accounts(exchange_id, account_name) VALUES (%s,%s) "
                    "ON CONFLICT(exchange_id, account_name) DO NOTHING",
                    (exchange_id, account),
                )
                cur.execute("SELECT account_id FROM accounts WHERE exchange_id=%s AND account_name=%s", (exchange_id, account))
                account_id = cur.fetchone()[0]

                sym2id = {}
                for sym in symbols:
                    cur.execute(
                        "INSERT INTO symbols(exchange_id, symbol) VALUES (%s,%s) "
                        "ON CONFLICT(exchange_id, symbol) DO NOTHING",
                        (exchange_id, sym),
                    )
                    cur.execute("SELECT symbol_id FROM symbols WHERE exchange_id=%s AND symbol=%s", (exchange_id, sym))
                    sym2id[sym] = cur.fetchone()[0]
            conn.commit()

        sym2id["_exchange_id"] = exchange_id
        sym2id["_account_id"] = account_id
        return sym2id

    def get_last_pos_uid(self, exchange_id: int, account_id: int, symbol_id: int, strategy_id: str) -> str | None:
        q = "SELECT pos_uid FROM positions WHERE exchange_id=%s AND account_id=%s AND symbol_id=%s AND strategy_id=%s LIMIT 1"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (exchange_id, account_id, symbol_id, strategy_id))
                row = cur.fetchone()
        return row[0] if row and row[0] else None

    def get_latest_open_pos_uid(self, exchange_id: int, account_id: int, symbol_id: int, strategy_id: str) -> str | None:
        q = """SELECT pos_uid FROM positions
                 WHERE exchange_id=%s AND account_id=%s AND symbol_id=%s AND strategy_id=%s
                   AND qty <> 0 AND pos_uid IS NOT NULL
                 LIMIT 1"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (exchange_id, account_id, symbol_id, strategy_id))
                row = cur.fetchone()
        return row[0] if row and row[0] else None

    def upsert_hedge_link(self, row: dict) -> None:
        q = """INSERT INTO hedge_links (
                 exchange_id, base_account_id, hedge_account_id, symbol_id,
                 base_pos_uid, hedge_pos_uid, hedge_ratio, created_at
               ) VALUES (
                 %(exchange_id)s, %(base_account_id)s, %(hedge_account_id)s, %(symbol_id)s,
                 %(base_pos_uid)s, %(hedge_pos_uid)s, %(hedge_ratio)s, %(created_at)s
               ) ON CONFLICT DO NOTHING"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, row)
            conn.commit()

    # OMS helpers
    def client_order_exists(self, exchange_id: int, account_id: int, client_order_id: str) -> bool:
        if not client_order_id:
            return False
        q = "SELECT 1 FROM orders WHERE exchange_id=%s AND account_id=%s AND client_order_id=%s LIMIT 1"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (exchange_id, account_id, client_order_id))
                row = cur.fetchone()
        return bool(row)

    def upsert_order_placeholder(self, row: dict) -> None:
        # placeholder uses order_id = CLIENT:<client_order_id>
        self.upsert_orders([row])

    def expire_stuck_pending(self, exchange_id: int, account_id: int, timeout_sec: int) -> int:
        q = """UPDATE orders
               SET status='EXPIRED', updated_at=now(), source='oms_reconcile'
               WHERE exchange_id=%s AND account_id=%s
                 AND status='PENDING_SUBMIT'
                 AND updated_at < now() - (%s || ' seconds')::interval"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (exchange_id, account_id, int(timeout_sec)))
                n = cur.rowcount
            conn.commit()
        return int(n)

    def resolve_placeholders_with_open_orders(self, exchange_id: int, account_id: int, open_orders: list[dict]) -> int:
        # For each open order, if there is a placeholder CLIENT:<cid>, copy row to real order_id and mark OPEN
        # Then mark placeholder as EXPIRED (or keep as history). We'll mark as EXPIRED for simplicity.
        n = 0
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                for oo in open_orders:
                    cid = oo.get("client_order_id")
                    oid = oo.get("order_id")
                    if not cid or not oid:
                        continue
                    # find placeholder
                    cur.execute(
                        "SELECT symbol_id, strategy_id, pos_uid, qty, price, reduce_only, created_at "
                        "FROM orders WHERE exchange_id=%s AND account_id=%s AND order_id=%s LIMIT 1",
                        (exchange_id, account_id, f"CLIENT:{cid}")
                    )
                    ph = cur.fetchone()
                    if not ph:
                        continue
                    symbol_id, strategy_id, pos_uid, qty, price, reduce_only, created_at = ph
                    # upsert real row
                    cur.execute(
                        """INSERT INTO orders(exchange_id, account_id, order_id, symbol_id, strategy_id, pos_uid,
                               client_order_id, side, type, reduce_only, price, qty, filled_qty, status, created_at, updated_at, source)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),'oms_reconcile')
                           ON CONFLICT (exchange_id, account_id, order_id)
                           DO UPDATE SET
                             client_order_id=EXCLUDED.client_order_id,
                             filled_qty=EXCLUDED.filled_qty,
                             status=EXCLUDED.status,
                             updated_at=EXCLUDED.updated_at,
                             source=EXCLUDED.source""",
                        (exchange_id, account_id, oid, symbol_id, strategy_id, pos_uid, cid,
                         oo.get("side"), oo.get("type"), reduce_only,
                         oo.get("price"), oo.get("qty"), oo.get("filled_qty"),
                         oo.get("status") or "OPEN", created_at)
                    )
                    # expire placeholder
                    cur.execute(
                        "UPDATE orders SET status='EXPIRED', updated_at=now(), source='oms_reconcile' "
                        "WHERE exchange_id=%s AND account_id=%s AND order_id=%s",
                        (exchange_id, account_id, f"CLIENT:{cid}")
                    )
                    n += 1
            conn.commit()
        return int(n)

    # risk helper
    def get_today_realized_pnl(self, exchange_id: int, account_id: int) -> float:
        q = """SELECT COALESCE(SUM(realized_pnl), 0)
               FROM trades
               WHERE exchange_id=%s AND account_id=%s
                 AND ts >= date_trunc('day', now() AT TIME ZONE 'UTC')"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (exchange_id, account_id))
                row = cur.fetchone()
        return float(row[0] or 0.0)

    def retention_cleanup(self, *, candles_days:int, snapshots_days:int, funding_days:int,
                      orders_days:int, trades_days:int, fills_days:int,
                      balance_days:int = 30,
                      balance_5m_days:int = 180, balance_1h_days:int = 1825,
                      oi_5m_days:int = 90, oi_15m_days:int = 180, oi_1h_days:int = 365,
                      dry_run:bool = False) -> dict:
    """Delete old rows to keep DB bounded. Safe to run periodically."""
    stmts = [
        ("candles", "DELETE FROM candles WHERE open_time < now() - (%s || ' days')::interval", (int(candles_days),)),
        ("price_snapshots", "DELETE FROM price_snapshots WHERE ts < now() - (%s || ' days')::interval", (int(snapshots_days),)),
        ("account_balance_snapshots", "DELETE FROM account_balance_snapshots WHERE ts < now() - (%s || ' days')::interval", (int(balance_days),)),
        ("account_balance_5m", "DELETE FROM account_balance_5m WHERE ts < now() - (%s || ' days')::interval", (int(balance_5m_days),)),
        ("account_balance_1h", "DELETE FROM account_balance_1h WHERE ts < now() - (%s || ' days')::interval", (int(balance_1h_days),)),
        ("open_interest_5m", "DELETE FROM open_interest WHERE interval='5m' AND ts < now() - (%s || ' days')::interval", (int(oi_5m_days),)),
        ("open_interest_15m", "DELETE FROM open_interest WHERE interval='15m' AND ts < now() - (%s || ' days')::interval", (int(oi_15m_days),)),
        ("open_interest_1h", "DELETE FROM open_interest WHERE interval='1h' AND ts < now() - (%s || ' days')::interval", (int(oi_1h_days),)),
        ("funding", "DELETE FROM funding WHERE funding_time < now() - (%s || ' days')::interval", (int(funding_days),)),
        ("orders", "DELETE FROM orders WHERE updated_at IS NOT NULL AND updated_at < now() - (%s || ' days')::interval", (int(orders_days),)),
        ("trades", "DELETE FROM trades WHERE ts < now() - (%s || ' days')::interval", (int(trades_days),)),
        ("fills", "DELETE FROM order_fills WHERE ts < now() - (%s || ' days')::interval", (int(fills_days),)),
    ]

    out: dict[str,int] = {}
    with self.pool.connection() as conn:
        with conn.cursor() as cur:
            for name, sql, params in stmts:
                if dry_run:
                    cur.execute("SELECT count(*) FROM (" + sql.replace("DELETE", "SELECT 1") + ") t", params)
                    out[name] = int(cur.fetchone()[0])
                else:
                    cur.execute(sql, params)
                    out[name] = int(cur.rowcount or 0)
        if not dry_run:
            conn.commit()
    return out

def downsample_balance(self, *, exchange_id: int, account_id: int) -> dict:
    """Downsample raw account_balance_snapshots into 5m and 1h buckets using LAST snapshot in bucket."""
    res = {"5m": 0, "1h": 0}
    with self.pool.connection() as conn:
        with conn.cursor() as cur:
            # 5m
            cur.execute("""
            INSERT INTO account_balance_5m(exchange_id, ts, wallet_balance, equity, available_balance, margin_used, unrealized_pnl)
            SELECT exchange_id,
                   bucket_ts AS ts,
                   wallet_balance, equity, available_balance, margin_used, unrealized_pnl
            FROM (
                SELECT DISTINCT ON (exchange_id, bucket_ts)
                    exchange_id,
                    (date_trunc('minute', ts) - (EXTRACT(minute FROM ts)::int % 5) * interval '1 minute') AS bucket_ts,
                    wallet_balance, equity, available_balance, margin_used, unrealized_pnl,
                    ts AS src_ts
                FROM account_balance_snapshots
                WHERE exchange_id=%s AND account_id=%s
                ORDER BY exchange_id, bucket_ts, ts DESC
            ) s
            ON CONFLICT (exchange_id, ts) DO UPDATE SET
                wallet_balance=EXCLUDED.wallet_balance,
                equity=EXCLUDED.equity,
                available_balance=EXCLUDED.available_balance,
                margin_used=EXCLUDED.margin_used,
                unrealized_pnl=EXCLUDED.unrealized_pnl
            """, (int(exchange_id), int(account_id)))
            res["5m"] = int(cur.rowcount or 0)

            # 1h
            cur.execute("""
            INSERT INTO account_balance_1h(exchange_id, ts, wallet_balance, equity, available_balance, margin_used, unrealized_pnl)
            SELECT exchange_id,
                   bucket_ts AS ts,
                   wallet_balance, equity, available_balance, margin_used, unrealized_pnl
            FROM (
                SELECT DISTINCT ON (exchange_id, bucket_ts)
                    exchange_id,
                    date_trunc('hour', ts) AS bucket_ts,
                    wallet_balance, equity, available_balance, margin_used, unrealized_pnl,
                    ts AS src_ts
                FROM account_balance_snapshots
                WHERE exchange_id=%s AND account_id=%s
                ORDER BY exchange_id, bucket_ts, ts DESC
            ) s
            ON CONFLICT (exchange_id, ts) DO UPDATE SET
                wallet_balance=EXCLUDED.wallet_balance,
                equity=EXCLUDED.equity,
                available_balance=EXCLUDED.available_balance,
                margin_used=EXCLUDED.margin_used,
                unrealized_pnl=EXCLUDED.unrealized_pnl
            """, (int(exchange_id), int(account_id)))
            res["1h"] = int(cur.rowcount or 0)
        conn.commit()
    return res

def upsert_positions(self, rows: Iterable[dict]) -> None:
        rows = list(rows)
        if not rows: return

        # v9.1: back-compat defaults (so older callers can still upsert)
        for r in rows:
            r.setdefault("avg_price", r.get("entry_price"))
            r.setdefault("exit_price", None)
            # prefer avg_price for value; fall back to mark_price then entry_price
            px_val = r.get("avg_price") or r.get("mark_price") or r.get("entry_price") or 0.0
            qty_val = r.get("qty") or 0.0
            r.setdefault("position_value_usdt", float(qty_val) * float(px_val))
            r.setdefault("leverage", None)
            r.setdefault("status", "OPEN")
            r.setdefault("opened_at", r.get("updated_at"))
            r.setdefault("closed_at", None)
            r.setdefault("realized_pnl", None)

            r.setdefault("stop_loss_1", None)
            r.setdefault("stop_loss_2", None)
            r.setdefault("take_profit_1", None)
            r.setdefault("take_profit_2", None)
            r.setdefault("take_profit_3", None)
            r.setdefault("scale_in_count", 0)
            r.setdefault("strategy_name", r.get("strategy_id", "unknown"))
        q = """INSERT INTO positions(exchange_id, account_id, symbol_id, strategy_id, strategy_name, pos_uid,
                 side, qty, entry_price, avg_price, exit_price, mark_price,
                 position_value_usdt, leverage,
                 stop_loss_1, stop_loss_2,
                 take_profit_1, take_profit_2, take_profit_3,
                 scale_in_count,
                 status, opened_at, closed_at,
                 unrealized_pnl, realized_pnl,
                 updated_at, source)
               VALUES (%(exchange_id)s, %(account_id)s, %(symbol_id)s, %(strategy_id)s, %(strategy_name)s, %(pos_uid)s,
                 %(side)s, %(qty)s, %(entry_price)s, %(avg_price)s, %(exit_price)s, %(mark_price)s,
                 %(position_value_usdt)s, %(leverage)s,
                 %(stop_loss_1)s, %(stop_loss_2)s,
                 %(take_profit_1)s, %(take_profit_2)s, %(take_profit_3)s,
                 %(scale_in_count)s,
                 %(status)s, %(opened_at)s, %(closed_at)s,
                 %(unrealized_pnl)s, %(realized_pnl)s,
                 %(updated_at)s, %(source)s)
               ON CONFLICT (exchange_id, account_id, symbol_id)
               DO UPDATE SET
                 strategy_id=EXCLUDED.strategy_id,
                 strategy_name=EXCLUDED.strategy_name,
                 pos_uid=EXCLUDED.pos_uid,
                 side=EXCLUDED.side,
                 qty=EXCLUDED.qty,
                 entry_price=EXCLUDED.entry_price,
                 avg_price=EXCLUDED.avg_price,
                 exit_price=EXCLUDED.exit_price,
                 mark_price=EXCLUDED.mark_price,
                 position_value_usdt=EXCLUDED.position_value_usdt,
                 leverage=EXCLUDED.leverage,
                 stop_loss_1=EXCLUDED.stop_loss_1,
                 stop_loss_2=EXCLUDED.stop_loss_2,
                 take_profit_1=EXCLUDED.take_profit_1,
                 take_profit_2=EXCLUDED.take_profit_2,
                 take_profit_3=EXCLUDED.take_profit_3,
                 scale_in_count=EXCLUDED.scale_in_count,
                 status=EXCLUDED.status,
                 opened_at=EXCLUDED.opened_at,
                 closed_at=EXCLUDED.closed_at,
                 unrealized_pnl=EXCLUDED.unrealized_pnl,
                 realized_pnl=EXCLUDED.realized_pnl,
                 updated_at=EXCLUDED.updated_at,
                 source=EXCLUDED.source"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(q, rows)
            conn.commit()

    def upsert_orders(self, rows: Iterable[dict]) -> None:
        rows = list(rows)
        if not rows: return
        q = """INSERT INTO orders(exchange_id, account_id, order_id, symbol_id, strategy_id, pos_uid,
                 client_order_id, side, type, reduce_only, price, qty, filled_qty, status,
                 created_at, updated_at, source)
               VALUES (%(exchange_id)s, %(account_id)s, %(order_id)s, %(symbol_id)s, %(strategy_id)s, %(pos_uid)s,
                 %(client_order_id)s, %(side)s, %(type)s, %(reduce_only)s, %(price)s, %(qty)s, %(filled_qty)s, %(status)s,
                 %(created_at)s, %(updated_at)s, %(source)s)
               ON CONFLICT (exchange_id, account_id, order_id)
               DO UPDATE SET strategy_id=COALESCE(EXCLUDED.strategy_id, orders.strategy_id),
                 pos_uid=COALESCE(EXCLUDED.pos_uid, orders.pos_uid),
                 client_order_id=COALESCE(EXCLUDED.client_order_id, orders.client_order_id),
                 side=COALESCE(EXCLUDED.side, orders.side),
                 type=COALESCE(EXCLUDED.type, orders.type),
                 reduce_only=COALESCE(EXCLUDED.reduce_only, orders.reduce_only),
                 price=COALESCE(EXCLUDED.price, orders.price),
                 qty=COALESCE(EXCLUDED.qty, orders.qty),
                 filled_qty=COALESCE(EXCLUDED.filled_qty, orders.filled_qty),
                 status=COALESCE(EXCLUDED.status, orders.status),
                 created_at=COALESCE(EXCLUDED.created_at, orders.created_at),
                 updated_at=COALESCE(EXCLUDED.updated_at, orders.updated_at),
                 source=EXCLUDED.source"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(q, rows)
            conn.commit()

    def upsert_trades(self, rows: Iterable[dict]) -> None:
        rows = list(rows)
        if not rows: return
        q = """INSERT INTO trades(exchange_id, account_id, trade_id, order_id, symbol_id, strategy_id, pos_uid,
                 side, price, qty, fee, fee_asset, realized_pnl, ts, source)
               VALUES (%(exchange_id)s, %(account_id)s, %(trade_id)s, %(order_id)s, %(symbol_id)s, %(strategy_id)s, %(pos_uid)s,
                 %(side)s, %(price)s, %(qty)s, %(fee)s, %(fee_asset)s, %(realized_pnl)s, %(ts)s, %(source)s)
               ON CONFLICT (exchange_id, account_id, trade_id)
               DO UPDATE SET order_id=COALESCE(EXCLUDED.order_id, trades.order_id),
                 strategy_id=COALESCE(EXCLUDED.strategy_id, trades.strategy_id),
                 pos_uid=COALESCE(EXCLUDED.pos_uid, trades.pos_uid),
                 side=COALESCE(EXCLUDED.side, trades.side),
                 price=COALESCE(EXCLUDED.price, trades.price),
                 qty=COALESCE(EXCLUDED.qty, trades.qty),
                 fee=COALESCE(EXCLUDED.fee, trades.fee),
                 fee_asset=COALESCE(EXCLUDED.fee_asset, trades.fee_asset),
                 realized_pnl=COALESCE(EXCLUDED.realized_pnl, trades.realized_pnl),
                 ts=EXCLUDED.ts,
                 source=EXCLUDED.source"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(q, rows)
            conn.commit()

    def upsert_order_fills(self, rows: Iterable[dict]) -> None:
        rows = list(rows)
        if not rows: return
        q = """INSERT INTO order_fills(exchange_id, account_id, fill_uid, symbol_id, order_id, trade_id,
                 client_order_id, price, qty, realized_pnl, ts, source)
               VALUES (%(exchange_id)s, %(account_id)s, %(fill_uid)s, %(symbol_id)s, %(order_id)s, %(trade_id)s,
                 %(client_order_id)s, %(price)s, %(qty)s, %(realized_pnl)s, %(ts)s, %(source)s)
               ON CONFLICT (exchange_id, account_id, fill_uid)
               DO UPDATE SET price=COALESCE(EXCLUDED.price, order_fills.price),
                 qty=COALESCE(EXCLUDED.qty, order_fills.qty),
                 realized_pnl=COALESCE(EXCLUDED.realized_pnl, order_fills.realized_pnl),
                 ts=EXCLUDED.ts,
                 source=EXCLUDED.source"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(q, rows)
            conn.commit()

    def upsert_candles(self, rows: Iterable[dict]) -> None:
        rows = list(rows)
        if not rows: return
        q = """INSERT INTO candles(exchange_id, symbol_id, interval, open_time, open, high, low, close, volume, source)
               VALUES (%(exchange_id)s, %(symbol_id)s, %(interval)s, %(open_time)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(source)s)
               ON CONFLICT (exchange_id, symbol_id, interval, open_time)
               DO UPDATE SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume, source=EXCLUDED.source"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(q, rows)
            conn.commit()

    def upsert_funding(self, rows: Iterable[dict]) -> None:
        rows = list(rows)
        if not rows: return
        q = """INSERT INTO funding(exchange_id, symbol_id, funding_time, funding_rate, mark_price, source)
               VALUES (%(exchange_id)s, %(symbol_id)s, %(funding_time)s, %(funding_rate)s, %(mark_price)s, %(source)s)
               ON CONFLICT (exchange_id, symbol_id, funding_time)
               DO UPDATE SET funding_rate=EXCLUDED.funding_rate, mark_price=EXCLUDED.mark_price, source=EXCLUDED.source"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(q, rows)
            conn.commit()

    def insert_snapshots(self, rows: Iterable[dict]) -> None:
        rows = list(rows)
        if not rows: return
        q = """INSERT INTO price_snapshots(exchange_id, symbol_id, price, price_type, ts, source)
               VALUES (%(exchange_id)s, %(symbol_id)s, %(price)s, %(price_type)s, %(ts)s, %(source)s)"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(q, rows)
            conn.commit()


    # --- v9 Market State Layer ---
    def insert_account_balance_snapshots(self, rows: Iterable[dict]) -> None:
        rows = list(rows)
        if not rows:
            return
        q = """INSERT INTO account_balance_snapshots(exchange_id, account_id, ts,
                 wallet_balance, equity, available_balance, margin_used, unrealized_pnl, source)
               VALUES (%(exchange_id)s, %(account_id)s, %(ts)s,
                 %(wallet_balance)s, %(equity)s, %(available_balance)s, %(margin_used)s, %(unrealized_pnl)s, %(source)s)
               ON CONFLICT (exchange_id, account_id, ts)
               DO UPDATE SET wallet_balance=EXCLUDED.wallet_balance, equity=EXCLUDED.equity,
                 available_balance=EXCLUDED.available_balance, margin_used=EXCLUDED.margin_used,
                 unrealized_pnl=EXCLUDED.unrealized_pnl, source=EXCLUDED.source"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(q, rows)
            conn.commit()

    def upsert_open_interest(self, rows: Iterable[dict]) -> None:
        rows = list(rows)
        if not rows:
            return
        q = """INSERT INTO open_interest(exchange_id, symbol_id, interval, ts,
                 open_interest, open_interest_value, source)
               VALUES (%(exchange_id)s, %(symbol_id)s, %(interval)s, %(ts)s,
                 %(open_interest)s, %(open_interest_value)s, %(source)s)
               ON CONFLICT (exchange_id, symbol_id, interval, ts)
               DO UPDATE SET open_interest=EXCLUDED.open_interest,
                 open_interest_value=EXCLUDED.open_interest_value,
                 source=EXCLUDED.source"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(q, rows)
            conn.commit()

    def get_latest_account_balance_snapshot(self, exchange_id: int, account_id: int) -> dict | None:
        q = """SELECT ts, wallet_balance, equity, available_balance, margin_used, unrealized_pnl
               FROM account_balance_snapshots
               WHERE exchange_id=%s AND account_id=%s
               ORDER BY ts DESC
               LIMIT 1"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (exchange_id, account_id))
                row = cur.fetchone()
        if not row:
            return None
        ts, wallet, equity, avail, margin_used, unreal = row
        return {
            "ts": ts,
            "wallet_balance": float(wallet),
            "equity": float(equity),
            "available_balance": float(avail),
            "margin_used": float(margin_used),
            "unrealized_pnl": float(unreal),
        }

    def get_equity_peak(self, exchange_id: int, account_id: int, lookback_days: int = 90) -> float | None:
        q = """SELECT max(equity)
               FROM account_balance_snapshots
               WHERE exchange_id=%s AND account_id=%s
                 AND ts >= now() - (%s || ' days')::interval"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (exchange_id, account_id, int(lookback_days)))
                row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return float(row[0])

    # --- v9.2 Position Ledger (fills-driven) ---

    def upsert_position_ledger_open(self, row: dict) -> None:
        """Create/refresh an OPEN ledger row for a pos_uid (idempotent)."""
        q = """INSERT INTO position_ledger(exchange_id, account_id, pos_uid, symbol_id, strategy_id, strategy_name,
                    side, status, opened_at, entry_price, avg_price, qty_opened, qty_current, position_value_usdt,
                    scale_in_count, realized_pnl, fees, updated_at, source)
               VALUES (%(exchange_id)s, %(account_id)s, %(pos_uid)s, %(symbol_id)s, %(strategy_id)s, %(strategy_name)s,
                    %(side)s, %(status)s, %(opened_at)s, %(entry_price)s, %(avg_price)s, %(qty_opened)s, %(qty_current)s,
                    %(position_value_usdt)s, %(scale_in_count)s, %(realized_pnl)s, %(fees)s, %(updated_at)s, %(source)s)
               ON CONFLICT (exchange_id, account_id, pos_uid)
               DO UPDATE SET
                 strategy_id=COALESCE(EXCLUDED.strategy_id, position_ledger.strategy_id),
                 strategy_name=COALESCE(EXCLUDED.strategy_name, position_ledger.strategy_name),
                 side=COALESCE(EXCLUDED.side, position_ledger.side),
                 status=COALESCE(EXCLUDED.status, position_ledger.status),
                 entry_price=COALESCE(EXCLUDED.entry_price, position_ledger.entry_price),
                 avg_price=COALESCE(EXCLUDED.avg_price, position_ledger.avg_price),
                 qty_opened=GREATEST(position_ledger.qty_opened, EXCLUDED.qty_opened),
                 qty_current=COALESCE(EXCLUDED.qty_current, position_ledger.qty_current),
                 position_value_usdt=COALESCE(EXCLUDED.position_value_usdt, position_ledger.position_value_usdt),
                 scale_in_count=GREATEST(position_ledger.scale_in_count, EXCLUDED.scale_in_count),
                 updated_at=EXCLUDED.updated_at,
                 source=EXCLUDED.source"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, row)
            conn.commit()

    def apply_fill_to_position_ledger(self, *, exchange_id:int, account_id:int, symbol_id:int,
                                     strategy_id:str, strategy_name:str, pos_uid:str,
                                     fill_side:str, price:float, qty:float,
                                     realized_pnl:float|None, fee:float|None,
                                     ts, source:str='ws_user') -> dict:
        """Apply one execution fill to position_ledger.

        Rules:
          - Uses BUY/SELL fill_side; ledger side is LONG/SHORT.
          - Weighted avg_price updates only when exposure increases (scale-in).
          - realized_pnl accumulates on reduce/close events.
          - When position reaches 0 -> status=CLOSED with exit_price=fill price.
        Returns small dict with {status, side, qty_current, avg_price, exit_price, closed_at}.
        """
        from datetime import datetime, timezone

        if qty is None or float(qty) <= 0:
            return {}

        now_ts = ts
        # Normalize timestamps
        if isinstance(now_ts, (int, float)):
            now_ts = datetime.fromtimestamp(float(now_ts), tz=timezone.utc)
        if isinstance(now_ts, datetime) and now_ts.tzinfo is None:
            now_ts = now_ts.replace(tzinfo=timezone.utc)

        fill_side_u = (fill_side or '').upper()
        delta_signed = float(qty) if fill_side_u == 'BUY' else -float(qty)

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT side, status, opened_at, entry_price, avg_price, qty_current, qty_opened,
                              realized_pnl, fees, scale_in_count
                         FROM position_ledger
                        WHERE exchange_id=%s AND account_id=%s AND pos_uid=%s
                        FOR UPDATE""",
                    (exchange_id, account_id, pos_uid)
                )
                row = cur.fetchone()

                if row is None:
                    # Create an OPEN row, infer side from delta
                    side = 'LONG' if delta_signed > 0 else 'SHORT'
                    qty_current_signed = delta_signed
                    qty_current_abs = abs(qty_current_signed)
                    entry = float(price) if price is not None else None
                    avg = entry
                    q_ins = """INSERT INTO position_ledger(exchange_id, account_id, pos_uid, symbol_id,
                                   strategy_id, strategy_name, side, status, opened_at,
                                   entry_price, avg_price, qty_opened, qty_current, qty_closed,
                                   position_value_usdt, scale_in_count, realized_pnl, fees, updated_at, source)
                               VALUES (%s,%s,%s,%s,%s,%s,%s,'OPEN',%s,
                                   %s,%s,%s,%s,0,
                                   %s,0,%s,%s,%s,%s)"""
                    cur.execute(q_ins, (
                        exchange_id, account_id, pos_uid, symbol_id,
                        strategy_id, strategy_name, side, now_ts,
                        entry, avg, qty_current_abs, qty_current_abs,
                        (qty_current_abs * (avg or 0.0)),
                        float(realized_pnl or 0.0),
                        float(fee or 0.0),
                        now_ts, source
                    ))
                    conn.commit()
                    return {"status":"OPEN","side":side,"qty_current":qty_current_abs,"avg_price":avg}

                side, status, opened_at, entry_price, avg_price, qty_current, qty_opened, rp, fees, scale_in_count = row
                qty_current = float(qty_current or 0.0)

                # signed qty according to ledger side
                qty_signed = qty_current if side == 'LONG' else -qty_current
                new_qty_signed = qty_signed + delta_signed

                # Determine if exposure increased (scale-in)
                increased = abs(new_qty_signed) > abs(qty_signed) and abs(qty_signed) > 0
                opened_new = abs(qty_signed) == 0 and abs(new_qty_signed) > 0

                # Weighted average update only on exposure increase
                new_avg = float(avg_price or entry_price or price or 0.0)
                new_entry = float(entry_price or price or 0.0)

                if opened_new:
                    new_entry = float(price or 0.0)
                    new_avg = new_entry
                elif increased:
                    old_notional = abs(qty_signed) * float(avg_price or entry_price or 0.0)
                    add_notional = abs(delta_signed) * float(price or 0.0)
                    denom = abs(new_qty_signed)
                    if denom > 0:
                        new_avg = (old_notional + add_notional) / denom
                    scale_in_count = int(scale_in_count or 0) + 1

                new_qty_abs = abs(new_qty_signed)

                # Accumulate realized pnl and fees
                rp_new = float(rp or 0.0) + float(realized_pnl or 0.0)
                fees_new = float(fees or 0.0) + float(fee or 0.0)

                new_status = status or 'OPEN'
                exit_price = None
                closed_at = None
                qty_closed_delta = 0.0

                if new_qty_abs == 0.0 and abs(qty_signed) > 0:
                    new_status = 'CLOSED'
                    exit_price = float(price or 0.0)
                    closed_at = now_ts
                    qty_closed_delta = abs(qty_signed)  # everything closed
                elif abs(new_qty_signed) < abs(qty_signed):
                    # partial reduce
                    new_status = 'PARTIAL'
                    qty_closed_delta = abs(qty_signed) - abs(new_qty_signed)

                # If sign flips (rare in isolated logic), mark FLIPPED and close; next fill should open new pos_uid
                if abs(qty_signed) > 0 and new_qty_abs > 0 and (qty_signed > 0) != (new_qty_signed > 0):
                    new_status = 'FLIPPED'
                    exit_price = float(price or 0.0)
                    closed_at = now_ts
                    qty_closed_delta = abs(qty_signed)

                q_upd = """UPDATE position_ledger
                            SET status=%s,
                                entry_price=%s,
                                avg_price=%s,
                                exit_price=COALESCE(%s, exit_price),
                                opened_at=COALESCE(opened_at, %s),
                                closed_at=COALESCE(%s, closed_at),
                                qty_opened=GREATEST(qty_opened, %s),
                                qty_current=%s,
                                qty_closed=qty_closed + %s,
                                position_value_usdt=%s,
                                scale_in_count=%s,
                                realized_pnl=%s,
                                fees=%s,
                                updated_at=%s,
                                source=%s
                          WHERE exchange_id=%s AND account_id=%s AND pos_uid=%s"""
                cur.execute(q_upd, (
                    new_status,
                    new_entry if new_entry != 0.0 else None,
                    new_avg if new_avg != 0.0 else None,
                    exit_price,
                    now_ts,
                    closed_at,
                    max(float(qty_opened or 0.0), new_qty_abs),
                    new_qty_abs,
                    float(qty_closed_delta or 0.0),
                    new_qty_abs * float(new_avg or new_entry or price or 0.0),
                    int(scale_in_count or 0),
                    rp_new,
                    fees_new,
                    now_ts,
                    source,
                    exchange_id, account_id, pos_uid
                ))
            conn.commit()

        return {"status": new_status, "side": side, "qty_current": new_qty_abs, "avg_price": new_avg,
                "exit_price": exit_price, "closed_at": closed_at}

    def close_position_ledger_if_open(self, *, exchange_id:int, account_id:int, pos_uid:str,
                                     closed_at, exit_price:float|None=None, source:str='rest') -> bool:
        from datetime import datetime, timezone
        ts = closed_at
        if isinstance(ts, (int,float)):
            ts = datetime.fromtimestamp(float(ts), tz=timezone.utc)
        if isinstance(ts, datetime) and ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        q = """UPDATE position_ledger
                    SET status='CLOSED',
                        closed_at=COALESCE(closed_at, %s),
                        exit_price=COALESCE(%s, exit_price),
                        qty_current=0,
                        updated_at=%s,
                        source=%s
                  WHERE exchange_id=%s AND account_id=%s AND pos_uid=%s
                    AND status IN ('OPEN','PARTIAL')"""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(q, (ts, exit_price, ts, source, exchange_id, account_id, pos_uid))
                changed = cur.rowcount > 0
            conn.commit()
        return changed
