# src/platform/data/storage/postgres/storage.py
from __future__ import annotations

import json
import hashlib
import time
import logging
from datetime import datetime,timezone,timedelta
from typing import Any,Iterable,Sequence,Mapping,List,Tuple,Dict,Optional
from psycopg.types.json import Jsonb
from psycopg2.extras import execute_values



from psycopg import sql as _sql

from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)

def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)

log = logging.getLogger("storage.sql")


class PostgreSQLStorage:

    @staticmethod
    def _safe_int(v, default=None):
        try:
            return int(v) if v is not None else default
        except Exception:
            return default

    @staticmethod
    def _safe_str(v, default=None):
        try:
            if v is None:
                return default
            s = str(v)
            return s if s else default
        except Exception:
            return default

    @staticmethod
    def _safe_float(v, default=None):
        try:
            return float(v) if v is not None else default
        except Exception:
            return default

    @staticmethod
    def _mk_fill_uid(
            *,
            exchange_id: int,
            account_id: int,
            symbol_id: int,
            order_id: str | None,
            trade_id: str | None,
            ts_ms: int | None,
            price,
            qty,
    ) -> str:
        """
        Детерминированный UID для fill.
        """
        if trade_id:
            return f"tid:{trade_id}"

        base = {
            "ex": exchange_id,
            "acc": account_id,
            "sym": symbol_id,
            "oid": order_id,
            "ts": ts_ms,
            "price": str(price),
            "qty": str(qty),
        }

        raw = json.dumps(base, sort_keys=True, ensure_ascii=False).encode("utf-8")
        h = hashlib.sha1(raw).hexdigest()
        return f"h:{h}"

    """
    PostgreSQL storage — registry, market state, OMS helpers, retention, exchangeInfo,
    positions, balances snapshots, orders, trades, stats.
    """

    def __init__(self, pool: ConnectionPool):
        self.pool = pool
        self.logger = logging.getLogger("storage.postgres")
    # ======================================================================
    # HELPERS
    # ======================================================================

    def _exec_many(self, query: str, rows: list[dict], *, chunk_size: int = 500) -> int:
        """
        Execute executemany() safely with rollback+logging.

        Returns:
          affected rows (best-effort). If driver doesn't provide rowcount reliably,
          returns number of attempted rows.
        """
        if not rows:
            return 0

        # chunking to avoid huge packets and easier debugging
        total_attempted = 0
        total_affected: Optional[int] = 0

        def chunks(xs: list[dict], n: int) -> Iterable[list[dict]]:
            if n <= 0:
                yield xs
                return
            for i in range(0, len(xs), n):
                yield xs[i: i + n]

        with self.pool.connection() as conn:
            try:
                with conn.cursor() as cur:
                    for part in chunks(rows, int(chunk_size)):
                        total_attempted += len(part)
                        cur.executemany(query, part)

                        # rowcount в executemany зависит от драйвера/режима
                        rc = getattr(cur, "rowcount", None)
                        if isinstance(rc, int) and rc >= 0:
                            total_affected = (total_affected or 0) + rc
                        else:
                            total_affected = None

                conn.commit()

            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass

                # Логируем полезный контекст
                sample = rows[0] if rows else None
                sample_keys = sorted(list(sample.keys()))[:60] if isinstance(sample, dict) else None

                log.exception(
                    "[DB][_exec_many] FAILED: rows=%s chunk_size=%s err=%r sample_keys=%s sample_row=%r",
                    len(rows), chunk_size, e, sample_keys, sample
                )
                raise

        # если rowcount неинформативен — вернём попытку
        return int(total_affected) if total_affected is not None else int(total_attempted)

    def _exec_one_fetchone(self, query: str, params: Any = None):
        """
        For SELECT ... returning 1 row.
        Does NOT commit.
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params or ())
                return cur.fetchone()

    def _exec_one_commit(self, query: str, params: Any = None) -> int:
        """
        For INSERT/UPDATE/DELETE single statement.
        Commits and returns rowcount best-effort.
        """
        with self.pool.connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(query, params or ())
                    rc = getattr(cur, "rowcount", None)
                conn.commit()
                return int(rc) if isinstance(rc, int) and rc >= 0 else 1
            except Exception as e:
                try:
                    conn.rollback()
                except Exception:
                    pass
                log.exception("[DB][_exec_one_commit] FAILED err=%r query=%r params=%r", e, query[:500], params)
                raise

    def _exec_one(self, query: str, params: Any = None):
        return self._exec_one_fetchone(query, params=params)

    # ======================================================================
    # IDS / REGISTRY (used by run_instances.py)
    # ======================================================================

    def ensure_exchange_account_symbol(
        self,
        *,
        exchange: str,
        account: str,
        symbols: list[str],
    ) -> tuple[int, int, dict[str, int]]:

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

        return int(exchange_id), int(account_id), symbol_ids


    def ensure_exchange_account_symbol_map(
            self,
            *,
            exchange: str,
            account: str,
            symbols: list[str],
    ) -> dict:
        """
        Backward-compatible wrapper.
        """
        exchange_id, account_id, symbol_ids = self.ensure_exchange_account_symbol(
            exchange=exchange,
            account=account,
            symbols=symbols,
        )
        out = {"_exchange_id": exchange_id, "_account_id": account_id}
        out.update(symbol_ids)
        return out

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

        # ✅ Деактивация отсутствующих в universe:
        #   - is_active = FALSE
        #   - status = 'DELISTED'
        #   - last_seen_at гарантированно не NULL
        sql_deactivate_missing = """
                                 UPDATE symbols
                                 SET is_active    = FALSE,
                                     status       = 'DELISTED',
                                     last_seen_at = COALESCE(last_seen_at, NOW())
                                 WHERE exchange_id = %(exchange_id)s
                                   AND is_active = TRUE
                                   AND NOT (symbol = ANY (%(symbols)s::text[])); \
                                 """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql_upsert,
                    {
                        "exchange_id": int(exchange_id),
                        "symbols": symbols_norm,
                    },
                )
                rows = cur.fetchall() or []

                if deactivate_missing:
                    cur.execute(
                        sql_deactivate_missing,
                        {
                            "exchange_id": int(exchange_id),
                            "symbols": symbols_norm,
                        },
                    )

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
    # POSITIONS (FIXED)
    # ======================================================================

    def upsert_position(self, position: Any) -> int:
        return self.upsert_positions([position])

    @staticmethod
    def _norm_side(v: Any) -> str:
        s = str(v or "").upper().strip()
        if s in ("LONG", "SHORT", "FLAT"):
            return s
        return "FLAT"

    @staticmethod
    def _f(x: Any, default: float = 0.0) -> float:
        try:
            v = float(x)
            if v != v:  # NaN
                return float(default)
            return v
        except Exception:
            return float(default)

    def _normalize_positions_row(self, row: dict) -> dict:
        """
        Делает row безопасным для constraints таблицы positions:

        chk_flat_consistency:
          (side='FLAT' AND qty=0 AND status='CLOSED')
          OR (side IN ('LONG','SHORT') AND qty>0 AND status='OPEN')

        + гарантирует наличие ключей, которые ожидает SQL.
        """
        if not isinstance(row, dict):
            raise TypeError("positions row must be dict")

        now = _utcnow()

        # обязательные идентификаторы
        row["exchange_id"] = int(row.get("exchange_id") or 0)
        row["account_id"] = int(row.get("account_id") or 0)
        row["symbol_id"] = int(row.get("symbol_id") or 0)

        # стратегия
        row.setdefault("strategy_id", "unknown")
        row.setdefault("strategy_name", None)

        # lifecycle keys (ключи должны быть)
        row.setdefault("pos_uid", "")
        row.setdefault("opened_at", None)
        row.setdefault("closed_at", None)

        # торговые поля
        side = self._norm_side(row.get("side"))
        qty = abs(self._f(row.get("qty"), 0.0))

        if qty <= 0.0:
            row["side"] = "FLAT"
            row["qty"] = 0.0
            row["status"] = "CLOSED"

            # при CLOSED можно выставлять нули (так удобнее для аналитики)
            row.setdefault("entry_price", 0.0)
            row.setdefault("avg_price", 0.0)
            row.setdefault("exit_price", 0.0)
            row.setdefault("mark_price", 0.0)
            row.setdefault("unrealized_pnl", 0.0)
        else:
            if side not in ("LONG", "SHORT"):
                side = "LONG"
            row["side"] = side
            row["qty"] = float(qty)
            row["status"] = "OPEN"

            row.setdefault("entry_price", None)
            row.setdefault("avg_price", None)
            row.setdefault("exit_price", None)
            row.setdefault("mark_price", None)
            row.setdefault("unrealized_pnl", None)

        row.setdefault("position_value_usdt", None)
        row.setdefault("scale_in_count", 0)
        row.setdefault("realized_pnl", 0.0)
        row.setdefault("fees", 0.0)

        row.setdefault("last_trade_id", None)
        row.setdefault("last_ts", None)

        # ✅ updated_at NOT NULL — гарантируем
        if row.get("updated_at") is None:
            row["updated_at"] = now

        row.setdefault("source", "position_aggregate")

        # защита от None в NOT NULL side/status уже обеспечена выше
        return row

    def upsert_positions(self, rows):
        if not rows:
            return 0

        norm_rows: list[dict] = []
        for r in rows:
            rr = dict(r or {})
            rr = self._normalize_positions_row(rr)
            norm_rows.append(rr)

        sql = """
              INSERT INTO positions (exchange_id, account_id, symbol_id, \
                                     strategy_id, strategy_name, \
                                     pos_uid, \
                                     side, qty, \
                                     entry_price, avg_price, exit_price, \
                                     mark_price, unrealized_pnl, \
                                     position_value_usdt, \
                                     scale_in_count, \
                                     realized_pnl, fees, \
                                     last_trade_id, last_ts, \
                                     status, \
                                     opened_at, closed_at, \
                                     updated_at, source)
              VALUES (%(exchange_id)s, %(account_id)s, %(symbol_id)s, \
                      %(strategy_id)s, %(strategy_name)s, \
                      %(pos_uid)s, \
                      %(side)s, %(qty)s, \
                      %(entry_price)s, %(avg_price)s, %(exit_price)s, \
                      %(mark_price)s, %(unrealized_pnl)s, \
                      %(position_value_usdt)s, \
                      %(scale_in_count)s, \
                      %(realized_pnl)s, %(fees)s, \
                      %(last_trade_id)s, %(last_ts)s, \
                      %(status)s, \
                      %(opened_at)s, %(closed_at)s, \
                      %(updated_at)s, %(source)s) ON CONFLICT (exchange_id, account_id, symbol_id)
            DO \
              UPDATE SET
                  -- стратегия: если в positions уже не unknown — не трогаем
                  strategy_id = CASE \
                  WHEN positions.strategy_id = 'unknown' AND EXCLUDED.strategy_id IS NOT NULL AND EXCLUDED.strategy_id <> 'unknown' \
                  THEN EXCLUDED.strategy_id \
                  ELSE positions.strategy_id
              END \
              ,
                strategy_name = COALESCE(positions.strategy_name, EXCLUDED.strategy_name),

                -- lifecycle write-once
                pos_uid = CASE
                    WHEN COALESCE(positions.pos_uid, '') <> '' THEN positions.pos_uid
                    WHEN COALESCE(EXCLUDED.pos_uid, '') <> '' THEN EXCLUDED.pos_uid
                    ELSE positions.pos_uid
              END \
              ,

                opened_at = COALESCE(positions.opened_at, EXCLUDED.opened_at),

                closed_at = CASE
                    WHEN positions.closed_at IS NOT NULL THEN positions.closed_at
                    WHEN EXCLUDED.closed_at IS NOT NULL THEN EXCLUDED.closed_at
                    ELSE positions.closed_at
              END \
              ,

                -- текущее состояние
                side = EXCLUDED.side,
                qty = EXCLUDED.qty,
                status = EXCLUDED.status,

                -- цены/поля
                entry_price = EXCLUDED.entry_price,
                avg_price = EXCLUDED.avg_price,
                exit_price = EXCLUDED.exit_price,
                mark_price = EXCLUDED.mark_price,
                unrealized_pnl = EXCLUDED.unrealized_pnl,
                position_value_usdt = EXCLUDED.position_value_usdt,

                scale_in_count = COALESCE(EXCLUDED.scale_in_count, positions.scale_in_count),

                realized_pnl = EXCLUDED.realized_pnl,
                fees = EXCLUDED.fees,

                last_trade_id = COALESCE(NULLIF(EXCLUDED.last_trade_id, ''), positions.last_trade_id),
                last_ts = COALESCE(EXCLUDED.last_ts, positions.last_ts),

                updated_at = EXCLUDED.updated_at,
                source = EXCLUDED.source \
              """

        return self._exec_many(sql, norm_rows)

    # ======================================================================
    # POSITION SNAPSHOTS (OK + CLEAN SQL)
    # ======================================================================

    def upsert_position_snapshots(self, rows):
        rows = list(rows or [])
        if not rows:
            return 0

        now = _utcnow()
        prepared: list[dict] = []

        for r in rows:
            d = dict(r or {})

            d["exchange_id"] = int(d.get("exchange_id") or 0)
            d["account_id"] = int(d.get("account_id") or 0)
            d["symbol_id"] = int(d.get("symbol_id") or 0)

            qty = abs(self._f(d.get("qty"), 0.0))
            side = self._norm_side(d.get("side"))

            if qty <= 0:
                d["qty"] = 0.0
                d["side"] = "FLAT"
            else:
                d["qty"] = float(qty)
                if side not in ("LONG", "SHORT"):
                    side = "LONG"
                d["side"] = side

            entry = self._f(d.get("entry_price"), 0.0)
            mark = self._f(d.get("mark_price"), entry)

            d["entry_price"] = float(entry)
            d["mark_price"] = float(mark)

            # position_value должен быть числом (часто NOT NULL)
            pv = d.get("position_value", None)
            if pv is None:
                pv = abs(qty) * float(mark)
            d["position_value"] = float(self._f(pv, 0.0))

            d["unrealized_pnl"] = float(self._f(d.get("unrealized_pnl"), 0.0))
            d["realized_pnl"] = float(self._f(d.get("realized_pnl"), 0.0))
            d["fees"] = float(self._f(d.get("fees"), 0.0))

            d.setdefault("last_ts", now)
            d.setdefault("updated_at", now)
            d.setdefault("source", "position_aggregate")

            prepared.append(d)

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
                  side = EXCLUDED.side, \
                  qty = EXCLUDED.qty, \
                  entry_price = EXCLUDED.entry_price, \
                  mark_price = EXCLUDED.mark_price, \
                  position_value = EXCLUDED.position_value, \
                  unrealized_pnl = EXCLUDED.unrealized_pnl, \
                  realized_pnl = EXCLUDED.realized_pnl, \
                  fees = EXCLUDED.fees, \
                  last_ts = EXCLUDED.last_ts, \
                  updated_at = EXCLUDED.updated_at, \
                  source = EXCLUDED.source \
              """
        return self._exec_many(sql, prepared)

    # ======================================================================
    # POSITION LEDGER (FIXED: closed_at write-once + strategy unknown protection)
    # ======================================================================

    def upsert_position_ledger(self, rows):
        rows = list(rows or [])
        if not rows:
            return 0

        now = _utcnow()
        prepared: list[dict] = []

        for r in rows:
            d = dict(r or {})
            d["exchange_id"] = int(d.get("exchange_id") or 0)
            d["account_id"] = int(d.get("account_id") or 0)
            d["symbol_id"] = int(d.get("symbol_id") or 0)

            d["pos_uid"] = str(d.get("pos_uid") or "").strip()
            if not d["pos_uid"]:
                # pos_uid обязателен для PK, делаем детерминированный fallback
                d["pos_uid"] = f"{d['exchange_id']}:{d['account_id']}:{d['symbol_id']}"

            d.setdefault("strategy_id", "unknown")
            d.setdefault("strategy_name", None)

            d.setdefault("updated_at", now)
            d.setdefault("source", "ledger")

            prepared.append(d)

        sql = """
              INSERT INTO position_ledger (exchange_id, account_id, pos_uid, symbol_id, \
                                           strategy_id, strategy_name, \
                                           side, status, \
                                           opened_at, closed_at, \
                                           entry_price, avg_price, exit_price, \
                                           qty_opened, qty_current, qty_closed, \
                                           position_value_usdt, scale_in_count, \
                                           realized_pnl, fees, \
                                           updated_at, source)
              VALUES (%(exchange_id)s, %(account_id)s, %(pos_uid)s, %(symbol_id)s, \
                      %(strategy_id)s, %(strategy_name)s, \
                      %(side)s, %(status)s, \
                      %(opened_at)s, %(closed_at)s, \
                      %(entry_price)s, %(avg_price)s, %(exit_price)s, \
                      %(qty_opened)s, %(qty_current)s, %(qty_closed)s, \
                      %(position_value_usdt)s, %(scale_in_count)s, \
                      %(realized_pnl)s, %(fees)s, \
                      %(updated_at)s, %(source)s) ON CONFLICT (exchange_id, account_id, pos_uid, opened_at)
            DO \
              UPDATE SET
                  closed_at = COALESCE (position_ledger.closed_at, EXCLUDED.closed_at), \

                  side = EXCLUDED.side, \
                  status = EXCLUDED.status, \

                  strategy_id = CASE \
                  WHEN position_ledger.strategy_id = 'unknown' \
                  AND EXCLUDED.strategy_id IS NOT NULL \
                  AND EXCLUDED.strategy_id <> 'unknown' \
                  THEN EXCLUDED.strategy_id \
                  ELSE position_ledger.strategy_id
              END \
              ,
                strategy_name = COALESCE(position_ledger.strategy_name, EXCLUDED.strategy_name),

                entry_price = COALESCE(position_ledger.entry_price, EXCLUDED.entry_price),
                avg_price = COALESCE(EXCLUDED.avg_price, position_ledger.avg_price),
                exit_price = COALESCE(EXCLUDED.exit_price, position_ledger.exit_price),

                qty_opened = COALESCE(position_ledger.qty_opened, EXCLUDED.qty_opened),
                qty_current = EXCLUDED.qty_current,
                qty_closed = GREATEST(COALESCE(position_ledger.qty_closed, 0), COALESCE(EXCLUDED.qty_closed, 0)),

                position_value_usdt = EXCLUDED.position_value_usdt,
                scale_in_count = EXCLUDED.scale_in_count,
                realized_pnl = EXCLUDED.realized_pnl,
                fees = EXCLUDED.fees,

                updated_at = EXCLUDED.updated_at,
                source = EXCLUDED.source \
              """
        return self._exec_many(sql, prepared)

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
                n = cur.rowcount
            conn.commit()
        return int(n or 0)

    def upsert_trades(self, trades: list[dict]) -> None:
        if not trades:
            return

        query = """
                INSERT INTO trades (exchange_id, account_id, trade_id, order_id, symbol_id, \
                                    strategy_id, pos_uid, side, price, qty, fee, fee_asset, \
                                    realized_pnl, ts, source, raw_json)
                VALUES (%(exchange_id)s, %(account_id)s, %(trade_id)s, %(order_id)s, %(symbol_id)s, \
                        %(strategy_id)s, %(pos_uid)s, %(side)s, %(price)s, %(qty)s, %(fee)s, %(fee_asset)s, \
                        %(realized_pnl)s, %(ts)s, %(source)s, %(raw_json)s) ON CONFLICT (exchange_id, account_id, trade_id)
        DO \
                UPDATE SET
                    fee = EXCLUDED.fee, \
                    fee_asset = EXCLUDED.fee_asset, \
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
                "fee_asset": str(t.get("fee_asset") or ""),
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
        if not rows:
            return 0

        now = datetime.now(timezone.utc)
        for r in rows:
            r.setdefault("quote_volume", None)
            r.setdefault("trades", None)
            r.setdefault("taker_buy_base", None)
            r.setdefault("taker_buy_quote", None)
            r.setdefault("taker_sell_base", None)
            r.setdefault("taker_sell_quote", None)
            r.setdefault("delta_quote", None)
            r.setdefault("delta_base", None)
            r.setdefault("cvd_quote", None)
            r.setdefault("source", "unknown")
            r.setdefault("updated_at", now)

        sql = """
              INSERT INTO candles (exchange_id, symbol_id, interval, open_time, \
                                   open, high, low, close, volume, \
                                   quote_volume, trades, \
                                   taker_buy_base, taker_buy_quote, \
                                   taker_sell_base, taker_sell_quote, \
                                   delta_quote, delta_base, \
                                   cvd_quote, \
                                   source, updated_at)
              VALUES (%(exchange_id)s, %(symbol_id)s, %(interval)s, %(open_time)s, \
                      %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, \
                      %(quote_volume)s, %(trades)s, \
                      %(taker_buy_base)s, %(taker_buy_quote)s, \
                      %(taker_sell_base)s, %(taker_sell_quote)s, \
                      %(delta_quote)s, %(delta_base)s, \
                      %(cvd_quote)s, \
                      %(source)s, %(updated_at)s) ON CONFLICT (exchange_id, symbol_id, interval, open_time)
        DO \
              UPDATE SET \
                  open = EXCLUDED.open, \
                  high = EXCLUDED.high, \
                  low = EXCLUDED.low, \
                  close = EXCLUDED.close, \
                  volume = EXCLUDED.volume, \

                  quote_volume = COALESCE (EXCLUDED.quote_volume, candles.quote_volume), \
                  trades = COALESCE (EXCLUDED.trades, candles.trades), \
                  taker_buy_base = COALESCE (EXCLUDED.taker_buy_base, candles.taker_buy_base), \
                  taker_buy_quote = COALESCE (EXCLUDED.taker_buy_quote, candles.taker_buy_quote), \
                  taker_sell_base = COALESCE (EXCLUDED.taker_sell_base, candles.taker_sell_base), \
                  taker_sell_quote = COALESCE (EXCLUDED.taker_sell_quote, candles.taker_sell_quote), \
                  delta_quote = COALESCE (EXCLUDED.delta_quote, candles.delta_quote), \
                  delta_base = COALESCE (EXCLUDED.delta_base, candles.delta_base), \
                  cvd_quote = COALESCE (EXCLUDED.cvd_quote, candles.cvd_quote), \
                  source = EXCLUDED.source, \
                  updated_at = GREATEST(candles.updated_at, EXCLUDED.updated_at) \
              """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()

        return len(rows)

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
                        int(exchange_id),
                        int(account_id),
                        state.get("wallet_balance"),
                        state.get("equity"),
                        state.get("available_balance"),
                        state.get("unrealized_pnl"),
                    ),
                )
            conn.commit()

    def exec_ddl(self, sql: str) -> None:
        """Execute DDL SQL that may contain multiple statements.

        Supports:
          - multiple statements separated by ';'
          - DO $$ ... $$; and DO $tag$ ... $tag$; blocks
          - CREATE FUNCTION/PROCEDURE with dollar-quoting
          - ignores ';' inside quotes and dollar-quoted blocks
          - ignores '-- line comments' and '/* block comments */'
        """
        if not sql:
            return

        def _split_sql_statements(script: str) -> list[str]:
            s = script
            out: list[str] = []
            buf: list[str] = []

            i = 0
            n = len(s)

            in_squote = False  # '...'
            in_dquote = False  # "..."
            in_line_comment = False  # -- ....
            in_block_comment = False  # /* ... */
            dollar_tag: str | None = None  # '$$' or '$tag$'

            def startswith_at(prefix: str) -> bool:
                return s.startswith(prefix, i)

            def flush():
                stmt = "".join(buf).strip()
                if stmt:
                    out.append(stmt)
                buf.clear()

            while i < n:
                ch = s[i]

                # -------- comments handling --------
                if in_line_comment:
                    buf.append(ch)
                    if ch == "\n":
                        in_line_comment = False
                    i += 1
                    continue

                if in_block_comment:
                    buf.append(ch)
                    if startswith_at("*/"):
                        buf.append("*")  # we already added current char; add '*'? (we'll handle properly below)
                    # simpler: detect end and consume both chars
                    if startswith_at("*/"):
                        # we already appended ch ('*' maybe not), so do exact consume:
                        buf.pop()  # remove current ch, we will append full "*/"
                        buf.append("*/")
                        i += 2
                        in_block_comment = False
                        continue
                    i += 1
                    continue

                # start of comments (only if not inside quotes/dollar)
                if not in_squote and not in_dquote and dollar_tag is None:
                    if startswith_at("--"):
                        buf.append("--")
                        i += 2
                        in_line_comment = True
                        continue
                    if startswith_at("/*"):
                        buf.append("/*")
                        i += 2
                        in_block_comment = True
                        continue

                # -------- dollar-quoted blocks --------
                if not in_squote and not in_dquote:
                    if dollar_tag is not None:
                        # check if we are closing the tag
                        if s.startswith(dollar_tag, i):
                            buf.append(dollar_tag)
                            i += len(dollar_tag)
                            dollar_tag = None
                            continue
                        # otherwise just consume
                        buf.append(ch)
                        i += 1
                        continue
                    else:
                        # detect opening dollar tag: $...$
                        if ch == "$":
                            j = i + 1
                            # find next '$' to close tag header
                            while j < n and s[j] != "$":
                                # allowed tag chars: letters/digits/underscore (but we can be permissive)
                                j += 1
                            if j < n and s[j] == "$":
                                tag = s[i: j + 1]  # includes both '$'
                                # start dollar mode
                                dollar_tag = tag
                                buf.append(tag)
                                i = j + 1
                                continue

                # -------- string quotes --------
                if dollar_tag is None:
                    if not in_dquote and ch == "'":
                        buf.append(ch)
                        if in_squote:
                            # handle escaped '' inside single-quote
                            if i + 1 < n and s[i + 1] == "'":
                                # stays inside string, consume both
                                buf.append("'")
                                i += 2
                                continue
                            in_squote = False
                        else:
                            in_squote = True
                        i += 1
                        continue

                    if not in_squote and ch == '"':
                        buf.append(ch)
                        # double quotes escape by "" (rare, but handle)
                        if in_dquote:
                            if i + 1 < n and s[i + 1] == '"':
                                buf.append('"')
                                i += 2
                                continue
                            in_dquote = False
                        else:
                            in_dquote = True
                        i += 1
                        continue

                # -------- statement boundary --------
                if ch == ";" and not in_squote and not in_dquote and dollar_tag is None:
                    flush()
                    i += 1
                    continue

                buf.append(ch)
                i += 1

            flush()
            return out

        statements = _split_sql_statements(sql)

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
                INSERT INTO open_interest (exchange_id, symbol_id, interval, ts, \
                                           open_interest, open_interest_value, source)
                VALUES (%(exchange_id)s, %(symbol_id)s, %(interval)s, %(ts)s, \
                        %(open_interest)s, %(open_interest_value)s, %(source)s) ON CONFLICT (exchange_id, symbol_id, interval, ts)
            DO \
                UPDATE SET
                    open_interest = EXCLUDED.open_interest, \
                    open_interest_value = EXCLUDED.open_interest_value, \
                    source = EXCLUDED.source \
                """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, rows)
            conn.commit()

        # ВАЖНО: rowcount может врать -> возвращаем len(rows)
        return len(rows)

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


    def insert_order_fill(self, fill: Any) -> int:
        return self.insert_order_fills([fill])

    def insert_order_fills(self, fills: Sequence[Any]) -> int:
        """
        INSERT fills into public.order_fills.

        OMS invariants:
          • fill_uid == trade_id (ВСЕГДА)
          • без trade_id → это НЕ fill
          • qty / price обязательны
          • NULL / мусор не пишем
          • idempotent (ON CONFLICT DO NOTHING)
        """
        if not fills:
            return 0

        sql = """
              INSERT INTO order_fills (exchange_id, \
                                       account_id, \
                                       fill_uid, \
                                       symbol_id, \
                                       order_id, \
                                       trade_id, \
                                       client_order_id, \
                                       price, \
                                       qty, \
                                       realized_pnl, \
                                       ts, \
                                       source)
              VALUES (%(exchange_id)s, \
                      %(account_id)s, \
                      %(fill_uid)s, \
                      %(symbol_id)s, \
                      %(order_id)s, \
                      %(trade_id)s, \
                      %(client_order_id)s, \
                      %(price)s, \
                      %(qty)s, \
                      %(realized_pnl)s, \
                      %(ts)s, \
                      %(source)s) ON CONFLICT (exchange_id, account_id, fill_uid)
            DO NOTHING; \
              """

        now = _utcnow()

        def get(obj: Any, key: str, default=None):
            return obj.get(key, default) if isinstance(obj, dict) else getattr(obj, key, default)

        rows: list[dict[str, Any]] = []

        for f in fills:
            # ---------------------------
            # обязательные идентификаторы
            ex_id = get(f, "exchange_id")
            acc_id = get(f, "account_id")
            sym_id = get(f, "symbol_id")

            if ex_id is None or acc_id is None or sym_id is None:
                continue

            trade_id = get(f, "trade_id")
            if trade_id is None:
                # ❌ без trade_id — это не fill
                continue

            # ---------------------------
            # 🔒 ЖЁСТКИЙ OMS-инвариант
            fill_uid = str(trade_id)

            # ---------------------------
            # price / qty обязательны
            price = get(f, "price")
            qty = get(f, "qty")

            if price is None or qty is None:
                continue

            # ---------------------------
            # ts нормализация
            ts = get(f, "ts")
            if isinstance(ts, (int, float)):
                # если прилетело в ms
                ts = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            elif ts is None:
                ts = now

            rows.append({
                "exchange_id": int(ex_id),
                "account_id": int(acc_id),
                "fill_uid": fill_uid,  # ✅ ВСЕГДА = trade_id
                "symbol_id": int(sym_id),

                "order_id": str(get(f, "order_id")) if get(f, "order_id") is not None else None,
                "trade_id": str(trade_id),
                "client_order_id": (
                    str(get(f, "client_order_id"))
                    if get(f, "client_order_id") is not None
                    else None
                ),

                "price": price,
                "qty": qty,
                "realized_pnl": get(f, "realized_pnl") or 0,

                "ts": ts,
                "source": get(f, "source") or "ws_user",
            })

        if not rows:
            return 0

        return self._exec_many(sql, rows)


    def upsert_order_event(self, ev: Any) -> None:
        # ---------------------------
        # side normalization
        side = ev.side
        if hasattr(side, "name"):
            side = side.name
        elif isinstance(side, str):
            side = side.upper()
        else:
            side = None

        # ---------------------------
        # status / type normalization
        status = ev.status.name if hasattr(ev.status, "name") else ev.status
        otype = ev.type.name if hasattr(ev.type, "name") else ev.type

        # ---------------------------
        # raw_json (NOT NULL-ish)
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

        # ---------------------------
        # recv_ts (NOT NULL)
        recv_ts = getattr(ev, "recv_ts", None)
        if isinstance(recv_ts, (int, float)):
            recv_ts = datetime.fromtimestamp(recv_ts, tz=timezone.utc)
        elif recv_ts is None:
            recv_ts = datetime.now(timezone.utc)

        # ---------------------------
        # обязательные поля
        if (
                ev.exchange_id is None
                or ev.account_id is None
                or ev.symbol_id is None
                or ev.order_id is None
                or ev.ts_ms is None
        ):
            return  # не роняем WS

        # стратегия / pos_uid
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
                INSERT INTO order_events (exchange_id, account_id, order_id, symbol_id, \
                                          client_order_id, status, side, type, \
                                          reduce_only, price, qty, filled_qty, \
                                          strategy_id, pos_uid, \
                                          source, ts_ms, recv_ts, raw_json)
                VALUES (%(exchange_id)s, %(account_id)s, %(order_id)s, %(symbol_id)s, \
                        %(client_order_id)s, %(status)s, %(side)s, %(type)s, \
                        %(reduce_only)s, %(price)s, %(qty)s, %(filled_qty)s, \
                        %(strategy_id)s, %(pos_uid)s, \
                        %(source)s, %(ts_ms)s, %(recv_ts)s, %(raw_json)s) ON CONFLICT (exchange_id, account_id, order_id, ts_ms, status, filled_qty)
            DO NOTHING; \
                """
        self._exec_many(query, [row])

    def exists_order_event(self, *, exchange_id, account_id, order_id, status) -> bool:
        sql = """
              SELECT 1
              FROM order_events
              WHERE exchange_id = %s
                AND account_id = %s
                AND order_id = %s
                AND status = %s LIMIT 1; \
              """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (exchange_id, account_id, order_id, status))
                return cur.fetchone() is not None


    def get_order_by_id(
            self,
            *,
            exchange_id: int,
            account_id: int,
            order_id: str,
        ) -> dict | None:
        # SQL запрос для поиска ордера по order_id
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
                # Выполняем запрос с параметрами
                cur.execute(query, (int(exchange_id), int(account_id), str(order_id)))
                row = cur.fetchone()

                # Если ордер найден, возвращаем его как словарь
                if row:
                    cols = [d[0] for d in cur.description]
                    return dict(zip(cols, row))

        # Если ордер не найден
        return None

    def insert_liquidation_events(self, rows: list[dict]) -> int:
        if not rows:
            return 0

        from psycopg.types.json import Jsonb  # локально, чтобы точно работало

        sql = """
              INSERT INTO liquidation_events (exchange_id, symbol_id, ts, event_ms, \
                                              side, price, qty, filled_qty, avg_price, \
                                              status, order_type, time_in_force, \
                                              notional, is_long_liq, raw_json)
              VALUES (%(exchange_id)s, %(symbol_id)s, %(ts)s, %(event_ms)s, \
                      %(side)s, %(price)s, %(qty)s, %(filled_qty)s, %(avg_price)s, \
                      %(status)s, %(order_type)s, %(time_in_force)s, \
                      %(notional)s, %(is_long_liq)s, %(raw_json)s) ON CONFLICT DO NOTHING \
              """

        fixed = []
        for r in rows:
            rr = dict(r)
            # raw_json может быть dict/list -> оборачиваем в Jsonb
            rr["raw_json"] = Jsonb(rr.get("raw_json"))
            fixed.append(rr)

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, fixed)
            conn.commit()
        return len(fixed)


    def upsert_liquidation_1m(self, rows: list[dict]) -> int:
        if not rows:
            return 0

        sql = """
              INSERT INTO liquidation_1m (exchange_id, symbol_id, bucket_ts, \
                                          long_notional, short_notional, long_qty, short_qty, events, updated_at)
              VALUES (%(exchange_id)s, %(symbol_id)s, %(bucket_ts)s, \
                      %(long_notional)s, %(short_notional)s, %(long_qty)s, %(short_qty)s, %(events)s, %(updated_at)s) ON CONFLICT (exchange_id, symbol_id, bucket_ts)
        DO \
              UPDATE SET
                  long_notional = liquidation_1m.long_notional + EXCLUDED.long_notional, \
                  short_notional = liquidation_1m.short_notional + EXCLUDED.short_notional, \
                  long_qty = liquidation_1m.long_qty + EXCLUDED.long_qty, \
                  short_qty = liquidation_1m.short_qty + EXCLUDED.short_qty, \
                  events = liquidation_1m.events + EXCLUDED.events, \
                  updated_at = EXCLUDED.updated_at \
              """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()
        return len(rows)


    def cleanup_market_state_5m(self, *, exchange_id: int, keep_days: int) -> int:
        """
        Удаляет старые записи из market_state_5m.
        keep_days: сколько дней хранить.
        """
        keep_days = int(keep_days)
        if keep_days <= 0:
            return 0

        sql = """
              DELETE \
              FROM market_state_5m
              WHERE exchange_id = %(exchange_id)s
                AND open_time < now() - (%(keep_days)s || ' days')::interval \
              """

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, {"exchange_id": int(exchange_id), "keep_days": keep_days})
                deleted = cur.rowcount or 0
            conn.commit()

        return int(deleted)

    def get_open_interest_watermarks_bulk(self, exchange_id: int, intervals: list[str]) -> dict[
        str, dict[int, datetime]]:
        """
        Возвращает watermarks (MAX ts) по каждому symbol_id и interval одним SQL запросом.
        out: { "5m": {symbol_id: ts, ...}, "15m": {...}, ... }
        """
        exchange_id = int(exchange_id)
        intervals = list(intervals or [])
        if not intervals:
            return {}

        sql = """
              SELECT interval, symbol_id, MAX (ts) AS last_ts
              FROM open_interest
              WHERE exchange_id = %s
                AND interval = ANY (%s)
              GROUP BY interval, symbol_id \
              """

        out: dict[str, dict[int, datetime]] = {iv: {} for iv in intervals}

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (exchange_id, intervals))
                for interval, symbol_id, last_ts in cur.fetchall():
                    if last_ts is None:
                        continue
                    out[str(interval)][int(symbol_id)] = last_ts

        return out

    # ============================================================
    # SYMBOL HELPERS (for collectors)
    # ============================================================

    def list_active_symbol_ids(self, exchange_id: int) -> list[int]:
        """
        Быстрый список активных symbol_id для exchange.
        Используется collectors (OI/MarketState/etc).
        """
        sql = """
        SELECT symbol_id
        FROM symbols
        WHERE exchange_id = %s AND is_active = true
        ORDER BY symbol_id
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(exchange_id),))
                rows = cur.fetchall() or []
        return [int(r[0]) for r in rows]

    def list_active_symbols_map(self, exchange_id: int) -> dict[str, int]:
        """
        {SYMBOL: symbol_id} только активные.
        """
        sql = """
        SELECT symbol, symbol_id
        FROM symbols
        WHERE exchange_id = %s AND is_active = true
        ORDER BY symbol
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(exchange_id),))
                rows = cur.fetchall() or []
        return {str(sym).upper(): int(sid) for sym, sid in rows}

    def get_symbol_name(self, symbol_id: int) -> str | None:
        """
        Возвращает "BTCUSDT" по symbol_id.
        Нужно для OpenInterest collector и любых REST коллекоров.
        """
        sql = "SELECT symbol FROM symbols WHERE symbol_id = %s"
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(symbol_id),))
                row = cur.fetchone()
        if not row:
            return None
        return str(row[0]).upper()


    def cleanup_market_trades(
            self,
            *,
            exchange_id: int,
            keep_days: int,
            batch_size: int = 50_000,
            sleep_sec: float = 0.05,
            max_batches: int = 0,
    ) -> int:
        """
        ✅ Batched cleanup для market_trades по ts.

        - keep_days <= 0 => skip
        - batch_size: сколько строк удаляем за раз
        - sleep_sec: пауза между батчами
        - max_batches: 0 = без лимита, иначе ограничим число батчей за 1 запуск (полезно для safety)
        """
        keep_days = int(keep_days)
        if keep_days <= 0:
            return 0

        batch_size = max(1000, int(batch_size))
        sleep_sec = max(0.0, float(sleep_sec))
        max_batches = int(max_batches or 0)

        total_deleted = 0
        batches = 0

        sql = """
              WITH del AS (SELECT ctid \
                           FROM market_trades \
                           WHERE exchange_id = %(exchange_id)s \
                             AND ts < now() - (%(keep_days)s || ' days'):: interval
                  LIMIT %(batch_size)s
                  )
              DELETE \
              FROM market_trades
              WHERE ctid IN (SELECT ctid FROM del); \
              """

        while True:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql,
                        {
                            "exchange_id": int(exchange_id),
                            "keep_days": keep_days,
                            "batch_size": batch_size,
                        },
                    )
                    deleted = int(cur.rowcount or 0)
                conn.commit()

            if deleted <= 0:
                break

            total_deleted += deleted
            batches += 1

            if max_batches > 0 and batches >= max_batches:
                break

            if sleep_sec > 0:
                time.sleep(sleep_sec)

        return total_deleted

    def cleanup_candles_trades_agg(
            self,
            *,
            exchange_id: int,
            keep_days: int,
            batch_size: int = 50_000,
            sleep_sec: float = 0.05,
            max_batches: int = 0,
    ) -> int:
        """
        ✅ Batched cleanup для candles_trades_agg по open_time.
        """
        keep_days = int(keep_days)
        if keep_days <= 0:
            return 0

        batch_size = max(1000, int(batch_size))
        sleep_sec = max(0.0, float(sleep_sec))
        max_batches = int(max_batches or 0)

        total_deleted = 0
        batches = 0

        sql = """
              WITH del AS (SELECT ctid \
                           FROM candles_trades_agg \
                           WHERE exchange_id = %(exchange_id)s \
                             AND open_time < now() - (%(keep_days)s || ' days'):: interval
                  LIMIT %(batch_size)s
                  )
              DELETE \
              FROM candles_trades_agg
              WHERE ctid IN (SELECT ctid FROM del); \
              """

        while True:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql,
                        {
                            "exchange_id": int(exchange_id),
                            "keep_days": keep_days,
                            "batch_size": batch_size,
                        },
                    )
                    deleted = int(cur.rowcount or 0)
                conn.commit()

            if deleted <= 0:
                break

            total_deleted += deleted
            batches += 1

            if max_batches > 0 and batches >= max_batches:
                break

            if sleep_sec > 0:
                time.sleep(sleep_sec)

        return total_deleted
