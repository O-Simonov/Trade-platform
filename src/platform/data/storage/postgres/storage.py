from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Tuple

logger = logging.getLogger(__name__)


class PostgreSQLStorage:
    """
    Base PostgreSQL storage layer.
    All DB access for the platform goes through this class.
    """

    def __init__(self, pool):
        self.pool = pool


    # =========================================================================
    # POSITIONS UID (BOOTSTRAP STUB)
    # =========================================================================

    def get_last_pos_uid(
        self,
        exchange_id: int,
        account_id: int,
        symbol_id: int,
        strategy_id: int,
    ) -> int:
        """
        Return last known position UID for given scope.

        Stub implementation:
        - returns 0 if no positions yet
        - TradingInstance will start from UID=1
        """
        return 0

    # =========================================================================
    # DDL
    # =========================================================================

    def exec_ddl(self, ddl_sql: str) -> None:
        """
        Execute DDL script containing multiple SQL statements.
        Compatible with psycopg3.
        """
        statements = [s.strip() for s in ddl_sql.split(";") if s.strip()]

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                for stmt in statements:
                    cur.execute(stmt)
            conn.commit()

        logger.info("[DB] DDL applied (%d statements)", len(statements))

    # =========================================================================
    # REGISTRY (EXCHANGE / ACCOUNT / SYMBOL)
    # =========================================================================

    def ensure_exchange_account_symbol(
        self,
        exchange: str,
        account: str,
        symbols: list[str],
    ) -> Dict[str, int]:
        """
        Ensure exchange, account and symbols exist.

        Returns:
          {
            "_exchange_id": int,
            "_account_id": int,
            "BTCUSDT": int,
            ...
          }
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
                    INSERT INTO exchange_accounts (exchange_id, name)
                    VALUES (%s, %s)
                    ON CONFLICT (exchange_id, name)
                    DO UPDATE SET name = EXCLUDED.name
                    RETURNING account_id
                    """,
                    (exchange_id, account),
                )
                account_id = cur.fetchone()[0]

                ids: Dict[str, int] = {
                    "_exchange_id": exchange_id,
                    "_account_id": account_id,
                }

                # --- symbols ---
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

    # =========================================================================
    # RETENTION / CLEANUP
    # =========================================================================

    def retention_cleanup(
        self,
        *,
        candles_days: int = 180,
        snapshots_days: int = 30,
        funding_days: int = 90,
        orders_days: int = 90,
        trades_days: int = 90,
        fills_days: int = 90,
        balance_days: int = 30,
        balance_5m_days: int = 180,
        balance_1h_days: int = 1825,
        oi_5m_days: int = 90,
        oi_15m_days: int = 180,
        oi_1h_days: int = 365,
        dry_run: bool = False,
    ) -> Dict[str, int]:
        """Delete old rows to keep DB bounded."""

        stmts: List[Tuple[str, str, Tuple[int]]] = [
            ("candles", "DELETE FROM candles WHERE open_time < now() - (%s || ' days')::interval", (candles_days,)),
            ("account_balance_snapshots", "DELETE FROM account_balance_snapshots WHERE ts < now() - (%s || ' days')::interval", (balance_days,)),
            ("account_balance_5m", "DELETE FROM account_balance_5m WHERE ts < now() - (%s || ' days')::interval", (balance_5m_days,)),
            ("account_balance_1h", "DELETE FROM account_balance_1h WHERE ts < now() - (%s || ' days')::interval", (balance_1h_days,)),
            ("open_interest_5m", "DELETE FROM open_interest WHERE interval='5m' AND ts < now() - (%s || ' days')::interval", (oi_5m_days,)),
            ("open_interest_15m", "DELETE FROM open_interest WHERE interval='15m' AND ts < now() - (%s || ' days')::interval", (oi_15m_days,)),
            ("open_interest_1h", "DELETE FROM open_interest WHERE interval='1h' AND ts < now() - (%s || ' days')::interval", (oi_1h_days,)),
            ("orders", "DELETE FROM orders WHERE updated_at IS NOT NULL AND updated_at < now() - (%s || ' days')::interval", (orders_days,)),
            ("trades", "DELETE FROM trades WHERE ts < now() - (%s || ' days')::interval", (trades_days,)),
            ("fills", "DELETE FROM order_fills WHERE ts < now() - (%s || ' days')::interval", (fills_days,)),
            ("funding", "DELETE FROM funding WHERE funding_time < now() - (%s || ' days')::interval", (funding_days,)),
        ]

        result: Dict[str, int] = {}
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                for name, sql, params in stmts:
                    cur.execute(sql, params)
                    result[name] = int(cur.rowcount or 0)
            conn.commit()

        return result

    # =========================================================================
    # STUBS (NEXT STAGES)
    # =========================================================================

    def downsample_balance(self) -> Dict[str, int]:
        """Build 5m / 1h balance snapshots from raw snapshots."""
        return {"5m": 0, "1h": 0}

    def upsert_positions(self, rows: Iterable[Dict[str, Any]]) -> int:
        """Upsert positions (baseline)."""
        return len(list(rows))

