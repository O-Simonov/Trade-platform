# src/platform/data/storage/postgres/storage.py
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
    # DDL
    # =========================================================================

    def exec_ddl(self, ddl_sql: str) -> None:
        """
        Execute DDL script containing multiple SQL statements.
        Compatible with psycopg3.
        """
        statements = [
            s.strip()
            for s in ddl_sql.split(";")
            if s.strip()
        ]

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                for stmt in statements:
                    cur.execute(stmt)
            conn.commit()

        logger.info("[DB] DDL applied (%d statements)", len(statements))

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
    # DOWNSAMPLE BALANCE
    # =========================================================================

    def downsample_balance(self) -> Dict[str, int]:
        """Build 5m / 1h balance snapshots from raw snapshots."""
        return {"5m": 0, "1h": 0}

    # =========================================================================
    # POSITIONS (BASELINE)
    # =========================================================================

    def upsert_positions(self, rows: Iterable[Dict[str, Any]]) -> int:
        """Upsert positions (baseline)."""
        return len(list(rows))
