from __future__ import annotations

import logging

from typing import Any, Iterable

logger = logging.getLogger(__name__)
log = logging.getLogger("storage.sql")

class AccountRepositoryMixin:
    def insert_account_balance_snapshots(self, rows: Iterable[dict]) -> int:
        rows = list(rows or [])
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

    @staticmethod
    def _resolve_ids(exchange, account, exchange_id, account_id):
        return exchange_id or exchange, account_id or account

    def get_latest_balances(self, *, exchange=None, account=None, exchange_id=None, account_id=None) -> dict:
        ex_id = self._resolve_exchange_id(exchange, exchange_id)
        acc_id = self._resolve_account_id(ex_id, account, account_id)
        if ex_id is None or acc_id is None:
            return {}

        query = """
            SELECT wallet_balance, available_balance, margin_used, equity, unrealized_pnl
            FROM account_balance_snapshots
            WHERE exchange_id = %s
              AND account_id = %s
            ORDER BY ts DESC
            LIMIT 1
        """
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (ex_id, acc_id))
                row = cur.fetchone()
                if not row:
                    return {}
                cols = [d[0] for d in cur.description]
                return dict(zip(cols, row))

    def _resolve_exchange_id(self, exchange: Any, exchange_id: Any):
        if exchange_id is not None:
            return int(exchange_id)
        if isinstance(exchange, int):
            return int(exchange)
        if isinstance(exchange, str):
            row = self._exec_one("SELECT exchange_id FROM exchanges WHERE name = %s", (exchange,))
            return int(row[0]) if row else None
        return None

    def _resolve_account_id(self, exchange_id: int, account: Any, account_id: Any):
        if account_id is not None:
            return int(account_id)
        if isinstance(account, int):
            return int(account)
        if isinstance(account, str) and exchange_id is not None:
            row = self._exec_one(
                "SELECT account_id FROM accounts WHERE exchange_id=%s AND account_name=%s",
                (int(exchange_id), account),
            )
            return int(row[0]) if row else None
        return None

    def upsert_account_state(self, *, exchange_id: int, account_id: int, state: dict) -> None:
        sql = """
            INSERT INTO account_state (
                exchange_id,
                account_id,
                ts,
                wallet_balance,
                equity,
                available_balance,
                unrealized_pnl
            )
            VALUES (%s, %s, NOW(), %s, %s, %s, %s)
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

