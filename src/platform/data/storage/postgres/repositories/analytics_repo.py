from __future__ import annotations

import logging

logger = logging.getLogger(__name__)
log = logging.getLogger("storage.sql")

class AnalyticsRepositoryMixin:
    def get_today_realized_pnl(self, exchange_id: int, account_id: int) -> float:
        query = """
            SELECT COALESCE(SUM(realized_pnl), 0)
            FROM trades
            WHERE exchange_id = %s
              AND account_id = %s
              AND ts >= date_trunc('day', now())
        """
        row = self._exec_one(query, (int(exchange_id), int(account_id)))
        return float(row[0] or 0.0)

