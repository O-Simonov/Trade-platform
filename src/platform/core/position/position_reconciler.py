# src/platform/core/position/position_reconciler.py
from __future__ import annotations

import logging
from typing import Iterable

from src.platform.core.position.aggregate import PositionAggregate


class PositionReconciler:
    """
    Reconcile in-memory positions with REST positions from exchange.

    REST positions may be:
      - dict-like
      - object-like (preferred)
    """

    def __init__(
        self,
        *,
        exchange,
        position_manager,
        storage,
        exchange_id: int,
        account_id: int,
        account: str,
        logger: logging.Logger | None = None,
    ):
        self.exchange = exchange
        self.pm = position_manager
        self.storage = storage

        self.exchange_id = int(exchange_id)
        self.account_id = int(account_id)
        self.account = account

        self.logger = logger or logging.getLogger(__name__)

    # ------------------------------------------------------------
    def _get(self, p, key: str, default=None):
        """
        Safe getter for both object and dict REST positions.
        """
        if hasattr(p, key):
            return getattr(p, key)
        if isinstance(p, dict):
            return p.get(key, default)
        return default

    # ------------------------------------------------------------
    def run_once(self) -> int:
        """
        Perform one reconcile cycle.
        Returns number of reconciled positions.
        """
        try:
            rest_positions = self.exchange.fetch_positions(account=self.account)
        except Exception:
            self.logger.exception("[POSITIONS][RECONCILE] REST fetch failed")
            return 0

        if not rest_positions:
            self.logger.debug("[POSITIONS][RECONCILE] no open positions from exchange")
            return 0

        updated: list[PositionAggregate] = []

        for p in rest_positions:
            try:
                symbol_id = int(self._get(p, "symbol_id", 0))
                if not symbol_id:
                    continue

                qty = float(self._get(p, "qty", 0.0))
                entry_price = float(self._get(p, "entry_price", 0.0))
                unrealized_pnl = float(self._get(p, "unrealized_pnl", 0.0))

                agg = self.pm.get_or_create(
                    self.exchange_id,
                    self.account_id,
                    symbol_id,
                )

                changed = agg.apply_rest_snapshot(
                    qty=qty,
                    entry_price=entry_price,
                    unrealized_pnl=unrealized_pnl,
                )

                if changed:
                    updated.append(agg)

            except Exception:
                self.logger.exception(
                    "[POSITIONS][RECONCILE] failed symbol_id=%s",
                    self._get(p, "symbol"),
                )

        if updated and hasattr(self.storage, "upsert_positions"):
            try:
                payload = [a.to_row() for a in updated]
                self.storage.upsert_positions(payload)
            except Exception:
                self.logger.exception("[POSITIONS][RECONCILE] DB upsert failed")

        if updated:
            self.logger.info(
                "[POSITIONS][RECONCILE] reconciled %d positions",
                len(updated),
            )

        return len(updated)
