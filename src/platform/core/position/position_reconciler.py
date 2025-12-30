# src/platform/core/position/position_reconciler.py
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from src.platform.core.position.aggregate import PositionAggregate

class PositionReconciler:
    """
    STEP J: REST reconcile positions → update in-memory aggregates → persist to DB.

    Safety guarantees:
      • symbol_id is ALWAYS > 0
      • unknown symbols are skipped
      • no phantom positions in DB

    Supported REST position formats:
      - dict-like: {symbol_id?, symbol?, qty, entry_price, unrealized_pnl}
      - object-like with attributes
    """

    def __init__(
        self,
        *,
        exchange: Any,
        position_manager: Any,
        storage: Any,
        exchange_id: int,
        account_id: int,
        account: str,
        symbol_ids: dict[str, int] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.exchange = exchange
        self.pm = position_manager
        self.storage = storage

        self.exchange_id = int(exchange_id)
        self.account_id = int(account_id)
        self.account = str(account)

        # registry: "LTCUSDT" → symbol_id
        self.symbol_ids: dict[str, int] = symbol_ids or {}

        self.logger = logger or logging.getLogger("positions.reconciler")

    # ------------------------------------------------------------
    @staticmethod
    def _get(p: Any, key: str, default: Any = None) -> Any:
        """Safe getter for both dict-like and object-like REST positions."""
        if isinstance(p, dict):
            return p.get(key, default)
        return getattr(p, key, default)

    # ------------------------------------------------------------
    def _resolve_symbol_id(self, p: Any) -> int:
        """
        Resolve symbol_id strictly:
          1) p.symbol_id / p["symbol_id"]
          2) p.symbol → symbol_ids registry

        Returns:
          symbol_id (>0) or 0 if unresolved
        """
        raw_sid = self._get(p, "symbol_id", None)
        if raw_sid:
            try:
                sid = int(raw_sid)
                if sid > 0:
                    return sid
            except Exception:
                pass

        sym = self._get(p, "symbol", None)
        if sym:
            sym = str(sym).upper()
            sid = self.symbol_ids.get(sym)
            if sid:
                return int(sid)

        return 0

    # ------------------------------------------------------------
    def run_once(self) -> list[PositionAggregate]:
        """
        Perform one reconcile cycle.

        Returns:
          list of PositionAggregate that changed
        """
        try:
            rest_positions = self.exchange.fetch_positions(account=self.account)
        except Exception:
            self.logger.exception("[POSITIONS][RECONCILE] REST fetch failed")
            return []

        if not rest_positions:
            self.logger.debug("[POSITIONS][RECONCILE] no positions from exchange")
            return []

        updated: list[PositionAggregate] = []

        for p in rest_positions:
            try:
                symbol_id = self._resolve_symbol_id(p)
                symbol = self._get(p, "symbol", None)

                if symbol_id <= 0:
                    self.logger.warning(
                        "[POSITIONS][RECONCILE] skip unresolved symbol (symbol=%s)",
                        symbol,
                    )
                    continue

                qty = float(self._get(p, "qty", 0.0) or 0.0)
                entry = float(self._get(p, "entry_price", 0.0) or 0.0)
                upnl = float(self._get(p, "unrealized_pnl", 0.0) or 0.0)

                agg = self.pm.get_or_create(
                    self.exchange_id,
                    self.account_id,
                    symbol_id,
                )

                if agg.apply_rest_snapshot(
                    qty=qty,
                    entry_price=entry,
                    unrealized_pnl=upnl,
                ):
                    updated.append(agg)

            except Exception:
                self.logger.exception(
                    "[POSITIONS][RECONCILE] failed symbol=%s",
                    self._get(p, "symbol", None),
                )

        if updated and hasattr(self.storage, "upsert_positions"):
            try:
                self.storage.upsert_positions([a.to_row() for a in updated])
            except Exception:
                self.logger.exception("[POSITIONS][RECONCILE] DB upsert failed")

        # Deleting phantom positions (symbol_id = 0)
        try:
            self.storage.execute_raw(
                "DELETE FROM positions WHERE symbol_id = 0",
                log=True,
            )

        except Exception:
            self.logger.exception("[POSITIONS][RECONCILE] DB cleanup failed")

        if updated:
            self.logger.info(
                "[POSITIONS][RECONCILE] reconciled %d positions",
                len(updated),
            )

        return updated
