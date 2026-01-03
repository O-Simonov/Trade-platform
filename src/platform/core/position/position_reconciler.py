# src/platform/core/position/position_reconciler.py
from __future__ import annotations

import time
import logging
from typing import Any, List, Dict, Tuple

from src.platform.core.position.aggregate import PositionAggregate


class PositionReconciler:
    """
    K6.4/K6.5

    REST reconcile positions -> update in-memory aggregates ONLY.

    - REST secondary
    - NO direct DB writes here
    - Conservative heal after N mismatches
    """

    CLEANUP_INTERVAL_SEC = 60.0
    DESYNC_THRESHOLD = 3

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
        self.storage = storage

        self.exchange_id = int(exchange_id)
        self.account_id = int(account_id)
        self.account = str(account)

        self.symbol_ids: dict[str, int] = symbol_ids or {}

        self.pm = position_manager
        self.logger = logger or logging.getLogger("positions.reconciler")
        self.logger.info("[POSITIONS][RECONCILE] position_manager_id=%s", id(self.pm))


        self._last_cleanup_ts: float = 0.0
        self._desync: Dict[Tuple[int, int, int], int] = {}  # key -> count

    # ------------------------------------------------------------
    @staticmethod
    def _get(p: Any, key: str, default: Any = None) -> Any:
        if isinstance(p, dict):
            return p.get(key, default)
        return getattr(p, key, default)

    def _resolve_symbol_id(self, p: Any) -> int:
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
            sid = self.symbol_ids.get(str(sym).upper())
            if sid:
                return int(sid)

        return 0

    def _cleanup_phantoms(self) -> None:
        now = time.time()
        if now - self._last_cleanup_ts < self.CLEANUP_INTERVAL_SEC:
            return

        try:
            self.storage.execute_raw("DELETE FROM positions WHERE symbol_id = 0", log=True)
            self._last_cleanup_ts = now
        except Exception:
            self.logger.exception("[POSITIONS][RECONCILE] DB cleanup failed")

    # ------------------------------------------------------------
    def run_once(self) -> List[PositionAggregate]:
        try:
            rest_positions = self.exchange.fetch_positions(account=self.account)
        except Exception:
            self.logger.exception("[POSITIONS][RECONCILE] REST fetch failed")
            return []

        updated: List[PositionAggregate] = []

        for p in rest_positions or []:
            symbol = self._get(p, "symbol", None)
            symbol_id = self._resolve_symbol_id(p)
            if symbol_id <= 0:
                self.logger.debug("[K6.4][SKIP] unresolved symbol=%s", symbol)
                continue

            qty = float(self._get(p, "qty", 0.0) or 0.0)
            entry = float(self._get(p, "entry_price", 0.0) or 0.0)
            upnl = float(self._get(p, "unrealized_pnl", 0.0) or 0.0)

            rest_side = self._get(p, "side", None)  # "LONG"/"SHORT"/"FLAT" (если norm_position так делает)
            mp = self._get(p, "mark_price", None)
            if mp is None:
                mp = self._get(p, "last_price", None)

            agg = self.pm.get_or_create(self.exchange_id, self.account_id, symbol_id)
            key = (self.exchange_id, self.account_id, symbol_id)

            # compare (side+qty)
            agg_side = agg.side_str()
            rest_side_txt = str(rest_side or "FLAT").upper()
            if qty <= 0:
                rest_side_txt = "FLAT"

            mismatch = (agg_side != rest_side_txt) or (abs(float(agg.qty or 0.0) - qty) > 1e-12)

            if mismatch:
                cnt = self._desync.get(key, 0) + 1
                self._desync[key] = cnt

                self.logger.warning(
                    "[K6.4][DESYNC] symbol=%s rest=(%s,%.8f) agg=(%s,%.8f) cnt=%d",
                    symbol, rest_side_txt, qty, agg_side, float(agg.qty or 0.0), cnt
                )

                if cnt >= self.DESYNC_THRESHOLD:
                    # heal conservatively
                    if agg.is_open() and qty > 0 and rest_side_txt in ("LONG", "SHORT") and agg_side in ("LONG", "SHORT"):
                        if agg_side != rest_side_txt:
                            self.logger.error(
                                "[K6.4][BLOCK] side flip denied symbol=%s rest=%s agg=%s",
                                symbol, rest_side_txt, agg_side
                            )
                            self._desync[key] = 0
                            continue

                    self.logger.warning("[K6.4][HEAL] applying REST snapshot symbol=%s", symbol)

                    if agg.apply_rest_snapshot(
                        side=rest_side_txt,
                        qty=qty,
                        entry_price=entry,
                        unrealized_pnl=upnl,
                        mark_price=float(mp) if mp is not None else None,
                    ):
                        updated.append(agg)

                    self._desync[key] = 0

                continue

            # consistent -> reset counter
            if self._desync.get(key, 0) > 0:
                self._desync[key] = 0

            # also allow minor conservative updates (entry/uPnL/mark) if needed
            if agg.apply_rest_snapshot(
                side=rest_side_txt,
                qty=qty,
                entry_price=entry,
                unrealized_pnl=upnl,
                mark_price=float(mp) if mp is not None else None,
            ):
                updated.append(agg)

        self._cleanup_phantoms()

        if updated:
            self.logger.debug("[POSITIONS][RECONCILE] updated=%d", len(updated))

        return updated
