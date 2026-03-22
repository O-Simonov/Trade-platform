from __future__ import annotations

import logging

from typing import Any

log = logging.getLogger("traders.trade_liquidation")

class TradeLiquidationReportingMixin:
    def _dlog(self, msg: str, *args: Any) -> None:
        if getattr(self.p, "debug", False):
            if args:
                log.info("[TL][debug] " + msg, *args)
            else:
                log.info("[TL][debug] " + msg)

    def _ilog(self, msg: str, *args: Any) -> None:
        """Info-log helper used by some reconciliation/backfill paths.

        Some builds historically referenced self._ilog; to avoid AttributeError we
        provide a thin wrapper around the module log.
        """
        try:
            if args:
                log.info(msg, *args)
            else:
                log.info(msg)
        except Exception:
            # Never let logging break trading loop
            pass

