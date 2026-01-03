# src/platform/core/position/position_flusher.py
from __future__ import annotations

import logging
from typing import Any


class PositionFlusher:
    """
    Flush dirty PositionAggregate objects to DB.

    K6.4 / K6.5:
      ✔ flushes BOTH open and closed (FLAT) dirty aggregates
      ✔ persists CLOSE lifecycle transitions
      ✔ ignores invalid / phantom aggregates
      ✔ clears dirty ONLY after successful DB write
    """

    def __init__(
        self,
        *,
        position_manager: Any,
        storage: Any,
        logger: logging.Logger | None = None,
    ) -> None:
        self.pm = position_manager
        self.storage = storage
        self.logger = logger or logging.getLogger("positions.flusher")

    def flush(self) -> int:
        rows: list[dict] = []
        dirty_aggs = []

        for agg in self.pm.iter_dirty_positions():
            if int(getattr(agg, "symbol_id", 0) or 0) <= 0:
                self.logger.warning("[POSITIONS][FLUSH] skip invalid symbol_id=%s", getattr(agg, "symbol_id", 0))
                agg.clear_dirty()
                continue

            rows.append(agg.to_row())
            dirty_aggs.append(agg)

        if not rows:
            return 0

        try:
            self.storage.upsert_positions(rows)

            for agg in dirty_aggs:
                agg.clear_dirty()

            self.logger.debug(
                "[POSITIONS][FLUSH] upserted=%d (open=%d closed=%d)",
                len(rows),
                sum(1 for r in rows if r.get("status") == "OPEN"),
                sum(1 for r in rows if r.get("status") == "CLOSED"),
            )
            return len(rows)

        except Exception:
            self.logger.exception("[POSITIONS][FLUSH] DB upsert failed")
            return 0
