# src/platform/core/oms/metrics_reporter.py
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


FINAL_STATUSES = {"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED"}


@dataclass
class OrderRow:
    client_order_id: str
    order_id: str
    status: str
    source: str
    ts_ms: int


def _safe_int(x, default=0) -> int:
    try:
        return int(x)
    except Exception:
        return int(default)


def _avg(values: List[int]) -> float:
    if not values:
        return 0.0
    return float(sum(values) / len(values))


class OmsMetricsReporter:
    """
    STEP 7:
      Computes lifecycle metrics from `orders` table:
        - PH created (order_id LIKE 'PH::%')
        - PH resolved: PH has matching real order row by same client_order_id
        - FILLED latency: filled_ts - ph_ts
        - PH->REAL latency: first_real_ts - ph_ts
    """

    def __init__(self, *, storage, exchange_id: int, account_id: int, logger, window_sec: int = 900):
        self.storage = storage
        self.exchange_id = int(exchange_id)
        self.account_id = int(account_id)
        self.logger = logger
        self.window_sec = int(window_sec)
        self._last_log_ts = 0.0

    def tick(self) -> None:
        since_ms = int((time.time() - self.window_sec) * 1000)

        rows_raw = self.storage.fetch_orders_metrics_window(
            exchange_id=self.exchange_id,
            account_id=self.account_id,
            since_ts_ms=since_ms,
        )

        rows: List[OrderRow] = [
            OrderRow(
                client_order_id=str(r["client_order_id"] or ""),
                order_id=str(r["order_id"] or ""),
                status=str(r["status"] or ""),
                source=str(r["source"] or ""),
                ts_ms=_safe_int(r["ts_ms"], 0),
            )
            for r in rows_raw
            if r.get("client_order_id")
        ]

        if not rows:
            # не шумим если пусто
            return

        # group by client_order_id
        by_cid: Dict[str, List[OrderRow]] = {}
        for rr in rows:
            by_cid.setdefault(rr.client_order_id, []).append(rr)

        ph_created = 0
        ph_resolved = 0

        final_counts: Dict[str, int] = {}
        ph_to_real_lat: List[int] = []
        ph_to_filled_lat: List[int] = []

        for cid, items in by_cid.items():
            ph = None
            real_items: List[OrderRow] = []

            for it in items:
                if it.order_id.startswith("PH::"):
                    # берем самый ранний PH
                    if ph is None or it.ts_ms < ph.ts_ms:
                        ph = it
                else:
                    real_items.append(it)

            if ph is not None:
                ph_created += 1

            if ph is not None and real_items:
                # earliest real
                first_real = min(real_items, key=lambda x: x.ts_ms or 10**18)
                if first_real.ts_ms and ph.ts_ms:
                    ph_resolved += 1
                    ph_to_real_lat.append(int(first_real.ts_ms - ph.ts_ms))

            # финальный статус берем по max(ts_ms) среди REAL (если есть), иначе по PH
            chosen = None
            if real_items:
                chosen = max(real_items, key=lambda x: x.ts_ms or 0)
            elif ph is not None:
                chosen = ph

            if chosen:
                st = (chosen.status or "").upper()
                if st in FINAL_STATUSES:
                    final_counts[st] = final_counts.get(st, 0) + 1

            # latency до FILLED: нужен FILLED реальный event
            if ph is not None and real_items:
                filled_items = [x for x in real_items if (x.status or "").upper() == "FILLED"]
                if filled_items:
                    filled = max(filled_items, key=lambda x: x.ts_ms or 0)
                    if filled.ts_ms and ph.ts_ms:
                        ph_to_filled_lat.append(int(filled.ts_ms - ph.ts_ms))

        self.logger.info(
            "[OMS][METRICS] window=%ss ph_created=%d ph_resolved=%d final=%s "
            "avg_ms_ph_to_real=%.1f avg_ms_ph_to_filled=%.1f",
            self.window_sec,
            ph_created,
            ph_resolved,
            final_counts,
            _avg(ph_to_real_lat),
            _avg(ph_to_filled_lat),
        )
