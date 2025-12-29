# src/platform/core/oms/writer.py
from __future__ import annotations

import logging
import json
from datetime import datetime, timezone

from .events import OrderEvent, TradeEvent


class OmsWriter:
    """
    OMS Writer (STEP G.2)

    ✔ append immutable order_events
    ✔ upsert trades
    ✔ upsert order snapshot

    ❌ no FSM
    ❌ no business logic
    """

    def __init__(self, storage, *, logger: logging.Logger | None = None):
        self.storage = storage
        self.logger = logger or logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # ORDER EVENTS (immutable log)
    # ------------------------------------------------------------------
    def append_order_event(self, evt: OrderEvent | None) -> None:
        if evt is None:
            return

        try:
            if not hasattr(self.storage, "append_order_events"):
                return

            now = datetime.now(tz=timezone.utc)

            row = {
                "exchange_id": int(evt.exchange_id or 0),
                "account_id": int(evt.account_id or 0),
                "order_id": str(evt.order_id),
                "symbol_id": int(evt.symbol_id or 0),
                "client_order_id": str(evt.client_order_id or ""),
                "status": str(evt.status or ""),
                "side": str(evt.side or ""),
                "type": str(evt.type or ""),
                "reduce_only": bool(evt.reduce_only),
                "price": float(evt.price) if evt.price is not None else None,
                "qty": float(evt.qty) if evt.qty is not None else None,
                "filled_qty": float(evt.filled_qty) if evt.filled_qty is not None else None,
                "source": str(evt.source or "ws_user"),
                "ts_ms": int(evt.ts_ms or 0),
                "recv_ts": now,
                "raw_json": json.dumps(evt.raw_json or {}, ensure_ascii=False, default=str),
            }

            self.storage.append_order_events([row])

        except Exception:
            self.logger.exception(
                "[OMS][ORDER_EVENT] append failed order_id=%s",
                getattr(evt, "order_id", None),
            )

    # ------------------------------------------------------------------
    # TRADES (fills)
    # ------------------------------------------------------------------
    def write_trade(self, trade_evt: TradeEvent | None) -> None:
        if trade_evt is None:
            return

        try:
            self.storage.upsert_trades([trade_evt.to_row()])
        except Exception:
            self.logger.exception(
                "[OMS][TRADE] upsert failed trade_id=%s",
                getattr(trade_evt, "trade_id", None),
            )

    # ------------------------------------------------------------------
    # ORDER SNAPSHOT (derived state)
    # ------------------------------------------------------------------
    def upsert_order_snapshot(self, agg) -> None:
        try:
            row = {
                "exchange_id": int(agg.exchange_id),
                "account_id": int(agg.account_id),
                "symbol_id": int(agg.symbol_id),

                "order_id": str(agg.order_id),
                "client_order_id": str(agg.client_order_id or ""),

                "status": str(agg.status),
                "side": str(agg.side or ""),
                "type": str(agg.type or ""),
                "reduce_only": bool(agg.reduce_only),

                "price": float(agg.price) if agg.price is not None else None,
                "qty": float(agg.qty) if agg.qty is not None else None,
                "filled_qty": float(agg.filled_qty) if agg.filled_qty is not None else None,

                # DB constraint safe
                "strategy_id": getattr(agg, "strategy_id", None) or "unknown",
                "pos_uid": getattr(agg, "pos_uid", None),

                "ts_ms": int(agg.ts_ms),
                "raw_json": {},
            }

            self.storage.upsert_orders([row])

        except Exception:
            self.logger.exception(
                "[OMS][ORDER_SNAPSHOT] upsert failed order_id=%s",
                getattr(agg, "order_id", None),
            )
