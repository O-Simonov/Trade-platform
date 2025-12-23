# src/platform/core/oms/writer.py
from __future__ import annotations

import logging
from .events import OrderEvent, TradeEvent

logger = logging.getLogger(__name__)


class OmsWriter:
    def __init__(self, storage, logger=None):
        self.storage = storage
        self.logger = logger or logging.getLogger(__name__)

    # --- public API ---

    def write_order(self, order_evt: OrderEvent):
        if not order_evt:
            return
        return self.upsert_order(order_evt)

    def write_trade(self, trade_evt: TradeEvent | None):
        if trade_evt is None:
            return
        return self.upsert_trade(trade_evt)

    # --- internal ---

    def upsert_order(self, order_evt: OrderEvent):
        self.logger.debug(
            "[OMS][ORDER] order_id=%s status=%s qty=%s price=%s",
            order_evt.order_id,
            order_evt.status,
            order_evt.qty,
            order_evt.price,
        )
        return self.storage.upsert_order(order_evt)

    def upsert_trade(self, trade_evt: TradeEvent):
        self.logger.debug(
            "[OMS][TRADE] trade_id=%s order_id=%s qty=%s price=%s pnl=%s",
            trade_evt.trade_id,
            trade_evt.order_id,
            trade_evt.qty,
            trade_evt.price,
            trade_evt.realized_pnl,
        )
        return self.storage.upsert_trade(trade_evt)
