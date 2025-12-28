# src/platform/core/oms/writer.py
from __future__ import annotations
import logging
import json
from datetime import datetime, timezone

from .events import OrderEvent, TradeEvent
from .state_machine import OrderStateMachine, OrderSnapshot, InvalidOrderTransition


class OmsWriter:
    def __init__(self, storage, *, oms=None, logger: logging.Logger | None = None):
        self.storage = storage
        self.oms = oms  # OrderManager (опционально)
        self.logger = logger or logging.getLogger(__name__)
        self.sm = OrderStateMachine()


    def _append_order_event(self, order_evt: OrderEvent) -> None:
        """STEP G.1: append raw order events into DB (idempotent insert)."""
        try:
            if not hasattr(self.storage, "append_order_events"):
                return
            now = datetime.now(tz=timezone.utc)
            row = {
                "exchange_id": int(order_evt.exchange_id),
                "account_id": int(order_evt.account_id),
                "order_id": str(order_evt.order_id),
                "symbol_id": int(order_evt.symbol_id),
                "client_order_id": str(order_evt.client_order_id or ""),
                "status": str(order_evt.status or ""),
                "side": str(order_evt.side or ""),
                "type": str(order_evt.type or ""),
                "reduce_only": bool(order_evt.reduce_only),
                "price": float(order_evt.price) if order_evt.price is not None else None,
                "qty": float(order_evt.qty or 0.0) if order_evt.qty is not None else None,
                "filled_qty": float(order_evt.filled_qty or 0.0) if order_evt.filled_qty is not None else None,
                "source": str(order_evt.source or "ws_user"),
                "ts_ms": int(order_evt.ts_ms or 0),
                "recv_ts": now,
                "raw_json": json.dumps(order_evt.raw or {}, ensure_ascii=False, default=str),
            }
            self.storage.append_order_events([row])
        except Exception:
            self.logger.exception("[OMS][ORDER_EVENT] append failed")


    def write_order(self, order_evt: OrderEvent | None) -> None:
        if order_evt is None:
            return
        self.upsert_order(order_evt)

    def write_trade(self, trade_evt: TradeEvent | None) -> None:
        if trade_evt is None:
            return
        self.upsert_trade(trade_evt)

    def upsert_order(self, order_evt: OrderEvent) -> None:
        # 1) enrichment из placeholder по client_order_id
        meta = None
        if self.oms and order_evt.client_order_id:
            try:
                meta = self.oms.get_pending_by_client_id(order_evt.client_order_id)
            except Exception:
                meta = None

        if meta:
            order_evt.strategy_id = meta.get("strategy_id") or order_evt.strategy_id
            order_evt.pos_uid = meta.get("pos_uid") or order_evt.pos_uid

        # 2) текущий ордер из БД (для проверки переходов)
        prev_row = self.storage.get_order(
            exchange_id=order_evt.exchange_id,
            account_id=order_evt.account_id,
            order_id=str(order_evt.order_id),
        )

        prev = None
        if prev_row:
            prev = OrderSnapshot(
                status=str(prev_row.get("status") or ""),
                ts_ms=int(prev_row.get("ts_ms") or 0) if prev_row.get("ts_ms") is not None else None,
                filled_qty=float(prev_row.get("filled_qty") or 0.0) if prev_row.get("filled_qty") is not None else None,
            )

        if self.sm.should_ignore_event(prev, int(order_evt.ts_ms)):
            self.logger.debug(
                "[OMS][ORDER] ignore out-of-order order_id=%s prev_ts=%s evt_ts=%s status=%s",
                order_evt.order_id,
                getattr(prev, "ts_ms", None),
                order_evt.ts_ms,
                order_evt.status,
            )
            return

        # 3) lifecycle
        try:
            next_status = self.sm.apply(prev, str(order_evt.status))
        except InvalidOrderTransition as e:
            self.logger.warning(
                "[OMS][ORDER] invalid transition order_id=%s prev=%s next=%s (ignored): %s",
                order_evt.order_id,
                getattr(prev, "status", None),
                order_evt.status,
                e,
            )
            return

        # 4) unified upsert через upsert_order_placeholder (чтобы схема совпадала с твоей)
        now = datetime.now(timezone.utc)
        created_at = prev_row.get("created_at") if prev_row else None
        if not created_at:
            created_at = datetime.fromtimestamp(int(order_evt.ts_ms) / 1000, tz=timezone.utc) if order_evt.ts_ms else now

        row = {
            "exchange_id": int(order_evt.exchange_id),
            "account_id": int(order_evt.account_id),
            "order_id": str(order_evt.order_id),
            "symbol_id": int(order_evt.symbol_id),

            "strategy_id": str(order_evt.strategy_id or (meta.get("strategy_id") if meta else "unknown") or "unknown"),
            "pos_uid": order_evt.pos_uid or (meta.get("pos_uid") if meta else None),

            "client_order_id": str(order_evt.client_order_id or ""),
            "side": str(order_evt.side or ""),
            "type": str(order_evt.type or ""),
            "reduce_only": bool(order_evt.reduce_only),

            "price": float(order_evt.price) if order_evt.price is not None else None,
            "qty": float(order_evt.qty or 0.0),
            "filled_qty": float(order_evt.filled_qty or 0.0),

            "status": str(next_status),
            "source": str(order_evt.source or "ws_user"),
            "ts_ms": int(order_evt.ts_ms or 0),

            "created_at": created_at,
            "updated_at": now,

            "raw_json": json.dumps(order_evt.raw, ensure_ascii=False, default=str),
        }

        self.storage.upsert_order_placeholder(row)

    def upsert_trade(self, trade_evt: TradeEvent) -> None:
        self.logger.debug(
            "[OMS][TRADE] trade_id=%s order_id=%s qty=%s price=%s pnl=%s",
            trade_evt.trade_id,
            trade_evt.order_id,
            trade_evt.qty,
            trade_evt.price,
            trade_evt.realized_pnl,
        )
        self.storage.upsert_trades([trade_evt.to_row()])
