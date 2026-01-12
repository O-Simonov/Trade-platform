# src/platform/core/oms/oms.py
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional, Tuple

from src.platform.core.models.order import OrderIntent
from src.platform.core.models.enums import Side as ModelSide  # LONG/SHORT/FLAT
from src.platform.core.oms.decisions import resolve_order, Side as DecSide
from src.platform.core.oms.events import OrderEvent
from src.platform.core.position.position_manager import PositionManager


class OrderManager:
    """
    K6.8 OMS

    Responsibilities:
      ✔ order lifecycle FSM
      ✔ pending submit tracking
      ✔ Order ↔ Position decision (OPEN / REDUCE)
      ✔ reduceOnly enforcement
      ✖ NO exchange I/O
    """

    EPS_QTY_ABS: float = 1e-12

    def __init__(
        self,
        *,
        storage: Any,
        exchange_id: int,
        account_id: int,
        position_manager: PositionManager,
        logger: logging.Logger | None = None,
    ) -> None:
        self.storage = storage
        self.exchange_id = int(exchange_id)
        self.account_id = int(account_id)
        self.pm = position_manager
        self.logger = logger or logging.getLogger("oms")

        # key: (exchange_id, account_id, client_order_id) -> ts
        self._pending: Dict[Tuple[int, int, str], float] = {}

        # remember last OPEN side to allow reduce-only close even if PM lags
        self._last_open_side: Dict[Tuple[int, int, int], ModelSide] = {}

        self.logger.info("[OMS] position_manager_id=%s", id(self.pm))

    # ============================================================
    # Pending submits
    # ============================================================

    def record_pending_submit(self, *, client_order_id: str) -> None:
        if not client_order_id:
            return
        key = (self.exchange_id, self.account_id, str(client_order_id))
        self._pending[key] = time.time()

    def should_submit(self, client_order_id: str) -> bool:
        if not client_order_id:
            return False
        key = (self.exchange_id, self.account_id, str(client_order_id))
        return key not in self._pending

    def reconcile_pending_timeouts(self, timeout_sec: float) -> int:
        now = time.time()
        timeout_sec = float(timeout_sec or 0.0)
        if timeout_sec <= 0:
            return 0

        removed = 0
        for key, ts in list(self._pending.items()):
            if now - float(ts or 0.0) >= timeout_sec:
                self._pending.pop(key, None)
                removed += 1
                ex, acc, cid = key
                self.logger.warning("[OMS] pending timeout ex=%s acc=%s cid=%s", ex, acc, cid)
        return removed

    # ============================================================
    # WS → OMS
    # ============================================================

    def apply_event(self, ev: OrderEvent) -> None:
        """
        Any ORDER_EVENT with client_order_id clears pending.
        """
        cid = getattr(ev, "client_order_id", None)
        if not cid:
            return
        ex = int(getattr(ev, "exchange_id", 0) or self.exchange_id)
        acc = int(getattr(ev, "account_id", 0) or self.account_id)
        key = (ex, acc, str(cid))
        self._pending.pop(key, None)

    # ============================================================
    # helpers
    # ============================================================

    @staticmethod
    def _to_dec_side(v: Any) -> DecSide:
        if v is None:
            return DecSide.FLAT
        if isinstance(v, DecSide):
            return v
        s = str(v).strip().upper()
        if s in ("LONG", "BUY"):
            return DecSide.LONG
        if s in ("SHORT", "SELL"):
            return DecSide.SHORT
        return DecSide.FLAT

    @staticmethod
    def _to_model_side(v: Any) -> Optional[ModelSide]:
        if v is None:
            return None
        if isinstance(v, ModelSide):
            return v
        s = str(v).strip().upper()
        if s in ("LONG", "BUY"):
            return ModelSide.LONG
        if s in ("SHORT", "SELL"):
            return ModelSide.SHORT
        if s == "FLAT":
            return ModelSide.FLAT
        return None

    @staticmethod
    def _opposite_model_side(s: ModelSide) -> ModelSide:
        if s == ModelSide.LONG:
            return ModelSide.SHORT
        if s == ModelSide.SHORT:
            return ModelSide.LONG
        # if unknown -> safe default (reduceOnly will protect)
        return ModelSide.SHORT

    @staticmethod
    def _qty_abs(v: Any) -> float:
        try:
            return abs(float(v or 0.0))
        except Exception:
            return 0.0

    # ============================================================
    # Intent resolution (Strategy → OMS decision)
    # ============================================================

    def resolve_intent(self, intent: OrderIntent) -> Optional[OrderIntent]:
        if intent is None:
            return None

        sym = str(getattr(intent, "symbol", "") or "").upper()
        qty_req = float(getattr(intent, "qty", 0.0) or 0.0)
        if not sym or qty_req <= 0.0:
            return None

        symbol_id = int(getattr(intent, "symbol_id", 0) or 0)
        if symbol_id <= 0:
            return None

        reduce_only_requested = bool(getattr(intent, "reduce_only", False))

        # ----- current position from PM -----
        agg = self.pm.get(self.exchange_id, self.account_id, symbol_id)

        pos_qty_abs = 0.0
        pos_side_model = ModelSide.FLAT
        pos_side_dec = DecSide.FLAT

        if agg is not None:
            pos_qty_abs = self._qty_abs(getattr(agg, "qty", 0.0))
            ms = self._to_model_side(getattr(agg, "side", None))
            if ms is not None:
                pos_side_model = ms
            pos_side_dec = self._to_dec_side(pos_side_model)

        if pos_qty_abs <= self.EPS_QTY_ABS:
            pos_qty_abs = 0.0
            pos_side_model = ModelSide.FLAT
            pos_side_dec = DecSide.FLAT

        # ----- requested side -----
        req_model_side = self._to_model_side(getattr(intent, "side", None))

        # If side missing but reduceOnly=True -> compute close side
        if req_model_side is None and reduce_only_requested:
            key = (self.exchange_id, self.account_id, symbol_id)

            if pos_side_model in (ModelSide.LONG, ModelSide.SHORT) and pos_qty_abs > 0.0:
                req_model_side = self._opposite_model_side(pos_side_model)
            elif key in self._last_open_side:
                req_model_side = self._opposite_model_side(self._last_open_side[key])
            else:
                req_model_side = ModelSide.SHORT  # safe default

            # IMPORTANT: set into intent so Exchange will never see None
            intent.side = req_model_side

        if req_model_side is None:
            self.logger.warning("[OMS][BLOCK] %s reason=missing_side", sym)
            return None

        order_side_dec = self._to_dec_side(req_model_side)

        dec = resolve_order(
            pos_side=pos_side_dec,
            pos_qty=pos_qty_abs,
            order_side=order_side_dec,
            order_qty=qty_req,
        )

        if not dec.allowed:
            self.logger.warning("[OMS][BLOCK] %s reason=%s", sym, dec.reason)
            return None

        # Safe override: reduceOnly close even if PM says FLAT (WS lag)
        if reduce_only_requested and dec.mode == "OPEN":
            dec.mode = "REDUCE"
            dec.reduce_only = True
            dec.qty = qty_req
            self.logger.warning(
                "[OMS][SAFE OVERRIDE] %s reduceOnly requested but PM is FLAT → send reduceOnly anyway",
                sym,
            )

        # persist normalized
        intent.qty = float(dec.qty)
        intent.reduce_only = bool(dec.reduce_only)
        intent.side = req_model_side  # ensure not None

        # remember last_open_side for laggy closes
        if dec.mode == "OPEN" and req_model_side in (ModelSide.LONG, ModelSide.SHORT):
            self._last_open_side[(self.exchange_id, self.account_id, symbol_id)] = req_model_side

        if dec.mode == "OPEN":
            self.logger.info(
                "[OMS][OPEN] %s %s qty=%.6f reduce_only=%s",
                sym, req_model_side, intent.qty, intent.reduce_only
            )
        elif dec.mode == "REDUCE":
            self.logger.info(
                "[OMS][REDUCE] %s %s qty=%.6f reduce_only=%s",
                sym, req_model_side, intent.qty, intent.reduce_only
            )
        else:
            self.logger.info(
                "[OMS] %s mode=%s side=%s qty=%.6f reduce_only=%s",
                sym, dec.mode, req_model_side, intent.qty, intent.reduce_only
            )

        return intent
