# src/platform/core/position/aggregator.py
from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Dict, Optional, Set, Tuple

from src.platform.core.oms.events import TradeEvent
from .position_state import PositionState


logger = logging.getLogger(__name__)


def _d(x) -> Decimal:
    try:
        return Decimal(str(x))
    except (InvalidOperation, TypeError):
        return Decimal("0")


class PositionAggregator:
    """
    Строит текущую позицию по потоку TradeEvent (fills).

    Модель:
      - qty signed: LONG>0, SHORT<0
      - entry_price: средняя цена входа для текущей открытой позиции
      - realized_pnl: накапливается при частичном/полном закрытии
      - fees: суммарные комиссии

    Идемпотентность:
      - защищаемся от повторов по trade_id (seen_trade_ids)
    """

    EPS = Decimal("1e-8")

    def __init__(self, storage=None, *, logger_: Optional[logging.Logger] = None):
        self.storage = storage
        self.log = logger_ or logger

        # key: (exchange_id, account_id, symbol_id)
        self.positions: Dict[Tuple[int, int, int], PositionState] = {}

        # key: (exchange_id, account_id) -> seen trade ids
        self.seen_trade_ids: Dict[Tuple[int, int], Set[str]] = {}

    def _pos_key(self, t: TradeEvent) -> Tuple[int, int, int]:
        return (t.exchange_id, t.account_id, t.symbol_id)

    def _acct_key(self, t: TradeEvent) -> Tuple[int, int]:
        return (t.exchange_id, t.account_id)

    def _pos_uid(self, t: TradeEvent) -> str:
        # стабильный UID под one-way (BOTH)
        return f"{t.exchange_id}:{t.account_id}:{t.symbol_id}:BOTH"

    def get(self, exchange_id: int, account_id: int, symbol_id: int) -> PositionState:
        key = (exchange_id, account_id, symbol_id)
        if key not in self.positions:
            self.positions[key] = PositionState(
                exchange_id=exchange_id,
                account_id=account_id,
                symbol_id=symbol_id,
                pos_uid=f"{exchange_id}:{account_id}:{symbol_id}:BOTH",
            )
        return self.positions[key]

    @staticmethod
    def _pos_side_from_qty(qty: Decimal) -> str:
        return "LONG" if qty > 0 else "SHORT"

    @staticmethod
    def _delta_from_trade(side: str, qty: Decimal) -> Optional[Decimal]:
        s = (side or "").upper()
        if s in ("BUY", "LONG"):
            return qty
        if s in ("SELL", "SHORT"):
            return -qty
        return None

    @staticmethod
    def _realized_for_close(
        close_qty_abs: Decimal, close_price: Decimal, entry: Decimal, direction: Decimal
    ) -> Decimal:
        # direction: +1 for long, -1 for short (based on prev_qty sign)
        if direction > 0:
            return (close_price - entry) * close_qty_abs
        return (entry - close_price) * close_qty_abs

    def on_trade(self, t: TradeEvent) -> Optional[PositionState]:
        """
        Обновляет позицию по одному трейду.
        Возвращает обновлённый PositionState или None (если дубликат/мусор).
        """

        # --- дедуп по trade_id ---
        trade_id = str(t.trade_id or "")
        if not trade_id:
            return None

        acct_key = self._acct_key(t)
        if acct_key not in self.seen_trade_ids:
            self.seen_trade_ids[acct_key] = set()
        if trade_id in self.seen_trade_ids[acct_key]:
            return None
        self.seen_trade_ids[acct_key].add(trade_id)

        # --- достаём/создаём PositionState ---
        key = self._pos_key(t)
        pos = self.positions.get(key)
        if not pos:
            pos = PositionState(
                exchange_id=t.exchange_id,
                account_id=t.account_id,
                symbol_id=t.symbol_id,
                pos_uid=self._pos_uid(t),
            )
            self.positions[key] = pos

        # ✅ если уже CLOSED — больше не трогаем
        if (pos.status or "").upper() == "CLOSED":
            self.log.debug(f"[POSITION] Ignore trade {trade_id} — position already CLOSED")
            return pos

        # --- нормализуем входные поля ---
        price = _d(t.price)
        qty = _d(t.qty)
        fee = _d(t.fee)

        if price <= 0 or qty <= 0:
            return None

        delta = self._delta_from_trade(t.side, qty)
        if delta is None:
            return None

        prev_qty = _d(pos.qty)
        prev_entry = _d(pos.entry_price)
        prev_realized = _d(pos.realized_pnl)
        prev_fees = _d(pos.fees)

        new_fees = prev_fees + fee
        now_ts = t.ts or datetime.now(timezone.utc)

        # CASE 1: flat -> open
        if abs(prev_qty) <= self.EPS:
            pos.qty = float(delta)
            pos.entry_price = float(price)
            pos.realized_pnl = float(prev_realized)
            pos.fees = float(new_fees)

            pos.opened_at = now_ts
            pos.closed_at = None
            pos.status = "OPEN"

            pos.last_trade_id = trade_id
            pos.last_ts = now_ts

            self._maybe_persist(pos)
            return pos

        # CASE 2: same direction (increase position)
        if (prev_qty > 0 and delta > 0) or (prev_qty < 0 and delta < 0):
            new_qty = prev_qty + delta
            abs_prev = abs(prev_qty)
            abs_delta = abs(delta)

            if abs_prev + abs_delta > 0:
                new_entry = (prev_entry * abs_prev + price * abs_delta) / (abs_prev + abs_delta)
            else:
                new_entry = price

            pos.qty = float(new_qty)
            pos.entry_price = float(new_entry)
            pos.realized_pnl = float(prev_realized)
            pos.fees = float(new_fees)

            pos.status = "OPEN"
            pos.last_trade_id = trade_id
            pos.last_ts = now_ts


            self._maybe_persist(pos)
            return pos

        # CASE 3: opposite direction (reduce/close/flip)
        abs_prev = abs(prev_qty)
        abs_delta = abs(delta)
        close_abs = min(abs_prev, abs_delta)

        direction = Decimal("1") if prev_qty > 0 else Decimal("-1")
        realized_add = self._realized_for_close(close_abs, price, prev_entry, direction)
        new_realized = prev_realized + realized_add

        new_qty = prev_qty + delta  # could be 0 or flipped

        # fully closed
        if abs(new_qty) <= self.EPS:
            pos.qty = 0.0
            pos.entry_price = 0.0
            pos.realized_pnl = float(new_realized)
            pos.fees = float(new_fees)

            pos.closed_at = now_ts
            pos.status = "CLOSED"

            pos.last_trade_id = trade_id
            pos.last_ts = now_ts

            self._maybe_persist(pos)
            return pos

        # flipped: остаток в противоположную сторону
        pos.qty = float(new_qty)
        pos.entry_price = float(price)  # новый вход = цена флипа
        pos.realized_pnl = float(new_realized)
        # 🔹 Binance realized pnl — накапливаем ВСЕГДА
        pos.exchange_realized_pnl += float(t.realized_pnl or 0)
        pos.fees = float(new_fees)

        pos.status = "OPEN"
        pos.last_trade_id = trade_id
        pos.last_ts = now_ts


        self._maybe_persist(pos)
        return pos

    def _maybe_persist(self, pos: PositionState) -> None:
        """Пишем позицию в БД если storage умеет."""
        if not self.storage:
            return

        # поддержим несколько возможных имён методов, чтобы не ломать проект
        if hasattr(self.storage, "upsert_position"):
            self.storage.upsert_position(pos)
        elif hasattr(self.storage, "upsert_positions"):
            self.storage.upsert_positions([pos])

    def on_mark_price(self, exchange_id: int, account_id: int, symbol_id: int, mark_price: float):
        """
        Обновление unrealized_pnl по mark price.
        Вызывается на каждом тике markPrice.
        """
        key = (exchange_id, account_id, symbol_id)
        pos = self.positions.get(key)

        if not pos:
            return None

        if pos.qty == 0 or pos.entry_price <= 0:
            pos.unrealized_pnl = 0.0
            return pos

        qty = abs(pos.qty)

        if pos.qty > 0:  # LONG
            pos.unrealized_pnl = (mark_price - pos.entry_price) * qty
        else:            # SHORT
            pos.unrealized_pnl = (pos.entry_price - mark_price) * qty

        pos.mark_price = mark_price
        pos.updated_at = datetime.now(tz=timezone.utc)

        self._maybe_persist(pos)
        return pos
