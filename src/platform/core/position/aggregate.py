# src/platform/core/position/aggregate.py
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Any

from src.platform.core.models.enums import Side
from src.platform.core.oms.events import TradeEvent

EPS = 1e-12


def _is_close(a: float, b: float, eps: float = EPS) -> bool:
    return abs(float(a) - float(b)) <= eps


def _norm_pos_side(v: Any) -> Optional[Side]:
    if v is None:
        return None
    if isinstance(v, Side):
        return v
    s = str(v).strip().upper()
    if s == "LONG":
        return Side.LONG
    if s == "SHORT":
        return Side.SHORT
    return None


@dataclass(slots=True)
class PositionAggregate:
    """
    K6.7 Position FSM (one-way):
      - qty ALWAYS >= 0
      - direction stored in side: LONG/SHORT/None(FLAT)
    """

    exchange_id: int
    account_id: int
    symbol_id: int

    side: Optional[Side]          # LONG / SHORT / None (FLAT)
    qty: float                    # >= 0

    entry_price: float            # avg entry
    realized_pnl: float
    unrealized_pnl: float
    fees: float

    last_ts_ms: int
    mark_price: float = 0.0       # last mark price

    last_trade_id: str = ""       # useful for DB
    _dirty: bool = False

    # ------------------------------------------------------------------
    @classmethod
    def empty(cls, *, exchange_id: int, account_id: int, symbol_id: int) -> "PositionAggregate":
        return cls(
            exchange_id=int(exchange_id),
            account_id=int(account_id),
            symbol_id=int(symbol_id),
            side=None,
            qty=0.0,
            entry_price=0.0,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            fees=0.0,
            last_ts_ms=0,
            mark_price=0.0,
            last_trade_id="",
            _dirty=False,
        )

    # ------------------------------------------------------------------
    def is_open(self) -> bool:
        return self.side is not None and self.qty > 0.0

    def side_str(self) -> str:
        if self.side == Side.LONG:
            return "LONG"
        if self.side == Side.SHORT:
            return "SHORT"
        return "FLAT"

    # ------------------------------------------------------------------
    def mark_dirty(self) -> None:
        self._dirty = True

    def is_dirty(self) -> bool:
        return bool(self._dirty)

    def clear_dirty(self) -> None:
        self._dirty = False

    # ------------------------------------------------------------------
    def _flat_reset(self) -> None:
        self.side = None
        self.qty = 0.0
        self.entry_price = 0.0
        self.unrealized_pnl = 0.0
        self.mark_price = 0.0

    # ------------------------------------------------------------------
    def apply_trade(self, evt: TradeEvent) -> bool:
        """
        Authoritative source: TradeEvent stream (fills).
        evt.side: "BUY"/"SELL"
        """
        ts = int(getattr(evt, "ts_ms", 0) or 0)
        if ts <= self.last_ts_ms:
            return False

        side_txt = str(getattr(evt, "side", "") or "").upper()
        if side_txt not in ("BUY", "SELL"):
            return False

        trade_qty = abs(float(getattr(evt, "qty", 0.0) or 0.0))
        trade_price = float(getattr(evt, "price", 0.0) or 0.0)
        if trade_qty <= 0.0 or trade_price <= 0.0:
            return False

        # current signed qty
        cur_signed = 0.0
        if self.side == Side.LONG:
            cur_signed = +float(self.qty or 0.0)
        elif self.side == Side.SHORT:
            cur_signed = -float(self.qty or 0.0)

        # trade delta signed
        delta = trade_qty if side_txt == "BUY" else -trade_qty
        new_signed = cur_signed + delta

        # ----------------------------
        # case 1: increasing same dir or opening from FLAT
        # ----------------------------
        if cur_signed == 0.0 or (cur_signed > 0) == (delta > 0):
            # weighted avg entry
            prev_abs = abs(cur_signed)
            notional = prev_abs * float(self.entry_price or 0.0) + trade_qty * trade_price
            new_abs = abs(new_signed)

            self.side = Side.LONG if new_signed > 0 else Side.SHORT if new_signed < 0 else None
            self.qty = new_abs
            self.entry_price = (notional / new_abs) if new_abs > 0 else 0.0

        # ----------------------------
        # case 2: reducing / closing / flipping
        # ----------------------------
        else:
            closed_qty = min(abs(cur_signed), trade_qty)

            # pnl: LONG closed by SELL => (sell - entry)*closed
            #      SHORT closed by BUY => (entry - buy)*closed
            if cur_signed > 0:
                pnl = closed_qty * (trade_price - float(self.entry_price or 0.0))
            else:
                pnl = closed_qty * (float(self.entry_price or 0.0) - trade_price)

            self.realized_pnl += float(pnl)

            # after trade
            if _is_close(new_signed, 0.0):
                self._flat_reset()
            else:
                self.side = Side.LONG if new_signed > 0 else Side.SHORT
                self.qty = abs(new_signed)
                # flipped: remaining position entry = trade_price
                if (cur_signed > 0) != (new_signed > 0):
                    self.entry_price = trade_price
                # else: reduced but not flipped -> keep entry_price

        self.fees += float(getattr(evt, "fee", 0.0) or 0.0)
        self.last_ts_ms = ts
        self.last_trade_id = str(getattr(evt, "trade_id", "") or "")

        self.mark_dirty()
        return True

    # ------------------------------------------------------------------
    def apply_mark_price(self, price: float) -> bool:
        price = float(price or 0.0)
        if price <= 0.0 or not self.is_open() or self.entry_price <= 0.0:
            return False

        if _is_close(price, self.mark_price):
            return False

        self.mark_price = price

        if self.side == Side.LONG:
            new_upnl = (self.mark_price - self.entry_price) * self.qty
        elif self.side == Side.SHORT:
            new_upnl = (self.entry_price - self.mark_price) * self.qty
        else:
            new_upnl = 0.0

        if _is_close(new_upnl, self.unrealized_pnl):
            return False

        self.unrealized_pnl = float(new_upnl)
        self.mark_dirty()
        return True

    # ------------------------------------------------------------------
    def apply_rest_snapshot(
        self,
        *,
        side: Any,
        qty: float,
        entry_price: float,
        unrealized_pnl: float,
        mark_price: float | None = None,
    ) -> bool:
        """
        REST snapshot (secondary).

        Rules:
          - REST can close position (qty->0 or side=FLAT)
          - REST can correct qty/entry in SAME direction
          - REST must NOT flip direction if both non-zero
        """
        rest_side = _norm_pos_side(side)
        qty = float(qty or 0.0)
        entry_price = float(entry_price or 0.0)
        unrealized_pnl = float(unrealized_pnl or 0.0)

        # normalize REST "flat"
        if qty <= 0.0 or rest_side is None:
            rest_side = None
            qty = 0.0

        # forbid flip if both non-flat
        if self.is_open() and qty > 0.0 and rest_side is not None and self.side is not None:
            if rest_side != self.side:
                return False

        changed = False

        # apply
        if rest_side != self.side:
            self.side = rest_side
            changed = True

        if not _is_close(self.qty, qty):
            self.qty = qty
            changed = True

        if self.side is None or self.qty <= 0.0:
            # clean flat state
            if self.entry_price != 0.0:
                self.entry_price = 0.0
                changed = True
            if self.unrealized_pnl != 0.0:
                self.unrealized_pnl = 0.0
                changed = True
            if self.mark_price != 0.0:
                self.mark_price = 0.0
                changed = True
        else:
            if entry_price > 0.0 and not _is_close(self.entry_price, entry_price):
                self.entry_price = entry_price
                changed = True

            # accept REST uPnL only if we don't have mark yet
            if self.mark_price <= 0.0 and not _is_close(self.unrealized_pnl, unrealized_pnl):
                self.unrealized_pnl = unrealized_pnl
                changed = True

            if mark_price is not None and float(mark_price or 0.0) > 0.0:
                mp = float(mark_price)
                if not _is_close(self.mark_price, mp):
                    self.mark_price = mp
                    changed = True

        if changed:
            self.last_ts_ms = int(time.time() * 1000)
            self.mark_dirty()

        return changed

    # ------------------------------------------------------------------
    def to_row(self) -> dict:
        """
        Row dict for storage.upsert_positions(rows)

        IMPORTANT:
          - We include BOTH mark_price and last_price keys,
            so it works with older/newer schemas (extra keys are harmless).
        """
        side_txt = self.side_str()
        status = "OPEN" if self.is_open() else "CLOSED"
        qty_abs = float(self.qty or 0.0)

        return {
            "exchange_id": self.exchange_id,
            "account_id": self.account_id,
            "symbol_id": self.symbol_id,

            "side": side_txt,
            "qty": qty_abs,
            "entry_price": float(self.entry_price or 0.0),

            # schema variants:
            "mark_price": float(self.mark_price or 0.0),
            "last_price": float(self.mark_price or 0.0),

            "realized_pnl": float(self.realized_pnl or 0.0),
            "unrealized_pnl": float(self.unrealized_pnl or 0.0),
            "fees": float(self.fees or 0.0),

            "last_trade_id": str(self.last_trade_id or ""),
            "last_ts": datetime.fromtimestamp(self.last_ts_ms / 1000, tz=timezone.utc) if self.last_ts_ms else None,
            "status": status,

            "updated_at": datetime.now(tz=timezone.utc),
            "source": "position_aggregate",
        }
