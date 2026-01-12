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


def _side_is_long(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, Side):
        return getattr(v, "name", "").upper() == "LONG"
    return str(v).strip().upper() == "LONG"


def _side_is_short(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, Side):
        return getattr(v, "name", "").upper() == "SHORT"
    return str(v).strip().upper() == "SHORT"


def _side_is_flat(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, Side):
        return getattr(v, "name", "").upper() in ("FLAT", "NONE")
    return str(v).strip().upper() in ("FLAT", "NONE", "")


def _norm_pos_side(v: Any) -> Optional[Side]:
    """
    Normalize to Side.LONG / Side.SHORT / None (FLAT).
    """
    if _side_is_flat(v):
        return None
    if _side_is_long(v):
        return Side.LONG
    if _side_is_short(v):
        return Side.SHORT
    return None


@dataclass(slots=True)
class PositionAggregate:
    """
    K6.7 Position FSM:
      - qty ALWAYS >= 0
      - direction stored in side: LONG/SHORT/None(FLAT)
      - lifecycle fields for position_ledger
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
    mark_price: float = 0.0

    last_trade_id: str = ""
    _dirty: bool = False

    # -------- lifecycle (ledger) --------
    pos_uid: str = ""
    opened_at: datetime | None = None
    closed_at: datetime | None = None
    qty_opened: float = 0.0
    qty_closed: float = 0.0
    scale_in_count: int = 0

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
            pos_uid="",
            opened_at=None,
            closed_at=None,
            qty_opened=0.0,
            qty_closed=0.0,
            scale_in_count=0,
        )

    def is_open(self) -> bool:
        return self.side is not None and self.qty > 0.0

    def side_str(self) -> str:
        if self.side == Side.LONG:
            return "LONG"
        if self.side == Side.SHORT:
            return "SHORT"
        return "FLAT"

    def mark_dirty(self) -> None:
        self._dirty = True

    def is_dirty(self) -> bool:
        return bool(self._dirty)

    def clear_dirty(self) -> None:
        self._dirty = False

    def _new_lifecycle(self, ts_ms: int) -> None:
        """
        Called when FLAT -> OPEN.
        """
        dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc) if ts_ms else datetime.now(tz=timezone.utc)
        self.opened_at = dt
        self.closed_at = None
        self.qty_opened = 0.0
        self.qty_closed = 0.0
        self.scale_in_count = 0
        self.pos_uid = f"{self.exchange_id}:{self.account_id}:{self.symbol_id}:{ts_ms}"

        # reset per-position accumulators for a new lifecycle
        self.realized_pnl = 0.0
        self.fees = 0.0

    def _flat_reset(self) -> None:
        """
        OPEN -> FLAT transition: keep realized/fees (for last lifecycle),
        but clear direction/qty/entry/mark/upnl.
        """
        self.side = None
        self.qty = 0.0
        self.entry_price = 0.0
        self.unrealized_pnl = 0.0
        self.mark_price = 0.0

    def apply_trade(self, evt: TradeEvent) -> bool:
        ts = int(getattr(evt, "ts_ms", 0) or 0)
        trade_id = str(getattr(evt, "trade_id", "") or "").strip()

        if ts < int(self.last_ts_ms or 0):
            return False
        if ts == int(self.last_ts_ms or 0):
            if trade_id and trade_id == str(self.last_trade_id or ""):
                return False

        side_txt = str(getattr(evt, "side", "") or "").upper()
        qty = float(getattr(evt, "qty", 0.0) or 0.0)
        trade_price = float(getattr(evt, "price", 0.0) or 0.0)
        if qty <= 0.0 or trade_price <= 0.0:
            return False
        if side_txt not in ("BUY", "SELL"):
            return False

        # current signed qty
        cur_qty = float(self.qty or 0.0)
        cur_side = self.side
        if cur_side == Side.LONG and cur_qty > 0:
            cur_signed = +cur_qty
        elif cur_side == Side.SHORT and cur_qty > 0:
            cur_signed = -cur_qty
        else:
            cur_signed = 0.0

        trade_signed = +qty if side_txt == "BUY" else -qty
        new_signed = cur_signed + trade_signed

        entry = float(self.entry_price or 0.0)

        # CASE A: flat -> open (new lifecycle)
        if _is_close(cur_signed, 0.0):
            if not _is_close(new_signed, 0.0):
                self._new_lifecycle(ts)
                self.side = Side.LONG if new_signed > 0 else Side.SHORT
                self.qty = abs(new_signed)
                self.entry_price = trade_price
                self.qty_opened = float(self.qty)
            else:
                # flat + flat (rare)
                self._flat_reset()

        # CASE B: position exists
        else:
            closed_qty = 0.0
            if (cur_signed > 0) != (trade_signed > 0):
                closed_qty = min(abs(cur_signed), abs(trade_signed))

            # realized on closing part
            if closed_qty > 0.0 and entry > 0.0:
                if cur_signed > 0:
                    pnl = closed_qty * (trade_price - entry)   # close LONG with SELL
                else:
                    pnl = closed_qty * (entry - trade_price)   # close SHORT with BUY
                self.realized_pnl = float(self.realized_pnl or 0.0) + float(pnl)
                self.qty_closed = float(self.qty_closed or 0.0) + float(closed_qty)

            if _is_close(new_signed, 0.0):
                # fully closed
                self._flat_reset()
                if self.opened_at is not None:
                    self.closed_at = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                    # if we know qty_opened, clamp closed to opened
                    if self.qty_opened > 0:
                        self.qty_closed = min(self.qty_opened, self.qty_closed if self.qty_closed > 0 else self.qty_opened)
            else:
                new_side = Side.LONG if new_signed > 0 else Side.SHORT
                new_qty = abs(new_signed)
                flipped = (cur_signed > 0) != (new_signed > 0)

                # scale-in counter (same direction and increased size)
                if not flipped and (abs(new_signed) > abs(cur_signed)):
                    self.scale_in_count = int(self.scale_in_count or 0) + 1

                self.side = new_side
                self.qty = new_qty
                if flipped:
                    # new lifecycle inside same pos_uid is debatable, но для ledger проще:
                    # считаем это "FLIPPED" внутри одного uid — entry обновляем, uid оставляем.
                    self.entry_price = trade_price
                # else keep entry_price

        # fees
        fee = float(getattr(evt, "fee", 0.0) or 0.0)
        self.fees = float(self.fees or 0.0) + fee

        self.last_ts_ms = ts
        if trade_id:
            self.last_trade_id = trade_id

        self.mark_dirty()
        return True

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

    def apply_rest_mark(self, *, mark_price: float | None = None, unrealized_pnl: float | None = None) -> bool:
        if not self.is_open():
            return False

        changed = False
        mp = float(mark_price or 0.0) if mark_price is not None else 0.0
        if mp > 0.0 and not _is_close(self.mark_price, mp):
            self.mark_price = mp
            changed = True

        if self.entry_price > 0.0 and self.mark_price > 0.0:
            if self.side == Side.LONG:
                calc = (self.mark_price - self.entry_price) * self.qty
            elif self.side == Side.SHORT:
                calc = (self.entry_price - self.mark_price) * self.qty
            else:
                calc = 0.0

            if not _is_close(self.unrealized_pnl, calc):
                self.unrealized_pnl = float(calc)
                changed = True
        else:
            if unrealized_pnl is not None:
                up = float(unrealized_pnl or 0.0)
                if not _is_close(self.unrealized_pnl, up):
                    self.unrealized_pnl = up
                    changed = True

        if changed:
            self.last_ts_ms = int(time.time() * 1000)
            self.mark_dirty()

        return changed

    def apply_rest_snapshot(
        self,
        *,
        side: Any | None,
        qty: float | None,
        entry_price: float | None,
        unrealized_pnl: float | None,
        mark_price: float | None = None,
    ) -> bool:
        if side is None and qty is None and entry_price is None:
            return self.apply_rest_mark(mark_price=mark_price, unrealized_pnl=unrealized_pnl)

        rest_side = _norm_pos_side(side)
        q = float(qty or 0.0)
        ep = float(entry_price or 0.0)
        upnl = float(unrealized_pnl or 0.0)

        if q <= 0.0 or rest_side is None:
            rest_side = None
            q = 0.0

        if self.is_open() and q > 0.0 and rest_side is not None and self.side is not None:
            if rest_side != self.side:
                return False

        changed = False

        if rest_side != self.side:
            self.side = rest_side
            changed = True

        if not _is_close(self.qty, q):
            self.qty = q
            changed = True

        if self.side is None or self.qty <= 0.0:
            if self.entry_price != 0.0:
                self.entry_price = 0.0
                changed = True
            if self.unrealized_pnl != 0.0:
                self.unrealized_pnl = 0.0
                changed = True
            if self.mark_price != 0.0:
                self.mark_price = 0.0
                changed = True
            if self.opened_at is not None and self.closed_at is None:
                # best-effort: mark as closed (we might have restarted and only REST knows)
                self.closed_at = datetime.now(tz=timezone.utc)
                changed = True
        else:
            # if we resurrect an open from REST and have no opened_at -> create lifecycle now
            if self.opened_at is None:
                self._new_lifecycle(int(time.time() * 1000))
                changed = True

            if ep > 0.0 and not _is_close(self.entry_price, ep):
                self.entry_price = ep
                changed = True

            if self.apply_rest_mark(mark_price=mark_price, unrealized_pnl=upnl):
                changed = True

        if changed:
            self.last_ts_ms = int(time.time() * 1000)
            self.mark_dirty()

        return changed

    def to_row(self) -> dict:
        side_txt = self.side_str()
        status = "OPEN" if self.is_open() else "CLOSED"
        qty_abs = float(self.qty or 0.0)

        # notional (do not allow NULL later)
        px = float(self.mark_price or 0.0) if float(self.mark_price or 0.0) > 0 else float(self.entry_price or 0.0)
        position_value_usdt = abs(qty_abs) * float(px or 0.0)

        last_ts = datetime.fromtimestamp(self.last_ts_ms / 1000, tz=timezone.utc) if self.last_ts_ms else None

        return {
            "exchange_id": self.exchange_id,
            "account_id": self.account_id,
            "symbol_id": self.symbol_id,

            "side": side_txt,
            "qty": qty_abs,
            "entry_price": float(self.entry_price or 0.0),
            "mark_price": float(self.mark_price or 0.0),
            "last_price": float(self.mark_price or 0.0),

            "position_value_usdt": float(position_value_usdt or 0.0),

            "realized_pnl": float(self.realized_pnl or 0.0),
            "unrealized_pnl": float(self.unrealized_pnl or 0.0),
            "fees": float(self.fees or 0.0),

            "last_trade_id": str(self.last_trade_id or ""),
            "last_ts": last_ts,
            "status": status,
            "updated_at": datetime.now(tz=timezone.utc),
            "source": "position_aggregate",

            # ledger
            "pos_uid": str(self.pos_uid or ""),
            "opened_at": self.opened_at,
            "closed_at": self.closed_at,
            "qty_opened": float(self.qty_opened or 0.0),
            "qty_current": float(qty_abs),
            "qty_closed": float(self.qty_closed or 0.0),
            "scale_in_count": int(self.scale_in_count or 0),
        }
