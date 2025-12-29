# src/platform/core/position/aggregate.py
from __future__ import annotations
from dataclasses import dataclass

from src.platform.core.oms.events import TradeEvent
from datetime import datetime, timezone

def _utcnow():
    return datetime.now(timezone.utc)

@dataclass(slots=True)
class PositionAggregate:
    exchange_id: int
    account_id: int
    symbol_id: int

    qty: float
    avg_price: float

    realized_pnl: float
    fees: float

    last_ts_ms: int

    @classmethod
    def empty(cls, *, exchange_id: int, account_id: int, symbol_id: int):
        return cls(
            exchange_id=exchange_id,
            account_id=account_id,
            symbol_id=symbol_id,
            qty=0.0,
            avg_price=0.0,
            realized_pnl=0.0,
            fees=0.0,
            last_ts_ms=0,
        )

    def apply_trade(self, evt: TradeEvent) -> bool:
        """
        Apply TradeEvent to position.
        Returns True if position changed.
        """

        ts = int(evt.ts_ms or 0)
        if ts <= self.last_ts_ms:
            return False

        if not evt.side:
            return False

        side = evt.side.upper()
        if side not in ("BUY", "SELL"):
            return False

        trade_qty = abs(float(evt.qty or 0.0))
        trade_price = float(evt.price or 0.0)
        if trade_qty <= 0 or trade_price <= 0:
            return False

        signed_qty = trade_qty if side == "BUY" else -trade_qty

        prev_qty = self.qty
        prev_avg = self.avg_price
        new_qty = prev_qty + signed_qty

        # ------------------------------------------------------------
        # Opening or increasing in same direction
        # ------------------------------------------------------------
        if prev_qty == 0 or (prev_qty > 0) == (signed_qty > 0):
            notional = abs(prev_qty) * prev_avg + trade_qty * trade_price
            self.qty = new_qty
            self.avg_price = notional / abs(self.qty) if self.qty != 0 else 0.0

        # ------------------------------------------------------------
        # Reducing / closing / flipping
        # ------------------------------------------------------------
        else:
            closed_qty = min(abs(prev_qty), trade_qty)

            # realized pnl on closed part
            pnl = closed_qty * (trade_price - prev_avg)
            if prev_qty < 0:  # closing short
                pnl = -pnl

            self.realized_pnl += pnl
            self.qty = new_qty

            # position fully closed
            if self.qty == 0:
                self.avg_price = 0.0

            # flipped to opposite side
            elif (prev_qty > 0) != (self.qty > 0):
                self.avg_price = trade_price

            # partially reduced, same side remains
            else:
                self.avg_price = prev_avg

        self.fees += float(evt.fee or 0.0)
        self.last_ts_ms = ts
        return True


    def to_row(self, *, last_trade_id: str | None = None) -> dict:
        return {
            "exchange_id": self.exchange_id,
            "account_id": self.account_id,
            "symbol_id": self.symbol_id,

            "side": "LONG" if self.qty > 0 else "SHORT" if self.qty < 0 else "FLAT",
            "qty": abs(self.qty),
            "entry_price": self.avg_price,

            "realized_pnl": self.realized_pnl,
            "fees": self.fees,

            # ✅ ВАЖНО
            "last_ts": datetime.fromtimestamp(self.last_ts_ms / 1000, tz=timezone.utc),

            "last_trade_id": last_trade_id,
            "status": "OPEN" if self.qty != 0 else "CLOSED",
            "updated_at": datetime.now(tz=timezone.utc),
            "source": "trade_aggregate",
        }
