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
        side = "FLAT"
        status = "CLOSED"
        if self.qty > 0:
            side = "LONG"
            status = "OPEN"
        elif self.qty < 0:
            side = "SHORT"
            status = "OPEN"

        return {
            "exchange_id": int(self.exchange_id),
            "account_id": int(self.account_id),
            "symbol_id": int(self.symbol_id),

            "side": side,
            "qty": float(self.qty),
            "entry_price": float(self.avg_price),

            "realized_pnl": float(self.realized_pnl),
            "unrealized_pnl": 0.0,  # будет обновляться mark_price poller’ом/тикером
            "exchange_realized_pnl": 0.0,  # если нужно — позже добавим накопление из Binance

            "fees": float(self.fees),
            "mark_price": 0.0,

            "last_trade_id": last_trade_id,
            "status": status,
            "last_ts": int(self.last_ts_ms),

            "updated_at": _utcnow(),
            "source": "trade_aggregate",  # ✅ вот это и нужно
        }
