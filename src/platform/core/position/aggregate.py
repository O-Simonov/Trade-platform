# src/platform/core/position/aggregate.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from src.platform.core.oms.events import TradeEvent


@dataclass(slots=True)
class PositionAggregate:
    exchange_id: int
    account_id: int
    symbol_id: int

    # signed quantity: +LONG / -SHORT
    qty: float

    # entry price (avg)
    entry_price: float

    # PnL
    realized_pnl: float
    unrealized_pnl: float

    fees: float

    last_ts_ms: int

    # ------------------------------------------------------------------
    @classmethod
    def empty(cls, *, exchange_id: int, account_id: int, symbol_id: int) -> "PositionAggregate":
        return cls(
            exchange_id=exchange_id,
            account_id=account_id,
            symbol_id=symbol_id,
            qty=0.0,
            entry_price=0.0,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            fees=0.0,
            last_ts_ms=0,
        )

    # ------------------------------------------------------------------
    def apply_trade(self, evt: TradeEvent) -> bool:
        """
        Apply TradeEvent (authoritative source).
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
        prev_entry = self.entry_price
        new_qty = prev_qty + signed_qty

        # ------------------------------------------------------------
        # Same direction (open / increase)
        # ------------------------------------------------------------
        if prev_qty == 0 or (prev_qty > 0) == (signed_qty > 0):
            notional = abs(prev_qty) * prev_entry + trade_qty * trade_price
            self.qty = new_qty
            self.entry_price = notional / abs(self.qty) if self.qty != 0 else 0.0

        # ------------------------------------------------------------
        # Reduce / close / flip
        # ------------------------------------------------------------
        else:
            closed_qty = min(abs(prev_qty), trade_qty)

            pnl = closed_qty * (trade_price - prev_entry)
            if prev_qty < 0:  # closing short
                pnl = -pnl

            self.realized_pnl += pnl
            self.qty = new_qty

            if self.qty == 0:
                self.entry_price = 0.0
            elif (prev_qty > 0) != (self.qty > 0):  # flipped
                self.entry_price = trade_price

        self.fees += float(evt.fee or 0.0)
        self.last_ts_ms = ts
        return True

    # ------------------------------------------------------------------
    def apply_rest_snapshot(
        self,
        *,
        qty: float,
        entry_price: float,
        unrealized_pnl: float,
    ) -> bool:
        """
        REST snapshot (secondary source).
        """
        changed = False

        if self.qty != qty:
            self.qty = qty
            changed = True

        if entry_price and self.entry_price != entry_price:
            self.entry_price = entry_price
            changed = True

        if self.unrealized_pnl != unrealized_pnl:
            self.unrealized_pnl = unrealized_pnl
            changed = True

        return changed

    # ------------------------------------------------------------------
    def to_row(self, *, last_trade_id: str | None = None) -> dict:
        return {
            "exchange_id": self.exchange_id,
            "account_id": self.account_id,
            "symbol_id": self.symbol_id,

            "side": "LONG" if self.qty > 0 else "SHORT" if self.qty < 0 else "FLAT",
            "qty": abs(self.qty),
            "entry_price": self.entry_price,

            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "fees": self.fees,

            "last_ts": datetime.fromtimestamp(self.last_ts_ms / 1000, tz=timezone.utc),
            "last_trade_id": last_trade_id,
            "status": "OPEN" if self.qty != 0 else "CLOSED",
            "updated_at": datetime.now(tz=timezone.utc),
            "source": "position_aggregate",
        }
