# src/platform/core/position/position_snapshot.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _utc_dt_from_ms(ts_ms: int) -> datetime:
    ts_ms = int(ts_ms or 0)
    if ts_ms <= 0:
        return datetime.now(tz=timezone.utc)
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


@dataclass(slots=True)
class PositionSnapshot:
    exchange_id: int
    account_id: int
    symbol_id: int

    side: str                 # LONG/SHORT/FLAT
    qty: float                # abs qty
    entry_price: float
    mark_price: float

    position_value: float     # abs(qty) * mark
    unrealized_pnl: float     # (mark-entry) * signed_qty
    realized_pnl: float
    fees: float

    last_ts: datetime         # TIMESTAMPTZ
    updated_at: datetime      # TIMESTAMPTZ
    source: str = "snapshot"

    def to_row(self) -> Dict[str, Any]:
        return {
            "exchange_id": int(self.exchange_id),
            "account_id": int(self.account_id),
            "symbol_id": int(self.symbol_id),

            "side": str(self.side),
            "qty": float(self.qty),
            "entry_price": float(self.entry_price),
            "mark_price": float(self.mark_price),

            "position_value": float(self.position_value),
            "unrealized_pnl": float(self.unrealized_pnl),
            "realized_pnl": float(self.realized_pnl),
            "fees": float(self.fees),

            "last_ts": self.last_ts,
            "updated_at": self.updated_at,
            "source": str(self.source),
        }


def build_snapshot_from_agg(
    *,
    exchange_id: int,
    account_id: int,
    symbol_id: int,
    qty_signed: float,
    avg_price: float,
    realized_pnl: float,
    fees: float,
    last_ts_ms: int,
    mark_price: float,
    source: str = "snapshot",
) -> PositionSnapshot:
    qty_signed = float(qty_signed or 0.0)
    avg_price = float(avg_price or 0.0)
    mark_price = float(mark_price or 0.0)

    if qty_signed > 0:
        side = "LONG"
    elif qty_signed < 0:
        side = "SHORT"
    else:
        side = "FLAT"

    qty_abs = abs(qty_signed)
    position_value = qty_abs * mark_price

    # signed PnL = (mark-entry) * signed_qty
    # for short (signed_qty < 0) this naturally flips sign
    unrealized = (mark_price - avg_price) * qty_signed if qty_signed != 0 else 0.0

    now = datetime.now(tz=timezone.utc)

    return PositionSnapshot(
        exchange_id=int(exchange_id),
        account_id=int(account_id),
        symbol_id=int(symbol_id),

        side=side,
        qty=float(qty_abs),
        entry_price=float(avg_price if qty_signed != 0 else 0.0),
        mark_price=float(mark_price if qty_signed != 0 else 0.0),

        position_value=float(position_value if qty_signed != 0 else 0.0),
        unrealized_pnl=float(unrealized),
        realized_pnl=float(realized_pnl or 0.0),
        fees=float(fees or 0.0),

        last_ts=_utc_dt_from_ms(int(last_ts_ms or 0)),
        updated_at=now,
        source=source,
    )
