from __future__ import annotations
from typing import Optional
from src.platform.data.storage.base import Storage
from src.platform.market_state.models import AccountBalanceSnapshot

def latest_balance_snapshot(storage: Storage, exchange_id: int, account_id: int) -> Optional[AccountBalanceSnapshot]:
    row = storage.get_latest_account_balance_snapshot(exchange_id, account_id)
    if not row:
        return None
    return AccountBalanceSnapshot(
        ts=row["ts"],
        wallet_balance=float(row["wallet_balance"]),
        equity=float(row["equity"]),
        available_balance=float(row["available_balance"]),
        margin_used=float(row["margin_used"]),
        unrealized_pnl=float(row["unrealized_pnl"]),
    )
