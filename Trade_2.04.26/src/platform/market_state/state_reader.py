# src/platform/market_state/state_reader.py
from __future__ import annotations
from typing import Optional, TypedDict
from src.platform.data.storage.base import Storage


class BalanceSnapshot(TypedDict):
    ts: object
    wallet_balance: float
    equity: float
    available_balance: float
    margin_used: float
    unrealized_pnl: float


def latest_balance_snapshot(
    storage: Storage,
    exchange_id: int,
    account_id: int,
) -> Optional[BalanceSnapshot]:
    row = storage.get_latest_account_balance_snapshot(exchange_id, account_id)
    if not row:
        return None

    return {
        "ts": row["ts"],
        "wallet_balance": float(row["wallet_balance"]),
        "equity": float(row["equity"]),
        "available_balance": float(row["available_balance"]),
        "margin_used": float(row["margin_used"]),
        "unrealized_pnl": float(row["unrealized_pnl"]),
    }
