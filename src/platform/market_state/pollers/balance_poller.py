from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

from src.platform.exchanges.base.exchange import ExchangeAdapter
from src.platform.data.storage.base import Storage


class BalancePoller(threading.Thread):
    def __init__(
        self,
        *,
        exchange: ExchangeAdapter,
        storage: Storage,
        exchange_id: int,
        account_id: int,
        account: str,
        poll_sec: float = 30.0,
    ):
        super().__init__(
            daemon=True,
            name=f"BalancePoller-{exchange.name}-{account}",
        )
        self.ex = exchange
        self.db = storage
        self.exchange_id = exchange_id
        self.account_id = account_id
        self.account = account
        self.poll_sec = float(poll_sec)
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def run(self):
        while not self._stop.is_set():
            try:
                st = self.ex.fetch_account_state(self.account)

                row = {
                    "exchange_id": self.exchange_id,
                    "account_id": self.account_id,
                    "ts": datetime.now(timezone.utc),
                    "wallet_balance": float(st["wallet_balance"]),
                    "equity": float(st["equity"]),
                    "available_balance": float(st["available_balance"]),
                    "margin_used": float(
                        st["wallet_balance"] - st["available_balance"]
                    ),
                    "unrealized_pnl": float(st["unrealized_pnl"]),
                    "source": "rest",
                }

                self.db.insert_account_balance_snapshots([row])

            except Exception as e:
                print(f"[BalancePoller-{self.account}] error: {e}")

            for _ in range(int(self.poll_sec * 5)):
                if self._stop.is_set():
                    break
                time.sleep(0.2)
