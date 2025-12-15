from __future__ import annotations
import threading, time
from datetime import datetime, timezone
from platform.exchanges.base.exchange import ExchangeAdapter
from platform.data.storage.base import Storage

class BalancePoller(threading.Thread):
    def __init__(self, *, exchange: ExchangeAdapter, storage: Storage,
                 exchange_id: int, account_id: int, account: str,
                 poll_sec: float = 30.0, on_update=None):
        super().__init__(daemon=True, name=f"BalancePoller-{exchange.name}-{account}")
        self.ex = exchange
        self.db = storage
        self.exchange_id = exchange_id
        self.account_id = account_id
        self.account = account
        self.poll_sec = float(poll_sec)
        self.on_update = on_update
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def run(self):
        while not self._stop.is_set():
            try:
                st = self.ex.fetch_account_state(self.account)
                now = datetime.now(timezone.utc)
                row = {
                    "exchange_id": self.exchange_id,
                    "account_id": self.account_id,
                    "ts": now,
                    "wallet_balance": float(st.get("wallet_balance") or 0.0),
                    "equity": float(st.get("equity") or 0.0),
                    "available_balance": float(st.get("available_balance") or 0.0),
                    "margin_used": float(st.get("margin_used") or 0.0),
                    "unrealized_pnl": float(st.get("unrealized_pnl") or 0.0),
                    "source": "rest",
                }
                self.db.insert_account_balance_snapshots([row])
                if self.on_update:
                    self.on_update(row)
            except Exception as e:
                print(f"[BalancePoller-{self.account}] error: {e}")
            # sleep with small granularity so stop() is responsive
            for _ in range(int(self.poll_sec*5)):
                if self._stop.is_set(): break
                time.sleep(0.2)
