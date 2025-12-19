from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

from src.platform.exchanges.base.exchange import ExchangeAdapter
from src.platform.data.storage.base import Storage


class OpenInterestPoller(threading.Thread):
    def __init__(
        self,
        *,
        exchange: ExchangeAdapter,
        storage: Storage,
        exchange_id: int,
        account: str,
        symbol_ids: dict[str, int],
        intervals: list[str] | None = None,
        poll_sec: float = 300.0,
        limit: int = 30,
    ):
        super().__init__(
            daemon=True,
            name=f"OIPoller-{exchange.name}-{account}",
        )
        self.ex = exchange
        self.db = storage
        self.exchange_id = exchange_id
        self.account = account
        self.symbol_ids = symbol_ids
        self.intervals = intervals or ["5m", "15m", "1h"]
        self.poll_sec = float(poll_sec)
        self.limit = int(limit)
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def run(self):
        while not self._stop.is_set():
            try:
                rows: list[dict] = []

                for symbol, symbol_id in self.symbol_ids.items():
                    for interval in self.intervals:
                        items = self.ex.fetch_open_interest_hist(
                            self.account,
                            symbol=symbol,
                            interval=interval,
                            limit=self.limit,
                        )

                        for r in items or []:
                            ts = r.get("ts")
                            if not ts:
                                continue

                            if isinstance(ts, (int, float)):
                                ts = datetime.fromtimestamp(ts, tz=timezone.utc)
                            elif isinstance(ts, datetime) and ts.tzinfo is None:
                                ts = ts.replace(tzinfo=timezone.utc)

                            rows.append({
                                "exchange_id": self.exchange_id,
                                "symbol_id": symbol_id,
                                "interval": interval,
                                "ts": ts,
                                "open_interest": float(
                                    r.get("open_interest") or 0.0
                                ),
                                "open_interest_value": float(
                                    r.get("open_interest_value") or 0.0
                                ),
                                "source": "rest",
                            })

                if rows:
                    self.db.upsert_open_interest(rows)

            except Exception as e:
                print(f"[OpenInterestPoller-{self.account}] error: {e}")

            # granular sleep (responsive stop)
            for _ in range(int(self.poll_sec * 5)):
                if self._stop.is_set():
                    break
                time.sleep(0.2)
