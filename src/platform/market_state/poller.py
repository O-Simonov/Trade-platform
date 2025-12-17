# src/platform/market_state/poller.py
from __future__ import annotations
import time
import threading
import logging

from src.platform.exchanges.binance.exchange import BinanceExchange
from src.platform.market_state.models import AccountState, PositionState

logger = logging.getLogger(__name__)


class BinanceAccountPoller(threading.Thread):
    """
    ONE poller per (exchange, account)
    """

    def __init__(
        self,
        *,
        exchange: BinanceExchange,
        account: str,
        storage,
        poll_sec: float = 10.0,
    ):
        super().__init__(daemon=True, name=f"MarketState-{exchange.name}-{account}")
        self.exchange = exchange
        self.account = account
        self.storage = storage
        self.poll_sec = poll_sec
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def run(self):
        logger.info("MarketState poller started: %s / %s", self.exchange.name, self.account)

        while not self._stop.is_set():
            try:
                self._poll_once()
            except Exception as e:
                logger.exception("MarketState poll error: %s", e)

            time.sleep(self.poll_sec)

    def _poll_once(self):
        # --- account info ---
        acc = self.exchange.fetch_account(self.account)

        # --- positions ---
        raw_pos = self.exchange.fetch_positions(self.account)

        positions = {}
        for p in raw_pos:
            qty = float(p["positionAmt"])
            if qty == 0:
                continue

            side = "LONG" if qty > 0 else "SHORT"
            positions[p["symbol"]] = PositionState(
                symbol=p["symbol"],
                qty=abs(qty),
                entry_price=float(p["entryPrice"]),
                unrealized_pnl=float(p["unRealizedProfit"]),
                leverage=int(p["leverage"]),
                side=side,
            )

        state = AccountState(
            wallet_balance=float(acc["totalWalletBalance"]),
            available_balance=float(acc["availableBalance"]),
            margin_balance=float(acc["totalMarginBalance"]),
            positions=positions,
            ts_ms=int(time.time() * 1000),
        )

        # 🔑 единая точка записи
        self.storage.upsert_account_state(
            exchange=self.exchange.name,
            account=self.account,
            state=state,
        )
