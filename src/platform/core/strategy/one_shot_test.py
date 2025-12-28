# src/platform/core/strategy/one_shot_test.py
from __future__ import annotations

import time
from typing import List, Optional

from src.platform.core.strategy.base import Strategy
from src.platform.core.models.order import (
    OrderIntent,
    OrderIntentType,
    OrderType,
)
from src.platform.core.models.enums import Side


class OneShotTestStrategy(Strategy):
    """
    Simple test strategy:
      • opens position on first tick
      • closes after hold_sec
    """

    strategy_id = "one_shot_test"

    def __init__(
        self,
        *,
        exchange: str,
        account: str,
        qty: float,
        hold_sec: float,
    ):
        super().__init__(exchange=exchange, account=account)

        self.qty = float(qty)
        self.hold_sec = float(hold_sec)

        self._opened_ts: Optional[float] = None
        self._symbol: Optional[str] = None
        self._pos_uid: Optional[str] = None

        self._pending: List[OrderIntent] = []

    # ------------------------------------------------------------------

    def on_tick(self, *, symbol: str, price: float) -> None:
        now = time.time()

        # --- OPEN ---
        if self._opened_ts is None:
            self._opened_ts = now
            self._symbol = symbol
            self._pos_uid = f"oneshot:{symbol}:{int(now)}"

            self._pending.append(
                OrderIntent(
                    symbol=symbol,
                    side=Side.LONG,
                    qty=self.qty,
                    order_type=OrderType.MARKET,
                    reduce_only=False,
                    exchange=self.exchange,
                    account=self.account,
                    intent_type=OrderIntentType.OPEN,
                    pos_uid=self._pos_uid,
                    comment="ONE-SHOT OPEN",
                )
            )
            return

        # --- CLOSE ---
        if now - self._opened_ts >= self.hold_sec:
            self._pending.append(
                OrderIntent(
                    symbol=self._symbol,
                    side=Side.SHORT,
                    qty=self.qty,
                    order_type=OrderType.MARKET,
                    reduce_only=True,
                    exchange=self.exchange,
                    account=self.account,
                    intent_type=OrderIntentType.CLOSE,
                    pos_uid=self._pos_uid,
                    comment="ONE-SHOT CLOSE",
                )
            )

            # prevent повторов
            self._opened_ts = float("inf")

    # ------------------------------------------------------------------

    def get_intents(self) -> List[OrderIntent]:
        if not self._pending:
            return []

        out = self._pending
        self._pending = []
        return out
