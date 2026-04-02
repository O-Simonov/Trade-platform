# src/platform/core/strategy/one_shot_test.py
from __future__ import annotations

import time
import logging
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
    One-shot test strategy (STEP1 - make it always close):

    • sends OPEN once on first tick
    • starts timer immediately after OPEN is sent
    • sends CLOSE after hold_sec (reduce_only=True)
    """

    strategy_id = "one_shot_test"

    def __init__(self, *, exchange: str, account: str, qty: float, hold_sec: float):
        super().__init__(exchange=exchange, account=account)

        self.qty = float(qty)
        self.hold_sec = float(hold_sec)

        self._symbol: Optional[str] = None
        self._pos_uid: Optional[str] = None

        self._open_sent = False
        self._opened_ts: Optional[float] = None
        self._close_sent = False

        self._pending: List[OrderIntent] = []

        self.logger = logging.getLogger("src.platform.core.strategy.one_shot_test")

    # ------------------------------------------------------------------

    def on_tick(self, *, symbol: str, price: float) -> None:
        now = time.time()

        # --- OPEN once ---
        if not self._open_sent:
            self._open_sent = True
            self._symbol = symbol
            self._pos_uid = f"oneshot:{symbol}:{int(now)}"
            self._opened_ts = now  # start timer immediately

            self.logger.info("[ONE-SHOT] SEND OPEN: %s LONG qty=%.6f", symbol, self.qty)

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

        # --- CLOSE once after hold_sec ---
        if (
            not self._close_sent
            and self._opened_ts is not None
            and now - self._opened_ts >= self.hold_sec
        ):
            self._close_sent = True

            self.logger.info(
                "[ONE-SHOT] SEND CLOSE: %s reduce-only qty=%.6f (held %.1f sec)",
                self._symbol,
                self.qty,
                now - self._opened_ts,
            )

            self._pending.append(
                OrderIntent(
                    symbol=self._symbol,
                    side=None,  # ← ЯВНО
                    qty=self.qty,
                    order_type=OrderType.MARKET,
                    reduce_only=True,  # ← защита от reverse
                    exchange=self.exchange,
                    account=self.account,
                    intent_type=OrderIntentType.CLOSE,
                    pos_uid=self._pos_uid,
                    comment="ONE-SHOT CLOSE",
                )
            )
    # ------------------------------------------------------------------

    def get_intents(self) -> List[OrderIntent]:
        if not self._pending:
            return []
        out = self._pending
        self._pending = []
        return out

    # ------------------------------------------------------------------

    def on_positions_update(self, *, positions):
        # optional debug: keep it, but do not gate close on it at STEP1
        self.logger.info("[STRAT] positions_update count=%s", len(positions) if positions else 0)
