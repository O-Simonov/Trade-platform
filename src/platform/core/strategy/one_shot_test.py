# src/platform/core/strategy/one_shot_test.py
from __future__ import annotations

import time
from src.platform.core.strategy.base import Strategy
from src.platform.core.models.order import OrderIntent
from src.platform.core.models.enums import Side


class OneShotTestStrategy(Strategy):
    """
    TEST STRATEGY:
      - открывает 1 ордер
      - ждёт hold_sec
      - закрывает позицию reduce_only
      - больше НИЧЕГО не делает
    """

    strategy_id = "one_shot_test"

    def __init__(
        self,
        *,
        exchange: str,
        account: str,
        qty: float,
        hold_sec: float = 10.0,
    ):
        self.exchange = exchange
        self.account = account
        self.qty = float(qty)
        self.hold_sec = float(hold_sec)

        self._opened = False
        self._closed = False
        self._open_ts: float | None = None

    def on_tick(self, symbol, price, position, pos_uid):
        now = time.time()

        # ------------------------------------------------------------------
        # 1️⃣ OPEN — строго один раз
        # ------------------------------------------------------------------
        if not self._opened:
            self._opened = True
            self._open_ts = now

            print("🔥 ONE-SHOT TEST: OPEN")

            return OrderIntent(
                exchange=self.exchange,
                account=self.account,
                symbol=symbol,
                side=Side.LONG,
                qty=self.qty,
                intent_type="TEST_OPEN",
                strategy_id=self.strategy_id,
                pos_uid=pos_uid,
            )

        # ------------------------------------------------------------------
        # 2️⃣ WAIT UNTIL POSITION EXISTS
        # ------------------------------------------------------------------
        if not position or position.qty == 0:
            return None

        # ------------------------------------------------------------------
        # 3️⃣ CLOSE AFTER hold_sec
        # ------------------------------------------------------------------
        if (
            self._opened
            and not self._closed
            and self._open_ts
            and now - self._open_ts >= self.hold_sec
        ):
            self._closed = True

            close_side = Side.SHORT if position.side == Side.LONG else Side.LONG

            print("🔥 ONE-SHOT TEST: CLOSE")

            return OrderIntent(
                exchange=self.exchange,
                account=self.account,
                symbol=symbol,
                side=close_side,
                qty=abs(position.qty),   # 🔥 ВАЖНО
                reduce_only=True,
                intent_type="TEST_CLOSE",
                strategy_id=self.strategy_id,
                pos_uid=pos_uid,
            )

        return None
