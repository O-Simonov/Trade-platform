from __future__ import annotations
import time
from src.platform.core.strategy.base import Strategy
from src.platform.core.models.order import OrderIntent
from src.platform.core.models.enums import Side


class OneShotTestStrategy(Strategy):
    strategy_id = "one_shot_test"

    def __init__(self, *, exchange: str, account: str, qty: float, hold_sec: float):
        self.exchange = exchange
        self.account = account
        self.qty = float(qty)
        self.hold_sec = float(hold_sec)

        self._open_sent = False
        self._close_sent = False
        self._open_ts: float | None = None

    def on_tick(self, symbol, price, position, pos_uid):
        now = time.time()

        # 1️⃣ OPEN (один раз)
        if not self._open_sent:
            self._open_sent = True
            self._open_ts = now

            print("🔥 ONE-SHOT TEST: OPEN")

            return OrderIntent(
                exchange=self.exchange,
                account=self.account,
                symbol=symbol,
                side=Side.LONG,
                qty=self.qty,
                reduce_only=False,
                intent_type="TEST_OPEN",
                strategy_id=self.strategy_id,
                pos_uid=pos_uid,
            )

        # 2️⃣ CLOSE через hold_sec — НЕ ЗАВИСИМ ОТ position
        if (
            self._open_sent
            and not self._close_sent
            and self._open_ts is not None
            and (now - self._open_ts) >= self.hold_sec
        ):
            self._close_sent = True
            print("🔥 ONE-SHOT TEST: CLOSE")

            return OrderIntent(
                exchange=self.exchange,
                account=self.account,
                symbol=symbol,
                side=Side.SHORT,
                qty=self.qty,
                reduce_only=True,
                intent_type="TEST_CLOSE",
                strategy_id=self.strategy_id,
                pos_uid=pos_uid,
            )

        return None
