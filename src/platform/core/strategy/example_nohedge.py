from __future__ import annotations
import time
from src.platform.core.strategy.base import Strategy
from src.platform.core.models.order import OrderIntent
from src.platform.core.models.enums import Side


class ExampleNoHedge(Strategy):
    strategy_id = "example_nohedge"

    def __init__(self, exchange: str, account: str, qty: float):
        self.exchange = exchange
        self.account = account
        self.qty = float(qty)

        self._opened_at: float | None = None
        self._done = False

    def on_tick(self, symbol, price, position, pos_uid):
        # 🔒 ТОЛЬКО ОДИН ЦИКЛ
        if self._done:
            return None

        # 1️⃣ ОТКРЫВАЕМ ОДИН РАЗ
        if self._opened_at is None and (position is None or position.qty == 0):
            self._opened_at = time.time()
            return OrderIntent(
                exchange=self.exchange,
                account=self.account,
                symbol=symbol,
                side=Side.LONG,
                qty=self.qty,
                strategy_id=self.strategy_id,
                pos_uid=pos_uid,
                intent_type="ENTRY",
            )

        # 2️⃣ ЗАКРЫВАЕМ ЧЕРЕЗ 10 СЕК
        if (
            position
            and position.qty > 0
            and self._opened_at
            and time.time() - self._opened_at >= 10
        ):
            self._done = True
            return OrderIntent(
                exchange=self.exchange,
                account=self.account,
                symbol=symbol,
                side=Side.SHORT,
                qty=abs(position.qty),
                reduce_only=True,
                strategy_id=self.strategy_id,
                pos_uid=pos_uid,
                intent_type="CLOSE",
            )

        return None
