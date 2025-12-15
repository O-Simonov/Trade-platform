from __future__ import annotations
from platform.core.strategy.base import Strategy
from platform.core.models.order import OrderIntent
from platform.core.models.enums import Side

class ExampleNoHedge(Strategy):
    strategy_id = "example_nohedge"
    def __init__(self, exchange: str, account: str, qty: float, tp_usdt: float):
        self.exchange = exchange
        self.account = account
        self.qty = float(qty)
        self.tp_usdt = float(tp_usdt)

    def on_tick(self, symbol, price, position, pos_uid):
        if position is None or position.qty == 0:
            return OrderIntent(exchange=self.exchange, account=self.account, symbol=symbol,
                               side=Side.LONG, qty=self.qty, strategy_id=self.strategy_id,
                               pos_uid=pos_uid, intent_type="ENTRY")
        if (position.unrealized_pnl or 0.0) >= self.tp_usdt:
            close_side = Side.SHORT if position.side == Side.LONG else Side.LONG
            return OrderIntent(exchange=self.exchange, account=self.account, symbol=symbol,
                               side=close_side, qty=abs(position.qty), reduce_only=True,
                               strategy_id=self.strategy_id, pos_uid=pos_uid, intent_type="CLOSE")
        return None
