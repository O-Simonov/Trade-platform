from datetime import datetime, timezone
from typing import Any, Dict, Optional
from .models import Fill

def _dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)

def parse_order_trade_update(msg: Dict[str, Any],
                             exchange="binance",
                             strategy_name=None,
                             pos_uid=None) -> Optional[Fill]:
    if msg.get("e") != "ORDER_TRADE_UPDATE":
        return None
    o = msg.get("o") or {}
    if o.get("x") != "TRADE":
        return None

    side = o["S"]
    ps = o.get("ps", "BOTH")
    position_side = ps if ps in ("LONG","SHORT") else ("LONG" if side=="BUY" else "SHORT")

    price = float(o.get("L", 0))
    qty = float(o.get("l", 0))
    if price <= 0 or qty <= 0:
        return None

    return Fill(
        trade_id=int(o["t"]),
        exchange=exchange,
        symbol=o["s"],
        order_id=int(o["i"]),
        side=side,
        position_side=position_side,
        price=price,
        qty=qty,
        quote_qty=price * qty,
        commission=float(o.get("n") or 0),
        commission_asset=o.get("N"),
        realized_pnl=float(o.get("rp") or 0),
        is_maker=o.get("m"),
        executed_at=_dt(o["T"]),
        strategy_name=strategy_name,
        pos_uid=pos_uid,
    )
