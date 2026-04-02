# src/platform/exchanges/binance/oms_rest.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Sequence, Any

from src.platform.exchanges.binance.rest import BinanceFuturesREST


def _utc(ts_ms: int | None) -> datetime | None:
    if not ts_ms:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


class BinanceOmsRestAdapter:
    """
    Binance REST adapter for OMS reconciliation.

    IMPORTANT:
      - Does NOT import anything from OmsReconciler
      - Returns list[dict] compatible with storage.upsert_orders()
      - client_order_id is the ONLY key for merge
    """

    def __init__(
        self,
        *,
        rest: BinanceFuturesREST,
        symbol_id_map: dict[str, int],
    ):
        self.rest = rest
        self.symbol_id_map = symbol_id_map  # {"LTCUSDT": 64, ...}

    # ------------------------------------------------------------------
    # OMS interface
    # ------------------------------------------------------------------

    def fetch_open_orders(self, *, symbol_ids: Sequence[int]) -> list[dict]:
        """
        Fetch open orders from Binance (REST) and normalize to OMS order dicts.
        """
        out: list[dict] = []

        # Binance allows symbol=None → all open orders
        raw = self.rest.open_orders(symbol=None)

        for r in raw or []:
            symbol = r.get("symbol")
            if not symbol:
                continue

            symbol_id = self.symbol_id_map.get(symbol)
            if symbol_id is None:
                continue

            # optional filter by symbol_ids
            if symbol_ids and symbol_id not in symbol_ids:
                continue

            order_id = r.get("orderId")
            client_order_id = r.get("clientOrderId")

            if not order_id:
                continue

            out.append({
                "exchange_id": None,  # будет подставлено storage
                "account_id": None,   # будет подставлено storage
                "order_id": str(order_id),
                "client_order_id": client_order_id,
                "symbol_id": symbol_id,
                "side": r.get("side"),
                "type": r.get("type"),
                "reduce_only": bool(r.get("reduceOnly")),
                "price": float(r.get("price") or 0.0),
                "qty": float(r.get("origQty") or 0.0),
                "filled_qty": float(r.get("executedQty") or 0.0),
                "status": r.get("status"),
                "ts_ms": int(r.get("updateTime") or 0),
                "created_at": _utc(r.get("time")),
                "updated_at": _utc(r.get("updateTime")),
                "source": "rest_reconcile",
                "raw_json": r,
            })

        return out
