# src/platform/exchanges/binance/exchange.py
from __future__ import annotations

import os
import logging
from typing import List, Optional

from src.platform.exchanges.base.exchange import (
    ExchangeAdapter,
    TickCallback,
    UserEventCallback,
    CandleCallback,
)
from src.platform.core.models.order import OrderIntent
from src.platform.core.models.enums import Side
from src.platform.core.models.position import Position

from src.platform.exchanges.binance.rest import BinanceFuturesREST
from src.platform.exchanges.binance.ws import BinanceWS, WS_STREAM_BASE, WS_WS_BASE
from src.platform.exchanges.binance.normalize import (
    norm_position,
    norm_markprice_tick,
    norm_kline_event,
)
from src.platform.exchanges.binance.order_normalizer import normalize_order


class BinanceExchange(ExchangeAdapter):
    """
    Binance USDⓈ-M Futures exchange adapter.
    OMS-safe, REST/WS separated.
    """

    name = "binance"

    def __init__(self):
        self.logger = logging.getLogger("binance.exchange")

        # REST
        self._rest_by_account: dict[str, BinanceFuturesREST] = {}
        self.rest: Optional[BinanceFuturesREST] = None  # exposed for OMS reconcile (optional)

        # WS
        self._tick_ws: dict[str, BinanceWS] = {}
        self._kline_ws: dict[str, BinanceWS] = {}
        self._user_ws: dict[str, BinanceWS] = {}

        # bind context
        self.storage = None
        self.exchange_id: Optional[int] = None
        self.symbol_ids: dict[str, int] = {}

    def bind(self, *, storage, exchange_id: int, symbol_ids: dict[str, int]) -> None:
        self.storage = storage
        self.exchange_id = int(exchange_id)
        self.symbol_ids = dict(symbol_ids or {})

    # ---------------- REST ----------------

    def _rest(self, account: str) -> BinanceFuturesREST:
        if account in self._rest_by_account:
            cli = self._rest_by_account[account]
        else:
            key = os.environ.get(f"BINANCE_{account.upper()}_API_KEY")
            sec = os.environ.get(f"BINANCE_{account.upper()}_API_SECRET")
            if not key or not sec:
                raise RuntimeError(f"Missing Binance credentials for account={account}")

            cli = BinanceFuturesREST(key, sec)
            self._rest_by_account[account] = cli

        if self.rest is None:
            self.rest = cli

        return cli

    # ---------------- subscriptions ----------------

    def subscribe_ticks(self, *, account: str, symbols: list[str], cb: TickCallback) -> None:
        streams = [f"{s.lower()}@markPrice@1s" for s in symbols]
        if not streams:
            return

        url = WS_STREAM_BASE + "/".join(streams)
        self.logger.info("WS TICKS URL = %s", url)

        def on_msg(data: dict):
            res = norm_markprice_tick(data)
            if res:
                sym, px = res
                cb(sym, px)

        ws = BinanceWS(url=url, on_message=on_msg, name=f"BinanceTicks-{account}")
        ws.start()
        self._tick_ws[account] = ws

    def subscribe_user_stream(self, *, account: str, cb: UserEventCallback) -> None:
        rest = self._rest(account)
        resp = rest.create_listen_key()
        lk = resp.get("listenKey")
        if not lk:
            raise RuntimeError("listenKey error")

        url = f"{WS_WS_BASE}/{lk}"
        ws = BinanceWS(url=url, on_message=cb, name=f"BinanceUser-{account}")
        ws.start()
        self._user_ws[account] = ws

    def subscribe_candles(
        self,
        *,
        account: str,
        symbols: List[str],
        intervals: List[str],
        cb: CandleCallback,
    ) -> None:
        streams = [f"{s.lower()}@kline_{itv}" for s in symbols for itv in intervals]
        if not streams:
            return

        url = WS_STREAM_BASE + "/" + "/".join(streams)
        self.logger.info("WS CANDLES URL = %s", url)

        def on_msg(data: dict):
            row = norm_kline_event(data)
            if row:
                cb(row)

        ws = BinanceWS(url=url, on_message=on_msg, name=f"BinanceKlines-{account}")
        ws.start()
        self._kline_ws[account] = ws

    # ---------------- trading ----------------

    def place_order(self, intent: OrderIntent) -> dict:
        """
        Submit order to Binance.
        IMPORTANT: signature must match ExchangeAdapter.place_order(self, intent)
        """
        if not self.storage or self.exchange_id is None:
            raise RuntimeError("BinanceExchange not bound")

        rest = self._rest(intent.account)

        # symbol_id
        if intent.symbol not in self.symbol_ids:
            raise KeyError(f"symbol_id not found for symbol={intent.symbol}")

        symbol_id = int(self.symbol_ids[intent.symbol])
        side = "BUY" if intent.side == Side.LONG else "SELL"

        # filters
        filters = self.storage.get_symbol_filters(
            exchange_id=self.exchange_id,
            symbol_id=symbol_id,
        )

        qty, price = normalize_order(
            qty=float(intent.qty),
            price=float(intent.price) if intent.price is not None else None,
            qty_step=float(filters["qty_step"]),
            min_qty=float(filters["min_qty"]) if filters.get("min_qty") else None,
            max_qty=float(filters["max_qty"]) if filters.get("max_qty") else None,
            price_tick=float(filters["price_tick"]) if filters.get("price_tick") else None,
            min_notional=float(filters["min_notional"]) if filters.get("min_notional") else None,
        )

        if qty is None or qty <= 0:
            raise ValueError("Normalized qty <= 0")

        params: dict[str, str] = {
            "symbol": intent.symbol,
            "side": side,
            "type": intent.order_type.value,
            "quantity": str(qty),
        }

        if intent.reduce_only:
            params["reduceOnly"] = "true"

        # OMS idempotency (always via intent.client_order_id)
        if getattr(intent, "client_order_id", None):
            params["newClientOrderId"] = str(intent.client_order_id)

        if price is not None:
            params["price"] = str(price)
            params["timeInForce"] = "GTC"

        self.logger.info(
            "[ORDER SUBMIT] %s %s qty=%s price=%s reduce_only=%s cid=%s",
            intent.symbol, side, qty, price, intent.reduce_only, getattr(intent, "client_order_id", None),
        )

        return rest.new_order(**params)

    # ---------------- state ----------------

    def fetch_positions(self, *, account: str) -> list[Position]:
        raw = self._rest(account).position_risk()
        out: list[Position] = []
        for r in raw:
            p = norm_position(self.name, account, r)
            if p:
                out.append(p)
        return out

    def fetch_account_state(self, *, account: str) -> dict:
        raw = self._rest(account).account()

        wallet = float(raw.get("totalWalletBalance") or 0.0)
        unreal = float(raw.get("totalUnrealizedProfit") or 0.0)
        equity = float(raw.get("totalMarginBalance") or (wallet + unreal))
        available = float(raw.get("availableBalance") or 0.0)

        return {
            "wallet_balance": wallet,
            "equity": equity,
            "available_balance": available,
            "unrealized_pnl": unreal,
        }
