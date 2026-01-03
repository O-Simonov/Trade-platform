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
from src.platform.core.models.order import OrderIntent, OrderType
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
        self.rest: Optional[BinanceFuturesREST] = None  # exposed (optional)

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

    # ---------------- url helper ----------------

    @staticmethod
    def _stream_url(streams: list[str]) -> str:
        base = str(WS_STREAM_BASE)
        if base.endswith("="):
            return base + "/".join(streams)
        return base.rstrip("/") + "/" + "/".join(streams)

    # ---------------- subscriptions ----------------

    def subscribe_ticks(self, *, account: str, symbols: list[str], cb: TickCallback) -> None:
        streams = [f"{s.lower()}@markPrice@1s" for s in symbols]
        if not streams:
            return

        url = self._stream_url(streams)
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
        # NOTE: listenKey is REST and may fail on bans/rate limits.
        rest = self._rest(account)
        try:
            resp = rest.create_listen_key()
        except Exception as e:
            self.logger.error("[BinanceUser] create_listen_key failed: %s", e)
            raise

        lk = (resp or {}).get("listenKey")
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

        url = self._stream_url(streams)
        self.logger.info("WS CANDLES URL = %s", url)

        def on_msg(data: dict):
            row = norm_kline_event(data)
            if row:
                cb(row)

        ws = BinanceWS(url=url, on_message=on_msg, name=f"BinanceKlines-{account}")
        ws.start()
        self._kline_ws[account] = ws

    # ---------------- trading ----------------

    def _resolve_symbol_id_from_intent(self, intent: OrderIntent) -> int:
        """
        K6.8: OMS may set intent.symbol_id.
        If not — take from registry self.symbol_ids.
        """
        sid = int(getattr(intent, "symbol_id", 0) or 0)
        if sid > 0:
            return sid

        sym = (intent.symbol or "").upper()
        if sym in self.symbol_ids:
            return int(self.symbol_ids[sym])

        raise KeyError(f"symbol_id not found for symbol={intent.symbol}")

    def _map_side(self, intent: OrderIntent) -> str:
        if intent.side == Side.LONG:
            return "BUY"
        if intent.side == Side.SHORT:
            return "SELL"
        raise ValueError(f"Unsupported intent.side={intent.side}")

    def place_order(self, intent: OrderIntent) -> dict:
        """
        Submit order to Binance Futures (UM).

        K6.8:
          - supports reduceOnly
          - keeps client_order_id for idempotency
          - normalizes qty/price using symbol filters
        """
        if not self.storage or self.exchange_id is None:
            raise RuntimeError("BinanceExchange not bound")

        if not intent.account:
            raise ValueError("OrderIntent.account is empty (required for BinanceExchange)")

        rest = self._rest(intent.account)

        symbol = (intent.symbol or "").upper()
        symbol_id = self._resolve_symbol_id_from_intent(intent)
        side = self._map_side(intent)

        # filters from DB
        filters = self.storage.get_symbol_filters(
            exchange_id=self.exchange_id,
            symbol_id=symbol_id,
        )
        if not filters:
            raise RuntimeError(f"symbol_filters not found: exchange_id={self.exchange_id} symbol_id={symbol_id} symbol={symbol}")

        qty, price = normalize_order(
            qty=float(intent.qty),
            price=float(intent.price) if intent.price is not None else None,
            qty_step=float(filters["qty_step"] or 0.0),
            min_qty=float(filters["min_qty"] or 0.0),
            max_qty=float(filters["max_qty"]) if filters.get("max_qty") is not None else None,
            price_tick=float(filters["price_tick"]) if filters.get("price_tick") is not None else None,
            min_notional=float(filters["min_notional"]) if filters.get("min_notional") is not None else None,
        )

        if qty <= 0:
            raise ValueError("Normalized qty <= 0")

        params: dict[str, str] = {
            "symbol": symbol,
            "side": side,
            "type": intent.order_type.value,
            "quantity": str(qty),
        }

        # reduceOnly
        if bool(intent.reduce_only):
            params["reduceOnly"] = "true"

        # idempotency
        if getattr(intent, "client_order_id", None):
            params["newClientOrderId"] = str(intent.client_order_id)

        # LIMIT price/TIF
        if intent.order_type == OrderType.LIMIT:
            if price is None or price <= 0:
                raise ValueError("LIMIT order requires price")
            params["price"] = str(price)
            params["timeInForce"] = "GTC"

        # Optional: positionSide (hedge mode)
        pos_side = os.environ.get("BINANCE_POSITION_SIDE", "").strip().upper()
        if pos_side in ("LONG", "SHORT", "BOTH"):
            params["positionSide"] = pos_side

        self.logger.info(
            "[ORDER SUBMIT] %s %s qty=%s price=%s reduce_only=%s cid=%s",
            symbol, side, qty, price, bool(intent.reduce_only), getattr(intent, "client_order_id", None),
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
