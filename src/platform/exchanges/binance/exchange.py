# src/platform/exchanges/binance/exchange.py
from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone
from typing import List, Optional, Any
import logging

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
from src.platform.exchanges.binance.normalize import (
    norm_position,
    norm_markprice_tick,
    norm_kline_event,
    norm_premium_index_item,
    norm_user_event,
)
from src.platform.exchanges.binance.order_normalizer import normalize_order
from src.platform.exchanges.binance.ws import BinanceWS, WS_STREAM_BASE, WS_WS_BASE




class BinanceExchange(ExchangeAdapter):
    """
    Binance USDⓈ-M Futures exchange adapter (v9)
    """
    name = "binance"

    def __init__(self):


        self._dual_side_by_account: dict[str, bool] = {}

        self._log = logging.getLogger("binance.exchange")
        self._rest_by_account: dict[str, BinanceFuturesREST] = {}
        self._public_rest: Optional[BinanceFuturesREST] = None

        self._tick_ws: dict[str, BinanceWS] = {}
        self._kline_ws: dict[str, BinanceWS] = {}
        self._user_ws: dict[str, BinanceWS] = {}

        self._listen_key: dict[str, str] = {}
        self._keepalive_threads: dict[str, threading.Thread] = {}

        # Bind from TradingInstance
        self.storage = None
        self.exchange_id: int | None = None
        self.symbol_ids: dict[str, int] = {}
        self.account_ids: dict[str, int] = {}  # account -> account_id
        self.oms_by_account: dict[str, Any] = {}  # account -> OrderManager (опционально)

    # ------------------------------------------------------------------
    # BIND CONTEXT (called by TradingInstance)
    # ------------------------------------------------------------------
    def bind(self, *, storage, exchange_id: int, symbol_ids: dict[str, int]) -> None:
        self.storage = storage
        self.exchange_id = int(exchange_id)
        self.symbol_ids = dict(symbol_ids or {})

    # ------------------------------------------------------------------
    # REST CLIENTS
    # ------------------------------------------------------------------
    def _rest(self, account: str) -> BinanceFuturesREST:
        if account in self._rest_by_account:
            return self._rest_by_account[account]

        key_var = f"BINANCE_{account.upper()}_API_KEY"
        sec_var = f"BINANCE_{account.upper()}_API_SECRET"

        api_key = os.environ.get(key_var)
        api_sec = os.environ.get(sec_var)

        if not api_key or not api_sec:
            raise RuntimeError(
                f"Missing Binance credentials for account={account}: {key_var}/{sec_var}"
            )

        cli = BinanceFuturesREST(api_key, api_sec)
        self._rest_by_account[account] = cli
        return cli

    def _rest_public(self) -> BinanceFuturesREST:
        """
        Public endpoints (signed=False) don’t require real keys.
        """
        if self._public_rest is None:
            self._public_rest = BinanceFuturesREST(api_key="", api_secret="")
        return self._public_rest

    # ------------------------------------------------------------------
    # WEBSOCKETS
    # ------------------------------------------------------------------
    def subscribe_ticks(self, account: str, symbols: list[str], cb):
        streams = [f"{s.lower()}@markPrice@1s" for s in symbols]
        if not streams:
            return

        url = WS_STREAM_BASE + "/".join(streams)
        print("WS TICKS URL =", url)

        def on_msg(data: dict):
            res = norm_markprice_tick(data)
            if res:
                sym, px = res
                cb(sym, px)

        ws = BinanceWS(url=url, on_message=on_msg, name=f"BinanceTicks-{account}")
        ws.start()
        self._tick_ws[account] = ws

    def subscribe_user_stream(self, account: str, cb):
        rest = self._rest(account)

        lk = self._listen_key.get(account)
        if not lk:
            resp = rest.create_listen_key()
            lk = resp.get("listenKey")
            if not lk:
                raise RuntimeError("listenKey error")
            self._listen_key[account] = lk

        url = f"{WS_WS_BASE}/{lk}"
        ws = BinanceWS(url=url, on_message=cb, name=f"BinanceUser-{account}")
        ws.start()
        self._user_ws[account] = ws

    def subscribe_candles(
        self,
        account: str,
        symbols: List[str],
        intervals: List[str],
        cb: CandleCallback,
    ) -> None:
        streams = [f"{s.lower()}@kline_{itv}" for s in symbols for itv in intervals]
        if not streams:
            return

        url = WS_STREAM_BASE + "/" + "/".join(streams)

        self._log.info("[WS] subscribe_candles %s intervals=%s", symbols, intervals)
        self._log.info("WS CANDLES URL = %s", url)

        def on_msg(data: dict):
            row = norm_kline_event(data)
            if row:
                cb(row)

        ws = BinanceWS(url=url, on_message=on_msg, name=f"BinanceKlines-{account}")
        ws.start()
        self._kline_ws[account] = ws

    # ------------------------------------------------------------------
    # FETCH
    # ------------------------------------------------------------------
    def fetch_positions(self, account: str) -> list[Position]:
        raw = self._rest(account).position_risk()
        out: list[Position] = []
        for r in raw:
            p = norm_position(self.name, account, r)
            if p:
                out.append(p)
        return out

    def fetch_open_orders(self, account: str, symbol: str | None = None) -> list[dict]:
        return self._rest(account).open_orders(symbol=symbol)

    def fetch_funding_snapshot(self, symbols: list[str]) -> list[dict]:
        """
        Funding snapshot (premiumIndex).
        Account не нужен — берём любой активный REST.
        """
        out: list[dict] = []

        if not self._rest_by_account:
            return out  # REST ещё не инициализирован

        rest = next(iter(self._rest_by_account.values()))

        for s in symbols:
            try:
                item = rest.premium_index(symbol=s)
                row = norm_premium_index_item(item)
                if row:
                    out.append(row)
            except Exception as e:
                print(f"[Funding] premiumIndex {s} error: {e}")

        return out

    # ------------------------------------------------------------------
    # ORDER PLACEMENT (D2 — NORMALIZED)
    # ------------------------------------------------------------------
    def place_order(self, intent: OrderIntent) -> dict:
        if not self.storage or self.exchange_id is None:
            raise RuntimeError("BinanceExchange is not bound: storage/exchange_id missing. Call ex.bind(...)")

        if intent.symbol not in self.symbol_ids:
            raise KeyError(f"symbol_id not found for {intent.symbol}. Did you bind symbol_ids correctly?")

        rest = self._rest(intent.account)
        side = "BUY" if intent.side == Side.LONG else "SELL"

        filters = self.storage.get_symbol_filters(
            exchange_id=self.exchange_id,
            symbol_id=self.symbol_ids[intent.symbol],
        )

        qty, price = normalize_order(
            qty=float(intent.qty),
            price=(float(intent.price) if intent.price is not None else None),
            qty_step=float(filters["qty_step"]),
            min_qty=float(filters["min_qty"]),
            max_qty=(float(filters["max_qty"]) if filters.get("max_qty") is not None else None),
            price_tick=(float(filters["price_tick"]) if filters.get("price_tick") is not None else None),
            min_notional=(float(filters["min_notional"]) if filters.get("min_notional") is not None else None),
        )

        if qty <= 0:
            raise ValueError(f"Normalized qty <= 0 for {intent.symbol}")

        params: dict = {
            "symbol": intent.symbol,
            "side": side,
            "type": intent.order_type.value,
            "quantity": str(qty),
        }

        if intent.reduce_only:
            params["reduceOnly"] = "true"

        if intent.client_order_id:
            params["newClientOrderId"] = intent.client_order_id

        if price is not None:
            params["price"] = str(price)
            params["timeInForce"] = "GTC"

        return rest.new_order(**params)

    # ------------------------------------------------------------------
    # ACCOUNT / OI
    # ------------------------------------------------------------------
    def fetch_account_state(self, account: str) -> dict:
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

    def fetch_open_interest_hist(
        self,
        account: str,
        *,
        symbol: str,
        interval: str,
        limit: int = 30,
    ) -> list[dict]:
        raw = self._rest(account).open_interest_hist(symbol=symbol, period=interval, limit=int(limit))

        out: list[dict] = []
        for r in raw or []:
            ts_ms = int(r.get("timestamp") or 0)
            if ts_ms <= 0:
                continue
            out.append(
                {
                    "ts": datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc),
                    "open_interest": float(r.get("sumOpenInterest") or r.get("openInterest") or 0.0),
                    "open_interest_value": float(r.get("sumOpenInterestValue") or r.get("openInterestValue") or 0.0),
                }
            )
        return out
