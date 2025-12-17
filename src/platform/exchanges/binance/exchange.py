from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone
from typing import List

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
from src.platform.exchanges.binance.ws import BinanceWS, WS_BASE
from src.platform.exchanges.binance.normalize import (
    norm_position,
    norm_markprice_tick,
    norm_kline_event,
    norm_premium_index_item,
)
from src.platform.exchanges.binance.order_normalizer import normalize_order


class BinanceExchange(ExchangeAdapter):
    """
    Binance USDⓈ-M Futures exchange adapter (v9)
    """
    name = "binance"

    def __init__(self):
        self._rest_by_account: dict[str, BinanceFuturesREST] = {}
        self._tick_ws: dict[str, BinanceWS] = {}
        self._kline_ws: dict[str, BinanceWS] = {}
        self._user_ws: dict[str, BinanceWS] = {}
        self._listen_key: dict[str, str] = {}
        self._keepalive_threads: dict[str, threading.Thread] = {}

        # Эти поля устанавливаются TradingInstance
        self.storage = None
        self.exchange_id: int | None = None
        self.symbol_ids: dict[str, int] = {}

    # ======================================================================
    # REST
    # ======================================================================

    def _rest(self, account: str) -> BinanceFuturesREST:
        if account in self._rest_by_account:
            return self._rest_by_account[account]

        key_var = f"BINANCE_{account.upper()}_API_KEY"
        sec_var = f"BINANCE_{account.upper()}_API_SECRET"

        api_key = os.environ.get(key_var)
        api_sec = os.environ.get(sec_var)

        if not api_key or not api_sec:
            raise RuntimeError(
                f"Missing Binance credentials for account={account}: "
                f"{key_var}/{sec_var}"
            )

        cli = BinanceFuturesREST(api_key, api_sec)
        self._rest_by_account[account] = cli
        return cli

    # ======================================================================
    # WEBSOCKETS
    # ======================================================================

    def subscribe_ticks(
        self,
        account: str,
        symbols: List[str],
        cb: TickCallback,
    ) -> None:
        streams = [f"{s.lower()}@markPrice@1s" for s in symbols]
        url = WS_BASE + "/" + "/".join(streams)

        def on_msg(data: dict):
            res = norm_markprice_tick(data)
            if res:
                sym, px = res
                cb(sym, px)

        ws = BinanceWS(url=url, on_message=on_msg, name=f"BinanceTicks-{account}")
        ws.start()
        self._tick_ws[account] = ws

    def subscribe_candles(
        self,
        account: str,
        symbols: List[str],
        intervals: List[str],
        cb: CandleCallback,
    ) -> None:
        streams = [
            f"{s.lower()}@kline_{itv}"
            for s in symbols
            for itv in intervals
        ]
        if not streams:
            return

        url = WS_BASE + "/" + "/".join(streams)

        def on_msg(data: dict):
            row = norm_kline_event(data)
            if row:
                cb(row)

        ws = BinanceWS(url=url, on_message=on_msg, name=f"BinanceKlines-{account}")
        ws.start()
        self._kline_ws[account] = ws

    def subscribe_user_stream(
        self,
        account: str,
        cb: UserEventCallback,
    ) -> None:
        rest = self._rest(account)

        listen_key = self._listen_key.get(account)
        if not listen_key:
            resp = rest.create_listen_key()
            listen_key = resp.get("listenKey")
            if not listen_key:
                raise RuntimeError(f"Failed to create listenKey: {resp}")
            self._listen_key[account] = listen_key

        url = WS_BASE + "/" + listen_key
        ws = BinanceWS(url=url, on_message=cb, name=f"BinanceUser-{account}")
        ws.start()
        self._user_ws[account] = ws

        if account not in self._keepalive_threads:
            def keepalive():
                while True:
                    try:
                        rest.keepalive_listen_key(listen_key)
                    except Exception as e:
                        print(f"[BinanceUser-{account}] keepalive error: {e}")
                    time.sleep(30 * 60)

            t = threading.Thread(
                target=keepalive,
                daemon=True,
                name=f"BinanceListenKeyKeepAlive-{account}",
            )
            t.start()
            self._keepalive_threads[account] = t

    # ======================================================================
    # FETCH
    # ======================================================================

    def fetch_positions(self, account: str) -> list[Position]:
        raw = self._rest(account).position_risk()
        out: list[Position] = []

        for r in raw:
            p = norm_position(self.name, account, r)
            if p:
                out.append(p)

        return out

    def fetch_open_orders(
        self,
        account: str,
        symbol: str | None = None,
    ) -> list[dict]:
        return self._rest(account).open_orders(symbol=symbol)

    # ======================================================================
    # ORDER PLACEMENT (D2 — NORMALIZED)
    # ======================================================================

    def place_order(self, intent: OrderIntent) -> dict:
        """
        Единственная точка отправки ордера в Binance.
        Нормализация строго по symbol_filters из БД.
        """
        if not self.storage or self.exchange_id is None:
            raise RuntimeError("Exchange is not bound to storage / exchange_id")

        rest = self._rest(intent.account)

        side = "BUY" if intent.side == Side.LONG else "SELL"

        # --- load filters from DB ---
        filters = self.storage.get_symbol_filters(
            exchange_id=self.exchange_id,
            symbol_id=self.symbol_ids[intent.symbol],
        )

        qty, price = normalize_order(
            qty=intent.qty,
            price=intent.price,
            qty_step=filters["qty_step"],
            min_qty=filters["min_qty"],
            max_qty=filters.get("max_qty"),
            price_tick=filters.get("price_tick"),
            min_notional=filters.get("min_notional"),
        )

        if qty <= 0:
            raise ValueError(f"Normalized qty <= 0 for {intent.symbol}")

        params = {
            "symbol": intent.symbol,
            "side": side,
            "type": intent.order_type.value,
            "quantity": str(qty),
        }

        if intent.reduce_only:
            params["reduceOnly"] = True

        if intent.client_order_id:
            params["newClientOrderId"] = intent.client_order_id

        if price is not None:
            params["price"] = str(price)
            params["timeInForce"] = "GTC"

        return rest.new_order(**params)

    # ======================================================================
    # ACCOUNT / OI
    # ======================================================================

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
        raw = self._rest(account).open_interest_hist(
            symbol=symbol,
            period=interval,
            limit=int(limit),
        )

        out = []
        for r in raw or []:
            ts_ms = int(r.get("timestamp") or 0)
            if ts_ms <= 0:
                continue

            out.append({
                "ts": datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc),
                "open_interest": float(
                    r.get("sumOpenInterest") or r.get("openInterest") or 0.0
                ),
                "open_interest_value": float(
                    r.get("sumOpenInterestValue") or r.get("openInterestValue") or 0.0
                ),
            })

        return out
