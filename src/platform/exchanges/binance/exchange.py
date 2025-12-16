from __future__ import annotations
import os, threading, time
from datetime import datetime, timezone
from src.platform.exchanges.base.exchange import ExchangeAdapter, TickCallback, UserEventCallback, CandleCallback
from src.platform.core.models.order import OrderIntent
from src.platform.core.models.enums import Side
from src.platform.core.models.position import Position
from src.platform.exchanges.binance.rest import BinanceFuturesREST
from src.platform.exchanges.binance.ws import BinanceWS, WS_BASE
from src.platform.exchanges.binance.normalize import norm_position, norm_markprice_tick, norm_kline_event, norm_premium_index_item

class BinanceExchange(ExchangeAdapter):
    name = "binance"

    def __init__(self):
        self._rest_by_account: dict[str, BinanceFuturesREST] = {}
        self._tick_ws: dict[str, BinanceWS] = {}
        self._kline_ws: dict[str, BinanceWS] = {}
        self._user_ws: dict[str, BinanceWS] = {}
        self._listen_key: dict[str, str] = {}
        self._keepalive_threads: dict[str, threading.Thread] = {}

    def _rest(self, account: str) -> BinanceFuturesREST:
        if account in self._rest_by_account:
            return self._rest_by_account[account]
        key_var = f"BINANCE_{account.upper()}_API_KEY"
        sec_var = f"BINANCE_{account.upper()}_API_SECRET"
        api_key = os.environ.get(key_var, "")
        api_sec = os.environ.get(sec_var, "")
        if not api_key or not api_sec:
            raise RuntimeError(f"Missing env vars for account={account}: {key_var}/{sec_var}")
        cli = BinanceFuturesREST(api_key, api_sec)
        self._rest_by_account[account] = cli
        return cli

    def subscribe_ticks(self, account: str, symbols: list[str], cb: TickCallback) -> None:
        streams = [f"{s.lower()}@markPrice@1s" for s in symbols]
        url = WS_BASE + "/" + "/".join(streams)
        def on_msg(data: dict):
            t = norm_markprice_tick(data)
            if not t:
                return
            sym, px = t
            cb(sym, px)
        ws = BinanceWS(url=url, on_message=on_msg, name=f"BinanceTicks-{account}")
        ws.start()
        self._tick_ws[account] = ws

    def subscribe_candles(self, account: str, symbols: list[str], intervals: list[str], cb: CandleCallback) -> None:
        streams = [f"{s.lower()}@kline_{itv}" for s in symbols for itv in intervals]
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

    def subscribe_user_stream(self, account: str, cb: UserEventCallback) -> None:
        rest = self._rest(account)
        lk = self._listen_key.get(account)
        if not lk:
            resp = rest.create_listen_key()
            lk = resp.get("listenKey")
            if not lk:
                raise RuntimeError(f"Failed to create listenKey for {account}: {resp}")
            self._listen_key[account] = lk

        url = WS_BASE + "/" + lk
        ws = BinanceWS(url=url, on_message=cb, name=f"BinanceUser-{account}")
        ws.start()
        self._user_ws[account] = ws

        if account not in self._keepalive_threads:
            def keepalive():
                while True:
                    try:
                        rest.keepalive_listen_key(lk)
                    except Exception as e:
                        print(f"[BinanceUser-{account}] keepalive error: {e}")
                    time.sleep(30*60)
            t = threading.Thread(target=keepalive, daemon=True, name=f"BinanceListenKeyKeepAlive-{account}")
            t.start()
            self._keepalive_threads[account] = t

    def fetch_positions(self, account: str) -> list[Position]:
        raw = self._rest(account).position_risk()
        out: list[Position] = []
        for r in raw:
            p = norm_position(self.name, account, r)
            if p:
                out.append(p)
        return out

    def fetch_funding_snapshot(self, symbols: list[str]) -> list[dict]:
        out = []
        if not self._rest_by_account:
            return out
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

    def fetch_open_orders(self, account: str, symbol: str | None = None) -> list[dict]:
        return self._rest(account).open_orders(symbol=symbol)

    def place_order(self, intent: OrderIntent) -> dict:
        rest = self._rest(intent.account)
        side = "BUY" if intent.side == Side.LONG else "SELL"
        params = {"symbol": intent.symbol, "side": side, "type": intent.order_type.value, "quantity": str(intent.qty)}
        if intent.reduce_only:
            params["reduceOnly"] = "true"
        if intent.client_order_id:
            params["newClientOrderId"] = intent.client_order_id
        if intent.price is not None:
            params["price"] = str(intent.price)
            params.setdefault("timeInForce", "GTC")
        return rest.new_order(**params)


    # --- v9 Market State Layer ---
    def fetch_account_state(self, account: str) -> dict:
        raw = self._rest(account).account()
        wallet = float(raw.get("totalWalletBalance") or 0.0)
        unreal = float(raw.get("totalUnrealizedProfit") or 0.0)
        # totalMarginBalance is effectively equity on futures
        equity = float(raw.get("totalMarginBalance") or (wallet + unreal))
        available = float(raw.get("availableBalance") or 0.0)
        margin_used = wallet - available
        return {
            "wallet_balance": wallet,
            "equity": equity,
            "available_balance": available,
            "margin_used": float(margin_used),
            "unrealized_pnl": unreal,
        }

    def fetch_open_interest_hist(self, account: str, *, symbol: str, interval: str, limit: int = 30) -> list[dict]:
        raw = self._rest(account).open_interest_hist(symbol=symbol, period=interval, limit=int(limit))
        out = []
        for r in raw or []:
            # keys typically: "sumOpenInterest", "sumOpenInterestValue", "timestamp"
            ts_ms = int(r.get("timestamp") or 0)
            if ts_ms <= 0:
                continue
            out.append({
                "ts": datetime.fromtimestamp(ts_ms/1000.0, tz=timezone.utc),
                "open_interest": float(r.get("sumOpenInterest") or r.get("openInterest") or 0.0),
                "open_interest_value": float(r.get("sumOpenInterestValue") or r.get("openInterestValue") or 0.0),
            })
        return out
