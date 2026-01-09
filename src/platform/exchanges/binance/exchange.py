# src/platform/exchanges/binance/exchange.py
from __future__ import annotations

import os
import time
import threading
import logging
import random
from typing import List, Optional, Any

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

    Fixes / additions:
      ✅ listenKey keepalive thread (NO early calls, has 429 backoff)
      ✅ warmup wait via ws.connected.wait()
      ✅ create_listen_key() may return dict or str
      ✅ stop WS best-effort + join() to avoid duplicates
      ✅ close() best-effort: stop keepalive + stop ws + close listenKey
      ✅ place_order uses REST.new_order kwargs correctly
    """

    name = "binance"

    def __init__(self) -> None:
        self.logger = logging.getLogger("binance.exchange")

        # REST
        self._rest_by_account: dict[str, BinanceFuturesREST] = {}
        self.rest: Optional[BinanceFuturesREST] = None  # exposed (optional)

        # WS
        self._tick_ws: dict[str, BinanceWS] = {}
        self._kline_ws: dict[str, BinanceWS] = {}
        self._user_ws: dict[str, BinanceWS] = {}

        # user-stream keepalive
        self._user_keepalive_stop: dict[str, threading.Event] = {}
        self._user_keepalive_thr: dict[str, threading.Thread] = {}
        self._user_listen_key: dict[str, str] = {}

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

    # ---------------- internals ----------------

    def _stop_ws_best_effort(self, ws: object) -> None:
        """
        Stop WS and join thread best-effort.
        Prevents double WS threads => double events/ACK logs.
        """
        try:
            if hasattr(ws, "stop") and callable(getattr(ws, "stop")):
                ws.stop()
            elif hasattr(ws, "close") and callable(getattr(ws, "close")):
                ws.close()
        except Exception:
            self.logger.debug("WS stop failed", exc_info=True)

        try:
            if isinstance(ws, threading.Thread):
                ws.join(timeout=2.0)
        except Exception:
            pass

    def _stop_user_keepalive(self, account: str) -> None:
        ev = self._user_keepalive_stop.get(account)
        if ev is not None:
            try:
                ev.set()
            except Exception:
                pass

        thr = self._user_keepalive_thr.get(account)
        if thr is not None:
            try:
                thr.join(timeout=2.0)
            except Exception:
                pass

        self._user_keepalive_stop.pop(account, None)
        self._user_keepalive_thr.pop(account, None)

    @staticmethod
    def _is_http_429(exc: BaseException) -> bool:
        s = str(exc)
        return ("HTTP 429" in s) or ("Too many requests" in s) or ("-1003" in s)

    def _start_user_keepalive(self, *, account: str, rest: BinanceFuturesREST, listen_key: str) -> None:
        """
        Binance Futures: keepalive listenKey.
        ВАЖНО: НЕ делать keepalive сразу после старта — первый раз через ~25-30 минут.
        env:
          BINANCE_LISTENKEY_KEEPALIVE_SEC=1500  (25 мин)  [default]
        """
        self._stop_user_keepalive(account)

        stop_ev = threading.Event()
        self._user_keepalive_stop[account] = stop_ev

        try:
            interval_sec = int(os.environ.get("BINANCE_LISTENKEY_KEEPALIVE_SEC", "1500"))
        except Exception:
            interval_sec = 1500

        interval_sec = max(60, int(interval_sec))

        # small jitter to avoid synchronized bursts if multiple processes
        def jitter() -> float:
            return random.uniform(0.0, 5.0)

        def loop() -> None:
            self.logger.info("[BinanceUser] keepalive thread started: account=%s interval=%ss", account, interval_sec)

            # FIRST keepalive must be delayed (no early PUT spam)
            if stop_ev.wait(float(interval_sec) + jitter()):
                self.logger.info("[BinanceUser] keepalive thread stopped: account=%s", account)
                return

            backoff = 60.0          # start backoff on failure
            backoff_max = 15 * 60.0 # cap 15 min

            while not stop_ev.is_set():
                try:
                    rest.keepalive_listen_key(listen_key)
                    # success => reset backoff
                    backoff = 60.0
                    self.logger.debug("[BinanceUser] keepalive ok: account=%s", account)

                    # next scheduled keepalive
                    if stop_ev.wait(float(interval_sec) + jitter()):
                        break

                except Exception as e:
                    # 429 => do not spam exceptions, do controlled backoff
                    if self._is_http_429(e):
                        self.logger.warning(
                            "[BinanceUser] keepalive rate-limited (429). Backoff %.1fs account=%s",
                            backoff, account
                        )
                    else:
                        self.logger.exception("[BinanceUser] keepalive failed: account=%s", account)

                    if stop_ev.wait(backoff + jitter()):
                        break

                    backoff = min(backoff * 2.0, backoff_max)

            self.logger.info("[BinanceUser] keepalive thread stopped: account=%s", account)

        thr = threading.Thread(
            target=loop,
            name=f"BinanceUserKeepalive-{account}",
            daemon=True,
        )
        thr.start()
        self._user_keepalive_thr[account] = thr

    @staticmethod
    def _ws_warmup_seconds() -> float:
        """
        Warmup to reduce chance missing very fast fills after submit.
        env:
          BINANCE_USER_WARMUP_SEC=1.5
          BINANCE_WS_WARMUP_SEC=1.5 (alias)
        """
        v = os.environ.get("BINANCE_USER_WARMUP_SEC") or os.environ.get("BINANCE_WS_WARMUP_SEC") or "1.5"
        v = str(v).strip()
        try:
            return max(0.0, float(v))
        except Exception:
            return 1.5

    @staticmethod
    def _extract_listen_key(resp: Any) -> str:
        """
        create_listen_key may return dict {"listenKey": "..."} OR str "...".
        """
        if isinstance(resp, str):
            return resp.strip()
        if isinstance(resp, dict):
            return str(resp.get("listenKey") or "").strip()
        return str(getattr(resp, "listenKey", "") or "").strip()

    # ---------------- subscriptions ----------------

    def subscribe_ticks(self, *, account: str, symbols: list[str], cb: TickCallback) -> None:
        streams = [f"{s.lower()}@markPrice@1s" for s in symbols]
        if not streams:
            return

        url = self._stream_url(streams)
        self.logger.info("WS TICKS URL = %s", url)

        def on_msg(data: dict) -> None:
            res = norm_markprice_tick(data)
            if res:
                sym, px = res
                cb(sym, px)

        old = self._tick_ws.get(account)
        if old is not None:
            self._stop_ws_best_effort(old)

        ws = BinanceWS(url=url, on_message=on_msg, name=f"BinanceTicks-{account}")
        ws.start()
        self._tick_ws[account] = ws

    def subscribe_user_stream(self, *, account: str, cb: UserEventCallback) -> None:
        """
        User Data Stream:
          1) REST create_listen_key()
          2) WS connect WS_WS_BASE/<listenKey>
          3) keepalive ~ every 25-30 minutes (NO early calls, 429 backoff)
          4) warmup wait (важно для OMS)
        """
        rest = self._rest(account)

        try:
            resp = rest.create_listen_key()
        except Exception as e:
            self.logger.error("[BinanceUser] create_listen_key failed: %s", e)
            raise

        listen_key = self._extract_listen_key(resp)
        if not listen_key:
            raise RuntimeError(f"listenKey error: {resp!r}")

        self._user_listen_key[account] = listen_key

        # stop old user WS
        old = self._user_ws.get(account)
        if old is not None:
            self._stop_ws_best_effort(old)

        # keepalive
        self._start_user_keepalive(account=account, rest=rest, listen_key=listen_key)

        # start user WS (safe wrapper)
        url = f"{WS_WS_BASE}/{listen_key}"

        def on_user_payload(payload: dict) -> None:
            ev = payload.get("e") or payload.get("eventType") or "?"
            if ev != "?":
                self.logger.info("[BinanceUser][EVT] account=%s e=%s", account, ev)
            else:
                self.logger.debug("[BinanceUser][EVT] account=%s payload=%r", account, payload)

            try:
                cb(payload)
            except Exception:
                s = repr(payload)
                if len(s) > 1200:
                    s = s[:1200] + "…"
                self.logger.exception(
                    "[BinanceUser] handler failed: account=%s e=%s payload=%s",
                    account, ev, s
                )

        ws = BinanceWS(url=url, on_message=on_user_payload, name=f"BinanceUser-{account}")
        ws.start()
        self._user_ws[account] = ws

        # warmup: wait until connected (or timeout)
        warmup = float(self._ws_warmup_seconds() or 0.0)
        if warmup > 0:
            self.logger.info("[BinanceUser] warmup wait: %.2fs (account=%s)", warmup, account)
            try:
                ws.connected.wait(timeout=warmup)
            except Exception:
                time.sleep(warmup)

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

        def on_msg(data: dict) -> None:
            row = norm_kline_event(data)
            if row:
                cb(row)

        old = self._kline_ws.get(account)
        if old is not None:
            self._stop_ws_best_effort(old)

        ws = BinanceWS(url=url, on_message=on_msg, name=f"BinanceKlines-{account}")
        ws.start()
        self._kline_ws[account] = ws

    # ---------------- trading ----------------

    def _resolve_symbol_id_from_intent(self, intent: OrderIntent) -> int:
        sid = int(getattr(intent, "symbol_id", 0) or 0)
        if sid > 0:
            return sid

        sym = (intent.symbol or "").upper()
        if sym in self.symbol_ids:
            return int(self.symbol_ids[sym])

        raise KeyError(f"symbol_id not found for symbol={intent.symbol}")

    @staticmethod
    def _map_side(self_intent: OrderIntent) -> str:
        if self_intent.side == Side.LONG:
            return "BUY"
        if self_intent.side == Side.SHORT:
            return "SELL"
        raise ValueError(f"Unsupported intent.side={self_intent.side}")

    def place_order(self, intent: OrderIntent) -> dict:
        """
        Submit order to Binance Futures (UM).

        BinanceFuturesREST.new_order expects kwargs:
          symbol, side, order_type, quantity, price, time_in_force,
          reduce_only, client_order_id, **extra
        """
        if not self.storage or self.exchange_id is None:
            raise RuntimeError("BinanceExchange not bound")

        if not intent.account:
            raise ValueError("OrderIntent.account is empty (required for BinanceExchange)")

        rest = self._rest(intent.account)

        symbol = (intent.symbol or "").upper()
        symbol_id = self._resolve_symbol_id_from_intent(intent)
        side = self._map_side(intent)

        filters = self.storage.get_symbol_filters(
            exchange_id=self.exchange_id,
            symbol_id=symbol_id,
        )
        if not filters:
            raise RuntimeError(
                f"symbol_filters not found: exchange_id={self.exchange_id} symbol_id={symbol_id} symbol={symbol}"
            )

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

        order_type = str(intent.order_type.value).upper()
        reduce_only = bool(getattr(intent, "reduce_only", False))
        client_order_id = getattr(intent, "client_order_id", None)

        time_in_force: str | None = None
        if intent.order_type == OrderType.LIMIT:
            if price is None or price <= 0:
                raise ValueError("LIMIT order requires price")
            time_in_force = "GTC"

        extra: dict[str, Any] = {}
        pos_side = os.environ.get("BINANCE_POSITION_SIDE", "").strip().upper()
        if pos_side in ("LONG", "SHORT", "BOTH"):
            extra["positionSide"] = pos_side

        self.logger.info(
            "[ORDER SUBMIT] %s %s type=%s qty=%s price=%s reduce_only=%s cid=%s",
            symbol, side, order_type, qty, price, reduce_only, client_order_id
        )

        resp = rest.new_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=float(qty),
            price=float(price) if price is not None else None,
            time_in_force=time_in_force,
            reduce_only=reduce_only if reduce_only else None,
            client_order_id=str(client_order_id) if client_order_id else None,
            **extra,
        )

        if isinstance(resp, dict):
            self.logger.info(
                "[ORDER ACK] symbol=%s orderId=%s status=%s cid=%s",
                symbol, resp.get("orderId"), resp.get("status"), client_order_id
            )
            if not resp.get("orderId"):
                self.logger.error("[ORDER ACK][BAD] resp=%r", resp)
        else:
            self.logger.error("[ORDER ACK][BAD] resp=%r", resp)

        return dict(resp or {})

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

        margin_used = raw.get("totalInitialMargin")
        if margin_used is None:
            pm = float(raw.get("totalPositionInitialMargin") or 0.0)
            om = float(raw.get("totalOpenOrderInitialMargin") or 0.0)
            margin_used = pm + om
        margin_used = float(margin_used or 0.0)

        return {
            "wallet_balance": wallet,
            "equity": equity,
            "available_balance": available,
            "margin_used": margin_used,
            "unrealized_pnl": unreal,
        }

    # ---------------- lifecycle ----------------

    def close(self) -> None:
        """
        Best-effort: stop keepalive + stop ws + close listenKey.
        """
        # stop keepalive threads
        for account in list(self._user_keepalive_stop.keys()):
            self._stop_user_keepalive(account)

        # best-effort close listenKey
        for account, lk in list(self._user_listen_key.items()):
            try:
                if lk:
                    rest = self._rest_by_account.get(account)
                    if rest is not None and hasattr(rest, "close_listen_key"):
                        rest.close_listen_key(lk)
            except Exception:
                self.logger.debug("[BinanceUser] close_listen_key failed: account=%s", account, exc_info=True)

        self._user_listen_key.clear()

        # stop WS
        for ws in list(self._tick_ws.values()) + list(self._kline_ws.values()) + list(self._user_ws.values()):
            self._stop_ws_best_effort(ws)

        self._tick_ws.clear()
        self._kline_ws.clear()
        self._user_ws.clear()
