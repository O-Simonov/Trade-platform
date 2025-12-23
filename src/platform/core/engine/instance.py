# src/platform/core/engine/instance.py
from __future__ import annotations

import logging
import threading
import time
import uuid as _uuid
from typing import Any

from src.platform.core.strategy.base import Strategy
from src.platform.exchanges.base.exchange import ExchangeAdapter
from src.platform.data.storage.base import Storage
from src.platform.core.utils.idempotency import make_client_order_id
from src.platform.core.oms.oms import OrderManager
from src.platform.core.risk.risk_engine import RiskEngine, RiskLimits

from src.platform.market_state.reader import MarketStateReader
from src.platform.market_state.pollers.balance_poller import BalancePoller
from src.platform.market_state.pollers.oi_poller import OpenInterestPoller
from src.platform.core.oms.writer import OmsWriter
from src.platform.core.oms.parser import parse_binance_user_event




logger = logging.getLogger(__name__)


def new_pos_uid() -> str:
    return _uuid.uuid4().hex

class TradingInstance:
    exchange: ExchangeAdapter
    storage: Storage
    strategy: Strategy

    # --- runtime ---
    account: str
    role: str
    symbols: list[str]
    candle_intervals: list[str]
    def __init__(self, *, exchange, storage, strategy, account, role, symbols,
                 candle_intervals, funding_poll_sec, oms_reconcile_sec,
                 oms_pending_timeout_sec, ids, risk_limits,
                 base_ref=None, hedge_ratio=None, dry_run=True):
        self.logger = logging.getLogger("src.platform.core.engine.instance")



        self.oms_writer = OmsWriter(storage)

        self.ex = exchange
        self.db = storage
        self.strategy = strategy

        self.exchange = exchange
        self.storage = storage

        self.account = account
        self.role = (role or "MIXED").upper()
        self.symbols = [s.upper() for s in (symbols or [])]
        self.candle_intervals = candle_intervals or []
        self.ids = dict(ids)
        self.dry_run = bool(dry_run)

        # ✅ удобные алиасы (ЧТОБЫ НЕ ПУТАТЬСЯ)
        self.exchange_id: int = int(self.ids["_exchange_id"])
        self.account_id: int = int(self.ids["_account_id"])
        self.symbol_ids: dict[str, int] = {s: int(self.ids[s]) for s in self.symbols if s in self.ids}

        # OMS + Risk
        self.oms = OrderManager(
            storage=self.db,
            exchange_id=self.exchange_id,
            account_id=self.account_id,
        )
        self.risk = RiskEngine(risk_limits)

        self.oms_pending_timeout_sec = int(oms_pending_timeout_sec or 20)
        self.oms_reconcile_sec = float(oms_reconcile_sec or 15.0)

        # MarketState
        ex_name = getattr(self.ex, "name", self.ex.__class__.__name__.lower())
        self.market_state = MarketStateReader(storage=self.db, exchange=ex_name, account=self.account)

        # positions cache
        self._positions_cache: dict[str, Any] = {}
        self._pos_uid_by_symbol: dict[str, str] = {}

        # loops
        self._stop = threading.Event()
        self._ms_refresh_sec = 2.0

        self.logger.info(
            "[Init] account=%s role=%s symbols=%s dry_run=%s",
            self.account, self.role, ",".join(self.symbols), self.dry_run,
        )

    # ---------------- POSITIONS ----------------

    def refresh_positions(self) -> None:
        state = self.market_state.get_positions() or {}
        self._positions_cache = state
        for sym, p in state.items():
            if sym not in self._pos_uid_by_symbol and getattr(p, "qty", 0) != 0:
                self._pos_uid_by_symbol[sym] = new_pos_uid()

    def _market_state_refresh_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.refresh_positions()
            except Exception as e:
                self.logger.warning("[MarketStateRefresh][%s] %s", self.account, e)
            time.sleep(self._ms_refresh_sec)

    def _ensure_pos_uid(self, sym: str) -> str:
        uid = self._pos_uid_by_symbol.get(sym)
        if not uid:
            uid = new_pos_uid()
            self._pos_uid_by_symbol[sym] = uid
        return uid

    def _open_positions_count(self) -> int:
        return sum(1 for p in self._positions_cache.values() if getattr(p, "qty", 0) != 0)

    # ---------------- USER WS ----------------

    def on_user_event(self, event: dict):
        try:
            o = event.get("o")
            if not o:
                return

            symbol = o.get("s")
            if not symbol:
                return

            symbol_id = self.ids.get(symbol)
            if not symbol_id:
                self.logger.warning(
                    "[USER EVENT] Unknown symbol %s (ids=%s)", symbol, list(self.ids.keys())
                )
                return

            res = parse_binance_user_event(
                event,
                exchange_id=self.ids["_exchange_id"],
                account_id=self.ids["_account_id"],
                symbol_id=symbol_id,  # ✅ ВЕРНО
            )

            if not res:
                return

            order_evt, trade_evt = res

            self.oms_writer.write_order(order_evt)

            if trade_evt:
                self.oms_writer.write_trade(trade_evt)

        except Exception:
            self.logger.exception("[USER EVENT PARSE ERROR]")


    def on_candle(self, candle: dict):
        """
        candle = {
            symbol, interval, open_time,
            open, high, low, close, volume, source
        }
        """
        try:
            # 1️⃣ передаём в стратегию
            if hasattr(self.strategy, "on_candle"):
                self.strategy.on_candle(
                    symbol=candle["symbol"],
                    interval=candle["interval"],
                    candle=candle,
                )

            # 2️⃣ (опционально) сохранить в БД
            self.storage.upsert_candles([{
                "exchange_id": self.exchange.exchange_id,
                "symbol_id": self.exchange.symbol_ids[candle["symbol"]],
                "interval": candle["interval"],
                "open_time": candle["open_time"],
                "open": candle["open"],
                "high": candle["high"],
                "low": candle["low"],
                "close": candle["close"],
                "volume": candle["volume"],
                "source": candle.get("source", "ws"),
            }])

        except Exception as e:
            self.logger.exception("on_candle error: %s", e)

    # ---------------- TICKS ----------------
    def on_tick(self, symbol: str, price: float) -> None:

        symbol = symbol.upper()
        position = self._positions_cache.get(symbol)
        pos_uid = self._ensure_pos_uid(symbol)
        self.logger.info(
            "[TICK] %s price=%s position=%s",
            symbol,
            price,
            "YES" if position else "NO",
        )

        intent = self.strategy.on_tick(symbol, float(price), position, pos_uid)
        if intent is None:
            return

        # ✅ ЛОГ — ВОТ ЗДЕСЬ
        self.logger.info(
            "[INTENT] %s %s qty=%s reduce_only=%s type=%s",
            intent.symbol, intent.side, intent.qty, intent.reduce_only, intent.intent_type,
        )

        # client_order_id for idempotency
        cid = make_client_order_id(
            intent.exchange,
            intent.account,
            intent.symbol,
            getattr(intent.side, "value", str(intent.side)),
            f"{intent.qty:.8f}",
            pos_uid,
            intent.intent_type,
        )

        if not self.oms.should_submit(cid):
            self.logger.info("[OMS] skip duplicate client_order_id=%s", cid)
            return

        balances = self.market_state.get_balances() or {}
        day_pnl = self.db.get_today_realized_pnl(self.ids["_exchange_id"], self.ids["_account_id"])

        ok, reason = self.risk.allow_order(
            intent=intent,
            price=float(price),
            open_positions_count=self._open_positions_count(),
            current_day_realized_pnl=float(day_pnl),
            market_state=balances,
        )
        if not ok:
            self.logger.info("[RISK BLOCK] %s: %s", intent.symbol, reason)
            return

        # placeholder before submit
        self.oms.record_pending_submit(
            client_order_id=cid,
            symbol_id=self.ids[symbol],
            strategy_id=self.strategy.strategy_id,
            pos_uid=pos_uid,
            intent=intent,
        )

        if self.dry_run:
            self.logger.info("[DRY_RUN] submit %s %s qty=%s cid=%s", intent.symbol, intent.side, intent.qty, cid)
            return

        # real submit
        self.logger.info("[SUBMIT] %s %s qty=%s cid=%s", intent.symbol, intent.side, intent.qty, cid)
        self.ex.place_order(intent)

    # ---------------- OMS reconcile ----------------

    def _oms_reconcile_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.oms.reconcile_pending_timeouts(self.oms_pending_timeout_sec)
            except Exception as e:
                self.logger.warning("[OMS reconcile %s] %s", self.account, e)
            time.sleep(self.oms_reconcile_sec)

    # ---------------- RUN ----------------

    def run(self):
        logger.info(
            "[Instance] running: account=%s role=%s symbols=%s dry_run=%s",
            self.account,
            self.role,
            ",".join(self.symbols),
            self.dry_run,
        )

        # 🔹 1. TICKS (цены)
        self.exchange.subscribe_ticks(
            account=self.account,
            symbols=self.symbols,
            cb=self.on_tick,
        )

        # 🔹 2. USER STREAM (ордера / fills / trades)
        self.exchange.subscribe_user_stream(
            account=self.account,
            cb=self.on_user_event,  # 🔥 ВОТ ЗДЕСЬ
        )

        # 🔹 3. CANDLES (если есть)
        if self.candle_intervals:
            self.exchange.subscribe_candles(
                account=self.account,
                symbols=self.symbols,
                intervals=self.candle_intervals,
                cb=self.on_candle,  # 🔥 ВАЖНО
            )
            self.logger.info(
                "[WS] subscribe_candles account=%s symbols=%s intervals=%s",
                self.account,
                self.symbols,
                self.candle_intervals,
            )

        # основной loop
        while not self._stop.is_set():
            time.sleep(1.0)

    def stop(self) -> None:
        self._stop.set()
