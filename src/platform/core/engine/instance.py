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

from src.platform.exchanges.binance.normalize import norm_user_event  # как в твоём normalize.ru

logger = logging.getLogger(__name__)


def new_pos_uid() -> str:
    return _uuid.uuid4().hex


class TradingInstance:
    """
    TradingInstance — v9
    - ticks -> strategy intent -> OMS -> risk -> submit
    - MarketStateReader -> positions/balances
    - D3: User WS -> orders/fills/trades in DB
    """

    def __init__(
        self,
        *,
        exchange: ExchangeAdapter,
        storage: Storage,
        strategy: Strategy,
        account: str,
        role: str,
        symbols: list[str],
        candle_intervals: list[str],
        funding_poll_sec: float,
        oms_reconcile_sec: float,
        oms_pending_timeout_sec: float,
        ids: dict,
        risk_limits: RiskLimits,
        base_ref: dict | None = None,
        hedge_ratio: float | None = None,
        dry_run: bool = True,
    ):
        self.logger = logging.getLogger("src.platform.core.engine.instance")

        self.ex = exchange
        self.db = storage
        self.strategy = strategy

        self.account = account
        self.role = (role or "MIXED").upper()
        self.symbols = [s.upper() for s in (symbols or [])]
        self.candle_intervals = candle_intervals or []
        self.ids = dict(ids)
        self.dry_run = bool(dry_run)

        self.funding_poll_sec = float(funding_poll_sec)
        self.base_ref = base_ref
        self.hedge_ratio = hedge_ratio

        # OMS + Risk
        self.oms = OrderManager(
            storage=self.db,
            exchange_id=self.ids["_exchange_id"],
            account_id=self.ids["_account_id"],
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

    def on_user_event(self, evt: dict) -> None:
        parsed = norm_user_event(evt)
        if not parsed:
            return

        ex_id = self.ids["_exchange_id"]
        acc_id = self.ids["_account_id"]

        # orders
        if "order_row" in parsed:
            row = parsed["order_row"]
            sym = row.get("symbol")
            sym_id = self.ex.symbol_ids.get(sym) if sym else None
            if sym_id:
                self.db.upsert_orders([{**row, "exchange_id": ex_id, "account_id": acc_id, "symbol_id": sym_id}])

        # fills
        if "fill_row" in parsed:
            row = parsed["fill_row"]
            sym = row.get("symbol")
            sym_id = self.ex.symbol_ids.get(sym) if sym else None
            if sym_id:
                self.db.upsert_order_fills([{**row, "exchange_id": ex_id, "account_id": acc_id, "symbol_id": sym_id}])

        # trades (+ привязка strategy_id/pos_uid через OMS pending placeholder)
        if "trade_row" in parsed:
            row = parsed["trade_row"]
            sym = row.get("symbol")
            sym_id = self.ex.symbol_ids.get(sym) if sym else None
            if sym_id:
                meta = self.oms.get_pending_by_client_id(row.get("client_order_id"))
                self.db.upsert_trades([{
                    **row,
                    "exchange_id": ex_id,
                    "account_id": acc_id,
                    "symbol_id": sym_id,
                    "strategy_id": meta["strategy_id"] if meta else None,
                    "pos_uid": meta["pos_uid"] if meta else None,
                }])

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

    def run(self) -> None:
        self.refresh_positions()

        threading.Thread(target=self._oms_reconcile_loop, daemon=True, name=f"OMSReconcile-{self.account}").start()
        threading.Thread(target=self._market_state_refresh_loop, daemon=True, name=f"MarketStateRefresh-{self.account}").start()

        BalancePoller(
            exchange=self.ex,
            storage=self.db,
            exchange_id=self.ids["_exchange_id"],
            account_id=self.ids["_account_id"],
            account=self.account,
            poll_sec=30.0,

        ).start()

        OpenInterestPoller(
            exchange=self.ex,
            storage=self.db,
            exchange_id=self.ids["_exchange_id"],
            account=self.account,
            symbol_ids={s: self.ids[s] for s in self.symbols},
            intervals=["5m", "15m", "1h"],
            poll_sec=300.0,
        ).start()

        def _ticks_worker():
            self.logger.info("[WS] subscribe_ticks account=%s symbols=%s", self.account, self.symbols)
            self.ex.subscribe_ticks(account=self.account, symbols=self.symbols, cb=self.on_tick)

        def _user_worker():
            self.logger.info("[WS] subscribe_user_stream account=%s", self.account)
            self.ex.subscribe_user_stream(account=self.account, cb=self.on_user_event)

        threading.Thread(target=_ticks_worker, daemon=True, name=f"Ticks-{self.account}").start()
        threading.Thread(target=_user_worker, daemon=True, name=f"UserWS-{self.account}").start()

        self.logger.info(
            "[Instance] running: account=%s role=%s symbols=%s dry_run=%s",
            self.account, self.role, ",".join(self.symbols), self.dry_run,
        )

        while not self._stop.is_set():
            time.sleep(60)

    def stop(self) -> None:
        self._stop.set()
