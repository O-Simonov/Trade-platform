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

logger = logging.getLogger(__name__)


def new_pos_uid() -> str:
    return _uuid.uuid4().hex


class TradingInstance:
    """
    TradingInstance — v9 (C3/D3 compatible)
    ✅ NO direct REST positions in instance
    ✅ Reads positions/balances via MarketStateReader
    ✅ D3: binds exchange adapter with storage/exchange_id/symbol_ids for order normalization
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
        self.ex = exchange
        self.db = storage
        self.strategy = strategy

        self.account = account
        self.role = (role or "MIXED").upper()
        self.symbols = [s.upper() for s in (symbols or [])]
        self.candle_intervals = candle_intervals or []

        self.ids = ids
        self.dry_run = bool(dry_run)

        # ------------------------------------------------------------------
        # D3: bind exchange adapter to storage + ids (needed for normalize_order via DB filters)
        # ------------------------------------------------------------------
        self._bind_exchange_context()

        # --- Market State Reader ---
        ex_name = getattr(self.ex, "name", self.ex.__class__.__name__.lower())
        self.market_state = MarketStateReader(
            storage=self.db,
            exchange=ex_name,
            account=self.account,
        )

        # --- position cache ---
        self._positions_cache: dict[str, Any] = {}
        self._pos_uid_by_symbol: dict[str, str] = {}

        # --- risk / OMS ---
        self.risk = RiskEngine(risk_limits)
        self.oms = OrderManager(
            storage=self.db,
            exchange_id=int(self.ids["_exchange_id"]),
            account_id=int(self.ids["_account_id"]),
        )

        self.oms_pending_timeout_sec = int(oms_pending_timeout_sec or 20)
        self.oms_reconcile_sec = float(oms_reconcile_sec or 15.0)

        # loops
        self._stop = threading.Event()
        self._ms_refresh_sec = 2.0  # refresh local cache from MarketState (NO REST)

        # poll intervals (you can tune later)
        self._balance_poll_sec = 30.0
        self._oi_poll_sec = 300.0
        self._oi_intervals = ["5m", "15m", "1h"]

    # ------------------------------------------------------------------
    # D3 BINDING
    # ------------------------------------------------------------------

    def _bind_exchange_context(self) -> None:
        """
        D3:
        - ex.exchange_id
        - ex.storage
        - ex.symbol_ids (symbol -> symbol_id)
        so BinanceExchange.place_order() can do:
            filters = self.storage.get_symbol_filters(exchange_id=..., symbol_id=...)
        """
        try:
            ex_id = int(self.ids["_exchange_id"])
            # exchange_id
            try:
                setattr(self.ex, "exchange_id", ex_id)
            except Exception:
                pass

            # storage
            try:
                setattr(self.ex, "storage", self.db)
            except Exception:
                pass

            # symbol_ids mapping
            sym_ids: dict[str, int] = {}
            for s in self.symbols:
                if s in self.ids:
                    sym_ids[s] = int(self.ids[s])

            try:
                setattr(self.ex, "symbol_ids", sym_ids)
            except Exception:
                pass

            logger.info(
                "[BindExchange][%s] exchange_id=%s symbol_ids=%s",
                self.account,
                getattr(self.ex, "exchange_id", None),
                getattr(self.ex, "symbol_ids", None),
            )
        except Exception as e:
            logger.warning("[BindExchange][%s] failed: %s", self.account, e)

    # ------------------------------------------------------------------
    # POSITIONS (FROM MARKET STATE)
    # ------------------------------------------------------------------

    def refresh_positions(self) -> None:
        """
        Read positions from MarketState (NO REST).
        """
        state = self.market_state.get_positions() or {}
        self._positions_cache = state

        # assign pos_uid when we first see a non-zero qty position
        for sym, p in state.items():
            if sym not in self._pos_uid_by_symbol and getattr(p, "qty", 0) != 0:
                self._pos_uid_by_symbol[sym] = new_pos_uid()

    def _market_state_refresh_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.refresh_positions()
            except Exception as e:
                logger.warning("[MarketStateRefresh][%s] %s", self.account, e)
            time.sleep(self._ms_refresh_sec)

    def _ensure_pos_uid(self, sym: str) -> str:
        uid = self._pos_uid_by_symbol.get(sym)
        if not uid:
            uid = new_pos_uid()
            self._pos_uid_by_symbol[sym] = uid
        return uid

    def _open_positions_count(self) -> int:
        return sum(1 for p in self._positions_cache.values() if getattr(p, "qty", 0) != 0)

    # ------------------------------------------------------------------
    # TICKS
    # ------------------------------------------------------------------

    def on_tick(self, symbol: str, price: float) -> None:
        position = self._positions_cache.get(symbol)
        pos_uid = self._ensure_pos_uid(symbol)

        intent = self.strategy.on_tick(symbol, float(price), position, pos_uid)
        if not intent:
            return

        cid = make_client_order_id(
            intent.exchange,
            intent.account,
            intent.symbol,
            getattr(intent.side, "value", str(intent.side)),
            f"{float(intent.qty):.8f}",
            pos_uid,
            intent.intent_type,
        )

        # idempotency gate
        if not self.oms.should_submit(cid):
            return

        balances = self.market_state.get_balances() or {}

        # daily pnl (stub is OK in DRY_RUN, but keep call)
        day_pnl = self.db.get_today_realized_pnl(
            int(self.ids["_exchange_id"]),
            int(self.ids["_account_id"]),
        )

        ok, reason = self.risk.allow_order(
            intent=intent,
            price=float(price),
            open_positions_count=self._open_positions_count(),
            current_day_realized_pnl=float(day_pnl),
            market_state=balances,
        )

        if not ok:
            logger.info("[RISK BLOCK][%s] %s", intent.symbol, reason)
            return

        # record pending in OMS (placeholder row)
        self.oms.record_pending_submit(
            client_order_id=cid,
            symbol_id=int(self.ids[symbol]),
            strategy_id=self.strategy.strategy_id,
            pos_uid=pos_uid,
            intent=intent,
        )

        if self.dry_run:
            logger.info("[DRY_RUN][%s] %s qty=%s", intent.symbol, intent.side, intent.qty)
            return

        # submit via exchange adapter
        if hasattr(self.ex, "place_order"):
            self.ex.place_order(intent)
        elif hasattr(self.ex, "submit_order"):
            self.ex.submit_order(intent)
        else:
            raise AttributeError("ExchangeAdapter has no place_order/submit_order method")

    # ------------------------------------------------------------------
    # OMS RECONCILE
    # ------------------------------------------------------------------

    def _oms_reconcile_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.oms.reconcile_pending_timeouts(self.oms_pending_timeout_sec)
            except Exception as e:
                logger.warning("[OMS reconcile %s] %s", self.account, e)
            time.sleep(self.oms_reconcile_sec)

    # ------------------------------------------------------------------
    # RUN
    # ------------------------------------------------------------------

    def run(self) -> None:
        # initial snapshot from MarketState (NO REST)
        self.refresh_positions()

        # background loops
        threading.Thread(
            target=self._oms_reconcile_loop,
            daemon=True,
            name=f"OMSReconcile-{self.account}",
        ).start()

        threading.Thread(
            target=self._market_state_refresh_loop,
            daemon=True,
            name=f"MarketStateRefresh-{self.account}",
        ).start()

        # pollers (centralized REST)
        BalancePoller(
            exchange=self.ex,
            storage=self.db,
            exchange_id=int(self.ids["_exchange_id"]),
            account_id=int(self.ids["_account_id"]),
            account=self.account,
            poll_sec=self._balance_poll_sec,
        ).start()

        OpenInterestPoller(
            exchange=self.ex,
            storage=self.db,
            exchange_id=int(self.ids["_exchange_id"]),
            account=self.account,
            symbol_ids={s: int(self.ids[s]) for s in self.symbols if s in self.ids},
            intervals=self._oi_intervals,
            poll_sec=self._oi_poll_sec,
        ).start()

        # subscribe ticks in a separate thread so run() continues
        def _ticks_worker():
            self.ex.subscribe_ticks(
                self.account,
                self.symbols,
                lambda sym, px: self.on_tick(sym, px),
            )

        threading.Thread(
            target=_ticks_worker,
            daemon=True,
            name=f"Ticks-{self.account}",
        ).start()

        logger.info(
            "[Instance] running: account=%s role=%s symbols=%s dry_run=%s",
            self.account,
            self.role,
            ",".join(self.symbols),
            self.dry_run,
        )

        while not self._stop.is_set():
            time.sleep(60)

    def stop(self) -> None:
        self._stop.set()
