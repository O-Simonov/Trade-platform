# src/platform/core/engine/instance.py
from __future__ import annotations

import logging
import threading
import time
from typing import List, Optional

from src.platform.core.models.order import OrderIntent
from src.platform.core.strategy.base import Strategy
from src.platform.core.risk.risk_engine import RiskLimits
from src.platform.exchanges.base.exchange import ExchangeAdapter
from src.platform.data.storage.postgres.storage import PostgreSQLStorage


class TradingInstance:
    def __init__(
        self,
        *,
        exchange: ExchangeAdapter,
        storage: PostgreSQLStorage,
        strategy: Strategy,
        account: str,
        role: str,
        symbols: List[str],
        candle_intervals: List[str],
        ids: dict,
        risk_limits: RiskLimits,
        dry_run: bool,
        funding_poll_sec: float = 120.0,
        oms_reconcile_sec: float = 15.0,
        oms_pending_timeout_sec: float = 20.0,
        base_ref: Optional[str] = None,
        hedge_ratio: Optional[float] = None,
    ):
        self.logger = logging.getLogger("src.platform.core.engine.instance")

        # --- identity ---
        self.exchange = exchange
        self.ex = exchange  # ✅ backward-compatible alias (runner/old code)
        self.account = account
        self.role = role
        self.symbols = list(symbols or [])
        self.strategy = strategy

        # ✅ FIX: сохраняем candle_intervals
        self.candle_intervals = list(candle_intervals or [])

        # --- infra ---
        self.storage = storage
        self.ids = dict(ids or {})
        self.exchange_id = int(self.ids["_exchange_id"])
        self.account_id = int(self.ids.get("_account_id") or 0)

        # symbol_ids shortcut
        self.symbol_ids: dict[str, int] = {
            s: int(self.ids[s]) for s in self.symbols if s in self.ids
        }

        # --- risk ---
        self.risk_limits = risk_limits
        self.dry_run = bool(dry_run)

        # --- optional ---
        self.base_ref = base_ref
        self.hedge_ratio = hedge_ratio

        # --- timing ---
        self.funding_poll_sec = float(funding_poll_sec)
        self.oms_reconcile_sec = float(oms_reconcile_sec)
        self.oms_pending_timeout_sec = float(oms_pending_timeout_sec)

        # --- state ---
        self._running = False
        self._stop = threading.Event()

        # OMS hooks (будут подключаться на STEP E)
        self.oms = None
        self.oms_reconciler = None

        # runtime cache
        self._last_account_state: dict | None = None

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Main blocking loop.
        WS callbacks feed data → intents processed here.
        """
        self.logger.info(
            "[Instance] running: account=%s role=%s symbols=%s dry_run=%s",
            self.account,
            self.role,
            ",".join(self.symbols),
            self.dry_run,
        )

        self._running = True

        # --- subscriptions ---
        self.exchange.subscribe_ticks(
            account=self.account,
            symbols=self.symbols,
            cb=self._on_tick,
        )

        self.exchange.subscribe_user_stream(
            account=self.account,
            cb=self._on_user_event,
        )

        if self.candle_intervals:
            self.exchange.subscribe_candles(
                account=self.account,
                symbols=self.symbols,
                intervals=self.candle_intervals,
                cb=self._on_candle,
            )

        # OMS reconcile loop (daemon)
        t = threading.Thread(target=self._oms_loop, daemon=True, name=f"OMSLoop-{self.account}")
        t.start()

        # Account polling loop (STEP F)
        t_acc = threading.Thread(
            target=self._account_loop,
            daemon=True,
            name=f"AccountLoop-{self.account}",
        )
        t_acc.start()

        self.strategy.on_start()

        # --- idle loop ---
        while self._running and not self._stop.is_set():
            self._drain_intents()
            time.sleep(0.05)

    def stop(self) -> None:
        self._running = False
        self._stop.set()
        try:
            self.strategy.on_stop()
        except Exception:
            self.logger.exception("[StrategyStopError]")

    # ------------------------------------------------------------------
    # WS callbacks
    # ------------------------------------------------------------------

    def _on_tick(self, symbol: str, price: float) -> None:
        self.logger.info("[TICK] %s price=%s", symbol, price)
        self.strategy.on_tick(symbol=symbol, price=price)

    def _on_candle(self, candle: dict) -> None:
        # оставляю как у тебя: strategy сама решит что делать
        if hasattr(self.strategy, "on_candle"):
            self.strategy.on_candle(candle=candle)

    def _on_user_event(self, event: dict) -> None:
        """
        User stream events (orders / trades).
        STEP E будет сюда подключать parser + oms.apply_event(...)
        """
        # пока заглушка, чтобы не падать
        return

    # ------------------------------------------------------------------
    # intents → OMS
    # ------------------------------------------------------------------

    def _drain_intents(self) -> None:
        intents: List[OrderIntent] = self.strategy.get_intents()
        if not intents:
            return

        for intent in intents:
            self._process_intent(intent)

    def _process_intent(self, intent: OrderIntent) -> None:
        """
        Сейчас: минимальная отправка.
        STEP D/E: сюда добавится preflight + state machine.
        """
        if self.dry_run:
            self.logger.info(
                "[DRY_RUN] %s %s qty=%s cid=%s",
                intent.symbol,
                intent.side.name,
                intent.qty,
                getattr(intent, "client_order_id", None),
            )
            return

        self.logger.info(
            "[SUBMIT] %s %s qty=%s cid=%s",
            intent.symbol,
            intent.side.name,
            intent.qty,
            getattr(intent, "client_order_id", None),
        )

        # ✅ ExchangeAdapter.place_order(intent) — без extra args
        self.exchange.place_order(intent)

    # ------------------------------------------------------------------
    # OMS reconcile loop (STEP E.4)
    # ------------------------------------------------------------------

    def _oms_loop(self) -> None:
        """
        STEP E.4:
          - reconcile pending timeouts
          - REST reconcile open orders (run_once requires symbol_ids)
        """
        while not self._stop.is_set():
            try:
                if self.oms:
                    self.oms.reconcile_pending_timeouts(self.oms_pending_timeout_sec)

                if self.oms_reconciler:
                    # ✅ твой reconciler требует symbol_ids
                    sym_ids = list(self.symbol_ids.values())
                    if sym_ids:
                        self.oms_reconciler.run_once(symbol_ids=sym_ids)
            except Exception:
                self.logger.exception("[OMS LOOP ERROR]")
            time.sleep(self.oms_reconcile_sec)

    def _account_loop(self) -> None:
        """
        STEP F:
          - poll account state
          - store snapshot
          - update runtime cache for RiskEngine
        """
        while not self._stop.is_set():
            try:
                state = self.exchange.fetch_account_state(account=self.account)

                self.storage.upsert_account_state(
                    exchange_id=self.exchange_id,
                    account_id=self.account_id,
                    state=state,
                )

                self._last_account_state = state

                self.logger.debug(
                    "[ACCOUNT] wallet=%s equity=%s avail=%s",
                    state.get("wallet_balance"),
                    state.get("equity"),
                    state.get("available_balance"),
                )

            except Exception:
                self.logger.exception("[ACCOUNT LOOP ERROR]")

            time.sleep(self.funding_poll_sec)
