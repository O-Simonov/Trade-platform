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

from src.platform.core.oms.oms import OrderManager
from src.platform.core.oms.parser import parse_binance_user_event
from src.platform.core.oms.events import TradeEvent

from src.platform.core.position.position_manager import PositionManager
from src.platform.core.position.position_reconciler import PositionReconciler


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
        # STEP I/J: позиционный reconcile интервал
        positions_reconcile_sec: float = 2.0,
    ):
        self.logger = logging.getLogger("src.platform.core.engine.instance")

        # --- identity ---
        self.exchange = exchange
        self.ex = exchange  # backward compatibility
        self.account = account
        self.role = role
        self.symbols = list(symbols or [])
        self.strategy = strategy

        self.candle_intervals = list(candle_intervals or [])

        # --- infra ---
        self.storage = storage
        self.ids = dict(ids or {})
        self.exchange_id = int(self.ids["_exchange_id"])
        self.account_id = int(self.ids.get("_account_id") or 0)

        # map only for configured symbols
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

        self.positions_reconcile_sec = float(positions_reconcile_sec)

        # --- state ---
        self._running = False
        self._stop = threading.Event()

        # --- OMS ---
        self.oms = OrderManager(
            storage=self.storage,
            exchange_id=self.exchange_id,
            account_id=self.account_id,
        )

        # --- Positions (STEP J) ---
        self.position_manager = PositionManager(
            storage=self.storage,
            exchange_id=self.exchange_id,
            account_id=self.account_id,
            logger=logging.getLogger("positions.manager"),
        )

        self.position_reconciler: Optional[PositionReconciler] = PositionReconciler(
            exchange=self.exchange,
            position_manager=self.position_manager,
            storage=self.storage,
            exchange_id=self.exchange_id,
            account_id=self.account_id,
            account=self.account,
            symbol_ids=self.symbol_ids,  # ✅ важно: помогает восстановить symbol_id по symbol
            logger=logging.getLogger("positions.reconciler"),
        )

        # runtime cache
        self._last_account_state: Optional[dict] = None
        self._last_idle_log = 0.0

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------

    def run(self) -> None:
        self.logger.info(
            "[Instance] running: account=%s role=%s symbols=%s dry_run=%s",
            self.account,
            self.role,
            ",".join(self.symbols),
            self.dry_run,
        )

        self._running = True

        # --- WS subscriptions ---
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

        # OMS loop
        threading.Thread(
            target=self._oms_loop,
            daemon=True,
            name=f"OMSLoop-{self.account}",
        ).start()

        # account polling loop
        threading.Thread(
            target=self._account_loop,
            daemon=True,
            name=f"AccountLoop-{self.account}",
        ).start()

        # STEP I loop (REST reconcile)
        threading.Thread(
            target=self._position_reconcile_loop,
            daemon=True,
            name=f"PositionsRecon-{self.account}",
        ).start()

        self.strategy.on_start()

        while self._running and not self._stop.is_set():
            self._drain_intents()
            now = time.time()
            if now - self._last_idle_log >= 10.0:
                self.logger.info("[Instance] idle (alive)")
                self._last_idle_log = now
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
        self.strategy.on_tick(symbol=symbol, price=price)

    def _on_candle(self, candle: dict) -> None:
        if hasattr(self.strategy, "on_candle"):
            self.strategy.on_candle(candle=candle)

    def _on_user_event(self, event: dict) -> None:
        """
        STEP G/J — UserStream → OMS + Positions aggregation (TradeEvent → PositionAggregate)
        """
        if not self.oms:
            return

        try:
            events = parse_binance_user_event(
                event,
                exchange=self.exchange.name,
                account=self.account,
            )

            for ev in events:
                # 1) OMS state machine
                self.oms.apply_event(ev)

                # 2) STEP J: realtime positions from trades
                if isinstance(ev, TradeEvent):

                    # ---------- FIX 1: symbol_id ----------
                    if not ev.symbol_id or ev.symbol_id <= 0:
                        sym = (ev.symbol or "").upper()
                        ev.symbol_id = self.symbol_ids.get(sym, 0)

                    if not ev.symbol_id:
                        self.logger.warning(
                            "[POSITIONS][WS] skip trade without symbol_id (symbol=%s)",
                            ev.symbol,
                        )
                        continue

                    # ---------- FIX 2: ts_ms ----------
                    if not ev.ts_ms or ev.ts_ms <= 0:
                        raw = ev.raw_json
                        if not isinstance(raw, dict):
                            raw = {}

                        ev.ts_ms = (
                                raw.get("T")
                                or raw.get("eventTime")
                                or raw.get("E")
                                or int(time.time() * 1000)
                        )

                    if not ev.ts_ms:
                        self.logger.warning(
                            "[POSITIONS][WS] trade without ts_ms (symbol=%s trade_id=%s)",
                            ev.symbol,
                            ev.trade_id,
                        )
                        continue

                    # ---------- APPLY ----------
                    agg = self.position_manager.on_trade_event(ev)
                    if agg and hasattr(self.storage, "upsert_positions"):
                        self.storage.upsert_positions(
                            [agg.to_row(last_trade_id=str(ev.trade_id or ""))]
                        )

        except Exception:
            self.logger.exception("[OMS][USER_EVENT ERROR]")

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
        if self.dry_run:
            self.logger.info(
                "[DRY_RUN] %s %s qty=%s cid=%s",
                intent.symbol,
                intent.side.name,
                intent.qty,
                intent.client_order_id,
            )
            return

        # OMS idempotency gate
        if not self.oms.should_submit(intent.client_order_id):
            self.logger.debug("[OMS] skip duplicate cid=%s", intent.client_order_id)
            return

        self.logger.info(
            "[SUBMIT] %s %s qty=%s cid=%s",
            intent.symbol,
            intent.side.name,
            intent.qty,
            intent.client_order_id,
        )

        self.oms.record_pending_submit(
            client_order_id=intent.client_order_id,
            symbol_id=self.symbol_ids[intent.symbol],
            strategy_id=self.strategy.strategy_id,
            pos_uid=intent.pos_uid,
            intent=intent,
        )

        self.exchange.place_order(intent)

    # ------------------------------------------------------------------
    # OMS reconcile loop
    # ------------------------------------------------------------------

    def _oms_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.oms.reconcile_pending_timeouts(self.oms_pending_timeout_sec)
            except Exception:
                self.logger.exception("[OMS LOOP ERROR]")

            time.sleep(self.oms_reconcile_sec)

    # ------------------------------------------------------------------
    # STEP I: positions reconcile loop (REST)
    # ------------------------------------------------------------------

    def _position_reconcile_loop(self) -> None:
        while not self._stop.is_set():
            try:
                if self.position_reconciler:
                    self.position_reconciler.run_once()
            except Exception:
                self.logger.exception("[POSITIONS][RECON LOOP ERROR]")

            time.sleep(self.positions_reconcile_sec)

    # ------------------------------------------------------------------
    # account polling
    # ------------------------------------------------------------------

    def _account_loop(self) -> None:
        while not self._stop.is_set():
            try:
                state = self.exchange.fetch_account_state(account=self.account)

                self.storage.upsert_account_state(
                    exchange_id=self.exchange_id,
                    account_id=self.account_id,
                    state=state,
                )

                self._last_account_state = state

            except Exception:
                self.logger.exception("[ACCOUNT LOOP ERROR]")

            time.sleep(self.funding_poll_sec)
