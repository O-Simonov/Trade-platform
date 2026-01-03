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
from src.platform.core.position.position_flusher import PositionFlusher


class TradingInstance:
    """
    Trading runtime (K6.8):

      WS user events  → OMS (pending) → PositionManager (authoritative trades)
      WS ticks        → PositionManager (mark price uPnL)
      REST reconcile  → PositionManager (secondary safety)
      PositionFlusher → DB persistence (ONLY here)
    """

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
        positions_reconcile_sec: float = 2.0,
        positions_flush_sec: float = 2.0,
        base_ref: Optional[str] = None,
        hedge_ratio: Optional[float] = None,
    ) -> None:
        self.logger = logging.getLogger("src.platform.core.engine.instance")

        # identity
        self.exchange = exchange
        self.account = account
        self.role = role
        self.symbols = list(symbols)
        self.strategy = strategy
        self.candle_intervals = list(candle_intervals)

        # infra / ids
        self.storage = storage
        self.ids = dict(ids)

        self.exchange_id = int(self.ids["_exchange_id"])
        self.account_id = int(self.ids.get("_account_id", 0))

        # registry: "LTCUSDT" -> symbol_id
        self.symbol_ids: dict[str, int] = {
            s.upper(): int(self.ids[s]) for s in self.symbols if s in self.ids
        }

        # risk / flags
        self.risk_limits = risk_limits
        self.dry_run = bool(dry_run)
        self.base_ref = base_ref
        self.hedge_ratio = hedge_ratio

        # timing
        self.funding_poll_sec = float(funding_poll_sec)
        self.oms_reconcile_sec = float(oms_reconcile_sec)
        self.oms_pending_timeout_sec = float(oms_pending_timeout_sec)
        self.positions_reconcile_sec = float(positions_reconcile_sec)
        self.positions_flush_sec = float(positions_flush_sec)

        # runtime state
        self._running = False
        self._stop = threading.Event()
        self._last_idle_log = 0.0
        self._last_account_state: Optional[dict] = None

        # ------------------------------------------------------------
        # Positions (create FIRST, OMS depends on it)
        # ------------------------------------------------------------
        self.position_manager = PositionManager(
            storage=self.storage,
            exchange_id=self.exchange_id,
            account_id=self.account_id,
            logger=logging.getLogger("positions.manager"),
        )

        # ------------------------------------------------------------
        # OMS (K6.8) — needs position_manager
        # ------------------------------------------------------------
        self.oms = OrderManager(
            storage=self.storage,
            exchange_id=self.exchange_id,
            account_id=self.account_id,
            position_manager=self.position_manager,  # ✅ FIX (иначе TypeError)
        )

        self.position_reconciler = PositionReconciler(
            exchange=self.exchange,
            position_manager=self.position_manager,
            storage=self.storage,
            exchange_id=self.exchange_id,
            account_id=self.account_id,
            account=self.account,
            symbol_ids=self.symbol_ids,
            logger=logging.getLogger("positions.reconciler"),
        )

        self.position_flusher = PositionFlusher(
            position_manager=self.position_manager,
            storage=self.storage,
            logger=logging.getLogger("positions.flusher"),
        )

    # ============================================================
    # lifecycle
    # ============================================================

    def run(self) -> None:
        self.logger.info(
            "[Instance] running: account=%s role=%s symbols=%s dry_run=%s",
            self.account,
            self.role,
            ",".join(self.symbols),
            self.dry_run,
        )

        self._running = True

        # WS ticks (safe)
        self.exchange.subscribe_ticks(
            account=self.account,
            symbols=self.symbols,
            cb=self._on_tick,
        )

        # ✅ В DRY_RUN НЕ лезем за listenKey (REST), иначе при бане IP всё падает
        if not self.dry_run:
            self.exchange.subscribe_user_stream(
                account=self.account,
                cb=self._on_user_event,
            )
        else:
            self.logger.warning("[DRY_RUN] skip user_stream subscribe (listenKey requires REST)")

        # candles тоже WS (safe)
        if self.candle_intervals:
            self.exchange.subscribe_candles(
                account=self.account,
                symbols=self.symbols,
                intervals=self.candle_intervals,
                cb=self._on_candle,
            )

        # background loops
        threading.Thread(target=self._oms_loop, daemon=True).start()

        # ✅ Эти циклы — REST. В dry_run лучше не запускать, чтобы не ловить 429/418.
        if not self.dry_run:
            threading.Thread(target=self._account_loop, daemon=True).start()
            threading.Thread(target=self._position_reconcile_loop, daemon=True).start()
        else:
            self.logger.warning("[DRY_RUN] skip REST loops (account/reconcile)")

        # flush loop не REST
        threading.Thread(target=self._position_flush_loop, daemon=True).start()

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

        # final flush
        try:
            self.position_flusher.flush()
        except Exception:
            self.logger.exception("[POSITIONS][FINAL FLUSH ERROR]")

    # ============================================================
    # WS callbacks
    # ============================================================

    def _on_tick(self, symbol: str, price: float) -> None:
        symbol_id = self.symbol_ids.get(symbol.upper())
        if not symbol_id:
            return
        self.position_manager.on_mark_price(symbol_id=symbol_id, price=price)

        # стратегию тоже кормим тиком (если нужно)
        self.strategy.on_tick(symbol=symbol, price=price)

    def _on_candle(self, candle: dict) -> None:
        if hasattr(self.strategy, "on_candle"):
            self.strategy.on_candle(candle=candle)

    def _on_user_event(self, event: dict) -> None:
        try:
            events = parse_binance_user_event(
                event,
                exchange=self.exchange.name,
                account=self.account,
            )

            for ev in events:
                # OMS снимает pending по ACK/FILL/CANCEL
                self.oms.apply_event(ev)

                # Positions строим только из TradeEvent
                if not isinstance(ev, TradeEvent):
                    continue

                if not ev.symbol_id:
                    ev.symbol_id = self.symbol_ids.get((ev.symbol or "").upper(), 0)

                if not ev.symbol_id:
                    self.logger.debug(
                        "[USER_STREAM] skip trade without symbol_id symbol=%s",
                        getattr(ev, "symbol", None),
                    )
                    continue

                if not ev.ts_ms:
                    ev.ts_ms = int(time.time() * 1000)

                self.position_manager.on_trade_event(ev)

        except Exception:
            self.logger.exception("[USER_STREAM ERROR]")

    # ============================================================
    # intents → OMS → Exchange
    # ============================================================

    def _drain_intents(self) -> None:
        for intent in self.strategy.get_intents() or []:
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

        # set symbol_id
        sym = (intent.symbol or "").upper()
        sid = int(self.symbol_ids.get(sym, 0) or 0)
        if sid <= 0:
            self.logger.warning("[INTENT] symbol_id not found for symbol=%s", intent.symbol)
            return
        intent.symbol_id = sid
        intent.account = self.account
        intent.exchange = self.exchange.name

        # normalize with OMS
        intent2 = self.oms.resolve_intent(intent)
        if not intent2:
            return

        if not self.oms.should_submit(intent2.client_order_id):
            return

        self.oms.record_pending_submit(client_order_id=intent2.client_order_id)

        self.exchange.place_order(intent2)

    # ============================================================
    # background loops
    # ============================================================

    def _oms_loop(self) -> None:
        while not self._stop.is_set():
            self.oms.reconcile_pending_timeouts(self.oms_pending_timeout_sec)
            time.sleep(self.oms_reconcile_sec)

    def _position_reconcile_loop(self) -> None:
        while not self._stop.is_set():
            self.position_reconciler.run_once()
            time.sleep(self.positions_reconcile_sec)

    def _position_flush_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.position_flusher.flush()
            except Exception:
                self.logger.exception("[POSITIONS][FLUSH LOOP ERROR]")
            time.sleep(self.positions_flush_sec)

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
