# src/platform/core/engine/instance.py
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import List, Optional, Any

from src.platform.core.models.order import OrderIntent
from src.platform.core.strategy.base import Strategy
from src.platform.core.risk.risk_engine import RiskLimits
from src.platform.exchanges.base.exchange import ExchangeAdapter
from src.platform.data.storage.postgres.storage import PostgreSQLStorage

from src.platform.core.position.position_manager import PositionManager
from src.platform.core.oms.oms import OrderManager
from src.platform.core.oms.parser import parse_binance_user_event
from src.platform.core.oms.events import TradeEvent, OrderEvent

from src.platform.core.position.position_reconciler import PositionReconciler
from src.platform.core.position.position_flusher import PositionFlusher


class TradingInstance:
    """
    Trading runtime (K6.9):

      Strategy → OMS → Exchange
      WS user events → OMS → DB (order_events, trades, fills)
      WS ticks → PositionManager (uPnL)
      REST reconcile → PositionManager (NOT in DRY_RUN)
      Account state → DB (account_state + account_balance_snapshots)

    Key fixes:
      ✔ ACCOUNT_UPDATE handled from WS (preferred)
      ✔ REST account polling is fallback only + backoff on 429/-1003
      ✔ In DRY_RUN we skip REST position reconcile (avoids 429 storms)
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
        balance_snapshot_sec: float = 120.0,
        oms_reconcile_sec: float = 15.0,
        oms_pending_timeout_sec: float = 20.0,
        positions_reconcile_sec: float = 30.0,
        positions_flush_sec: float = 30.0,
        base_ref: Optional[str] = None,
        hedge_ratio: Optional[float] = None,
    ) -> None:
        self.logger = logging.getLogger("src.platform.core.engine.instance")

        # identity
        self.exchange = exchange
        self.account = str(account)
        self.role = str(role)
        self.symbols = list(symbols)
        self.strategy = strategy
        self.candle_intervals = list(candle_intervals)

        # infra
        self.storage = storage
        self.ids = dict(ids)

        self.exchange_id = int(self.ids["_exchange_id"])
        self.account_id = int(self.ids.get("_account_id", 0) or 0)
        if self.account_id <= 0:
            self.logger.warning(
                "[Instance] account_id is not set in ids; got=%s (account=%s). "
                "Some joins/meta may be degraded.",
                self.account_id,
                self.account,
            )

        # symbol ids: ids dict uses exact symbol keys
        self.symbol_ids = {s.upper(): int(self.ids[s]) for s in self.symbols if s in self.ids}

        # meta caches (order_id/client_order_id → strategy_id/pos_uid)
        self._order_meta_by_cid: dict[str, dict] = {}
        self._order_meta_by_oid: dict[str, dict] = {}

        # risk / flags
        self.risk_limits = risk_limits
        self.dry_run = bool(dry_run)
        self.base_ref = base_ref
        self.hedge_ratio = hedge_ratio

        # timing
        self.funding_poll_sec = float(funding_poll_sec)
        # how often we write account_balance_snapshots (history)
        self.balance_snapshot_sec = float(balance_snapshot_sec)
        self.oms_reconcile_sec = float(oms_reconcile_sec)
        self.oms_pending_timeout_sec = float(oms_pending_timeout_sec)
        self.positions_reconcile_sec = float(positions_reconcile_sec)
        self.positions_flush_sec = float(positions_flush_sec)

        # runtime
        self._running = False
        self._stop = threading.Event()
        self._last_idle_log = 0.0
        self._last_balance_snapshot_ts = 0.0

        # account REST backoff (429/-1003)
        self._acct_rate_limit_until = 0.0
        self._acct_backoff_sec = 30.0
        self._acct_backoff_max = 300.0

        # WS account updates tracking (preferred)
        self._last_ws_account_update_ts = 0.0
        self._prefer_ws_account = True
        self._ws_account_stale_sec = 180.0  # if WS silent > 3 min → allow REST fallback

        # components
        self.position_manager = PositionManager(
            storage=self.storage,
            exchange_id=self.exchange_id,
            account_id=self.account_id,
            logger=logging.getLogger("positions.manager"),
        )

        self.oms = OrderManager(
            storage=self.storage,
            exchange_id=self.exchange_id,
            account_id=self.account_id,
            position_manager=self.position_manager,
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
            "[Instance] running account=%s role=%s symbols=%s dry_run=%s",
            self.account,
            self.role,
            ",".join(self.symbols),
            self.dry_run,
        )

        self._running = True

        # Market data
        self.exchange.subscribe_ticks(
            account=self.account,
            symbols=self.symbols,
            cb=self._on_tick,
        )

        # User stream (needed even in DRY_RUN to record events + ACCOUNT_UPDATE)
        self.exchange.subscribe_user_stream(
            account=self.account,
            cb=self._on_user_event,
        )

        # Candles (optional)
        if self.candle_intervals:
            self.exchange.subscribe_candles(
                account=self.account,
                symbols=self.symbols,
                intervals=self.candle_intervals,
                cb=self._on_candle,
            )

        # Background loops
        threading.Thread(target=self._oms_loop, daemon=True).start()
        threading.Thread(target=self._account_loop, daemon=True).start()

        # IMPORTANT: do NOT REST-reconcile positions in DRY_RUN (avoids Binance 429)
        if not self.dry_run:
            threading.Thread(target=self._position_reconcile_loop, daemon=True).start()
        else:
            self.logger.info("[RECONCILE] skipped (dry_run=True)")

        threading.Thread(target=self._position_flush_loop, daemon=True).start()

        # Strategy start
        self.strategy.on_start()

        while self._running and not self._stop.is_set():
            self._drain_intents()

            now = time.time()
            if now - self._last_idle_log >= 10.0:
                self.logger.info("[Instance] idle (alive)")
                self._last_idle_log = now

            time.sleep(0.05)

    # ============================================================
    # WS callbacks
    # ============================================================

    def _on_tick(self, symbol: str, price: float) -> None:
        sid = self.symbol_ids.get(symbol.upper())
        if sid:
            self.position_manager.on_mark_price(symbol_id=sid, price=price)
        self.strategy.on_tick(symbol=symbol, price=price)

    def _on_candle(self, candle: dict) -> None:
        if hasattr(self.strategy, "on_candle"):
            self.strategy.on_candle(candle=candle)

    def _handle_ws_account_update(self, event: dict) -> None:
        """
        Binance Futures USER WS: ACCOUNT_UPDATE
        Preferred source of balances (avoid REST polling / 429).
        Writes:
          - account_state (latest)
          - account_balance_snapshots (history, throttled)
        """
        try:
            payload = event.get("a") or {}
            balances = payload.get("B") or []
            positions = payload.get("P") or []

            # USDT-M: pick USDT balance
            usdt = None
            for b in balances:
                if str(b.get("a") or "").upper() == "USDT":
                    usdt = b
                    break
            if not usdt:
                return

            wallet_balance = float(usdt.get("wb") or 0.0)  # wallet balance
            cross_wallet = usdt.get("cw")
            available_balance = float(cross_wallet) if cross_wallet is not None else wallet_balance

            # sum unrealized PnL across positions
            upnl = 0.0
            for p in positions:
                try:
                    upnl += float(p.get("up") or 0.0)
                except Exception:
                    pass

            equity = wallet_balance + upnl

            state = {
                "wallet_balance": wallet_balance,
                "equity": equity,
                "available_balance": available_balance,
                "unrealized_pnl": upnl,
            }

            # latest state
            self.storage.upsert_account_state(
                exchange_id=self.exchange_id,
                account_id=self.account_id,
                state=state,
            )

            # mark WS freshness
            self._last_ws_account_update_ts = time.time()

            # history snapshots (throttled)
            now = time.time()
            if now - self._last_balance_snapshot_ts >= self.balance_snapshot_sec:
                margin_used = max(equity - available_balance, 0.0)
                row = {
                    "exchange_id": self.exchange_id,
                    "account_id": self.account_id,
                    "ts": datetime.now(timezone.utc),
                    "wallet_balance": wallet_balance,
                    "equity": equity,
                    "available_balance": available_balance,
                    "margin_used": float(margin_used),
                    "unrealized_pnl": upnl,
                    "source": "ws_user",
                }
                self.storage.insert_account_balance_snapshots([row])
                self._last_balance_snapshot_ts = now

                self.logger.info(
                    "[BALANCE SNAPSHOT][WS] account=%s wallet=%.8f equity=%.8f avail=%.8f upnl=%.8f",
                    self.account,
                    wallet_balance,
                    equity,
                    available_balance,
                    upnl,
                )

        except Exception:
            self.logger.exception("[ACCOUNT_UPDATE][WS] parse/write failed")

    def _on_user_event(self, event: dict) -> None:
        try:
            # 0) ACCOUNT_UPDATE: handle directly (avoid REST + keep latest balances)
            if str(event.get("e") or "").upper() == "ACCOUNT_UPDATE":
                self._handle_ws_account_update(event)
                return

            events = parse_binance_user_event(
                event,
                exchange=self.exchange.name,
                account=self.account,
                symbol_ids=self.symbol_ids,
                source="ws_user",
            )

            for ev in events:
                ev.exchange_id = ev.exchange_id or self.exchange_id
                ev.account_id = ev.account_id or self.account_id

                # =========================
                # ORDER EVENTS
                # =========================
                if isinstance(ev, OrderEvent):
                    # 1) meta by client_order_id
                    cid = getattr(ev, "client_order_id", None)
                    if cid and cid in self._order_meta_by_cid:
                        meta = self._order_meta_by_cid[cid]
                        ev.strategy_id = meta.get("strategy_id")
                        ev.pos_uid = meta.get("pos_uid")

                    # 2) meta by order_id cache
                    if ev.order_id and not getattr(ev, "strategy_id", None):
                        meta = self._order_meta_by_oid.get(str(ev.order_id))
                        if meta:
                            ev.strategy_id = meta.get("strategy_id")
                            ev.pos_uid = meta.get("pos_uid")

                    # 3) snapshot from DB orders
                    if ev.order_id and not getattr(ev, "strategy_id", None):
                        snap = self.storage.get_order(
                            exchange_id=ev.exchange_id,
                            account_id=ev.account_id,
                            order_id=str(ev.order_id),
                        )
                        if snap:
                            ev.strategy_id = snap.get("strategy_id")
                            ev.pos_uid = snap.get("pos_uid")

                    # 4) cache by order_id
                    if ev.order_id and ev.strategy_id:
                        self._order_meta_by_oid[str(ev.order_id)] = {
                            "strategy_id": ev.strategy_id,
                            "pos_uid": ev.pos_uid,
                        }

                    # 5) OMS
                    self.oms.apply_event(ev)

                    # 6) immutable log
                    self.storage.upsert_order_event(ev)
                    # 7) snapshot orders (derived current state)
                    self.storage.upsert_orders([ev.to_row()])

                # =========================
                # TRADE EVENTS
                # =========================
                elif isinstance(ev, TradeEvent):
                    ev.ts_ms = ev.ts_ms or int(time.time() * 1000)

                    # try cache by order_id
                    meta = self._order_meta_by_oid.get(str(ev.order_id))
                    if meta:
                        ev.strategy_id = meta.get("strategy_id")
                        ev.pos_uid = meta.get("pos_uid")
                    else:
                        # fallback: read snapshot orders from DB
                        snap = None
                        try:
                            if ev.order_id:
                                snap = self.storage.get_order(
                                    exchange_id=ev.exchange_id,
                                    account_id=ev.account_id,
                                    order_id=str(ev.order_id),
                                )
                        except Exception:
                            snap = None

                        if snap:
                            ev.strategy_id = snap.get("strategy_id") or "unknown"
                            ev.pos_uid = snap.get("pos_uid")
                            # warm cache
                            self._order_meta_by_oid[str(ev.order_id)] = {
                                "strategy_id": ev.strategy_id,
                                "pos_uid": ev.pos_uid,
                            }
                        else:
                            ev.strategy_id = "unknown"
                            ev.pos_uid = None

                    # persist
                    self.storage.upsert_trades([ev.to_row()])
                    self.storage.insert_order_fills([ev])

                    # position manager (WS-primary)
                    self.position_manager.on_trade_event(ev)

                    # reconciler hold-off after our fills (prevents immediate “heal” after trade)
                    try:
                        sid = getattr(ev, "symbol_id", None)
                        if sid:
                            self.position_reconciler.on_trade_event(
                                exchange_id=ev.exchange_id,
                                account_id=ev.account_id,
                                symbol_id=int(sid),
                                ts=float(ev.ts_ms) / 1000.0,
                            )
                    except Exception:
                        pass

        except Exception:
            self.logger.exception("[USER_STREAM ERROR]")

    # ============================================================
    # intents → OMS → exchange
    # ============================================================

    def _drain_intents(self) -> None:
        for intent in self.strategy.get_intents() or []:
            self._process_intent(intent)

    def _process_intent(self, intent: OrderIntent) -> None:
        # DRY_RUN: do not place real orders
        if self.dry_run:
            return

        sid = self.symbol_ids.get((intent.symbol or "").upper())
        if not sid:
            return

        intent.symbol_id = sid
        intent.exchange = self.exchange.name
        intent.account = self.account

        intent2 = self.oms.resolve_intent(intent)
        if not intent2:
            return

        cid = str(intent2.client_order_id)
        if not self.oms.should_submit(cid):
            return

        strategy_id = (
            getattr(self.strategy, "strategy_id", None)
            or getattr(self.strategy, "name", None)
            or "unknown"
        )

        pos_uid = f"{self.exchange_id}:{self.account_id}:{sid}"

        self._order_meta_by_cid[cid] = {"strategy_id": strategy_id, "pos_uid": pos_uid}

        self.oms.record_pending_submit(client_order_id=cid)
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
            try:
                self.position_reconciler.run_once()
            except Exception:
                self.logger.exception("[RECONCILE LOOP ERROR]")
            time.sleep(self.positions_reconcile_sec)

    def _position_flush_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.position_flusher.flush()
            except Exception:
                self.logger.exception("[POSITION FLUSH ERROR]")
            time.sleep(self.positions_flush_sec)

    def _account_loop(self) -> None:
        """
        Prefer balances from WS ACCOUNT_UPDATE.
        REST is fallback only, with backoff on Binance 429/-1003.
        """
        while not self._stop.is_set():
            try:
                now = time.time()

                # 1) if in backoff after 429/-1003, wait a bit and retry later
                if now < self._acct_rate_limit_until:
                    time.sleep(1.0)
                    continue

                # 2) if WS updated recently, skip REST
                if self._prefer_ws_account:
                    ws_age = now - float(self._last_ws_account_update_ts or 0.0)
                    if 0.0 <= ws_age < self._ws_account_stale_sec:
                        time.sleep(1.0)
                        continue

                # 3) REST fallback
                state = self.exchange.fetch_account_state(account=self.account)
                self.storage.upsert_account_state(
                    exchange_id=self.exchange_id,
                    account_id=self.account_id,
                    state=state,
                )

                # snapshots (history) — throttle
                if now - self._last_balance_snapshot_ts >= self.balance_snapshot_sec:
                    wb = float(state.get("wallet_balance") or 0.0)
                    eq = float(state.get("equity") or 0.0)
                    av = float(state.get("available_balance") or 0.0)
                    upnl = float(state.get("unrealized_pnl") or 0.0)

                    mu = state.get("margin_used")
                    if mu is None:
                        mu = max(eq - av, 0.0)

                    row = {
                        "exchange_id": self.exchange_id,
                        "account_id": self.account_id,
                        "ts": datetime.now(timezone.utc),
                        "wallet_balance": wb,
                        "equity": eq,
                        "available_balance": av,
                        "margin_used": float(mu or 0.0),
                        "unrealized_pnl": upnl,
                        "source": "rest",
                    }
                    self.storage.insert_account_balance_snapshots([row])
                    self._last_balance_snapshot_ts = now

                    self.logger.info(
                        "[BALANCE SNAPSHOT][REST] account=%s wallet=%.8f equity=%.8f avail=%.8f upnl=%.8f",
                        self.account,
                        wb,
                        eq,
                        av,
                        upnl,
                    )

                # success → reset backoff
                self._acct_backoff_sec = 30.0

            except Exception as e:
                msg = str(e)
                if "HTTP 429" in msg or "Too many requests" in msg or "-1003" in msg:
                    self._acct_rate_limit_until = time.time() + self._acct_backoff_sec
                    self.logger.warning(
                        "[ACCOUNT][REST] rate-limited (429/-1003). Backoff %.1fs",
                        self._acct_backoff_sec,
                    )
                    self._acct_backoff_sec = min(self._acct_backoff_sec * 2.0, self._acct_backoff_max)
                else:
                    self.logger.exception("[ACCOUNT LOOP ERROR]")

            # keep original pace for REST fallback attempts
            time.sleep(self.funding_poll_sec)
