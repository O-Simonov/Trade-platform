# src/platform/core/engine/instance.py
from __future__ import annotations

import logging
import threading
import time
import uuid as _uuid
from datetime import datetime, timezone
from typing import Any

from src.platform.core.strategy.base import Strategy
from src.platform.exchanges.base.exchange import ExchangeAdapter
from src.platform.data.storage.base import Storage
from src.platform.core.utils.idempotency import make_client_order_id
from src.platform.core.oms.oms import OrderManager
from src.platform.core.risk.risk_engine import RiskEngine, RiskLimits

from src.platform.market_state.reader import MarketStateReader
from src.platform.core.oms.writer import OmsWriter
from src.platform.core.oms.parser import parse_binance_user_event
from src.platform.core.position import PositionAggregator

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


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

    def __init__(
        self,
        *,
        exchange,
        storage,
        strategy,
        account,
        role,
        symbols,
        candle_intervals,
        funding_poll_sec,
        oms_reconcile_sec,
        oms_pending_timeout_sec,
        ids,
        risk_limits,
        base_ref=None,
        hedge_ratio=None,
        dry_run=True,
    ):
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

        # ✅ удобные алиасы
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

        # ✅ PositionAggregator
        self.pos_agg = PositionAggregator(storage=self.storage, logger_=self.logger)

        # ✅ Position Snapshot loop (DB sync)
        self._pos_snapshot_sec = 3.0
        self._pos_snapshot_only_if_changed = True
        self._pos_snapshot_last_hash: dict[tuple[int, int, int], int] = {}

        # ✅ Equity Curve loop
        self._equity_snapshot_sec = 5.0
        self._equity_snapshot_only_if_changed = True
        self._equity_last_hash: int | None = None

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

    def _total_unrealized_pnl(self) -> float:
        total = 0.0
        for (ex_id, acc_id, _sym_id), pos in list(self.pos_agg.positions.items()):
            if ex_id == self.exchange_id and acc_id == self.account_id:
                total += float(getattr(pos, "unrealized_pnl", 0.0) or 0.0)
        return float(total)

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
                self.logger.warning("[USER EVENT] Unknown symbol %s (ids=%s)", symbol, list(self.ids.keys()))
                return

            res = parse_binance_user_event(
                event,
                exchange_id=self.ids["_exchange_id"],
                account_id=self.ids["_account_id"],
                symbol_id=symbol_id,
            )
            if not res:
                return

            order_evt, trade_evt = res
            self.oms_writer.write_order(order_evt)

            if trade_evt:
                self.oms_writer.write_trade(trade_evt)
                pos = self.pos_agg.on_trade(trade_evt)
                if pos:
                    self.logger.info(
                        f"[POS] symbol_id={pos.symbol_id} side={pos.side} qty={pos.qty} "
                        f"entry={pos.entry_price} realized={pos.realized_pnl} fees={pos.fees} status={pos.status}"
                    )

        except Exception:
            self.logger.exception("[USER EVENT PARSE ERROR]")

    # ---------------- CANDLES ----------------

    def on_candle(self, candle: dict):
        """
        candle = {
            symbol, interval, open_time,
            open, high, low, close, volume, source
        }
        """
        try:
            if hasattr(self.strategy, "on_candle"):
                self.strategy.on_candle(symbol=candle["symbol"], interval=candle["interval"], candle=candle)

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
            symbol, price, "YES" if position else "NO",
        )

        # ✅ MARK PRICE -> PositionAggregator
        symbol_id = self.symbol_ids.get(symbol)
        if symbol_id:
            self.pos_agg.on_mark_price(
                exchange_id=self.exchange_id,
                account_id=self.account_id,
                symbol_id=symbol_id,
                mark_price=float(price),
            )

        intent = self.strategy.on_tick(symbol, float(price), position, pos_uid)
        if intent is None:
            return

        self.logger.info(
            "[INTENT] %s %s qty=%s reduce_only=%s type=%s",
            intent.symbol, intent.side, intent.qty, intent.reduce_only, intent.intent_type,
        )

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

        # ✅ floating pnl
        total_unreal = self._total_unrealized_pnl()

        ok, reason = self.risk.allow_order(
            intent=intent,
            price=float(price),
            open_positions_count=self._open_positions_count(),
            current_day_realized_pnl=float(day_pnl),
            market_state=balances,
            total_unrealized_pnl=total_unreal,
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

    # ---------------- POS SNAPSHOT LOOP ----------------

    def _pos_snapshot_loop(self) -> None:
        """
        Периодически пишет состояние позиций из PositionAggregator в таблицу positions.
        Не спамит: если _pos_snapshot_only_if_changed=True, пишет только при изменении хеша.
        """
        while not self._stop.is_set():
            try:
                now = _utcnow()
                rows = []

                for key, pos in list(self.pos_agg.positions.items()):
                    ex_id, acc_id, sym_id = key
                    if ex_id != self.exchange_id or acc_id != self.account_id:
                        continue

                    h = hash((
                        round(float(getattr(pos, "qty", 0.0) or 0.0), 12),
                        round(float(getattr(pos, "entry_price", 0.0) or 0.0), 12),
                        round(float(getattr(pos, "realized_pnl", 0.0) or 0.0), 8),
                        round(float(getattr(pos, "unrealized_pnl", 0.0) or 0.0), 8),
                        round(float(getattr(pos, "mark_price", 0.0) or 0.0), 12),
                        str(getattr(pos, "status", "") or ""),
                        str(getattr(pos, "last_trade_id", "") or ""),
                    ))

                    if self._pos_snapshot_only_if_changed and self._pos_snapshot_last_hash.get(key) == h:
                        continue
                    self._pos_snapshot_last_hash[key] = h

                    if hasattr(pos, "updated_at"):
                        pos.updated_at = now
                    if hasattr(pos, "source"):
                        pos.source = "snapshot"

                    rows.append(pos)

                if rows and hasattr(self.storage, "upsert_positions"):
                    self.storage.upsert_positions(rows)
                    self.logger.debug("[POS SNAPSHOT] upserted=%d", len(rows))

            except Exception as e:
                self.logger.warning("[POS SNAPSHOT] %s", e)

            time.sleep(self._pos_snapshot_sec)

    # ---------------- EQUITY CURVE LOOP ----------------

    def _equity_snapshot_loop(self) -> None:
        """
        Периодически пишет equity curve в account_balance_snapshots.
        equity = wallet_balance + total_unrealized_pnl
        """
        while not self._stop.is_set():
            try:
                now = _utcnow()
                ms = self.market_state.get_balances() or {}

                wallet = float(ms.get("wallet_balance") or ms.get("walletBalance") or 0.0)
                available = float(ms.get("available_balance") or ms.get("availableBalance") or 0.0)
                margin_used = float(ms.get("margin_used") or ms.get("marginUsed") or 0.0)

                unreal = float(self._total_unrealized_pnl() or 0.0)
                equity = wallet + unreal

                h = hash((
                    round(wallet, 8),
                    round(available, 8),
                    round(margin_used, 8),
                    round(unreal, 8),
                    round(equity, 8),
                ))

                if self._equity_snapshot_only_if_changed and self._equity_last_hash == h:
                    time.sleep(self._equity_snapshot_sec)
                    continue
                self._equity_last_hash = h

                if hasattr(self.storage, "insert_account_balance_snapshots"):
                    self.storage.insert_account_balance_snapshots([{
                        "exchange_id": self.exchange_id,
                        "account_id": self.account_id,
                        "ts": now,
                        "wallet_balance": wallet,
                        "equity": equity,
                        "available_balance": available,
                        "margin_used": margin_used,
                        "unrealized_pnl": unreal,
                        "source": "snapshot",
                    }])
                    self.logger.debug(
                        "[EQUITY] wallet=%.4f equity=%.4f unreal=%.4f avail=%.4f used=%.4f",
                        wallet, equity, unreal, available, margin_used
                    )

            except Exception as e:
                self.logger.warning("[EQUITY] %s", e)

            time.sleep(self._equity_snapshot_sec)

    # ---------------- RUN ----------------

    def run(self):
        logger.info(
            "[Instance] running: account=%s role=%s symbols=%s dry_run=%s",
            self.account, self.role, ",".join(self.symbols), self.dry_run,
        )

        # 🔹 0. refresh loop (positions cache)
        threading.Thread(
            target=self._market_state_refresh_loop,
            name=f"MarketStateRefresh-{self.account}",
            daemon=True,
        ).start()

        # 🔹 0.5 OMS reconcile
        threading.Thread(
            target=self._oms_reconcile_loop,
            name=f"OmsReconcile-{self.account}",
            daemon=True,
        ).start()

        # ✅ POS snapshot loop
        threading.Thread(
            target=self._pos_snapshot_loop,
            name=f"PosSnapshot-{self.account}",
            daemon=True,
        ).start()

        # ✅ Equity curve loop
        threading.Thread(
            target=self._equity_snapshot_loop,
            name=f"EquitySnapshot-{self.account}",
            daemon=True,
        ).start()

        # 🔹 1. TICKS
        self.exchange.subscribe_ticks(account=self.account, symbols=self.symbols, cb=self.on_tick)

        # 🔹 2. USER STREAM
        self.exchange.subscribe_user_stream(account=self.account, cb=self.on_user_event)

        # 🔹 3. CANDLES
        if self.candle_intervals:
            self.exchange.subscribe_candles(
                account=self.account,
                symbols=self.symbols,
                intervals=self.candle_intervals,
                cb=self.on_candle,
            )
            self.logger.info(
                "[WS] subscribe_candles account=%s symbols=%s intervals=%s",
                self.account, self.symbols, self.candle_intervals,
            )

        while not self._stop.is_set():
            time.sleep(1.0)

    def stop(self) -> None:
        self._stop.set()
