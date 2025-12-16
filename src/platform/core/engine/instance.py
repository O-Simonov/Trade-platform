# src/platform/core/engine/instance.py
from __future__ import annotations

import threading
import time
import uuid as _uuid
from datetime import datetime, timezone
from typing import Optional

from src.platform.core.strategy.base import Strategy
from src.platform.exchanges.base.exchange import ExchangeAdapter
from src.platform.data.storage.base import Storage
from src.platform.core.models.enums import Side
from src.platform.exchanges.binance.normalize import norm_user_event, norm_open_order
from src.platform.core.utils.idempotency import make_client_order_id
from src.platform.core.oms.oms import OrderManager
from src.platform.core.risk.risk_engine import RiskEngine, RiskLimits
from src.platform.market_state.pollers.balance_poller import BalancePoller
from src.platform.market_state.pollers.oi_poller import OpenInterestPoller


def new_pos_uid() -> str:
    return _uuid.uuid4().hex


class TradingInstance:
    """
    Orchestrator per (exchange, account, strategy).
    Responsibilities:
      - subscribe to market data + user stream
      - call Strategy.on_tick / on_candle
      - run RiskEngine checks
      - use OMS for idempotency / reconciliation
      - write snapshots / positions / orders to storage
      - v9 Market State Layer: balance + open interest pollers

    dry_run:
      - if True, NO REAL orders will be sent to exchange adapter
      - still records a DRY order row to DB to prevent repeated submits
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

        self.base_ref = base_ref or None
        self.hedge_ratio = float(hedge_ratio) if hedge_ratio is not None else None

        self.symbols = [s.upper() for s in (symbols or [])]
        self.candle_intervals = list(candle_intervals or [])

        self.funding_poll_sec = float(funding_poll_sec or 120.0)
        self.oms_reconcile_sec = float(oms_reconcile_sec or 15.0)
        self.oms_pending_timeout_sec = int(oms_pending_timeout_sec or 20)

        self.ids = ids
        self.dry_run = bool(dry_run)

        # local caches
        self._positions_cache = {}  # symbol -> Position
        self._pos_uid_by_symbol: dict[str, str] = {}

        # market-state cache (from pollers)
        self._market_state_lock = threading.Lock()
        self._latest_market_state: dict | None = None

        # core components
        self.risk = RiskEngine(risk_limits)
        self.oms = OrderManager(
            storage=storage,
            exchange_id=self.ids["_exchange_id"],
            account_id=self.ids["_account_id"],
        )

    # ---------------------------------------------------------------------
    # Position UID helpers
    # ---------------------------------------------------------------------

    def _load_pos_uids_from_db(self) -> None:
        ex_id = self.ids["_exchange_id"]
        acc_id = self.ids["_account_id"]
        for sym in self.symbols:
            if sym not in self.ids:
                continue
            uid = self.db.get_last_pos_uid(ex_id, acc_id, self.ids[sym], self.strategy.strategy_id)
            if uid:
                self._pos_uid_by_symbol[sym] = uid

    def _ensure_pos_uid(self, sym: str) -> str:
        uid = self._pos_uid_by_symbol.get(sym)
        if not uid:
            uid = new_pos_uid()
            self._pos_uid_by_symbol[sym] = uid
        return uid

    def _open_positions_count(self) -> int:
        return sum(1 for p in self._positions_cache.values() if getattr(p, "qty", 0) != 0)

    # ---------------------------------------------------------------------
    # Market State (v9)
    # ---------------------------------------------------------------------

    def _on_market_state_update(self, state: dict) -> None:
        with self._market_state_lock:
            self._latest_market_state = dict(state or {})

    def _get_market_state(self) -> dict | None:
        with self._market_state_lock:
            return dict(self._latest_market_state) if self._latest_market_state else None

    # ---------------------------------------------------------------------
    # Positions refresh (REST)
    # ---------------------------------------------------------------------

    def refresh_positions(self) -> None:
        """
        Pulls positions from exchange, updates local cache, and upserts DB positions rows
        (including v9.2 extended columns if available on Position model).
        """
        pos = self.ex.fetch_positions(self.account)
        self._positions_cache = {p.symbol: p for p in pos}

        # assign pos_uid to new non-flat symbols
        for sym, p in self._positions_cache.items():
            if sym not in self._pos_uid_by_symbol and getattr(p, "qty", 0) != 0:
                self._pos_uid_by_symbol[sym] = new_pos_uid()

        now = datetime.now(timezone.utc)
        rows: list[dict] = []

        for sym in self.symbols:
            if sym not in self.ids:
                continue

            symbol_id = self.ids[sym]
            p = self._positions_cache.get(sym)
            pos_uid = self._pos_uid_by_symbol.get(sym)

            if not p:
                rows.append(
                    {
                        "exchange_id": self.ids["_exchange_id"],
                        "account_id": self.ids["_account_id"],
                        "symbol_id": symbol_id,
                        "strategy_id": self.strategy.strategy_id,
                        "pos_uid": pos_uid,
                        "side": Side.FLAT.value,
                        "qty": 0.0,
                        "entry_price": None,
                        "avg_price": None,
                        "exit_price": None,
                        "mark_price": None,
                        "position_value_usdt": None,
                        "leverage": None,
                        "stop_loss_1": None,
                        "stop_loss_2": None,
                        "take_profit_1": None,
                        "take_profit_2": None,
                        "take_profit_3": None,
                        "scale_in_count": None,
                        "strategy_name": self.strategy.strategy_id,
                        "status": None,
                        "opened_at": None,
                        "closed_at": None,
                        "realized_pnl": None,
                        "unrealized_pnl": None,
                        "updated_at": now,
                        "source": "rest",
                    }
                )

                # v9.2 fallback: if REST shows FLAT, try to close open ledger row
                if pos_uid:
                    try:
                        self.db.close_position_ledger_if_open(
                            exchange_id=self.ids["_exchange_id"],
                            account_id=self.ids["_account_id"],
                            pos_uid=pos_uid,
                            closed_at=now,
                            exit_price=None,
                            source="rest",
                        )
                    except Exception:
                        pass
                continue

            qty = float(getattr(p, "qty", 0.0) or 0.0)
            side_val = getattr(getattr(p, "side", None), "value", None) or str(getattr(p, "side", "FLAT"))

            # best-effort extended fields (your Position model already contains many of these)
            entry_price = getattr(p, "entry_price", None)
            avg_price = getattr(p, "avg_price", None) or entry_price
            exit_price = getattr(p, "exit_price", None)
            mark_price = getattr(p, "mark_price", None)
            leverage = getattr(p, "leverage", None)

            # compute notional fallback if exchange adapter did not provide position_value_usdt
            pos_value_usdt = getattr(p, "position_value_usdt", None)
            if pos_value_usdt is None:
                px = avg_price or mark_price or entry_price or 0.0
                try:
                    pos_value_usdt = abs(qty) * float(px)
                except Exception:
                    pos_value_usdt = None

            rows.append(
                {
                    "exchange_id": self.ids["_exchange_id"],
                    "account_id": self.ids["_account_id"],
                    "symbol_id": symbol_id,
                    "strategy_id": self.strategy.strategy_id,
                    "pos_uid": pos_uid,
                    "side": side_val,
                    "qty": qty,
                    "entry_price": entry_price,
                    "avg_price": avg_price,
                    "exit_price": exit_price,
                    "mark_price": mark_price,
                    "position_value_usdt": pos_value_usdt,
                    "leverage": leverage,
                    "stop_loss_1": getattr(p, "stop_loss_1", None),
                    "stop_loss_2": getattr(p, "stop_loss_2", None),
                    "take_profit_1": getattr(p, "take_profit_1", None),
                    "take_profit_2": getattr(p, "take_profit_2", None),
                    "take_profit_3": getattr(p, "take_profit_3", None),
                    "scale_in_count": getattr(p, "scale_in_count", None),
                    "strategy_name": (getattr(p, "strategy_name", None) or self.strategy.strategy_id),
                    "status": getattr(p, "status", None),
                    "opened_at": datetime.fromtimestamp(getattr(p, "opened_at_ts", 0) or 0, tz=timezone.utc)
                    if getattr(p, "opened_at_ts", None)
                    else None,
                    "closed_at": datetime.fromtimestamp(getattr(p, "closed_at_ts", 0) or 0, tz=timezone.utc)
                    if getattr(p, "closed_at_ts", None)
                    else None,
                    "realized_pnl": getattr(p, "realized_pnl", None),
                    "unrealized_pnl": getattr(p, "unrealized_pnl", None),
                    "updated_at": now,
                    "source": "rest",
                }
            )

        if rows:
            self.db.upsert_positions(rows)

    # ---------------------------------------------------------------------
    # DRY RUN order recording (prevents repeated submits)
    # ---------------------------------------------------------------------

    def _record_dry_order(self, *, client_order_id: str, intent, symbol: str, pos_uid: str, price: float) -> None:
        """
        Inserts an order row with client_order_id so OMS should_submit() becomes false next time.
        """
        now = datetime.now(timezone.utc)
        try:
            side_val = getattr(getattr(intent, "side", None), "value", str(getattr(intent, "side", "")))
        except Exception:
            side_val = str(getattr(intent, "side", ""))

        row = {
            "exchange_id": self.ids["_exchange_id"],
            "account_id": self.ids["_account_id"],
            "order_id": f"DRY:{client_order_id}",
            "symbol_id": self.ids[symbol],
            "strategy_id": self.strategy.strategy_id,
            "pos_uid": pos_uid,
            "client_order_id": client_order_id,
            "side": side_val,
            "type": getattr(intent, "order_type", None) or getattr(intent, "type", None),
            "reduce_only": bool(getattr(intent, "reduce_only", False)),
            "price": float(getattr(intent, "price", None) or price),
            "qty": float(getattr(intent, "qty", 0.0) or 0.0),
            "filled_qty": 0.0,
            "status": "DRY_RUN",
            "created_at": now,
            "updated_at": now,
            "source": "dry_run",
        }
        try:
            self.db.upsert_orders([row])
        except Exception as e:
            print(f"[DRY_RUN] failed to record dry order: {e}")

    # ---------------------------------------------------------------------
    # Events
    # ---------------------------------------------------------------------

    def on_tick(self, symbol: str, price: float) -> None:
        now = datetime.now(timezone.utc)

        # snapshot mark price
        self.db.insert_snapshots(
            [
                {
                    "exchange_id": self.ids["_exchange_id"],
                    "symbol_id": self.ids[symbol],
                    "price": float(price),
                    "price_type": "mark",
                    "ts": now,
                    "source": "ws_markPrice",
                }
            ]
        )

        position = self._positions_cache.get(symbol)
        pos_uid = self._ensure_pos_uid(symbol)

        intent = self.strategy.on_tick(symbol, price, position, pos_uid)
        if not intent:
            return

        cid = make_client_order_id(
            intent.exchange,
            intent.account,
            intent.symbol,
            getattr(intent.side, "value", str(intent.side)),
            f"{intent.qty:.8f}",
            pos_uid,
            intent.intent_type,
        )
        intent = type(intent)(**{**intent.__dict__, "client_order_id": cid, "pos_uid": pos_uid})

        # OMS idempotency
        if not self.oms.should_submit(cid):
            return

        # Risk checks
        day_pnl = self.db.get_today_realized_pnl(self.ids["_exchange_id"], self.ids["_account_id"])
        mstate = self._get_market_state()

        ok, reason = self.risk.allow_order(
            intent=intent,
            price=float(price),
            open_positions_count=self._open_positions_count(),
            current_day_realized_pnl=float(day_pnl),
            market_state=mstate,
        )
        if not ok:
            print(f"[RISK BLOCK] {intent.symbol} {reason}")
            return

        # record OMS placeholder (so we don't double-submit if crash)
        self.oms.record_pending_submit(
            client_order_id=cid,
            symbol_id=self.ids[symbol],
            strategy_id=self.strategy.strategy_id,
            pos_uid=pos_uid,
            intent=intent,
        )

        # DRY_RUN: do NOT send order to exchange
        if self.dry_run:
            print(f"[DRY_RUN] suppress order: {intent.symbol} {getattr(intent.side,'value',intent.side)} qty={intent.qty}")
            # store a DRY order row so OMS should_submit() stays false on next ticks
            self._record_dry_order(client_order_id=cid, intent=intent, symbol=symbol, pos_uid=pos_uid, price=float(price))
            return

        # REAL RUN
        self.ex.place_order(intent)

        # Hedge linking
        if self.role == "HEDGE" and not getattr(intent, "reduce_only", False):
            self._maybe_link_hedge(symbol, pos_uid)

        # refresh
        self.refresh_positions()

    def on_candle(self, candle: dict) -> None:
        sym = candle["symbol"]
        if sym not in self.ids:
            return

        self.db.upsert_candles(
            [
                {
                    "exchange_id": self.ids["_exchange_id"],
                    "symbol_id": self.ids[sym],
                    "interval": candle["interval"],
                    "open_time": candle["open_time"],
                    "open": candle["open"],
                    "high": candle["high"],
                    "low": candle["low"],
                    "close": candle["close"],
                    "volume": candle["volume"],
                    "source": candle.get("source", "ws_kline"),
                }
            ]
        )

    def on_user_event(self, raw: dict) -> None:
        """
        User stream events from exchange.
        We normalize them (binance) and pass into OMS / ledger ingestion.
        """
        try:
            ev = norm_user_event(raw)
        except Exception:
            return

        # NOTE: exact OMS/ledger integration depends on your current OMS implementation.
        # Keep existing behavior: delegate to OMS if it has the method(s).
        try:
            self.oms.on_user_event(ev, ids=self.ids, strategy_id=self.strategy.strategy_id)
        except Exception:
            pass

        # after events, refresh positions (best effort)
        try:
            self.refresh_positions()
        except Exception:
            pass

    # ---------------------------------------------------------------------
    # Hedge linking
    # ---------------------------------------------------------------------

    def _maybe_link_hedge(self, sym: str, hedge_pos_uid: str) -> None:
        if self.role != "HEDGE" or not self.base_ref:
            return
        ex_id = self.ids["_exchange_id"]
        base_account_id = self.base_ref.get("account_id")
        if not base_account_id:
            return
        base_uid = self.db.get_latest_open_pos_uid(ex_id, base_account_id, self.ids[sym], self.strategy.strategy_id)
        if not base_uid:
            return
        self.db.upsert_hedge_link(
            {
                "exchange_id": ex_id,
                "base_account_id": base_account_id,
                "hedge_account_id": self.ids["_account_id"],
                "symbol_id": self.ids[sym],
                "base_pos_uid": base_uid,
                "hedge_pos_uid": hedge_pos_uid,
                "hedge_ratio": self.hedge_ratio,
                "created_at": datetime.now(timezone.utc),
            }
        )

    # ---------------------------------------------------------------------
    # Background loops
    # ---------------------------------------------------------------------

    def _funding_loop(self) -> None:
        while True:
            try:
                # best-effort: if adapter supports funding, store it (not fatal if missing)
                if hasattr(self.ex, "fetch_funding_rates"):
                    rows = self.ex.fetch_funding_rates(self.account, self.symbols)
                    if rows:
                        self.db.upsert_funding_rates(rows)
            except Exception as e:
                print(f"[FundingLoop {self.account}] error: {e}")
            time.sleep(self.funding_poll_sec)

    def _oms_reconcile_loop(self) -> None:
        while True:
            try:
                # expire stuck pending placeholders
                n_exp = self.oms.reconcile_pending_timeouts(self.oms_pending_timeout_sec)
                if n_exp:
                    print(f"[OMS] expired pending: {n_exp}")

                # fetch open orders (REST) and normalize
                open_raw = self.ex.fetch_open_orders(self.account, symbol=None)
                open_norm = [norm_open_order(x) for x in open_raw]

                # resolve placeholders with open orders
                n_res = self.oms.reconcile_open_orders(open_norm)
                if n_res:
                    print(f"[OMS] resolved placeholders: {n_res}")

                # upsert open orders statuses (also keeps DB fresh)
                rows = []
                for oo in open_norm:
                    sym = oo["symbol"]
                    if sym not in self.ids:
                        continue
                    rows.append(
                        {
                            "exchange_id": self.ids["_exchange_id"],
                            "account_id": self.ids["_account_id"],
                            "order_id": oo["order_id"],
                            "symbol_id": self.ids[sym],
                            "strategy_id": self.strategy.strategy_id,
                            "pos_uid": self._pos_uid_by_symbol.get(sym),
                            "client_order_id": oo.get("client_order_id"),
                            "side": oo.get("side"),
                            "type": oo.get("type"),
                            "reduce_only": None,
                            "price": oo.get("price"),
                            "qty": oo.get("qty"),
                            "filled_qty": oo.get("filled_qty"),
                            "status": oo.get("status") or "OPEN",
                            "created_at": oo.get("created_at"),
                            "updated_at": datetime.now(timezone.utc),
                            "source": "rest_openOrders",
                        }
                    )
                if rows:
                    self.db.upsert_orders(rows)

            except Exception as e:
                print(f"[OMS reconcile {self.account}] error: {e}")

            time.sleep(self.oms_reconcile_sec)

    # ---------------------------------------------------------------------
    # Run
    # ---------------------------------------------------------------------

    def run(self) -> None:
        self._load_pos_uids_from_db()
        self.refresh_positions()

        # subscriptions
        self.ex.subscribe_user_stream(self.account, self.on_user_event)
        self.ex.subscribe_ticks(self.account, self.symbols, lambda sym, px: self.on_tick(sym, px))
        if self.candle_intervals:
            self.ex.subscribe_candles(self.account, self.symbols, self.candle_intervals, self.on_candle)

        # background workers
        threading.Thread(target=self._funding_loop, daemon=True, name=f"FundingLoop-{self.account}").start()
        threading.Thread(target=self._oms_reconcile_loop, daemon=True, name=f"OMSReconcile-{self.account}").start()

        # --- v9 Market State Layer pollers ---
        BalancePoller(
            exchange=self.ex,
            storage=self.db,
            exchange_id=self.ids["_exchange_id"],
            account_id=self.ids["_account_id"],
            account=self.account,
            poll_sec=30.0,
            on_update=self._on_market_state_update,
        ).start()

        OpenInterestPoller(
            exchange=self.ex,
            storage=self.db,
            exchange_id=self.ids["_exchange_id"],
            account=self.account,
            symbol_ids={s: self.ids[s] for s in self.symbols if s in self.ids},
            intervals=["5m", "15m", "1h"],
            poll_sec=300.0,
        ).start()

        # keep thread alive
        while True:
            time.sleep(60.0)
