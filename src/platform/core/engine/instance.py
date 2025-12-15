from __future__ import annotations
import threading, time, uuid as _uuid
from datetime import datetime, timezone
from platform.core.strategy.base import Strategy
from platform.exchanges.base.exchange import ExchangeAdapter
from platform.data.storage.base import Storage
from platform.core.models.enums import Side
from platform.exchanges.binance.normalize import norm_user_event, norm_open_order
from platform.core.utils.idempotency import make_client_order_id
from platform.core.oms.oms import OrderManager
from platform.core.risk.risk_engine import RiskEngine, RiskLimits
from platform.market_state.pollers.balance_poller import BalancePoller
from platform.market_state.pollers.oi_poller import OpenInterestPoller

def new_pos_uid() -> str:
    return _uuid.uuid4().hex

class TradingInstance:
    def __init__(self, *, exchange: ExchangeAdapter, storage: Storage, strategy: Strategy,
                 account: str, role: str,
                 symbols: list[str], candle_intervals: list[str],
                 funding_poll_sec: float, oms_reconcile_sec: float, oms_pending_timeout_sec: float,
                 ids: dict, risk_limits: RiskLimits,
                 base_ref: dict | None = None, hedge_ratio: float | None = None):
        self.ex = exchange
        self.db = storage
        self.strategy = strategy
        self.account = account
        self.role = (role or "MIXED").upper()
        self.base_ref = base_ref or None
        self.hedge_ratio = float(hedge_ratio) if hedge_ratio is not None else None

        self.symbols = [s.upper() for s in symbols]
        self.candle_intervals = list(candle_intervals or [])
        self.funding_poll_sec = float(funding_poll_sec or 120.0)
        self.oms_reconcile_sec = float(oms_reconcile_sec or 15.0)
        self.oms_pending_timeout_sec = int(oms_pending_timeout_sec or 20)
        self.ids = ids

        self._positions_cache = {}
        self._pos_uid_by_symbol: dict[str, str] = {}

        self._market_state_lock = threading.Lock()
        self._latest_market_state: dict | None = None

        self.risk = RiskEngine(risk_limits)
        self.oms = OrderManager(storage=storage, exchange_id=self.ids["_exchange_id"], account_id=self.ids["_account_id"])

    def _load_pos_uids_from_db(self):
        ex_id = self.ids["_exchange_id"]
        acc_id = self.ids["_account_id"]
        for sym in self.symbols:
            uid = self.db.get_last_pos_uid(ex_id, acc_id, self.ids[sym], self.strategy.strategy_id)
            if uid:
                self._pos_uid_by_symbol[sym] = uid

    def refresh_positions(self):
        pos = self.ex.fetch_positions(self.account)
        self._positions_cache = {p.symbol: p for p in pos}
        for sym, p in self._positions_cache.items():
            if sym not in self._pos_uid_by_symbol and p.qty != 0:
                self._pos_uid_by_symbol[sym] = new_pos_uid()

        now = datetime.now(timezone.utc)
        rows = []
        for sym in self.symbols:
            symbol_id = self.ids[sym]
            p = self._positions_cache.get(sym)
            pos_uid = self._pos_uid_by_symbol.get(sym)
            if not p:
                rows.append({
                    "exchange_id": self.ids["_exchange_id"],
                    "account_id": self.ids["_account_id"],
                    "symbol_id": symbol_id,
                    "strategy_id": self.strategy.strategy_id,
                    "pos_uid": pos_uid,
                    "side": Side.FLAT.value,
                    "qty": 0.0,
                    "entry_price": None,
                    "mark_price": None,
                    "unrealized_pnl": None,
                    "updated_at": now,
                    "source": "rest",
                })

                # v9.2: if REST shows FLAT, close any open ledger row (fallback when WS fill missed)
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
            else:
                rows.append({
                    "exchange_id": self.ids["_exchange_id"],
                    "account_id": self.ids["_account_id"],
                    "symbol_id": symbol_id,
                    "strategy_id": self.strategy.strategy_id,
                    "pos_uid": pos_uid,
                    "side": p.side.value,
                    "qty": p.qty,
                    "entry_price": p.entry_price,
                    "avg_price": (p.avg_price or p.entry_price),
                    "exit_price": p.exit_price,
                    "mark_price": p.mark_price,
                    "position_value_usdt": (p.position_value_usdt if p.position_value_usdt is not None else (p.qty * float((p.avg_price or p.mark_price or p.entry_price or 0.0)))),
                    "leverage": p.leverage,
                    "stop_loss_1": p.stop_loss_1,
                    "stop_loss_2": p.stop_loss_2,
                    "take_profit_1": p.take_profit_1,
                    "take_profit_2": p.take_profit_2,
                    "take_profit_3": p.take_profit_3,
                    "scale_in_count": p.scale_in_count,
                    "strategy_name": (p.strategy_name or self.strategy.strategy_id),
                    "status": p.status,
                    "opened_at": datetime.fromtimestamp(p.opened_at_ts, tz=timezone.utc) if p.opened_at_ts else None,
                    "closed_at": datetime.fromtimestamp(p.closed_at_ts, tz=timezone.utc) if p.closed_at_ts else None,
                    "realized_pnl": p.realized_pnl,
                    "unrealized_pnl": p.unrealized_pnl,
                    "updated_at": now,
                    "source": "rest",
                })
        self.db.upsert_positions(rows)

    def _ensure_pos_uid(self, sym: str) -> str:
        uid = self._pos_uid_by_symbol.get(sym)
        if not uid:
            uid = new_pos_uid()
            self._pos_uid_by_symbol[sym] = uid
        return uid

    def _maybe_link_hedge(self, sym: str, hedge_pos_uid: str):
        if self.role != "HEDGE" or not self.base_ref:
            return
        ex_id = self.ids["_exchange_id"]
        base_account_id = self.base_ref.get("account_id")
        if not base_account_id:
            return
        base_uid = self.db.get_latest_open_pos_uid(ex_id, base_account_id, self.ids[sym], self.strategy.strategy_id)
        if not base_uid:
            return
        self.db.upsert_hedge_link({
            "exchange_id": ex_id,
            "base_account_id": base_account_id,
            "hedge_account_id": self.ids["_account_id"],
            "symbol_id": self.ids[sym],
            "base_pos_uid": base_uid,
            "hedge_pos_uid": hedge_pos_uid,
            "hedge_ratio": self.hedge_ratio,
            "created_at": datetime.now(timezone.utc),
        })

    def _open_positions_count(self) -> int:
        return sum(1 for p in self._positions_cache.values() if p.qty != 0)

    def on_tick(self, symbol: str, price: float):
        now = datetime.now(timezone.utc)
        self.db.insert_snapshots([{
            "exchange_id": self.ids["_exchange_id"],
            "symbol_id": self.ids[symbol],
            "price": float(price),
            "price_type": "mark",
            "ts": now,
            "source": "ws_markPrice",
        }])

        position = self._positions_cache.get(symbol)
        pos_uid = self._ensure_pos_uid(symbol)
        intent = self.strategy.on_tick(symbol, price, position, pos_uid)
        if not intent:
            return

        cid = make_client_order_id(
            intent.exchange, intent.account, intent.symbol,
            getattr(intent.side, "value", str(intent.side)),
            f"{intent.qty:.8f}", pos_uid, intent.intent_type
        )
        intent = type(intent)(**{**intent.__dict__, "client_order_id": cid, "pos_uid": pos_uid})

        if not self.oms.should_submit(cid):
            return

        day_pnl = self.db.get_today_realized_pnl(self.ids["_exchange_id"], self.ids["_account_id"])
        mstate = self._get_market_state()
        ok, reason = self.risk.allow_order(intent=intent, price=float(price),
                                           open_positions_count=self._open_positions_count(),
                                           current_day_realized_pnl=float(day_pnl), market_state=mstate)
        if not ok:
            print(f"[RISK BLOCK] {intent.symbol} {reason}")
            return

        self.oms.record_pending_submit(client_order_id=cid, symbol_id=self.ids[symbol],
                                       strategy_id=self.strategy.strategy_id, pos_uid=pos_uid, intent=intent)

        self.ex.place_order(intent)

        if self.role == "HEDGE" and not intent.reduce_only:
            self._maybe_link_hedge(symbol, pos_uid)

        self.refresh_positions()

    def on_candle(self, candle: dict):
        sym = candle["symbol"]
        if sym not in self.ids:
            return
        self.db.upsert_candles([{
            "exchange_id": self.ids["_exchange_id"],
            "symbol_id": self.ids[sym],
            "interval": candle["interval"],
            "open_time": candle["open_time"],
            "open": candle["open"],
            "high": candle["high"],
            "low": candle["low"],
            "close": candle["close"],
            "volume": candle["volume"],
            "source": candle.get("source","ws_kline"),
        }])

    def on_user_event(self, evt: dict):
        parsed = norm_user_event(evt)
        ex_id = self.ids["_exchange_id"]
        acc_id = self.ids["_account_id"]

        def _pos_uid(sym: str) -> str | None:
            return self._pos_uid_by_symbol.get(sym)

        order_row = parsed.get("order_row")
        if order_row and order_row.get("symbol") in self.ids:
            sym = order_row["symbol"]
            self.db.upsert_orders([{
                "exchange_id": ex_id,
                "account_id": acc_id,
                "order_id": order_row["order_id"],
                "symbol_id": self.ids[sym],
                "strategy_id": self.strategy.strategy_id,
                "pos_uid": _pos_uid(sym),
                "client_order_id": order_row.get("client_order_id"),
                "side": order_row.get("side"),
                "type": order_row.get("type"),
                "reduce_only": order_row.get("reduce_only"),
                "price": order_row.get("price"),
                "qty": order_row.get("qty"),
                "filled_qty": order_row.get("filled_qty"),
                "status": order_row.get("status"),
                "created_at": order_row.get("created_at"),
                "updated_at": order_row.get("updated_at"),
                "source": order_row.get("source"),
            }])

        trade_row = parsed.get("trade_row")
        if trade_row and trade_row.get("symbol") in self.ids:
            sym = trade_row["symbol"]
            self.db.upsert_trades([{
                "exchange_id": ex_id,
                "account_id": acc_id,
                "trade_id": trade_row["trade_id"],
                "order_id": trade_row.get("order_id"),
                "symbol_id": self.ids[sym],
                "strategy_id": self.strategy.strategy_id,
                "pos_uid": _pos_uid(sym),
                "side": trade_row.get("side"),
                "price": trade_row.get("price"),
                "qty": trade_row.get("qty"),
                "fee": trade_row.get("fee"),
                "fee_asset": trade_row.get("fee_asset"),
                "realized_pnl": trade_row.get("realized_pnl"),
                "ts": trade_row.get("ts"),
                "source": trade_row.get("source"),
            }])

        fill_row = parsed.get("fill_row")
        if fill_row and fill_row.get("symbol") in self.ids:
            sym = fill_row["symbol"]
            self.db.upsert_order_fills([{
                "exchange_id": ex_id,
                "account_id": acc_id,
                "fill_uid": fill_row["fill_uid"],
                "symbol_id": self.ids[sym],
                "order_id": fill_row.get("order_id"),
                "trade_id": fill_row.get("trade_id"),
                "client_order_id": fill_row.get("client_order_id"),
                "price": fill_row.get("price"),
                "qty": fill_row.get("qty"),
                "realized_pnl": fill_row.get("realized_pnl"),
                "ts": fill_row.get("ts"),
                "source": fill_row.get("source"),
            }])


            # v9.2: apply fill into position_ledger (fills-driven exit/avg/scale-in)
            try:
                fill_side = None
                if trade_row:
                    fill_side = trade_row.get("side")
                if not fill_side and order_row:
                    fill_side = order_row.get("side")

                if fill_side and pos_uid:
                    res = self.db.apply_fill_to_position_ledger(
                        exchange_id=ex_id,
                        account_id=acc_id,
                        symbol_id=self.ids[sym],
                        strategy_id=self.strategy.strategy_id,
                        strategy_name=(self.strategy.strategy_id),
                        pos_uid=pos_uid,
                        fill_side=fill_side,
                        price=float(fill_row.get("price") or 0.0),
                        qty=float(fill_row.get("qty") or 0.0),
                        realized_pnl=fill_row.get("realized_pnl"),
                        fee=None,
                        ts=fill_row.get("ts"),
                        source=fill_row.get("source") or "ws_user",
                    )
                    # If broker-side flip happens, rotate pos_uid so next fills belong to a new ledger row
                    if res and res.get("status") == "FLIPPED":
                        self._pos_uid_by_symbol[sym] = new_pos_uid()
            except Exception:
                # ledger must never break trading loop
                pass

    def _funding_loop(self):
        while True:
            try:
                rows = self.ex.fetch_funding_snapshot(self.symbols)
                out = []
                for r in rows:
                    sym = r["symbol"]
                    if sym not in self.ids:
                        continue
                    out.append({
                        "exchange_id": self.ids["_exchange_id"],
                        "symbol_id": self.ids[sym],
                        "funding_time": r["funding_time"],
                        "funding_rate": r["funding_rate"],
                        "mark_price": r.get("mark_price"),
                        "source": r.get("source","rest"),
                    })
                if out:
                    self.db.upsert_funding(out)
            except Exception as e:
                print(f"[FundingLoop {self.account}] error: {e}")
            time.sleep(self.funding_poll_sec)

    def _oms_reconcile_loop(self):
        while True:
            try:
                # expire stuck pending placeholders
                n_exp = self.oms.reconcile_pending_timeouts(self.oms_pending_timeout_sec)
                if n_exp:
                    print(f"[OMS] expired pending: {n_exp}")

                # fetch open orders (REST)
                open_raw = self.ex.fetch_open_orders(self.account, symbol=None)
                open_norm = [norm_open_order(x) for x in open_raw]
                # update/resolve placeholders
                n_res = self.oms.reconcile_open_orders(open_norm)
                if n_res:
                    print(f"[OMS] resolved placeholders: {n_res}")

                # upsert open orders statuses (also keeps DB fresh)
                rows = []
                for oo in open_norm:
                    sym = oo["symbol"]
                    if sym not in self.ids:
                        continue
                    rows.append({
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
                    })
                if rows:
                    self.db.upsert_orders(rows)
            except Exception as e:
                print(f"[OMS reconcile {self.account}] error: {e}")
            time.sleep(self.oms_reconcile_sec)

    def run(self):
        self._load_pos_uids_from_db()
        self.refresh_positions()

        self.ex.subscribe_user_stream(self.account, self.on_user_event)
        self.ex.subscribe_ticks(self.account, self.symbols, lambda sym, px: self.on_tick(sym, px))
        if self.candle_intervals:
            self.ex.subscribe_candles(self.account, self.symbols, self.candle_intervals, self.on_candle)

        threading.Thread(target=self._funding_loop, daemon=True, name=f"FundingLoop-{self.account}").start()
        threading.Thread(target=self._oms_reconcile_loop, daemon=True, name=f"OMSReconcile-{self.account}").start()

        # --- v9 Market State Layer pollers ---
        BalancePoller(
            exchange=self.ex, storage=self.db,
            exchange_id=self.ids["_exchange_id"], account_id=self.ids["_account_id"], account=self.account,
            poll_sec=30.0,
            on_update=self._on_market_state_update,
        ).start()

        OpenInterestPoller(
            exchange=self.ex, storage=self.db,
            exchange_id=self.ids["_exchange_id"], account=self.account,
            symbol_ids={s: self.ids[s] for s in self.symbols if s in self.ids},
            intervals=["5m", "15m", "1h"],
            poll_sec=300.0,
            limit=30,
        ).start()

        while True:
            time.sleep(1.0)
