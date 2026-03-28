from __future__ import annotations

import json
import logging
import math
import os
import random
import time
import threading
import uuid
import hmac
import hashlib
import urllib.parse
import re
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR, ROUND_HALF_UP, ROUND_UP, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

import requests

from src.platform.data.storage.postgres.storage import PostgreSQLStorage


from .params import *
from .params import _ensure_paper_tables, _has_column, _utc_now
from .reporting import TradeLiquidationReportingMixin
from .filters import TradeLiquidationFiltersMixin
from .candidate_finder import TradeLiquidationCandidateFinderMixin
from .entry_rules import TradeLiquidationEntryRulesMixin
from .exit_rules import TradeLiquidationExitRulesMixin
from .hedge_logic import TradeLiquidationHedgeLogicMixin
from .order_builder import TradeLiquidationOrderBuilderMixin, BinanceUMFuturesRest
from .risk_adapter import TradeLiquidationRiskAdapterMixin

log = logging.getLogger("traders.trade_liquidation")

class TradeLiquidation(
    TradeLiquidationReportingMixin,
    TradeLiquidationFiltersMixin,
    TradeLiquidationCandidateFinderMixin,
    TradeLiquidationEntryRulesMixin,
    TradeLiquidationExitRulesMixin,
    TradeLiquidationHedgeLogicMixin,
    TradeLiquidationOrderBuilderMixin,
    TradeLiquidationRiskAdapterMixin,
):
    STRATEGY_ID = "trade_liquidation"
    def __init__(self, store: PostgreSQLStorage, params: TradeLiquidationParams | Dict[str, Any]):
        self.store = store

        # logger alias (module-level logger is `log`)
        self.log = log

        # cache symbol precision filters from DB (price_tick / qty_step)
        self.p = params if isinstance(params, TradeLiquidationParams) else TradeLiquidationParams.from_dict(params or {})
        self._symbol_filters_map: Dict[str, Dict[str, Any]] = self._load_symbol_filters_map()

        # --------------------------------------------------
        # Backward-compatible aliases (some legacy methods use old names)
        # --------------------------------------------------
        self.exchange_id = int(getattr(self.p, "exchange_id", 1) or 1)
        self.params = self.p
        self.mode = getattr(self.p, "mode", "paper")
        self.strategy_id = self.STRATEGY_ID


        _ensure_paper_tables(self.store)

        # resolve account_id (uses your storage helper)
        self.account_id = self.store._resolve_account_id(
            self.p.exchange_id,
            account=self.p.account_name,
            account_id=self.p.account_id,
        )

        self._pl_has_raw_meta = _has_column(self.store, "position_ledger", "raw_meta")

        self._is_live = str(self.p.mode or "paper").strip().lower() == "live"
        # The Binance USD-M Futures REST client instance.
        # (Some helper code paths historically referenced "_binance_rest"; we keep
        # a compatible alias below.)
        self._binance: Optional[BinanceUMFuturesRest] = None
        if self._is_live:
            api_key = str(os.getenv(self.p.api_key_env) or "").strip()
            api_secret = str(os.getenv(self.p.api_secret_env) or "").strip()
            if not api_key or not api_secret:
                raise RuntimeError(
                    f"LIVE mode requires env {self.p.api_key_env} and {self.p.api_secret_env} to be set"
                )
            self._binance = BinanceUMFuturesRest(
                api_key=api_key,
                api_secret=api_secret,
                base_url=str(self.p.base_url),
                recv_window_ms=int(self.p.recv_window_ms),
                timeout_sec=int(self.p.timeout_sec),
                debug=bool(self.p.debug),
                connect_timeout_sec=float(getattr(self.p, "binance_connect_timeout_sec", 3.0) or 3.0),
                read_timeout_sec=float(getattr(self.p, "binance_read_timeout_sec", self.p.timeout_sec) or self.p.timeout_sec),
                max_retries=int(getattr(self.p, "binance_max_retries", 2) or 2),
                retry_backoff_sec=float(getattr(self.p, "binance_retry_backoff_sec", 1.0) or 1.0),
                # let REST client round price/qty using DB filters (best-effort)
                symbol_filters_resolver=lambda _sym: self._symbol_filters_map.get(str(_sym or "").upper()),
            )

        # Backward compatible alias used by some newer helper functions.
        # Prefer self._binance everywhere.
        self._binance_rest = self._binance

        # --------------------------------------------------
        # Step 4: Async REST (ThreadPool)
        # --------------------------------------------------
        self._rest_pool: Optional[ThreadPoolExecutor] = None
        self._rest_snapshot_lock = threading.Lock()
        self._rest_snapshot: Dict[str, Any] = {}
        self._rest_snapshot_ts: Optional[datetime] = None

        # cycle counter (used for reconcile/recovery throttling)
        self._cycle_n = 0

        # Runtime caches
        #   pos_uid -> last avg_price that TP/TRAIL were anchored to
        self._avg_anchor_cache: Dict[str, float] = {}
        # throttle bracket order replace (TP/Trailing) to avoid churn
        self._bracket_replace_last_ts: Dict[Tuple[str, str], float] = {}

        # cached symbol map (symbol_id -> symbol)
        self._symbols_cache: Optional[Dict[int, str]] = None
        self._symbols_cache_ts: Optional[datetime] = None

        # last exchange /fapi/v2/positionRisk payload (live only)
        # used to keep DB "positions" / "position_snapshots" in sync with markPrice
        self._last_position_risk: Optional[List[Dict[str, Any]]] = None

        if self._is_live and self._binance is not None and bool(getattr(self.p, "async_rest_enabled", False)):
            workers = int(getattr(self.p, "async_rest_workers", 4) or 4)
            workers = max(1, min(16, workers))
            self._rest_pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="tl_rest")
            self._dlog("async REST enabled: workers=%d", workers)

        if self.p.debug:
            self._dlog("position_ledger.raw_meta present=%s", self._pl_has_raw_meta)
            self._dlog("mode=%s account_id=%s hedge=%s", self.p.mode, str(self.account_id), str(self.p.hedge_enabled))

    def _symbols_map(self, cache_ttl_sec: float = 600.0) -> Dict[int, str]:
        """Return {symbol_id: symbol} cache (DB read is cheap but keep it tidy)."""
        try:
            now = _utc_now()
            if self._symbols_cache is not None and self._symbols_cache_ts is not None:
                if 0 <= (now - self._symbols_cache_ts).total_seconds() <= float(cache_ttl_sec or 0.0):
                    return dict(self._symbols_cache)

            sym_rows = list(
                self.store.query_dict(
                    "SELECT symbol_id, symbol FROM symbols WHERE exchange_id=%(ex)s",
                    {"ex": int(self.exchange_id)},
                )
            )
            mp = {int(r["symbol_id"]): str(r["symbol"]) for r in sym_rows if r.get("symbol_id") is not None and r.get("symbol")}
            self._symbols_cache = dict(mp)
            self._symbols_cache_ts = now
            return mp
        except Exception:
            return {}

    def _position_risk_mark_map(self, position_risk: Optional[List[Dict[str, Any]]]) -> Dict[Tuple[str, str], Dict[str, float]]:
        """Build {(SYMBOL, SIDE): {mark, entry, pnl, notional}} from Binance /fapi/v2/positionRisk."""
        out: Dict[Tuple[str, str], Dict[str, float]] = {}
        if not position_risk:
            return out

        for pr in position_risk:
            if not isinstance(pr, dict):
                continue
            sym = str(pr.get("symbol") or "").upper().strip()
            if not sym:
                continue

            # Binance returns both positionAmt and positionSide (LONG/SHORT/BOTH)
            pos_side = str(pr.get("positionSide") or "").upper().strip()
            try:
                amt = float(pr.get("positionAmt") or 0.0)
            except Exception:
                amt = 0.0
            if abs(amt) <= 0.0:
                continue

            side = "LONG" if amt > 0 else "SHORT"
            if pos_side in ("LONG", "SHORT"):
                side = pos_side

            def _f(v: Any) -> float:
                try:
                    return float(v)
                except Exception:
                    return 0.0

            out[(sym, side)] = {
                "mark": _f(pr.get("markPrice") or 0.0),
                "entry": _f(pr.get("entryPrice") or 0.0),
                "pnl": _f(pr.get("unRealizedProfit") or 0.0),
                "notional": _f(pr.get("notional") or 0.0),
            }

        return out

    def _sync_positions_tables_from_position_risk(self) -> None:
        """Persist mark_price (+derived pnl/value) into DB using /positionRisk as the source of truth.

        This is the most robust source:
          - candles/klines can lag or be missing for some symbols
          - positionRisk provides markPrice directly from exchange
          - it makes v_positions_* views useful even if WS collectors are not running
        """

        if not self._is_live:
            return
        pr = self._last_position_risk
        if not pr:
            return

        try:
            mark_map = self._position_risk_mark_map(pr)
            if not mark_map:
                return

            ledger_rows = list(
                self.store.query_dict(
                    """
                    SELECT
                      exchange_id, account_id, pos_uid, symbol_id, strategy_id, strategy_name,
                      side, status, opened_at, closed_at,
                      entry_price, avg_price,
                      qty_opened, qty_current, qty_closed,
                      realized_pnl, fees,
                      updated_at
                    FROM public.position_ledger
                    WHERE exchange_id=%(ex)s AND account_id=%(acc)s
                      AND status='OPEN'
                      AND side IN ('LONG','SHORT')
                      AND COALESCE(qty_current,0) > 0
                    """,
                    {"ex": int(self.exchange_id), "acc": int(self.account_id)},
                )
            )
            if not ledger_rows:
                return

            id_to_sym = self._symbols_map()
            now = _utc_now()
            now_ms = int(now.timestamp() * 1000)

            pos_rows: List[Dict[str, Any]] = []
            snap_rows: List[Dict[str, Any]] = []

            for lr in ledger_rows:
                try:
                    sid = int(lr.get("symbol_id") or 0)
                except Exception:
                    sid = 0
                if sid <= 0:
                    continue
                sym = str(id_to_sym.get(sid) or "").upper().strip()
                if not sym:
                    continue

                side = str(lr.get("side") or "").upper().strip()
                if side not in ("LONG", "SHORT"):
                    continue

                mm = mark_map.get((sym, side))
                if not mm:
                    continue

                try:
                    qty = float(lr.get("qty_current") or 0.0)
                except Exception:
                    qty = 0.0
                if qty <= 0.0:
                    continue

                # Prefer ledger entry, fallback to avg/exchange entry
                try:
                    entry = float(lr.get("entry_price") or 0.0)
                except Exception:
                    entry = 0.0
                try:
                    avg = float(lr.get("avg_price") or 0.0)
                except Exception:
                    avg = 0.0
                if entry <= 0.0 and avg > 0.0:
                    entry = avg
                if entry <= 0.0:
                    entry = float(mm.get("entry") or 0.0)

                mark = float(mm.get("mark") or 0.0)
                if mark <= 0.0:
                    continue

                # Derived metrics
                unreal = (mark - entry) * qty if side == "LONG" else (entry - mark) * qty
                pos_val = abs(qty * mark)

                pos_rows.append(
                    {
                        "exchange_id": int(self.exchange_id),
                        "account_id": int(self.account_id),
                        "symbol_id": sid,
                        "strategy_id": str(lr.get("strategy_id") or self.STRATEGY_ID),
                        "pos_uid": str(lr.get("pos_uid") or ""),
                        "side": side,
                        "qty": qty,
                        "entry_price": entry,
                        "mark_price": mark,
                        "unrealized_pnl": unreal,
                        "updated_at": now,
                        "source": "tl_position_risk",
                        "avg_price": float(lr.get("avg_price") or entry),
                        "position_value_usdt": pos_val,
                        "status": "OPEN",
                        "opened_at": lr.get("opened_at"),
                        "closed_at": lr.get("closed_at"),
                        "realized_pnl": lr.get("realized_pnl"),
                        "scale_in_count": lr.get("scale_in_count"),
                        "strategy_name": lr.get("strategy_name"),
                        "fees": lr.get("fees"),
                        "last_ts": lr.get("updated_at"),
                    }
                )

                snap_rows.append(
                    {
                        "exchange_id": int(self.exchange_id),
                        "account_id": int(self.account_id),
                        "symbol_id": sid,
                        "side": side,
                        "qty": qty,
                        "entry_price": entry,
                        "mark_price": mark,
                        "position_value": pos_val,
                        "unrealized_pnl": unreal,
                        "realized_pnl": float(lr.get("realized_pnl") or 0.0),
                        "fees": float(lr.get("fees") or 0.0),
                        "last_ts": now,
                        "updated_at": now,
                        "source": "tl_position_risk",
                        "avg_price": float(lr.get("avg_price") or entry),
                        "last_ts_ms": now_ms,
                    }
                )

            if pos_rows:
                self.store.upsert_positions(pos_rows)
            if snap_rows:
                self.store.upsert_position_snapshots(snap_rows)

        except Exception:
            self.log.exception("[TL][sync_positions] failed")

    def _load_symbol_filters_map(self) -> Dict[str, Dict[str, Any]]:
        """Load per-symbol precision/limits from DB table public.symbol_filters.

        Expected schema (confirmed in your DB):
          exchange_id, symbol_id, price_tick, qty_step, min_qty, max_qty, min_notional, max_leverage, margin_type, updated_at

        Returns dict keyed by symbol name (e.g. 'BTCUSDT') with raw DB values (Decimal/None).
        """
        ex_id = int(getattr(self.p, "exchange_id", 1) or 1)

        sql = """
        SELECT
          s.symbol AS symbol,
          sf.symbol_id AS symbol_id,
          sf.price_tick,
          sf.qty_step,
          sf.min_qty,
          sf.max_qty,
          sf.min_notional,
          sf.max_leverage,
          sf.margin_type,
          sf.updated_at
        FROM public.symbol_filters sf
        JOIN public.symbols s
          ON s.exchange_id = sf.exchange_id
         AND s.symbol_id   = sf.symbol_id
        WHERE sf.exchange_id = %(ex_id)s
        """

        try:
            rows = self.store.query_dict(sql, {"ex_id": ex_id}) or []
        except Exception as e:
            log.warning("[TL] failed to load symbol_filters from DB: %s", e)
            return {}

        out: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            sym = (r.get("symbol") or "").strip()
            if not sym:
                continue
            out[sym] = dict(r)

        log.info("[TL] loaded symbol_filters rows=%s exchange_id=%s", len(out), ex_id)
        return out

    def _rest_snapshot_set(self, **kv: Any) -> None:
        with self._rest_snapshot_lock:
            self._rest_snapshot.update(kv)
            self._rest_snapshot_ts = _utc_now()

        # Keep last /positionRisk payload for DB sync (markPrice source of truth)
        if "position_risk" in kv:
            pr = kv.get("position_risk")
            self._last_position_risk = pr if isinstance(pr, list) else None

    def _rest_snapshot_get(self, key: str) -> Any:
        with self._rest_snapshot_lock:
            return self._rest_snapshot.get(key)

    def _get_position_amt_live(self, symbol: str, side: str) -> float:
        """Return absolute position amount for (symbol, side) on the exchange.

        side: 'LONG' or 'SHORT' (hedge-mode side). In one-way mode, we infer by sign.
        Returns 0.0 when no such open position exists or when not live.
        """
        if not getattr(self, "_is_live", False) or getattr(self, "_binance", None) is None:
            return 0.0

        sym = (symbol or "").upper().strip()
        sd = (side or "").upper().strip()
        if not sym or sd not in ("LONG", "SHORT"):
            return 0.0

        rows = self._rest_snapshot_get("position_risk")
        if not isinstance(rows, list):
            try:
                rows = self._binance.position_risk()
            except Exception:
                rows = []

        for pr in rows or []:
            if not isinstance(pr, dict):
                continue
            if str(pr.get("symbol") or "").upper().strip() != sym:
                continue

            # Binance futures: positionAmt >0 long, <0 short (one-way); in hedge-mode also has positionSide.
            try:
                amt = float(pr.get("positionAmt") or 0.0)
            except Exception:
                amt = 0.0
            if abs(amt) <= 0.0:
                continue

            ps = str(pr.get("positionSide") or "").upper().strip()
            real_side = "LONG" if amt > 0 else "SHORT"
            if ps in ("LONG", "SHORT"):
                real_side = ps

            if real_side == sd:
                return abs(amt)

        return 0.0

    def _has_open_position_live(self, symbol: str, side: str) -> bool:
        return self._get_position_amt_live(symbol, side) > 0.0

    def _get_local_algo_open_cache(self) -> Dict[str, Dict[str, Any]]:
        cache = getattr(self, "_algo_local_open", None)
        if not isinstance(cache, dict):
            self._algo_local_open = {}
            cache = self._algo_local_open
        return cache

    def _mark_local_algo_open(
        self,
        client_algo_id: str,
        *,
        symbol: str,
        position_side: Optional[str] = None,
        order_type: Optional[str] = None,
        side: Optional[str] = None,
        quantity: Optional[float] = None,
        trigger_price: Optional[float] = None,
        close_position: Optional[bool] = None,
        pos_uid: Optional[str] = None,
    ) -> None:
        cid = str(client_algo_id or "").strip()
        if not cid:
            return
        cache = self._get_local_algo_open_cache()
        cache[cid] = {
            "clientAlgoId": cid,
            "symbol": str(symbol or "").upper(),
            "positionSide": str(position_side or "").upper(),
            "orderType": str(order_type or "").upper(),
            "type": str(order_type or "").upper(),
            "side": str(side or "").upper(),
            "quantity": None if quantity is None else float(quantity),
            "origQty": None if quantity is None else float(quantity),
            "triggerPrice": None if trigger_price is None else float(trigger_price),
            "activatePrice": None if trigger_price is None else float(trigger_price),
            "closePosition": None if close_position is None else bool(close_position),
            "pos_uid": str(pos_uid or ""),
            "placed_at": float(time.time()),
        }

    def _clear_local_algo_open(self, client_algo_id: str) -> None:
        cid = str(client_algo_id or "").strip()
        if not cid:
            return
        cache = getattr(self, "_algo_local_open", None)
        if isinstance(cache, dict):
            cache.pop(cid, None)

    def _get_mark_price(self, symbol: str, position_side: Optional[str] = None) -> float:
        """Compat wrapper: always return a mark price even in hedge mode.

        Binance premiumIndex is per-symbol; for mark we should *not* require positionSide.
        """
        try:
            return float(self._get_mark_price_live(symbol, None))
        except Exception:
            return 0.0

    def _get_mark_price_live(self, symbol: str, position_side: Optional[str] = None) -> float:
        """Best-effort current markPrice for a symbol (LIVE).

        Uses prefetched REST snapshot (position_risk) when available; falls back to direct REST call.
        """
        if not self._is_live or self._binance is None:
            return 0.0
        sym = (symbol or "").upper().strip()
        if not sym:
            return 0.0
        ps_need = (position_side or "").upper().strip() if position_side else ""

        rows = self._rest_snapshot_get("position_risk")
        if not isinstance(rows, list):
            try:
                rows = self._binance.position_risk()
            except Exception:
                rows = []
        for it in rows or []:
            try:
                if str(it.get("symbol") or "").upper().strip() != sym:
                    continue
                ps = str(it.get("positionSide") or "").upper().strip()
                if ps_need and ps and ps != ps_need:
                    continue
                mp = _safe_float(it.get("markPrice"), 0.0)
                if mp > 0:
                    return float(mp)
            except Exception:
                continue
        return 0.0

    def _maybe_refresh_rest_snapshot(self) -> None:
        """Fetch REST data in parallel (balance / positionRisk / openOrders).

        This reduces total wall time when multiple REST endpoints are needed.
        Even if async is disabled, we still populate snapshot synchronously.
        """
        if not self._is_live or self._binance is None:
            return

        need_open_orders = bool(getattr(self.p, "live_cleanup_remaining_orders", True)) or bool(getattr(self.p, "auto_recovery_enabled", False))
        need_position_risk = bool(getattr(self.p, "reconcile_enabled", False)) or float(getattr(self.p, "portfolio_cap_ratio", 0.0) or 0.0) > 0
        need_balance = bool(getattr(self.p, "use_exchange_balance", True))

        timeout_s = float(getattr(self.p, "async_rest_timeout_sec", 8.0) or 8.0)

        futs: Dict[str, Future] = {}

        # 1) openOrders (all)
        if need_open_orders:
            f = self._rest_submit(self._binance.open_orders)
            if f is not None:
                futs["open_orders_all"] = f

            # 1b) openAlgoOrders (all) — conditional orders live here
            f2 = self._rest_submit(self._binance.open_algo_orders)
            if f2 is not None:
                futs["open_algo_orders_all"] = f2

        # 2) positionRisk
        if need_position_risk:
            f = self._rest_submit(self._binance.position_risk)
            if f is not None:
                futs["position_risk"] = f

        # 3) balance (may be cached; this call uses poll interval)
        if need_balance:
            f = self._rest_submit(self._wallet_balance_usdt)
            if f is not None:
                futs["wallet_balance_usdt"] = f
                # margin_used is written by balance-writer; used for portfolio_cap_ratio
                fmu = self._rest_submit(self._margin_used_usdt)
                if fmu is not None:
                    futs["margin_used_usdt"] = fmu

        # If async is disabled, do sync fetches (still useful for caching/avoid duplicates).
        if not futs:
            if need_open_orders:
                try:
                    oo = self._binance.open_orders()
                    ao = self._binance.open_algo_orders()
                    self._rest_snapshot_set(
                        open_orders_all=oo,
                        open_algo_orders_all=ao,
                    )
                    # Persist open algo orders for accounting/monitoring (best-effort)
                    try:
                        rows = []
                        active_cids: list[str] = []
                        for r in ao if isinstance(ao, list) else []:
                            cid = str(r.get("clientAlgoId") or r.get("clientOrderId") or "").strip()
                            if not cid:
                                continue
                            if cid.startswith("TL_"):
                                active_cids.append(cid)
                            rows.append(
                                {
                                    "exchange_id": int(self.exchange_id),
                                    "account_id": int(self.account_id),
                                    "client_algo_id": cid,
                                    "algo_id": str(r.get("algoId") or r.get("algoOrderId") or r.get("orderId") or ""),
                                    "symbol": str(r.get("symbol") or "").upper(),
                                    "side": str(r.get("side") or ""),
                                    "position_side": str(r.get("positionSide") or ""),
                                    "type": str(r.get("type") or ""),
                                    "quantity": None if r.get("quantity") is None else float(r.get("quantity")),
                                    "trigger_price": None if r.get("triggerPrice") is None else float(r.get("triggerPrice")),
                                    "working_type": str(r.get("workingType") or ""),
                                    "status": "OPEN",
                                    "strategy_id": str(self.strategy_id),
                                    "pos_uid": None,
                                    "raw_json": r if isinstance(r, dict) else {"result": r},
                                }
                            )
                        if rows:
                            self.store.upsert_algo_orders(rows)

                        # Mark previously OPEN algo orders as NOT_FOUND if they disappeared from exchange.
                        try:
                            self.store.sync_open_algo_orders_not_found(
                                exchange_id=int(self.exchange_id),
                                account_id=int(self.account_id),
                                active_client_algo_ids=active_cids,
                                prefix="TL_",
                                not_found_status="CANCELED",
                            )
                        except Exception:
                            pass
                    except Exception:
                        pass

                except Exception:
                    log.debug("[trade_liquidation] openOrders sync fetch failed", exc_info=True)

            if need_position_risk:
                try:
                    self._rest_snapshot_set(position_risk=self._binance.position_risk())
                except Exception:
                    log.debug("[trade_liquidation] positionRisk sync fetch failed", exc_info=True)
            if need_balance:
                try:
                    self._rest_snapshot_set(wallet_balance_usdt=float(self._wallet_balance_usdt()))
                    try:
                        self._rest_snapshot_set(margin_used_usdt=float(self._margin_used_usdt()))
                    except Exception:
                        pass
                except Exception:
                    log.debug("[trade_liquidation] balance sync fetch failed", exc_info=True)
            return

        # Gather results
        out: Dict[str, Any] = {}
        for k, f in futs.items():
            try:
                out[k] = f.result(timeout=timeout_s)
            except Exception:
                log.debug("[trade_liquidation] async REST task failed: %s", k, exc_info=True)

        if out:
            # Persist open algo orders + reconcile missing ones (best-effort)
            try:
                ao = out.get("open_algo_orders_all")
                rows = []
                active_cids: list[str] = []
                for r in ao if isinstance(ao, list) else []:
                    cid = str(r.get("clientAlgoId") or r.get("clientOrderId") or "").strip()
                    if not cid:
                        continue
                    if cid.startswith("TL_"):
                        active_cids.append(cid)
                    rows.append(
                        {
                            "exchange_id": int(self.exchange_id),
                            "account_id": int(self.account_id),
                            "client_algo_id": cid,
                            "algo_id": str(r.get("algoId") or r.get("algoOrderId") or r.get("orderId") or ""),
                            "symbol": str(r.get("symbol") or "").upper(),
                            "side": str(r.get("side") or ""),
                            "position_side": str(r.get("positionSide") or ""),
                            "type": str(r.get("type") or ""),
                            "quantity": None if r.get("quantity") is None else float(r.get("quantity")),
                            "trigger_price": None if r.get("triggerPrice") is None else float(r.get("triggerPrice")),
                            "working_type": str(r.get("workingType") or ""),
                            "status": "OPEN",
                            "strategy_id": str(self.strategy_id),
                            "pos_uid": None,
                            "raw_json": r if isinstance(r, dict) else {"result": r},
                        }
                    )
                if rows:
                    self.store.upsert_algo_orders(rows)

                # Mark missing
                self.store.sync_open_algo_orders_not_found(
                    exchange_id=int(self.exchange_id),
                    account_id=int(self.account_id),
                    active_client_algo_ids=active_cids,
                    prefix="TL_",
                    not_found_status="CANCELED",
                )
            except Exception:
                pass
            self._rest_snapshot_set(**out)

    def run_once(self) -> Dict[str, Any]:
        started = _utc_now()

        # cycle counter
        try:
            self._cycle_n = int(getattr(self, "_cycle_n", 0) or 0) + 1
        except Exception:
            self._cycle_n = 1

        # Step 4: prefetch REST state (parallel where possible)
        # This populates in-memory snapshot for this cycle.
        self._maybe_refresh_rest_snapshot()

        # LIVE: cleanup leftover bracket order after SL/TP filled (source of truth: order_events/fills)
        canceled_leftovers = 0
        if self._is_live and bool(getattr(self.p, "live_cleanup_remaining_orders", True)):
            canceled_leftovers = int(self._live_cleanup_remaining_brackets() or 0)

        # LIVE: hard cleanup — if positionAmt==0 on exchange, cancel *all* orders for symbol
        # (manual close / liquidation / reduce-only close / etc.)
        if self._is_live and bool(getattr(self.p, "live_cancel_orders_when_flat", True)):
            canceled_leftovers += int(self._live_cancel_all_orders_if_flat() or 0)

        # Step 1: auto-recovery (ensure SL/TP exist) using async snapshot (openOrders/positionRisk)
        recovered = 0

        def _tl_safe_trigger_price(*, symbol: str, close_side: str, desired: float, mark: float, tick: float, buffer_pct: float) -> float:
            """Ensure conditional trigger price won't immediately trigger.

            For close_side=SELL (closing LONG): trigger/activation must be ABOVE current mark.
            For close_side=BUY  (closing SHORT): trigger/activation must be BELOW current mark.
            """
            p = float(desired or 0.0)
            m = float(mark or 0.0)
            buf = float(buffer_pct or 0.0) / 100.0
            if m > 0:
                if str(close_side).upper() == "SELL":
                    # must be above mark
                    floor = m * (1.0 + max(buf, 0.0005))
                    p = max(p, floor)
                    # round UP so it stays above
                    if tick and tick > 0:
                        p = _round_price_to_tick(p, tick, mode="up")
                else:
                    # BUY: must be below mark
                    ceil = m * (1.0 - max(buf, 0.0005))
                    p = min(p if p > 0 else ceil, ceil)
                    # round DOWN so it stays below
                    if tick and tick > 0:
                        p = _round_price_to_tick(p, tick, mode="down")
            else:
                # no mark -> just tick rounding in the safe direction
                if tick and tick > 0:
                    p = _round_price_to_tick(p, tick, mode="up" if str(close_side).upper() == "SELL" else "down")
            return float(p)

        def _tl_place_algo_with_retry(place_fn, *, max_tries: int = 3):
            """Retry placing conditional orders when Binance says 'would immediately trigger' (-2021)."""
            last = None
            for i in range(int(max_tries or 1)):
                try:
                    return place_fn(i)
                except Exception as e:
                    last = e
                    s = str(e)
                    if ("-2021" in s) or ("immediately trigger" in s.lower()):
                        continue
                    raise
            if last:
                raise last
            return None

        if self._is_live and bool(getattr(self.p, "auto_recovery_enabled", False)):
            recovered = int(self._auto_recovery_brackets() or 0)

        # Step 2: reconcile ledger ↔ Binance positions using async snapshot (positionRisk)
        reconcile_stats: Dict[str, int] = {}
        if self._is_live and bool(getattr(self.p, "reconcile_enabled", False)):
            reconcile_stats = dict(self._reconcile_ledger_vs_exchange() or {})

            # Persist markPrice (+derived pnl/value) into DB from /positionRisk.
            # This is more accurate/robust than candles and works even if WS collectors are offline.
            self._sync_positions_tables_from_position_risk()

        # Step 2.1: reconcile averaging adds vs Binance order history (best-effort, rate-limited)
        reconcile_adds_stats: Dict[str, int] = {}
        if self._is_live and bool(getattr(self.p, "reconcile_binance_adds_enabled", True)):
            try:
                reconcile_adds_stats = dict(self._reconcile_adds_vs_binance() or {})
            except Exception:
                log.debug("[trade_liquidation] reconcile_adds_vs_binance failed", exc_info=True)

        # Step 2.5: backfill exit_price/realized_pnl for already CLOSED ledger rows (best-effort, rate-limited)
        if self._is_live and bool(getattr(self.p, "backfill_enabled", True)):
            try:
                # Run once immediately on startup (then continue on interval inside the function).
                if (not bool(getattr(self, "_backfill_startup_done", False))
                        and bool(getattr(self.p, "backfill_run_on_startup", True))):
                    self._backfill_closed_exit_pnl(force=True, log_empty=True)
                    self._backfill_startup_done = True
                else:
                    self._backfill_closed_exit_pnl()
            except Exception:
                log.debug("[trade_liquidation] backfill_closed_exit_pnl failed", exc_info=True)

        open_positions = self._get_open_positions()
        max_symbols_per_cycle = int(getattr(self.p, "max_symbols_per_cycle", 5) or 5)
        managed_positions = self._cycle_subset(open_positions or [], "open_positions", max_symbols_per_cycle)
        self._cycle_open_positions_subset = list(managed_positions or [])

        # LIVE: averaging adds maintenance.
        # Ensure we have the next ADD order queued, but never exceed max_adds.
        # We place at most ONE missing ADD per cycle to avoid spamming Binance endpoints.
        if self._is_live and bool(getattr(self.p, "averaging_enabled", False)) and self._cfg_max_adds() > 0:
            try:
                placed_any = False
                for p in managed_positions or []:
                    if self._live_ensure_next_add_order(pos=p):
                        placed_any = True
                        break
                if placed_any:
                    # refresh snapshot next cycle; current cycle continues
                    pass
            except Exception:
                log.debug("[trade_liquidation][LIVE][AVG] ensure_next_add failed", exc_info=True)
        self._dlog(
            "cycle start: open_positions=%d/%d managed=%d signal_tf=%s | sl_mode=%s tp_mode=%s | trailing=%s | avg=%s levels=%s | canceled_leftovers=%d",
            len(open_positions),
            int(self.p.max_open_positions),
            len(managed_positions),
            ",".join(self.p.allowed_timeframes) if self.p.allowed_timeframes else "*",
            str(self.p.sl_order_mode),
            str(self.p.tp_order_mode),
            "ON" if bool(self.p.trailing_enabled) else "OFF",
            "ON" if bool(self.p.averaging_enabled) else "OFF",
            str(self.p.levels_method),
            canceled_leftovers,
        )

        expired = 0
        if bool(getattr(self.p, "expire_old_signals", False)):
            expired = int(self._expire_old_new_signals())

        close_stats = self._process_open_positions()
        open_stats = self._process_new_signals()
        try:
            st = (
                str(open_stats.get("open_symbols")),
                str(open_stats.get("open_legs")),
                str(open_stats.get("capacity")),
                str(open_stats.get("considered")),
                str(open_stats.get("opened")),
                str(open_stats.get("skipped")),
                str(open_stats.get("blocked_reason")),
            )
            self._stateful_info(
                "entry_status",
                st,
                "[trade_liquidation][ENTRY_STATUS] open_symbols=%s open_legs=%s capacity=%s considered=%s opened=%s skipped=%s blocked_reason=%s",
                *st,
            )
        except Exception:
            pass

        elapsed = (_utc_now() - started).total_seconds()
        try:
            if bool(getattr(self.p, "enable_cycle_summary", True)):
                every = max(1, int(getattr(self.p, "cycle_summary_interval", 1) or 1))
                if int(getattr(self, "_cycle_n", 0) or 0) % every == 0:
                    log.info(
                        "[trade_liquidation][CYCLE_SUMMARY] elapsed=%.2fs managed=%d total_open=%d recovered=%d open_symbols=%s open_legs=%s capacity=%s opened=%s skipped=%s blocked_reason=%s closed=%s",
                        float(elapsed),
                        len(managed_positions),
                        len(open_positions),
                        int(recovered),
                        str(open_stats.get("open_symbols")),
                        str(open_stats.get("open_legs")),
                        str(open_stats.get("capacity")),
                        str(open_stats.get("opened")),
                        str(open_stats.get("skipped")),
                        str(open_stats.get("blocked_reason")),
                        str(close_stats),
                    )
        except Exception:
            pass

        out = {
            "strategy": self.STRATEGY_ID,
            "paper": not self._is_live,
            "live": self._is_live,
            "expired": expired,
            "closed": close_stats,
            "opened": open_stats,
            "canceled_leftovers": canceled_leftovers,
            "recovered": recovered,
            "reconcile": reconcile_stats,
            "reconcile_adds": reconcile_adds_stats,
            "elapsed_s": elapsed,
        }
        self._dlog("cycle end: closed=%s opened=%s elapsed=%.2fs", str(close_stats), str(open_stats), float(elapsed))
        return out

    def _qty_close(self, a: float, b: float, tol: float = 0.002) -> bool:
        try:
            return abs(float(a) - float(b)) <= float(tol)
        except Exception:
            return False

    def _reconcile_ledger_vs_exchange(self) -> Dict[str, int]:
        """Compare OPEN ledger positions with Binance positions and fix obvious drift.

        Uses snapshot "position_risk" (prefetched in run_once).
        """
        if not self._is_live:
            return {}
        if self._binance is None:
            return {}

        every_n = int(getattr(self.p, "reconcile_every_n_cycles", 1) or 1)
        every_n = max(1, every_n)
        if int(getattr(self, "_cycle_n", 0) or 0) % every_n != 0:
            return {"skipped": 1}

        tol = float(getattr(self.p, "reconcile_qty_tolerance", 1e-8) or 1e-8)
        auto_close = bool(getattr(self.p, "reconcile_auto_close_ledger", True))
        # Optional micro-step: if exchange qty differs from ledger, adjust qty_current to exchange qty.
        # Safe by default: OFF.
        auto_adjust_qty = bool(getattr(self.p, "reconcile_auto_adjust_ledger_qty_current", False))
        # New: auto-adjust ledger position_value_usdt (mark-to-market) independently of qty.
        auto_adjust_value = bool(getattr(self.p, "reconcile_auto_adjust_ledger_value_usdt", True))
        # New: refresh value_usdt even when within tolerance (to keep ledger close to exchange mark).
        auto_refresh_value = bool(getattr(self.p, "reconcile_refresh_position_value_usdt", False))
        refresh_min_delta = float(getattr(self.p, "reconcile_value_refresh_min_delta_usdt", 0.1) or 0.1)
        adjust_only_our = bool(getattr(self.p, "reconcile_adjust_only_our_positions", True))
        ignore_external = bool(getattr(self.p, "reconcile_ignore_external_positions", True))
        ext_lvl = str(getattr(self.p, "reconcile_external_log_level", "debug") or "debug").strip().lower()
        oo_all = self._rest_snapshot_get("open_orders_all")
        # Needed for "our position" detection (clientAlgoId TL_...)
        oo_algo_all = self._rest_snapshot_get("open_algo_orders_all")

        sym_map = self._symbols_map()

        # 1) exchange positions
        pr = self._rest_snapshot_get("position_risk")
        rows = pr if isinstance(pr, list) else []
        ex_pos: Dict[Tuple[str, str], float] = {}
        ex_usdt: Dict[Tuple[str, str], float] = {}
        for it in rows:
            sym = str(it.get("symbol") or "").strip()
            if not sym:
                continue
            amt = _safe_float(it.get("positionAmt"), 0.0)
            if abs(amt) <= 0:
                continue
            # Determine side
            ps = str(it.get("positionSide") or "").upper().strip()
            if ps in {"LONG", "SHORT"}:
                side = ps
            else:
                side = "LONG" if amt > 0 else "SHORT"
            ex_pos[(sym, side)] = abs(float(amt))
            try:
                notional = abs(_safe_float(it.get('notional'), 0.0))
            except Exception:
                notional = 0.0
            if notional <= 0:
                mp = _safe_float(it.get('markPrice'), 0.0)
                if mp > 0:
                    notional = abs(float(amt)) * float(mp)
            ex_usdt[(sym, side)] = abs(float(notional))

        # 2) ledger positions
        led = list(
            self.store.query_dict(
                """
                SELECT symbol_id, pos_uid, side, status, qty_current, position_value_usdt, entry_price, avg_price, raw_meta, opened_at
                FROM position_ledger
                WHERE exchange_id=%(ex)s
                  AND account_id=%(acc)s
                  AND strategy_id=%(sid)s
                  AND status='OPEN'
                ORDER BY opened_at DESC
                """,
                {"ex": int(self.exchange_id), "acc": int(self.account_id), "sid": str(self.STRATEGY_ID)},
            )
        )

        missing_on_exchange = 0
        qty_mismatch = 0
        closed_ledger = 0
        external = 0

        for p in led:
            symbol_id = int(p.get("symbol_id") or 0)
            sym = sym_map.get(symbol_id)
            if not sym:
                continue
            side = str(p.get("side") or "").upper().strip()
            if side not in {"LONG", "SHORT"}:
                continue

            led_qty = abs(_safe_float(p.get("qty_current"), 0.0))
            ex_qty = abs(_safe_float(ex_pos.get((sym, side)), 0.0))

            if self._is_flat_qty(ex_qty, sym):
                missing_on_exchange += 1
                if auto_close:
                    try:
                        self.store.execute(
                            """
                            UPDATE position_ledger
                            SET status='CLOSED',
                                closed_at=now(),
                                qty_current=0,
                                updated_at=now(),
                                raw_meta = jsonb_set(
                                    COALESCE(raw_meta, '{}'::jsonb),
                                    '{close_reason}',
                                    to_jsonb(%(reason)s::text),
                                    true
                                )
                            WHERE exchange_id=%(exchange_id)s
                              AND account_id=%(account_id)s
                              AND pos_uid=%(pos_uid)s
                            """,
                            {
                                "exchange_id": int(self.exchange_id),
                                "account_id": int(self.account_id),
                                "pos_uid": str(p.get("pos_uid") or ""),
                                "reason": "reconcile_missing_on_exchange",
                            },
                        )
                        closed_ledger += 1
                        try:
                            c = self._live_cancel_symbol_orders(sym)
                            if c:
                                self._dlog("reconcile: flat on exchange -> canceled %s orders for %s", c, sym)
                        except Exception as _e:
                            self._dlog("reconcile: failed to cancel orders for %s: %s", sym, _e)
                    except Exception:
                        log.exception("[trade_liquidation][RECONCILE] failed to auto-close ledger pos_uid=%s", str(p.get("pos_uid") or ""))
                continue

            # Qty/Value mismatch checks.
            led_val = float(_safe_float(p.get("position_value_usdt"), 0.0))
            ex_val = float(ex_usdt.get((sym, side), 0.0) or 0.0)
            usdt_tol = float(getattr(self.p, "reconcile_value_tolerance_usdt", 0.5) or 0.5)

            qty_diff = abs(ex_qty - led_qty)
            usdt_diff = abs(ex_val - led_val) if (ex_val > 0 and led_val > 0) else 0.0
            try:
                qty_step = float(self._qty_step_for_symbol(sym) or 0.0)
            except Exception:
                qty_step = 0.0
            qty_tol_eff = max(float(tol), (qty_step / 2.0) if qty_step > 0 else 0.0, 0.002)
            qty_close = self._qty_close(led_qty, ex_qty, qty_tol_eff)

            if (not qty_close) or usdt_diff > usdt_tol:
                if not qty_close:
                    qty_mismatch += 1
                    if bool(getattr(self.p, "reconcile_log_diffs", True)):
                        log.warning(
                            "[trade_liquidation][RECONCILE] qty mismatch %s %s: ledger=%.8f exchange=%.8f (pos_uid=%s)",
                            sym,
                            side,
                            float(led_qty),
                            float(ex_qty),
                            str(p.get("pos_uid") or ""),
                        )
                if usdt_diff > usdt_tol and bool(getattr(self.p, "reconcile_log_diffs", True)):
                    log.warning(
                        "[trade_liquidation][RECONCILE] value mismatch %s %s: ledger_usdt=%.8f exchange_usdt=%.8f diff=%.8f (pos_uid=%s)",
                        sym,
                        side,
                        float(led_val),
                        float(ex_val),
                        float(ex_val - led_val),
                        str(p.get("pos_uid") or ""),
                    )

                
                # Micro-step: optionally auto-adjust ledger qty_current and/or position_value_usdt to exchange values.
                # - qty_current adjustment is OFF by default (reconcile_auto_adjust_ledger_qty_current=False)
                # - value_usdt adjustment is ON by default (reconcile_auto_adjust_ledger_value_usdt=True) but only when diff > tolerance
                # - value_usdt refresh is OFF by default (reconcile_refresh_position_value_usdt=False) and keeps ledger mark-to-market (writes only if delta > reconcile_value_refresh_min_delta_usdt)
                if (auto_adjust_qty or auto_adjust_value or auto_refresh_value) and ex_qty > 0:
                    try:
                        pos_uid = str(p.get("pos_uid") or "")
                        if adjust_only_our and not self._is_our_position_by_symbol_side(sym, side, oo_all, oo_algo_all, p.get("raw_meta")):
                            log.info(
                                "[trade_liquidation][RECONCILE] skip auto-adjust qty/value for external pos_uid=%s (%s %s)",
                                pos_uid,
                                sym,
                                side,
                            )
                            continue

                        do_qty = bool(auto_adjust_qty) and (qty_diff > tol)
                        do_val = False
                        if ex_val > 0:
                            if bool(auto_refresh_value):
                                # refresh even when within tolerance (but avoid excessive writes)
                                if led_val <= 0:
                                    do_val = True
                                elif abs(ex_val - led_val) > float(refresh_min_delta):
                                    do_val = True
                            elif bool(auto_adjust_value):
                                # adjust only when beyond tolerance
                                if usdt_diff > usdt_tol:
                                    do_val = True

                        if not do_qty and not do_val:
                            continue

                        new_qty_f = float(ex_qty) if do_qty else float(led_qty)
                        new_val_d = _dec(abs(ex_val)).quantize(Decimal("0.00000001")) if do_val else _dec(led_val).quantize(Decimal("0.00000001"))

                        if self._pl_has_raw_meta:
                            meta = {
                                "reconcile_adjust": {
                                    "old_qty": float(led_qty),
                                    "new_qty": float(new_qty_f),
                                    "old_usdt": float(led_val),
                                    "new_usdt": float(new_val_d),
                                    "ts": _utc_now().isoformat(),
                        "follow_ts": _utc_now().isoformat(),
                                    "mode": ("qty" if do_qty else "") + ("+val" if do_val else ""),
                                }
                            }
                            self.store.execute(
                                """
                                UPDATE position_ledger
                                SET qty_current=%(q)s,
                                    position_value_usdt=%(v)s,
                                    updated_at=now(),
                                    raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb
                                WHERE exchange_id=%(ex)s
                                  AND account_id=%(acc)s
                                  AND pos_uid=%(pos_uid)s
                                  AND status='OPEN'
                                """,
                                {
                                    "q": float(new_qty_f),
                                    "v": new_val_d,
                                    "meta": json.dumps(meta),
                                    "ex": int(self.exchange_id),
                                    "acc": int(self.account_id),
                                    "pos_uid": pos_uid,
                                },
                            )
                        else:
                            self.store.execute(
                                """
                                UPDATE position_ledger
                                SET qty_current=%(q)s,
                                    position_value_usdt=%(v)s,
                                    updated_at=now()
                                WHERE exchange_id=%(ex)s
                                  AND account_id=%(acc)s
                                  AND pos_uid=%(pos_uid)s
                                  AND status='OPEN'
                                """,
                                {
                                    "q": float(new_qty_f),
                                    "v": new_val_d,
                                    "ex": int(self.exchange_id),
                                    "acc": int(self.account_id),
                                    "pos_uid": pos_uid,
                                },
                            )

                        if bool(getattr(self.p, "reconcile_log_diffs", True)):
                            log.info(
                                "[trade_liquidation][RECONCILE] adjusted ledger %s %s old_qty=%.8f new_qty=%.8f old_usdt=%.8f new_usdt=%.8f (pos_uid=%s)",
                                sym,
                                side,
                                float(led_qty),
                                float(new_qty_f),
                                float(led_val),
                                float(new_val_d),
                                pos_uid,
                            )
                    except Exception:
                        log.exception(
                            "[trade_liquidation][RECONCILE] failed to auto-adjust ledger qty/value (pos_uid=%s)",
                            str(p.get("pos_uid") or ""),
                        )


        # 3) external positions (on exchange, not in ledger)
        led_keys = set()
        for p in led:
            sid = int(p.get("symbol_id") or 0)
            sym = sym_map.get(sid)
            if not sym:
                continue
            side = str(p.get("side") or "").upper().strip()
            if side in {"LONG", "SHORT"}:
                led_keys.add((sym, side))

        for k in ex_pos.keys():
            if k not in led_keys:
                external += 1
                if not ignore_external:
                    log.warning("[trade_liquidation][RECONCILE] external exchange position: %s %s", k[0], k[1])
                else:
                    if ext_lvl == "info":
                        log.info("[trade_liquidation][RECONCILE] external exchange position: %s %s", k[0], k[1])
                    elif ext_lvl == "warning":
                        log.warning("[trade_liquidation][RECONCILE] external exchange position: %s %s", k[0], k[1])
                    else:
                        log.debug("[trade_liquidation][RECONCILE] external exchange position: %s %s", k[0], k[1])

        return {
            "ledger_open": int(len(led)),
            "exchange_open": int(len(ex_pos)),
            "missing_on_exchange": int(missing_on_exchange),
            "qty_mismatch": int(qty_mismatch),
            "closed_ledger": int(closed_ledger),
            "external": int(external),
        }

    def _reconcile_adds_vs_binance(self) -> Dict[str, int]:
        """Reconcile averaging adds (ADDn) between DB and Binance (best-effort).

        We use /fapi/v1/allOrders per symbol and detect executed TL_*_ADDn orders.
        If DB is behind, we:
          - upsert executed ADD orders into `orders` (by client_order_id)
          - bump `position_ledger.scale_in_count` to max executed ADDn (GREATEST)

        Throttled by `reconcile_binance_adds_every_sec` (default 300s).
        """

        stats: Dict[str, int] = {"checked": 0, "ledger_bumped": 0, "orders_upserted": 0, "symbols_failed": 0}
        if self._binance is None:
            return stats

        every = float(getattr(self.p, "reconcile_binance_adds_every_sec", 300.0) or 300.0)
        now = _utc_now()
        last = getattr(self, "_last_reconcile_adds_ts", None)
        try:
            if last is not None and (now - last).total_seconds() < max(5.0, every):
                return stats
        except Exception:
            pass
        self._last_reconcile_adds_ts = now

        # Cache symbol->symbol_id
        if not hasattr(self, "_symid_cache"):
            self._symid_cache = {}

        def _symbol_id(symbol: str) -> Optional[int]:
            s = str(symbol or "").upper()
            if not s:
                return None
            if s in self._symid_cache:
                return self._symid_cache[s]
            try:
                row = self.store.fetch_one(
                    "SELECT symbol_id FROM symbols WHERE exchange_id=%(ex)s AND symbol=%(sym)s LIMIT 1",
                    {"ex": int(self.exchange_id), "sym": s},
                )
                sid = int(row.get("symbol_id")) if isinstance(row, dict) and row.get("symbol_id") is not None else None
                self._symid_cache[s] = sid
                return sid
            except Exception:
                self._symid_cache[s] = None
                return None

        # Pull OPEN positions from ledger
        try:
            pos_rows = self.store.fetch_all(
                """
                SELECT pos_uid, symbol, COALESCE(scale_in_count,0) AS ledger_adds, opened_at
                FROM position_ledger
                WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND status='OPEN'
                ORDER BY opened_at DESC
                """,
                {"ex": int(self.exchange_id), "acc": int(self.account_id)},
            )
        except Exception:
            return stats

        add_re = re.compile(r"_ADD(\d+)$")

        for r in pos_rows if isinstance(pos_rows, list) else []:
            try:
                pos_uid = str((r or {}).get("pos_uid") or "")
                symbol = str((r or {}).get("symbol") or "").upper()
                ledger_adds = int((r or {}).get("ledger_adds") or 0)
                if not symbol or not pos_uid:
                    continue

                stats["checked"] += 1
                sid = _symbol_id(symbol)
                if sid is None:
                    continue

                # Narrow startTime around position open (reduce payload)
                start_ms = None
                try:
                    opened_at = (r or {}).get("opened_at")
                    if opened_at is not None:
                        start_ms = int(opened_at.timestamp() * 1000) - int(10 * 60 * 1000)
                except Exception:
                    start_ms = None

                orders = self._binance.all_orders(symbol=symbol, startTime=start_ms, limit=1000)

                max_add = 0
                upsert_rows: list[dict] = []
                for o in orders if isinstance(orders, list) else []:
                    cid = str(o.get("clientOrderId") or o.get("origClientOrderId") or "").strip()
                    if not cid:
                        continue
                    m = add_re.search(cid)
                    if not m:
                        continue
                    if not cid.startswith("TL_"):
                        continue

                    status = str(o.get("status") or "").upper()
                    try:
                        exec_qty = float(o.get("executedQty") or 0.0)
                    except Exception:
                        exec_qty = 0.0

                    if status not in ("FILLED", "PARTIALLY_FILLED") and exec_qty <= 0:
                        continue

                    n = int(m.group(1))
                    max_add = max(max_add, n)

                    oid = str(o.get("orderId") or "").strip()
                    if not oid:
                        continue

                    try:
                        qty = float(o.get("origQty") or o.get("origQuantity") or o.get("quantity") or 0.0)
                    except Exception:
                        qty = 0.0

                    try:
                        price = float(o.get("price") or 0.0)
                    except Exception:
                        price = 0.0

                    try:
                        avg_price = float(o.get("avgPrice") or o.get("avg_price") or 0.0)
                    except Exception:
                        avg_price = 0.0

                    try:
                        ts_ms = int(o.get("updateTime") or o.get("time") or o.get("transactTime") or 0)
                    except Exception:
                        ts_ms = 0

                    upsert_rows.append(
                        {
                            "exchange_id": int(self.exchange_id),
                            "account_id": int(self.account_id),
                            "symbol_id": int(sid),
                            "order_id": oid,
                            "client_order_id": cid,
                            "side": str(o.get("side") or ""),
                            "type": str(o.get("type") or ""),
                            "reduce_only": bool(o.get("reduceOnly") or o.get("reduce_only") or False),
                            "price": None if price == 0.0 else price,
                            "qty": qty,
                            "filled_qty": exec_qty,
                            "avg_price": None if avg_price == 0.0 else avg_price,
                            "status": status,
                            "strategy_id": str(self.STRATEGY_ID),
                            "pos_uid": pos_uid,
                            "ts_ms": ts_ms,
                            "raw_json": o,
                        }
                    )

                if upsert_rows:
                    self.store.upsert_orders(upsert_rows)
                    stats["orders_upserted"] += int(len(upsert_rows))

                if max_add > ledger_adds:
                    self.store.execute(
                        """
                        UPDATE position_ledger
                        SET scale_in_count = GREATEST(COALESCE(scale_in_count,0), %(n)s),
                            updated_at = now()
                        WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(uid)s AND status='OPEN'
                        """,
                        {"ex": int(self.exchange_id), "acc": int(self.account_id), "uid": pos_uid, "n": int(max_add)},
                    )
                    stats["ledger_bumped"] += 1

            except Exception:
                stats["symbols_failed"] += 1
                continue

        return stats

    def _auto_recovery_brackets(self) -> int:
        """Ensure protective orders exist for OPEN ledger positions.

        Uses snapshot "open_orders_all" + "position_risk".
        """
        if not self._is_live or self._binance is None:
            return 0

        sym_map = self._symbols_map()
        prefix = _sanitize_coid_prefix(str(getattr(self.p, "client_order_id_prefix", "TL") or "TL"))

        # --- bracket replace throttling / tolerance knobs ---
        REPLACE_COOLDOWN_SEC = float(getattr(self.p, "replace_cooldown_sec", 60) or 60)
        AVG_EPS_PCT = float(getattr(self.p, "avg_eps_pct", 0.02) or 0.02)  # percent
        PRICE_EPS_TICKS = int(getattr(self.p, "price_eps_ticks", 2) or 2)
        now_ts = time.time()

        max_age_h = float(getattr(self.p, "recovery_max_age_hours", 48) or 48)
        place_sl = bool(getattr(self.p, "recovery_place_sl", True))
        # Hedge strategy: do not auto-place SL brackets (hedge replaces SL)
        if bool(getattr(self.p, "hedge_enabled", False)):
            place_sl = False
        place_tp = bool(getattr(self.p, "recovery_place_tp", True))
        place_trailing = bool(getattr(self.p, "recovery_place_trailing", True))
        hedge_mode = bool(getattr(self.p, "hedge_enabled", False))

        # Open orders snapshot
        open_all = self._rest_snapshot_get("open_orders_all")
        open_rows = open_all if isinstance(open_all, list) else []
        open_by_symbol: Dict[str, set[str]] = {}
        for oo in open_rows:
            sym = str(oo.get("symbol") or "").strip()
            if not sym:
                continue
            open_by_symbol.setdefault(sym, set()).add(str(oo.get("clientOrderId") or ""))

        # Algo open orders snapshot (conditional orders live here)
        open_algo_all = self._rest_snapshot_get("open_algo_orders_all")
        open_algo_rows = open_algo_all if isinstance(open_algo_all, list) else []
        open_algo_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
        for ao in open_algo_rows:
            sym = str((ao or {}).get("symbol") or "").strip()
            if not sym:
                continue
            open_algo_by_symbol.setdefault(sym, []).append(dict(ao))

        # Algo (conditional) orders snapshot: SL/TP/trailing live here
        algo_all = self._rest_snapshot_get("open_algo_orders_all")
        algo_rows = algo_all if isinstance(algo_all, list) else []
        for ao in algo_rows:
            sym = str(ao.get("symbol") or "").strip()
            if not sym:
                continue
            # Binance returns clientAlgoId for algo orders
            open_by_symbol.setdefault(sym, set()).add(str(ao.get("clientAlgoId") or ""))

        # Exchange positions snapshot (for positionSide in hedge, and to skip already-closed positions)
        pr = self._rest_snapshot_get("position_risk")
        pr_rows = pr if isinstance(pr, list) else []
        ex_has_pos: set[Tuple[str, str]] = set()
        # Current exchange quantity by (symbol, side) from positionRisk.
        # Used to place trailing stops / qty-based TP when ledger qty_current may be stale (partial close etc.).
        ex_qty_map: Dict[Tuple[str, str], float] = {}
        ex_mark_map: Dict[Tuple[str, str], float] = {}
        ex_entry_map: Dict[Tuple[str, str], float] = {}
        for it in pr_rows:
            sym = str(it.get("symbol") or "").strip()
            if not sym:
                continue
            amt = _safe_float(it.get("positionAmt"), 0.0)
            if abs(amt) <= 0:
                continue
            ps = str(it.get("positionSide") or "").upper().strip()
            if ps in {"LONG", "SHORT"}:
                side = ps
            else:
                side = "LONG" if amt > 0 else "SHORT"
            ex_has_pos.add((sym, side))
            ex_qty_map[(sym, side)] = abs(float(amt))
            try:
                ex_mark_map[(sym, side)] = abs(float(it.get('markPrice') or 0.0))
            except Exception:
                pass
            try:
                ex_entry_map[(sym, side)] = abs(float(it.get('entryPrice') or it.get('avgEntryPrice') or it.get('avgPrice') or 0.0))
            except Exception:
                pass

        # Ledger OPEN positions (main recovery loop works on OPEN rows only)
        led = list(
            self.store.query_dict(
                """
            SELECT symbol_id, pos_uid, side, status, qty_current, entry_price, avg_price, raw_meta, opened_at, source
                FROM position_ledger
                WHERE exchange_id=%(ex)s
                  AND account_id=%(acc)s
                  AND strategy_id=%(sid)s
                  AND status='OPEN'
                ORDER BY opened_at DESC
                """,
                {"ex": int(self.exchange_id), "acc": int(self.account_id), "sid": str(self.STRATEGY_ID)},
            )
        )

        # Separate snapshot for hedge ownership discovery.
        # HEDGE_TRL can be managed from a CLOSED live main row while the hedge leg is still open
        # on exchange, so building hedge_managed_sides only from OPEN rows is insufficient.
        led_live_for_hedge = list(
            self.store.query_dict(
                """
            SELECT symbol_id, pos_uid, side, status, qty_current, entry_price, avg_price, raw_meta, opened_at, source
                FROM position_ledger
                WHERE exchange_id=%(ex)s
                  AND account_id=%(acc)s
                  AND strategy_id=%(sid)s
                  AND source='live'
                  AND status IN ('OPEN','CLOSED')
                ORDER BY opened_at DESC
                """,
                {"ex": int(self.exchange_id), "acc": int(self.account_id), "sid": str(self.STRATEGY_ID)},
            )
        )

        # Precompute symbol/side pairs that are hedge-managed by another OPEN live position.
        # For any OPEN live ledger row with raw_meta.hedge.base_qty > 0, the opposite side is
        # controlled by _live_hedge_manage() and must NOT receive generic RECOVERY TRAILING/ADD.
        hedge_managed_sides: set[tuple[str, str]] = set()
        try:
            for _lp in led_live_for_hedge:
                try:
                    if str(_lp.get("status") or "").upper().strip() != "OPEN":
                        continue
                    _sid = int(_lp.get("symbol_id") or 0)
                    _sym = sym_map.get(_sid)
                    if not _sym:
                        continue
                    _side = str(_lp.get("side") or "").upper().strip()
                    if _side not in {"LONG", "SHORT"}:
                        continue
                    _rm = _lp.get("raw_meta") or {}
                    if isinstance(_rm, str):
                        try:
                            import json as _json
                            _rm = _json.loads(_rm) if _rm.strip() else {}
                        except Exception:
                            _rm = {}
                    _h = (_rm.get("hedge") or {}) if isinstance(_rm, dict) else {}
                    _bq = float(_h.get("base_qty") or 0.0) if isinstance(_h, dict) else 0.0
                    if _bq > 0.0:
                        # raw_meta.hedge may be attached either to the hedge-managed leg itself
                        # or to the main leg that owns the hedge state. To be robust across both
                        # layouts, exclude both the current side and its opposite side from
                        # generic RECOVERY TRAILING/ADD recovery.
                        hedge_managed_sides.add((_sym, _side))
                        _opp = "SHORT" if _side == "LONG" else "LONG"
                        hedge_managed_sides.add((_sym, _opp))
                except Exception:
                    continue
        except Exception:
            hedge_managed_sides = set()

        # --- Reconcile DB: if TP/TRL/SL were manually canceled on the exchange,
        # the DB may still show them as NEW and the trader will think protection exists.
        # We treat the exchange as source of truth and mark such stale DB rows as CANCELED.
        try:
            active_client_ids: set[str] = set()
            # regular orders (openOrders): open_by_symbol already stores clientOrderId strings
            for _ids in open_by_symbol.values():
                try:
                    active_client_ids.update(str(x) for x in (_ids or []) if x)
                except Exception:
                    pass

            # conditional orders (openAlgoOrders): open_algo_by_symbol stores list[dict]
            for _rows in open_algo_by_symbol.values():
                try:
                    for o in (_rows or []):
                        if isinstance(o, dict):
                            cid = o.get("clientAlgoId") or o.get("clientOrderId") or o.get("newClientOrderId")
                            if cid:
                                active_client_ids.add(str(cid))
                        elif o:
                            # if upstream ever stores ids directly
                            active_client_ids.add(str(o))
                except Exception:
                    pass

            pos_uids = [str(r.get("pos_uid")) for r in led if r.get("pos_uid")]
            if pos_uids:
                like_tp = r"TL\_%\_TP%"
                like_trl = r"TL\_%\_TRL%"
                like_sl = r"TL\_%\_SL%"

                if active_client_ids:
                    self.store.execute(
                        '''
                        UPDATE public.orders
                           SET status = 'CANCELED',
                               updated_at = NOW(),
                               source = 'reconcile_exchange'
                         WHERE strategy_id = 'trade_liquidation'
                           AND pos_uid = ANY(%s)
                           AND status IN ('NEW','PARTIALLY_FILLED')
                           AND (
                                 client_order_id LIKE %s ESCAPE '\\'
                              OR client_order_id LIKE %s ESCAPE '\\'
                              OR client_order_id LIKE %s ESCAPE '\\'
                           )
                           AND NOT (client_order_id = ANY(%s))
                        ''',
                        (pos_uids, like_tp, like_trl, like_sl, list(active_client_ids)),
                    )
                else:
                    # No active TP/TRL/SL on exchange → cancel all DB-protective orders for these positions
                    self.store.execute(
                        '''
                        UPDATE public.orders
                           SET status = 'CANCELED',
                               updated_at = NOW(),
                               source = 'reconcile_exchange'
                         WHERE strategy_id = 'trade_liquidation'
                           AND pos_uid = ANY(%s)
                           AND status IN ('NEW','PARTIALLY_FILLED')
                           AND (
                                 client_order_id LIKE %s ESCAPE '\\'
                              OR client_order_id LIKE %s ESCAPE '\\'
                              OR client_order_id LIKE %s ESCAPE '\\'
                           )
                        ''',
                        (pos_uids, like_tp, like_trl, like_sl),
                    )
        except Exception:
            log.exception("[TL][auto-recovery] reconcile stale TP/TRL/SL in DB failed")

        now = _utc_now()
        recovered = 0
        recovery_actions = 0
        max_symbols = int(getattr(self.p, "max_recovery_symbols_per_cycle", 5) or 5)
        max_actions = int(getattr(self.p, "max_recovery_actions_per_cycle", 6) or 6)
        led = self._cycle_subset(led or [], "recovery_positions", max_symbols)

        def _reserve_recovery_action() -> bool:
            nonlocal recovery_actions
            if recovery_actions >= max_actions:
                return False
            recovery_actions += 1
            return True

        def _rollback_recovery_action() -> None:
            nonlocal recovery_actions
            recovery_actions = max(0, int(recovery_actions) - 1)

        # In hedge mode, a symbol side can be managed as a dedicated hedge leg of the
        # opposite main position. In that case generic main TRAILING recovery must not
        # place a second _TRL on the same (symbol, positionSide), otherwise recovery and
        # hedge manager will duplicate trailing activation orders.
        hedge_managed_sides: set[tuple[str, str]] = set()
        two_sided_symbols: set[str] = set()
        if hedge_mode:
            try:
                sides_by_symbol: Dict[str, set[str]] = {}
                for sym_i, side_i in ex_has_pos:
                    sides_by_symbol.setdefault(str(sym_i).upper(), set()).add(str(side_i).upper())
                two_sided_symbols = {sym_i for sym_i, sides_i in sides_by_symbol.items() if {"LONG", "SHORT"}.issubset(sides_i)}
            except Exception:
                two_sided_symbols = set()
            try:
                for lp in led_live_for_hedge:
                    try:
                        sym_i = sym_map.get(int(lp.get("symbol_id") or 0))
                        side_i = str(lp.get("side") or "").upper().strip()
                        if (not sym_i) or side_i not in {"LONG", "SHORT"}:
                            continue
                        sym_u_i = str(sym_i).upper()
                        if sym_u_i in two_sided_symbols:
                            hedge_managed_sides.add((sym_u_i, side_i))
                        rm_i = lp.get("raw_meta") or {}
                        if isinstance(rm_i, str):
                            try:
                                import json as _json
                                rm_i = _json.loads(rm_i) if rm_i.strip() else {}
                            except Exception:
                                rm_i = {}
                        if not isinstance(rm_i, dict):
                            continue
                        hedge_meta = rm_i.get("hedge") or {}
                        if not isinstance(hedge_meta, dict):
                            continue
                        base_qty = float(hedge_meta.get("base_qty") or 0.0)
                        if base_qty <= 0.0:
                            continue
                        # raw_meta.hedge can live either on the hedge leg itself or on the main
                        # leg that controls hedge lifecycle. Exclude both sides here to avoid
                        # generic RECOVERY TRAILING/ADD1 racing with dedicated HEDGE_TRL logic.
                        hedge_managed_sides.add((sym_u_i, side_i))
                        opp_i = "SHORT" if side_i == "LONG" else "LONG"
                        hedge_managed_sides.add((sym_u_i, opp_i))
                    except Exception:
                        continue
            except Exception:
                hedge_managed_sides = set()

        def _tl_safe_trigger_price(*, symbol: str, close_side: str, desired: float, mark: float, tick: float, buffer_pct: float) -> float:
            """Ensure conditional trigger price won't immediately trigger.

            For close_side=SELL (closing LONG): trigger/activation must be ABOVE current mark.
            For close_side=BUY  (closing SHORT): trigger/activation must be BELOW current mark.
            """
            p = float(desired or 0.0)
            m = float(mark or 0.0)
            buf = float(buffer_pct or 0.0) / 100.0
            if m > 0:
                if str(close_side).upper() == "SELL":
                    # must be above mark
                    floor = m * (1.0 + max(buf, 0.0005))
                    p = max(p, floor)
                    # round UP so it stays above
                    if tick and tick > 0:
                        p = _round_price_to_tick(p, tick, mode="up")
                else:
                    # BUY: must be below mark
                    ceil = m * (1.0 - max(buf, 0.0005))
                    p = min(p if p > 0 else ceil, ceil)
                    # round DOWN so it stays below
                    if tick and tick > 0:
                        p = _round_price_to_tick(p, tick, mode="down")
            else:
                # no mark -> just tick rounding in the safe direction
                if tick and tick > 0:
                    p = _round_price_to_tick(p, tick, mode="up" if str(close_side).upper() == "SELL" else "down")
            return float(p)

        def _tl_place_algo_with_retry(place_fn, *, max_tries: int = 3):
            """Retry placing conditional orders when Binance says 'would immediately trigger' (-2021)."""
            last = None
            for i in range(int(max_tries or 1)):
                try:
                    return place_fn(i)
                except Exception as e:
                    last = e
                    s = str(e)
                    if ("-2021" in s) or ("immediately trigger" in s.lower()):
                        continue
                    raise
            if last:
                raise last
            return None


        for p in led:
            if recovery_actions >= max_actions:
                break
            try:
                opened_at = p.get("opened_at")
                if isinstance(opened_at, datetime):
                    age_h = (now - opened_at.replace(tzinfo=timezone.utc) if opened_at.tzinfo is None else now - opened_at.astimezone(timezone.utc)).total_seconds() / 3600.0
                    if age_h > max_age_h:
                        log.info("[trade_liquidation][RECOVERY] skip pos_uid=%s reason=max_age age_h=%.2f max_age_h=%.2f", str(p.get("pos_uid") or ""), float(age_h), float(max_age_h))
                        continue
            except Exception:
                pass

            symbol_id = int(p.get("symbol_id") or 0)
            sym = sym_map.get(symbol_id)
            if not sym:
                continue
            side = str(p.get("side") or "").upper().strip()
            if side not in {"LONG", "SHORT"}:
                continue

            pos_uid = str(p.get("pos_uid") or "").strip()
            if not pos_uid:
                continue
            raw_meta = p.get("raw_meta") or {}
            if isinstance(raw_meta, str):
                try:
                    import json as _json
                    raw_meta = _json.loads(raw_meta) if raw_meta.strip() else {}
                except Exception:
                    raw_meta = {}
            qty_ex = abs(_safe_float(ex_qty_map.get((sym, side)), 0.0))
            qty_led = abs(_safe_float(p.get("qty_current"), 0.0))
            qty_use = float(qty_ex if qty_ex > 0 else qty_led)
            entry_ledger = _safe_float(p.get("entry_price"), 0.0)
            entry = float(ex_entry_map.get((sym, side), entry_ledger) or 0.0)

            src_kind = str(p.get("source") or "live").lower().strip()
            if src_kind == "live_hedge":
                # Hedge legs are managed by hedge_logic, but auto-recovery must still be able
                # to restore missing HEDGE_TRL / HEDGE_SL after restart or manual cancel.
                try:
                    hedge_ps = str(side).upper()
                    base_pos_uid = ""
                    try:
                        _rm_h = raw_meta or {}
                        if isinstance(_rm_h, str):
                            _rm_h = json.loads(_rm_h) if _rm_h.strip() else {}
                        if isinstance(_rm_h, dict):
                            _hm = _rm_h.get("hedge_mirror") or {}
                            if isinstance(_hm, dict):
                                base_pos_uid = str(_hm.get("base_pos_uid") or "").strip()
                    except Exception:
                        base_pos_uid = ""
                    if not base_pos_uid:
                        try:
                            _row = self.store.fetch_one(
                                """
                                SELECT hl.base_pos_uid
                                  FROM hedge_links hl
                                 WHERE hl.exchange_id=%s
                                   AND hl.hedge_account_id=%s
                                   AND hl.symbol_id=%s
                                   AND hl.hedge_pos_uid=%s
                                 ORDER BY hl.created_at DESC NULLS LAST
                                 LIMIT 1
                                """,
                                (int(self.exchange_id), int(self.account_id), int(symbol_id), str(pos_uid)),
                            )
                            base_pos_uid = str((_row or {}).get("base_pos_uid") or "").strip()
                        except Exception:
                            base_pos_uid = ""
                    if base_pos_uid:
                        base_pos = None
                        try:
                            base_pos = self.store.fetch_one(
                                """
                                SELECT symbol_id, pos_uid, side, status, qty_current, entry_price, avg_price, raw_meta, opened_at, source
                                  FROM position_ledger
                                 WHERE exchange_id=%s AND account_id=%s AND pos_uid=%s
                                 ORDER BY updated_at DESC NULLS LAST, opened_at DESC NULLS LAST
                                 LIMIT 1
                                """,
                                (int(self.exchange_id), int(self.account_id), str(base_pos_uid)),
                            )
                        except Exception:
                            base_pos = None
                        base_pos = dict(base_pos or {})
                        if not base_pos.get("pos_uid"):
                            base_pos["pos_uid"] = str(base_pos_uid)
                        tok_h = _coid_token(str(base_pos_uid), n=20)
                        cid_htrl = f"{prefix}_{tok_h}_HEDGE_TRL"
                        cid_hsl = f"{prefix}_{tok_h}_HEDGE_SL"
                        hedge_close_side = "BUY" if hedge_ps == "SHORT" else "SELL"
                        has_htrl = self._find_known_algo_order_by_client_id(sym, cid_htrl) is not None
                        if not has_htrl:
                            has_htrl = self._find_semantic_open_algo(
                                sym,
                                client_suffix="_HEDGE_TRL",
                                position_side=hedge_ps,
                                order_type="TRAILING_STOP_MARKET",
                                side=hedge_close_side,
                                close_position=False,
                            ) is not None
                        has_hsl = self._find_known_algo_order_by_client_id(sym, cid_hsl) is not None
                        if not has_hsl:
                            has_hsl = self._find_semantic_open_algo(
                                sym,
                                client_suffix="_HEDGE_SL",
                                position_side=hedge_ps,
                                order_type="STOP_MARKET",
                                side=hedge_close_side,
                                close_position=True,
                            ) is not None
                        need_htrl = bool(place_trailing) and bool(getattr(self.p, "hedge_trailing_enabled", False)) and (not has_htrl)
                        need_hsl = bool(getattr(self.p, "recovery_place_sl", True)) and float(getattr(self.p, "hedge_stop_loss_pct", 0.0) or 0.0) > 0.0 and (not has_hsl)
                        if need_htrl or need_hsl:
                            log.info(
                                "[trade_liquidation][RECOVERY][HEDGE] restoring %s %s base_pos_uid=%s hedge_pos_uid=%s need_hsl=%s need_htrl=%s",
                                sym, hedge_ps, str(base_pos_uid), str(pos_uid), bool(need_hsl), bool(need_htrl),
                            )
                            if _reserve_recovery_action():
                                before_htrl = has_htrl
                                before_hsl = has_hsl
                                try:
                                    self._ensure_immediate_hedge_protection(
                                        base_pos=base_pos,
                                        symbol=sym,
                                        symbol_id=int(symbol_id),
                                        hedge_ps=hedge_ps,
                                        hedge_amt=float(qty_use or 0.0),
                                        hedge_entry=float(entry or avg_price or 0.0),
                                        mark=float(ex_mark_map.get((sym, side)) or 0.0),
                                        allow_trailing=bool(need_htrl),
                                        allow_sl=bool(need_hsl),
                                    )
                                    after_htrl = self._find_known_algo_order_by_client_id(sym, cid_htrl) is not None
                                    if (not after_htrl) and need_htrl:
                                        after_htrl = self._find_semantic_open_algo(
                                            sym,
                                            client_suffix="_HEDGE_TRL",
                                            position_side=hedge_ps,
                                            order_type="TRAILING_STOP_MARKET",
                                            side=hedge_close_side,
                                            close_position=False,
                                        ) is not None
                                    after_hsl = self._find_known_algo_order_by_client_id(sym, cid_hsl) is not None
                                    if (not after_hsl) and need_hsl:
                                        after_hsl = self._find_semantic_open_algo(
                                            sym,
                                            client_suffix="_HEDGE_SL",
                                            position_side=hedge_ps,
                                            order_type="STOP_MARKET",
                                            side=hedge_close_side,
                                            close_position=True,
                                        ) is not None
                                    recovered += int(bool(need_htrl and (not before_htrl) and after_htrl))
                                    recovered += int(bool(need_hsl and (not before_hsl) and after_hsl))
                                except Exception:
                                    _rollback_recovery_action()
                                    raise
                    else:
                        log.info(
                            "[trade_liquidation][RECOVERY][HEDGE] skip %s %s hedge_pos_uid=%s reason=no_base_binding",
                            sym, hedge_ps, str(pos_uid),
                        )
                except Exception:
                    log.exception("[trade_liquidation][RECOVERY][HEDGE] failed for %s %s pos_uid=%s", sym, side, pos_uid)
                continue

            is_hedge_managed_side = (sym, side) in hedge_managed_sides

            # If exchange says no position - skip recovery (reconcile will handle ledger close)
            if (sym, side) not in ex_has_pos:
                log.info("[trade_liquidation][RECOVERY] skip %s %s pos_uid=%s reason=no_exchange_position", sym, side, str(p.get("pos_uid") or ""))
                continue

            # Use exchange qty for protective orders (especially important for TRAILING_STOP_MARKET and limit TP),
            # because ledger qty_current can lag after partial closes / external reduce-only fills.
            qty_ex = abs(_safe_float(ex_qty_map.get((sym, side)), 0.0))
            avg_price = _safe_float(p.get("avg_price"), 0.0)
            raw_meta = p.get("raw_meta") or {}
            if isinstance(raw_meta, str):
                try:
                    import json as _json
                    raw_meta = _json.loads(raw_meta) if raw_meta.strip() else {}
                except Exception:
                    raw_meta = {}
            qty_led = abs(_safe_float(p.get("qty_current"), 0.0))
            qty_use = float(qty_ex if qty_ex > 0 else qty_led)
            if qty_use <= 0:
                continue

            pos_uid = str(p.get("pos_uid") or "").strip()
            if not pos_uid:
                continue

            tok = _coid_token(pos_uid, n=20)
            cid_sl = f"{prefix}_{tok}_SL"
            cid_tp = f"{prefix}_{tok}_TP"
            cid_trl = f"{prefix}_{tok}_TRL"

            close_side = "SELL" if side == "LONG" else "BUY"
            position_side = side if hedge_mode else "BOTH"


            
            existing = open_by_symbol.get(sym, set())

            # Also detect *any* existing protective orders on Binance for this symbol/side,
            # even if their client ids are not ours. This prevents Binance error -4130
            # ("An open stop or take profit order with GTE and closePosition ... is existing.")
            def _to_bool(x: Any) -> bool:
                if isinstance(x, bool):
                    return bool(x)
                if x is None:
                    return False
                s = str(x).strip().lower()
                return s in {"1", "true", "yes", "y", "on"}

            def _detect_existing_protection() -> tuple[bool, bool, bool]:
                rows = open_algo_by_symbol.get(sym, []) if isinstance(open_algo_by_symbol, dict) else []
                has_any_sl = False
                has_any_tp = False
                has_any_trl = False
                for o in rows:
                    try:
                        otype = str(o.get("orderType") or o.get("type") or "").upper()
                        oside = str(o.get("side") or "").upper()
                        ops = str(o.get("positionSide") or "").upper()
                        # In hedge mode positionSide is LONG/SHORT; otherwise it can be BOTH/empty.
                        if hedge_mode:
                            if ops and ops not in {position_side, "BOTH"}:
                                continue
                        # Make sure it's the close direction
                        if oside and oside != close_side:
                            continue

                        close_pos = _to_bool(o.get("closePosition"))

                        # TP: closePosition=true TP orders (Binance typically enforces one)
                        if otype in {"TAKE_PROFIT_MARKET", "TAKE_PROFIT"} and close_pos:
                            has_any_tp = True

                        # SL: stop-market/stop with closePosition OR any trailing stop
                        if otype in {"STOP_MARKET", "STOP"} and close_pos:
                            has_any_sl = True
                        if otype == "TRAILING_STOP_MARKET":
                            has_any_trl = True
                    except Exception:
                        continue
                return has_any_sl, has_any_tp, has_any_trl

            any_sl, any_tp, any_trl = _detect_existing_protection()

            has_sl = (cid_sl in existing) or bool(any_sl)
            has_tp = (cid_tp in existing) or bool(any_tp)
            has_trl = (cid_trl in existing) or bool(any_trl)
            main_partial_tp_enabled = bool(getattr(self.p, "main_partial_tp_enabled", False))

            # Strong dedupe for our own protective algo orders. Exchange snapshots can lag,
            # and without DB/local markers the next cycle may re-place the same TP/TRL.
            if (not main_partial_tp_enabled) and (not has_tp):
                tp_known = self._find_known_algo_order_by_client_id(sym, cid_tp)
                if tp_known is None:
                    tp_known = self._find_semantic_open_algo(
                        sym,
                        client_suffix="_TP",
                        position_side=position_side if hedge_mode else None,
                        order_type="TAKE_PROFIT_MARKET",
                        side=close_side,
                    )
                has_tp = tp_known is not None
            if not has_trl:
                trl_known = self._find_known_algo_order_by_client_id(sym, cid_trl)
                if trl_known is None:
                    trl_known = self._find_semantic_open_algo(
                        sym,
                        client_suffix="_TRL",
                        position_side=position_side if hedge_mode else None,
                        order_type="TRAILING_STOP_MARKET",
                        side=close_side,
                    )
                has_trl = trl_known is not None

            # Compute protective prices from the *actual* average entry.
            # Prefer exchange-reported entryPrice (positionRisk) when available.
            entry_ledger = _safe_float(p.get("entry_price"), 0.0)
            entry = float(ex_entry_map.get((sym, side), entry_ledger) or 0.0)
            # price tick for proper rounding (used for TP / trailing activation sanity checks)
            try:
                _tick_dec = self._price_tick_for_symbol(sym)
                tick = float(_tick_dec) if (_tick_dec is not None and float(_tick_dec) > 0) else 0.0
            except Exception:
                tick = 0.0
            if not tick or tick <= 0:
                tick = 1e-8
            if entry <= 0:
                continue

            if side == "LONG":
                sl_price = entry * (1.0 - _pct_to_mult(self.p.stop_loss_pct))
                tp_price = entry * (1.0 + _pct_to_mult(self.p.take_profit_pct))
            else:
                sl_price = entry * (1.0 + _pct_to_mult(self.p.stop_loss_pct))
                tp_price = entry * (1.0 - _pct_to_mult(self.p.take_profit_pct))

            # Hard guarantee: TP must be on the correct side of entry.
            # (Some safety/rounding logic can otherwise push it across.)
            tp_price = _ensure_tp_trail_side(tp_price, entry, tick, side, kind="tp")
            if main_partial_tp_enabled:
                try:
                    tp_order = None
                    for ao in (open_algo_by_symbol.get(sym) or []):
                        if str(ao.get("clientAlgoId") or "") == cid_tp:
                            tp_order = ao
                            break
                    if tp_order is not None:
                        _last = float(self._bracket_replace_last_ts.get((pos_uid, "tp"), 0.0) or 0.0)
                        if (now_ts - _last) >= float(REPLACE_COOLDOWN_SEC):
                            try:
                                self._cancel_algo_by_client_id_safe(sym, cid_tp)
                            except Exception:
                                pass
                            has_tp = False
                            self._bracket_replace_last_ts[(pos_uid, "tp")] = now_ts
                            log.info("[trade_liquidation][RECOVER] canceled base TP for %s %s because main_partial_tp_enabled=true", sym, side)
                except Exception:
                    pass
            else:
                # If our TP exists but trigger price no longer matches (e.g. after averaging fill),
                # cancel it so we can recreate at the new correct level.
                try:
                    tp_order = None
                    for ao in (open_algo_by_symbol.get(sym) or []):
                        if str(ao.get("clientAlgoId") or "") == cid_tp:
                            tp_order = ao
                            break
                    if tp_order is not None:
                        try:
                            tp_trg = float(tp_order.get("triggerPrice") or tp_order.get("stopPrice") or 0.0)
                        except Exception:
                            tp_trg = 0.0
                        tol = max(
                            float(tick or 0.0) * float(PRICE_EPS_TICKS),
                            abs(float(tp_price)) * (float(AVG_EPS_PCT) / 100.0),
                            1e-12,
                        )
                        if tp_trg > 0.0 and abs(tp_trg - float(tp_price)) > tol:
                            # cancel stale TP (throttle to avoid churn)
                            _last = float(self._bracket_replace_last_ts.get((pos_uid, "tp"), 0.0) or 0.0)
                            if (now_ts - _last) >= float(REPLACE_COOLDOWN_SEC):
                                try:
                                    self._cancel_algo_by_client_id_safe(sym, cid_tp)
                                except Exception:
                                    pass
                                has_tp = False
                                self._bracket_replace_last_ts[(pos_uid, "tp")] = now_ts
                except Exception:
                    pass

                # Ensure TP exists (and matches current avg/entry-based level)
                if not has_tp:
                    try:
                        tp_side = "SELL" if side == "LONG" else "BUY"
                        resp_tp = self._binance.new_algo_order(
                            symbol=sym,
                            side=tp_side,
                            type="TAKE_PROFIT_MARKET",
                            closePosition="true",
                            workingType="MARK_PRICE",
                            positionSide=side,
                            algoType="CONDITIONAL",
                            clientAlgoId=cid_tp,
                            triggerPrice=str(tp_price),
                        )
                        try:
                            self._upsert_algo_order_shadow(resp_tp, pos_uid=pos_uid, strategy_id=self.STRATEGY_ID)
                        except Exception:
                            pass
                        try:
                            self._mark_local_algo_open(
                                cid_tp,
                                symbol=sym,
                                position_side=position_side,
                                order_type="TAKE_PROFIT_MARKET",
                                side=tp_side,
                                quantity=float(qty_use),
                                trigger_price=float(tp_price),
                                close_position=True,
                                pos_uid=pos_uid,
                            )
                        except Exception:
                            pass
                        has_tp = True
                    except Exception as _e:
                        log.warning("[trade_liquidation][RECOVER] failed to place TP %s %s: %s", sym, side, _e)

            # place missing orders
            
            if place_sl and (not has_sl) and sl_price > 0:
                # Defer STOP_MARKET SL until last averaging add (if enabled)
                defer_sl = bool(getattr(self.p, "defer_stop_loss_until_last_add", False))
                avg_enabled = bool(getattr(self.p, "averaging_enabled", False))
                cfg_max_adds = int(self._cfg_max_adds() or 0)
                allow_place_sl = True
                if avg_enabled and defer_sl and cfg_max_adds > 0:
                    try:
                        rm = p.get("raw_meta") or {}
                        st = (rm.get("avg_state") or {}) if isinstance(rm, dict) else {}
                        adds_done = int(st.get("adds_done", 0) or 0)
                        max_adds = int(st.get("max_adds", cfg_max_adds) or cfg_max_adds)
                        scale_in = int(p.get("scale_in_count", 0) or 0)
                        if max_adds > 0 and min(adds_done, scale_in) < max_adds:
                            allow_place_sl = False
                    except Exception:
                        allow_place_sl = False
                if allow_place_sl:
                    if not _reserve_recovery_action():
                        break
                    try:
                        if avg_enabled and defer_sl and cfg_max_adds > 0:
                            rm = p.get("raw_meta") or {}
                            st = (rm.get("avg_state") or {}) if isinstance(rm, dict) else {}
                            last_add_fill = _safe_float(st.get("last_add_fill_price"), 0.0)
                            dist_pct = float(getattr(self.p, "sl_after_last_add_distance_pct", 0.0) or 0.0)
                            if last_add_fill > 0 and dist_pct > 0:
                                if side == "LONG":
                                    sl_price = last_add_fill * (1.0 - _pct_to_mult(dist_pct))
                                else:
                                    sl_price = last_add_fill * (1.0 + _pct_to_mult(dist_pct))
                                log.info("[trade_liquidation][RECOVER] using deferred main SL from last add %s %s last_add=%.8f dist_pct=%.4f sl=%.8f", sym, side, float(last_add_fill), float(dist_pct), float(sl_price))
                        sl_mode = str(getattr(self.p, "sl_order_mode", "stop_market") or "stop_market").strip().lower()
                        if sl_mode in {"trailing_stop_market", "trailing", "tsm"} and bool(getattr(self.p, "trailing_enabled", True)):
                            callback_rate = float(getattr(self.p, "trailing_trail_pct", 0.6) or 0.6)
                            activation_pct = float(getattr(self.p, "trailing_activation_pct", 1.2) or 1.2)
                            buffer_pct = float(getattr(self.p, "trailing_activation_buffer_pct", 0.25) or 0.25)

                            # Get markPrice from exchange snapshot if available (best-effort).
                            mark = _safe_float(ex_mark_map.get((sym, side)), 0.0)

                            # For SELL trailing stop, activation must be ABOVE current mark.
                            # For BUY trailing stop, activation must be BELOW current mark.
                            if close_side.upper() == "SELL":
                                desired_act = entry * (1.0 + activation_pct / 100.0)
                            else:
                                desired_act = entry * (1.0 - activation_pct / 100.0)

                            # Ensure activation is on correct side of the *actual* entry.
                            desired_act = _ensure_tp_trail_side(desired_act, entry, tick, side, kind="trail_activate")

                            tick_dec = self._price_tick_for_symbol(sym)
                            tick = float(tick_dec) if tick_dec is not None else 0.0

                            def _place(i_try: int):
                                # increase buffer a bit on each retry
                                buf = buffer_pct * (1.0 + 0.7 * float(i_try))
                                activation_price = _tl_safe_trigger_price(
                                    symbol=sym,
                                    close_side=close_side,
                                    desired=desired_act,
                                    mark=mark,
                                    tick=tick,
                                    buffer_pct=buf,
                                )

                                # Round and enforce side vs actual entry.
                                activation_price = _round_price_to_tick(
                                    activation_price,
                                    tick,
                                    mode="up" if close_side.upper() == "SELL" else "down",
                                )
                                activation_price = _ensure_tp_trail_side(
                                    activation_price,
                                    entry,
                                    tick,
                                    side,
                                    kind="trail_activate",
                                )
                                # Guard: never place trailing without an open MAIN position on the exchange
                                try:
                                    _qty_tmp = float(qty_use or 0.0)
                                except Exception:
                                    _qty_tmp = 0.0
                                if _qty_tmp <= 0.0:
                                    self._dlog("skip TRL: qty<=0 sym=%s side=%s", sym, position_side)
                                    return None
                                if hedge_mode and (not self._has_open_position_live(sym, str(position_side))):
                                    # Main leg is already flat on exchange; ensure we don't leave a dangling TRL
                                    self._dlog("skip TRL: no open MAIN position sym=%s side=%s", sym, position_side)
                                    try:
                                        self._cancel_algo_by_client_id_safe(sym, cid_trl)
                                    except Exception:
                                        pass
                                    return None
                                if (not hedge_mode):
                                    # One-way mode: infer by requested side via position_side if given, else allow
                                    if position_side:
                                        if (not self._has_open_position_live(sym, str(position_side))):
                                            self._dlog("skip TRL: no open position sym=%s side=%s", sym, position_side)
                                            try:
                                                self._cancel_algo_by_client_id_safe(sym, cid_trl)
                                            except Exception:
                                                pass
                                            return None

                                params = dict(
                                    symbol=sym,
                                    side=close_side,
                                    type="TRAILING_STOP_MARKET",
                                    quantity=float(qty_use),
                                    activationPrice=float(activation_price),
                                    callbackRate=float(callback_rate),
                                    workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                                    newClientOrderId=cid_trl,
                                    positionSide=position_side if hedge_mode else None,
                                )
                                if (not hedge_mode) and bool(self.p.reduce_only):
                                    params["reduceOnly"] = True
                                return self._binance.new_order(**params)

                            resp = _tl_place_algo_with_retry(_place, max_tries=int(getattr(self.p, "trailing_place_retries", 3) or 3))

                        else:
                            # In hedge mode, SL is implemented as a conditional market order that OPENS the opposite hedge leg.
                            if hedge_mode and bool(getattr(self.p, "hedge_enabled", False)):
                                hedge_qty, _hedge_koff_eff, _funding_pct = self._compute_live_hedge_qty_with_funding(
                                    symbol=str(sym).upper(),
                                    main_side=str(position_side).upper(),
                                    main_qty=float(qty_use),
                                )
                                hedge_ps = "SHORT" if position_side == "LONG" else "LONG"
                                params = dict(
                                    symbol=sym,
                                    side=close_side,
                                    type="STOP_MARKET",
                                    stopPrice=float(sl_price),
                                    quantity=hedge_qty,
                                    workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                                    newClientOrderId=cid_trl,
                                    positionSide=hedge_ps,
                                )
                            else:
                                params = dict(
                                    symbol=sym,
                                    side=close_side,
                                    type="STOP_MARKET",
                                    stopPrice=float(sl_price),
                                    closePosition=True,
                                    workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                                    newClientOrderId=cid_trl,
                                    positionSide=position_side if hedge_mode else None,
                                )
                                if (not hedge_mode) and bool(self.p.reduce_only):
                                    params["reduceOnly"] = True
                            def _place_sl2(i: int):
                                p2 = dict(params)
                                try:
                                    px = float(p2.get("stopPrice") or p2.get("triggerPrice") or 0.0)
                                    # Mark-based protection from -2021: force stop further away from current mark.
                                    mark_sl = 0.0
                                    try:
                                        mark_sl = float(self._get_mark_price(sym) or 0.0)
                                    except Exception:
                                        mark_sl = 0.0
                                    if mark_sl <= 0:
                                        try:
                                            mark_sl = float(self._get_last_price(sym) or 0.0)
                                        except Exception:
                                            mark_sl = 0.0

                                    buf = float(getattr(self.p, "tp_trigger_buffer_pct", 0.25) or 0.25)
                                    # Increase buffer on retries so we actually move away if Binance says -2021.
                                    eff_buf = buf * (1.0 + float(i))
                                    tick = float(tick_size) if tick_size else 0.0

                                    if mark_sl > 0:
                                        if close_side == "SELL":
                                            # long SL: SELL stop must be BELOW mark
                                            px = min(px, mark_sl * (1.0 - eff_buf / 100.0))
                                            # push one more tick down each retry (if tick known)
                                            if tick > 0:
                                                px = px - tick
                                                px = _round_price_to_tick(px, tick, mode="down")
                                        else:
                                            # short SL: BUY stop must be ABOVE mark
                                            px = max(px, mark_sl * (1.0 + eff_buf / 100.0))
                                            if tick > 0:
                                                px = px + tick
                                                px = _round_price_to_tick(px, tick, mode="up")

                                    # Ensure numeric and set both keys (some wrappers drop stopPrice if triggerPrice is present)
                                    p2["triggerPrice"] = float(px)
                                    p2["stopPrice"] = float(px)
                                except Exception:
                                    pass
                                return self._binance.new_order(**p2)

                            resp = _tl_place_algo_with_retry(_place_sl2, max_tries=int(getattr(self.p, "trailing_place_retries", 3) or 3))

                        if isinstance(resp, dict):
                            self._upsert_order_shadow(
                                pos_uid=pos_uid,
                                order_id=resp.get("orderId") or resp.get("order_id") or "",
                                client_order_id=cid_sl,
                                symbol_id=symbol_id,
                                side=close_side,
                                order_type=str(resp.get("type") or "STOP_MARKET"),
                                qty=float(qty_use),
                                price=None,
                                reduce_only=True,
                                status=str(resp.get("status") or "NEW"),
                            )
                        recovered += 1
                        log.info("[trade_liquidation][RECOVERY] placed missing SL %s %s pos_uid=%s", sym, side, pos_uid)
                    except Exception:
                        _rollback_recovery_action()
                        log.exception("[trade_liquidation][RECOVERY] failed to place SL %s %s pos_uid=%s", sym, side, pos_uid)

            if (not main_partial_tp_enabled) and place_tp and (not has_tp) and tp_price > 0:
                try:
                    tp_mode = str(getattr(self.p, "tp_order_mode", "take_profit_market") or "take_profit_market").strip().lower()
                    if tp_mode in {"take_profit_market", "takeprofit_market", "tp_market", "market"}:
                        buffer_pct = float(getattr(self.p, "tp_trigger_buffer_pct", 0.25) or 0.25)
                        mark = _safe_float(ex_mark_map.get((sym, side)), 0.0)
                        tick_dec = self._price_tick_for_symbol(sym)
                        tick = float(tick_dec) if tick_dec is not None else 0.0

                        def _place(i_try: int):
                            buf = buffer_pct * (1.0 + 0.7 * float(i_try))
                            trigger_price = _tl_safe_trigger_price(
                                symbol=sym,
                                close_side=close_side,
                                desired=float(tp_price),
                                mark=mark,
                                tick=tick,
                                buffer_pct=buf,
                            )
                            params = dict(
                                symbol=sym,
                                side=close_side,
                                type="TAKE_PROFIT_MARKET",
                                stopPrice=float(trigger_price),
                                closePosition=True,
                                workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                                newClientOrderId=cid_tp,
                                positionSide=position_side if hedge_mode else None,
                            )
                            if (not hedge_mode) and bool(self.p.reduce_only):
                                params["reduceOnly"] = True
                            return self._binance.new_order(**params)

                        resp = _tl_place_algo_with_retry(_place, max_tries=int(getattr(self.p, "tp_place_retries", 3) or 3))

                    else:
                        params = dict(
                            symbol=sym,
                            side=close_side,
                            type="TAKE_PROFIT",
                            timeInForce=str(getattr(self.p, "time_in_force", "GTC")),
                            quantity=float(qty_use),
                            price=float(tp_price),
                            stopPrice=float(tp_price),
                            newClientOrderId=cid_tp,
                            positionSide=position_side if hedge_mode else None,
                        )
                        if (not hedge_mode) and bool(self.p.reduce_only):
                            params["reduceOnly"] = True
                        def _place_tp2(i: int):
                            p2 = dict(params)
                            try:
                                px = float(p2.get("stopPrice") or p2.get("triggerPrice") or 0.0)
                                if i > 0 and tick_size:
                                    # move one tick away from immediate trigger
                                    if close_side == "SELL":
                                        px = px + float(tick_size) * i
                                        px = _round_price_to_tick(px, float(tick_size), mode="up")
                                    else:
                                        px = px - float(tick_size) * i
                                        px = _round_price_to_tick(px, float(tick_size), mode="down")
                                # Binance uses triggerPrice for algo orders, stopPrice for normal - keep both
                                p2["triggerPrice"] = float(px)
                                p2["stopPrice"] = float(px)
                                p2["triggerPrice"] = float(px)
                            except Exception:
                                pass
                            return self._binance.new_order(**p2)

                        resp = _tl_place_algo_with_retry(_place_tp2, max_tries=int(getattr(self.p, "tp_place_retries", 3) or 3))

                    if isinstance(resp, dict):
                        self._upsert_order_shadow(
                            pos_uid=pos_uid,
                            order_id=resp.get("orderId") or resp.get("order_id") or "",
                            client_order_id=cid_tp,
                            symbol_id=symbol_id,
                            side=close_side,
                            order_type=str(resp.get("type") or "TAKE_PROFIT_MARKET"),
                            qty=float(qty_use),
                            price=None,
                            reduce_only=True,
                            status=str(resp.get("status") or "NEW"),
                        )
                        try:
                            self._upsert_algo_order_shadow(resp, pos_uid=pos_uid, strategy_id=self.STRATEGY_ID)
                        except Exception:
                            pass
                        try:
                            self._mark_local_algo_open(
                                cid_tp,
                                symbol=sym,
                                position_side=position_side,
                                order_type=str(resp.get("type") or "TAKE_PROFIT_MARKET"),
                                side=close_side,
                                quantity=float(qty_use),
                                trigger_price=float(tp_price),
                                close_position=True,
                                pos_uid=pos_uid,
                            )
                        except Exception:
                            pass
                        recovered += 1
                    log.info("[trade_liquidation][RECOVERY] placed missing TP %s %s pos_uid=%s", sym, side, pos_uid)
                except Exception:
                    _rollback_recovery_action()
                    log.exception("[trade_liquidation][RECOVERY] failed to place TP %s %s pos_uid=%s", sym, side, pos_uid)

            # If TRAILING exists but based on old entry/avg price, cancel so it can be re-placed on the new level.
            # This also covers manual position additions that change avgEntryPrice on the exchange.
            if cid_trl and has_trl:
                try:
                    # Find existing trailing algo order (if any)
                    trl_ao = None
                    for ao in open_algo_by_symbol.get(sym, []) or []:
                        if str(ao.get("clientAlgoId") or "") == str(cid_trl):
                            trl_ao = ao
                            break

                    if trl_ao is not None:
                        # Desired activation price from current exchange entry price
                        mark = float(ex_mark_map.get(sym) or 0.0)
                        activation_pct = float(getattr(self.p, "trailing_activation_pct", 0.0) or 0.0)
                        activation_buf_pct = float(getattr(self.p, "trailing_activation_buffer_pct", 0.0) or 0.0)
                        activation = max(0.0, activation_pct - activation_buf_pct)
                        ref_price = float(entry)

                        if close_side == "SELL":
                            desired_act = ref_price * (1.0 + activation / 100.0)
                            desired_act = _tl_safe_trigger_price(desired_act, mark, "above", tick=tick)
                        else:
                            desired_act = ref_price * (1.0 - activation / 100.0)
                            desired_act = _tl_safe_trigger_price(desired_act, mark, "below", tick=tick)

                        # Compare with current activation price
                        cur_act = trl_ao.get("activatePrice") or trl_ao.get("triggerPrice")
                        try:
                            cur_act_f = float(cur_act) if cur_act is not None else None
                        except Exception:
                            cur_act_f = None

                        # Tolerance: 2 ticks
                        tol = max(
                    float(tick or 0.0) * float(PRICE_EPS_TICKS),
                    abs(float(tr_activate)) * (float(AVG_EPS_PCT) / 100.0),
                    1e-12,
                )
                        if cur_act_f is None or abs(cur_act_f - float(desired_act)) > tol:
                            self.log.info(
                                f"[trade_liquidation][auto-recovery] stale TRL {sym} {pos_side}: "
                                f"cur_activate={cur_act_f} desired_activate={desired_act} -> cancel+recreate"
                            )
                            _last = float(self._bracket_replace_last_ts.get((pos_uid, "trl"), 0.0) or 0.0)
                            if (now_ts - _last) < float(REPLACE_COOLDOWN_SEC):
                                continue
                            self._bracket_replace_last_ts[(pos_uid, "trl")] = now_ts
                            try:
                                self._cancel_algo_by_client_id_safe(sym, cid_trl)
                            except Exception:
                                # fallback - might be a regular order
                                try:
                                    self._binance.cancel_order(symbol=sym, origClientOrderId=cid_trl)
                                except Exception:
                                    pass
                            has_trl = False
                except Exception:
                    # Don't break recovery loop on debug/compare errors
                    pass
            # Trailing stop recovery (independent from SL deferral): ensure TRAILING is present if enabled
            try:
                trailing_enabled = bool(getattr(self.p, "trailing_enabled", False))
                # Important hedge-mode guard: if this symbol side is currently managed as a
                # hedge leg of the opposite main position, generic _TRL recovery must stay
                # silent here. The dedicated HEDGE_TRL manager owns that side.
                if trailing_enabled and hedge_mode and ((str(sym).upper(), str(side).upper()) in hedge_managed_sides):
                    self._dlog(
                        "skip generic TRL recovery for hedge-managed side sym=%s side=%s pos_uid=%s",
                        sym, side, pos_uid,
                    )
                    continue
                opp_side = "SHORT" if str(side).upper() == "LONG" else "LONG"
                # Strong last-line guard: when both hedge-mode sides are currently open on exchange,
                # generic recovery must not place a normal _TRL for either side. Those symbols are
                # managed by live TP/TRL + HEDGE_TRL flows, and recovery here creates duplicate
                # trailing activation orders after restart.
                if trailing_enabled and hedge_mode and ((str(sym).upper(), opp_side) in ex_has_pos):
                    self._dlog(
                        "skip generic TRL recovery because opposite side is open sym=%s side=%s opp=%s pos_uid=%s",
                        sym, side, opp_side, pos_uid,
                    )
                    continue
                if trailing_enabled and hedge_mode and (str(sym).upper() in two_sided_symbols):
                    self._dlog(
                        "skip generic TRL recovery for two-sided hedge symbol sym=%s side=%s pos_uid=%s",
                        sym, side, pos_uid,
                    )
                    continue
                if place_trailing and trailing_enabled and (not has_trl) and (not is_hedge_managed_side):
                    if not _reserve_recovery_action():
                        break
                    try:
                        if not hasattr(self, "_recent_main_trl_place_ts"):
                            self._recent_main_trl_place_ts = {}
                    except Exception:
                        pass
                    recent_trl_ts = 0.0
                    trl_guard_sec = float(getattr(self.p, "trailing_place_guard_sec", 180) or 180)
                    try:
                        recent_trl_ts = float(getattr(self, "_recent_main_trl_place_ts", {}).get(cid_trl, 0.0) or 0.0)
                    except Exception:
                        recent_trl_ts = 0.0
                    if recent_trl_ts and (time.time() - recent_trl_ts) < trl_guard_sec:
                        continue
                    # reference price: before any averaging adds -> entry_price, after adds -> avg_price
                    try:
                        avg_state = (raw_meta or {}).get("avg_state") or {}
                        adds_done = int(avg_state.get("adds_done") or 0)
                    except Exception:
                        adds_done = 0
                    ref_price = float(avg_price or 0) if adds_done > 0 and avg_price else float(entry or 0)
                    if ref_price and qty_use and float(qty_use) > 0:
                        mark = self._get_mark_price(sym, position_side if hedge_mode else None)
                        tick = float(getattr(self, "_symbol_tick", {}).get(sym, 0) or 0) or float(getattr(self, "_symbol_tick_size", {}).get(sym, 0) or 0) or 0.0
                        if tick <= 0:
                            # fallback: reuse ticks from sym meta if present
                            tick = float((raw_meta or {}).get("tick_size") or 0) or 0.0
                        act_pct = float(getattr(self.p, "trailing_activation_pct", 0.5) or 0.5)
                        act_des = ref_price * (1 + act_pct / 100.0) if side == "LONG" else ref_price * (1 - act_pct / 100.0)
                        act = _round_to_tick(act_des, tick_size=tick, mode="nearest") if tick > 0 else act_des
                        buf = float(getattr(self.p, "trailing_activation_buffer_pct", 0.15) or 0.15)
                        if side == "LONG":
                            act = self._safe_price_above_mark(act, mark, tick, buf)
                        else:
                            act = self._safe_price_below_mark(act, mark, tick, buf)
                        cb = float(
                            getattr(self.p, "trailing_callback_rate", None)
                            or getattr(self.p, "trailing_trail_pct", None)
                            or 0.5
                        )
                        def _place_trl(i_try: int):
                            p_trl = dict(
                                symbol=sym,
                                side=close_side,
                                type="TRAILING_STOP_MARKET",
                                quantity=str(qty_use),
                                callbackRate=cb,
                                workingType="MARK_PRICE",
                                positionSide=position_side if hedge_mode else None,
                                algoType="CONDITIONAL",
                                activatePrice=str(act),
                                newClientOrderId=cid_trl,
                            )
                            return self._binance.new_order(**p_trl)
                        resp_trl = _tl_place_algo_with_retry(_place_trl, max_tries=int(getattr(self.p, "trailing_place_retries", 3) or 3))
                        self._upsert_order_shadow(
                            symbol_id=int(symbol_id),
                            order_id=str(resp_trl.get("algoId") or resp_trl.get("orderId") or ""),
                            client_order_id=cid_trl,
                            side=close_side,
                            order_type="TRAILING_STOP_MARKET",
                            status=str(resp_trl.get("algoStatus") or resp_trl.get("status") or "NEW"),
                            qty=float(qty_use),
                            price=float(act) if act is not None else None,
                            reduce_only=True,
                            pos_uid=pos_uid,
                        )
                        try:
                            self._upsert_algo_order_shadow(resp_trl, pos_uid=pos_uid, strategy_id=self.STRATEGY_ID)
                        except Exception:
                            pass
                        try:
                            self._mark_local_algo_open(
                                cid_trl,
                                symbol=sym,
                                position_side=position_side,
                                order_type="TRAILING_STOP_MARKET",
                                side=close_side,
                                quantity=float(qty_use),
                                trigger_price=float(act) if act is not None else None,
                                close_position=None,
                                pos_uid=pos_uid,
                            )
                        except Exception:
                            pass
                        recovered += 1
                        try:
                            self._recent_main_trl_place_ts[cid_trl] = time.time()
                        except Exception:
                            pass
                        log.info("[trade_liquidation][RECOVERY] placed missing TRAILING %s %s pos_uid=%s", sym, side, pos_uid) if not (isinstance(resp_trl, dict) and resp_trl.get("_duplicate")) else None
            except Exception:
                _rollback_recovery_action()
                log.exception("[trade_liquidation][RECOVERY] failed trailing recovery for %s %s pos_uid=%s", sym, side, pos_uid)

            # Averaging ADD recovery: after restart ADD orders may be missing because they were previously
            # placed only at ENTRY time. If averaging is enabled, ensure at least ADD1 exists.
            try:
                averaging_enabled = bool(getattr(self.p, "averaging_enabled", False) or getattr(self.p, "avg_enabled", False))
                if averaging_enabled:
                    if hedge_mode and ((str(sym).upper(), str(side).upper()) in hedge_managed_sides):
                        self._dlog(
                            "skip generic ADD1 recovery for hedge-managed side sym=%s side=%s pos_uid=%s",
                            sym, side, pos_uid,
                        )
                    elif hedge_mode and (str(sym).upper() in two_sided_symbols):
                        self._dlog(
                            "skip generic ADD1 recovery for two-sided hedge symbol sym=%s side=%s pos_uid=%s",
                            sym, side, pos_uid,
                        )
                    else:
                        self._recovery_ensure_add1(
                            sym=sym,
                            side=side,
                            position_side=position_side,
                            pos_uid=pos_uid,
                            symbol_id=int(symbol_id),
                            entry_price=Decimal(str(avg_price or entry or 0)),
            mark_price=_dec(ex_mark_map.get((sym, side)) or "0"),
                            pos_qty=Decimal(str(qty_use or 0)),
                            existing_client_ids=existing,
                        )
            except Exception:
                log.exception("[trade_liquidation][RECOVERY] failed ADD recovery for %s %s pos_uid=%s", sym, side, pos_uid)

        return int(recovered)

    def _recovery_ensure_add1(
        self,
        *,
        sym: str,
        side: str,
        position_side: str,
        pos_uid: str,
        symbol_id: int,
        entry_price: Decimal,
        mark_price: Decimal | None = None,
        pos_qty: Decimal,
        existing_client_ids: set,
    ) -> None:
        """Ensure the first averaging add order exists.

        If there is no significant level available (we don't have it during recovery), we place a
        fallback add level at least `min_level_distance_pct` away from the average entry.
        Default min is 5%.
        """

        tok = _coid_token(pos_uid)
        sym_u = (sym or "").upper()
        prefix = _sanitize_coid_prefix(str(getattr(self.p, "client_order_id_prefix", "TL") or "TL"))
        cid_add = f"{prefix}_{tok}_ADD1"

        # Refresh adds_done from DB truth before recovery placement.
        # This protects against re-placing ADD1 after it already filled on a previous cycle
        # but position_ledger.scale_in_count still lags behind exchange/order-events sync.
        if self._is_live:
            try:
                adds_done_live = int(
                    self._live_refresh_scale_in_count(
                        pos_uid=str(pos_uid),
                        prefix=prefix,
                        sym=sym_u,
                        pos_side=str(side or "").upper(),
                    )
                    or 0
                )
                if adds_done_live >= 1:
                    return
            except Exception:
                pass

        # Respect max additions cap: if configured max_adds < 1, never place ADD1.
        max_adds_cfg = int(self._cfg_max_adds() or 0)
        if max_adds_cfg < 1:
            return

        # If ledger already indicates we reached the cap (e.g. ADD1 filled previously),
        # do not place a new ADD1 during recovery.
        try:
            rows = list(
                self.store.query_dict(
                    """
                    SELECT COALESCE(scale_in_count, 0) AS scale_in_count
                    FROM position_ledger
                    WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s
                      AND status IN ('OPEN','CLOSED') AND source='live' AND strategy_id=%(sid)s
                    LIMIT 1;
                    """,
                    {
                        "ex": int(self.exchange_id),
                        "acc": int(self.account_id),
                        "pos_uid": str(pos_uid),
                        "sid": self.STRATEGY_ID,
                    },
                )
            )
            if rows and int(rows[0].get("scale_in_count") or 0) >= max_adds_cfg:
                return
        except Exception:
            # best-effort, non-fatal
            pass

        # If any ADD exists on the current exchange snapshot, do nothing.
        for cid in (existing_client_ids or set()):
            if isinstance(cid, str) and (f"{prefix}_{tok}_ADD" in cid):
                return

        # Strong DB-side dedupe for ADD1.
        # Recovery snapshots may lag behind actual fills/cancels, so we also trust our local
        # order shadow / fills history. If ADD1 is already open or has already filled, do not place it again.
        try:
            row = self.store.fetch_one(
                """
                SELECT status
                FROM public.orders
                WHERE exchange_id=%s AND account_id=%s AND strategy_id=%s
                  AND client_order_id=%s
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (int(self.exchange_id), int(self.account_id), str(self.STRATEGY_ID), str(cid_add)),
            )
            if row:
                db_status = str((row.get("status") if isinstance(row, dict) else row[0]) or "").upper().strip()
                if db_status in {"NEW", "OPEN", "PARTIALLY_FILLED", "FILLED"}:
                    return
        except Exception:
            pass
        try:
            fill_row = self.store.fetch_one(
                """
                SELECT 1 AS ok
                FROM public.order_fills
                WHERE exchange_id=%s AND account_id=%s AND strategy_id=%s
                  AND client_order_id=%s
                LIMIT 1
                """,
                (int(self.exchange_id), int(self.account_id), str(self.STRATEGY_ID), str(cid_add)),
            )
            if fill_row:
                return
        except Exception:
            pass

        if entry_price is None or entry_price <= 0 or pos_qty is None or pos_qty <= 0:
            return

        # Min distance (default 5%)
        min_pct = Decimal(str(getattr(self.p, "min_level_distance_pct", 5) or 5))
        if min_pct < 0:
            min_pct = Decimal("0")

        if str(side).upper() == "LONG":
            add_price = entry_price * (Decimal("1") - (min_pct / Decimal("100")))
            add_side = "BUY"
        else:
            add_price = entry_price * (Decimal("1") + (min_pct / Decimal("100")))
            add_side = "SELL"

        tick = self._price_tick_for_symbol(sym_u) or Decimal("0")
        if tick and tick > 0:
            # for LONG BUY add we want to be conservative: round down; for SHORT SELL add round up
            # NOTE: _round_to_step uses keyword-only arg `rounding` (not `mode`).
            add_price = _round_to_step(add_price, tick, rounding=("floor" if add_side == "BUY" else "ceiling"))

        # Qty sizing: default = position qty / max_adds, unless averaging_add_qty_pct is specified
        max_adds = int(self._cfg_max_adds() or 1)
        add_qty_pct = Decimal(str(getattr(self.p, "averaging_add_qty_pct", 0) or 0))
        if add_qty_pct > 0:
            add_qty = pos_qty * (add_qty_pct / Decimal("100"))
        else:
            add_qty = (pos_qty / Decimal(str(max_adds))) if max_adds > 0 else pos_qty

        qty_step = self._qty_step_for_symbol(sym_u) or Decimal("0")
        if qty_step and qty_step > 0:
            add_qty = _round_qty_to_step(add_qty, qty_step, mode="down")

        if add_qty <= 0:
            return

        # Ensure ADD trigger price is on the correct side of current mark price to avoid immediate-trigger (Binance -2021).
        # For LONG we add with BUY when price falls; for SHORT we add with SELL when price rises.
        try:
            tick = _dec(tick)
        except Exception:
            tick = _dec('0')
        if mark_price is not None and mark_price > 0 and tick and tick > 0:
            if add_side == 'BUY':
                # must be strictly below mark
                if add_price >= mark_price:
                    add_price = mark_price - tick
            else:
                # must be strictly above mark
                if add_price <= mark_price:
                    add_price = mark_price + tick

        # If price already moved beyond the first add level, push trigger further away in steps of min_pct from entry.
        min_pct = _dec(str(getattr(self.p, 'averaging_min_level_pct', getattr(self.p, 'min_level_pct', 5)) or 5)) / _dec('100')
        if entry_price and min_pct > 0 and mark_price is not None and mark_price > 0 and tick and tick > 0:
            for i in range(1, 26):
                if add_side == 'BUY':
                    cand = entry_price * (_dec('1') - (min_pct * _dec(i)))
                    cand = _round_to_step(cand, tick, rounding='down')
                    if cand < (mark_price - tick):
                        add_price = cand
                        break
                else:
                    cand = entry_price * (_dec('1') + (min_pct * _dec(i)))
                    cand = _round_to_step(cand, tick, rounding='up')
                    if cand > (mark_price + tick):
                        add_price = cand
                        break

        params = dict(
            symbol=sym_u,
            side=add_side,
            # Conditional market order for averaging (trigger -> market)
            type='TAKE_PROFIT_MARKET',
            stopPrice=float(add_price),
            quantity=float(add_qty),
            workingType="MARK_PRICE",
            priceProtect=True,
            newClientOrderId=cid_add,
            positionSide=position_side,
        )

        resp = self._binance.new_order(**params)

        # Shadow insert with supported args only
        try:
            if isinstance(resp, dict):
                self._upsert_order_shadow(
                    pos_uid=pos_uid,
                    order_id=str(resp.get("orderId") or resp.get("order_id") or ""),
                    client_order_id=cid_add,
                    symbol_id=int(symbol_id),
                    side=add_side,
                    order_type=params.get("type"),
                    qty=float(add_qty),
                    price=float(add_price),
                    reduce_only=False,
                    status=str(resp.get("status") or "NEW"),
                )
        except Exception:
            pass

        try:
            if not hasattr(self, "_recent_recovery_add_place_ts"):
                self._recent_recovery_add_place_ts = {}
            self._recent_recovery_add_place_ts[str(cid_add)] = time.time()
        except Exception:
            pass

        log.info("[trade_liquidation][RECOVERY] placed missing ADD1 %s %s pos_uid=%s price=%s qty=%s", sym_u, side, pos_uid, add_price, add_qty)



