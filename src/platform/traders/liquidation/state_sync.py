from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import Future

from .params import *

log = logging.getLogger("traders.trade_liquidation")


class TradeLiquidationStateSyncMixin:
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

