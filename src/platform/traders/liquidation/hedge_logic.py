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

log = logging.getLogger("traders.trade_liquidation")

class TradeLiquidationHedgeLogicMixin:
    def _live_ensure_hedge_after_last_add(
        self,
        *,
        pos: dict,
        sym: str,
        pos_side: str,
        pos_uid: str,
        prefix: str,
        tok: str,
        adds_done: int,
        max_adds: int,
    ) -> bool:
        """After the last allowed add, place a *conditional market* hedge opener.

        This is NOT the main trailing order. We use a dedicated client id:
          - {prefix}_{tok}_HEDGE

        Trigger is derived from the last add fill and `sl_after_last_add_distance_pct`.
        Quantity is derived from the *exchange* position size (positionRisk.positionAmt)
        multiplied by `hedge_koff` and rounded to qty step.

        We also cancel a legacy hedge order that used `{prefix}_{tok}_TRL` with type STOP_MARKET.
        """
        try:
            dist_pct = float(getattr(self.p, "sl_after_last_add_distance_pct", 0.0) or 0.0)
        except Exception:
            dist_pct = 0.0
        if dist_pct <= 0:
            return False

        if int(adds_done) < int(max_adds) or int(max_adds) <= 0:
            return False

        last_n = int(min(int(adds_done), int(max_adds)))
        last_fill = float(self._live_last_add_fill_price(sym=sym, prefix=prefix, tok=tok, add_n=last_n) or 0.0)
        if last_fill <= 0:
            return False

        # Resolve symbol_id for DB filtering (orders/order_fills store symbol_id)
        symbol_id = 0
        try:
            sym_row = self.store.query_one(
                """
                SELECT symbol_id
                FROM public.symbols
                WHERE exchange_id=%(ex)s AND upper(symbol)=%(sym)s
                LIMIT 1;
                """,
                {"ex": int(self.exchange_id), "sym": str(sym).upper()},
            )
            symbol_id = int(sym_row[0]) if sym_row and sym_row[0] is not None else 0
        except Exception:
            symbol_id = 0
        if symbol_id <= 0:
            return False

        # Compare by DB write time (orders.updated_at), not fill time (order_fills.ts).
        last_add_seen_at = None
        hedge_seen_at = None
        try:
            cid_add = f"{prefix}_{tok}_ADD{int(last_n)}"
            row = self.store.query_one(
                """
                SELECT MAX(updated_at)
                  FROM public.orders
                 WHERE exchange_id=%(ex)s
                   AND account_id=%(acc)s
                   AND symbol_id=%(symbol_id)s
                   AND client_order_id=%(cid)s
                   AND status IN ('FILLED','PARTIALLY_FILLED');
                """,
                {"ex": int(self.exchange_id), "acc": int(self.account_id), "symbol_id": int(symbol_id), "cid": cid_add},
            )
            last_add_seen_at = row[0] if row else None
        except Exception:
            last_add_seen_at = None

        cid_hedge = f"{prefix}_{tok}_HEDGE"
        try:
            row = self.store.query_one(
                """
                SELECT MAX(updated_at)
                  FROM public.algo_orders
                 WHERE exchange_id=%(ex)s
                   AND account_id=%(acc)s
                   AND symbol=%(symbol)s
                   AND client_algo_id LIKE %(cid_like)s;
                """,
                {"ex": int(self.exchange_id), "acc": int(self.account_id), "symbol": str(sym).upper(), "cid_like": f"{cid_hedge}%"},
            )
            hedge_seen_at = row[0] if row else None
        except Exception:
            hedge_seen_at = None

        must_refresh = False
        refresh_reason = None
        try:
            if last_add_seen_at is not None and (hedge_seen_at is None or hedge_seen_at < last_add_seen_at):
                must_refresh = True
                refresh_reason = "after_add"
        except Exception:
            must_refresh = False

        # Anti-churn: if we very recently placed after-last-add hedge, don't cancel/recreate it
        # even if DB snapshots lag behind. This uses hedge_cooldown_sec from config.
        try:
            cd_sec = float(getattr(self.p, "hedge_cooldown_sec", 0.0) or 0.0)
        except Exception:
            cd_sec = 0.0
        if cd_sec and cd_sec > 0 and self._pl_has_raw_meta:
            try:
                rm = pos.get("raw_meta") if isinstance(pos.get("raw_meta"), dict) else {}
                ts_s = ((rm.get("after_last_add_hedge") or {}).get("ts") or ((rm.get("live_entry") or {}).get("after_last_add_hedge") or {}).get("ts"))
                if ts_s:
                    now_ms = self._to_epoch_ms(_utc_now()) or 0
                    ts_ms = self._to_epoch_ms(ts_s) or 0
                    if now_ms and ts_ms and (now_ms - ts_ms) < int(cd_sec * 1000):
                        # don't touch the hedge too often
                        must_refresh = False
            except Exception:
                pass

        side_u = str(pos_side or "").upper()
        close_side = "SELL" if side_u == "LONG" else "BUY"
        hedge_ps = "SHORT" if side_u == "LONG" else "LONG"

        # Trigger price from last add fill
        if side_u == "LONG":
            stop_price = last_fill * (1.0 - dist_pct / 100.0)
        else:
            stop_price = last_fill * (1.0 + dist_pct / 100.0)

        # Tick rounding
        tick = 0.0
        try:
            tick = float(self._price_tick_for_symbol(sym) or 0.0)
        except Exception:
            tick = 0.0
        if tick and tick > 0:
            stop_price = float(_round_price_to_tick(stop_price, tick, mode=("down" if close_side == "SELL" else "up")))

        # Avoid immediate trigger wrt mark
        mark = 0.0
        try:
            pr = self._rest_snapshot_get("position_risk") or []
            for r in pr if isinstance(pr, list) else []:
                if str(r.get("symbol") or "").upper() != str(sym).upper():
                    continue
                ps = str(r.get("positionSide") or "").upper()
                if ps and ps in {"LONG", "SHORT"} and ps != side_u:
                    continue
                mark = float(r.get("markPrice") or 0.0)
                if mark > 0:
                    break
        except Exception:
            mark = 0.0
        if mark > 0 and tick and tick > 0:
            if close_side == "BUY" and stop_price <= mark:
                stop_price = float(_round_price_to_tick(mark + tick, tick, mode="up"))
            if close_side == "SELL" and stop_price >= mark:
                stop_price = float(_round_price_to_tick(mark - tick, tick, mode="down"))

        # Quantity: must be based on exchange position size
        qty_main = 0.0
        try:
            pr = self._rest_snapshot_get("position_risk") or []
            for r in pr if isinstance(pr, list) else []:
                if str(r.get("symbol") or "").upper() != str(sym).upper():
                    continue
                ps = str(r.get("positionSide") or "").upper()
                if ps in {"LONG", "SHORT"} and ps != side_u:
                    continue
                amt = float(r.get("positionAmt") or 0.0)
                if abs(amt) > 0:
                    qty_main = abs(float(amt))
                    break
        except Exception:
            qty_main = 0.0
        if qty_main <= 0:
            qty_main = _safe_float(pos.get("qty_current") or 0.0, 0.0)
        if qty_main <= 0:
            return False

        hedge_koff = float(getattr(self.p, "hedge_koff", 1.0) or 1.0)
        hedge_qty = float(abs(float(qty_main)) * hedge_koff)

        # qty step rounding
        try:
            qty_step = float(self._qty_step_for_symbol(sym) or 0.0)
        except Exception:
            qty_step = 0.0
        if qty_step and qty_step > 0:
            hedge_qty = float(_round_qty_to_step(hedge_qty, qty_step, mode="down"))
        if hedge_qty <= 0:
            return False

        try:
            hedge_guard_sec = float(getattr(self.p, "hedge_reissue_cooldown_sec", 120) or 120)
        except Exception:
            hedge_guard_sec = 120.0
        _hedge_payload = {
            "symbol": str(sym).upper(),
            "position_side": str(hedge_ps).upper(),
            "side": str(close_side).upper(),
            "stop": round(float(stop_price), 12),
            "qty": round(float(hedge_qty), 12),
        }
        if self._memo_recent_desired_order("_recent_hedge_desired", cid_hedge, _hedge_payload, hedge_guard_sec):
            return False

        # Find open algos by clientAlgoId
        open_algos = self._rest_snapshot_get("open_algo_orders_all") or []
        found_hedge = None
        found_legacy = None
        legacy_cid = f"{prefix}_{tok}_TRL"
        for ao in open_algos if isinstance(open_algos, list) else []:
            try:
                if str(ao.get("symbol") or "").upper() != str(sym).upper():
                    continue
                cid = str(ao.get("clientAlgoId") or "")
                otype = str(ao.get("orderType") or ao.get("type") or "").upper()
                ps = str(ao.get("positionSide") or "").upper()
                side_ao = str(ao.get("side") or "").upper()
                reduce_only_ao = bool(ao.get("reduceOnly"))
                close_pos_ao = bool(ao.get("closePosition"))
                qty_ao = float(ao.get("quantity") or ao.get("origQty") or 0.0)
                trig_ao = float(ao.get("triggerPrice") or ao.get("stopPrice") or 0.0)
                if cid == cid_hedge:
                    found_hedge = ao
                if cid == legacy_cid and otype in {"STOP_MARKET", "STOP"}:
                    found_legacy = ao
                # Semantic match for the pending hedge opener.
                # It must belong to the hedge side, use the same opening side, and MUST NOT
                # be a reduce-only / close-position main protective order.
                if found_hedge is None and otype in {"STOP_MARKET", "STOP"}:
                    if ps != str(hedge_ps).upper():
                        continue
                    if side_ao != str(close_side).upper():
                        continue
                    if reduce_only_ao or close_pos_ao:
                        continue
                    qty_tol = max(float(qty_step or 0.0) * 2.0, abs(float(hedge_qty or 0.0)) * 0.001, 1e-8)
                    trig_tol = max(float(tick or 0.0) * 2.0, abs(float(stop_price or 0.0)) * 0.0002, 1e-8)
                    if hedge_qty > 0 and qty_ao > 0 and abs(qty_ao - float(hedge_qty)) > qty_tol:
                        continue
                    if stop_price > 0 and trig_ao > 0 and abs(trig_ao - float(stop_price)) > trig_tol:
                        continue
                    found_hedge = ao
            except Exception:
                continue
        if found_hedge is None:
            try:
                row = self.store.query_one(
                    """
                    SELECT client_algo_id, trigger_price, quantity, updated_at
                      FROM public.algo_orders
                     WHERE exchange_id=%(ex)s AND account_id=%(acc)s
                       AND upper(symbol)=%(sym)s
                       AND upper(COALESCE(position_side,'')) = %(ps)s
                       AND upper(COALESCE(side,'')) = %(side)s
                       AND upper(type) IN ('STOP_MARKET','STOP')
                       AND COALESCE(reduce_only, FALSE) = FALSE
                       AND status='OPEN'
                     ORDER BY updated_at DESC
                     LIMIT 1;
                    """,
                    {
                        "ex": int(self.exchange_id),
                        "acc": int(self.account_id),
                        "sym": str(sym).upper(),
                        "ps": str(hedge_ps).upper(),
                        "side": str(close_side).upper(),
                    },
                )
                if row and row[0]:
                    found_hedge = {
                        "clientAlgoId": str(row[0] or ""),
                        "triggerPrice": float(row[1] or 0.0),
                        "quantity": float(row[2] or 0.0),
                    }
            except Exception:
                pass

        # If hedge POSITION is already open on the exchange (opposite positionSide has non-zero amt),
        # we must NOT place a pending hedge opener again.
        hedge_ps = "SHORT" if side_u == "LONG" else "LONG"
        hedge_amt = 0.0
        try:
            pr = self._rest_snapshot_get("position_risk") or []
            for r in pr if isinstance(pr, list) else []:
                if str(r.get("symbol") or "").upper() != str(sym).upper():
                    continue
                if str(r.get("positionSide") or "").upper() != hedge_ps:
                    continue
                amt = float(r.get("positionAmt") or 0.0)
                if abs(amt) > 0:
                    hedge_amt = abs(float(amt))
                    break
        except Exception:
            hedge_amt = 0.0

        if hedge_amt > 0:
            # Best-effort: cancel pending hedge opener if it exists.
            if found_hedge is not None:
                try:
                    resp_cancel = self._binance.cancel_algo_order(symbol=sym, clientAlgoId=cid_hedge)
                    try:
                        rawj = resp_cancel if isinstance(resp_cancel, dict) else {"result": resp_cancel}
                        if isinstance(rawj, dict):
                            rawj.setdefault("cancel_reason", "hedge_already_open")
                            rawj.setdefault("cancel_source", "bot_rule")
                        self.store.set_algo_order_status(
                            exchange_id=int(self.exchange_id),
                            account_id=int(self.account_id),
                            client_algo_id=str(cid_hedge),
                            status="CANCELED",
                            raw_json=rawj,
                        )
                    except Exception:
                        pass
                except Exception:
                    pass
            return False

        # If hedge exists and trigger already matches, keep it (ignore must_refresh).
        # DB may lag behind exchange; we prefer exchange snapshot here.
        if found_hedge is not None:
            try:
                cur = found_hedge.get("triggerPrice") or found_hedge.get("stopPrice")
                cur_f = float(cur) if cur is not None else 0.0
            except Exception:
                cur_f = 0.0

            # Read main uPnL from positionRisk (main side only)
            main_upnl = 0.0
            try:
                pr = self._rest_snapshot_get("position_risk") or []
                for r in pr if isinstance(pr, list) else []:
                    if str(r.get("symbol") or "").upper() != str(sym).upper():
                        continue
                    if str(r.get("positionSide") or "").upper() != side_u:
                        continue
                    main_upnl = float(r.get("unRealizedProfit") or 0.0)
                    break
            except Exception:
                main_upnl = 0.0

            # If main is already in profit -> pending hedge opener is not needed (optional cancel)
            cancel_when_profit = bool(getattr(self.p, "hedge_after_last_add_cancel_when_main_profit", True))
            if cancel_when_profit and main_upnl > 0:
                try:
                    resp_cancel = self._binance.cancel_algo_order(symbol=sym, clientAlgoId=cid_hedge)
                    try:
                        rawj = resp_cancel if isinstance(resp_cancel, dict) else {"result": resp_cancel}
                        if isinstance(rawj, dict):
                            rawj.setdefault("cancel_reason", "main_profit")
                            rawj.setdefault("cancel_source", "bot_rule")
                        self.store.set_algo_order_status(
                            exchange_id=int(self.exchange_id),
                            account_id=int(self.account_id),
                            client_algo_id=str(cid_hedge),
                            status="CANCELED",
                            raw_json=rawj,
                        )
                    except Exception:
                        pass
                    log.info(
                        "[TL][HEDGE][FOLLOW] canceled pending after-last-add hedge for %s %s: main_upnl=%.6f > 0",
                        sym,
                        side_u,
                        main_upnl,
                    )
                except Exception:
                    pass
                return True

            # Follow / reprice: if price moved away from current trigger too much, pull trigger closer to mark
            follow_enabled = bool(getattr(self.p, "hedge_after_last_add_follow_enabled", False))
            try:
                follow_dist = float(getattr(self.p, "hedge_after_last_add_follow_distance_pct", 0.0) or 0.0)
            except Exception:
                follow_dist = 0.0
            try:
                follow_pull = float(getattr(self.p, "hedge_after_last_add_follow_pullback_pct", 0.0) or 0.0)
            except Exception:
                follow_pull = 0.0
            try:
                follow_min_move = float(getattr(self.p, "hedge_after_last_add_follow_min_trigger_move_pct", 0.0) or 0.0)
            except Exception:
                follow_min_move = 0.0
            try:
                follow_cd_sec = float(getattr(self.p, "hedge_after_last_add_follow_cooldown_sec", 0.0) or 0.0)
            except Exception:
                follow_cd_sec = 0.0

            if follow_enabled and follow_dist > 0 and follow_pull > 0 and mark > 0 and cur_f > 0 and main_upnl <= 0:
                # cooldown (stored in raw_meta)
                if follow_cd_sec and follow_cd_sec > 0 and self._pl_has_raw_meta:
                    try:
                        rm = pos.get("raw_meta") if isinstance(pos.get("raw_meta"), dict) else {}
                        ts_s = ((rm.get("after_last_add_hedge") or {}).get("follow_ts") or ((rm.get("live_entry") or {}).get("after_last_add_hedge") or {}).get("follow_ts"))
                        if ts_s:
                            now_ms = self._to_epoch_ms(_utc_now()) or 0
                            ts_ms = self._to_epoch_ms(ts_s) or 0
                            if now_ms and ts_ms and (now_ms - ts_ms) < int(follow_cd_sec * 1000):
                                follow_enabled = False
                    except Exception:
                        pass

                if follow_enabled:
                    try:
                        if close_side == "SELL":
                            # main LONG -> hedge SHORT -> trigger below mark
                            dist_pct_now = (mark - cur_f) / max(mark, 1e-12) * 100.0
                            new_trigger = mark * (1.0 - follow_pull / 100.0)
                        else:
                            # main SHORT -> hedge LONG -> trigger above mark
                            dist_pct_now = (cur_f - mark) / max(mark, 1e-12) * 100.0
                            new_trigger = mark * (1.0 + follow_pull / 100.0)

                        # round to tick
                        if tick and tick > 0:
                            new_trigger = float(
                                _round_price_to_tick(new_trigger, tick, mode=("down" if close_side == "SELL" else "up"))
                            )

                        move_pct = abs(new_trigger - cur_f) / max(mark, 1e-12) * 100.0
                        if dist_pct_now >= follow_dist and (follow_min_move <= 0 or move_pct >= follow_min_move):
                            try:
                                if not hasattr(self, "_recent_hedge_follow_state") or not isinstance(self._recent_hedge_follow_state, dict):
                                    self._recent_hedge_follow_state = {}
                                _f_key = str(cid_hedge)
                                _f_prev = self._recent_hedge_follow_state.get(_f_key) or {}
                                _f_prev_stop = float(_f_prev.get("stop") or 0.0)
                                _f_prev_ts = float(_f_prev.get("ts") or 0.0)
                                _f_cd = float(getattr(self.p, "hedge_follow_guard_sec", 180) or 180)
                                _f_tol = max(float(tick or 0.0) * 2.0, abs(float(new_trigger)) * 0.0002, 1e-8)
                                if _f_prev_ts and (time.time() - _f_prev_ts) < _f_cd and abs(_f_prev_stop - float(new_trigger)) <= _f_tol:
                                    return False
                            except Exception:
                                pass
                            log.info(
                                "[TL][HEDGE][FOLLOW] reprice after-last-add hedge %s %s: mark=%.8f old=%.8f dist=%.4f%% -> new=%.8f (thr=%.4f%% pull=%.4f%%)",
                                sym,
                                side_u,
                                mark,
                                cur_f,
                                dist_pct_now,
                                new_trigger,
                                follow_dist,
                                follow_pull,
                            )
                            stop_price = float(new_trigger)
                            must_refresh = True
                            refresh_reason = "reprice"
                            try:
                                self._recent_hedge_follow_state[str(cid_hedge)] = {"stop": float(new_trigger), "ts": time.time()}
                            except Exception:
                                pass
                        else:
                            # if trigger already close enough to desired stop_price and no refresh requested -> keep
                            tol = max(float(tick or 0.0) * 2.0, abs(stop_price) * 0.0002, 1e-8)
                            if cur_f > 0 and abs(cur_f - float(stop_price)) <= tol and not must_refresh:
                                return False
                    except Exception:
                        pass
            else:
                # Default: if trigger already matches, or DB/exchange already reports an open hedge stopper, keep it.
                try:
                    tol = max(float(tick or 0.0) * 2.0, abs(stop_price) * 0.0002, 1e-8)
                    if cur_f > 0 and abs(cur_f - float(stop_price)) <= tol:
                        return False
                    if cur_f > 0 and not must_refresh:
                        return False
                except Exception:
                    pass

        # Cancel existing hedge (best-effort) only if we really want to refresh it.
        if must_refresh and found_hedge is not None:
            for cid in (cid_hedge,):
                try:
                    resp_cancel = self._binance.cancel_algo_order(symbol=sym, clientAlgoId=cid)
                    try:
                        rawj = resp_cancel if isinstance(resp_cancel, dict) else {"result": resp_cancel}
                        if isinstance(rawj, dict):
                            rawj.setdefault("cancel_reason", refresh_reason or "refresh")
                            rawj.setdefault("cancel_source", "bot_refresh")
                        self.store.set_algo_order_status(
                            exchange_id=int(self.exchange_id),
                            account_id=int(self.account_id),
                            client_algo_id=str(cid),
                            status="CANCELED",
                            raw_json=rawj,
                        )
                    except Exception:
                        pass
                except Exception:
                    try:
                        self._binance.cancel_order(symbol=sym, origClientOrderId=cid)
                    except Exception:
                        pass

        # Cancel legacy hedge stored under _TRL if it's STOP_MARKET
        if found_legacy is not None:
            try:
                resp_cancel = self._binance.cancel_algo_order(symbol=sym, clientAlgoId=legacy_cid)
                try:
                    rawj = resp_cancel if isinstance(resp_cancel, dict) else {"result": resp_cancel}
                    if isinstance(rawj, dict):
                        rawj.setdefault("cancel_reason", "legacy_cleanup")
                        rawj.setdefault("cancel_source", "bot_refresh")
                    self.store.set_algo_order_status(
                        exchange_id=int(self.exchange_id),
                        account_id=int(self.account_id),
                        client_algo_id=str(legacy_cid),
                        status="CANCELED",
                        raw_json=rawj,
                    )
                except Exception:
                    pass
            except Exception:
                pass

        # Hedge position side is opposite
        # (already computed above and reused for semantic exchange/DB matching)

        try:
            try:
                if not hasattr(self, "_recent_hedge_place_state") or not isinstance(self._recent_hedge_place_state, dict):
                    self._recent_hedge_place_state = {}
                _hedge_guard_sec = float(getattr(self.p, "hedge_place_guard_sec", 180) or 180)
                _hedge_key = str(cid_hedge)
                _hedge_prev = self._recent_hedge_place_state.get(_hedge_key) or {}
                _prev_stop = float(_hedge_prev.get("stop") or 0.0)
                _prev_ts = float(_hedge_prev.get("ts") or 0.0)
                _tol = max(float(tick or 0.0) * 2.0, abs(float(stop_price)) * 0.0002, 1e-8)
                if _prev_ts and (time.time() - _prev_ts) < _hedge_guard_sec and abs(_prev_stop - float(stop_price)) <= _tol:
                    return False
            except Exception:
                pass

            place_cid = str(cid_hedge)
            # Binance can permanently reserve a clientAlgoId after a cancel/reissue cycle.
            # If that happens, using the canonical *_HEDGE id again yields -4116 but no live
            # order appears on exchange. Retry with a short unique suffix and rely on semantic
            # matching + DB LIKE queries for subsequent detection.
            try:
                suffix = format(int(time.time() * 1000) % 65536, '04x')
                alt_cid = f"{cid_hedge}_{suffix}"
                if len(alt_cid) > 36:
                    alt_cid = alt_cid[:36]
            except Exception:
                alt_cid = None

            def _place_once(client_id: str):
                return self._binance.new_order(
                    symbol=sym,
                    side=close_side,
                    type="STOP_MARKET",
                    stopPrice=float(stop_price),
                    quantity=float(hedge_qty),
                    workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                    newClientOrderId=client_id,
                    positionSide=hedge_ps,
                )

            try:
                resp = _place_once(place_cid)
            except Exception as e:
                msg = str(e)
                if alt_cid and ("-4116" in msg or "ClientOrderId is duplicated" in msg):
                    place_cid = alt_cid
                    resp = _place_once(place_cid)
                else:
                    raise
            _dup_resp = bool(isinstance(resp, dict) and resp.get("_duplicate"))

            # persist algo hedge order (best-effort)
            try:
                self.store.upsert_algo_orders(
                    [
                        {
                            "exchange_id": int(self.exchange_id),
                            "account_id": int(self.account_id),
                            "client_algo_id": str(place_cid),
                            "algo_id": None if resp is None else str(resp.get("algoId") or resp.get("algoOrderId") or resp.get("orderId") or ""),
                            "symbol": str(sym).upper(),
                            "side": str(close_side),
                            "position_side": str(hedge_ps),
                            "type": "STOP_MARKET",
                            "quantity": float(hedge_qty),
                            "trigger_price": float(stop_price),
                            "working_type": str(getattr(self.p, "working_type", "MARK_PRICE")),
                            "status": "OPEN",
                            "strategy_id": str(self.strategy_id),
                            "pos_uid": str(pos_uid),
                            "raw_json": (resp if isinstance(resp, dict) else {"result": resp, "canonical_client_algo_id": str(cid_hedge)}),
                        }
                    ]
                )
            except Exception:
                pass
        except Exception:
            return False

        # store meta flag (best-effort)
        try:
            if self._pl_has_raw_meta:
                meta = {
                    "after_last_add_hedge": {
                        "add_n": int(last_n),
                        "fill": float(last_fill),
                        "stop": float(stop_price),
                        "pct": float(dist_pct),
                        "qty_main": float(qty_main),
                        "qty_hedge": float(hedge_qty),
                        "client_algo_id": str(place_cid),
                        "canonical_client_algo_id": str(cid_hedge),
                        "ts": _utc_now().isoformat(),
                    }
                }
                self.store.execute(
                    """
                    UPDATE public.position_ledger
                       SET raw_meta = COALESCE(raw_meta, '{}'::jsonb) || %(meta)s::jsonb
                     WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND strategy_id=%(st)s
                       AND symbol_id=%(symbol_id)s AND side=%(side)s AND source='live'
                       AND updated_at = (
                           SELECT MAX(updated_at) FROM public.position_ledger
                            WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND strategy_id=%(st)s
                              AND symbol_id=%(symbol_id)s AND side=%(side)s AND source='live'
                       );
                    """,
                    {
                        "meta": json.dumps(meta),
                        "ex": int(self.exchange_id),
                        "acc": int(self.account_id),
                        "st": str(self.strategy_id),
                        "symbol_id": int(symbol_id),
                        "side": str(side_u),
                    },
                )
        except Exception:
            pass

        try:
            if not hasattr(self, "_recent_hedge_place_state") or not isinstance(self._recent_hedge_place_state, dict):
                self._recent_hedge_place_state = {}
            self._recent_hedge_place_state[str(cid_hedge)] = {"stop": float(stop_price), "ts": time.time()}
            self._recent_hedge_place_state[str(place_cid)] = {"stop": float(stop_price), "ts": time.time()}
        except Exception:
            pass

        if _dup_resp:
            return False
        self.log.info(
            "[trade_liquidation][LIVE][HEDGE] placed after-last-add STOP_MARKET %s %s stop=%.8f (last_add=%.8f add_n=%s pct=%.4f qty=%.8f koff=%.4f)",
            str(sym).upper(),
            str(side_u),
            float(stop_price),
            float(last_fill),
            str(last_n),
            float(dist_pct),
            float(hedge_qty),
            float(hedge_koff),
        )
        return True

    def _live_hedge_manage(self) -> Dict[str, int]:
        """Manage hedge positions in LIVE mode when hedge_enabled is True.

        Rule:
          - When SL condition would be met, open an opposite market position sized:
                hedge_qty = abs(main_qty) * hedge_koff
            but only if EMA(hedge_ema_window) on hedge_ema_interval supports hedge direction.
          - Close hedge when:
                a) price reaches significant level (same as averaging level) OR
                b) EMA flips against hedge (if enabled).
        """
        if (not self._is_live) or self._binance is None:
            return {"checked": 0, "opened": 0, "closed": 0, "skipped": 0}

        if not bool(getattr(self.p, "hedge_enabled", False)):
            return {"checked": 0, "opened": 0, "closed": 0, "skipped": 0}

        hedge_koff = float(getattr(self.p, "hedge_koff", 1.2) or 1.2)
        ema_win = int(getattr(self.p, "hedge_ema_window", 20) or 20)
        ema_tf = str(getattr(self.p, "hedge_ema_interval", "5m") or "5m")
        ema_confirm = int(getattr(self.p, "hedge_ema_confirm_bars", 3) or 3)
        ema_min_slope = float(getattr(self.p, "hedge_ema_min_slope_pct", 0.0) or 0.0)
        ema_flip_mult = float(getattr(self.p, "hedge_ema_flip_slope_mult", 0.7) or 0.7)
        hedge_cooldown_sec = float(getattr(self.p, "hedge_cooldown_sec", 180) or 180)
        if ema_flip_mult <= 0:
            ema_flip_mult = 0.7
        ema_close_slope = float(ema_min_slope) * float(ema_flip_mult)

        # Exchange snapshot positions (positionRisk)
        pr = self._rest_snapshot_get("position_risk")
        pr_rows = pr if isinstance(pr, list) else []
        pr_map = {}  # (symbol, positionSide)-> dict
        for r in pr_rows:
            sym = str(r.get("symbol") or "").strip()
            ps = str(r.get("positionSide") or ("BOTH" if not bool(getattr(self.p,"hedge_enabled",False)) else "")).strip().upper()
            if not sym:
                continue
            pr_map[(sym, ps)] = r

        # Ledger OPEN positions for strategy
        led = []
        # NOTE: position_ledger schema can differ between deployments.
        # We only need basic fields + optional raw_meta (for hedge state).
        try:
            sel = "symbol_id, pos_uid, side, status, qty_current, avg_price, entry_price, COALESCE(source,'live') AS source"
            if getattr(self, "_pl_has_raw_meta", False):
                sel += ", raw_meta"
            led = list(
                self.store.query_dict(
                    f"""
                    SELECT {sel}
                    FROM position_ledger
                    WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND strategy_id=%(sid)s AND status IN ('OPEN','CLOSED') AND COALESCE(source,'live') IN ('live','live_hedge')
                    ORDER BY opened_at DESC;
                    """,
                    {"ex": int(self.exchange_id), "acc": int(self.account_id), "sid": str(self.STRATEGY_ID)},
                )
            )
        except Exception:
            # fallback without raw_meta
            led = list(
                self.store.query_dict(
                    """
                    SELECT symbol_id, pos_uid, side, status, qty_current, avg_price, entry_price, COALESCE(source,'live') AS source
                    FROM position_ledger
                    WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND strategy_id=%(sid)s AND status IN ('OPEN','CLOSED') AND COALESCE(source,'live') IN ('live','live_hedge')
                    ORDER BY opened_at DESC;
                    """,
                    {"ex": int(self.exchange_id), "acc": int(self.account_id), "sid": str(self.STRATEGY_ID)},
                )
            )

        checked = opened = closed = skipped = 0
        sym_map = self._symbols_map()

        for p in led:
            checked += 1
            symbol_id = int(p["symbol_id"])
            symbol = sym_map.get(symbol_id) or ""
            if not symbol:
                skipped += 1
                continue

            main_side = str(p.get("side") or "").upper()
            if main_side not in {"LONG","SHORT"}:
                skipped += 1
                continue
            src_kind = str(p.get("source") or "live").lower().strip()
            # IMPORTANT: hedge manager must be driven only by the base/main strategy rows.
            # OPEN/CLOSED live_hedge rows are auxiliary mirror records for the opposite leg.
            # If we process them here as if they were independent main positions, the loop starts
            # managing the hedge-of-a-hedge, which leads to missing or flapping HEDGE_TRL orders
            # after hedge reopen/recovery (for example ETHUSDT LONG in the user's logs).
            if src_kind == "live_hedge":
                # Auxiliary hedge mirror rows must not drive hedge-of-a-hedge logic.
                # But if such a row is still OPEN while the opposite leg is already
                # absent on exchange, proactively close the stale mirror here so the
                # next invariant pass can manage the surviving real leg as the main one.
                try:
                    hedge_ps_aux = str(main_side).upper()
                    hedge_pos_aux = pr_map.get((symbol, hedge_ps_aux))
                    hedge_amt_aux = 0.0
                    if hedge_pos_aux:
                        try:
                            hedge_amt_aux = abs(float(hedge_pos_aux.get("positionAmt") or 0.0))
                        except Exception:
                            hedge_amt_aux = 0.0
                    if float(hedge_amt_aux or 0.0) <= 0.0 and str(p.get("status") or "OPEN").upper() == "OPEN":
                        try:
                            self._close_position_exchange(
                                p,
                                exit_price=float(self._get_mark_price(symbol, hedge_ps_aux) or self._get_mark_price(symbol, main_side) or 0.0),
                                realized_pnl=0.0,
                                close_time_ms=None,
                                reason="stale_live_hedge_mirror",
                                timeframe="hedge_reconcile",
                            )
                        except Exception:
                            log.debug(
                                "[TL][hedge_ready] failed to auto-close stale live_hedge mirror sym=%s side=%s pos_uid=%s",
                                symbol,
                                hedge_ps_aux,
                                str(p.get("pos_uid") or ""),
                                exc_info=True,
                            )
                except Exception:
                    pass
                skipped += 1
                continue


            pl_status = str(p.get("status") or "OPEN").upper().strip()
            if pl_status not in {"OPEN", "CLOSED"}:
                pl_status = "OPEN"

            qty = abs(float(p.get("qty_current") or 0.0))
            if pl_status == "OPEN" and qty <= 0:
                skipped += 1
                continue

            entry = _safe_float(p.get("avg_price"), default=_safe_float(p.get("entry_price"), default=0.0))
            if pl_status == "OPEN" and entry <= 0:
                skipped += 1
                continue

            # Determine hedge direction and required EMA direction
            hedge_side = "SHORT" if main_side == "LONG" else "LONG"
            need_ema = "DOWN" if hedge_side == "SHORT" else "UP"
            # EMA direction for OPEN must be stricter (min slope); for CLOSE we can use a softer threshold
            ema_dir_open = self._ema_direction(
                symbol_id,
                window=ema_win,
                interval=ema_tf,
                confirm_bars=ema_confirm,
                min_slope_pct=ema_min_slope,
                log_ctx="open",
            )
            if need_ema and ema_dir_open is None:
                self.log.info(
                    f"[TL][ema] {symbol}: open EMA direction is not available (need_ema={need_ema}); check candles history / interval"
                )
            ema_dir_close = self._ema_direction(
                symbol_id,
                window=ema_win,
                interval=ema_tf,
                confirm_bars=ema_confirm,
                min_slope_pct=ema_close_slope,
                log_ctx="close",
            )
            if need_ema and ema_dir_close is None:
                self.log.info(
                    f"[TL][ema] {symbol}: close EMA direction is not available (need_ema={need_ema}); check candles history / interval"
                )
            hedge_ok = (ema_dir_open == need_ema)

            # Detect existing hedge position on exchange
            # In hedge mode we should have positionSide LONG/SHORT.
            main_ps = main_side
            hedge_ps = hedge_side
            main_pos = pr_map.get((symbol, main_ps))
            hedge_pos = pr_map.get((symbol, hedge_ps))

            hedge_amt = 0.0
            hedge_entry = 0.0
            if hedge_pos:
                try:
                    hedge_amt = abs(float(hedge_pos.get("positionAmt") or 0.0))
                except Exception:
                    hedge_amt = 0.0
                try:
                    hedge_entry = abs(float(hedge_pos.get("entryPrice") or hedge_pos.get("avgEntryPrice") or 0.0))
                except Exception:
                    hedge_entry = 0.0

            # If main is already closed in ledger, still manage hedge only if it exists on exchange
            if pl_status != "OPEN" and float(hedge_amt or 0.0) <= 0.0:
                skipped += 1
                continue

            # Main position info (for PnL controls) + hedge base qty from ledger raw_meta
            main_amt = 0.0
            main_entry_ex = 0.0
            main_upnl_ex: Optional[float] = None
            if main_pos:
                try:
                    main_amt = abs(float(main_pos.get("positionAmt") or 0.0))
                except Exception:
                    main_amt = 0.0
                try:
                    main_entry_ex = abs(float(main_pos.get("entryPrice") or main_pos.get("avgEntryPrice") or 0.0))
                except Exception:
                    main_entry_ex = 0.0
                try:
                    v = main_pos.get("unRealizedProfit")
                    if v is not None:
                        main_upnl_ex = float(v)
                except Exception:
                    main_upnl_ex = None

            hedge_upnl_ex: Optional[float] = None
            if hedge_pos:
                try:
                    v = hedge_pos.get("unRealizedProfit")
                    if v is not None:
                        hedge_upnl_ex = float(v)
                except Exception:
                    hedge_upnl_ex = None

            hedge_base_qty: Optional[float] = None
            hedge_last_action_ts: float = 0.0
            hedge_last_open_ts: float = 0.0
            hedge_last_close_ts: float = 0.0
            hedge_last_close_px: float = 0.0
            hedge_reopen_anchor_px: float = 0.0
            hedge_be_best_px: float = 0.0
            hedge_be_armed: bool = False
            raw_meta = p.get("raw_meta") if isinstance(p, dict) else None
            if raw_meta is not None:
                try:
                    rm = raw_meta
                    if isinstance(rm, str):
                        import json as _json
                        rm = _json.loads(rm) if rm.strip() else {}
                    if isinstance(rm, dict):
                        h = rm.get("hedge") or {}
                        if isinstance(h, dict):
                            if h.get("base_qty") is not None:
                                try:
                                    hedge_base_qty = float(h.get("base_qty"))
                                except Exception:
                                    hedge_base_qty = None
                            try:
                                hedge_last_action_ts = float(h.get("last_action_ts") or 0.0)
                            except Exception:
                                hedge_last_action_ts = 0.0
                            try:
                                hedge_last_open_ts = float(h.get("last_open_ts") or 0.0)
                            except Exception:
                                hedge_last_open_ts = 0.0
                            try:
                                hedge_last_close_ts = float(h.get("last_close_ts") or 0.0)
                            except Exception:
                                hedge_last_close_ts = 0.0
                            try:
                                hedge_last_close_px = float(h.get("last_close_px") or 0.0)
                            except Exception:
                                hedge_last_close_px = 0.0
                            try:
                                hedge_reopen_anchor_px = float(h.get("reopen_anchor_px") or 0.0)
                            except Exception:
                                hedge_reopen_anchor_px = 0.0
                            try:
                                hedge_be_best_px = float(h.get("be_best_px") or 0.0)
                            except Exception:
                                hedge_be_best_px = 0.0
                            try:
                                hedge_be_armed = bool(h.get("be_armed"))
                            except Exception:
                                hedge_be_armed = False
                            if hedge_reopen_anchor_px <= 0.0:
                                hedge_reopen_anchor_px = float(hedge_last_close_px or 0.0)
                except Exception:
                    hedge_base_qty = None

            all_pl_limit = float(getattr(self.p, "all_P_L", 0.0) or 0.0)

            mark = 0.0
            try:
                mark = float(self._get_mark_price(symbol, hedge_ps) or self._get_mark_price(symbol, main_ps) or 0.0)
            except Exception:
                mark = 0.0
            if mark <= 0:
                skipped += 1
                continue


            # If hedge position has just been closed externally (e.g., by HEDGE_SL/HEDGE_TRL on exchange),
            # persist last_close_ts + last_close_px into position_ledger.raw_meta so that reopen filters
            # can work after restarts.
            try:
                if float(hedge_amt or 0.0) <= 0.0 and float(hedge_last_open_ts or 0.0) > 0.0:
                    # Detect transition: we have a recorded last_open_ts newer than last_close_ts
                    # (meaning hedge was open at least once) but currently exchange hedge_amt==0.
                    if float(hedge_last_open_ts or 0.0) > float(hedge_last_close_ts or 0.0):
                        if getattr(self, "_pl_has_raw_meta", False):
                            import json as _json
                            meta = {"hedge": {"last_close_reason": "ALGO", "last_close_ts": float(time.time()), "last_close_px": float(mark), "reopen_anchor_px": float(mark), "last_action_ts": float(time.time())}}
                            uqm = '''
                            UPDATE position_ledger
                            SET raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb,
                                updated_at = now()
                            WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s
                            '''
                            self.store.execute(uqm, {"meta": _json.dumps(meta), "ex": int(self.exchange_id), "acc": int(self.account_id), "pos_uid": str(pos_uid)})
                        hedge_last_close_ts = float(time.time())
                        hedge_last_close_px = float(mark)
                        hedge_reopen_anchor_px = float(mark)
            except Exception:
                pass

            # Compute unrealized PnL (prefer exchange fields; fallback to mark/entry)
            def _calc_upnl(side: str, entry_px: float, amt: float, upnl_ex: Optional[float]) -> float:
                if upnl_ex is not None:
                    try:
                        return float(upnl_ex)
                    except Exception:
                        pass
                if entry_px <= 0 or amt <= 0:
                    return 0.0
                if side == "LONG":
                    return (mark - entry_px) * amt
                return (entry_px - mark) * amt

            main_entry_use = float(main_entry_ex or entry or 0.0)
            main_upnl = _calc_upnl(main_side, main_entry_use, float(main_amt or qty), main_upnl_ex)
            hedge_upnl = 0.0
            if hedge_amt > 0:
                hedge_entry_use = float(hedge_entry or 0.0)
                if hedge_entry_use <= 0 and hedge_pos:
                    try:
                        hedge_entry_use = abs(float(hedge_pos.get("entryPrice") or hedge_pos.get("avgEntryPrice") or 0.0))
                    except Exception:
                        hedge_entry_use = 0.0
                hedge_upnl = _calc_upnl(hedge_side, hedge_entry_use, float(hedge_amt), hedge_upnl_ex)

            combined_upnl = float(main_upnl) + float(hedge_upnl)


            # ------------------------------------------------------------------
            # Hedge trailing stop (reduce-only) after hedge position is OPEN
            # ------------------------------------------------------------------
            if hedge_amt > 0 and bool(getattr(self.p, "hedge_trailing_enabled", False)):
                try:
                    # Strict ownership guard:
                    # HEDGE_TRL must belong to a real OPEN hedge leg that is linked to the current
                    # base position. Without this, historical CLOSED base rows can recreate
                    # HEDGE_TRL on the main positionSide and duplicate the normal main TRL.
                    hedge_leg_owned = False
                    try:
                        row = self.store.fetch_one(
                            """
                            SELECT 1 AS ok
                            FROM position_ledger pl
                            JOIN hedge_links hl
                              ON hl.exchange_id = pl.exchange_id
                             AND hl.symbol_id = pl.symbol_id
                             AND hl.base_account_id = pl.account_id
                             AND hl.hedge_account_id = pl.account_id
                             AND hl.hedge_pos_uid = pl.pos_uid
                            WHERE pl.exchange_id = %s
                              AND pl.account_id = %s
                              AND pl.symbol_id = %s
                              AND pl.side = %s
                              AND pl.status = 'OPEN'
                              AND COALESCE(pl.source, 'live') = 'live_hedge'
                              AND hl.base_pos_uid = %s
                            LIMIT 1
                            """,
                            (
                                int(self.exchange_id),
                                int(self.account_id),
                                int(symbol_id),
                                str(hedge_ps),
                                str(p.get('pos_uid') or ''),
                            ),
                        )
                        hedge_leg_owned = bool(row)
                    except Exception:
                        hedge_leg_owned = False

                    # Fallback: if the explicit hedge_links row is not visible yet (for example,
                    # immediately after a hedge re-open was detected from exchange state), allow
                    # the dedicated HEDGE_TRL manager to work when there is any OPEN live_hedge row
                    # on the same symbol/side for this account. This prevents a reopened hedge leg
                    # from staying without its own trailing protection just because the link row was
                    # created later than the exchange position snapshot.
                    if not hedge_leg_owned:
                        try:
                            row_any = self.store.fetch_one(
                                """
                                SELECT pl.pos_uid
                                FROM position_ledger pl
                                WHERE pl.exchange_id = %s
                                  AND pl.account_id = %s
                                  AND pl.symbol_id = %s
                                  AND pl.side = %s
                                  AND pl.status = 'OPEN'
                                  AND COALESCE(pl.source, 'live') = 'live_hedge'
                                ORDER BY pl.updated_at DESC NULLS LAST, pl.opened_at DESC NULLS LAST
                                LIMIT 1
                                """,
                                (
                                    int(self.exchange_id),
                                    int(self.account_id),
                                    int(symbol_id),
                                    str(hedge_ps),
                                ),
                            )
                            hedge_leg_owned = bool(row_any)
                        except Exception:
                            pass

                    # Guard: only manage HEDGE_TRL when hedge position is really open on exchange
                    # and we have a live_hedge row linked to this exact base position.
                    if (not hedge_leg_owned) or (not self._has_open_position_live(symbol, str(hedge_ps))):
                        tok = _coid_token(str(p.get('pos_uid') or ''), n=20)
                        cid_htrl = f"TL_{tok}_HEDGE_TRL"
                        self._dlog("skip HEDGE_TRL: no open hedge position sym=%s side=%s", symbol, hedge_ps)
                        try:
                            cache = getattr(self, "_hedge_trl_local_open", None)
                            if isinstance(cache, dict):
                                cache.pop(str(cid_htrl), None)
                        except Exception:
                            pass
                        self._cancel_algo_by_client_id_safe(symbol, cid_htrl)
                        raise StopIteration()

                    act_pct = float(getattr(self.p, "hedge_trailing_activation_pct", 1.5) or 1.5)
                    trail_pct = float(getattr(self.p, "hedge_trailing_trail_pct", 0.5) or 0.5)
                    buf_pct = float(getattr(self.p, "hedge_trailing_activation_buffer_pct", 0.25) or 0.25)
                    place_retries = int(getattr(self.p, "hedge_trailing_place_retries", 3) or 3)
                    work_type = str(getattr(self.p, "hedge_trailing_working_type", "MARK_PRICE") or "MARK_PRICE").upper()
                    reissue_on_qty = bool(getattr(self.p, "hedge_trailing_reissue_on_qty_change", True))
                    reissue_cd = float(getattr(self.p, "hedge_trailing_reissue_cooldown_sec", 30) or 30)
                    qty_tol = float(getattr(self.p, "hedge_trailing_qty_tolerance", 1e-8) or 1e-8)

                    # Binance constraints
                    if trail_pct < 0.1:
                        trail_pct = 0.1
                    if trail_pct > 10.0:
                        trail_pct = 10.0

                    # Build stable clientOrderId for hedge trailing
                    tok = _coid_token(str(p.get('pos_uid') or ''), n=20)
                    cid_htrl = f"TL_{tok}_HEDGE_TRL"

                    # DB-side dedupe: openAlgoOrders may not always return our hedge TRL promptly/consistently.
                    # If we already have an OPEN record in algo_orders for this client_algo_id, do not place again
                    # unless we are explicitly reissuing on hedge_qty change.
                    db_trl = None
                    try:
                        db_trl = self.store.fetch_one(
                            """
                            SELECT client_algo_id, quantity, updated_at
                            FROM algo_orders
                            WHERE exchange_id=%s AND account_id=%s AND client_algo_id=%s AND status='OPEN'
                            ORDER BY updated_at DESC
                            LIMIT 1
                            """,
                            (int(self.exchange_id), int(self.account_id), str(cid_htrl)),
                        )
                    except Exception:
                        db_trl = None


                    # Round qty to LOT_SIZE
                    q = float(hedge_amt)
                    try:
                        step = float(self._qty_step_for_symbol(symbol) or 0.0)
                    except Exception:
                        step = 0.0
                    if step and step > 0:
                        q = _round_qty_to_step(q, step, mode="down")

                    if q > 0:

                        # Strong dedupe for hedge trailing. Binance openAlgoOrders can lag, and on some
                        # accounts the same clientAlgoId may still be accepted again. So we combine:
                        #   1) in-process recent-place guard
                        #   2) exact clientAlgoId match in all available algo snapshots
                        #   3) semantic match: symbol + positionSide + TRAILING_STOP_MARKET + *_HEDGE_TRL
                        existing = None
                        try:
                            if not hasattr(self, "_hedge_trl_place_guard_ts"):
                                self._hedge_trl_place_guard_ts = {}
                        except Exception:
                            pass

                        place_guard_sec = float(getattr(self.p, "hedge_trailing_place_guard_sec", 20) or 20)
                        recent_place_ts = 0.0
                        try:
                            recent_place_ts = float(getattr(self, "_hedge_trl_place_guard_ts", {}).get(cid_htrl, 0.0) or 0.0)
                        except Exception:
                            recent_place_ts = 0.0

                        oo_rows = []
                        try:
                            for snap_name in ("open_algo_orders_all", "open_algo_orders"):
                                snap = self._rest_snapshot_get(snap_name)
                                if isinstance(snap, list):
                                    oo_rows.extend(snap)
                        except Exception:
                            oo_rows = []

                        try:
                            want_sym = str(symbol).upper()
                            want_ps = str(hedge_ps).upper()
                            # 1) exact client id match
                            for r in oo_rows:
                                if str(r.get("symbol") or "").upper() != want_sym:
                                    continue
                                cid = str(r.get("clientAlgoId") or r.get("clientOrderId") or "").strip()
                                if cid != cid_htrl:
                                    continue
                                existing = r
                                break
                            # 2) semantic match for same live hedge leg
                            if existing is None:
                                for r in oo_rows:
                                    if str(r.get("symbol") or "").upper() != want_sym:
                                        continue
                                    cid = str(r.get("clientAlgoId") or r.get("clientOrderId") or "").strip()
                                    if not cid.endswith("_HEDGE_TRL"):
                                        continue
                                    if want_ps and str(r.get("positionSide") or "").upper() not in {want_ps, "BOTH", ""}:
                                        continue
                                    otype = str(r.get("orderType") or r.get("type") or "").upper()
                                    if otype and otype != "TRAILING_STOP_MARKET":
                                        continue
                                    existing = r
                                    break
                        except Exception:
                            existing = None

                        # Fallback dedupe: Binance may not return some algo types in openAlgoOrders.
                        db_htrl = None
                        try:
                            db_htrl = self.store.fetch_one(
                                """SELECT status, quantity, trigger_price, updated_at
                                   FROM algo_orders
                                   WHERE exchange_id=%s AND account_id=%s AND client_algo_id=%s
                                   ORDER BY updated_at DESC
                                   LIMIT 1""",
                                (int(self.exchange_id), int(self.account_id), str(cid_htrl)),
                            )
                        except Exception:
                            db_htrl = None
                        have_db_open = bool(db_htrl and str(db_htrl.get("status") or "").upper() == "OPEN")

                        # In-memory dedupe for exchanges/accounts where openAlgoOrders does not
                        # reliably return TRAILING_STOP_MARKET hedge orders. Once we successfully
                        # place HEDGE_TRL, keep a local "open" marker until hedge leg disappears
                        # or we explicitly reissue/cancel it.
                        local_htrl = None
                        try:
                            cache = getattr(self, "_hedge_trl_local_open", None)
                            if not isinstance(cache, dict):
                                self._hedge_trl_local_open = {}
                                cache = self._hedge_trl_local_open
                            local_htrl = cache.get(str(cid_htrl))
                        except Exception:
                            local_htrl = None

                        local_open_same = False
                        if isinstance(local_htrl, dict):
                            try:
                                local_open_same = (
                                    str(local_htrl.get("symbol") or "").upper() == str(symbol).upper()
                                    and str(local_htrl.get("position_side") or "").upper() == str(hedge_ps).upper()
                                    and abs(float(local_htrl.get("quantity") or 0.0) - float(q)) <= max(qty_tol, 0.0)
                                )
                            except Exception:
                                local_open_same = False

                        # Determine if we need to (re)issue
                        need_place = (existing is None) and (not have_db_open) and (not local_open_same)
                        need_reissue = False
                        if (not need_place) and reissue_on_qty:
                            try:
                                src = existing
                                if src is None and db_htrl is not None:
                                    src = {"quantity": db_htrl.get("quantity")}
                                ex_q = float((src or {}).get("origQty") or (src or {}).get("quantity") or 0.0)
                            except Exception:
                                ex_q = 0.0
                            if abs(ex_q - q) > max(qty_tol, 0.0):
                                need_reissue = True

                        # Cooldown tracking for reissue to avoid churn
                        if not hasattr(self, "_hedge_trl_last_reissue_ts"):
                            self._hedge_trl_last_reissue_ts = {}
                        last_ts = float(self._hedge_trl_last_reissue_ts.get(cid_htrl, 0.0) or 0.0)
                        now_ts = time.time()

                        if need_reissue and (now_ts - last_ts) < reissue_cd:
                            need_reissue = False

                        if need_reissue:
                            # cancel existing before reissue
                            try:
                                self._binance.cancel_algo_order(symbol=symbol, clientAlgoId=cid_htrl)
                                try:
                                    cache = getattr(self, "_hedge_trl_local_open", None)
                                    if isinstance(cache, dict):
                                        cache.pop(str(cid_htrl), None)
                                except Exception:
                                    pass
                                self._hedge_trl_last_reissue_ts[cid_htrl] = now_ts
                                log.info("[TL][HEDGE_TRL] canceled for reissue %s %s cid=%s", symbol, hedge_ps, cid_htrl)

                                # Persist cancel into algo_orders (for audit)
                                try:
                                    self.store.upsert_algo_orders([
                                        {
                                            "exchange_id": int(self.exchange_id),
                                            "account_id": int(self.account_id),
                                            "client_algo_id": str(cid_htrl),
                                            "algo_id": str((existing or {}).get("orderId") or (existing or {}).get("algoId") or ""),
                                            "symbol": str(symbol),
                                            "side": str(existing.get("side") or ""),
                                            "position_side": str(hedge_ps).upper(),
                                            "type": "TRAILING_STOP_MARKET",
                                            "quantity": float(q),
                                            "trigger_price": 0.0,
                                            "working_type": str(work_type),
                                            "status": "CANCELED",
                                            "strategy_id": str(self.STRATEGY_ID),
                                            "pos_uid": str(p.get("pos_uid") or ""),
                                            "raw_json": {"cancel_reason": "reissue_qty_change", "cancel_source": "bot"},
                                        }
                                    ])
                                except Exception:
                                    pass
                            except Exception:
                                log.exception("[TL][HEDGE_TRL] failed to cancel for reissue %s %s cid=%s", symbol, hedge_ps, cid_htrl)

                            need_place = True

                        if need_place:
                            # Last local guard before sending REST request. Prevent duplicates inside the same
                            # cycle / shortly after a successful placement when exchange snapshots lag behind.
                            if recent_place_ts and (now_ts - recent_place_ts) < place_guard_sec:
                                need_place = False

                        if need_place:
                            # Activation must be computed from the hedge average entry price:
                            #   LONG  -> above entry by configured pct
                            #   SHORT -> below entry by configured pct
                            # Then we apply a mark-price safety buffer to avoid immediate trigger rejection,
                            # but keep activation on the correct side of the entry.
                            hedge_entry_ref = float(hedge_entry or 0.0)
                            if hedge_entry_ref <= 0.0 and hedge_pos:
                                try:
                                    hedge_entry_ref = abs(float(hedge_pos.get("entryPrice") or hedge_pos.get("avgEntryPrice") or 0.0))
                                except Exception:
                                    hedge_entry_ref = 0.0
                            if hedge_entry_ref <= 0.0:
                                raise RuntimeError(f"HEDGE_TRL: no valid hedge entry for {symbol} {hedge_ps}")

                            try:
                                tick_h = float(self._price_tick_for_symbol(symbol) or 0.0)
                            except Exception:
                                tick_h = 0.0

                            if str(hedge_ps).upper() == "SHORT":
                                activation = float(hedge_entry_ref) * (1.0 - act_pct / 100.0)
                                close_side = "BUY"
                                if float(mark or 0.0) > 0.0 and tick_h > 0.0:
                                    max_act = float(mark) * (1.0 - buf_pct / 100.0)
                                    if activation >= max_act:
                                        activation = float(_round_price_to_tick(max_act - tick_h, tick_h, mode="down"))
                            else:
                                activation = float(hedge_entry_ref) * (1.0 + act_pct / 100.0)
                                close_side = "SELL"
                                if float(mark or 0.0) > 0.0 and tick_h > 0.0:
                                    min_act = float(mark) * (1.0 + buf_pct / 100.0)
                                    if activation <= min_act:
                                        activation = float(_round_price_to_tick(min_act + tick_h, tick_h, mode="up"))

                            if tick_h > 0.0:
                                activation = float(_round_price_to_tick(
                                    activation,
                                    tick_h,
                                    mode=("down" if str(hedge_ps).upper() == "SHORT" else "up"),
                                ))
                                activation = float(_ensure_tp_trail_side(
                                    activation,
                                    hedge_entry_ref,
                                    tick_h,
                                    str(hedge_ps).upper(),
                                    kind="hedge_trail_activate",
                                ))

                            placed = False
                            last_err = None
                            for _ in range(max(1, place_retries)):
                                try:
                                    resp = self._binance.new_order(
                                        symbol=symbol,
                                        side=close_side,
                                        type="TRAILING_STOP_MARKET",
                                        quantity=float(q),
                                        positionSide=str(hedge_ps).upper(),
                                        newClientOrderId=cid_htrl,
                                        activationPrice=float(activation),
                                        callbackRate=float(trail_pct),
                                        workingType=work_type,
                                    )

                                    # Persist into algo_orders (TRAILING_STOP_MARKET is an algo order in Binance futures)
                                    try:
                                        self.store.upsert_algo_orders([
                                            {
                                                "exchange_id": int(self.exchange_id),
                                                "account_id": int(self.account_id),
                                                "client_algo_id": str(cid_htrl),
                                                "algo_id": str((resp or {}).get("algoId") or (resp or {}).get("orderId") or ""),
                                                "symbol": str(symbol),
                                                "side": str(close_side),
                                                "position_side": str(hedge_ps).upper(),
                                                "type": "TRAILING_STOP_MARKET",
                                                "quantity": float(q),
                                                "trigger_price": float(activation),
                                                "working_type": str(work_type),
                                                "status": "OPEN",
                                                "strategy_id": str(self.STRATEGY_ID),
                                                "pos_uid": str(p.get("pos_uid") or ""),
                                                "raw_json": dict(resp or {}, **{"kind": "HEDGE_TRL"}),
                                            }
                                        ])
                                    except Exception:
                                        pass
                                    # Shadow row in orders (optional, but helps audit)
                                    try:
                                        self._upsert_order_shadow(
                                            pos_uid=str(p.get("pos_uid") or ""),
                                            order_id=(resp or {}).get("orderId"),
                                            client_order_id=cid_htrl,
                                            symbol_id=int(symbol_id),
                                            side=close_side,
                                            order_type="TRAILING_STOP_MARKET",
                                            status="NEW",
                                            qty=float(q),
                                            price=float(activation),
                                            reduce_only=True,
                                        )
                                    except Exception:
                                        pass

                                    self._hedge_trl_last_reissue_ts[cid_htrl] = now_ts
                                    try:
                                        self._hedge_trl_place_guard_ts[cid_htrl] = now_ts
                                    except Exception:
                                        pass
                                    try:
                                        cache = getattr(self, "_hedge_trl_local_open", None)
                                        if not isinstance(cache, dict):
                                            self._hedge_trl_local_open = {}
                                            cache = self._hedge_trl_local_open
                                        cache[str(cid_htrl)] = {
                                            "symbol": str(symbol).upper(),
                                            "position_side": str(hedge_ps).upper(),
                                            "side": str(close_side).upper(),
                                            "quantity": float(q),
                                            "activation_price": float(activation),
                                            "callback_rate": float(trail_pct),
                                            "placed_at": float(now_ts),
                                            "pos_uid": str(p.get("pos_uid") or ""),
                                        }
                                    except Exception:
                                        pass
                                    try:
                                        snap = self._rest_snapshot_get("open_algo_orders_all")
                                        if isinstance(snap, list):
                                            snap.append({
                                                "symbol": str(symbol).upper(),
                                                "clientAlgoId": str(cid_htrl),
                                                "positionSide": str(hedge_ps).upper(),
                                                "orderType": "TRAILING_STOP_MARKET",
                                                "type": "TRAILING_STOP_MARKET",
                                                "side": str(close_side).upper(),
                                                "quantity": float(q),
                                                "callbackRate": float(trail_pct),
                                                "activatePrice": float(activation),
                                            })
                                    except Exception:
                                        pass
                                    log.info(
                                        "[TL][HEDGE_TRL] placed %s %s qty=%.8f activation=%.8f cb=%.4f cid=%s",
                                        symbol, str(hedge_ps).upper(), float(q), float(activation), float(trail_pct), cid_htrl
                                    )
                                    placed = True
                                    break
                                except Exception as e:
                                    last_err = e
                                    time.sleep(0.2)
                            if not placed and last_err is not None:
                                log.warning("[TL][HEDGE_TRL] failed to place %s %s cid=%s err=%s", symbol, hedge_ps, cid_htrl, str(last_err))

                except StopIteration:
                    # Guarded skip
                    pass

                except Exception:
                    log.exception("[TL][HEDGE_TRL] unexpected error for %s %s", symbol, hedge_ps)

            # Side-specific cleanup: if hedge leg is flat or hedge trailing disabled, cancel any dangling HEDGE_TRL
            if (hedge_amt <= 0) or (not bool(getattr(self.p, "hedge_trailing_enabled", False))):
                try:
                    tok = _coid_token(str(p.get('pos_uid') or ''), n=20)
                    cid_htrl = f"TL_{tok}_HEDGE_TRL"
                    self._cancel_algo_by_client_id_safe(symbol, cid_htrl)
                except Exception:
                    pass

            # ------------------------------------------------------------------
            # Hedge hard stop-loss (reduce-only) from hedge ENTRY price
            # Close hedge on adverse move ONLY by configured pct from hedge entry
            # ------------------------------------------------------------------
            if hedge_amt > 0:
                try:
                    hedge_sl_pct = float(getattr(self.p, "hedge_stop_loss_pct", 0.0) or 0.0)
                except Exception:
                    hedge_sl_pct = 0.0

                if hedge_sl_pct and hedge_sl_pct > 0:
                    # Place stop-loss for the hedge position as an ALGO order (kept in algo_orders)
                    try:
                        tok = _coid_token(str(p.get('pos_uid') or ''), n=20)
                        cid_hsl = f"TL_{tok}_HEDGE_SL"
                
                        hedge_entry_use = float(hedge_entry or 0.0)
                        # mark price is needed for fallback/BE logic; compute locally to avoid NameError
                        mp0 = float(self._get_mark_price_live(symbol) or 0.0)
                        if hedge_entry_use <= 0:
                            hedge_entry_use = mp0
                
                        if str(hedge_ps).upper() == 'SHORT':
                            stop_px = hedge_entry_use * (1.0 + hedge_sl_pct / 100.0)
                            sl_side = 'BUY'
                        else:
                            stop_px = hedge_entry_use * (1.0 - hedge_sl_pct / 100.0)
                            sl_side = 'SELL'

                        # Optional: move hedge SL to break-even (profit-lock) after activation move
                        try:
                            be_pct = float(getattr(self.p, "hedge_stop_loss_be_pct", 0.0) or 0.0)
                            be_act_pct = float(getattr(self.p, "hedge_stop_loss_be_activation_pct", 0.0) or 0.0)
                        except Exception:
                            be_pct, be_act_pct = 0.0, 0.0

                        be_applied = False
                        if be_pct > 0 and be_act_pct > 0 and hedge_entry_use > 0:
                            try:
                                be_pct_min = float(getattr(self.p, "hedge_stop_loss_be_pct_min", 0.0) or 0.0)
                                be_pct_max = float(getattr(self.p, "hedge_stop_loss_be_pct_max", 100.0) or 100.0)
                                be_act_min = float(getattr(self.p, "hedge_stop_loss_be_activation_pct_min", 0.0) or 0.0)
                                be_act_max = float(getattr(self.p, "hedge_stop_loss_be_activation_pct_max", 100.0) or 100.0)
                            except Exception:
                                be_pct_min, be_pct_max, be_act_min, be_act_max = 0.0, 100.0, 0.0, 100.0

                            # clamp (limits are configurable in YAML)
                            be_pct = max(be_pct_min, min(be_pct, be_pct_max))
                            be_act_pct = max(be_act_min, min(be_act_pct, be_act_max))

                            mp = mp0
                            if mp > 0:
                                if str(hedge_ps).upper() == "SHORT":
                                    # Activate when price moved down in favor; then set stop below entry (profit-lock)
                                    act_px = hedge_entry_use * (1.0 - be_act_pct / 100.0)
                                    if mp <= act_px:
                                        be_stop = hedge_entry_use * (1.0 - be_pct / 100.0)
                                        stop_px = min(stop_px, be_stop)
                                        be_applied = True
                                else:
                                    # LONG hedge: activate when price moved up in favor; set stop above entry
                                    act_px = hedge_entry_use * (1.0 + be_act_pct / 100.0)
                                    if mp >= act_px:
                                        be_stop = hedge_entry_use * (1.0 + be_pct / 100.0)
                                        stop_px = max(stop_px, be_stop)
                                        be_applied = True
                                        
                        # Quantity tolerance for deciding whether to reissue HEDGE_SL.
                        # Use max(config_tol, 0.5*step) to avoid endless cancel/recreate due to rounding.
                        try:
                            step = self._qty_step_for_symbol(symbol)
                            step = float(step) if step is not None else 0.0
                        except Exception:
                            step = 0.0
                        cfg_tol = float(getattr(self.p, 'hedge_trailing_qty_tolerance', 1e-8) or 1e-8)
                        qty_tol = max(cfg_tol, step * 0.5 if step > 0 else cfg_tol)
                        reissue_on_qty = bool(getattr(self.p, 'hedge_trailing_reissue_on_qty_change', True))
                        reissue_cd = float(getattr(self.p, 'hedge_trailing_reissue_cooldown_sec', 30) or 30)
                        # Prefer exchange/local snapshot state for recovery after restart.
                        ex_hsl = self._find_known_algo_order_by_client_id(symbol, cid_hsl)
                        if ex_hsl is None:
                            ex_hsl = self._find_semantic_open_algo(
                                symbol,
                                client_suffix="_HEDGE_SL",
                                position_side=str(hedge_ps).upper(),
                                order_type="STOP_MARKET",
                                side=sl_side,
                            )
                        ex_open = bool(ex_hsl is not None)
                        try:
                            ex_qty = float((ex_hsl or {}).get("quantity") or (ex_hsl or {}).get("origQty") or 0.0)
                        except Exception:
                            ex_qty = 0.0
                        try:
                            ex_trig = float((ex_hsl or {}).get("triggerPrice") or (ex_hsl or {}).get("stopPrice") or 0.0)
                        except Exception:
                            ex_trig = 0.0

                        db_hsl = self.store.get_algo_order(exchange_id=self.exchange_id, account_id=self.account_id, client_algo_id=cid_hsl)
                        db_open = bool(db_hsl and str(db_hsl.get('status') or '').upper() == 'OPEN')

                        # Treat as open if either DB or exchange says it's open.
                        is_open_now = bool(ex_open or db_open)

                        db_qty = None
                        try:
                            if db_hsl is not None:
                                db_qty = float(db_hsl.get('quantity') or 0)
                        except Exception:
                            db_qty = None

                        db_trig = None
                        try:
                            if db_hsl is not None:
                                db_trig = float(db_hsl.get('trigger_price') or 0.0)
                        except Exception:
                            db_trig = None

                        # If exchange snapshot has the order, use its live values for comparisons (recovery-safe)
                        if ex_open:
                            if ex_qty and ex_qty > 0:
                                db_qty = ex_qty
                            if ex_trig and ex_trig > 0:
                                db_trig = ex_trig

                        # Reissue if trigger price changed (e.g. moved to break-even).
                        # IMPORTANT: when SL has already been moved to BE/profit-lock, do NOT keep reissuing
                        # due to tiny rounding differences after restart.
                        try:
                            tick = self._price_tick_for_symbol(symbol)
                            tol_px = float(tick) * 0.5 if tick is not None and float(tick) > 0 else 1e-8
                        except Exception:
                            tol_px = 1e-8

                        # Compare using the same rounding as order placement
                        try:
                            desired_trig = float(self._fmt_price(symbol, stop_px))
                        except Exception:
                            desired_trig = float(stop_px)

                        trig_mismatch = bool(is_open_now and db_trig is not None and abs(db_trig - float(desired_trig)) > tol_px)

                        # If BE is already on exchange (trigger crossed to the BE side), don't reissue again.
                        # SHORT hedge: BE stop is below entry => trigger < entry
                        # LONG  hedge: BE stop is above entry => trigger > entry
                        be_already = False
                        try:
                            if is_open_now and db_trig is not None and hedge_entry_use > 0:
                                if str(hedge_ps).upper() == 'SHORT' and float(db_trig) < float(hedge_entry_use):
                                    be_already = True
                                elif str(hedge_ps).upper() != 'SHORT' and float(db_trig) > float(hedge_entry_use):
                                    be_already = True
                        except Exception:
                            be_already = False

                        if be_applied and be_already:
                            trig_mismatch = False

                        # Sticky BE: if SL was already moved to BE on exchange, never revert it back to the original SL
                        # even if the activation condition is no longer true (price can whipsaw around the threshold).
                        # We only allow reissue in this case if quantity changed.
                        if be_already and (db_trig is not None) and float(db_trig) > 0:
                            desired_trig = float(db_trig)
                            trig_mismatch = False

                        qty_mismatch = False
                        if is_open_now and reissue_on_qty and db_qty is not None:
                            try:
                                # Compare in exchange step precision
                                db_qty_f = float(self._fmt_qty(symbol, float(db_qty)))
                                want_qty_f = float(self._fmt_qty(symbol, float(hedge_amt)))
                                qty_mismatch = bool(abs(db_qty_f - want_qty_f) > qty_tol)
                            except Exception:
                                qty_mismatch = bool(abs(float(db_qty) - float(hedge_amt)) > qty_tol)
                        need_reissue = bool(qty_mismatch or trig_mismatch)
                
                        if (not is_open_now) or need_reissue:
                            now_ts = time.time()
                            if not hasattr(self, '_hsl_last_ts'):
                                self._hsl_last_ts = {}
                            last_ts = float(self._hsl_last_ts.get(cid_hsl, 0.0))
                            if (now_ts - last_ts) >= reissue_cd:
                                try:
                                    self._cancel_algo_by_client_id_safe(symbol, cid_hsl)
                                except Exception:
                                    pass
                
                                sl_working_type = str(
                
                                    getattr(self.p, 'hedge_stop_loss_working_type', None)
                
                                    or getattr(self.p, 'hedge_trailing_working_type', None)
                
                                    or getattr(self.p, 'working_type', 'MARK_PRICE')
                
                                    or 'MARK_PRICE'
                
                                ).upper()

                
                                resp_sl = self._binance.new_algo_order(
                                    symbol=symbol,
                                    side=sl_side,
                                    type='STOP_MARKET',
                                    quantity=self._fmt_qty(symbol, hedge_amt),
                                    workingType=sl_working_type,
                                    positionSide=hedge_ps,
                                    algoType='CONDITIONAL',
                                    clientAlgoId=cid_hsl,
                                    triggerPrice=self._fmt_price(symbol, desired_trig),
                                )
                                self._upsert_algo_order_shadow(resp_sl, pos_uid=str(p.get('pos_uid') or ''), strategy_id='trade_liquidation')
                                try:
                                    self._mark_local_algo_open(
                                        cid_hsl,
                                        symbol=symbol,
                                        position_side=str(hedge_ps).upper(),
                                        order_type='STOP_MARKET',
                                        side=sl_side,
                                        quantity=float(hedge_amt),
                                        trigger_price=float(desired_trig),
                                        close_position=False,
                                        pos_uid=str(p.get('pos_uid') or ''),
                                    )
                                except Exception:
                                    pass
                                self._hsl_last_ts[cid_hsl] = now_ts
                                log.info('[TL][HEDGE_SL] placed %s %s qty=%.8f stop=%.8f entry=%.8f pct=%.4f be=%s be_pct=%.4f act_pct=%.4f cid=%s', symbol, str(hedge_ps).upper(), float(hedge_amt), float(stop_px), float(hedge_entry_use), float(hedge_sl_pct), bool(be_applied), float(be_pct or 0.0), float(be_act_pct or 0.0), cid_hsl)
                    except Exception:
                        log.exception('[TL][HEDGE_SL] unexpected error for %s %s', symbol, hedge_ps)

            # Side-specific cleanup: if hedge leg is flat now, cancel any dangling HEDGE_SL
            if hedge_amt <= 0:
                try:
                    tok = _coid_token(str(p.get('pos_uid') or ''), n=20)
                    cid_hsl = f"TL_{tok}_HEDGE_SL"
                    self._cancel_algo_by_client_id_safe(symbol, cid_hsl)
                except Exception:
                    pass
            if all_pl_limit and all_pl_limit > 0 and combined_upnl >= float(all_pl_limit):
                try:
                    # close hedge first (if exists), then main
                    if hedge_amt > 0:
                        close_side = "BUY" if hedge_side == "SHORT" else "SELL"
                        self._binance.new_order(
                            symbol=symbol,
                            side=close_side,
                            type="MARKET",
                            quantity=float(hedge_amt),
                            positionSide=hedge_ps,
                        )
                        closed += 1
                    if main_amt > 0:
                        close_side = "BUY" if main_side == "SHORT" else "SELL"
                        self._binance.new_order(
                            symbol=symbol,
                            side=close_side,
                            type="MARKET",
                            quantity=float(main_amt),
                            positionSide=main_ps,
                        )
                        closed += 1
                    log.info("[TL][HEDGE] closed ALL %s combined_upnl=%.4f >= %.4f (main=%.4f hedge=%.4f)",
                             symbol, float(combined_upnl), float(all_pl_limit), float(main_upnl), float(hedge_upnl))
                except Exception:
                    log.exception("[TL][HEDGE] failed to close ALL for %s", symbol)
                    skipped += 1
                continue

            # Close hedge if main position is already positive unrealized PnL
            main_positive = (float(main_upnl) > 0.0)

            # Compute would-be SL price (same as default SL)
            if main_side == "LONG":
                sl_price = entry * (1.0 - _pct_to_mult(self.p.stop_loss_pct))
                sl_hit = (mark <= sl_price)
            else:
                sl_price = entry * (1.0 + _pct_to_mult(self.p.stop_loss_pct))
                sl_hit = (mark >= sl_price)

            # 1) Open hedge instead of SL
            # Open hedge instead of SL (or reopen after prior close) when conditions met
            open_trigger = bool(sl_hit) if pl_status == "OPEN" else False
            if pl_status == "OPEN" and (not open_trigger) and bool(getattr(self.p, "hedge_reopen_enabled", True)):
                # reopen hedge if main is in loss and drawdown beyond configured threshold
                try:
                    dd_thr = float(getattr(self.p, "hedge_reopen_drawdown_pct", 0.0) or 0.0)
                except Exception:
                    dd_thr = 0.0
                dd = 0.0
                if main_entry_use > 0:
                    if main_side == "LONG" and mark < main_entry_use:
                        dd = (main_entry_use - mark) / main_entry_use * 100.0
                    if main_side == "SHORT" and mark > main_entry_use:
                        dd = (mark - main_entry_use) / main_entry_use * 100.0
                if float(main_upnl) < 0.0 and (dd_thr <= 0.0 or dd >= dd_thr):
                    open_trigger = True

            
            # Optional extra filter for hedge re-entry after a hedge close (SL/TRL/ALGO).
            # Re-entry direction is defined relative to the MAIN leg:
            #   - main LONG  -> re-enter hedge only if price moved ABOVE the last hedge exit anchor
            #   - main SHORT -> re-enter hedge only if price moved BELOW the last hedge exit anchor
            # In addition, when price moves far in the opposite direction of the hedge (towards the MAIN),
            # the anchor is shifted towards the MAIN to gradually reduce the distance for the next hedge entry.
            try:
                if (not bool(sl_hit)) and bool(getattr(self.p, "hedge_reopen_price_filter_enabled", False)):
                    anchor_px = float(hedge_reopen_anchor_px or hedge_last_close_px or 0.0)
                    move_pct = float(getattr(self.p, "hedge_reopen_price_move_pct", 0.0) or 0.0) / 100.0
                    shift_trigger_pct = float(getattr(self.p, "hedge_reopen_anchor_shift_trigger_pct", 0.0) or 0.0) / 100.0
                    shift_step_pct = float(getattr(self.p, "hedge_reopen_anchor_shift_step_pct", 0.0) or 0.0) / 100.0
                    if anchor_px > 0.0:
                        new_anchor_px = float(anchor_px)
                        if shift_trigger_pct > 0.0 and shift_step_pct > 0.0:
                            if str(main_side).upper() == "LONG":
                                while float(mark) <= new_anchor_px * (1.0 - shift_trigger_pct):
                                    new_anchor_px = new_anchor_px * (1.0 - shift_step_pct)
                            else:
                                while float(mark) >= new_anchor_px * (1.0 + shift_trigger_pct):
                                    new_anchor_px = new_anchor_px * (1.0 + shift_step_pct)
                        if abs(new_anchor_px - anchor_px) > 1e-12:
                            try:
                                if getattr(self, "_pl_has_raw_meta", False):
                                    import json as _json
                                    meta = {"hedge": {"reopen_anchor_px": float(new_anchor_px)}}
                                    uqm = """
                                    UPDATE position_ledger
                                    SET raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb,
                                        updated_at = now()
                                    WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND status='OPEN';
                                    """
                                    self.store.execute(
                                        uqm,
                                        {
                                            "meta": _json.dumps(meta),
                                            "ex": int(self.exchange_id),
                                            "acc": int(self.account_id),
                                            "pos_uid": str(p.get("pos_uid")),
                                        },
                                    )
                            except Exception:
                                pass
                            hedge_reopen_anchor_px = float(new_anchor_px)
                            anchor_px = float(new_anchor_px)
                        if str(main_side).upper() == "LONG":
                            target_px = anchor_px * (1.0 + max(move_pct, 0.0))
                            if not (float(mark) >= target_px):
                                open_trigger = False
                        else:
                            target_px = anchor_px * (1.0 - max(move_pct, 0.0))
                            if not (float(mark) <= target_px):
                                open_trigger = False
            except Exception:
                pass
            
            if hedge_amt <= 0 and open_trigger:
                # cooldown check (open)
                if hedge_cooldown_sec and hedge_last_action_ts and (time.time() - float(hedge_last_action_ts)) < float(hedge_cooldown_sec):
                    skipped += 1
                    continue
                if not hedge_ok:
                    skipped += 1
                    continue

                # qty and rounding
                qty_step = float(self._qty_step_for_symbol(symbol) or 0.0) or 0.0
                # hedge qty: use stored base qty if available (same as first open), otherwise compute from current main qty
                hedge_qty = float(hedge_base_qty) if (hedge_base_qty is not None and float(hedge_base_qty) > 0) else (qty * float(hedge_koff))
                if qty_step and qty_step > 0:
                    hedge_qty = _round_qty_to_step(hedge_qty, qty_step, mode="down")
                if hedge_qty <= 0 or (qty_step and hedge_qty < qty_step):
                    skipped += 1
                    continue

                side_ord = "SELL" if hedge_side == "SHORT" else "BUY"
                try:
                    resp = self._binance.new_order(
                        symbol=symbol,
                        side=side_ord,
                        type="MARKET",
                        quantity=float(hedge_qty),
                        positionSide=hedge_ps,
                    )
                    opened += 1
                    # persist hedge base qty (same for reopens) into position_ledger.raw_meta if available
                    try:
                        if getattr(self, "_pl_has_raw_meta", False):
                            import json as _json
                            reason = "SL" if bool(sl_hit) else "REOPEN"
                            meta = {"hedge": {"base_qty": float(hedge_qty), "last_open_reason": str(reason), "last_open_ts": float(time.time()), "last_action_ts": float(time.time()), "be_best_px": float(mark), "be_armed": False}}
                            uqm = '''
                            UPDATE position_ledger
                            SET raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb,
                                updated_at = now()
                            WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND status='OPEN';
                            '''
                            self.store.execute(
                                uqm,
                                {
                                    "meta": _json.dumps(meta),
                                    "ex": int(self.exchange_id),
                                    "acc": int(self.account_id),
                                    "pos_uid": str(p.get("pos_uid")),
                                },
                            )
                    except Exception:
                        pass
                    log.info("[TL][HEDGE] opened %s %s qty=%.8f (main=%s qty=%.8f) mark=%.6f sl=%.6f ema_open=%s ema_close=%s",
                             symbol, hedge_ps, float(hedge_qty), main_side, float(qty), float(mark), float(sl_price), str(ema_dir_open), str(ema_dir_close))
                except Exception as e:
                    log.exception("[TL][HEDGE] failed to open hedge for %s: %s", symbol, e)
                    skipped += 1
                continue  # next position

            # 2) Close hedge if exists and conditions met
            if hedge_amt > 0:
                close_reason = None

                # close hedge if main became profitable (optional via config)
                if bool(getattr(self.p, "hedge_close_on_main_positive", True)) and main_positive:
                    close_reason = "MAIN_POSITIVE"

                # a) close hedge at/near entry after it has first moved into profit enough
                if close_reason is None and bool(getattr(self.p, "hedge_close_on_entry_enabled", False)):
                    try:
                        be_act_pct = float(getattr(self.p, "hedge_close_on_entry_profit_activation_pct", 0.0) or 0.0) / 100.0
                    except Exception:
                        be_act_pct = 0.0
                    try:
                        be_buf_pct = float(getattr(self.p, "hedge_close_on_entry_buffer_pct", 0.0) or 0.0) / 100.0
                    except Exception:
                        be_buf_pct = 0.0
                    hedge_entry_ref = float(hedge_entry_use or 0.0)
                    hedge_upnl_now = float(hedge_upnl or 0.0)
                    hedge_pnl_ok = hedge_upnl_now >= 0.0
                    if hedge_entry_ref > 0.0:
                        best_px = float(hedge_be_best_px or hedge_entry_ref or 0.0)
                        armed_now = bool(hedge_be_armed)
                        if str(hedge_side).upper() == "SHORT":
                            if best_px <= 0.0:
                                best_px = hedge_entry_ref
                            if float(mark) < best_px:
                                best_px = float(mark)
                            trigger_px = hedge_entry_ref * (1.0 - max(be_act_pct, 0.0))
                            if float(best_px) <= trigger_px:
                                armed_now = True
                            entry_close_px = hedge_entry_ref * (1.0 - max(be_buf_pct, 0.0))
                            if armed_now and hedge_pnl_ok and float(mark) >= entry_close_px:
                                close_reason = "ENTRY_BE"
                        else:
                            if best_px <= 0.0:
                                best_px = hedge_entry_ref
                            if float(mark) > best_px:
                                best_px = float(mark)
                            trigger_px = hedge_entry_ref * (1.0 + max(be_act_pct, 0.0))
                            if float(best_px) >= trigger_px:
                                armed_now = True
                            entry_close_px = hedge_entry_ref * (1.0 + max(be_buf_pct, 0.0))
                            if armed_now and hedge_pnl_ok and float(mark) <= entry_close_px:
                                close_reason = "ENTRY_BE"
                        if abs(best_px - float(hedge_be_best_px or 0.0)) > 1e-12 or bool(armed_now) != bool(hedge_be_armed):
                            try:
                                if getattr(self, "_pl_has_raw_meta", False):
                                    import json as _json
                                    meta = {"hedge": {"be_best_px": float(best_px), "be_armed": bool(armed_now)}}
                                    uqm = """
                                    UPDATE position_ledger
                                    SET raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb,
                                        updated_at = now()
                                    WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND status='OPEN';
                                    """
                                    self.store.execute(
                                        uqm,
                                        {
                                            "meta": _json.dumps(meta),
                                            "ex": int(self.exchange_id),
                                            "acc": int(self.account_id),
                                            "pos_uid": str(p.get("pos_uid")),
                                        },
                                    )
                            except Exception:
                                pass
                            hedge_be_best_px = float(best_px)
                            hedge_be_armed = bool(armed_now)

                # b) close on significant level
                if bool(getattr(self.p, "hedge_close_on_level", True)):
                    tf_main = str(p.get("timeframe") or "15m")
                    level = self._compute_significant_level(symbol_id, side=main_side, entry_ref=float(entry), timeframe=tf_main)
                    tol = float(getattr(self.p, "hedge_level_tolerance_pct", 0.10) or 0.10) / 100.0
                    if level > 0:
                        if main_side == "LONG":
                            # close SHORT hedge when price reaches support level (or slightly above)
                            if mark <= level * (1.0 + tol):
                                close_reason = "LEVEL"
                        else:
                            # close LONG hedge when price reaches resistance level (or slightly below)
                            if mark >= level * (1.0 - tol):
                                close_reason = "LEVEL"

                # b) close on EMA flip against hedge
                if close_reason is None and bool(getattr(self.p, "hedge_close_on_ema_flip", True)):
                    # Close hedge when EMA direction flips against hedge direction.
                    # NOTE: we only close the hedge if it's currently profitable (hedge_upnl > 0),
                    # to avoid "locking in" a loss on fast whipsaws.
                    if ema_dir_close and ema_dir_close in {"UP", "DOWN"}:
                        if ema_dir_close != need_ema:
                            if float(hedge_upnl) > 0.0:
                                close_reason = "EMA_FLIP"
                                self.log.info(
                                    f"[TL][hedge] EMA flip -> close hedge: {symbol} hedge_side={hedge_side} need_ema={need_ema} ema_dir_close={ema_dir_close} hedge_upnl={float(hedge_upnl):.6f}"
                                )
                            else:
                                self.log.debug(
                                    f"[TL][hedge] EMA flip detected but hedge_upnl<=0 -> keep hedge: {symbol} hedge_side={hedge_side} need_ema={need_ema} ema_dir_close={ema_dir_close} hedge_upnl={float(hedge_upnl):.6f}"
                                )
                        else:
                            self.log.debug(
                                f"[TL][hedge] EMA direction matches hedge -> keep hedge: {symbol} hedge_side={hedge_side} need_ema={need_ema} ema_dir_close={ema_dir_close}"
                            )
                    else:
                        self.log.debug(
                            f"[TL][hedge] EMA close direction not available -> skip ema_flip close: {symbol} hedge_side={hedge_side} need_ema={need_ema} ema_dir_close={ema_dir_close}"
                        )

                if close_reason:
                    # cooldown check (close)
                    if hedge_cooldown_sec and hedge_last_action_ts and (time.time() - float(hedge_last_action_ts)) < float(hedge_cooldown_sec):
                        skipped += 1
                        continue
                    close_side = "BUY" if hedge_side == "SHORT" else "SELL"
                    qty_step = float(self._qty_step_for_symbol(symbol) or 0.0) or 0.0
                    close_qty = float(hedge_amt)
                    if qty_step and qty_step > 0:
                        close_qty = _round_qty_to_step(close_qty, qty_step, mode="down")
                    if close_qty <= 0 or (qty_step and close_qty < qty_step):
                        skipped += 1
                        continue

                    def _stamp_hedge_close_attempt(reason: str, close_px: float, synthetic_closed: bool = False) -> None:
                        try:
                            if getattr(self, "_pl_has_raw_meta", False):
                                import json as _json
                                meta = {
                                    "hedge": {
                                        "base_qty": float(hedge_base_qty) if (hedge_base_qty is not None and float(hedge_base_qty) > 0) else float(hedge_amt),
                                        "last_close_reason": str(reason),
                                        "last_close_ts": float(time.time()),
                                        "last_close_px": float(close_px),
                                        "reopen_anchor_px": float(close_px),
                                        "last_action_ts": float(time.time()),
                                        "be_armed": False,
                                        "be_best_px": 0.0,
                                        "close_inflight": False if synthetic_closed else True,
                                        "close_inflight_ts": float(time.time()),
                                    }
                                }
                                uqm = """
                                UPDATE position_ledger
                                SET raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb,
                                    updated_at = now()
                                WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND status='OPEN';
                                """
                                self.store.execute(
                                    uqm,
                                    {
                                        "meta": _json.dumps(meta),
                                        "ex": int(self.exchange_id),
                                        "acc": int(self.account_id),
                                        "pos_uid": str(p.get("pos_uid")),
                                    },
                                )
                        except Exception:
                            pass

                    try:
                        # Stamp the close attempt before the exchange request so we do not spam duplicate
                        # close orders while the ledger is waiting for reconciliation.
                        _stamp_hedge_close_attempt(str(close_reason), float(mark), synthetic_closed=False)
                        close_params = dict(
                            symbol=symbol,
                            side=close_side,
                            type="MARKET",
                            quantity=float(close_qty),
                            positionSide=hedge_ps,
                        )
                        try:
                            # In Hedge Mode Binance typically forbids reduceOnly together with positionSide.
                            # We therefore close hedge legs without reduceOnly by default.
                            resp = self._binance.new_order(**close_params)
                        except Exception as e1:
                            msg1 = str(e1)
                            if '"code":-1106' in msg1 and 'reduceonly' in msg1.lower():
                                retry_params = dict(close_params)
                                retry_params.pop("reduceOnly", None)
                                resp = self._binance.new_order(**retry_params)
                            else:
                                raise
                        closed += 1
                        _stamp_hedge_close_attempt(str(close_reason), float(mark), synthetic_closed=False)
                        log.info("[TL][HEDGE] closed %s %s qty=%.8f reason=%s mark=%.6f ema_close=%s ema_open=%s",
                                 symbol, hedge_ps, float(close_qty), str(close_reason), float(mark), str(ema_dir_close), str(ema_dir_open))

                        # Optional: after hedge close, add to the still-open main leg by the same
                        # currently open main volume. This effectively doubles the main exposure
                        # from its live size at the moment of hedge close.
                        try:
                            add_enabled = bool(getattr(self.p, "hedge_close_add_enabled", False))
                            add_max_count = int(getattr(self.p, "hedge_close_add_max_count", 0) or 0)
                        except Exception:
                            add_enabled = False
                            add_max_count = 0

                        if add_enabled and add_max_count > 0 and float(main_amt) > 0.0:
                            try:
                                rm_pos = p.get("raw_meta")
                                if isinstance(rm_pos, str):
                                    rm_pos = json.loads(rm_pos) if rm_pos.strip() else {}
                                if not isinstance(rm_pos, dict):
                                    rm_pos = {}
                            except Exception:
                                rm_pos = {}

                            hedge_meta = rm_pos.get("hedge") if isinstance(rm_pos.get("hedge"), dict) else {}
                            add_meta = hedge_meta.get("main_add_on_hedge_close") if isinstance(hedge_meta.get("main_add_on_hedge_close"), dict) else {}
                            add_count_done = int(add_meta.get("count") or 0)

                            # In-memory guard against duplicate add placement while exchange/ledger
                            # snapshots are still catching up after the hedge close market order.
                            add_guard_ok = True
                            try:
                                if not hasattr(self, "_hedge_close_add_guard") or not isinstance(self._hedge_close_add_guard, dict):
                                    self._hedge_close_add_guard = {}
                                guard_key = f"{str(p.get('pos_uid') or '')}:{int(add_count_done + 1)}"
                                last_guard_ts = float(self._hedge_close_add_guard.get(guard_key) or 0.0)
                                if last_guard_ts > 0.0 and (time.time() - last_guard_ts) < 30.0:
                                    add_guard_ok = False
                                else:
                                    self._hedge_close_add_guard[guard_key] = time.time()
                            except Exception:
                                add_guard_ok = True

                            if add_guard_ok and add_count_done < add_max_count:
                                main_qty_to_add = float(main_amt)
                                if qty_step and qty_step > 0:
                                    main_qty_to_add = _round_qty_to_step(main_qty_to_add, qty_step, mode="down")

                                if main_qty_to_add > 0.0 and (not qty_step or main_qty_to_add >= qty_step):
                                    add_side = "BUY" if main_side == "LONG" else "SELL"
                                    try:
                                        add_resp = self._binance.new_order(
                                            symbol=symbol,
                                            side=add_side,
                                            type="MARKET",
                                            quantity=float(main_qty_to_add),
                                            positionSide=main_ps,
                                        )
                                        try:
                                            if getattr(self, "_pl_has_raw_meta", False):
                                                import json as _json
                                                add_meta_patch = {
                                                    "hedge": {
                                                        "main_add_on_hedge_close": {
                                                            "enabled": True,
                                                            "count": int(add_count_done) + 1,
                                                            "max_count": int(add_max_count),
                                                            "last_ts": float(time.time()),
                                                            "last_qty": float(main_qty_to_add),
                                                            "last_reason": str(close_reason),
                                                            "last_hedge_side": str(hedge_ps).upper(),
                                                            "last_main_side": str(main_ps).upper(),
                                                            "last_order_id": str(add_resp.get("orderId") or add_resp.get("order_id") or ""),
                                                        }
                                                    }
                                                }
                                                self.store.execute(
                                                    """
                                                    UPDATE position_ledger
                                                    SET raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb,
                                                        updated_at = now()
                                                    WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND status='OPEN';
                                                    """,
                                                    {
                                                        "meta": _json.dumps(add_meta_patch),
                                                        "ex": int(self.exchange_id),
                                                        "acc": int(self.account_id),
                                                        "pos_uid": str(p.get("pos_uid")),
                                                    },
                                                )
                                        except Exception:
                                            pass
                                        log.info(
                                            "[TL][HEDGE][MAIN_ADD] added to main after hedge close %s %s qty=%.8f count=%s/%s reason=%s",
                                            symbol,
                                            str(main_ps).upper(),
                                            float(main_qty_to_add),
                                            int(add_count_done) + 1,
                                            int(add_max_count),
                                            str(close_reason),
                                        )
                                    except Exception as add_e:
                                        log.exception(
                                            "[TL][HEDGE][MAIN_ADD] failed to add to main after hedge close for %s %s qty=%.8f: %s",
                                            symbol,
                                            str(main_ps).upper(),
                                            float(main_qty_to_add),
                                            add_e,
                                        )
                                else:
                                    log.info(
                                        "[TL][HEDGE][MAIN_ADD] skipped after hedge close for %s %s: computed qty too small qty=%.8f step=%.8f",
                                        symbol,
                                        str(main_ps).upper(),
                                        float(main_qty_to_add),
                                        float(qty_step or 0.0),
                                    )
                            elif add_enabled and add_count_done >= add_max_count:
                                log.info(
                                    "[TL][HEDGE][MAIN_ADD] limit reached for %s %s: count=%s max=%s",
                                    symbol,
                                    str(main_ps).upper(),
                                    int(add_count_done),
                                    int(add_max_count),
                                )
                    except Exception as e:
                        msg = str(e)
                        if 'ReduceOnly Order is rejected' in msg or '"code":-2022' in msg:
                            # The hedge is already closed (or a close is already fully matched) on the exchange.
                            # Treat it as an idempotent close and let the normal ledger reconciliation mark qty=0.
                            _stamp_hedge_close_attempt(f"{close_reason}_ALREADY_CLOSED", float(mark), synthetic_closed=True)
                            closed += 1
                            log.info("[TL][HEDGE] close already satisfied for %s %s qty=%.8f reason=%s mark=%.6f",
                                     symbol, hedge_ps, float(close_qty), str(close_reason), float(mark))
                        elif '"code":-1106' in msg and 'reduceonly' in msg.lower():
                            # Binance rejected an unnecessary reduceOnly flag; treat as config/API-shape issue for this request.
                            log.info("[TL][HEDGE] close retried without reduceOnly for %s %s qty=%.8f reason=%s",
                                     symbol, hedge_ps, float(close_qty), str(close_reason))
                        else:
                            log.exception("[TL][HEDGE] failed to close hedge for %s: %s", symbol, e)
                            skipped += 1

        return {"checked": checked, "opened": opened, "closed": closed, "skipped": skipped}

