from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional

from .params import *

log = logging.getLogger("traders.trade_liquidation")

class TradeLiquidationExitRulesMixin:
    def _fetch_exchange_exit_stats(
        self,
        symbol: str,
        pos_side: str,
        opened_at: Any,
        closed_at: Any,
        qty_expected: float,
    ) -> Dict[str, Any]:
        """Fetch best-effort exit_price / realized_pnl / fees / close_time_ms from Binance futures trade history.

        Strategy:
          - Query userTrades in a bounded window [opened_at-60s, closed_at+60s] when closed_at is known.
            This avoids the "last 1000 trades" truncation when the account is active.
          - Paginate forward if Binance returns exactly `limit` trades.
          - Aggregate closing-side fills (SELL for LONG, BUY for SHORT) with matching positionSide (or BOTH).
          - Return fees (USDT + other assets) even if we can't infer exit fills (so we can at least backfill fees/marker).
        """
        ps = str(pos_side or "").upper()
        close_order_side = "SELL" if ps == "LONG" else "BUY"

        # time window
        opened_ms = self._to_epoch_ms(opened_at)
        closed_ms = self._to_epoch_ms(closed_at)
        if opened_ms is None:
            opened_ms = int(time.time() * 1000) - 7 * 24 * 3600 * 1000  # fallback: last 7d
        start_ms = max(0, opened_ms - 60_000)
        end_ms = (closed_ms + 60_000) if closed_ms is not None else None

        limit = 1000
        trades_all: List[Dict[str, Any]] = []
        page_start = start_ms
        # paginate forward (Binance returns trades within [startTime,endTime] up to limit)
        for _ in range(20):  # hard safety cap
            batch = self._binance_rest.user_trades(
                symbol=symbol,
                start_time_ms=page_start,
                end_time_ms=end_ms,
                limit=limit,
            )
            if not batch:
                break
            trades_all.extend(batch)
            if len(batch) < limit:
                break
            # move start to just after last trade time to avoid duplicates
            try:
                last_t = max(int(x.get("time") or 0) for x in batch)
            except Exception:
                break
            if last_t <= 0:
                break
            page_start = last_t + 1
            # if we already passed end_ms, stop
            if end_ms is not None and page_start >= end_ms:
                break

        if not trades_all:
            return {"fees_usdt": 0.0, "fees_other": {}, "exit_qty": 0.0}

        # Aggregate
        qty_target = abs(float(qty_expected or 0.0))
        qty_target_tol = qty_target * 0.995 if qty_target > 0 else 0.0

        exit_qty = 0.0
        exit_notional = 0.0
        realized = 0.0
        fees_usdt = 0.0
        fees_other: Dict[str, float] = {}
        last_time_ms: Optional[int] = None

        try:
            trades_sorted = sorted(trades_all, key=lambda x: int(x.get("time") or 0))
        except Exception:
            trades_sorted = trades_all

        for t in trades_sorted:
            try:
                # fees (collect for ALL trades in window for that positionSide/BOTH)
                if "positionSide" in t and str(t.get("positionSide") or "").upper() not in ("", ps, "BOTH"):
                    continue
                ca = str(t.get("commissionAsset") or "").upper()
                cval = float(t.get("commission") or 0.0)
                if ca == "USDT":
                    fees_usdt += cval
                elif cval:
                    fees_other[ca] = fees_other.get(ca, 0.0) + cval

                # closing side fills for exit VWAP/pnl
                if str(t.get("side") or "").upper() != close_order_side:
                    continue
                q = float(t.get("qty") or 0.0)
                p = float(t.get("price") or 0.0)
                if q <= 0 or p <= 0:
                    continue
                exit_qty += q
                exit_notional += q * p
                realized += float(t.get("realizedPnl") or 0.0)
                last_time_ms = int(t.get("time") or 0) or last_time_ms

                if qty_target_tol > 0 and exit_qty >= qty_target_tol:
                    # we've likely covered the closing quantity; keep collecting fees already done
                    break
            except Exception:
                continue

        out: Dict[str, Any] = {
            "fees_usdt": float(fees_usdt),
            "fees_other": fees_other,
            "fees_done": True,
            "exit_qty": float(exit_qty),
        }
        if exit_qty > 0:
            out.update(
                {
                    "exit_price": (exit_notional / exit_qty) if exit_qty else None,
                    "realized_pnl": float(realized),
                    "close_time_ms": int(last_time_ms) if last_time_ms else None,
                }
            )
        return out

    def _close_position_exchange(
        self,
        pos: Dict[str, Any],
        exit_price: float,
        realized_pnl: float,
        close_time_ms: Optional[int],
        reason: str,
        timeframe: str,
    ) -> None:
        """Close a ledger position using exchange-derived exit price and realized PnL."""
        meta = {
            "exchange_exit": {
                "reason": reason,
                "timeframe": timeframe,
                "exit_price": float(exit_price),
                "realized_pnl": float(realized_pnl),
                "close_time_ms": int(close_time_ms) if close_time_ms else None,
            }
        }

        closed_at_sql = "now()"
        closed_at_param = None
        if close_time_ms:
            closed_at_sql = "to_timestamp(%(closed_ts)s)"
            closed_at_param = float(close_time_ms) / 1000.0

        if self._pl_has_raw_meta:
            q = f"""
            UPDATE position_ledger
            SET
              status='CLOSED',
              closed_at={closed_at_sql},
              qty_closed=qty_current,
              qty_current=0,
              exit_price=%(exit)s,
              realized_pnl=%(pnl)s,
              updated_at=now(),
              raw_meta = COALESCE(raw_meta,'{{}}'::jsonb) || %(meta)s::jsonb
            WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s AND pos_uid=%(pos_uid)s;
            """
            params = {
                "exit": float(exit_price),
                "pnl": float(realized_pnl),
                "meta": json.dumps(meta),
                "ex": int(self.exchange_id),
                "acc": int(self.account_id),
                "sym": int(pos["symbol_id"]),
                "pos_uid": str(pos["pos_uid"]),
            }
            if closed_at_param is not None:
                params["closed_ts"] = closed_at_param
        else:
            q = f"""
            UPDATE position_ledger
            SET
              status='CLOSED',
              closed_at={closed_at_sql},
              qty_closed=qty_current,
              qty_current=0,
              exit_price=%(exit)s,
              realized_pnl=%(pnl)s,
              updated_at=now()
            WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s AND pos_uid=%(pos_uid)s;
            """
            params = {
                "exit": float(exit_price),
                "pnl": float(realized_pnl),
                "ex": int(self.exchange_id),
                "acc": int(self.account_id),
                "sym": int(pos["symbol_id"]),
                "pos_uid": str(pos["pos_uid"]),
            }
            if closed_at_param is not None:
                params["closed_ts"] = closed_at_param

        self.store.execute(q, params)

        # log in LIVE style (avoid confusing [PAPER])
        self._ilog(
            "[trade_liquidation][LIVE] CLOSE %s %s qty=%.8f entry=%.8f exit=%.8f pnl=%.8f reason=%s",
            str(pos.get("symbol") or "").upper(),
            str(pos.get("side") or "").upper(),
            float(pos.get("qty_current") or 0.0),
            float(pos.get("avg_price") or pos.get("entry_price") or 0.0),
            float(exit_price),
            float(realized_pnl),
            reason,
        )

    def _backfill_closed_exit_pnl(self, force: bool = False, log_empty: bool = False) -> Dict[str, int]:
        """Background backfill for CLOSED ledger rows missing exit_price/realized_pnl.

        Goal: if a position is already CLOSED in DB but exit_price/realized_pnl are NULL (e.g. old runs),
        fetch Binance userTrades to infer close VWAP and realized PnL and write them into position_ledger.

        Runs in LIVE mode only. Rate-limited by backfill_interval_sec (default 600s).
        """
        # rate limit
        interval = float(getattr(self.p, "backfill_interval_sec", 600) or 600)
        now_ts = time.time()
        if not force:
            last_ts = float(getattr(self, "_last_backfill_ts", 0.0) or 0.0)
            if interval > 0 and (now_ts - last_ts) < interval:
                return {"checked": 0, "updated": 0, "skipped": 1}

        self._last_backfill_ts = now_ts

        lookback_days = int(getattr(self.p, "backfill_lookback_days", 14) or 14)
        batch = int(getattr(self.p, "backfill_batch_limit", 50) or 50)

        overwrite = bool(getattr(self.p, 'backfill_overwrite', False))
        if overwrite:
            q = """
                SELECT
                  pl.pos_uid, pl.symbol_id, pl.side, pl.qty_closed,
                  pl.opened_at, pl.closed_at, pl.exit_price, pl.realized_pnl, pl.fees, pl.raw_meta
                FROM public.position_ledger pl
                WHERE pl.exchange_id = %(exchange_id)s
                  AND pl.account_id = %(account_id)s
                  AND pl.strategy_id = %(strategy_id)s
                  AND pl.status = 'CLOSED'
                  AND COALESCE(pl.closed_at, pl.updated_at) >= (now() - (%(lookback_days)s || ' days')::interval)
                  AND (
                        %(overwrite)s
                        OR pl.exit_price IS NULL
                        OR pl.realized_pnl IS NULL
                        OR pl.fees IS NULL
                        OR pl.fees = 0
                        OR NOT (pl.raw_meta ? 'exchange_exit_backfill')
                      )
                ORDER BY pl.updated_at DESC
                LIMIT %(batch)s
            """
        else:
            q = """
                SELECT
                  pl.pos_uid, pl.symbol_id, pl.side, pl.qty_closed,
                  pl.opened_at, pl.closed_at, pl.exit_price, pl.realized_pnl, pl.fees, pl.raw_meta
                FROM public.position_ledger pl
                WHERE pl.exchange_id = %(exchange_id)s
                  AND pl.account_id = %(account_id)s
                  AND pl.strategy_id = %(strategy_id)s
                  AND pl.status = 'CLOSED'
                  AND COALESCE(pl.closed_at, pl.updated_at) >= (now() - (%(lookback_days)s || ' days')::interval)
                  AND (
                        pl.exit_price IS NULL
                     OR pl.realized_pnl IS NULL
                     OR (%(fill_fees)s AND (pl.fees IS NULL OR pl.fees = 0))
                     OR NOT (pl.raw_meta ? 'exchange_exit_backfill')
                  )
                ORDER BY pl.updated_at DESC
                LIMIT %(batch)s
            """
        rows = self.store.query_dict(q, {
            "exchange_id": int(self.exchange_id),
            "account_id": int(self.account_id),
            "strategy_id": str(self.STRATEGY_ID),
            "lookback_days": int(lookback_days),
            "batch": int(batch),
            "fill_fees": bool(getattr(self.p, "backfill_fill_fees", True)),
        }) or []

        updated = 0
        checked = 0

        for r in rows:
            checked += 1
            sym = (r.get("symbol") or "").upper()
            side = (r.get("side") or "").upper()  # LONG/SHORT
            qty_expected = _safe_float(r.get("qty_closed"), default=_safe_float(r.get("qty_opened"), default=0.0))
            if not sym or qty_expected <= 0:
                continue

            opened_ms = self._to_epoch_ms(r.get("opened_at"))
            closed_ms = self._to_epoch_ms(r.get("closed_at"))
            start_ms = (opened_ms - 60_000) if opened_ms else None
            end_ms = (closed_ms + 60_000) if closed_ms else None

            if getattr(self.p, "debug", False):
                log.info("[TL][debug] backfill candidate symbol=%s side=%s qty_expected=%.8f opened_ms=%s closed_ms=%s", sym, side, qty_expected, opened_ms, closed_ms)

            try:
                trades = self._binance_rest.user_trades(symbol=sym, start_time_ms=start_ms, end_time_ms=end_ms, limit=1000)
                if getattr(self.p, "debug", False):
                    log.info("[TL][debug] backfill userTrades fetched=%s symbol=%s window_ms=[%s,%s]", len(trades) if trades else 0, sym, start_ms, end_ms)
                # Retry with a wider window if nothing returned (closed_at may be set long after fills)
                if (not trades) and closed_ms:
                    start2 = max(int(closed_ms) - 3 * 24 * 60 * 60 * 1000, 0)
                    end2 = int(time.time() * 1000)
                    trades = self._binance_rest.user_trades(symbol=sym, start_time_ms=start2, end_time_ms=end2, limit=1000)
                    if getattr(self.p, "debug", False):
                        log.info("[TL][debug] backfill userTrades retry fetched=%s symbol=%s window_ms=[%s,%s]", len(trades) if trades else 0, sym, start2, end2)
                ps = str(side or "").upper()
                close_order_side = "SELL" if ps == "LONG" else "BUY"


                trades_sorted = sorted(trades or [], key=lambda t: int(t.get("time") or t.get("timestamp") or t.get("tradeTime") or 0))
                qty_target = abs(float(qty_expected or 0.0))
                qty_target_tol = qty_target * 0.995 if qty_target else 0.0

                # Collect only *closing* fills for this symbol/positionSide within the window.
                # In hedge-mode Binance may return BOTH; we allow BOTH or exact match.
                close_fills = []
                for t in trades_sorted:
                    try:
                        sym_t = str(t.get("symbol") or t.get("s") or "").upper()
                        if sym_t and sym_t != str(sym).upper():
                            continue

                        if "positionSide" in t and str(t.get("positionSide") or "").upper() not in ("", ps, "BOTH"):
                            continue

                        t_side = str(t.get("side") or t.get("order_side") or "").upper()
                        if not t_side or t_side != close_order_side:
                            continue

                        # Extract numeric fields robustly (wrappers sometimes rename keys)
                        def _f(keys, default=0.0):
                            for k in keys:
                                if k in t and t.get(k) not in (None, ""):
                                    try:
                                        return float(t.get(k))
                                    except Exception:
                                        pass
                            return default

                        def _i(keys, default=0):
                            for k in keys:
                                if k in t and t.get(k) not in (None, ""):
                                    try:
                                        return int(t.get(k))
                                    except Exception:
                                        pass
                            return default

                        qty = _f(["qty", "quantity", "executedQty", "executed_qty", "origQty", "orig_qty"], 0.0)
                        price = _f(["price", "avgPrice", "avg_price"], 0.0)

                        # Guard against buggy wrappers where 'qty' accidentally contains trade id / order id
                        if qty_target > 0:
                            too_big = max(qty_target * 10.0, 1000.0)
                            if qty > too_big:
                                # try alternatives
                                found_alt = False
                                for alt_k in ["quantity", "executedQty", "executed_qty", "origQty", "orig_qty"]:
                                    if alt_k in t and t.get(alt_k) not in (None, ""):
                                        try:
                                            alt = float(t.get(alt_k))
                                            if 0 < alt <= too_big:
                                                qty = alt
                                                found_alt = True
                                                break
                                        except Exception:
                                            pass
                                if (not found_alt) and qty > too_big:
                                    # still absurd -> skip this fill
                                    continue

                        if qty <= 0 or price <= 0:
                            continue

                        ca = str(t.get("commissionAsset") or t.get("commission_asset") or "").upper()
                        cv = _f(["commission", "commissionAmount", "commission_amount"], 0.0)

                        order_id = t.get("orderId") or t.get("order_id") or t.get("orderID") or None
                        trade_time = _i(["time", "timestamp", "tradeTime", "trade_time"], 0)

                        close_fills.append({
                            "order_id": str(order_id) if order_id is not None else "",
                            "qty": float(qty),
                            "price": float(price),
                            "realized": _f(["realizedPnl", "realized_pnl"], 0.0),
                            "commission_asset": ca,
                            "commission": float(cv),
                            "time_ms": int(trade_time) if trade_time else 0,
                        })
                    except Exception:
                        continue

                # Aggregate closing fills. If the position was closed by multiple orders, we sum all of them.
                # Additionally, we try to avoid mixing unrelated closes by preferring the most recent orders first
                # until we reach expected qty (if expected qty is known).
                order_agg = {}
                for f in close_fills:
                    key = f["order_id"] or "_no_order_id"
                    a = order_agg.get(key)
                    if not a:
                        a = {
                            "exit_qty": 0.0,
                            "exit_notional": 0.0,
                            "realized": 0.0,
                            "fees_usdt": 0.0,
                            "fees_other": {},
                            "last_time_ms": 0,
                        }
                        order_agg[key] = a
                    a["exit_qty"] += f["qty"]
                    a["exit_notional"] += f["qty"] * f["price"]
                    a["realized"] += f["realized"]
                    a["last_time_ms"] = max(a["last_time_ms"], int(f["time_ms"] or 0))

                    if f["commission"]:
                        if f["commission_asset"] == "USDT":
                            a["fees_usdt"] += f["commission"]
                        elif f["commission_asset"]:
                            a["fees_other"][f["commission_asset"]] = float(a["fees_other"].get(f["commission_asset"], 0.0)) + f["commission"]

                # Choose which order groups to use:
                # - if we know expected qty: take the newest groups until qty >= expected (tolerance)
                # - else: take all groups
                groups = list(order_agg.values())
                groups.sort(key=lambda g: int(g.get("last_time_ms") or 0), reverse=True)

                use_groups = []
                total_qty = 0.0
                if qty_target > 0:
                    for g in groups:
                        use_groups.append(g)
                        total_qty += float(g.get("exit_qty") or 0.0)
                        if total_qty >= qty_target_tol:
                            break
                else:
                    use_groups = groups

                # reset per-position accumulators (IMPORTANT: do not carry between symbols)
                exit_qty = 0.0
                exit_notional = 0.0
                realized = 0.0
                fees_usdt = 0.0
                fees_other = {}
                last_time_ms = 0

                for g in use_groups:
                    exit_qty += float(g.get("exit_qty") or 0.0)
                    exit_notional += float(g.get("exit_notional") or 0.0)
                    realized += float(g.get("realized") or 0.0)
                    fees_usdt += float(g.get("fees_usdt") or 0.0)
                    for k, v in (g.get("fees_other") or {}).items():
                        fees_other[k] = float(fees_other.get(k, 0.0)) + float(v or 0.0)
                    last_time_ms = max(last_time_ms, int(g.get("last_time_ms") or 0))

                have_exit = exit_qty > 0
                exit_price = (exit_notional / exit_qty) if exit_qty else None

                # If we couldn't infer exit fills (e.g. API window/limit issues), we can still backfill fees + marker
                if (not have_exit) or (exit_price is None):
                    # Even if fees are not available, write a marker so we don't re-check forever.
                    meta = {
                        "exchange_exit_backfill": {
                            "note": "no_exit_fills_in_window" if (fees_usdt > 0.0 or fees_other) else "no_exit_fills_or_fees",
                            "fees_usdt": float(fees_usdt),
                            "fees_other": fees_other,
                        }
                    }
                    # fees-only update (do not touch exit_price/realized_pnl)
                    if self._pl_has_raw_meta:
                        uq_fees = """
                        UPDATE position_ledger
                        SET
                          fees = COALESCE(NULLIF(fees, 0), %(fees)s),
                          raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb,
                          updated_at = now()
                        WHERE exchange_id = %(ex)s
                          AND account_id  = %(acc)s
                          AND pos_uid     = %(pos_uid)s
                          AND opened_at   = %(opened_at)s
                        """
                    else:
                        uq_fees = """
                        UPDATE position_ledger
                        SET
                          fees = COALESCE(NULLIF(fees, 0), %(fees)s),
                          updated_at = now()
                        WHERE exchange_id = %(ex)s
                          AND account_id  = %(acc)s
                          AND pos_uid     = %(pos_uid)s
                          AND opened_at   = %(opened_at)s
                        """
                    self.store.execute(
                        uq_fees,
                        dict(
                            ex=int(self.exchange_id),
                            acc=int(self.p.account_id),
                            pos_uid=r.get("pos_uid"),
                            opened_at=r.get("opened_at"),
                            fees=float(fees_usdt),
                            meta=json.dumps(meta) if isinstance(meta, dict) else "{}",
                        ),
                    )
                    updated += 1
                    continue

                # have_exit == True and exit_price is known -> full backfill
                meta = {
                    "exchange_exit_backfill": {
                        "exit_price": float(exit_price),
                        "realized_pnl": float(realized),
                        "close_time_ms": int(last_time_ms) if last_time_ms else None,
                        "exit_qty": float(exit_qty),
                        "fees_usdt": float(fees_usdt),
                        "fees_other": fees_other,
                    }
                }

                closed_at_param = float(last_time_ms) / 1000.0 if last_time_ms else None
                closed_at_sql = ("CASE WHEN %(closed_ts)s IS NOT NULL AND (%(overwrite)s = true OR closed_at IS NULL OR (opened_at IS NOT NULL AND closed_at < opened_at)) "
                               "THEN to_timestamp(%(closed_ts)s) ELSE closed_at END")

                if self._pl_has_raw_meta:
                    uq = f"""
                    UPDATE position_ledger
                    SET
                      exit_price = CASE WHEN %(overwrite)s = true THEN %(exit)s ELSE COALESCE(NULLIF(exit_price, 0), %(exit)s) END,
                      realized_pnl = CASE WHEN %(overwrite)s = true THEN %(pnl)s ELSE COALESCE(NULLIF(realized_pnl, 0), %(pnl)s) END,
                      fees = CASE WHEN %(overwrite)s = true THEN %(fees)s ELSE COALESCE(NULLIF(fees, 0), %(fees)s) END,
                      closed_at = {closed_at_sql},
                      updated_at = now(),
                      raw_meta = COALESCE(raw_meta,'{{}}'::jsonb) || %(meta)s::jsonb
                    WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s AND pos_uid=%(pos_uid)s
                      AND status='CLOSED';
                    """
                    params = {
                        "exit": float(exit_price),
                        "pnl": float(realized),
                        "fees": float(fees_usdt),
                        "overwrite": bool(overwrite),
                        "meta": json.dumps(meta),
                        "ex": int(self.exchange_id),
                        "acc": int(self.account_id),
                        "sym": int(r["symbol_id"]),
                        "pos_uid": str(r["pos_uid"]),
                    }
                    params["closed_ts"] = closed_at_param
                else:
                    uq = f"""
                    UPDATE position_ledger
                    SET
                      exit_price = COALESCE(exit_price, %(exit)s),
                      realized_pnl = CASE WHEN %(overwrite)s = true THEN %(pnl)s ELSE COALESCE(NULLIF(realized_pnl, 0), %(pnl)s) END,
                      fees = CASE WHEN %(overwrite)s = true THEN %(fees)s ELSE COALESCE(NULLIF(fees, 0), %(fees)s) END,
                      closed_at = {closed_at_sql},
                      updated_at = now()
                    WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s AND pos_uid=%(pos_uid)s
                      AND status='CLOSED';
                    """
                    params = {
                        "exit": float(exit_price),
                        "pnl": float(realized),
                        "fees": float(fees_usdt),
                        "overwrite": bool(overwrite),
                        "ex": int(self.exchange_id),
                        "acc": int(self.account_id),
                        "sym": int(r["symbol_id"]),
                        "pos_uid": str(r["pos_uid"]),
                    }
                    if closed_at_param is not None:
                        params["closed_ts"] = closed_at_param

                self.store.execute(uq, params)
                updated += 1
            except Exception:
                log.info("[TL] backfill failed sym=%s pos_uid=%s (will mark as attempted)", sym, r.get("pos_uid"), exc_info=True)
                try:
                    if self._pl_has_raw_meta:
                        meta = {"exchange_exit_backfill": {"note": "exception", "error": "userTrades_or_update_failed"}}
                        uqm = """
                        UPDATE position_ledger
                        SET raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb
                        WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND opened_at=%(opened_at)s;
                        """
                        self.store.execute(
                            uqm,
                            {
                                "ex": int(r["exchange_id"]),
                                "acc": int(r["account_id"]),
                                "pos_uid": str(r["pos_uid"]),
                                "opened_at": r["opened_at"],
                                "meta": json.dumps(meta),
                            },
                        )
                        updated += 1
                except Exception:
                    pass

        if updated or log_empty:
            log.info("[TL] backfill CLOSED exit/pnl updated=%s checked=%s (lookback_days=%s)", updated, checked, lookback_days)

        return {"checked": checked, "updated": updated, "skipped": 0}

    def _close_position(self, pos: Dict[str, Any], exit_price: float, reason: str, timeframe: str) -> None:
        side = str(pos.get("side") or "").upper()  # LONG/SHORT
        entry = _safe_float(pos.get("avg_price"), default=0.0)
        qty = _safe_float(pos.get("qty_current"), default=0.0)
        if qty <= 0 or entry <= 0:
            return

        pnl = (exit_price - entry) * qty if side == "LONG" else (entry - exit_price) * qty

        meta = {"paper_exit": {"reason": reason, "timeframe": timeframe, "exit_order_type": self.p.exit_order_type}}

        if self._pl_has_raw_meta:
            q = """
            UPDATE position_ledger
            SET
              status='CLOSED',
              closed_at=now(),
              qty_closed=qty_current,
              qty_current=0,
              exit_price=%(exit)s,
              realized_pnl=%(pnl)s,
              updated_at=now(),
              raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb
            WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s AND pos_uid=%(pos_uid)s;
            """
            params = {
                "exit": float(exit_price),
                "pnl": float(pnl),
                "meta": json.dumps(meta),
                "ex": int(self.exchange_id),
                "acc": int(self.account_id),
                "sym": int(pos["symbol_id"]),
                "pos_uid": str(pos["pos_uid"]),
            }
        else:
            q = """
            UPDATE position_ledger
            SET
              status='CLOSED',
              closed_at=now(),
              qty_closed=qty_current,
              qty_current=0,
              exit_price=%(exit)s,
              realized_pnl=%(pnl)s,
              updated_at=now()
            WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s AND pos_uid=%(pos_uid)s;
            """
            params = {
                "exit": float(exit_price),
                "pnl": float(pnl),
                "ex": int(self.exchange_id),
                "acc": int(self.account_id),
                "sym": int(pos["symbol_id"]),
                "pos_uid": str(pos["pos_uid"]),
            }

        self.store.execute(q, params)
        self._delete_risk(int(pos["symbol_id"]))

        log.info(
            "[trade_liquidation][PAPER] CLOSE %s %s qty=%.8f entry=%.6f exit=%.6f pnl=%.4f reason=%s",
            pos.get("symbol"),
            side,
            qty,
            entry,
            exit_price,
            pnl,
            reason,
        )

    def _process_open_positions(self) -> Dict[str, int]:
        # Paper: uses candle prices + paper_position_risk
        if self._is_live:
            # In LIVE mode SL/TP is usually handled by exchange orders and cleanup.
            # But if hedge_enabled is True, we actively manage hedge open/close here.
            if bool(getattr(self.p, "hedge_enabled", False)):
                hs = self._live_hedge_manage()

                # After hedge management the state can change within the same cycle:
                #   - an old hedge leg can be closed,
                #   - a stale live_hedge mirror row can be reconciled/removed,
                #   - the surviving leg can effectively become the new main leg.
                # In those situations we must immediately re-check the invariant for
                # every still-open main position: there must be either the next ADDn
                # order (when averaging is still available) or the after-last-add hedge.
                # Without this second pass a symbol can stay one or more cycles with
                # only TP/TRL and without ADD/HEDGE protection (the user's ETH case).
                post_invariant = 0
                if bool(getattr(self.p, "averaging_enabled", False)) and self._cfg_max_adds() > 0:
                    try:
                        open_positions = self._get_open_positions()
                        for p in open_positions or []:
                            try:
                                if self._live_ensure_next_add_order(pos=p):
                                    post_invariant += 1
                            except Exception:
                                log.debug(
                                    "[trade_liquidation][LIVE][POST_HEDGE][AVG] ensure_next_add failed pos_uid=%s sym=%s",
                                    str((p or {}).get("pos_uid") or ""),
                                    str((p or {}).get("symbol") or (p or {}).get("symbol_id") or ""),
                                    exc_info=True,
                                )
                    except Exception:
                        log.debug("[trade_liquidation][LIVE][POST_HEDGE] invariant refresh failed", exc_info=True)

                out = {"checked": int(hs.get("checked", 0)), "closed": int(hs.get("closed", 0)), "hedge": hs}
                if post_invariant:
                    out["post_invariant"] = int(post_invariant)
                return out
            return {"checked": 0, "closed": 0}

        positions = self._get_open_positions()
        closed = 0
        checked = 0

        if positions:
            names = [str(p.get("symbol") or p.get("symbol_id") or "?") for p in positions][: max(1, int(self.p.debug_top))]
            self._dlog("positions: %d -> %s", len(positions), ", ".join(names))
        else:
            self._dlog("positions: 0")

        for pos in positions:
            checked += 1
            symbol_id = int(pos["symbol_id"])
            risk = self._load_risk(symbol_id)
            if not risk:
                continue

            tf = str(risk.get("timeframe") or "15m")
            price = self._get_last_price(symbol_id, tf)
            if not price or price <= 0:
                continue

            side = str(pos.get("side") or "").upper()
            entry = _safe_float(pos.get("avg_price"), default=0.0)

            sl = _safe_float(risk.get("sl_price"), default=0.0)
            tp = _safe_float(risk.get("tp_price"), default=0.0)

            if self.p.recalc_sl_tp and entry > 0:
                if side == "LONG":
                    sl = entry * (1.0 - _pct_to_mult(self.p.stop_loss_pct))
                    tp = entry * (1.0 + _pct_to_mult(self.p.take_profit_pct))
                else:
                    sl = entry * (1.0 + _pct_to_mult(self.p.stop_loss_pct))
                    tp = entry * (1.0 - _pct_to_mult(self.p.take_profit_pct))
                self._update_risk(symbol_id, sl_price=float(sl), tp_price=float(tp))

            trail_enabled = bool(risk.get("trailing_enabled"))
            trail_armed = bool(risk.get("trail_armed"))
            best = _safe_float(risk.get("best_price"), default=0.0)

            if trail_enabled:
                if best <= 0:
                    best = price
                best = max(best, price) if side == "LONG" else min(best, price)

                act_pct = _safe_float(risk.get("trail_activation_pct"), default=self.p.trailing_activation_pct)
                tr_pct = _safe_float(risk.get("trail_pct"), default=self.p.trailing_trail_pct)

                if not trail_armed and entry > 0:
                    if side == "LONG" and price >= entry * (1.0 + _pct_to_mult(act_pct)):
                        trail_armed = True
                    if side == "SHORT" and price <= entry * (1.0 - _pct_to_mult(act_pct)):
                        trail_armed = True

                self._update_risk(symbol_id, best_price=float(best), trail_armed=bool(trail_armed))

                if trail_armed and best > 0:
                    if side == "LONG":
                        trail_stop = best * (1.0 - _pct_to_mult(tr_pct))
                        if price <= trail_stop:
                            self._close_position(pos, exit_price=price, reason="TRAIL", timeframe=tf)
                            closed += 1
                            continue
                    else:
                        trail_stop = best * (1.0 + _pct_to_mult(tr_pct))
                        if price >= trail_stop:
                            self._close_position(pos, exit_price=price, reason="TRAIL", timeframe=tf)
                            closed += 1
                            continue

            if sl > 0:
                if side == "LONG" and price <= sl:
                    self._close_position(pos, exit_price=price, reason="SL", timeframe=tf)
                    closed += 1
                    continue
                if side == "SHORT" and price >= sl:
                    self._close_position(pos, exit_price=price, reason="SL", timeframe=tf)
                    closed += 1
                    continue

            if tp > 0:
                if side == "LONG" and price >= tp:
                    self._close_position(pos, exit_price=price, reason="TP", timeframe=tf)
                    closed += 1
                    continue
                if side == "SHORT" and price <= tp:
                    self._close_position(pos, exit_price=price, reason="TP", timeframe=tf)
                    closed += 1
                    continue

        return {"checked": checked, "closed": closed}

