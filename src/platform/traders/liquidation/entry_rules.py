from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Dict, Optional

from .params import *

log = logging.getLogger("traders.trade_liquidation")

class TradeLiquidationEntryRulesMixin:
    def _try_claim_signal(self, signal_id: int) -> bool:
        """Atomically reserve a NEW signal for processing.

        This protects against duplicate entries when multiple trader loops/processes
        read the same NEW signal concurrently, or when a fast next cycle starts
        before the previous one has fully persisted its state.

        We use the existing DB enum value LOCKED as the temporary in-flight status.
        After a successful open the signal is moved to TAKEN; on failure it is
        returned back to NEW so it can be retried.
        """
        try:
            rows = list(
                self.store.query_dict(
                    """
                    UPDATE signals
                       SET status='LOCKED', updated_at=now()
                     WHERE id=%(id)s
                       AND status=%(st)s
                    RETURNING id
                    """,
                    {
                        "id": int(signal_id),
                        "st": str(self.p.signal_status_new),
                    },
                )
            )
            return bool(rows)
        except Exception:
            log.exception("[trade_liquidation] failed to claim signal_id=%s", int(signal_id))
            return False

    def _release_signal_claim(self, signal_id: int) -> None:
        """Release LOCKED claim so the signal can be retried later."""
        try:
            self.store.execute(
                "UPDATE signals SET status=%s, updated_at=now() WHERE id=%s AND status='LOCKED'",
                (str(self.p.signal_status_new), int(signal_id)),
            )
        except Exception:
            log.exception("[trade_liquidation] failed to release signal claim signal_id=%s", int(signal_id))

    def _mark_signal_taken(self, signal_id: int) -> None:
        self.store.execute("UPDATE signals SET status='TAKEN', updated_at=now() WHERE id=%s", (int(signal_id),))

    def _process_new_signals(self) -> Dict[str, int]:
        open_positions = self._get_open_positions()
        open_by_symbol = {int(p["symbol_id"]) for p in open_positions}
        # Safety: also respect OPEN ledger rows even if exchange snapshot is temporarily stale.
        # This prevents duplicate re-entry on the same symbol while reconcile/orphan cleanup
        # has not yet finished.
        try:
            ledger_rows = list(self.store.query_dict(
                """
                SELECT DISTINCT symbol_id
                FROM position_ledger
                WHERE exchange_id=%(ex)s
                  AND account_id=%(acc)s
                  AND strategy_id=%(sid)s
                  AND status='OPEN'
                  AND COALESCE(source, CASE WHEN %(mode)s='live' THEN 'live' ELSE 'paper' END)
                      = CASE WHEN %(mode)s='live' THEN 'live' ELSE 'paper' END
                """,
                {
                    "ex": int(self.exchange_id),
                    "acc": int(self.account_id),
                    "sid": str(self.STRATEGY_ID),
                    "mode": str(getattr(self, 'mode', 'live')).lower(),
                },
            ))
            open_by_symbol |= {int(r["symbol_id"]) for r in ledger_rows if r.get("symbol_id") is not None}
        except Exception:
            pass
        open_symbol_count = len(open_by_symbol)
        open_leg_count = len(open_positions)

        capacity = max(0, int(self.p.max_open_positions) - open_symbol_count)
        if capacity <= 0:
            self._stateful_info(
                "entry_block",
                ("max_open_symbols", int(capacity), int(open_symbol_count), int(open_leg_count), int(self.p.max_open_positions)),
                "[trade_liquidation][ENTRY_BLOCK] reason=max_open_symbols capacity=%d open_symbols=%d open_legs=%d max_open_positions=%d",
                int(capacity), int(open_symbol_count), int(open_leg_count), int(self.p.max_open_positions),
            )
            return {"capacity": 0, "considered": 0, "opened": 0, "skipped": 0, "open_symbols": open_symbol_count, "open_legs": open_leg_count, "blocked_reason": "max_open_symbols"}

        # Step 3: portfolio cap (LIVE): if usedMargin/wallet is too high, do NOT open new positions.
        if self._is_live:
            blocked, m = self._portfolio_cap_blocked()
            if blocked:
                log.warning(
                    "[trade_liquidation][CAP] blocked: used_margin=%.2f wallet=%.2f used/wallet=%.3f >= cap_ratio=%.3f",
                    float(m.get("used_margin", 0.0)),
                    float(m.get("wallet", 0.0)),
                    float(m.get("used_over_wallet", 0.0)),
                    float(m.get("cap_ratio", 0.0)),
                )
                self._stateful_info("entry_block", ("portfolio_cap", int(capacity), int(open_symbol_count), int(open_leg_count)), "[trade_liquidation][ENTRY_BLOCK] reason=portfolio_cap capacity=%d open_symbols=%d open_legs=%d", int(capacity), int(open_symbol_count), int(open_leg_count))
                return {"capacity": capacity, "considered": 0, "opened": 0, "skipped": 0, "blocked_by_cap": 1, "open_symbols": open_symbol_count, "open_legs": open_leg_count, "blocked_reason": "portfolio_cap"}

        # Prefer snapshot wallet balance if present (prefetched in run_once)
        wallet = _safe_float(self._rest_snapshot_get("wallet_balance_usdt"), 0.0)
        if wallet <= 0:
            wallet = self._wallet_balance_usdt()
        if wallet <= 0:
            log.warning("[trade_liquidation] wallet_balance(USDT)=0 -> skip opening")
            self._stateful_info("entry_block", ("wallet_zero", int(capacity), int(open_symbol_count), int(open_leg_count)), "[trade_liquidation][ENTRY_BLOCK] reason=wallet_zero capacity=%d open_symbols=%d open_legs=%d", int(capacity), int(open_symbol_count), int(open_leg_count))
            return {"capacity": capacity, "considered": 0, "opened": 0, "skipped": 0, "open_symbols": open_symbol_count, "open_legs": open_leg_count, "blocked_reason": "wallet_zero"}

        signals = self._fetch_new_signals(limit=50)

        if self.p.debug:
            self._dlog(
                "new signals: fetched=%d capacity=%d open=%d wallet=%.2fUSDT allowed_tfs=%s",
                len(signals),
                capacity,
                open_symbol_count,
                wallet,
                list(self.p.allowed_timeframes) if self.p.allowed_timeframes else "ALL",
            )

        opened = 0
        considered = 0
        skipped = 0
        skip_reasons: Dict[str, int] = {}
        seen_symbols: set[int] = set()

        for sig in signals:
            if opened >= capacity:
                break
            considered += 1

            symbol_id = int(sig["symbol_id"])
            tf = str(sig.get("timeframe") or "")
            if tf and self.p.allowed_timeframes and tf not in self.p.allowed_timeframes:
                skipped += 1
                skip_reasons["bad_timeframe"] = skip_reasons.get("bad_timeframe", 0) + 1
                continue

            if symbol_id in seen_symbols:
                skipped += 1
                skip_reasons["duplicate_symbol_in_batch"] = skip_reasons.get("duplicate_symbol_in_batch", 0) + 1
                continue
            seen_symbols.add(symbol_id)

            if symbol_id in open_by_symbol:
                skipped += 1
                skip_reasons["already_open_symbol"] = skip_reasons.get("already_open_symbol", 0) + 1
                continue

            if self._is_symbol_in_cooldown(symbol_id):
                skipped += 1
                skip_reasons["symbol_cooldown"] = skip_reasons.get("symbol_cooldown", 0) + 1
                continue

            signal_id = int(sig["signal_id"])
            if not self._try_claim_signal(signal_id):
                skipped += 1
                skip_reasons["claim_failed"] = skip_reasons.get("claim_failed", 0) + 1
                continue

            ok = False
            try:
                ok = self._open_from_signal(sig, wallet_balance_usdt=wallet)
            finally:
                if not ok:
                    self._release_signal_claim(signal_id)

            if ok:
                opened += 1
            else:
                skipped += 1
                skip_reasons["open_failed"] = skip_reasons.get("open_failed", 0) + 1

        fetched = int(len(signals))
        state = (fetched, int(considered), int(opened), int(skipped), int(capacity), int(open_symbol_count), int(open_leg_count), tuple(sorted((skip_reasons or {}).items())))
        self._stateful_info("entry_scan", state, "[trade_liquidation][ENTRY_SCAN] fetched=%d considered=%d opened=%d skipped=%d capacity=%d open_symbols=%d open_legs=%d skip_reasons=%s", fetched, int(considered), int(opened), int(skipped), int(capacity), int(open_symbol_count), int(open_leg_count), skip_reasons or {})
        return {"capacity": capacity, "considered": considered, "opened": opened, "skipped": skipped, "open_symbols": open_symbol_count, "open_legs": open_leg_count, "blocked_reason": None, "fetched": fetched}

    def _open_from_signal(self, sig: Dict[str, Any], wallet_balance_usdt: float) -> bool:
        signal_id = int(sig["signal_id"])
        symbol_id = int(sig["symbol_id"])
        symbol = str(sig.get("symbol") or f"symbol_id={symbol_id}")

        sig_side = str(sig.get("side") or "").upper()  # BUY/SELL
        ledger_side = _sig_side_to_ledger_side(sig_side)  # LONG/SHORT
        if ledger_side not in ("LONG", "SHORT"):
            return False

        entry = _safe_float(sig.get("entry_price"), default=0.0)
        tf = str(sig.get("timeframe") or "15m")
        if entry <= 0:
            p = self._get_last_price(symbol_id, tf)
            entry = p or 0.0
        if entry <= 0:
            return False

        # size by risk
        risk_usdt = wallet_balance_usdt * _pct_to_mult(self.p.risk_wallet_pct)
        if risk_usdt <= 0:
            return False
        sl_pct = float(self.p.stop_loss_pct)
        if sl_pct <= 0:
            return False

        notional = risk_usdt / _pct_to_mult(sl_pct)
        entry_cap_pct = float(getattr(self.p, "max_position_notional_pct_wallet", 0.0) or 0.0)
        lev = max(1.0, _safe_float(getattr(self.p, "leverage", 1.0), 1.0))
        cap = wallet_balance_usdt * lev * _pct_to_mult(entry_cap_pct)
        if cap > 0:
            notional = min(notional, cap)

        qty = notional / entry
        qty_step = _safe_float(sig.get("qty_step"), default=0.0)
        tick_size = _safe_float(sig.get("price_tick"), default=0.0)
        if qty_step > 0:
            qty = _round_step_down(qty, qty_step)
        if qty <= 0:
            return False

        # SL/TP price
        if self.p.recalc_sl_tp:
            if ledger_side == "LONG":
                sl_price = entry * (1.0 - _pct_to_mult(self.p.stop_loss_pct))
                tp_price = entry * (1.0 + _pct_to_mult(self.p.take_profit_pct))
            else:
                sl_price = entry * (1.0 + _pct_to_mult(self.p.stop_loss_pct))
                tp_price = entry * (1.0 - _pct_to_mult(self.p.take_profit_pct))
        else:
            sl_price = _safe_float(sig.get("stop_loss"), default=0.0)
            tp_price = _safe_float(sig.get("take_profit"), default=0.0)

        # Round SL/TP to tick size to avoid Binance precision errors (-1111).
        if tick_size > 0:
            if ledger_side == "LONG":
                # SL below entry => round down; TP above => round up
                if sl_price > 0:
                    sl_price = _round_price_to_tick(sl_price, tick_size, mode="down")
                if tp_price > 0:
                    tp_price = _round_price_to_tick(tp_price, tick_size, mode="up")
            else:
                # SHORT: SL above => round up; TP below => round down
                if sl_price > 0:
                    sl_price = _round_price_to_tick(sl_price, tick_size, mode="up")
                if tp_price > 0:
                    tp_price = _round_price_to_tick(tp_price, tick_size, mode="down")

        # LIVE: place real orders first
        if self._is_live:
            return self._open_live_from_signal(
                signal_id=signal_id,
                symbol=symbol,
                symbol_id=symbol_id,
                sig_side=sig_side,
                ledger_side=ledger_side,
                timeframe=tf,
                qty=float(qty),
                entry_estimate=float(entry),
                sl_price=float(sl_price) if sl_price > 0 else 0.0,
                tp_price=float(tp_price) if tp_price > 0 else 0.0,
                risk_usdt=float(risk_usdt),
                notional_usdt=float(notional),
                qty_step=float(qty_step),
                tick_size=float(tick_size),
            )

        # PAPER: create position_ledger + paper_position_risk
        pos_uid = str(uuid.uuid4())
        meta = {
            "paper_entry": {
                "signal_id": signal_id,
                "screener": self.p.screener_name,
                "timeframe": tf,
                "signal_side": sig_side,
                "ledger_side": ledger_side,
                "entry_order_type": self.p.entry_order_type,
                "risk_wallet_pct": self.p.risk_wallet_pct,
                "risk_usdt": risk_usdt,
                "notional_usdt": notional,
                "qty_step": qty_step,
            }
        }

        if self._pl_has_raw_meta:
            ins = """
            INSERT INTO position_ledger (
              exchange_id, account_id, symbol_id, pos_uid,
              strategy_id, source,
              side, status,
              opened_at,
              qty_opened, qty_current,
              entry_price, avg_price,
              position_value_usdt,
              scale_in_count,
              raw_meta,
              updated_at
            ) VALUES (
              %(ex)s, %(acc)s, %(sym)s, %(pos_uid)s,
              %(sid)s, 'paper',
              %(side)s, 'OPEN',
              now(),
              %(qty)s, %(qty)s,
              %(entry)s, %(entry)s,
              %(val)s,
              0,
              %(meta)s::jsonb,
              now()
            );
            """
            params = {
                "ex": int(self.exchange_id),
                "acc": int(self.account_id),
                "sym": int(symbol_id),
                "pos_uid": pos_uid,
                "sid": self.STRATEGY_ID,
                "side": ledger_side,
                "qty": float(qty),
                "entry": float(entry),
                "val": float(entry * qty),
                "meta": json.dumps(meta),
            }
        else:
            ins = """
            INSERT INTO position_ledger (
              exchange_id, account_id, symbol_id, pos_uid,
              strategy_id, source,
              side, status,
              opened_at,
              qty_opened, qty_current,
              entry_price, avg_price,
              position_value_usdt,
              scale_in_count,
              updated_at
            ) VALUES (
              %(ex)s, %(acc)s, %(sym)s, %(pos_uid)s,
              %(sid)s, 'paper',
              %(side)s, 'OPEN',
              now(),
              %(qty)s, %(qty)s,
              %(entry)s, %(entry)s,
              %(val)s,
              0,
              now()
            );
            """
            params = {
                "ex": int(self.exchange_id),
                "acc": int(self.account_id),
                "sym": int(symbol_id),
                "pos_uid": pos_uid,
                "sid": self.STRATEGY_ID,
                "side": ledger_side,
                "qty": float(qty),
                "entry": float(entry),
                "val": float(entry * qty),
            }

        try:
            self.store.execute(ins, params)
        except Exception as e:
            log.exception("[trade_liquidation][PAPER] failed to INSERT position_ledger for %s: %s", symbol, e)
            return False

        self.store.execute(
            """
            INSERT INTO paper_position_risk (
              exchange_id, account_id, symbol_id, pos_uid, strategy_id,
              timeframe,
              sl_price, tp_price,
              trailing_enabled, trail_activation_pct, trail_pct, trail_armed, best_price,
              additions_count
            ) VALUES (
              %(ex)s, %(acc)s, %(sym)s, %(pos_uid)s, %(sid)s,
              %(tf)s,
              %(sl)s, %(tp)s,
              %(ten)s, %(tap)s, %(trp)s, %(armed)s, %(best)s,
              0
            )
            ON CONFLICT (exchange_id, account_id, symbol_id) DO UPDATE SET
              pos_uid=EXCLUDED.pos_uid,
              strategy_id=EXCLUDED.strategy_id,
              timeframe=EXCLUDED.timeframe,
              sl_price=EXCLUDED.sl_price,
              tp_price=EXCLUDED.tp_price,
              trailing_enabled=EXCLUDED.trailing_enabled,
              trail_activation_pct=EXCLUDED.trail_activation_pct,
              trail_pct=EXCLUDED.trail_pct,
              trail_armed=EXCLUDED.trail_armed,
              best_price=EXCLUDED.best_price,
              updated_at=now();
            """,
            {
                "ex": int(self.exchange_id),
                "acc": int(self.account_id),
                "sym": int(symbol_id),
                "pos_uid": pos_uid,
                "sid": self.STRATEGY_ID,
                "tf": tf,
                "sl": float(sl_price) if sl_price > 0 else None,
                "tp": float(tp_price) if tp_price > 0 else None,
                "ten": bool(self.p.trailing_enabled),
                "tap": float(self.p.trailing_activation_pct),
                "trp": float(self.p.trailing_trail_pct),
                "armed": False,
                "best": float(entry),
            },
        )

        self._mark_signal_taken(signal_id)
        log.info(
            "[trade_liquidation][PAPER] OPEN %s %s(%s) qty=%.8f entry=%.6f SL=%.6f TP=%.6f (risk=%.2fUSDT notional=%.2fUSDT)",
            symbol,
            sig_side,
            ledger_side,
            qty,
            entry,
            sl_price,
            tp_price,
            risk_usdt,
            notional,
        )
        return True

    def _open_live_from_signal(
        self,
        *,
        signal_id: int,
        symbol: str,
        symbol_id: int,
        sig_side: str,
        ledger_side: str,
        timeframe: str,
        qty: float,
        entry_estimate: float,
        sl_price: float,
        tp_price: float,
        risk_usdt: float,
        notional_usdt: float,
        qty_step: float,
        tick_size: float,
    ) -> bool:
        """Open real position + place SL/TP (market entry)."""

        if not self._binance:
            log.error("[trade_liquidation][LIVE] Binance client not initialized (check API key/secret envs)")
            return False

        # entry direction
        entry_side = "BUY" if ledger_side == "LONG" else "SELL"
        close_side = "SELL" if entry_side == "BUY" else "BUY"

        # pos_uid is the join-key for all orders + ledger.
        # For LIVE openings from signals it must be deterministic to survive retries/restarts.
        pos_uid = _stable_live_signal_pos_uid(
            exchange_id=self.exchange_id,
            account_id=self.account_id,
            strategy_id=self.STRATEGY_ID,
            signal_id=signal_id,
            symbol_id=symbol_id,
            side=ledger_side,
        )
        prefix = _sanitize_coid_prefix(str(getattr(self.p, "client_order_id_prefix", "TL") or "TL"))
        tok = _coid_token(pos_uid, n=20)
        cid_entry = f"{prefix}_{tok}_ENTRY"
        cid_sl = f"{prefix}_{tok}_SL"
        cid_trl = f"{prefix}_{tok}_TRL"
        cid_hedge = f"{prefix}_{tok}_HEDGE"
        cid_tp = f"{prefix}_{tok}_TP"

        hedge_mode = bool(getattr(self.p, "hedge_enabled", False))
        position_side = "LONG" if entry_side == "BUY" else "SHORT"  # for dual-side

        # 1) optional leverage/margin settings
        try:
            if self.p.margin_type:
                try:
                    self._binance.change_margin_type(symbol=symbol, marginType=str(self.p.margin_type))
                except Exception:
                    pass
            if self.p.leverage is not None and int(self.p.leverage) > 0:
                try:
                    self._binance.change_leverage(symbol=symbol, leverage=int(self.p.leverage))
                except Exception:
                    pass
        except Exception:
            pass

        # 2) MARKET entry (idempotent by deterministic pos_uid/client ids)
        existing_pos = self._find_open_main_position_for_signal(signal_id=signal_id, symbol_id=symbol_id, ledger_side=ledger_side)
        shadow_entry = self._get_order_shadow_by_client_id(cid_entry)
        entry_preexisting = False

        if existing_pos:
            entry_preexisting = True
            pos_uid = str(existing_pos.get("pos_uid") or pos_uid)
            avg_price = _safe_float(existing_pos.get("avg_price") or existing_pos.get("entry_price"), default=0.0) or float(entry_estimate)
            resp = {"orderId": None, "status": "FILLED", "avgPrice": avg_price, "clientOrderId": cid_entry}
            log.info("[trade_liquidation][LIVE] reuse existing OPEN position for signal_id=%s symbol=%s pos_uid=%s", int(signal_id), symbol, pos_uid)
        elif shadow_entry and str(shadow_entry.get("status") or "").upper() in {"NEW", "PARTIALLY_FILLED", "FILLED"}:
            entry_preexisting = True
            pos_uid = str(shadow_entry.get("pos_uid") or pos_uid)
            avg_price = _safe_float(shadow_entry.get("price"), default=0.0) or float(entry_estimate)
            resp = {"orderId": shadow_entry.get("order_id"), "status": shadow_entry.get("status") or "FILLED", "avgPrice": avg_price, "clientOrderId": cid_entry}
            log.warning("[trade_liquidation][LIVE] found existing entry shadow for signal_id=%s symbol=%s pos_uid=%s; skip duplicate MARKET", int(signal_id), symbol, pos_uid)
        else:
            reduce_enabled, reduce_steps, reduce_factor = self._entry_reduce_retry_settings()
            qty_try = float(qty)
            resp = None

            for attempt in range(1, reduce_steps + 1):
                try:
                    resp = self._binance.new_order(
                        symbol=symbol,
                        side=entry_side,
                        type="MARKET",
                        quantity=float(qty_try),
                        newClientOrderId=cid_entry,
                        positionSide=position_side if hedge_mode else None,
                    )
                    qty = float(qty_try)
                    break
                except Exception as e:
                    msg = str(e)
                    # If the first attempt already reached the exchange, Binance may reject the same client id.
                    if "client" in msg.lower() and "id" in msg.lower() and "duplicate" in msg.lower():
                        shadow_entry = self._get_order_shadow_by_client_id(cid_entry)
                        if shadow_entry and str(shadow_entry.get("status") or "").upper() in {"NEW", "PARTIALLY_FILLED", "FILLED"}:
                            entry_preexisting = True
                            pos_uid = str(shadow_entry.get("pos_uid") or pos_uid)
                            avg_price = _safe_float(shadow_entry.get("price"), default=0.0) or float(entry_estimate)
                            resp = {"orderId": shadow_entry.get("order_id"), "status": shadow_entry.get("status") or "FILLED", "avgPrice": avg_price, "clientOrderId": cid_entry}
                            break
                        log.exception("[trade_liquidation][LIVE] duplicate MARKET client id for %s but no shadow row found", symbol)
                        return False

                    can_reduce = reduce_enabled and self._is_binance_max_position_error(e) and attempt < reduce_steps
                    if not can_reduce:
                        log.exception("[trade_liquidation][LIVE] MARKET entry failed for %s: %s", symbol, e)
                        return False

                    next_qty = self._reduce_entry_qty_for_retry(symbol, qty_try, reduce_factor)
                    if next_qty <= 0.0 or next_qty >= float(qty_try):
                        log.exception(
                            "[trade_liquidation][LIVE] MARKET entry failed for %s: %s (cannot reduce qty further; qty=%s)",
                            symbol,
                            e,
                            qty_try,
                        )
                        return False

                    log.warning(
                        "[trade_liquidation][LIVE] max position limit for %s, retry MARKET with smaller qty: %.12f -> %.12f (attempt %d/%d)",
                        symbol,
                        float(qty_try),
                        float(next_qty),
                        attempt + 1,
                        reduce_steps,
                    )
                    qty_try = float(next_qty)

            if resp is None:
                log.error("[trade_liquidation][LIVE] MARKET entry aborted for %s: no response after retries", symbol)
                return False

            # Binance response may include avgPrice or fills; if missing, fallback
            avg_price = _safe_float(resp.get("avgPrice"), default=0.0)
            if avg_price <= 0:
                avg_price = entry_estimate

        # Re-anchor precomputed bracket prices to the *actual* average entry.
        # This keeps the same distance (delta) that was computed from the estimated entry.
        try:
            if avg_price > 0 and entry_estimate > 0 and sl_price > 0:
                sl_price = float(avg_price) + (float(sl_price) - float(entry_estimate))
        except Exception:
            pass
        try:
            if avg_price > 0 and entry_estimate > 0 and tp_price > 0:
                tp_price = float(avg_price) + (float(tp_price) - float(entry_estimate))
        except Exception:
            pass

        # shadow order row (entry)
        try:
            self._upsert_order_shadow(
                pos_uid=pos_uid,
                order_id=resp.get("orderId") or resp.get("order_id") or "",
                client_order_id=cid_entry,
                symbol_id=symbol_id,
                side=entry_side,
                order_type="MARKET",
                qty=float(qty),
                price=None,
                reduce_only=False,
                status=str(resp.get("status") or "NEW"),
            )
        except Exception:
            pass

        # 3) SL/TP brackets
        sl_mode = str(getattr(self.p, "sl_order_mode", "stop_market") or "stop_market").strip().lower()
        tp_mode = str(getattr(self.p, "tp_order_mode", "take_profit_market") or "take_profit_market").strip().lower()

        sl_resp: Optional[dict] = None
        tp_resp: Optional[dict] = None

        def _place_sl() -> Optional[dict]:
            if sl_price <= 0:
                return None

            avg_enabled = bool(getattr(self.p, "averaging_enabled", False)) and int(self._cfg_max_adds() or 0) > 0

            # In hedge mode, main SL must remain a regular reduce-only/close-position STOP_MARKET
            # when enable_stop_loss=true. Do NOT reinterpret main SL as a hedge opener here.
            # If enable_stop_loss=false, we simply skip placing main SL on entry.
            if hedge_mode and (not bool(getattr(self.p, "enable_stop_loss", True))):
                return None

            # LIVE trailing stop (Binance Futures): TRAILING_STOP_MARKET
            # Note: in Hedge Mode Binance forbids reduceOnly param, so we omit it there.
            if sl_mode in {"trailing_stop_market", "trailing", "tsm"}:
                if not bool(getattr(self.p, "trailing_enabled", True)):
                    return None

                callback_rate = float(getattr(self.p, "trailing_trail_pct", 0.6) or 0.6)
                activation_pct = float(getattr(self.p, "trailing_activation_pct", 1.2) or 1.2)
                buffer_pct = float(getattr(self.p, "trailing_activation_buffer_pct", 0.20) or 0.20)
                retries = int(getattr(self.p, "trailing_place_retries", 5) or 5)
                retries = max(1, min(10, retries))

                # Prefer current mark price to avoid immediate-trigger (-2021).
                mark = self._get_mark_price_live(symbol, position_side if hedge_mode else None)
                if mark <= 0:
                    mark = avg_price

                last_err: Exception | None = None
                for k in range(retries):
                    extra = buffer_pct * float(k + 1)

                    # Direction rules for Binance trailing:
                    #  - SELL trailing stop: activationPrice must be ABOVE current mark.
                    #  - BUY  trailing stop: activationPrice must be BELOW current mark.
                    if close_side.upper() == "SELL":
                        base_activation = avg_price * (1.0 + activation_pct / 100.0)
                        activation_price = self._safe_price_above_mark(base_activation, mark, float(tick_size or 0.0), extra)
                    else:
                        base_activation = avg_price * (1.0 - activation_pct / 100.0)
                        activation_price = self._safe_price_below_mark(base_activation, mark, float(tick_size or 0.0), extra)

                    # Round and enforce side vs actual avg entry (Binance is sensitive to triggers).
                    activation_price = _round_price_to_tick(
                        activation_price,
                        tick_size,
                        mode="up" if close_side.upper() == "SELL" else "down",
                    )
                    activation_price = _ensure_tp_trail_side(
                        activation_price,
                        avg_price,
                        tick_size,
                        ledger_side,
                        kind="trail_activate",
                    )

                    params = dict(
                        symbol=symbol,
                        side=close_side,
                        type="TRAILING_STOP_MARKET",
                        quantity=float(qty),
                        activationPrice=float(activation_price),
                        callbackRate=float(callback_rate),
                        workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                        newClientOrderId=cid_trl,
                        positionSide=position_side if hedge_mode else None,
                    )
                    if (not hedge_mode) and bool(self.p.reduce_only):
                        params["reduceOnly"] = True

                    known_trl = self._find_open_algo_order_by_client_id(symbol, cid_trl) or self._find_semantic_open_algo(
                        symbol,
                        client_suffix="_TRL",
                        position_side=position_side if hedge_mode else None,
                        order_type="TRAILING_STOP_MARKET",
                        side=close_side,
                    )
                    if known_trl:
                        return known_trl

                    try:
                        return self._binance.new_order(**params)
                    except Exception as e:
                        last_err = e
                        if self._is_immediate_trigger_error(e) and k < retries - 1:
                            continue
                        raise

                if last_err is not None:
                    raise last_err
                return None


            # protective stop-loss as close-position market stop
            params = dict(
                symbol=symbol,
                side=close_side,
                type="STOP_MARKET",
                stopPrice=float(sl_price),
                newClientOrderId=cid_sl,
                positionSide=position_side if hedge_mode else None,
                closePosition=True,
                workingType="MARK_PRICE",
            )
            if (not hedge_mode) and bool(self.p.reduce_only):
                params["reduceOnly"] = True
            known_sl = self._find_open_algo_order_by_client_id(symbol, cid_sl) or self._find_semantic_open_algo(
                symbol,
                client_suffix="_SL",
                position_side=position_side if hedge_mode else None,
                order_type="STOP_MARKET",
                side=close_side,
            )
            if known_sl:
                return known_sl
            return self._binance.new_order(**params)

        def _place_tp() -> Optional[dict]:
            if tp_price <= 0:
                return None

            # TP must be on correct side of *actual* avg entry
            base_tp = tp_price
            try:
                base_tp = _ensure_tp_trail_side(base_tp, avg_price, float(tick_size or 0.0), ledger_side, kind="tp")
            except Exception:
                base_tp = tp_price

            if tp_mode in {"take_profit_market", "takeprofit_market", "tp_market", "market"}:
                buffer_pct = float(getattr(self.p, "trailing_activation_buffer_pct", 0.20) or 0.20)
                retries = int(getattr(self.p, "trailing_place_retries", 5) or 5)
                retries = max(1, min(10, retries))

                mark = self._get_mark_price_live(symbol, position_side if hedge_mode else None)
                if mark <= 0:
                    mark = avg_price

                last_err: Exception | None = None
                for k in range(retries):
                    extra = buffer_pct * float(k + 1)

                    # TP trigger must be on the profitable side of current mark, otherwise Binance returns -2021.
                    if ledger_side == "LONG":
                        trig = self._safe_price_above_mark(base_tp, mark, float(tick_size or 0.0), extra)
                    else:
                        trig = self._safe_price_below_mark(base_tp, mark, float(tick_size or 0.0), extra)

                    params = dict(
                        symbol=symbol,
                        side=close_side,
                        type="TAKE_PROFIT_MARKET",
                        stopPrice=float(trig),
                        closePosition=True,
                        workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                        newClientOrderId=cid_tp,
                        positionSide=position_side if hedge_mode else None,
                    )
                    if (not hedge_mode) and bool(self.p.reduce_only):
                        params["reduceOnly"] = True

                    known_tp = self._find_open_algo_order_by_client_id(symbol, cid_tp) or self._find_semantic_open_algo(
                        symbol,
                        client_suffix="_TP",
                        position_side=position_side if hedge_mode else None,
                        order_type="TAKE_PROFIT_MARKET",
                        side=close_side,
                        close_position=True,
                    )
                    if known_tp:
                        return known_tp

                    try:
                        return self._binance.new_order(**params)
                    except Exception as e:
                        last_err = e
                        if self._is_immediate_trigger_error(e) and k < retries - 1:
                            continue
                        raise

                if last_err is not None:
                    raise last_err
                return None

            # tp-limit (TAKE_PROFIT)
            params = dict(
                symbol=symbol,
                side=close_side,
                type="TAKE_PROFIT",
                timeInForce=str(getattr(self.p, "time_in_force", "GTC")),
                quantity=float(qty),
                price=float(base_tp),
                stopPrice=float(base_tp),
                newClientOrderId=cid_tp,
                positionSide=position_side if hedge_mode else None,
            )
            if (not hedge_mode) and bool(self.p.reduce_only):
                params["reduceOnly"] = True
            known_tp = self._find_open_algo_order_by_client_id(symbol, cid_tp) or self._find_semantic_open_algo(
                symbol,
                client_suffix="_TP",
                position_side=position_side if hedge_mode else None,
                order_type="TAKE_PROFIT",
                side=close_side,
            )
            if known_tp:
                return known_tp
            return self._binance.new_order(**params)

        try:
            # If averaging (scale-in) is enabled and we defer SL until the last add,
            # do NOT place STOP_MARKET SL on initial entry. Trailing (if enabled) is handled separately.
            avg_enabled = bool(getattr(self.p, "averaging_enabled", False)) and int(self._cfg_max_adds() or 0) > 0
            defer_sl = bool(getattr(self.p, "defer_stop_loss_until_last_add", False))
            if bool(getattr(self.p, "enable_stop_loss", True)) and (not (avg_enabled and defer_sl)):
                sl_resp = _place_sl()

        except Exception:
            log.exception("[trade_liquidation][LIVE] failed to place SL for %s", symbol)

        try:
            main_partial_tp_enabled = bool(getattr(self.p, "main_partial_tp_enabled", False))
            if not main_partial_tp_enabled:
                tp_resp = _place_tp()
            else:
                log.info("[trade_liquidation][LIVE] skip base TP for %s because main_partial_tp_enabled=true", symbol)
        except Exception:
            log.exception("[trade_liquidation][LIVE] failed to place TP for %s", symbol)

        # shadow order rows (SL/TP)
        try:
            if isinstance(sl_resp, dict):
                # If we placed a hedge-opener instead of a classic SL (averaging mode),
                # it is NOT reduce-only and uses a dedicated client id.
                avg_enabled_shadow = bool(getattr(self.p, "averaging_enabled", False)) and int(self._cfg_max_adds() or 0) > 0
                sl_is_hedge = bool(getattr(self.p, "hedge_enabled", False)) and hedge_mode and avg_enabled_shadow

                self._upsert_order_shadow(
                    pos_uid=pos_uid,
                    order_id=sl_resp.get("orderId") or sl_resp.get("order_id") or sl_resp.get("algoId") or "",
                    client_order_id=cid_hedge if sl_is_hedge else cid_sl,
                    symbol_id=symbol_id,
                    side=close_side,
                    order_type=str(sl_resp.get("type") or "STOP_MARKET"),
                    qty=float(qty),
                    price=None,
                    reduce_only=(False if sl_is_hedge else True),
                    status=str(sl_resp.get("status") or "NEW"),
                )
            if isinstance(tp_resp, dict):
                self._upsert_order_shadow(
                    pos_uid=pos_uid,
                    order_id=tp_resp.get("orderId") or tp_resp.get("order_id") or tp_resp.get("algoId") or "",
                    client_order_id=cid_tp,
                    symbol_id=symbol_id,
                    side=close_side,
                    order_type=str(tp_resp.get("type") or "TAKE_PROFIT_MARKET"),
                    qty=float(qty),
                    price=None,
                    reduce_only=True,
                    status=str(tp_resp.get("status") or "NEW"),
                )
        except Exception:
            pass

        # 3.5) Averaging add (conditional MARKET) right after entry (if enabled)
        add_resp: Optional[dict] = None
        try:
            if bool(getattr(self.p, "averaging_enabled", False)) and int(self._cfg_max_adds() or 0) > 0:
                mult = float(getattr(self.p, "averaging_add_position_multiplier", 1.0) or 1.0)
                # Safety: respect max additions cap even at ENTRY time (prevents duplicate ADD1 after retries/restarts).
                max_adds_cfg = int(self._cfg_max_adds() or 0)
                if max_adds_cfg < 1:
                    mult = 1.0  # disable add placement
                else:
                    try:
                        # Refresh from DB truth (deduped by ADDn/pos_uid) to avoid duplicate ADD1 after restarts.
                        try:
                            _ = self._live_refresh_scale_in_count(pos_uid=pos_uid, prefix=prefix, sym=symbol, pos_side=ledger_side)
                        except Exception:
                            pass
                        rows_sc = list(self.store.query_dict(
                            "SELECT COALESCE(scale_in_count, 0) AS scale_in_count FROM position_ledger WHERE pos_uid=%(u)s LIMIT 1;",
                            {"u": str(pos_uid)},
                        ))
                        if rows_sc and int(rows_sc[0].get("scale_in_count") or 0) >= max_adds_cfg:
                            mult = 1.0  # cap reached -> disable add placement
                    except Exception:
                        pass
                if mult > 1.0:
                    min_dist_pct = float(self._cfg_averaging_min_level_distance_pct())
                    dist_limit_pct = max(0.01, float(min_dist_pct))
                    entry_ref = float(avg_price or entry_estimate or 0)
                    level = 0.0
                    if entry_ref > 0:
                        level = float(self._pick_averaging_level(
                            symbol_id=int(symbol_id),
                            side=str(ledger_side or ""),
                            ref_price=float(entry_ref),
                            timeframe=str(timeframe or getattr(self.p, "averaging_levels_tf", "4h") or "4h"),
                            level_index=1,
                        ) or 0.0)
                        if level <= 0:
                            if ledger_side == "LONG":
                                level = entry_ref * (1.0 - (dist_limit_pct / 100.0))
                            else:
                                level = entry_ref * (1.0 + (dist_limit_pct / 100.0))

                    if level > 0:
                        add_qty_dec, qty_source = self._calc_averaging_add_qty(pos_qty=qty, next_n=1, sym=symbol)
                        add_qty = float(add_qty_dec)

                        wallet_live = _safe_float(self._rest_snapshot_get("wallet_balance_usdt"), 0.0)
                        if wallet_live <= 0:
                            wallet_live = _safe_float(self._wallet_balance_usdt(), 0.0)
                        add_ref_price = float(level or entry_ref or entry_estimate or 0.0)
                        add_cap_pct = float(getattr(self.p, "averaging_add_max_notional_pct_wallet", 0.0) or 0.0)
                        if add_cap_pct > 0.0 and wallet_live > 0.0 and add_ref_price > 0.0:
                            capped_add_qty = _cap_qty_by_wallet_notional(
                                qty=float(add_qty),
                                wallet_balance_usdt=float(wallet_live),
                                price=float(add_ref_price),
                                cap_pct_wallet=float(add_cap_pct),
                                qty_step=float(qty_step or 0.0),
                                leverage=float(getattr(self.p, "leverage", 1.0) or 1.0),
                            )
                            if capped_add_qty < float(add_qty):
                                log.info("[trade_liquidation][LIMIT] capped ENTRY ADD1 %s %s qty %.8f -> %.8f by averaging_add_max_notional_pct_wallet=%.4f", symbol, ledger_side, float(add_qty), float(capped_add_qty), float(add_cap_pct))
                            add_qty = float(capped_add_qty)
                        main_total_cap_pct = float(getattr(self.p, "main_position_max_notional_pct_wallet", 0.0) or 0.0)
                        if main_total_cap_pct > 0.0 and wallet_live > 0.0 and add_ref_price > 0.0:
                            max_total_qty = _cap_qty_by_wallet_notional(
                                qty=float(qty) + float(add_qty),
                                wallet_balance_usdt=float(wallet_live),
                                price=float(add_ref_price),
                                cap_pct_wallet=float(main_total_cap_pct),
                                qty_step=float(qty_step or 0.0),
                                leverage=float(getattr(self.p, "leverage", 1.0) or 1.0),
                            )
                            allowed_add_qty = max(0.0, float(max_total_qty) - float(qty))
                            if qty_step > 0.0:
                                allowed_add_qty = _round_qty_to_step(allowed_add_qty, qty_step, mode="down")
                            if allowed_add_qty < float(add_qty):
                                log.info("[trade_liquidation][LIMIT] capped ENTRY ADD1 total %s %s qty %.8f -> %.8f by main_position_max_notional_pct_wallet=%.4f", symbol, ledger_side, float(add_qty), float(allowed_add_qty), float(main_total_cap_pct))
                            add_qty = float(max(0.0, allowed_add_qty))
                        if add_qty >= float(qty_step):
                            cid_add = f"{prefix}_{tok}_ADD1"
                            # Use TAKE_PROFIT_MARKET as conditional entry: BUY triggers when mark <= stopPrice; SELL triggers when mark >= stopPrice
                            add_side = entry_side
                            add_type = "TAKE_PROFIT_MARKET"
                            stop_px = _round_price_to_tick(level, float(tick_size or 0.0), mode="down" if add_side == "BUY" else "up")
                            # Protect from -2021 (would immediately trigger): ensure trigger is on the correct side of mark
                            mark_add = 0.0
                            try:
                                mark_add = float(self._get_mark_price(symbol, position_side if hedge_mode else None) or 0.0)
                            except Exception:
                                mark_add = 0.0
                            extra_buf = float(getattr(self.p, "tp_trigger_buffer_pct", 0.25) or 0.25)
                            if add_side == "BUY":
                                stop_px = self._safe_price_below_mark(float(stop_px), float(mark_add), float(tick_size), extra_buf)
                            else:
                                stop_px = self._safe_price_above_mark(float(stop_px), float(mark_add), float(tick_size), extra_buf)

                            def _place_add(i: int):
                                # Recompute trigger on every retry from a fresh mark and widen the safety buffer.
                                px = float(stop_px)
                                try:
                                    fresh_mark = float(self._get_mark_price(symbol, position_side if hedge_mode else None) or 0.0)
                                except Exception:
                                    fresh_mark = 0.0
                                _tick = float(tick_size or 0.0)
                                # Base buffer from config plus retry widening; for very cheap symbols Binance often
                                # rejects triggers that are technically valid but too close to the current MARK_PRICE.
                                retry_buf = float(extra_buf) + max(0, int(i)) * 0.20
                                if fresh_mark > 0.0:
                                    if add_side == "BUY":
                                        px = self._safe_price_below_mark(float(level), fresh_mark, _tick, retry_buf)
                                    else:
                                        px = self._safe_price_above_mark(float(level), fresh_mark, _tick, retry_buf)
                                if i > 0 and _tick > 0:
                                    # One more tick away from mark for each retry even after the safe-price adjustment.
                                    px = px - _tick * i if add_side == "BUY" else px + _tick * i
                                    px = _round_price_to_tick(px, _tick, mode="down" if add_side == "BUY" else "up")
                                return self._binance.new_order(
                                    symbol=symbol,
                                    side=add_side,
                                    type=add_type,
                                    stopPrice=float(px),
                                    quantity=float(add_qty),
                                    workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                                    newClientOrderId=cid_add,
                                    positionSide=position_side if hedge_mode else None,
                                )

                            add_resp = _tl_place_algo_with_retry(_place_add, max_tries=int(getattr(self.p, "tp_place_retries", 3) or 3))

                            try:
                                self._upsert_order_shadow(
                                    pos_uid=pos_uid,
                                    order_id=add_resp.get("orderId") or add_resp.get("algoId") or "",
                                    client_order_id=cid_add,
                                    symbol_id=symbol_id,
                                    side=add_side,
                                    order_type=str(add_resp.get("type") or add_type),
                                    qty=float(add_qty),
                                    price=None,
                                    reduce_only=False,
                                    status=str(add_resp.get("status") or "NEW"),
                                )
                            except Exception:
                                pass
        except Exception:
            log.exception("[trade_liquidation][LIVE] failed to place averaging add for %s", symbol)


        # 4) ledger row (journal)
        meta = {
            "live_entry": {
                "signal_id": int(signal_id),
                "screener": str(self.p.screener_name),
                "timeframe": str(timeframe),
                "signal_side": str(sig_side),
                "ledger_side": str(ledger_side),
                "risk_wallet_pct": float(self.p.risk_wallet_pct),
                "risk_usdt": float(risk_usdt),
                "notional_usdt": float(notional_usdt),
                "qty_step": float(qty_step),
                "entry_order": resp,
                "sl_order": sl_resp,
                "tp_order": tp_resp,
                "add_order": add_resp,
            }
        }

        cols = [
            "exchange_id",
            "account_id",
            "symbol_id",
            "pos_uid",
            "strategy_id",
            "strategy_name",
            "side",
            "status",
            "opened_at",
            "qty_opened",
            "qty_current",
            "entry_price",
            "avg_price",
            "position_value_usdt",
            "scale_in_count",
            "updated_at",
            "source",
        ]
        vals = [
            "%(ex)s",
            "%(acc)s",
            "%(sym)s",
            "%(pos_uid)s",
            "%(sid)s",
            "%(sname)s",
            "%(side)s",
            "'OPEN'",
            "now()",
            "%(qty)s",
            "%(qty)s",
            "%(entry)s",
            "%(avg)s",
            "%(val)s",
            "0",
            "now()",
            "'live'",
        ]
        if self._pl_has_raw_meta:
            cols.insert(-2, "raw_meta")
            vals.insert(-2, "%(meta)s::jsonb")

        ins = f"INSERT INTO position_ledger ({', '.join(cols)}) VALUES ({', '.join(vals)});"
        try:
            already_open = self._find_open_main_position_for_signal(signal_id=signal_id, symbol_id=symbol_id, ledger_side=ledger_side)
            if not already_open:
                self.store.execute(
                    ins,
                    {
                        "ex": int(self.exchange_id),
                        "acc": int(self.account_id),
                        "sym": int(symbol_id),
                        "pos_uid": str(pos_uid),
                        "sid": self.STRATEGY_ID,
                        "sname": self.STRATEGY_ID,
                        "side": str(ledger_side),
                        "qty": float(qty),
                        "entry": float(avg_price),
                        "avg": float(avg_price),
                        "val": float(avg_price * qty),
                        "meta": json.dumps(meta),
                    },
                )
        except Exception:
            log.exception("[trade_liquidation][LIVE] failed to INSERT position_ledger for %s", symbol)

        # mark signal taken
        self._mark_signal_taken(int(signal_id))

        log.info(
            "[trade_liquidation][LIVE] OPEN %s %s qty=%.8f avg=%.6f SL=%.6f TP=%.6f (sl=%s tp=%s hedge_mode=%s hedge_opened=%s pos_uid=%s)",
            symbol,
            ledger_side,
            float(qty),
            float(avg_price),
            float(sl_price),
            float(tp_price),
            str(sl_mode),
            str(tp_mode),
            str(hedge_mode),
            "False",
            str(pos_uid),
        )

        # Ensure main trailing exists immediately for a fresh pre-last-add position.
        # Without this, a new LIVE position can exist for one or more cycles with ADDn
        # already placed but without the corresponding main TRL, especially after
        # partial closes / recovery churn on the same symbol.
        try:
            max_adds_cfg = int(self._cfg_max_adds() or 0)
        except Exception:
            max_adds_cfg = 0
        try:
            if bool(getattr(self.p, "trailing_enabled", False)):
                self._live_ensure_tp_trailing_pre_last_add(
                    pos={
                        "symbol": str(symbol),
                        "side": str(ledger_side),
                        "pos_uid": str(pos_uid),
                        "avg_price": float(avg_price),
                        "entry_price": float(avg_price),
                        "qty_current": float(qty),
                        "qty_opened": float(qty),
                        "symbol_id": int(symbol_id),
                        "raw_meta": meta,
                    },
                    sym=str(symbol),
                    pos_side=str(ledger_side),
                    pos_uid=str(pos_uid),
                    prefix=str(prefix),
                    tok=str(tok),
                    adds_done=0,
                    max_adds=int(max_adds_cfg),
                )
        except Exception:
            log.exception(
                "[trade_liquidation][LIVE][TP_TRL] failed immediate ensure after OPEN for %s %s (pos_uid=%s)",
                str(symbol).upper(),
                str(ledger_side).upper(),
                str(pos_uid),
            )
        return True

    def _get_order_shadow_by_client_id(self, client_order_id: str) -> Optional[Dict[str, Any]]:
        try:
            rows = list(self.store.query_dict(
                """
                SELECT order_id, client_order_id, pos_uid, symbol_id, side, type, status, qty, price, updated_at
                FROM public.orders
                WHERE exchange_id=%(ex)s
                  AND account_id=%(acc)s
                  AND client_order_id=%(coid)s
                ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                LIMIT 1;
                """,
                {"ex": int(self.exchange_id), "acc": int(self.account_id), "coid": str(client_order_id)},
            ))
            return rows[0] if rows else None
        except Exception:
            log.exception("[trade_liquidation] failed to lookup orders shadow client_order_id=%s", str(client_order_id))
            return None

