from __future__ import annotations


import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from .params import *

log = logging.getLogger("traders.trade_liquidation")

class TradeLiquidationCandidateFinderMixin:
    def _fetch_new_signals(self, limit: int = 50) -> List[Dict[str, Any]]:
        q = """

        SELECT
          sig.id AS signal_id,
          sig.signal_ts,
          sig.side,
          sig.exchange_id,
          sig.symbol_id,
          sig.timeframe,
          sig.entry_price,
          sig.stop_loss,
          sig.take_profit,
          sym.symbol,
          COALESCE(sf.qty_step, 0) AS qty_step,
          COALESCE(sf.price_tick, 0) AS price_tick,
          sig.context
        FROM signals sig
        JOIN screeners sc ON sc.screener_id = sig.screener_id
        JOIN symbols sym ON sym.symbol_id = sig.symbol_id
        LEFT JOIN symbol_filters sf
          ON sf.exchange_id = sig.exchange_id
         AND sf.symbol_id = sig.symbol_id
        WHERE sig.exchange_id=%(ex)s
          AND sc.name=%(scr)s
          AND sig.status=%(st)s
          AND sig.signal_ts >= now() - (%(age)s::text || ' minutes')::interval
        ORDER BY sig.signal_ts DESC
        LIMIT %(lim)s;
        
        """
        return list(
            self.store.query_dict(
                q,
                {
                    "ex": int(self.exchange_id),
                    "scr": str(self.p.screener_name),
                    "st": str(self.p.signal_status_new),
                    "age": int(self.p.max_signal_age_minutes),
                    "lim": int(limit),
                },
            )
        )

    def _expire_old_new_signals(self) -> int:
        age = self.p.expire_max_age_minutes if self.p.expire_max_age_minutes not in (None, 0, "0") else self.p.max_signal_age_minutes
        age = int(age) if age else int(self.p.max_signal_age_minutes)
        limit = int(getattr(self.p, "expire_batch_limit", 0) or 0)
        status_exp = str(getattr(self.p, "expire_status", "EXPIRED") or "EXPIRED").strip().upper()

        q = """
        WITH cte AS (
          SELECT sig.id
          FROM signals sig
          JOIN screeners sc ON sc.screener_id = sig.screener_id
          WHERE sig.exchange_id=%(ex)s
            AND sc.name=%(scr)s
            AND sig.status=%(st)s
            AND sig.signal_ts < now() - (%(age)s::text || ' minutes')::interval
          ORDER BY sig.signal_ts ASC
          LIMIT %(lim)s
        )
        UPDATE signals s
        SET status=%(newst)s,
            updated_at=now()
        FROM cte
        WHERE s.id = cte.id
        RETURNING s.id;
        """
        rows = list(
            self.store.query_dict(
                q,
                {
                    "ex": int(self.exchange_id),
                    "scr": str(self.p.screener_name),
                    "st": str(self.p.signal_status_new),
                    "age": int(age),
                    "lim": int(limit) if limit > 0 else 1000000000,
                    "newst": status_exp,
                },
            )
        )
        n = len(rows)
        if n > 0:
            log.info(
                "[trade_liquidation] expired old signals: %d (age>%dmin, screener=%s, limit=%s)",
                int(n),
                int(age),
                str(self.p.screener_name),
                str(limit if limit > 0 else "ALL"),
            )
        return n

    def _last_open_time_for_symbol(self, symbol_id: int) -> Optional[datetime]:
        q = """
        SELECT opened_at
        FROM position_ledger
        WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s
          AND strategy_id=%(sid)s AND source=%(src)s
        ORDER BY opened_at DESC
        LIMIT 1;
        """
        src = "live" if self._is_live else "paper"
        rows = list(
            self.store.query_dict(
                q,
                {"ex": int(self.exchange_id), "acc": int(self.account_id), "sym": int(symbol_id), "sid": self.STRATEGY_ID, "src": src},
            )
        )
        if not rows:
            return None
        v = rows[0].get("opened_at")
        return v if isinstance(v, datetime) else None

    def _promote_surviving_live_hedge_rows(self, pos_rows: List[Dict[str, Any]], qty_map: Dict[tuple, float], tol: float) -> Dict[str, Dict[str, Any]]:
        """If the original main leg is gone but the hedge leg remains open, promote the surviving
        live_hedge row to source='live' so the rest of the strategy manages it as the new main leg.
        """
        promoted: Dict[str, Dict[str, Any]] = {}
        if not bool(getattr(self.p, 'surviving_leg_promote_to_main_enabled', True)):
            return promoted
        try:
            by_symbol: Dict[str, List[Dict[str, Any]]] = {}
            for row in pos_rows or []:
                sym = str(row.get('symbol') or '').upper()
                if not sym:
                    continue
                by_symbol.setdefault(sym, []).append(row)

            for sym, rows in by_symbol.items():
                open_main_rows = []
                open_hedge_rows = []
                for row in rows:
                    side = str(row.get('side') or '').upper()
                    if side not in {'LONG', 'SHORT'}:
                        continue
                    exch_qty = float(qty_map.get((sym, side), 0.0) or 0.0)
                    if abs(exch_qty) <= tol:
                        continue
                    src = str(row.get('source') or 'live').strip().lower()
                    if src == 'live':
                        open_main_rows.append(row)
                    elif src == 'live_hedge':
                        open_hedge_rows.append(row)

                if open_main_rows or len(open_hedge_rows) != 1:
                    continue

                hedge_row = dict(open_hedge_rows[0])
                pos_uid = str(hedge_row.get('pos_uid') or '')
                if not pos_uid:
                    continue
                try:
                    patch = {
                        'promotion': {
                            'promoted_from_live_hedge': True,
                            'promotion_reason': 'surviving_leg_became_main',
                            'promotion_ts': float(time.time()),
                        }
                    }
                    if bool(getattr(self, '_pl_has_raw_meta', False)):
                        self.store.execute(
                            """
                            UPDATE position_ledger
                            SET source='live',
                                raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb,
                                updated_at=now()
                            WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND status='OPEN';
                            """,
                            {
                                'meta': json.dumps(patch),
                                'ex': int(self.exchange_id),
                                'acc': int(self.account_id),
                                'pos_uid': pos_uid,
                            },
                        )
                    else:
                        self.store.execute(
                            """
                            UPDATE position_ledger
                            SET source='live', updated_at=now()
                            WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND status='OPEN';
                            """,
                            {'ex': int(self.exchange_id), 'acc': int(self.account_id), 'pos_uid': pos_uid},
                        )
                    try:
                        self.store.execute(
                            """
                            DELETE FROM hedge_links
                            WHERE exchange_id=%(ex)s AND symbol_id=%(sym)s
                              AND (base_account_id=%(acc)s OR hedge_account_id=%(acc)s)
                              AND (base_pos_uid=%(pos_uid)s OR hedge_pos_uid=%(pos_uid)s);
                            """,
                            {'ex': int(self.exchange_id), 'sym': int(hedge_row.get('symbol_id')), 'acc': int(self.account_id), 'pos_uid': pos_uid},
                        )
                    except Exception:
                        pass
                    hedge_row['source'] = 'live'
                    promoted[pos_uid] = hedge_row
                    log.warning('[TL][PROMOTE_SURVIVING_LEG] promoted live_hedge -> live sym=%s side=%s pos_uid=%s', sym, str(hedge_row.get('side') or '').upper(), pos_uid)
                except Exception:
                    log.exception('[TL][PROMOTE_SURVIVING_LEG] failed sym=%s pos_uid=%s', sym, pos_uid)
            return promoted
        except Exception:
            log.exception('[TL][PROMOTE_SURVIVING_LEG] unexpected error')
            return promoted

    def _get_open_positions(self):
        """Возвращает OPEN позиции стратегии.

        В LIVE режиме считаем "открыто" по факту на бирже и при необходимости
        повышаем выжившую live_hedge-ногу до source='live', если исходная основная
        нога уже закрылась, а противоположная нога осталась на бирже открытой.
        """
        mode_u = str(self.mode).lower()
        if mode_u == 'live':
            source_clause = "COALESCE(pl.source,'live') IN ('live','live_hedge')"
        else:
            source_clause = "COALESCE(pl.source,'paper') = 'paper'"

        sql = f"""
        SELECT
          pl.exchange_id,
          pl.account_id,
          pl.pos_uid,
          pl.symbol_id,
          pl.strategy_id,
          pl.strategy_name,
          pl.side,
          pl.status,
          pl.opened_at,
          pl.closed_at,
          pl.entry_price,
          pl.avg_price,
          pl.exit_price,
          pl.qty_opened,
          pl.qty_current,
          pl.qty_closed,
          pl.position_value_usdt,
          pl.scale_in_count,
          pl.realized_pnl,
          pl.fees,
          pl.updated_at,
          pl.source,
          pl.raw_meta,
          s.symbol
        FROM public.position_ledger pl
        JOIN public.symbols s
          ON s.exchange_id = pl.exchange_id
         AND s.symbol_id   = pl.symbol_id
        WHERE pl.exchange_id = %(exchange_id)s
          AND pl.account_id  = %(account_id)s
          AND pl.strategy_id = %(strategy_id)s
          AND pl.status      = 'OPEN'
          AND ({source_clause})
        ORDER BY pl.opened_at ASC
        """
        pos = self.store.query_dict(sql, dict(
            exchange_id=int(self.exchange_id),
            account_id=int(self.account_id),
            strategy_id=str(self.strategy_id),
        ))

        if mode_u != 'live':
            return pos

        pr = self._rest_snapshot_get('position_risk')
        if not isinstance(pr, list):
            try:
                pr = self._binance.position_risk()
            except Exception as e:
                self._dlog('positionRisk fetch failed in _get_open_positions: %s', e)
                return [p for p in pos if str(p.get('source') or 'live').strip().lower() != 'live_hedge']

        qty_map = {}
        mark_map = {}
        for r in pr or []:
            sym = (r.get('symbol') or '').upper()
            ps = (r.get('positionSide') or '').upper()
            if not sym or not ps:
                continue
            try:
                amt = float(r.get('positionAmt') or 0.0)
            except Exception:
                amt = 0.0
            try:
                mp = float(r.get('markPrice') or 0.0)
            except Exception:
                mp = 0.0
            qty_map[(sym, ps)] = abs(amt)
            mark_map[(sym, ps)] = mp

        tol = float(getattr(self.params, 'reconcile_qty_tolerance', 1e-8) or 1e-8)
        auto_close = bool(getattr(self.params, 'reconcile_auto_close_ledger', True))
        promoted = self._promote_surviving_live_hedge_rows(pos, qty_map, tol)
        closed_now = 0
        open_real = []

        for p in pos:
            p = dict(p)
            pos_uid = str(p.get('pos_uid') or '')
            if pos_uid and pos_uid in promoted:
                p.update(promoted[pos_uid])
                p['source'] = 'live'

            sym = (p.get('symbol') or '').upper()
            side = (p.get('side') or '').upper()
            src = str(p.get('source') or 'live').strip().lower()
            exch_qty = float(qty_map.get((sym, side), 0.0) or 0.0)

            if src == 'live_hedge':
                continue

            if abs(exch_qty) <= tol:
                if auto_close:
                    try:
                        exit_price = float(mark_map.get((sym, side), 0.0) or 0.0)
                        stats = self._fetch_exchange_exit_stats(
                            symbol=sym,
                            pos_side=side,
                            opened_at=p.get('opened_at'),
                            closed_at=_utc_now(),
                            qty_expected=_safe_float(p.get('qty_current'), default=0.0),
                        )
                        ex_exit = stats.get('exit_price')
                        ex_pnl = stats.get('realized_pnl')
                        ex_close_ms = stats.get('close_time_ms')

                        if ex_exit is None:
                            ex_exit = exit_price
                        if not ex_exit:
                            ex_exit = float(p.get('avg_price') or p.get('entry_price') or 0.0)

                        if ex_pnl is None:
                            entry = _safe_float(p.get('avg_price'), default=_safe_float(p.get('entry_price'), default=0.0))
                            qty = _safe_float(p.get('qty_current'), default=0.0)
                            ex_pnl = (float(ex_exit) - entry) * qty if str(side).upper() == 'LONG' else (entry - float(ex_exit)) * qty

                        self._close_position_exchange(
                            p,
                            exit_price=float(ex_exit),
                            realized_pnl=float(ex_pnl),
                            close_time_ms=ex_close_ms,
                            reason='exchange_qty_zero',
                            timeframe='reconcile',
                        )
                        self._live_cancel_symbol_orders(sym)
                        closed_now += 1
                    except Exception as e:
                        self._dlog('auto-close ledger failed pos_uid=%s sym=%s side=%s: %s', p.get('pos_uid'), sym, side, e)
                continue

            open_real.append(p)

        if closed_now:
            log.info('[TL] ledger auto-closed (exchange qty=0): closed_now=%s', closed_now)

        return open_real

    def _get_last_price(self, symbol_id: int, timeframe: str) -> Optional[float]:
        q = """
        SELECT close
        FROM candles
        WHERE exchange_id=%(ex)s AND symbol_id=%(sym)s AND interval=%(tf)s
        ORDER BY open_time DESC
        LIMIT 1;
        """
        rows = list(self.store.query_dict(q, {"ex": int(self.exchange_id), "sym": int(symbol_id), "tf": str(timeframe)}))
        if not rows:
            return None
        return _safe_float(rows[0].get("close"), default=0.0)

    def _ema_direction(
        self,
        symbol_id: int,
        *,
        window: int = 20,
        interval: str = "5m",
        confirm_bars: int = 1,
        min_slope_pct: float = 0.0,
        log_ctx: Optional[str] = None,
    ) -> Optional[str]:
        """Return EMA direction: 'UP' | 'DOWN' | 'FLAT' | None.

        Anti-chop knobs:
        - confirm_bars: require N consecutive EMA moves in same direction.
        - min_slope_pct: require last-step EMA slope (% of EMA) to be strong enough.

        Uses candles table (close) for given interval.
        """
        ctx = f"[{log_ctx}] " if log_ctx else ""
        ema_log_enabled = bool(getattr(self.p, "ema_log_enabled", False) or getattr(self.p, "debug", False))

        def _ema_log(msg: str, *args: Any) -> None:
            if ema_log_enabled:
                log.info(msg, *args)

        try:
            window = int(window)
            if window < 2:
                _ema_log("%s[EMA] skip: window<2 (window=%s)", ctx, window)
                return None
        except Exception:
            _ema_log("%s[EMA] skip: bad window=%r", ctx, window)
            return None

        try:
            confirm_bars = int(confirm_bars)
        except Exception:
            confirm_bars = 1
        confirm_bars = max(1, confirm_bars)

        try:
            min_slope_pct = float(min_slope_pct or 0.0)
        except Exception:
            min_slope_pct = 0.0

        q = """
        SELECT close
        FROM candles
        WHERE exchange_id=%(ex)s AND symbol_id=%(sym)s AND interval=%(tf)s
        ORDER BY open_time ASC
        LIMIT %(n)s;
        """
        # take a bit more history to stabilize EMA
        n = max(window * 4, window + 5)
        rows = list(self.store.query_dict(q, {"ex": int(self.exchange_id), "sym": int(symbol_id), "tf": str(interval), "n": int(n)}))
        closes = [float(r.get("close") or 0.0) for r in rows if float(r.get("close") or 0.0) > 0]
        if len(closes) < window + 2:
            _ema_log(
                "%s[EMA] insufficient candles: got=%s need>=%s interval=%s symbol_id=%s",
                ctx,
                len(closes),
                window + 2,
                interval,
                symbol_id,
            )
            return None

        alpha = 2.0 / (float(window) + 1.0)
        # init EMA with SMA(window)
        ema = sum(closes[:window]) / float(window)

        # keep last (confirm_bars + 1) EMA points for direction validation
        keep = max(2, confirm_bars + 1)
        ema_tail: List[float] = [float(ema)]

        for c in closes[window:]:
            ema = (c - ema) * alpha + ema
            ema_tail.append(float(ema))
            if len(ema_tail) > keep:
                ema_tail = ema_tail[-keep:]

        if len(ema_tail) < 2:
            return None

        diffs = [ema_tail[i] - ema_tail[i - 1] for i in range(1, len(ema_tail))]
        if len(diffs) < confirm_bars:
            return None
        last_diffs = diffs[-confirm_bars:]

        ema_prev = float(ema_tail[-2])
        ema_last = float(ema_tail[-1])
        slope_pct = 0.0
        if ema_prev != 0:
            slope_pct = (ema_last - ema_prev) / ema_prev * 100.0

        all_up = all(d > 0 for d in last_diffs)
        all_down = all(d < 0 for d in last_diffs)

        if log_ctx and ema_log_enabled:
            _ema_log(
                "%s[EMA] computed: interval=%s window=%s confirm_bars=%s min_slope_pct=%.6f ema_prev=%.8f ema_last=%.8f slope_pct=%.6f last_diffs=%s",
                ctx,
                interval,
                window,
                confirm_bars,
                float(min_slope_pct),
                ema_prev,
                ema_last,
                slope_pct,
                last_diffs,
            )

        if all_up and slope_pct >= float(min_slope_pct):
            if log_ctx and ema_log_enabled:
                _ema_log("%s[EMA] direction=UP", ctx)
            return "UP"
        if all_down and slope_pct <= -float(min_slope_pct):
            if log_ctx and ema_log_enabled:
                _ema_log("%s[EMA] direction=DOWN", ctx)
            return "DOWN"
        if log_ctx and ema_log_enabled:
            _ema_log("%s[EMA] direction=FLAT", ctx)
        return "FLAT"

    def _compute_significant_level(self, symbol_id: int, *, side: str, entry_ref: float, timeframe: str) -> float:
        """Compute a significant support/resistance level using pivot highs/lows (same logic as live ADD1).

        Returns 0.0 if cannot compute.
        """
        try:
            lv_tf = str(getattr(self.p, "averaging_levels_tf", timeframe) or timeframe)
            lookback_h = int(getattr(self.p, "averaging_levels_lookback_hours", 168) or 168)
            left = int(getattr(self.p, "averaging_pivot_left", 3) or 3)
            right = int(getattr(self.p, "averaging_pivot_right", 3) or 3)
            min_dist_pct = float(self._cfg_averaging_min_level_distance_pct())
            dist_limit_pct = max(5.0, float(min_dist_pct))

            if entry_ref <= 0:
                return 0.0

            q = """
            SELECT high, low
            FROM candles
            WHERE exchange_id=%(ex)s AND symbol_id=%(sym)s AND interval=%(tf)s
              AND open_time >= (NOW() AT TIME ZONE 'UTC') - (%(h)s || ' hours')::interval
            ORDER BY open_time ASC;
            """
            rows = list(self.store.query_dict(q, {"ex": int(self.exchange_id), "sym": int(symbol_id), "tf": lv_tf, "h": int(lookback_h)}))
            lows = [float(r.get("low") or 0.0) for r in rows]
            highs = [float(r.get("high") or 0.0) for r in rows]
            if len(lows) < (left + right + 5):
                return 0.0

            piv_lows = []
            piv_highs = []
            for i in range(left, len(lows) - right):
                w = lows[i-left:i+right+1]
                if lows[i] > 0 and lows[i] == min(w):
                    piv_lows.append(lows[i])
                w2 = highs[i-left:i+right+1]
                if highs[i] > 0 and highs[i] == max(w2):
                    piv_highs.append(highs[i])

            side_u = str(side or "").upper()
            level = 0.0
            if side_u == "LONG":
                below = [x for x in piv_lows if x > 0 and x < entry_ref * (1.0 - dist_limit_pct/100.0)]
                if below:
                    level = max(below)
                else:
                    level = entry_ref * (1.0 - (dist_limit_pct / 100.0))
            else:
                above = [x for x in piv_highs if x > entry_ref * (1.0 + dist_limit_pct/100.0)]
                if above:
                    level = min(above)
                else:
                    level = entry_ref * (1.0 + (dist_limit_pct / 100.0))

            return float(level) if level and level > 0 else 0.0
        except Exception:
            return 0.0

