from __future__ import annotations

import json
import logging
from datetime import timezone
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple

from .params import *

log = logging.getLogger("traders.trade_liquidation")

class TradeLiquidationFiltersMixin:
    def _is_immediate_trigger_error(e: Exception) -> bool:
        s = str(e) if e is not None else ""
        s_l = s.lower()
        return ("immediately trigger" in s_l) or ("\"code\":-2021" in s_l) or ("code=-2021" in s_l)

    def _is_our_position_by_uid(self, pos_uid: str, open_orders_all: Any, raw_meta: Any) -> bool:
        """Heuristic: is this position created/managed by this trader.

        Returns True if:
          - pos_uid is present in any clientOrderId of current open orders (snapshot), or
          - raw_meta contains explicit marker that this trader created the position.
        Conservative by design: if unsure, returns False.
        """
        pos_uid = (pos_uid or "").strip()
        if not pos_uid:
            return False

        # 1) open orders marker (preferred)
        try:
            rows = open_orders_all if isinstance(open_orders_all, list) else []
            for o in rows:
                coid = str((o or {}).get("clientOrderId") or "")
                if pos_uid and pos_uid in coid:
                    return True
        except Exception:
            pass

        # 2) raw_meta marker (fallback)
        try:
            meta = raw_meta
            if isinstance(meta, str) and meta.strip():
                try:
                    meta = json.loads(meta)
                except Exception:
                    meta = None
            if isinstance(meta, dict):
                # explicit strategy/trader markers
                for k in ("strategy_id", "strategy", "trader", "origin", "bot", "source"):
                    v = meta.get(k)
                    if isinstance(v, str) and v.strip().lower() in ("trade_liquidation", "tl", "trade-liquidation"):
                        return True
                # sometimes we store pos_uid inside meta
                for k in ("pos_uid", "pos_uid", "position_uid"):
                    v = meta.get(k)
                    if isinstance(v, str) and v.strip() == pos_uid:
                        return True
        except Exception:
            pass

        return False

    def _is_our_position_by_symbol_side(
        self,
        symbol: str,
        side: str,
        open_orders_all: Any,
        open_algo_orders_all: Any,
        raw_meta: Any,
    ) -> bool:
        """Decide if position should be treated as 'ours' for reconcile purposes.

        Variant A: adjust ONLY our positions, but detect them reliably even if pos_uid is not embedded
        into client IDs (Binance does not include pos_uid).

        Heuristic (ordered):
          1) raw_meta live_entry.entry_order.clientOrderId starts with our prefix (e.g. 'TL_')
          2) any OPEN regular order for this symbol has clientOrderId starting with prefix
          3) any OPEN algo order for this symbol has clientAlgoId starting with prefix
          4) recent DB orders for this symbol have client_order_id starting with prefix (last 7 days)

        If unsure -> False (treat as external).
        """
        sym = str(symbol or "").strip().upper()
        sd = str(side or "").strip().upper()
        if not sym or sd not in {"LONG", "SHORT"}:
            return False

        prefix = str(getattr(self.p, "client_order_id_prefix", "TL") or "TL").strip()
        if prefix and not prefix.endswith("_"):
            prefix_ = prefix + "_"
        else:
            prefix_ = prefix

        # 1) raw_meta marker
        try:
            meta = raw_meta
            if isinstance(meta, str) and meta.strip():
                try:
                    meta = json.loads(meta)
                except Exception:
                    meta = None
            if isinstance(meta, dict):
                le = meta.get("live_entry") if isinstance(meta.get("live_entry"), dict) else None
                if le:
                    eo = le.get("entry_order") if isinstance(le.get("entry_order"), dict) else None
                    if eo:
                        coid = str(eo.get("clientOrderId") or "")
                        if coid.startswith(prefix_):
                            return True
        except Exception:
            pass

        # 2) open regular orders snapshot
        try:
            rows = open_orders_all if isinstance(open_orders_all, list) else []
            for o in rows:
                if str((o or {}).get("symbol") or "").strip().upper() != sym:
                    continue
                coid = str((o or {}).get("clientOrderId") or "")
                if coid.startswith(prefix_):
                    return True
        except Exception:
            pass

        # 3) open algo orders snapshot
        try:
            rows = open_algo_orders_all if isinstance(open_algo_orders_all, list) else []
            for o in rows:
                if str((o or {}).get("symbol") or "").strip().upper() != sym:
                    continue
                coid = str((o or {}).get("clientAlgoId") or "")
                if coid.startswith(prefix_):
                    return True
        except Exception:
            pass

        # 4) DB fallback (recent orders)
        try:
            row = self.store.query_one(
                """
                SELECT 1
                FROM orders o
                JOIN symbols s ON s.symbol_id=o.symbol_id AND s.exchange_id=o.exchange_id
                WHERE o.exchange_id=%(ex)s
                  AND o.account_id=%(acc)s
                  AND o.strategy_id=%(sid)s
                  AND s.symbol=%(sym)s
                  AND o.client_order_id LIKE %(pfx)s
                  AND o.updated_at >= (now() - interval '7 days')
                LIMIT 1
                """,
                {
                    "ex": int(self.exchange_id),
                    "acc": int(self.account_id),
                    "sid": str(self.STRATEGY_ID),
                    "sym": sym,
                    "pfx": prefix_ + "%",
                },
            )
            if row:
                return True
        except Exception:
            pass

        return False

    def _portfolio_cap_blocked(self) -> Tuple[bool, Dict[str, float]]:
        """Return (blocked, metrics) where blocked means do not open new positions.

        portfolio_cap_ratio is interpreted as:
            used_margin_usdt / wallet_balance_usdt >= portfolio_cap_ratio

        We prefer margin_used from balance snapshots (account_balance_snapshots/account_state)
        because it is Binance's own aggregate for the account. If it is unavailable, we fall
        back to best-effort summation from /fapi/v2/positionRisk.
        """
        ratio = float(getattr(self.p, "portfolio_cap_ratio", 0.0) or 0.0)
        if ratio <= 0:
            return False, {"cap_ratio": ratio, "used_margin": 0.0, "wallet": 0.0, "used_over_wallet": 0.0}

        wallet = self._rest_snapshot_get("wallet_balance_usdt")
        wallet_f = _safe_float(wallet, 0.0)
        if wallet_f <= 0:
            wallet_f = float(self._wallet_balance_usdt() or 0.0)

        # Prefer Binance account-level margin_used (written by balance-writer), fallback to positionRisk sum.
        used = _safe_float(self._rest_snapshot_get("margin_used_usdt"), 0.0)
        if used <= 0:
            pr = self._rest_snapshot_get("position_risk")
            used = self._portfolio_used_margin_usdt(pr)
        used_over = (used / wallet_f) if wallet_f > 0 else 0.0
        blocked = bool(wallet_f > 0 and used_over >= ratio)
        return blocked, {"cap_ratio": ratio, "used_margin": float(used), "wallet": float(wallet_f), "used_over_wallet": float(used_over)}

    def _cfg_max_adds(self) -> int:
        """Read max additions from config with backward-compatible keys.

        IMPORTANT:
          - Many configs still use `averaging_max_adds`.
          - `averaging_max_additions` has a non-zero default in Params, so we must not
            blindly prefer it when the legacy key is explicitly provided in config.
        Supported keys (priority):
          1) averaging_max_adds (legacy, if provided)
          2) averaging_max_additions (new)
          3) avg_max_adds (very old)
        """
        try:
            # If legacy key exists in config/extras, prefer it.
            v_legacy = None
            try:
                v_legacy = getattr(self.p, "averaging_max_adds", None)
            except Exception:
                v_legacy = None

            # New key (may be defaulted)
            v_new = getattr(self.p, "averaging_max_additions", None)

            # Heuristic: if legacy is set (not None) and differs from the new default value,
            # treat legacy as the source of truth.
            if v_legacy is not None:
                try:
                    n_legacy = int(v_legacy)
                except Exception:
                    n_legacy = 0
                try:
                    n_new = int(v_new) if v_new is not None else None
                except Exception:
                    n_new = None

                # If user set legacy and new is missing OR equals Params default (1),
                # prefer legacy.
                if n_new is None or n_new == 1 or n_new != n_legacy:
                    return max(0, int(n_legacy))

            # Otherwise fall back to new key
            if v_new is not None:
                return max(0, int(v_new))

            # Very old fallback
            v_old = getattr(self.p, "avg_max_adds", None)
            return max(0, int(v_old or 0))
        except Exception:
            return 0

    def _cfg_averaging_min_level_distance_pct(self) -> float:
        """Read averaging min level distance percent from config with backward-compatible keys.

        Reason:
          `averaging_min_level_distance_pct` has a non-zero default (15.0). If older configs use
          a legacy key, we must not accidentally ignore it just because the new key is defaulted.

        Supported keys (priority):
          1) averaging_min_level_distance_pct (new)
          2) min_level_distance_pct (legacy / older configs)
          3) avg_min_level_distance_pct (very old)
        """
        DEFAULT = 15.0
        try:
            v_new = getattr(self.p, "averaging_min_level_distance_pct", None)

            v_legacy = None
            try:
                v_legacy = getattr(self.p, "min_level_distance_pct", None)
            except Exception:
                v_legacy = None

            v_old = getattr(self.p, "avg_min_level_distance_pct", None)

            # If user provided legacy key and the new value looks like the default, prefer legacy.
            if v_legacy is not None:
                try:
                    n_legacy = float(v_legacy)
                except Exception:
                    n_legacy = DEFAULT
                try:
                    n_new = float(v_new) if v_new is not None else None
                except Exception:
                    n_new = None

                if n_new is None or abs(n_new - DEFAULT) < 1e-9 or abs(n_new - n_legacy) > 1e-9:
                    return max(0.0, float(n_legacy))

            if v_new is not None:
                return max(0.0, float(v_new))

            if v_old is not None:
                return max(0.0, float(v_old))

            return DEFAULT
        except Exception:
            return DEFAULT


    def _cfg_averaging_levels_method(self) -> str:
        """Read averaging levels method with backward-compatible keys.

        Supported values:
          - pivot
          - combined (pivot + volume confirmation)
        Unknown values fall back to ``pivot``.
        """
        try:
            raw = None
            for key in ("averaging_levels_method", "levels_method"):
                try:
                    raw = getattr(self.p, key, None)
                except Exception:
                    raw = None
                if raw not in (None, ""):
                    break
            method = str(raw or "pivot").strip().lower()
            if method not in {"pivot", "combined"}:
                return "pivot"
            return method
        except Exception:
            return "pivot"

    def _load_averaging_candles(self, symbol_id: int, timeframe: str) -> list[dict]:
        try:
            lv_tf = str(getattr(self.p, "averaging_levels_tf", timeframe) or timeframe)
            lookback_h = int(getattr(self.p, "averaging_levels_lookback_hours", 168) or 168)
            q = """
            SELECT open_time, high, low, close, volume
            FROM candles
            WHERE exchange_id=%(ex)s AND symbol_id=%(sym)s AND interval=%(tf)s
              AND open_time >= (NOW() AT TIME ZONE 'UTC') - (%(h)s || ' hours')::interval
            ORDER BY open_time ASC;
            """
            return list(self.store.query_dict(q, {"ex": int(self.exchange_id), "sym": int(symbol_id), "tf": lv_tf, "h": int(lookback_h)}))
        except Exception:
            return []

    def _extract_pivot_levels(self, rows: list[dict]) -> tuple[list[float], list[float]]:
        try:
            left = int(getattr(self.p, "averaging_pivot_left", 3) or 3)
            right = int(getattr(self.p, "averaging_pivot_right", 3) or 3)
            lows = [float((r or {}).get("low") or 0.0) for r in rows]
            highs = [float((r or {}).get("high") or 0.0) for r in rows]
            if len(lows) < (left + right + 5):
                return [], []
            piv_lows: list[float] = []
            piv_highs: list[float] = []
            for i in range(left, len(lows) - right):
                w_low = lows[i-left:i+right+1]
                cur_low = lows[i]
                if cur_low > 0 and cur_low == min(w_low):
                    piv_lows.append(cur_low)
                w_high = highs[i-left:i+right+1]
                cur_high = highs[i]
                if cur_high > 0 and cur_high == max(w_high):
                    piv_highs.append(cur_high)
            return piv_lows, piv_highs
        except Exception:
            return [], []

    def _extract_volume_profile_nodes(self, rows: list[dict]) -> list[float]:
        """Build simple volume-profile nodes from candles.

        We use candle HLC3 as the price proxy and candle volume as the weight.
        This is intentionally simple and stable enough for averaging levels.
        """
        try:
            bins = max(8, int(getattr(self.p, "averaging_vp_bins", 48) or 48))
            top_nodes = max(1, int(getattr(self.p, "averaging_vp_top_nodes", 10) or 10))
            min_sep_pct = float(getattr(self.p, "averaging_cluster_tolerance_pct", 0.25) or 0.25)
            prices: list[float] = []
            vols: list[float] = []
            lows: list[float] = []
            highs: list[float] = []
            for r in rows:
                hi = float((r or {}).get("high") or 0.0)
                lo = float((r or {}).get("low") or 0.0)
                cl = float((r or {}).get("close") or 0.0)
                vol = float((r or {}).get("volume") or 0.0)
                if hi <= 0 or lo <= 0 or cl <= 0 or vol <= 0:
                    continue
                lows.append(lo)
                highs.append(hi)
                prices.append((hi + lo + cl) / 3.0)
                vols.append(vol)
            if not prices:
                return []
            lo_all = min(lows)
            hi_all = max(highs)
            if hi_all <= lo_all:
                return []
            step = (hi_all - lo_all) / float(bins)
            if step <= 0:
                return []
            hist = [0.0] * bins
            centers = [lo_all + (i + 0.5) * step for i in range(bins)]
            for px, vol in zip(prices, vols):
                idx = int((px - lo_all) / step)
                if idx < 0:
                    idx = 0
                elif idx >= bins:
                    idx = bins - 1
                hist[idx] += vol
            ranked = sorted(range(bins), key=lambda i: hist[i], reverse=True)
            nodes: list[float] = []
            for idx in ranked:
                if hist[idx] <= 0:
                    continue
                px = float(centers[idx])
                if any(abs(px - n) / max(abs(n), 1e-9) * 100.0 <= min_sep_pct for n in nodes):
                    continue
                nodes.append(px)
                if len(nodes) >= top_nodes:
                    break
            return nodes
        except Exception:
            return []

    def _pick_averaging_level(
        self,
        *,
        symbol_id: int,
        side: str,
        ref_price: float,
        timeframe: str,
        level_index: int = 1,
    ) -> Optional[float]:
        """Pick averaging level according to configured method.

        Methods:
          - pivot: nearest qualifying pivot level
          - combined: nearest pivot with nearby volume-profile confirmation
        Returns ``None`` when no suitable level is found; caller should apply fallback.
        """
        try:
            side_u = str(side or "").upper()
            if ref_price <= 0 or side_u not in {"LONG", "SHORT"}:
                return None
            rows = self._load_averaging_candles(symbol_id, timeframe)
            if not rows:
                return None
            piv_lows, piv_highs = self._extract_pivot_levels(rows)
            method = self._cfg_averaging_levels_method()
            min_dist_pct = float(self._cfg_averaging_min_level_distance_pct())
            dist_limit_pct = max(0.01, float(min_dist_pct))
            level_index = max(1, int(level_index or 1))

            if side_u == "LONG":
                candidates = sorted({x for x in piv_lows if x > 0 and x < ref_price * (1.0 - dist_limit_pct/100.0)}, reverse=True)
            else:
                candidates = sorted({x for x in piv_highs if x > ref_price * (1.0 + dist_limit_pct/100.0)})

            if not candidates:
                return None

            if method == "pivot":
                return float(candidates[level_index - 1]) if len(candidates) >= level_index else None

            # combined = pivot + volume confirmation
            tol_pct = float(getattr(self.p, "averaging_level_tolerance_pct", 0.15) or 0.15)
            cluster_tol_pct = float(getattr(self.p, "averaging_cluster_tolerance_pct", 0.25) or 0.25)
            confirm_pct = max(tol_pct, cluster_tol_pct)
            vp_nodes = self._extract_volume_profile_nodes(rows)
            if not vp_nodes:
                return None

            confirmed: list[float] = []
            for lvl in candidates:
                if any(abs(lvl - node) / max(abs(lvl), 1e-9) * 100.0 <= confirm_pct for node in vp_nodes):
                    confirmed.append(float(lvl))
            return float(confirmed[level_index - 1]) if len(confirmed) >= level_index else None
        except Exception:
            return None

    def _parse_add_n(self, client_id: str) -> int:
        """Extract ADD number from client_order_id (.._ADD1/.._ADD2..)."""
        try:
            m = _ADD_RE.search(str(client_id or ""))
            if not m:
                return 0
            return max(0, int(m.group(1)))
        except Exception:
            return 0

    def _is_flat_qty(self, qty: Any, symbol: str) -> bool:
        """
        True if position amount is effectively zero for the symbol.

        Binance positionAmt can be very small residual; we treat anything below qty_step as flat.
        """
        try:
            q = abs(Decimal(str(qty)))
        except Exception:
            try:
                q = abs(Decimal(qty))
            except Exception:
                return False
        step = self._qty_step_for_symbol(symbol) or Decimal("0")
        if step > 0:
            return q < step
        return q == 0

    def _is_symbol_in_cooldown(self, symbol_id: int) -> bool:
        last = self._last_open_time_for_symbol(symbol_id)
        if not last:
            return False
        mins = (_utc_now() - last.astimezone(timezone.utc)).total_seconds() / 60.0
        return mins < float(self.p.per_symbol_cooldown_minutes)

    def _find_open_main_position_for_signal(self, signal_id: int, symbol_id: int, ledger_side: str) -> Optional[Dict[str, Any]]:
        """Find existing OPEN main live position for the same signal.

        Prefer deterministic pos_uid; also fall back to raw_meta.live_entry.signal_id when
        the DB row was already created by an older build. Hedge rows are excluded.
        """
        params = {
            "ex": int(self.exchange_id),
            "acc": int(self.account_id),
            "sid": str(self.STRATEGY_ID),
            "sym": int(symbol_id),
            "side": str(ledger_side),
            "signal_id": int(signal_id),
            "pos_uid": _stable_live_signal_pos_uid(
                exchange_id=self.exchange_id,
                account_id=self.account_id,
                strategy_id=self.STRATEGY_ID,
                signal_id=signal_id,
                symbol_id=symbol_id,
                side=ledger_side,
            ),
        }
        try:
            if self._pl_has_raw_meta:
                sql = """
                SELECT pos_uid, symbol_id, side, status, qty_current, entry_price, avg_price, raw_meta, opened_at, source
                FROM public.position_ledger
                WHERE exchange_id=%(ex)s
                  AND account_id=%(acc)s
                  AND strategy_id=%(sid)s
                  AND symbol_id=%(sym)s
                  AND side=%(side)s
                  AND status='OPEN'
                  AND COALESCE(source, 'live')='live'
                  AND (
                        pos_uid=%(pos_uid)s
                        OR COALESCE(raw_meta->'live_entry'->>'signal_id','')=%(signal_id)s::text
                      )
                ORDER BY opened_at DESC
                LIMIT 1;
                """
            else:
                sql = """
                SELECT pos_uid, symbol_id, side, status, qty_current, entry_price, avg_price, opened_at, source
                FROM public.position_ledger
                WHERE exchange_id=%(ex)s
                  AND account_id=%(acc)s
                  AND strategy_id=%(sid)s
                  AND symbol_id=%(sym)s
                  AND side=%(side)s
                  AND status='OPEN'
                  AND COALESCE(source, 'live')='live'
                  AND pos_uid=%(pos_uid)s
                ORDER BY opened_at DESC
                LIMIT 1;
                """
            rows = list(self.store.query_dict(sql, params))
            return rows[0] if rows else None
        except Exception:
            log.exception("[trade_liquidation] failed to lookup existing signal position signal_id=%s symbol_id=%s side=%s", int(signal_id), int(symbol_id), str(ledger_side))
            return None

