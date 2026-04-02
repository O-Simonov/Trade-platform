from __future__ import annotations

import logging
import random
import time
from typing import Any, Dict, Optional
from .params import *

log = logging.getLogger("traders.trade_liquidation")

class TradeLiquidationRiskAdapterMixin:

    def _portfolio_used_margin_usdt(self, position_risk: Any) -> float:
        """Best-effort estimate of used margin from /fapi/v2/positionRisk.

        Binance returns per-position fields like positionInitialMargin / isolatedMargin.
        We sum absolute margins for all non-zero positions.
        """
        try:
            rows = position_risk if isinstance(position_risk, list) else []
            used = 0.0
            for it in rows:
                amt = _safe_float(it.get("positionAmt"), 0.0)
                if abs(amt) <= 0:
                    continue
                m = _safe_float(it.get("positionInitialMargin"), 0.0)
                if m <= 0:
                    m = _safe_float(it.get("isolatedMargin"), 0.0)
                if m <= 0:
                    m = _safe_float(it.get("initialMargin"), 0.0)
                used += abs(float(m))
            return float(used)
        except Exception:
            return 0.0

    def _load_risk(self, symbol_id: int) -> Optional[Dict[str, Any]]:
        q = """
        SELECT *
        FROM paper_position_risk
        WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s;
        """
        rows = list(self.store.query_dict(q, {"ex": int(self.exchange_id), "acc": int(self.account_id), "sym": int(symbol_id)}))
        return rows[0] if rows else None

    def _update_risk(self, symbol_id: int, **kwargs: Any) -> None:
        if not kwargs:
            return
        sets = [f"{k}=%({k})s" for k in kwargs.keys()]
        kwargs["ex"] = int(self.exchange_id)
        kwargs["acc"] = int(self.account_id)
        kwargs["sym"] = int(symbol_id)
        q = f"""
        UPDATE paper_position_risk
        SET {', '.join(sets)}, updated_at=now()
        WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s;
        """
        self.store.execute(q, kwargs)

    def _delete_risk(self, symbol_id: int) -> None:
        self.store.execute(
            "DELETE FROM paper_position_risk WHERE exchange_id=%s AND account_id=%s AND symbol_id=%s",
            (int(self.exchange_id), int(self.account_id), int(symbol_id)),
        )

    def _margin_used_usdt(self) -> float:
        """Return latest used margin (USDT) from DB snapshots.

        This value is written by balance-writer into account_balance_snapshots.margin_used.
        Used for portfolio_cap_ratio when cap is configured.
        """
        if not hasattr(self, "_last_margin_used_usdt"):
            self._last_margin_used_usdt = 0.0
            self._last_margin_used_ts = 0.0

        try:
            # small cache to avoid DB hit every call
            now_ts = time.time()
            age = now_ts - float(getattr(self, "_last_margin_used_ts", 0.0) or 0.0)
            poll_sec = float(getattr(self.p, "balance_poll_sec", 10.0) or 10.0)
            if 0 <= age <= max(5.0, poll_sec):
                return float(getattr(self, "_last_margin_used_usdt", 0.0) or 0.0)

            row = self.store.fetch_one(
                """
                SELECT margin_used
                FROM account_balance_snapshots
                WHERE exchange_id = %(ex)s AND account_id = %(acc)s
                ORDER BY ts DESC
                LIMIT 1
                """,
                {"ex": int(self.exchange_id), "acc": int(self.account_id)},
            )
            mu = _safe_float(row.get("margin_used") if isinstance(row, dict) else None, default=0.0)
            if mu >= 0:
                self._last_margin_used_usdt = float(mu)
                self._last_margin_used_ts = time.time()
            return float(getattr(self, "_last_margin_used_usdt", 0.0) or 0.0)
        except Exception:
            return float(getattr(self, "_last_margin_used_usdt", 0.0) or 0.0)

    def _wallet_balance_usdt(self) -> float:
        """Return walletBalance for USDT.

        Robustness:
        - Retries transient Binance REST issues (timeouts / TLS handshakes / temporary network).
        - Keeps a short-lived cache of the last successful wallet balance.
        - Falls back to DB snapshot if exchange fetch fails.
        """
        # local cache (in-memory) to survive transient REST issues
        if not hasattr(self, "_last_wallet_balance_usdt"):
            self._last_wallet_balance_usdt = 0.0
            self._last_wallet_balance_ts = None  # type: ignore[assignment]

        cache_ttl = float(getattr(self.p, "wallet_balance_cache_ttl_sec", 180.0) or 180.0)
        # Poll interval: even if exchange is healthy, do NOT request balance more often than this.
        # This is the main speedup knob when your loop is frequent.
        poll_sec = float(getattr(self.p, "wallet_balance_poll_sec", 10.0) or 10.0)
        now = _utc_now()

        def _cache_ok() -> bool:
            ts = getattr(self, "_last_wallet_balance_ts", None)
            if ts is None:
                return False
            try:
                age = (now - ts).total_seconds()
                return 0 <= age <= cache_ttl and float(getattr(self, "_last_wallet_balance_usdt", 0.0) or 0.0) > 0
            except Exception:
                return False

        # 0) Fast path: reuse recent cached value (even if exchange is healthy)
        ts = getattr(self, "_last_wallet_balance_ts", None)
        if ts is not None:
            try:
                age = (now - ts).total_seconds()
                if 0 <= age <= poll_sec and float(getattr(self, "_last_wallet_balance_usdt", 0.0) or 0.0) > 0:
                    return float(getattr(self, "_last_wallet_balance_usdt", 0.0) or 0.0)
            except Exception:
                pass

        # 1) Exchange (preferred)
        if bool(getattr(self.p, "use_exchange_balance", True)) and self._binance is not None:
            retries = int(getattr(self.p, "wallet_balance_retries", 2) or 2)
            backoff = float(getattr(self.p, "wallet_balance_retry_backoff_sec", 1.0) or 1.0)

            for i in range(max(1, retries + 1)):
                try:
                    rows = self._binance.balance()
                    bal = 0.0
                    if isinstance(rows, list):
                        for it in rows:
                            if str(it.get("asset") or "").upper() == "USDT":
                                # Binance Futures /fapi/v2/balance returns keys like:
                                # balance, availableBalance, crossWalletBalance, etc.
                                for k in ("walletBalance", "crossWalletBalance", "balance", "maxWithdrawAmount"):
                                    bal = _safe_float(it.get(k), default=0.0)
                                    if bal > 0:
                                        break
                                break
                    if bal > 0:
                        self._last_wallet_balance_usdt = float(bal)
                        self._last_wallet_balance_ts = now
                        return float(bal)
                    # if response is weird but request succeeded: do not instantly kill cached value
                    if _cache_ok():
                        log.warning("[trade_liquidation] exchange balance returned 0/invalid -> using cached %.2f USDT", float(self._last_wallet_balance_usdt))
                        return float(self._last_wallet_balance_usdt)
                    return 0.0

                except Exception:
                    if i < max(1, retries + 1):
                        # small exponential backoff
                        delay = backoff * (2 ** max(0, i - 1)) + (random.random() * 0.2)
                        log.warning("[trade_liquidation] exchange balance fetch failed (attempt %d/%d); retry in %.2fs", i, max(1, retries + 1), delay)
                        time.sleep(delay)
                        continue

                    log.exception("[trade_liquidation] failed to fetch exchange balance; fallback to DB")

                    if _cache_ok():
                        log.warning("[trade_liquidation] using cached wallet balance %.2f USDT (age<=%.0fs)", float(self._last_wallet_balance_usdt), cache_ttl)
                        return float(self._last_wallet_balance_usdt)

                    break  # fallback to DB below

        # 2) DB fallback (account_balance_snapshots)
        try:
            row = self.store.fetch_one(
                """
                SELECT wallet_balance AS wallet_balance_usdt
                FROM account_balance_snapshots
                WHERE exchange_id=%(ex)s AND account_id=%(acc)s
                ORDER BY ts DESC
                LIMIT 1
                """,
                {"ex": int(self.exchange_id), "acc": int(self.account_id)},
            )
            bal = _safe_float(row.get("wallet_balance_usdt") if isinstance(row, dict) else None, default=0.0)
            if bal > 0:
                self._last_wallet_balance_usdt = float(bal)
                self._last_wallet_balance_ts = now
            return float(bal)
        except Exception:
            log.exception("[trade_liquidation] DB wallet balance fallback failed")
            if _cache_ok():
                log.warning("[trade_liquidation] using cached wallet balance %.2f USDT (db failed)", float(self._last_wallet_balance_usdt))
                return float(self._last_wallet_balance_usdt)
            return 0.0

