# src/platform/core/risk/risk_engine.py
from __future__ import annotations

from dataclasses import dataclass

from src.platform.core.models.order import OrderIntent


@dataclass(slots=True)
class RiskLimits:
    # ---------------- v8 limits ----------------
    max_open_positions: int = 10
    max_notional_usdt: float = 10_000.0
    max_order_qty: float = 1e9
    max_daily_loss_usdt: float = 1e9  # realized only (legacy)

    # ---------------- v9 Market State limits ----------------
    # drawdown is tracked vs in-memory equity peak
    max_drawdown_pct: float = 1e9          # e.g. 0.15 = 15%
    min_available_margin_pct: float = 0.0  # e.g. 0.2 = 20% of wallet
    max_margin_used_pct: float = 1e9       # e.g. 0.8 = 80% of wallet

    # ---------------- v10 Floating PnL / Equity limits ----------------
    # All optional; if None -> disabled
    max_unrealized_loss_abs: float | None = None   # e.g. 25.0 USDT (blocks if unreal < -25)
    max_unrealized_loss_pct: float | None = None   # e.g. 0.03 (3% of wallet)
    min_equity_abs: float | None = None            # e.g. 50.0 USDT (blocks if equity < 50)
    max_total_loss_day_abs: float | None = None    # realized + unrealized (blocks if total < -X)

    # ---------------- v11 PANIC MODE ----------------
    # When panic is active: allow ONLY reduce_only orders
    panic_min_equity_abs: float | None = None      # equity < X -> PANIC
    panic_max_drawdown_pct: float | None = None    # dd > X -> PANIC
    panic_auto_recover: bool = True                # if condition is gone -> exit PANIC


class RiskEngine:
    """
    RiskEngine (v11):
      - v8: basic order/position/notional/daily realized limits
      - v9: market-state based limits (drawdown, margin availability, margin used)
      - v10: floating PnL & equity limits using total_unrealized_pnl
      - v11: PANIC MODE (freeze trading) -> only reduce_only allowed

    Notes:
      - intent.reduce_only orders are allowed even in PANIC.
      - drawdown peak is tracked in-memory per RiskEngine instance.
      - PANIC state is tracked in-memory per RiskEngine instance.
    """

    def __init__(self, limits: RiskLimits):
        self.limits = limits
        self._equity_peak: float | None = None

        # PANIC MODE state
        self._panic: bool = False
        self._panic_reason: str | None = None

    @staticmethod
    def _ms_get_float(ms: dict | None, *keys: str, default: float = 0.0) -> float:
        if not ms:
            return float(default)
        for k in keys:
            v = ms.get(k)
            if v is not None:
                try:
                    return float(v)
                except Exception:
                    pass
        return float(default)

    @staticmethod
    def _safe_abs_float(x: float | int | None, default: float = 0.0) -> float:
        try:
            return abs(float(x))
        except Exception:
            return float(default)

    def is_panic(self) -> bool:
        return bool(self._panic)

    def panic_reason(self) -> str | None:
        return self._panic_reason

    def _check_panic(self, *, equity: float, drawdown_pct: float | None) -> tuple[bool, str | None]:
        # PANIC by equity
        if self.limits.panic_min_equity_abs is not None:
            thr = float(self.limits.panic_min_equity_abs)
            if equity > 0 and equity < thr:
                return True, f"equity<{thr:.2f}"

        # PANIC by drawdown
        if self.limits.panic_max_drawdown_pct is not None and drawdown_pct is not None:
            thr = float(self.limits.panic_max_drawdown_pct)
            if drawdown_pct > thr:
                return True, f"drawdown>{thr*100:.2f}%"

        return False, None

    def _update_panic_state(self, *, equity: float, drawdown_pct: float | None) -> None:
        panic, reason = self._check_panic(equity=equity, drawdown_pct=drawdown_pct)

        if panic:
            self._panic = True
            self._panic_reason = reason
            return

        # no panic now
        if self._panic and bool(self.limits.panic_auto_recover):
            self._panic = False
            self._panic_reason = None

    def allow_order(
        self,
        *,
        intent: OrderIntent,
        price: float,
        open_positions_count: int,
        current_day_realized_pnl: float,
        market_state: dict | None = None,
        total_unrealized_pnl: float = 0.0,
    ) -> tuple[bool, str]:
        """
        Returns: (ok, reason)

        market_state expected keys (best-effort):
          - wallet_balance
          - available_balance
          - margin_used
          - equity (optional; if absent -> wallet + total_unrealized_pnl)

        total_unrealized_pnl:
          - should be sum of PositionState.unrealized_pnl for this (exchange_id, account_id)
        """

        # ---------------- v8 rules ----------------
        if open_positions_count >= int(self.limits.max_open_positions) and not intent.reduce_only:
            return False, "max_open_positions reached"

        if abs(intent.qty) > float(self.limits.max_order_qty):
            return False, "max_order_qty exceeded"

        notional = abs(intent.qty) * float(price)
        if notional > float(self.limits.max_notional_usdt) and not intent.reduce_only:
            return False, "max_notional_usdt exceeded"

        if current_day_realized_pnl <= -self._safe_abs_float(self.limits.max_daily_loss_usdt) and not intent.reduce_only:
            return False, "max_daily_loss_usdt reached"

        # ---------------- Market State extraction (for v9/v10/v11) ----------------
        wallet = 0.0
        equity = 0.0
        available = 0.0
        margin_used = 0.0

        if market_state:
            wallet = self._ms_get_float(market_state, "wallet_balance", "walletBalance", default=0.0)
            equity_ms = self._ms_get_float(market_state, "equity", default=0.0)
            equity = equity_ms if equity_ms > 0 else (wallet + float(total_unrealized_pnl or 0.0))
            available = self._ms_get_float(market_state, "available_balance", "availableBalance", default=0.0)
            margin_used = self._ms_get_float(market_state, "margin_used", "marginUsed", default=0.0)

        # ---------------- v9 drawdown peak tracking & dd compute ----------------
        dd: float | None = None
        if market_state:
            # peak tracking (in-memory, per instance)
            if equity > 0:
                if self._equity_peak is None or equity > self._equity_peak:
                    self._equity_peak = equity

            # drawdown
            if self._equity_peak and self._equity_peak > 0 and equity > 0:
                dd = (self._equity_peak - equity) / self._equity_peak

        # ---------------- v11 PANIC MODE ----------------
        # Update panic state based on equity/dd; then allow ONLY reduce_only if panic
        if market_state:
            self._update_panic_state(equity=equity, drawdown_pct=dd)

            if self._panic and not intent.reduce_only:
                return False, f"PANIC MODE: {self._panic_reason or 'active'}"

        # ---------------- v10 floating/equity rules (require market_state) ----------------
        if market_state and not intent.reduce_only:
            # 1) minimal equity
            if self.limits.min_equity_abs is not None and equity < float(self.limits.min_equity_abs):
                return False, f"min_equity_abs reached (equity={equity:.2f})"

            # 2) unrealized loss ABS
            if self.limits.max_unrealized_loss_abs is not None:
                lim = self._safe_abs_float(self.limits.max_unrealized_loss_abs)
                if float(total_unrealized_pnl or 0.0) < -lim:
                    return False, f"max_unrealized_loss_abs reached (unreal={float(total_unrealized_pnl):.2f})"

            # 3) unrealized loss PCT (of wallet)
            if self.limits.max_unrealized_loss_pct is not None and wallet > 0:
                unreal = float(total_unrealized_pnl or 0.0)
                loss_pct = (-unreal / wallet) if unreal < 0 else 0.0
                if loss_pct > float(self.limits.max_unrealized_loss_pct):
                    return False, f"max_unrealized_loss_pct reached ({loss_pct*100:.2f}%)"

            # 4) total day loss (realized + unrealized)
            if self.limits.max_total_loss_day_abs is not None:
                lim = self._safe_abs_float(self.limits.max_total_loss_day_abs)
                total = float(current_day_realized_pnl) + float(total_unrealized_pnl or 0.0)
                if total < -lim:
                    return False, f"max_total_loss_day_abs reached (total={total:.2f})"

        # ---------------- v9 rules (require market_state) ----------------
        if market_state and not intent.reduce_only:
            # drawdown limit
            if dd is not None:
                if dd > float(self.limits.max_drawdown_pct):
                    return False, "max_drawdown_pct reached"

            # available margin percent / margin used percent
            if wallet > 0:
                avail_pct = available / wallet
                if avail_pct < float(self.limits.min_available_margin_pct):
                    return False, "min_available_margin_pct reached"

                used_pct = margin_used / wallet
                if used_pct > float(self.limits.max_margin_used_pct):
                    return False, "max_margin_used_pct reached"

        return True, "ok"
