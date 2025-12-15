from __future__ import annotations
from dataclasses import dataclass
from platform.core.models.order import OrderIntent

@dataclass
class RiskLimits:
    # v8 limits
    max_open_positions: int = 10
    max_notional_usdt: float = 10_000.0
    max_order_qty: float = 1e9
    max_daily_loss_usdt: float = 1e9

    # v9 Market State limits
    max_drawdown_pct: float = 1e9          # e.g. 0.15 = 15%
    min_available_margin_pct: float = 0.0 # e.g. 0.2 = 20% of wallet
    max_margin_used_pct: float = 1e9      # e.g. 0.8 = 80% of wallet

class RiskEngine:
    def __init__(self, limits: RiskLimits):
        self.limits = limits
        self._equity_peak: float | None = None

    def allow_order(self, *, intent: OrderIntent, price: float, open_positions_count: int,
                    current_day_realized_pnl: float,
                    market_state: dict | None = None) -> tuple[bool, str]:
        # --- v8 rules ---
        if open_positions_count >= int(self.limits.max_open_positions) and not intent.reduce_only:
            return False, "max_open_positions reached"
        if abs(intent.qty) > float(self.limits.max_order_qty):
            return False, "max_order_qty exceeded"
        notional = abs(intent.qty) * float(price)
        if notional > float(self.limits.max_notional_usdt) and not intent.reduce_only:
            return False, "max_notional_usdt exceeded"
        if current_day_realized_pnl <= -abs(float(self.limits.max_daily_loss_usdt)):
            return False, "max_daily_loss_usdt reached"

        # --- v9 rules (require market_state) ---
        if market_state:
            wallet = float(market_state.get("wallet_balance") or 0.0)
            equity = float(market_state.get("equity") or 0.0)
            available = float(market_state.get("available_balance") or 0.0)
            margin_used = float(market_state.get("margin_used") or 0.0)

            # peak tracking (in-memory, per instance)
            if equity > 0:
                if self._equity_peak is None or equity > self._equity_peak:
                    self._equity_peak = equity

            # drawdown
            if self._equity_peak and self._equity_peak > 0 and equity > 0:
                dd = (self._equity_peak - equity) / self._equity_peak
                if dd > float(self.limits.max_drawdown_pct) and not intent.reduce_only:
                    return False, "max_drawdown_pct reached"

            # available margin percent
            if wallet > 0:
                avail_pct = available / wallet
                if avail_pct < float(self.limits.min_available_margin_pct) and not intent.reduce_only:
                    return False, "min_available_margin_pct reached"

                used_pct = margin_used / wallet
                if used_pct > float(self.limits.max_margin_used_pct) and not intent.reduce_only:
                    return False, "max_margin_used_pct reached"

        return True, "ok"
