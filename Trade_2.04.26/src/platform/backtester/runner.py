# src/platform/backtester/runner.py
from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Sequence

from .core import (
    BacktestEngine,
    ClosedTrade,
    ExitReason,
    GenericEventDrivenStrategy,
    OrderRequest,
    OrderType,
    Side,
    StrategyParams,
)
from .data_feed import BacktestRange, PostgresConfig, PostgresDataFeed


@dataclass(slots=True)
class BacktestSummary:
    total_trades: int
    wins: int
    losses: int
    win_rate: float
    gross_profit: float
    gross_loss: float
    total_fees: float
    net_pnl: float
    avg_trade: float
    profit_factor: float
    max_drawdown: float


@dataclass(slots=True)
class RunConfig:
    dsn: str
    start: datetime
    end: datetime
    symbols: Optional[List[str]] = None
    screener_names: Optional[List[str]] = None
    statuses: Optional[List[str]] = None
    interval: str = "5m"
    exchange_id: Optional[int] = None
    candle_limit: Optional[int] = None
    signal_limit: Optional[int] = None


class SignalDrivenStrategy(GenericEventDrivenStrategy):
    """
    Uses entry/SL/TP directly from signal payload when available.
    Adds context-based filters to keep only stronger signals.
    """

    def on_signal(self, signal, portfolio, broker) -> None:
        if portfolio.has_open_position(signal.symbol):
            return

        entry_price = signal.payload.get("entry_price")
        if entry_price is None or float(entry_price) <= 0:
            return

        ctx = signal.payload.get("context") or {}

        # 1. Volume impulse filter
        vol_ratio = ctx.get("vol_ratio")
        if vol_ratio is not None:
            try:
                if float(vol_ratio) < 2.0:
                    return
            except (TypeError, ValueError):
                pass

        # 2. Liquidation size filter
        liq_long = ctx.get("liq_long_usdt")
        liq_short = ctx.get("liq_short_usdt")
        if liq_long is not None and liq_short is not None:
            try:
                if max(float(liq_long), float(liq_short)) < 20000:
                    return
            except (TypeError, ValueError):
                pass

        # 3. Volatility / volume change filter
        vol_change = ctx.get("vol_change_pct")
        if vol_change is not None:
            try:
                if float(vol_change) < 100:
                    return
            except (TypeError, ValueError):
                pass

        # 4. OI impulse filter
        oi_move = ctx.get("oi_trigger_ret_pct")
        if oi_move is not None:
            try:
                if float(oi_move) < 3:
                    return
            except (TypeError, ValueError):
                pass

        qty = self.params.initial_qty_usdt / float(entry_price)

        broker.submit(
            OrderRequest(
                symbol=signal.symbol,
                side=signal.side,
                order_type=OrderType.MARKET,
                qty=qty,
                tag="entry",
                meta={
                    "fill_price": float(entry_price),
                    "signal_payload": dict(signal.payload),
                    "strategy_id": signal.strategy_id,
                    "screener_name": signal.payload.get("screener_name"),
                    "timeframe": signal.payload.get("timeframe"),
                    "signal_id": signal.payload.get("signal_id"),
                },
            )
        )

    def on_candle(self, candle, portfolio, broker) -> None:
        pos = portfolio.get(candle.symbol)
        if not pos:
            return

        signal_payload = pos.meta.get("signal_payload", {})
        stop_loss = signal_payload.get("stop_loss")
        take_profit = signal_payload.get("take_profit")

        if stop_loss is not None and self._crossed(candle, float(stop_loss)):
            broker.submit(
                OrderRequest(
                    symbol=pos.symbol,
                    side=Side.SHORT if pos.side is Side.LONG else Side.LONG,
                    order_type=OrderType.STOP,
                    qty=pos.qty,
                    tag="signal_stop_loss",
                    reduce_only=True,
                    meta={
                        "fill_price": float(stop_loss),
                        "exit_reason": ExitReason.STOP_LOSS,
                    },
                )
            )
            return

        if take_profit is not None and self._crossed(candle, float(take_profit)):
            broker.submit(
                OrderRequest(
                    symbol=pos.symbol,
                    side=Side.SHORT if pos.side is Side.LONG else Side.LONG,
                    order_type=OrderType.TAKE_PROFIT,
                    qty=pos.qty,
                    tag="signal_take_profit",
                    reduce_only=True,
                    meta={
                        "fill_price": float(take_profit),
                        "exit_reason": ExitReason.TAKE_PROFIT_1,
                    },
                )
            )
            return

        super().on_candle(candle, portfolio, broker)


class BacktestReporter:
    @staticmethod
    def summarize(closed_trades: Sequence[ClosedTrade], equity_curve: Sequence[dict]) -> BacktestSummary:
        total_trades = len(closed_trades)
        wins = sum(1 for t in closed_trades if t.net_pnl > 0)
        losses = sum(1 for t in closed_trades if t.net_pnl < 0)
        gross_profit = sum(t.net_pnl for t in closed_trades if t.net_pnl > 0)
        gross_loss = sum(t.net_pnl for t in closed_trades if t.net_pnl < 0)
        total_fees = sum(t.fees for t in closed_trades)
        net_pnl = sum(t.net_pnl for t in closed_trades)
        avg_trade = net_pnl / total_trades if total_trades else 0.0
        win_rate = wins / total_trades if total_trades else 0.0
        profit_factor = gross_profit / abs(gross_loss) if gross_loss < 0 else 0.0
        max_drawdown = BacktestReporter._max_drawdown(equity_curve)

        return BacktestSummary(
            total_trades=total_trades,
            wins=wins,
            losses=losses,
            win_rate=win_rate,
            gross_profit=gross_profit,
            gross_loss=gross_loss,
            total_fees=total_fees,
            net_pnl=net_pnl,
            avg_trade=avg_trade,
            profit_factor=profit_factor,
            max_drawdown=max_drawdown,
        )

    @staticmethod
    def _max_drawdown(equity_curve: Sequence[dict]) -> float:
        peak = None
        max_dd = 0.0

        for row in equity_curve:
            equity = float(row["equity"])
            if peak is None or equity > peak:
                peak = equity
            dd = peak - equity
            if dd > max_dd:
                max_dd = dd

        return max_dd

    @staticmethod
    def print_summary(summary: BacktestSummary) -> None:
        print("=" * 80)
        print("BACKTEST SUMMARY")
        print("=" * 80)
        print(f"total_trades : {summary.total_trades}")
        print(f"wins         : {summary.wins}")
        print(f"losses       : {summary.losses}")
        print(f"win_rate     : {summary.win_rate:.2%}")
        print(f"gross_profit : {summary.gross_profit:.8f}")
        print(f"gross_loss   : {summary.gross_loss:.8f}")
        print(f"total_fees   : {summary.total_fees:.8f}")
        print(f"net_pnl      : {summary.net_pnl:.8f}")
        print(f"avg_trade    : {summary.avg_trade:.8f}")
        print(f"profit_factor: {summary.profit_factor:.4f}")
        print(f"max_drawdown : {summary.max_drawdown:.8f}")
        print("=" * 80)

    @staticmethod
    def print_breakdown(closed_trades: Sequence[ClosedTrade], *, key_name: str) -> None:
        buckets = {}

        for trade in closed_trades:
            key = trade.meta.get(key_name, "unknown")
            buckets.setdefault(key, []).append(trade)

        if not buckets:
            return

        print(f"\nBREAKDOWN BY {key_name.upper()}")
        print("-" * 80)

        for key, trades in sorted(buckets.items(), key=lambda x: str(x[0])):
            pnl = sum(t.net_pnl for t in trades)
            win_rate = sum(1 for t in trades if t.net_pnl > 0) / len(trades)
            print(f"{key}: trades={len(trades)}, pnl={pnl:.8f}, win_rate={win_rate:.2%}")


class BacktestRunner:
    def __init__(self, config: RunConfig):
        self.config = config
        self.feed = PostgresDataFeed(PostgresConfig(dsn=config.dsn))

    def run(self) -> BacktestSummary:
        time_range = BacktestRange(start=self.config.start, end=self.config.end)

        candles = self.feed.load_candles(
            time_range=time_range,
            symbols=self.config.symbols,
            interval=self.config.interval,
            exchange_id=self.config.exchange_id,
            limit=self.config.candle_limit,
        )

        signal_provider = self.feed.build_signal_provider(
            time_range=time_range,
            symbols=self.config.symbols,
            strategy_ids=self.config.screener_names,
            exchange_id=self.config.exchange_id,
            statuses=self.config.statuses,
            timeframes=[self.config.interval] if self.config.interval else None,
            limit=self.config.signal_limit,
        )

        params = StrategyParams()
        strategy = SignalDrivenStrategy(params)
        engine = BacktestEngine(
            strategy=strategy,
            signal_provider=signal_provider,
            params=params,
        )
        portfolio = engine.run(candles)

        summary = BacktestReporter.summarize(
            portfolio.closed_trades,
            portfolio.equity_curve,
        )
        BacktestReporter.print_summary(summary)
        BacktestReporter.print_breakdown(portfolio.closed_trades, key_name="screener_name")
        BacktestReporter.print_breakdown(portfolio.closed_trades, key_name="timeframe")
        return summary


def _parse_csv(value: Optional[str]) -> Optional[List[str]]:
    if not value:
        return None
    items = [item.strip() for item in value.split(",") if item.strip()]
    return items or None


def _parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Postgres-based backtest")
    parser.add_argument("--dsn", default=os.getenv("BACKTEST_DSN"), help="Postgres DSN")
    parser.add_argument("--start", required=True, help="Start datetime, e.g. 2026-03-01T00:00:00+00:00")
    parser.add_argument("--end", required=True, help="End datetime, e.g. 2026-03-26T23:59:59+00:00")
    parser.add_argument("--symbols", help="Comma-separated symbols, e.g. BTCUSDT,ETHUSDT")
    parser.add_argument("--screeners", help="Comma-separated screener names")
    parser.add_argument("--statuses", help="Comma-separated signal statuses, e.g. TAKEN or TAKEN,EXPIRED")
    parser.add_argument("--interval", default="5m", help="Candle interval, default 5m")
    parser.add_argument("--exchange-id", type=int, help="Optional exchange_id")
    parser.add_argument("--candle-limit", type=int, help="Optional LIMIT for candles")
    parser.add_argument("--signal-limit", type=int, help="Optional LIMIT for signals")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    if not args.dsn:
        raise SystemExit("DSN is required. Pass --dsn or set BACKTEST_DSN")

    config = RunConfig(
        dsn=args.dsn,
        start=_parse_dt(args.start),
        end=_parse_dt(args.end),
        symbols=_parse_csv(args.symbols),
        screener_names=_parse_csv(args.screeners),
        statuses=_parse_csv(args.statuses),
        interval=args.interval,
        exchange_id=args.exchange_id,
        candle_limit=args.candle_limit,
        signal_limit=args.signal_limit,
    )

    BacktestRunner(config).run()


if __name__ == "__main__":
    main()