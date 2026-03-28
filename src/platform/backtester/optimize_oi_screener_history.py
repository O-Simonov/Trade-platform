# src/platform/backtester/optimize_oi_screener_history.py
from __future__ import annotations

import argparse
import csv
import itertools
import math
import os
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import yaml

from src.platform.backtester.core import (
    BacktestEngine,
    ClosedTrade,
    ExitReason,
    GenericEventDrivenStrategy,
    OrderRequest,
    OrderType,
    Side,
    SignalEvent,
    StrategyParams,
)
from src.platform.backtester.data_feed import BacktestRange, PostgresConfig, PostgresDataFeed
from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage
from src.platform.screeners.scr_oi_binance import ScrOiBinance


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
class OptimizationRow:
    rank_score: float
    interval: str
    params: Dict[str, Any]
    generated_signals: int
    summary: BacktestSummary


class GeneratedSignalProvider:
    def __init__(self, events: Iterable[SignalEvent]):
        self._events_by_key: Dict[tuple, List[SignalEvent]] = {}
        for e in events:
            self._events_by_key.setdefault((e.symbol, e.ts), []).append(e)

    def events_for_candle(self, candle):
        return self._events_by_key.get((candle.symbol, candle.ts), [])


class SignalOnlyStrategy(GenericEventDrivenStrategy):
    """
    Strict signal-based execution:
      - entry from generated screener signal
      - stop/take from signal payload
      - no averaging / no trailing / no partial TP
    """

    def on_signal(self, signal, portfolio, broker) -> None:
        if portfolio.has_open_position(signal.symbol):
            return

        entry_price = signal.payload.get("entry_price")
        if entry_price is None:
            return
        try:
            entry_price = float(entry_price)
        except (TypeError, ValueError):
            return
        if entry_price <= 0:
            return

        qty = self.params.initial_qty_usdt / entry_price
        broker.submit(
            OrderRequest(
                symbol=signal.symbol,
                side=signal.side,
                order_type=OrderType.MARKET,
                qty=qty,
                tag="entry",
                meta={
                    "fill_price": entry_price,
                    "signal_payload": dict(signal.payload),
                    "strategy_id": signal.strategy_id,
                    "screener_name": signal.payload.get("screener_name"),
                    "timeframe": signal.payload.get("timeframe"),
                },
            )
        )

    def on_candle(self, candle, portfolio, broker) -> None:
        pos = portfolio.get(candle.symbol)
        if not pos:
            return

        payload = pos.meta.get("signal_payload", {})
        stop_loss = payload.get("stop_loss")
        take_profit = payload.get("take_profit")

        if stop_loss is not None and self._crossed(candle, float(stop_loss)):
            broker.submit(
                OrderRequest(
                    symbol=pos.symbol,
                    side=Side.SHORT if pos.side is Side.LONG else Side.LONG,
                    order_type=OrderType.STOP,
                    qty=pos.qty,
                    tag="signal_stop_loss",
                    reduce_only=True,
                    meta={"fill_price": float(stop_loss), "exit_reason": ExitReason.STOP_LOSS},
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
                    meta={"fill_price": float(take_profit), "exit_reason": ExitReason.TAKE_PROFIT_1},
                )
            )
            return


class Reporter:
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

        if gross_loss < 0:
            profit_factor = gross_profit / abs(gross_loss)
        elif gross_profit > 0:
            profit_factor = float("inf")
        else:
            profit_factor = 0.0

        max_drawdown = Reporter._max_drawdown(equity_curve)

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
            drawdown = peak - equity
            if drawdown > max_dd:
                max_dd = drawdown
        return max_dd


class HistoryOptimizer:
    def __init__(self, *, dsn: str, pool_dsn: str, exchange_id: int):
        self.dsn = dsn
        self.pool_dsn = pool_dsn
        self.exchange_id = exchange_id

        self.feed = PostgresDataFeed(PostgresConfig(dsn=dsn))
        self.pool = create_pool(pool_dsn)
        self.storage = PostgreSQLStorage(self.pool)

        self.screener = ScrOiBinance()

    def close(self) -> None:
        try:
            self.pool.close()
        except Exception:
            pass

    def run_case(
        self,
        *,
        start: datetime,
        end: datetime,
        interval: str,
        screener_params: Dict[str, Any],
    ) -> Tuple[int, BacktestSummary]:
        params = dict(screener_params)
        params["exchange_id"] = self.exchange_id
        params["interval"] = interval
        params["intervals"] = [interval]

        generated = self.screener.run(
            storage=self.storage,
            exchange_id=self.exchange_id,
            interval=interval,
            params=params,
        )
        signal_events = self._to_signal_events(generated, interval=interval)
        signal_provider = GeneratedSignalProvider(signal_events)

        candles = self.feed.load_candles(
            time_range=BacktestRange(start=start, end=end),
            interval=interval,
            exchange_id=self.exchange_id,
        )

        strategy = SignalOnlyStrategy(
            StrategyParams(
                averaging_enabled=False,
                trailing_enabled=False,
                main_partial_tp_enabled=False,
            )
        )
        engine = BacktestEngine(strategy=strategy, signal_provider=signal_provider, params=strategy.params)
        portfolio = engine.run(candles)

        return len(signal_events), Reporter.summarize(portfolio.closed_trades, portfolio.equity_curve)

    def _to_signal_events(self, signals: Sequence[Any], *, interval: str) -> List[SignalEvent]:
        out: List[SignalEvent] = []
        for s in signals:
            side = Side.LONG if str(s.side).upper() == "BUY" else Side.SHORT
            payload = {
                "entry_price": s.entry_price,
                "stop_loss": s.stop_loss,
                "take_profit": s.take_profit,
                "confidence": s.confidence,
                "score": s.score,
                "reason": s.reason,
                "context": s.context or {},
                "screener_name": self.screener.name,
                "timeframe": interval,
            }
            out.append(
                SignalEvent(
                    ts=s.signal_ts,
                    symbol=s.symbol,
                    strategy_id=self.screener.name,
                    side=side,
                    payload=payload,
                )
            )
        return out

    def grid_search(
        self,
        *,
        start: datetime,
        end: datetime,
        interval: str,
        base_params: Dict[str, Any],
        grid: Dict[str, Sequence[Any]],
        min_trades: int,
        top_n: int,
        progress_every: int = 10,
    ) -> List[OptimizationRow]:
        keys = list(grid.keys())
        combinations = list(itertools.product(*(grid[k] for k in keys)))
        total = len(combinations)

        rows: List[OptimizationRow] = []
        best_row: Optional[OptimizationRow] = None
        t0 = time.perf_counter()

        print("=" * 140)
        print(f"START GRID SEARCH | interval={interval} | combinations={total} | min_trades={min_trades} | top_n={top_n}")
        print("=" * 140)

        for i, values in enumerate(combinations, start=1):
            tuned = dict(base_params)
            tuned.update(dict(zip(keys, values)))

            combo_started = time.perf_counter()
            signal_count, summary = self.run_case(
                start=start,
                end=end,
                interval=interval,
                screener_params=tuned,
            )
            combo_elapsed = time.perf_counter() - combo_started

            if summary.total_trades >= min_trades:
                score = self._rank(summary)
                row = OptimizationRow(
                    rank_score=score,
                    interval=interval,
                    params={k: tuned[k] for k in keys},
                    generated_signals=signal_count,
                    summary=summary,
                )
                rows.append(row)

                if best_row is None or row.rank_score > best_row.rank_score:
                    best_row = row
                    pf = "inf" if math.isinf(row.summary.profit_factor) else f"{row.summary.profit_factor:.4f}"
                    print(
                        f"[BEST {i}/{total}] "
                        f"pnl={row.summary.net_pnl:.8f} pf={pf} wr={row.summary.win_rate:.2%} "
                        f"trades={row.summary.total_trades} dd={row.summary.max_drawdown:.8f} "
                        f"signals={row.generated_signals} params={row.params}"
                    )

            if i == 1 or i % progress_every == 0 or i == total:
                elapsed = time.perf_counter() - t0
                avg_sec = elapsed / i
                remaining = max(0.0, (total - i) * avg_sec)
                best_pnl = best_row.summary.net_pnl if best_row else 0.0
                best_pf = (
                    "inf"
                    if best_row and math.isinf(best_row.summary.profit_factor)
                    else (f"{best_row.summary.profit_factor:.4f}" if best_row else "n/a")
                )
                print(
                    f"[{i}/{total}] "
                    f"elapsed={elapsed:.1f}s avg={avg_sec:.2f}s/comb last={combo_elapsed:.2f}s "
                    f"accepted={len(rows)} best_pnl={best_pnl:.8f} best_pf={best_pf} "
                    f"eta={remaining:.1f}s"
                )

        rows.sort(key=lambda r: (r.rank_score, r.summary.net_pnl, r.summary.profit_factor), reverse=True)
        return rows[:top_n]

    @staticmethod
    def _rank(summary: BacktestSummary) -> float:
        pf = 10.0 if math.isinf(summary.profit_factor) else summary.profit_factor
        return (
            summary.net_pnl * 1.0
            + pf * 12.0
            + summary.win_rate * 5.0
            - summary.max_drawdown * 0.35
            + summary.total_trades * 0.05
        )


def load_yaml_params(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    screeners = cfg.get("screeners") or []
    for item in screeners:
        if item.get("name") == "scr_oi_binance":
            return dict(item.get("params") or {})
    raise ValueError("scr_oi_binance not found in YAML")


def apply_interval_overrides(params: Dict[str, Any], interval: str) -> Dict[str, Any]:
    out = dict(params)
    overrides = dict((params.get("per_interval_overrides") or {}).get(interval, {}) or {})
    out.update(overrides)
    out.pop("per_interval_overrides", None)
    out.pop("intervals", None)
    return out


def parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def parse_num_list(value: str) -> List[Any]:
    out: List[Any] = []
    for item in value.split(","):
        s = item.strip().lower()
        if not s:
            continue
        if s in {"true", "false"}:
            out.append(s == "true")
        elif s in {"none", "null", "off"}:
            out.append(None)
        else:
            try:
                x = float(s)
                if x.is_integer():
                    out.append(int(x))
                else:
                    out.append(x)
            except ValueError:
                out.append(item.strip())
    return out


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="True historical optimizer: regenerate screener signals from history, then backtest.")
    p.add_argument("--dsn", default=os.getenv("BACKTEST_DSN"), help="SQLAlchemy DSN, e.g. postgresql+psycopg2://...")
    p.add_argument("--pool-dsn", default=os.getenv("BACKTEST_POOL_DSN"), help="psycopg pool conninfo, e.g. dbname=... user=... password=... host=... port=...")
    p.add_argument("--yaml", required=True, help="Path to screener_oi.yaml")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--interval", required=True, help="5m or 15m")
    p.add_argument("--exchange-id", type=int, default=1)
    p.add_argument("--min-trades", type=int, default=10)
    p.add_argument("--top-n", type=int, default=20)
    p.add_argument("--progress-every", type=int, default=10, help="Print progress every N combinations")
    p.add_argument("--csv-out", help="Optional path to save all top results")

    p.add_argument("--buy-oi-rise-pct", default="2.0,2.5,3.0,3.5,4.0")
    p.add_argument("--oi-value-confirm-pct", default="1.0,1.5,2.0,2.5,3.0")
    p.add_argument("--buy-volume-mult", default="1.5,2.0,2.5,3.0")
    p.add_argument("--buy-volume-spike-mult", default="2.5,3.0,3.5,4.0,5.0")
    p.add_argument("--buy-price-rise-pct", default="0.5,1.0,1.5,2.0")
    p.add_argument("--buy-min-green-candles", default="1,2,3")
    p.add_argument("--price-sideways-max-pct", default="3.0,5.0,7.0")
    p.add_argument("--price-downtrend-min-pct", default="0.3,0.5,1.0")
    p.add_argument("--base-volume-max-mult", default="2.0,3.0,5.0")
    p.add_argument("--stop-loss-pct", default="1.5,2.0,2.5")
    p.add_argument("--take-profit-pct", default="4.0,5.0,6.0,7.0")
    p.add_argument("--buy-require-last-green", default="true")
    p.add_argument("--enable-funding", default="true")
    p.add_argument("--buy-require-funding-decreasing", default="true,false")
    p.add_argument("--buy-funding-max-pct", default="-0.02,-0.01,-0.001,-0.0001")
    return p


def print_results(rows: Sequence[OptimizationRow]) -> None:
    print("=" * 160)
    print("TOP TRUE-HISTORY OPTIMIZATION RESULTS")
    print("=" * 160)
    for i, row in enumerate(rows, start=1):
        s = row.summary
        pf = "inf" if math.isinf(s.profit_factor) else f"{s.profit_factor:.4f}"
        print(
            f"#{i:02d} rank={row.rank_score:.4f} interval={row.interval} signals={row.generated_signals} "
            f"trades={s.total_trades} pnl={s.net_pnl:.8f} pf={pf} wr={s.win_rate:.2%} "
            f"dd={s.max_drawdown:.8f} params={row.params}"
        )
    print("=" * 160)


def save_csv(path: str, rows: Sequence[OptimizationRow]) -> None:
    fieldnames = [
        "rank_score", "interval", "generated_signals", "total_trades", "wins", "losses", "win_rate",
        "gross_profit", "gross_loss", "total_fees", "net_pnl", "avg_trade", "profit_factor", "max_drawdown",
    ]
    param_keys = sorted({k for row in rows for k in row.params.keys()})
    fieldnames.extend(param_keys)

    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            summary = asdict(row.summary)
            base = {
                "rank_score": row.rank_score,
                "interval": row.interval,
                "generated_signals": row.generated_signals,
                **summary,
            }
            for key in param_keys:
                base[key] = row.params.get(key)
            writer.writerow(base)


def main() -> None:
    args = build_parser().parse_args()
    if not args.dsn:
        raise SystemExit("SQLAlchemy DSN is required. Pass --dsn or set BACKTEST_DSN")
    if not args.pool_dsn:
        raise SystemExit("Pool DSN is required. Pass --pool-dsn or set BACKTEST_POOL_DSN")

    base_params = apply_interval_overrides(load_yaml_params(args.yaml), args.interval)
    optimizer = HistoryOptimizer(
        dsn=args.dsn,
        pool_dsn=args.pool_dsn,
        exchange_id=args.exchange_id,
    )

    try:
        grid = {
            "buy_oi_rise_pct": parse_num_list(args.buy_oi_rise_pct),
            "oi_value_confirm_pct": parse_num_list(args.oi_value_confirm_pct),
            "buy_volume_mult": parse_num_list(args.buy_volume_mult),
            "buy_volume_spike_mult": parse_num_list(args.buy_volume_spike_mult),
            "buy_price_rise_pct": parse_num_list(args.buy_price_rise_pct),
            "buy_min_green_candles": parse_num_list(args.buy_min_green_candles),
            "price_sideways-max_pct": parse_num_list(args.price_sideways_max_pct) if False else parse_num_list(args.price_sideways_max_pct),
            "price_downtrend_min_pct": parse_num_list(args.price_downtrend_min_pct),
            "base_volume_max_mult": parse_num_list(args.base_volume_max_mult),
            "stop_loss_pct": parse_num_list(args.stop_loss_pct),
            "take_profit_pct": parse_num_list(args.take_profit_pct),
            "buy_require_last_green": parse_num_list(args.buy_require_last_green),
            "enable_funding": parse_num_list(args.enable_funding),
            "buy_require_funding_decreasing": parse_num_list(args.buy_require_funding_decreasing),
            "buy_funding_max_pct": parse_num_list(args.buy_funding_max_pct),
        }

        # fix accidental key spelling from old edits
        grid["price_sideways_max_pct"] = grid.pop("price_sideways-max_pct")

        rows = optimizer.grid_search(
            start=parse_dt(args.start),
            end=parse_dt(args.end),
            interval=args.interval,
            base_params=base_params,
            grid=grid,
            min_trades=args.min_trades,
            top_n=args.top_n,
            progress_every=max(1, int(args.progress_every)),
        )
        print_results(rows)
        if args.csv_out:
            save_csv(args.csv_out, rows)
    finally:
        optimizer.close()


if __name__ == "__main__":
    main()