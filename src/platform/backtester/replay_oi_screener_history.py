# src/platform/backtester/replay_oi_screener_history.py
from __future__ import annotations

import os

import argparse
import math
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import yaml
from sqlalchemy import text

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
from src.platform.screeners import scr_oi_binance as oi_module
from src.platform.screeners.scr_oi_binance import ScrOiBinance


@dataclass(slots=True)
class ReplaySummary:
    replay_points: int
    generated_signals: int
    deduped_signals: int
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


class GeneratedSignalProvider:
    def __init__(self, events: Iterable[SignalEvent]):
        self._events_by_key: Dict[tuple, List[SignalEvent]] = {}
        for e in events:
            self._events_by_key.setdefault((e.symbol, e.ts), []).append(e)

    def events_for_candle(self, candle):
        return self._events_by_key.get((candle.symbol, candle.ts), [])


class SignalOnlyStrategy(GenericEventDrivenStrategy):
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
    def summarize(
        replay_points: int,
        generated_signals: int,
        deduped_signals: int,
        closed_trades: Sequence[ClosedTrade],
        equity_curve: Sequence[dict],
    ) -> ReplaySummary:
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
        return ReplaySummary(
            replay_points=replay_points,
            generated_signals=generated_signals,
            deduped_signals=deduped_signals,
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
    def print_summary(summary: ReplaySummary) -> None:
        pf = "inf" if math.isinf(summary.profit_factor) else f"{summary.profit_factor:.4f}"
        print("=" * 100)
        print("OI HISTORICAL REPLAY SUMMARY")
        print("=" * 100)
        print(f"replay_points   : {summary.replay_points}")
        print(f"generated_signals: {summary.generated_signals}")
        print(f"deduped_signals : {summary.deduped_signals}")
        print(f"total_trades    : {summary.total_trades}")
        print(f"wins            : {summary.wins}")
        print(f"losses          : {summary.losses}")
        print(f"win_rate        : {summary.win_rate:.2%}")
        print(f"gross_profit    : {summary.gross_profit:.8f}")
        print(f"gross_loss      : {summary.gross_loss:.8f}")
        print(f"total_fees      : {summary.total_fees:.8f}")
        print(f"net_pnl         : {summary.net_pnl:.8f}")
        print(f"avg_trade       : {summary.avg_trade:.8f}")
        print(f"profit_factor   : {pf}")
        print(f"max_drawdown    : {summary.max_drawdown:.8f}")
        print("=" * 100)


@contextmanager
def patched_utc_now(fake_now: datetime):
    original = oi_module._utc_now
    oi_module._utc_now = lambda: fake_now
    try:
        yield
    finally:
        oi_module._utc_now = original


class OiHistoricalReplayRunner:
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

    def replay(
        self,
        *,
        start: datetime,
        end: datetime,
        interval: str,
        screener_params: Dict[str, Any],
        progress_every: int = 50,
        max_points: Optional[int] = None,
    ) -> Tuple[ReplaySummary, List[SignalEvent]]:
        replay_points = self._load_replay_timestamps(start=start, end=end, interval=interval)
        if max_points is not None:
            replay_points = replay_points[: max(0, int(max_points))]

        total_points = len(replay_points)
        print("=" * 120)
        print(f"START HISTORICAL REPLAY | interval={interval} | points={total_points}")
        print("=" * 120)

        params = dict(screener_params)
        params["exchange_id"] = self.exchange_id
        params["interval"] = interval
        params["intervals"] = [interval]

        raw_events: List[SignalEvent] = []
        seen_keys: set[tuple] = set()
        deduped_events: List[SignalEvent] = []

        for i, ts in enumerate(replay_points, start=1):
            with patched_utc_now(ts):
                generated = self.screener.run(
                    storage=self.storage,
                    exchange_id=self.exchange_id,
                    interval=interval,
                    params=params,
                )

            current_events = self._to_signal_events(generated, interval=interval)
            raw_events.extend(current_events)

            for e in current_events:
                key = (e.symbol, e.ts, e.side.value)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                deduped_events.append(e)

            if i == 1 or i % progress_every == 0 or i == total_points:
                print(
                    f"[{i}/{total_points}] ts={ts.isoformat()} generated_now={len(current_events)} "
                    f"generated_total={len(raw_events)} deduped_total={len(deduped_events)}"
                )

        signal_provider = GeneratedSignalProvider(deduped_events)
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

        summary = Reporter.summarize(
            replay_points=total_points,
            generated_signals=len(raw_events),
            deduped_signals=len(deduped_events),
            closed_trades=portfolio.closed_trades,
            equity_curve=portfolio.equity_curve,
        )
        return summary, deduped_events

    def _load_replay_timestamps(self, *, start: datetime, end: datetime, interval: str) -> List[datetime]:
        sql = text(
            """
            SELECT DISTINCT c.open_time AS ts
            FROM candles c
            WHERE c.exchange_id = :exchange_id
              AND c.interval = :interval_value
              AND c.open_time >= :start_ts
              AND c.open_time <= :end_ts
            ORDER BY c.open_time ASC
            """
        )
        with self.feed.engine.begin() as conn:
            rows = conn.execute(
                sql,
                {
                    "exchange_id": self.exchange_id,
                    "interval_value": interval,
                    "start_ts": start,
                    "end_ts": end,
                },
            ).mappings().all()
        return [row["ts"] for row in rows]

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


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Replay OI screener through historical timestamps, then backtest the generated signals.")
    p.add_argument("--dsn", default=os.getenv("BACKTEST_DSN"), help="SQLAlchemy DSN")
    p.add_argument("--pool-dsn", default=os.getenv("BACKTEST_POOL_DSN"), help="psycopg pool conninfo")
    p.add_argument("--yaml", required=True, help="Path to screener_oi.yaml")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--interval", required=True, help="5m or 15m")
    p.add_argument("--exchange-id", type=int, default=1)
    p.add_argument("--progress-every", type=int, default=50)
    p.add_argument("--max-points", type=int, help="Optional cap for replay timestamps during debugging")
    return p


def main() -> None:
    args = build_parser().parse_args()
    if not args.dsn:
        raise SystemExit("SQLAlchemy DSN is required. Pass --dsn or set BACKTEST_DSN")
    if not args.pool_dsn:
        raise SystemExit("Pool DSN is required. Pass --pool-dsn or set BACKTEST_POOL_DSN")

    params = apply_interval_overrides(load_yaml_params(args.yaml), args.interval)
    runner = OiHistoricalReplayRunner(
        dsn=args.dsn,
        pool_dsn=args.pool_dsn,
        exchange_id=args.exchange_id,
    )
    try:
        summary, _events = runner.replay(
            start=parse_dt(args.start),
            end=parse_dt(args.end),
            interval=args.interval,
            screener_params=params,
            progress_every=max(1, int(args.progress_every)),
            max_points=args.max_points,
        )
        Reporter.print_summary(summary)
    finally:
        runner.close()


if __name__ == "__main__":
    main()