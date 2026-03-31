from __future__ import annotations

import argparse
import math
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Sequence, Set, Tuple

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
from src.platform.screeners.scr_liquidation_binance import ScrLiquidationBinance


def _print(msg: str = "") -> None:
    print(msg, flush=True)


def _fmt_duration(seconds: float) -> str:
    seconds = max(0.0, float(seconds or 0.0))
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, sec = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m {sec:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m {sec:02d}s"


def _progress_bar(done: int, total: int, *, width: int = 24) -> str:
    total = max(1, int(total or 1))
    done = max(0, min(int(done), total))
    filled = int(width * done / total)
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


def _print_progress(prefix: str, done: int, total: int, *, start_perf: float, extra: str = "") -> None:
    now = time.perf_counter()
    elapsed = max(0.0, now - start_perf)
    avg = elapsed / done if done > 0 else 0.0
    remaining = max(0.0, (total - done) * avg)
    pct = (done / max(1, total)) * 100.0
    bar = _progress_bar(done, total)
    suffix = f" | {extra}" if extra else ""
    _print(f"{prefix} {bar} {done}/{total} ({pct:5.1f}%) | elapsed={_fmt_duration(elapsed)} | eta={_fmt_duration(remaining)}{suffix}")


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
        _print("=" * 100)
        _print("LIQUIDATION HISTORICAL REPLAY SUMMARY")
        _print("=" * 100)
        _print(f"replay_points    : {summary.replay_points}")
        _print(f"generated_signals: {summary.generated_signals}")
        _print(f"deduped_signals  : {summary.deduped_signals}")
        _print(f"total_trades     : {summary.total_trades}")
        _print(f"wins             : {summary.wins}")
        _print(f"losses           : {summary.losses}")
        _print(f"win_rate         : {summary.win_rate:.2%}")
        _print(f"gross_profit     : {summary.gross_profit:.8f}")
        _print(f"gross_loss       : {summary.gross_loss:.8f}")
        _print(f"total_fees       : {summary.total_fees:.8f}")
        _print(f"net_pnl          : {summary.net_pnl:.8f}")
        _print(f"avg_trade        : {summary.avg_trade:.8f}")
        _print(f"profit_factor    : {pf}")
        _print(f"max_drawdown     : {summary.max_drawdown:.8f}")
        _print("=" * 100)


class LiquidationHistoricalReplayRunner:
    def __init__(self, *, dsn: str, pool_dsn: str, exchange_id: int):
        self.dsn = dsn
        self.pool_dsn = pool_dsn
        self.exchange_id = exchange_id
        self.feed = PostgresDataFeed(PostgresConfig(dsn=dsn))
        self.pool = create_pool(pool_dsn)
        self.storage = PostgreSQLStorage(self.pool)
        self.screener = ScrLiquidationBinance()

    def close(self) -> None:
        try:
            self.pool.close()
        except Exception:
            pass

    @staticmethod
    def _parse_interval(interval: str) -> timedelta:
        s = str(interval).strip().lower()
        if s.endswith("m"):
            return timedelta(minutes=int(s[:-1]))
        if s.endswith("h"):
            return timedelta(hours=int(s[:-1]))
        if s.endswith("d"):
            return timedelta(days=int(s[:-1]))
        return timedelta(hours=1)

    @staticmethod
    def _ts_key(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.replace(microsecond=0)

    def replay(
        self,
        *,
        start: datetime,
        end: datetime,
        interval: str,
        screener_params: Dict[str, Any],
        progress_every: int = 50,
        max_points: Optional[int] = None,
        dedupe_by_symbol_side: bool = True,
        replay_mode: str = "fast",
        use_candidate_timestamps: bool = True,
        use_candidate_symbols: bool = True,
        candidate_liq_multiplier: float = 0.35,
    ) -> Tuple[ReplaySummary, List[SignalEvent]]:
        params = dict(screener_params)
        params["exchange_id"] = self.exchange_id
        params["interval"] = interval
        params["intervals"] = [interval]

        _print(f"Preparing preload context | interval={interval} | mode={replay_mode} ...")
        preload = self._build_preload_context(
            start=start,
            end=end,
            interval=interval,
            screener_params=params,
            replay_mode=replay_mode,
            use_candidate_timestamps=use_candidate_timestamps,
            use_candidate_symbols=use_candidate_symbols,
            candidate_liq_multiplier=candidate_liq_multiplier,
        )

        replay_points = preload["replay_points"]
        if max_points is not None:
            replay_points = replay_points[: max(0, int(max_points))]

        total_points = len(replay_points)
        total_symbols = len(preload["symbols"])
        _print(f"Preload ready | symbols={total_symbols} | replay_points={total_points} | candidate_timestamps={use_candidate_timestamps} | candidate_symbols={use_candidate_symbols}")
        _print("=" * 120)
        _print(f"START LIQUIDATION HISTORICAL REPLAY | interval={interval} | mode={replay_mode} | points={total_points}")
        _print("=" * 120)

        raw_events: List[SignalEvent] = []
        seen_keys: set[tuple] = set()
        deduped_events: List[SignalEvent] = []
        replay_started = time.perf_counter()

        common_ctx = {
            "symbols": preload["symbols"],
            "candles_by_symbol_interval": preload["candles_by_symbol_interval"],
            "candle_ts_index": preload["candle_ts_index"],
            "liq_by_symbol_bucket": preload["liq_by_symbol_bucket"],
            "liq_history_by_symbol_interval": preload["liq_history_by_symbol_interval"],
            "funding_by_symbol": preload["funding_by_symbol"],
            "run_mode": replay_mode,
        }

        for i, ts in enumerate(replay_points, start=1):
            params["as_of_ts"] = ts
            candidate_symbols = preload["candidate_symbols_by_ts"].get(self._ts_key(ts), set()) if use_candidate_symbols else set()
            self.screener.set_runtime_context({**common_ctx, "candidate_symbol_ids": tuple(candidate_symbols)})
            generated = self.screener.run(
                storage=self.storage,
                exchange_id=self.exchange_id,
                interval=interval,
                params=params,
            )

            current_events = self._to_signal_events(generated, interval=interval)
            raw_events.extend(current_events)

            for e in current_events:
                key = (e.symbol, e.ts, e.side.value) if dedupe_by_symbol_side else (e.symbol, e.ts, e.side.value, e.payload.get("reason"))
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                deduped_events.append(e)

            if i == 1 or i % progress_every == 0 or i == total_points:
                _print_progress(
                    "REPLAY",
                    i,
                    total_points,
                    start_perf=replay_started,
                    extra=(
                        f"ts={ts.isoformat()} | candidates={len(candidate_symbols) if candidate_symbols else len(preload['symbols'])} "
                        f"| generated_now={len(current_events)} | generated_total={len(raw_events)} | deduped_total={len(deduped_events)}"
                    ),
                )

        self.screener.clear_runtime_context()

        _print(f"Replay pass finished | raw_events={len(raw_events)} | deduped_events={len(deduped_events)}")
        _print("Loading candles for backtest ...")
        signal_provider = GeneratedSignalProvider(deduped_events)
        candles = self.feed.load_candles(
            time_range=BacktestRange(start=start, end=end),
            interval=interval,
            exchange_id=self.exchange_id,
        )

        _print(f"Loaded candles for backtest: {len(candles)}")
        strategy = SignalOnlyStrategy(
            StrategyParams(
                averaging_enabled=False,
                trailing_enabled=False,
                main_partial_tp_enabled=False,
            )
        )
        engine = BacktestEngine(strategy=strategy, signal_provider=signal_provider, params=strategy.params)
        _print("Running portfolio backtest ...")
        portfolio = engine.run(candles)
        _print("Backtest finished")

        summary = Reporter.summarize(
            replay_points=total_points,
            generated_signals=len(raw_events),
            deduped_signals=len(deduped_events),
            closed_trades=portfolio.closed_trades,
            equity_curve=portfolio.equity_curve,
        )
        return summary, deduped_events

    def _build_preload_context(
        self,
        *,
        start: datetime,
        end: datetime,
        interval: str,
        screener_params: Dict[str, Any],
        replay_mode: str,
        use_candidate_timestamps: bool,
        use_candidate_symbols: bool,
        candidate_liq_multiplier: float,
    ) -> Dict[str, Any]:
        _print("[preload] loading symbols ...")
        symbols = self._load_symbols()
        symbol_ids = [int(x["symbol_id"]) for x in symbols]
        if not symbol_ids:
            _print(
            f"[preload] done | candle_groups={len(candles_by_symbol_interval)} | funding_symbols={len(funding_by_symbol)} | liq_buckets={len(liq_by_symbol_bucket)} | replay_points={len(replay_points)}"
        )
        return {
                "symbols": [],
                "candles_by_symbol_interval": {},
                "candle_ts_index": {},
                "liq_by_symbol_bucket": {},
                "liq_history_by_symbol_interval": {},
                "funding_by_symbol": {},
                "candidate_symbols_by_ts": defaultdict(set),
                "replay_points": [],
            }

        needed_intervals = {str(interval)}
        if screener_params.get("require_htf_level_overlap"):
            needed_intervals.add(str(screener_params.get("htf_level_interval") or "15m"))

        period_levels = int(screener_params.get("period_levels") or 0)
        volume_avg_window = int(screener_params.get("volume_avg_window") or 0)
        confirm_lookforward = int(screener_params.get("confirm_lookforward") or 0)
        max_anchor_candidates = int(screener_params.get("max_anchor_candidates") or 1)
        lookback_bars = max(320, period_levels + volume_avg_window + confirm_lookforward + max_anchor_candidates + 90)
        candle_pad = self._parse_interval(interval) * lookback_bars

        _print(f"[preload] loading candles batch | symbols={len(symbol_ids)} | intervals={sorted(needed_intervals)} ...")
        candles_by_symbol_interval, candle_ts_index = self._preload_candles(
            symbol_ids=symbol_ids,
            intervals=sorted(needed_intervals),
            start=start - candle_pad,
            end=end,
        )
        _print("[preload] loading funding batch ...")
        funding_by_symbol = self._preload_funding(symbol_ids=symbol_ids, start=start - timedelta(days=14), end=end + timedelta(days=1))
        _print("[preload] loading liquidation batch ...")
        liq_by_symbol_bucket, candidate_symbols_by_ts = self._preload_liquidations(
            symbol_ids=symbol_ids,
            start=start - candle_pad,
            end=end + self._parse_interval(interval),
            interval=interval,
            soft_threshold=float(screener_params.get("volume_liquid_limit") or 0.0) * max(0.0, candidate_liq_multiplier),
        )
        _print("[preload] building liquidation histories ...")
        liq_history_by_symbol_interval = self._build_liq_histories(
            liq_by_symbol_bucket=liq_by_symbol_bucket,
            interval=interval,
            lookback=max(0, int(screener_params.get("liq_relative_window") or 0)),
        )
        _print("[preload] building replay timestamps ...")
        replay_points = self._load_replay_timestamps(
            start=start,
            end=end,
            interval=interval,
            use_candidates=use_candidate_timestamps,
            candidate_symbols_by_ts=candidate_symbols_by_ts,
        )
        if not use_candidate_symbols:
            candidate_symbols_by_ts = defaultdict(set)

        return {
            "symbols": symbols,
            "candles_by_symbol_interval": candles_by_symbol_interval,
            "candle_ts_index": candle_ts_index,
            "liq_by_symbol_bucket": liq_by_symbol_bucket,
            "liq_history_by_symbol_interval": liq_history_by_symbol_interval,
            "funding_by_symbol": funding_by_symbol,
            "candidate_symbols_by_ts": candidate_symbols_by_ts,
            "replay_points": replay_points,
        }

    def _load_symbols(self) -> List[Dict[str, Any]]:
        sql = text(
            """
            SELECT symbol_id, symbol
            FROM symbols
            WHERE exchange_id = :exchange_id AND is_active = true
            ORDER BY symbol_id ASC
            """
        )
        with self.feed.engine.begin() as conn:
            rows = conn.execute(sql, {"exchange_id": self.exchange_id}).mappings().all()
        return [{"symbol_id": int(r["symbol_id"]), "symbol": str(r["symbol"])} for r in rows]

    def _preload_candles(
        self,
        *,
        symbol_ids: Sequence[int],
        intervals: Sequence[str],
        start: datetime,
        end: datetime,
    ) -> Tuple[Dict[Tuple[int, str], List[Dict[str, Any]]], Dict[Tuple[int, str], List[datetime]]]:
        sql = text(
            """
            SELECT symbol_id, interval, open_time AS ts, open, high, low, close, volume, quote_volume
            FROM candles
            WHERE exchange_id = :exchange_id
              AND symbol_id = ANY(:symbol_ids)
              AND interval = ANY(:intervals)
              AND open_time >= :start_ts
              AND open_time <= :end_ts
            ORDER BY symbol_id ASC, interval ASC, open_time ASC
            """
        )
        out: DefaultDict[Tuple[int, str], List[Dict[str, Any]]] = defaultdict(list)
        with self.feed.engine.begin() as conn:
            rows = conn.execute(
                sql,
                {
                    "exchange_id": self.exchange_id,
                    "symbol_ids": list(symbol_ids),
                    "intervals": list(intervals),
                    "start_ts": start,
                    "end_ts": end,
                },
            ).mappings().all()
        for r in rows:
            key = (int(r["symbol_id"]), str(r["interval"]))
            out[key].append(
                {
                    "ts": self._ts_key(r["ts"]),
                    "open": float(r["open"] or 0.0),
                    "high": float(r["high"] or 0.0),
                    "low": float(r["low"] or 0.0),
                    "close": float(r["close"] or 0.0),
                    "volume": float(r["volume"] or 0.0),
                    "quote_volume": float(r["quote_volume"] or 0.0),
                }
            )
        ts_index = {k: [row["ts"] for row in v] for k, v in out.items()}
        return dict(out), ts_index

    def _preload_funding(
        self,
        *,
        symbol_ids: Sequence[int],
        start: datetime,
        end: datetime,
    ) -> Dict[int, List[Tuple[datetime, float]]]:
        sql = text(
            """
            SELECT symbol_id, funding_time, funding_rate
            FROM funding
            WHERE exchange_id = :exchange_id
              AND symbol_id = ANY(:symbol_ids)
              AND funding_time >= :start_ts
              AND funding_time <= :end_ts
            ORDER BY symbol_id ASC, funding_time ASC
            """
        )
        out: DefaultDict[int, List[Tuple[datetime, float]]] = defaultdict(list)
        with self.feed.engine.begin() as conn:
            rows = conn.execute(
                sql,
                {
                    "exchange_id": self.exchange_id,
                    "symbol_ids": list(symbol_ids),
                    "start_ts": start,
                    "end_ts": end,
                },
            ).mappings().all()
        for r in rows:
            out[int(r["symbol_id"])] .append((self._ts_key(r["funding_time"]), float(r["funding_rate"] or 0.0)))
        return dict(out)

    def _preload_liquidations(
        self,
        *,
        symbol_ids: Sequence[int],
        start: datetime,
        end: datetime,
        interval: str,
        soft_threshold: float,
    ) -> Tuple[Dict[Tuple[int, datetime], Dict[str, float]], DefaultDict[datetime, Set[int]]]:
        sql = text(
            """
            SELECT symbol_id, bucket_ts, long_notional, short_notional
            FROM liquidation_1m
            WHERE exchange_id = :exchange_id
              AND symbol_id = ANY(:symbol_ids)
              AND bucket_ts >= :start_ts
              AND bucket_ts <= :end_ts
            ORDER BY symbol_id ASC, bucket_ts ASC
            """
        )
        delta = self._parse_interval(interval)
        out: DefaultDict[Tuple[int, datetime], Dict[str, float]] = defaultdict(lambda: {"long": 0.0, "short": 0.0, "total": 0.0})
        candidate_symbols_by_ts: DefaultDict[datetime, Set[int]] = defaultdict(set)
        with self.feed.engine.begin() as conn:
            rows = conn.execute(
                sql,
                {
                    "exchange_id": self.exchange_id,
                    "symbol_ids": list(symbol_ids),
                    "start_ts": start,
                    "end_ts": end,
                },
            ).mappings().all()
        for r in rows:
            ts = self._ts_key(r["bucket_ts"])
            epoch = int(ts.timestamp())
            sec = int(delta.total_seconds())
            floored = datetime.fromtimestamp(epoch - (epoch % sec), tz=timezone.utc)
            key = (int(r["symbol_id"]), floored)
            long_v = float(r["long_notional"] or 0.0)
            short_v = float(r["short_notional"] or 0.0)
            rec = out[key]
            rec["long"] += long_v
            rec["short"] += short_v
            rec["total"] += long_v + short_v
        if soft_threshold > 0:
            for (symbol_id, ts), rec in out.items():
                if float(rec["total"]) >= soft_threshold:
                    candidate_symbols_by_ts[ts].add(int(symbol_id))
        return dict(out), candidate_symbols_by_ts

    def _build_liq_histories(
        self,
        *,
        liq_by_symbol_bucket: Dict[Tuple[int, datetime], Dict[str, float]],
        interval: str,
        lookback: int,
    ) -> Dict[Tuple[int, str, datetime, int], List[float]]:
        lookback = max(0, int(lookback))
        if lookback <= 1:
            return {}
        delta = self._parse_interval(interval)
        by_symbol: DefaultDict[int, Dict[datetime, float]] = defaultdict(dict)
        for (symbol_id, ts), rec in liq_by_symbol_bucket.items():
            by_symbol[int(symbol_id)][ts] = float(rec.get("total") or 0.0)
        out: Dict[Tuple[int, str, datetime, int], List[float]] = {}
        for symbol_id, bucket_map in by_symbol.items():
            keys = sorted(bucket_map.keys())
            for ts in keys:
                series: List[float] = []
                cur = ts - delta * (lookback - 1)
                for _ in range(lookback):
                    series.append(float(bucket_map.get(cur, 0.0)))
                    cur += delta
                out[(int(symbol_id), str(interval), ts, lookback)] = series
        return out

    def _load_replay_timestamps(
        self,
        *,
        start: datetime,
        end: datetime,
        interval: str,
        use_candidates: bool = False,
        candidate_symbols_by_ts: Optional[Dict[datetime, Set[int]]] = None,
    ) -> List[datetime]:
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
        points = [self._ts_key(row["ts"]) for row in rows]
        if use_candidates and candidate_symbols_by_ts is not None:
            points = [ts for ts in points if candidate_symbols_by_ts.get(self._ts_key(ts))]
        return points

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
        if item.get("name") == "scr_liquidation_binance":
            return dict(item.get("params") or {})
    raise ValueError("scr_liquidation_binance not found in YAML")


def apply_interval_overrides(params: Dict[str, Any], interval: str) -> Dict[str, Any]:
    out = dict(params)
    mapping_keys = [
        "confirm_lookforward",
        "fresh_signal_minutes",
        "volume_liquid_limit",
        "volume_spike_k",
    ]
    for key in mapping_keys:
        raw = out.get(f"{key}_map")
        if isinstance(raw, dict) and interval in raw:
            out[key] = raw[interval]
    if isinstance(out.get("volume_liquid_limit"), list):
        vals = out["volume_liquid_limit"]
        if len(vals) == 1:
            out["volume_liquid_limit"] = vals[0]
    if isinstance(out.get("volume_spike_k"), list):
        vals = out["volume_spike_k"]
        if len(vals) == 1:
            out["volume_spike_k"] = vals[0]
    out.pop("intervals", None)
    return out


def parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Replay liquidation screener through historical timestamps, then backtest the generated signals.")
    p.add_argument("--dsn", default=os.getenv("BACKTEST_DSN"), help="SQLAlchemy DSN")
    p.add_argument("--pool-dsn", default=os.getenv("BACKTEST_POOL_DSN"), help="psycopg pool conninfo")
    p.add_argument("--yaml", required=True, help="Path to screeners_liquidation.yaml")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--interval", required=True, help="5m, 15m, 1h ...")
    p.add_argument("--exchange-id", type=int, default=1)
    p.add_argument("--progress-every", type=int, default=50)
    p.add_argument("--max-points", type=int, help="Optional cap for replay timestamps during debugging")
    p.add_argument("--replay-mode", choices=["fast", "full"], default="fast")
    p.add_argument("--candidate-timestamps", action="store_true", default=True)
    p.add_argument("--all-timestamps", action="store_true", help="Use every candle timestamp instead of liquidation candidate timestamps")
    p.add_argument("--candidate-symbols", action="store_true", default=True)
    p.add_argument("--all-symbols", action="store_true", help="Process full symbol universe on each replay point")
    p.add_argument("--candidate-liq-multiplier", type=float, default=0.35)
    p.add_argument("--quiet", action="store_true", help="Reduce progress output to major stages only")
    return p


def main() -> None:
    args = build_parser().parse_args()
    if not args.dsn:
        raise SystemExit("SQLAlchemy DSN is required. Pass --dsn or set BACKTEST_DSN")
    if not args.pool_dsn:
        raise SystemExit("Pool DSN is required. Pass --pool-dsn or set BACKTEST_POOL_DSN")

    params = apply_interval_overrides(load_yaml_params(args.yaml), args.interval)
    runner = LiquidationHistoricalReplayRunner(
        dsn=args.dsn,
        pool_dsn=args.pool_dsn,
        exchange_id=args.exchange_id,
    )
    try:
        progress_every = max(1, int(args.progress_every))
        if args.quiet:
            progress_every = max(progress_every, 500)
        summary, _events = runner.replay(
            start=parse_dt(args.start),
            end=parse_dt(args.end),
            interval=args.interval,
            screener_params=params,
            progress_every=progress_every,
            max_points=args.max_points,
            replay_mode=str(args.replay_mode),
            use_candidate_timestamps=not bool(args.all_timestamps),
            use_candidate_symbols=not bool(args.all_symbols),
            candidate_liq_multiplier=float(args.candidate_liq_multiplier),
        )
        Reporter.print_summary(summary)
    finally:
        runner.close()


if __name__ == "__main__":
    main()
