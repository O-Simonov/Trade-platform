from __future__ import annotations

import argparse
import csv
import itertools
import json
import math
import os
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from src.platform.backtester.replay_pump_screener_history import (
    PumpHistoricalReplayRunner,
    ReplaySummary,
    apply_interval_overrides,
    load_yaml_params,
)


@dataclass(slots=True)
class OptimizationRow:
    rank_score: float
    interval: str
    params: Dict[str, Any]
    generated_signals: int
    deduped_signals: int
    summary: ReplaySummary


class PumpHistoryOptimizer:
    def __init__(self, *, dsn: str, pool_dsn: str, exchange_id: int):
        self.runner = PumpHistoricalReplayRunner(
            dsn=dsn,
            pool_dsn=pool_dsn,
            exchange_id=exchange_id,
        )

    def close(self) -> None:
        self.runner.close()

    def run_case(
        self,
        *,
        start: datetime,
        end: datetime,
        interval: str,
        screener_params: Dict[str, Any],
        max_points: Optional[int] = None,
    ) -> Tuple[int, int, ReplaySummary]:
        summary, _events = self.runner.replay(
            start=start,
            end=end,
            interval=interval,
            screener_params=screener_params,
            progress_every=10**9,
            max_points=max_points,
        )
        return summary.generated_signals, summary.deduped_signals, summary

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
        max_points: Optional[int] = None,
    ) -> List[OptimizationRow]:
        keys = list(grid.keys())
        combinations = list(itertools.product(*(grid[k] for k in keys)))
        total = len(combinations)

        rows: List[OptimizationRow] = []
        best_row: Optional[OptimizationRow] = None
        t0 = time.perf_counter()

        print("=" * 140)
        print(f"START PUMP GRID SEARCH | interval={interval} | combinations={total} | min_trades={min_trades} | top_n={top_n}")
        print("=" * 140)

        for i, values in enumerate(combinations, start=1):
            tuned = json.loads(json.dumps(base_params))
            for k, v in dict(zip(keys, values)).items():
                _set_nested_value(tuned, k, v)

            combo_started = time.perf_counter()
            signal_count, deduped_signals, summary = self.run_case(
                start=start,
                end=end,
                interval=interval,
                screener_params=tuned,
                max_points=max_points,
            )
            combo_elapsed = time.perf_counter() - combo_started

            if summary.total_trades >= min_trades:
                row = OptimizationRow(
                    rank_score=self._rank(summary),
                    interval=interval,
                    params={k: _get_nested_value(tuned, k) for k in keys},
                    generated_signals=signal_count,
                    deduped_signals=deduped_signals,
                    summary=summary,
                )
                rows.append(row)
                if best_row is None or row.rank_score > best_row.rank_score:
                    best_row = row
                    pf = "inf" if math.isinf(row.summary.profit_factor) else f"{row.summary.profit_factor:.4f}"
                    print(
                        f"[BEST {i}/{total}] pnl={row.summary.net_pnl:.8f} pf={pf} wr={row.summary.win_rate:.2%} "
                        f"trades={row.summary.total_trades} dd={row.summary.max_drawdown:.8f} "
                        f"signals={row.generated_signals}/{row.deduped_signals} params={row.params}"
                    )

            if i == 1 or i % progress_every == 0 or i == total:
                elapsed = time.perf_counter() - t0
                avg_sec = elapsed / i
                remaining = max(0.0, (total - i) * avg_sec)
                best_pnl = best_row.summary.net_pnl if best_row else 0.0
                best_pf = (
                    "inf" if best_row and math.isinf(best_row.summary.profit_factor)
                    else (f"{best_row.summary.profit_factor:.4f}" if best_row else "n/a")
                )
                print(
                    f"[{i}/{total}] elapsed={elapsed:.1f}s avg={avg_sec:.2f}s/comb last={combo_elapsed:.2f}s "
                    f"accepted={len(rows)} best_pnl={best_pnl:.8f} best_pf={best_pf} eta={remaining:.1f}s"
                )

        rows.sort(key=lambda r: (r.rank_score, r.summary.net_pnl, r.summary.profit_factor), reverse=True)
        return rows[:top_n]

    @staticmethod
    def _rank(summary: ReplaySummary) -> float:
        pf = 10.0 if math.isinf(summary.profit_factor) else summary.profit_factor
        return (
            summary.net_pnl * 1.0
            + pf * 12.0
            + summary.win_rate * 5.0
            - summary.max_drawdown * 0.35
            + summary.total_trades * 0.05
        )


def _set_nested_value(data: Dict[str, Any], path: str, value: Any) -> None:
    parts = [p for p in str(path).split(".") if p]
    if not parts:
        return
    cursor = data
    for part in parts[:-1]:
        next_val = cursor.get(part)
        if not isinstance(next_val, dict):
            next_val = {}
            cursor[part] = next_val
        cursor = next_val
    cursor[parts[-1]] = value


def _get_nested_value(data: Dict[str, Any], path: str) -> Any:
    cursor: Any = data
    for part in [p for p in str(path).split(".") if p]:
        if not isinstance(cursor, dict):
            return None
        cursor = cursor.get(part)
    return cursor


def parse_dt(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


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
                out.append(int(x) if x.is_integer() else x)
            except ValueError:
                out.append(item.strip())
    return out




def build_manual_grid(args: argparse.Namespace) -> Dict[str, Sequence[Any]]:
    return {
        "buy.min_price_change_pct": parse_num_list(args.buy_min_price_change_pct),
        "buy.min_price_change_atr": parse_num_list(args.buy_min_price_change_atr),
        "buy.min_volume_mult": parse_num_list(args.buy_min_volume_mult),
        "buy.min_volume_spike_mult": parse_num_list(args.buy_min_volume_spike_mult),
        "buy.min_oi_delta_pct": parse_num_list(args.buy_min_oi_delta_pct),
        "buy.min_cvd_delta_pct": parse_num_list(args.buy_min_cvd_delta_pct),
        "buy.min_signal_score": parse_num_list(args.buy_min_signal_score),
        "buy.confirmed_min_signal_score": parse_num_list(args.buy_confirmed_min_signal_score),
        "sell.min_pump_pct": parse_num_list(args.sell_min_pump_pct),
        "sell.reversal_from_peak_pct": parse_num_list(args.sell_reversal_from_peak_pct),
        "sell.min_signal_score": parse_num_list(args.sell_min_signal_score),
        "sell.confirmed_min_signal_score": parse_num_list(args.sell_confirmed_min_signal_score),
        "liquidity.min_base_quote_volume": parse_num_list(args.liquidity_min_base_quote_volume),
        "liquidity.min_notional_oi": parse_num_list(args.liquidity_min_notional_oi),
        "runtime.signal_mode": parse_num_list(args.runtime_signal_mode),
        "trend_filters.trend_method": parse_num_list(args.trend_filters_trend_method),
        "trend_filters.trend_mode": parse_num_list(args.trend_filters_trend_mode),
        "funding.enabled": parse_num_list(args.funding_enabled),
        "base_quality.require_calm_base": parse_num_list(args.base_quality_require_calm_base),
    }


def build_preset_grid(interval: str, preset: str) -> Dict[str, Sequence[Any]]:
    interval = str(interval).lower()
    preset = str(preset).lower()

    if preset == "balanced":
        by_interval = {
            "5m": {
                "runtime.signal_mode": ["both", "sell"],
                "trend_filters.trend_method": ["slope"],
                "trend_filters.trend_mode": ["soft", "hard"],
                "funding.enabled": [True],
                "base_quality.require_calm_base": [True],
                "buy.min_price_change_pct": [1.5, 1.8],
                "buy.min_volume_spike_mult": [1.8, 2.1],
                "buy.min_oi_delta_pct": [0.25, 0.45],
                "buy.min_signal_score": [42, 48],
                "sell.min_pump_pct": [2.2, 2.8],
                "sell.reversal_from_peak_pct": [0.30, 0.42],
                "sell.min_signal_score": [46, 52],
                "liquidity.min_base_quote_volume": [150000, 250000],
                "liquidity.min_notional_oi": [1500000, 3000000],
            },
            "15m": {
                "runtime.signal_mode": ["both", "sell"],
                "trend_filters.trend_method": ["slope", "both"],
                "trend_filters.trend_mode": ["soft", "hard"],
                "funding.enabled": [True],
                "base_quality.require_calm_base": [True],
                "buy.min_price_change_pct": [1.9, 2.2],
                "buy.min_volume_spike_mult": [2.0, 2.3],
                "buy.min_oi_delta_pct": [0.35, 0.60],
                "buy.min_signal_score": [46, 52],
                "sell.min_pump_pct": [2.8, 3.4],
                "sell.reversal_from_peak_pct": [0.45, 0.60],
                "sell.min_signal_score": [50, 56],
                "liquidity.min_base_quote_volume": [300000, 450000],
                "liquidity.min_notional_oi": [3500000, 6000000],
            },
            "1h": {
                "runtime.signal_mode": ["both", "sell"],
                "trend_filters.trend_method": ["both"],
                "trend_filters.trend_mode": ["soft", "hard"],
                "funding.enabled": [True],
                "base_quality.require_calm_base": [True],
                "buy.min_price_change_pct": [2.6, 3.0],
                "buy.min_volume_spike_mult": [2.2, 2.7],
                "buy.min_oi_delta_pct": [0.55, 0.90],
                "buy.min_signal_score": [52, 58],
                "sell.min_pump_pct": [4.0, 4.8],
                "sell.reversal_from_peak_pct": [0.55, 0.75],
                "sell.min_signal_score": [56, 62],
                "liquidity.min_base_quote_volume": [650000, 950000],
                "liquidity.min_notional_oi": [7000000, 12000000],
            },
        }
    elif preset == "narrow":
        by_interval = {
            "5m": {
                "runtime.signal_mode": ["both"],
                "trend_filters.trend_method": ["slope"],
                "trend_filters.trend_mode": ["hard"],
                "funding.enabled": [True],
                "base_quality.require_calm_base": [True],
                "buy.min_price_change_pct": [1.6, 1.7],
                "buy.min_volume_spike_mult": [1.9, 2.0],
                "buy.min_oi_delta_pct": [0.30, 0.40],
                "buy.min_signal_score": [44, 46],
                "sell.min_pump_pct": [2.4, 2.6],
                "sell.reversal_from_peak_pct": [0.34, 0.40],
                "sell.min_signal_score": [48, 50],
                "liquidity.min_base_quote_volume": [180000, 220000],
                "liquidity.min_notional_oi": [1800000, 2500000],
            },
            "15m": {
                "runtime.signal_mode": ["both"],
                "trend_filters.trend_method": ["both"],
                "trend_filters.trend_mode": ["hard"],
                "funding.enabled": [True],
                "base_quality.require_calm_base": [True],
                "buy.min_price_change_pct": [2.0, 2.1],
                "buy.min_volume_spike_mult": [2.1, 2.2],
                "buy.min_oi_delta_pct": [0.40, 0.50],
                "buy.min_signal_score": [48, 50],
                "sell.min_pump_pct": [3.0, 3.2],
                "sell.reversal_from_peak_pct": [0.50, 0.56],
                "sell.min_signal_score": [52, 54],
                "liquidity.min_base_quote_volume": [350000, 420000],
                "liquidity.min_notional_oi": [4000000, 5000000],
            },
            "1h": {
                "runtime.signal_mode": ["both"],
                "trend_filters.trend_method": ["both"],
                "trend_filters.trend_mode": ["hard"],
                "funding.enabled": [True],
                "base_quality.require_calm_base": [True],
                "buy.min_price_change_pct": [2.8, 2.9],
                "buy.min_volume_spike_mult": [2.3, 2.5],
                "buy.min_oi_delta_pct": [0.65, 0.80],
                "buy.min_signal_score": [54, 56],
                "sell.min_pump_pct": [4.3, 4.6],
                "sell.reversal_from_peak_pct": [0.60, 0.70],
                "sell.min_signal_score": [58, 60],
                "liquidity.min_base_quote_volume": [750000, 900000],
                "liquidity.min_notional_oi": [8000000, 10000000],
            },
        }
    elif preset == "conservative":
        by_interval = {
            "5m": {
                "runtime.signal_mode": ["both", "sell"],
                "trend_filters.trend_method": ["both"],
                "trend_filters.trend_mode": ["hard"],
                "funding.enabled": [True],
                "base_quality.require_calm_base": [True],
                "buy.min_price_change_pct": [1.8, 2.0],
                "buy.min_volume_spike_mult": [2.0, 2.2],
                "buy.min_oi_delta_pct": [0.35, 0.50],
                "buy.min_signal_score": [48, 52],
                "sell.min_pump_pct": [2.6, 3.0],
                "sell.reversal_from_peak_pct": [0.40, 0.50],
                "sell.min_signal_score": [52, 56],
                "liquidity.min_base_quote_volume": [220000, 320000],
                "liquidity.min_notional_oi": [2200000, 3500000],
            },
            "15m": {
                "runtime.signal_mode": ["both", "sell"],
                "trend_filters.trend_method": ["both"],
                "trend_filters.trend_mode": ["hard"],
                "funding.enabled": [True],
                "base_quality.require_calm_base": [True],
                "buy.min_price_change_pct": [2.1, 2.3],
                "buy.min_volume_spike_mult": [2.1, 2.3],
                "buy.min_oi_delta_pct": [0.45, 0.60],
                "buy.min_signal_score": [50, 54],
                "sell.min_pump_pct": [3.2, 3.6],
                "sell.reversal_from_peak_pct": [0.55, 0.65],
                "sell.min_signal_score": [54, 58],
                "liquidity.min_base_quote_volume": [400000, 550000],
                "liquidity.min_notional_oi": [5000000, 7000000],
            },
            "1h": {
                "runtime.signal_mode": ["both", "sell"],
                "trend_filters.trend_method": ["both"],
                "trend_filters.trend_mode": ["hard"],
                "funding.enabled": [True],
                "base_quality.require_calm_base": [True],
                "buy.min_price_change_pct": [3.0, 3.3],
                "buy.min_volume_spike_mult": [2.4, 2.7],
                "buy.min_oi_delta_pct": [0.70, 0.95],
                "buy.min_signal_score": [56, 60],
                "sell.min_pump_pct": [4.6, 5.2],
                "sell.reversal_from_peak_pct": [0.65, 0.80],
                "sell.min_signal_score": [60, 64],
                "liquidity.min_base_quote_volume": [900000, 1200000],
                "liquidity.min_notional_oi": [9000000, 13000000],
            },
        }
    elif preset == "wide":
        by_interval = {
            "5m": {
                "runtime.signal_mode": ["both", "sell"],
                "trend_filters.trend_method": ["slope", "both"],
                "trend_filters.trend_mode": ["soft", "hard"],
                "funding.enabled": [True],
                "base_quality.require_calm_base": [True, False],
                "buy.min_price_change_pct": [1.3, 1.9],
                "buy.min_volume_spike_mult": [1.6, 2.2],
                "buy.min_oi_delta_pct": [0.15, 0.55],
                "buy.min_signal_score": [38, 50],
                "sell.min_pump_pct": [2.0, 3.0],
                "sell.reversal_from_peak_pct": [0.25, 0.55],
                "sell.min_signal_score": [42, 54],
                "liquidity.min_base_quote_volume": [120000, 300000],
                "liquidity.min_notional_oi": [1200000, 3500000],
            },
            "15m": {
                "runtime.signal_mode": ["both", "sell"],
                "trend_filters.trend_method": ["slope", "both"],
                "trend_filters.trend_mode": ["soft", "hard"],
                "funding.enabled": [True],
                "base_quality.require_calm_base": [True, False],
                "buy.min_price_change_pct": [1.7, 2.3],
                "buy.min_volume_spike_mult": [1.8, 2.4],
                "buy.min_oi_delta_pct": [0.25, 0.70],
                "buy.min_signal_score": [42, 54],
                "sell.min_pump_pct": [2.6, 3.8],
                "sell.reversal_from_peak_pct": [0.35, 0.65],
                "sell.min_signal_score": [46, 58],
                "liquidity.min_base_quote_volume": [220000, 550000],
                "liquidity.min_notional_oi": [2500000, 7000000],
            },
            "1h": {
                "runtime.signal_mode": ["both", "sell"],
                "trend_filters.trend_method": ["slope", "both"],
                "trend_filters.trend_mode": ["soft", "hard"],
                "funding.enabled": [True],
                "base_quality.require_calm_base": [True, False],
                "buy.min_price_change_pct": [2.4, 3.2],
                "buy.min_volume_spike_mult": [2.0, 2.8],
                "buy.min_oi_delta_pct": [0.45, 1.05],
                "buy.min_signal_score": [48, 60],
                "sell.min_pump_pct": [3.8, 5.4],
                "sell.reversal_from_peak_pct": [0.50, 0.90],
                "sell.min_signal_score": [52, 64],
                "liquidity.min_base_quote_volume": [500000, 1200000],
                "liquidity.min_notional_oi": [5000000, 14000000],
            },
        }
    else:
        raise ValueError(f"Unknown preset: {preset}")

    selected = by_interval.get(interval)
    if not selected:
        raise ValueError(f"Unsupported interval for preset grid: {interval}")
    return selected


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="True historical optimizer for pump screener: regenerate signals from history, then backtest.")
    p.add_argument("--dsn", default=os.getenv("BACKTEST_DSN"), help="SQLAlchemy DSN")
    p.add_argument("--pool-dsn", default=os.getenv("BACKTEST_POOL_DSN"), help="psycopg pool conninfo")
    p.add_argument("--yaml", required=True, help="Path to screener_pump.yaml")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--interval", required=True, help="5m / 15m / 1h")
    p.add_argument("--exchange-id", type=int, default=1)
    p.add_argument("--min-trades", type=int, default=10)
    p.add_argument("--top-n", type=int, default=20)
    p.add_argument("--progress-every", type=int, default=10)
    p.add_argument("--max-points", type=int)
    p.add_argument("--csv-out", help="Optional path to save all top results")
    p.add_argument("--preset", choices=["balanced", "wide", "narrow", "conservative", "manual"], default="balanced", help="Ready-made optimization grid per interval; use manual to rely on explicit CLI ranges")

    p.add_argument("--buy-min-price-change-pct", default="1.2,1.5,1.8,2.2")
    p.add_argument("--buy-min-price-change-atr", default="0.5,0.7,0.9")
    p.add_argument("--buy-min-volume-mult", default="1.2,1.35,1.5,1.7")
    p.add_argument("--buy-min-volume-spike-mult", default="1.5,1.8,2.1,2.5")
    p.add_argument("--buy-min-oi-delta-pct", default="0.1,0.25,0.5,0.8")
    p.add_argument("--buy-min-cvd-delta-pct", default="0.0,0.05,0.1")
    p.add_argument("--buy-min-signal-score", default="32,38,44,50")
    p.add_argument("--buy-confirmed-min-signal-score", default="40,46,52,58")
    p.add_argument("--sell-min-pump-pct", default="2.0,2.6,3.2,4.0")
    p.add_argument("--sell-reversal-from-peak-pct", default="0.2,0.35,0.5,0.8")
    p.add_argument("--sell-min-signal-score", default="38,44,50,56")
    p.add_argument("--sell-confirmed-min-signal-score", default="46,52,58,64")
    p.add_argument("--liquidity-min-base-quote-volume", default="150000,250000,400000,700000")
    p.add_argument("--liquidity-min-notional-oi", default="1500000,3000000,5000000,8000000")
    p.add_argument("--runtime-signal-mode", default="both,buy,sell")
    p.add_argument("--trend-filters-trend-method", default="off,slope,both")
    p.add_argument("--trend-filters-trend-mode", default="soft,hard")
    p.add_argument("--funding-enabled", default="true,false")
    p.add_argument("--base-quality-require-calm-base", default="true,false")
    return p


def print_results(rows: Sequence[OptimizationRow]) -> None:
    print("=" * 160)
    print("TOP PUMP TRUE-HISTORY OPTIMIZATION RESULTS")
    print("=" * 160)
    for i, row in enumerate(rows, start=1):
        s = row.summary
        pf = "inf" if math.isinf(s.profit_factor) else f"{s.profit_factor:.4f}"
        print(
            f"#{i:02d} rank={row.rank_score:.4f} interval={row.interval} signals={row.generated_signals}/{row.deduped_signals} "
            f"trades={s.total_trades} pnl={s.net_pnl:.8f} pf={pf} wr={s.win_rate:.2%} dd={s.max_drawdown:.8f} params={row.params}"
        )
    print("=" * 160)


def save_csv(path: str, rows: Sequence[OptimizationRow]) -> None:
    fieldnames = [
        "rank_score", "interval", "generated_signals", "deduped_signals", "replay_points", "total_trades", "wins", "losses", "win_rate",
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
                "deduped_signals": row.deduped_signals,
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
    optimizer = PumpHistoryOptimizer(
        dsn=args.dsn,
        pool_dsn=args.pool_dsn,
        exchange_id=args.exchange_id,
    )

    try:
        grid = build_manual_grid(args) if args.preset == "manual" else build_preset_grid(args.interval, args.preset)
        total_combinations = 1
        for values in grid.values():
            total_combinations *= max(1, len(values))
        print(f"Using pump optimization preset={args.preset} interval={args.interval} combinations={total_combinations}")

        rows = optimizer.grid_search(
            start=parse_dt(args.start),
            end=parse_dt(args.end),
            interval=args.interval,
            base_params=base_params,
            grid=grid,
            min_trades=args.min_trades,
            top_n=args.top_n,
            progress_every=max(1, int(args.progress_every)),
            max_points=args.max_points,
        )
        print_results(rows)
        if args.csv_out:
            save_csv(args.csv_out, rows)
    finally:
        optimizer.close()


if __name__ == "__main__":
    main()
