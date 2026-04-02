from __future__ import annotations

import argparse
import csv
import json
import math
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import yaml

from src.platform.backtester.replay_liquidation_screener_history import (
    LiquidationHistoricalReplayRunner,
    ReplaySummary,
    apply_interval_overrides,
    load_yaml_params,
)


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


def _print_progress(prefix: str, done: int, total: int, *, started_at: float, extra: str = "") -> None:
    elapsed = max(0.0, time.perf_counter() - started_at)
    avg = elapsed / done if done > 0 else 0.0
    remaining = max(0.0, (total - done) * avg)
    pct = done / max(1, total) * 100.0
    bar = _progress_bar(done, total)
    suffix = f" | {extra}" if extra else ""
    _print(f"{prefix} {bar} {done}/{total} ({pct:5.1f}%) | elapsed={_fmt_duration(elapsed)} | eta={_fmt_duration(remaining)}{suffix}")


@dataclass(slots=True)
class OptimizationRow:
    rank_score: float
    search_mode: str
    interval: str
    params: Dict[str, Any]
    generated_signals: int
    deduped_signals: int
    summary: ReplaySummary


def _params_signature(params: Dict[str, Any]) -> str:
    return json.dumps(params, sort_keys=True, ensure_ascii=False, default=str)


def load_resume_rows(path: Optional[str]) -> List[OptimizationRow]:
    if not path:
        return []
    p = Path(path)
    if not p.exists() or p.stat().st_size <= 0:
        return []
    rows: List[OptimizationRow] = []
    with p.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for rec in reader:
            try:
                params = json.loads(rec.get("params_json") or "{}")
            except Exception:
                params = {}
                for k, v in rec.items():
                    if k in {"search_mode", "rank_score", "interval", "generated_signals", "deduped_signals", "replay_points",
                             "total_trades", "wins", "losses", "win_rate", "gross_profit", "gross_loss", "total_fees",
                             "net_pnl", "avg_trade", "profit_factor", "max_drawdown", "params_json", "evaluated_idx", "accepted_idx", "combo_elapsed_sec"}:
                        continue
                    if v not in (None, ""):
                        params[k] = v
            try:
                summary = ReplaySummary(
                    replay_points=int(float(rec.get("replay_points") or 0)),
                    generated_signals=int(float(rec.get("generated_signals") or 0)),
                    deduped_signals=int(float(rec.get("deduped_signals") or 0)),
                    total_trades=int(float(rec.get("total_trades") or 0)),
                    wins=int(float(rec.get("wins") or 0)),
                    losses=int(float(rec.get("losses") or 0)),
                    win_rate=float(rec.get("win_rate") or 0.0),
                    gross_profit=float(rec.get("gross_profit") or 0.0),
                    gross_loss=float(rec.get("gross_loss") or 0.0),
                    total_fees=float(rec.get("total_fees") or 0.0),
                    net_pnl=float(rec.get("net_pnl") or 0.0),
                    avg_trade=float(rec.get("avg_trade") or 0.0),
                    profit_factor=float(rec.get("profit_factor") or 0.0) if (rec.get("profit_factor") or "") != "inf" else float("inf"),
                    max_drawdown=float(rec.get("max_drawdown") or 0.0),
                )
                rows.append(OptimizationRow(
                    rank_score=float(rec.get("rank_score") or 0.0),
                    search_mode=str(rec.get("search_mode") or "grid"),
                    interval=str(rec.get("interval") or ""),
                    params=params,
                    generated_signals=int(float(rec.get("generated_signals") or 0)),
                    deduped_signals=int(float(rec.get("deduped_signals") or 0)),
                    summary=summary,
                ))
            except Exception:
                continue
    return rows


def _pick_resume_path(args: argparse.Namespace) -> Optional[str]:
    return args.resume_from_csv or args.csv_progress_out or args.csv_out or args.csv_snapshot_out


class ProgressCsvTracker:
    def __init__(self, progress_csv: Optional[str] = None, snapshot_csv: Optional[str] = None, snapshot_every: int = 1):
        self.progress_csv = Path(progress_csv) if progress_csv else None
        self.snapshot_csv = Path(snapshot_csv) if snapshot_csv else None
        self.snapshot_every = max(1, int(snapshot_every))
        self._progress_header_written = False

    def _ensure_progress_header(self) -> None:
        if self.progress_csv is None or self._progress_header_written:
            return
        self.progress_csv.parent.mkdir(parents=True, exist_ok=True)
        if self.progress_csv.exists() and self.progress_csv.stat().st_size > 0:
            self._progress_header_written = True
            return
        with self.progress_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "evaluated_idx", "accepted_idx", "accepted", "search_mode", "interval", "combo_elapsed_sec",
                "rank_score", "generated_signals", "deduped_signals", "replay_points", "total_trades",
                "wins", "losses", "win_rate", "gross_profit", "gross_loss", "total_fees",
                "net_pnl", "avg_trade", "profit_factor", "max_drawdown", "params_json",
            ])
            writer.writeheader()
        self._progress_header_written = True

    def append_progress(self, *, evaluated_idx: int, accepted_idx: int, accepted: bool, search_mode: str, interval: str, combo_elapsed_sec: float, row: OptimizationRow) -> None:
        if self.progress_csv is None:
            return
        self._ensure_progress_header()
        summary = asdict(row.summary)
        rec = {
            "evaluated_idx": int(evaluated_idx),
            "accepted_idx": int(accepted_idx),
            "accepted": bool(accepted),
            "search_mode": str(search_mode),
            "interval": str(interval),
            "combo_elapsed_sec": float(combo_elapsed_sec),
            "rank_score": float(row.rank_score),
            "generated_signals": int(row.generated_signals),
            "deduped_signals": int(row.deduped_signals),
            "params_json": json.dumps(row.params, ensure_ascii=False, sort_keys=True),
            **summary,
        }
        with self.progress_csv.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rec.keys()))
            writer.writerow(rec)

    def snapshot_top(self, *, rows: Sequence[OptimizationRow], force: bool = False) -> None:
        if self.snapshot_csv is None:
            return
        if not rows:
            return
        if not force and (len(rows) % self.snapshot_every) != 0:
            return
        save_csv(str(self.snapshot_csv), list(rows))


class LiquidationHistoryOptimizer:
    def __init__(self, *, dsn: str, pool_dsn: str, exchange_id: int, replay_mode: str = "fast", use_candidate_timestamps: bool = True, use_candidate_symbols: bool = True, candidate_liq_multiplier: float = 0.35):
        self.dsn = dsn
        self.pool_dsn = pool_dsn
        self.exchange_id = exchange_id
        self.replay_mode = replay_mode
        self.use_candidate_timestamps = use_candidate_timestamps
        self.use_candidate_symbols = use_candidate_symbols
        self.candidate_liq_multiplier = candidate_liq_multiplier
        self.runner = LiquidationHistoricalReplayRunner(
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
            replay_mode=self.replay_mode,
            use_candidate_timestamps=self.use_candidate_timestamps,
            use_candidate_symbols=self.use_candidate_symbols,
            candidate_liq_multiplier=self.candidate_liq_multiplier,
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
        progress_every: int = 5,
        max_points: Optional[int] = None,
        workers: int = 1,
        progress_tracker: Optional[ProgressCsvTracker] = None,
        resume_rows: Optional[Sequence[OptimizationRow]] = None,
    ) -> List[OptimizationRow]:
        import itertools

        keys = list(grid.keys())
        combinations = list(itertools.product(*(grid[k] for k in keys)))
        total = len(combinations)

        resume_rows = list(resume_rows or [])
        resume_map: Dict[str, OptimizationRow] = {_params_signature(r.params): r for r in resume_rows}
        rows: List[OptimizationRow] = [r for r in resume_rows if r.summary.total_trades >= min_trades]
        best_row: Optional[OptimizationRow] = max(rows, key=lambda r: r.rank_score, default=None)
        t0 = time.perf_counter()
        skipped = 0
        started_at = t0

        _print("=" * 140)
        _print(f"START LIQUIDATION GRID SEARCH | interval={interval} | combinations={total} | min_trades={min_trades} | top_n={top_n}")
        _print("=" * 140)

        if max(1, int(workers)) <= 1:
            iterator = []
            for i, values in enumerate(combinations, start=1):
                tuned = dict(base_params)
                tuned.update(dict(zip(keys, values)))
                row_params = {k: tuned[k] for k in keys}
                sig = _params_signature(row_params)
                if sig in resume_map:
                    skipped += 1
                    if i == 1 or i % progress_every == 0 or i == total:
                        elapsed = time.perf_counter() - t0
                        avg_sec = elapsed / max(1, i)
                        remaining = max(0.0, (total - i) * avg_sec)
                        best_pnl = best_row.summary.net_pnl if best_row else 0.0
                        best_pf = "inf" if best_row and math.isinf(best_row.summary.profit_factor) else (f"{best_row.summary.profit_factor:.4f}" if best_row else "0.0000")
                        _print_progress("GRID", i, total, started_at=t0, extra=f"avg={avg_sec:.2f}s/comb | skipped={skipped} | accepted={len(rows)} | best_pnl={best_pnl:.8f} | best_pf={best_pf}")
                    continue
                combo_started = time.perf_counter()
                generated_signals, deduped_signals, summary = self.run_case(
                    start=start,
                    end=end,
                    interval=interval,
                    screener_params=tuned,
                    max_points=max_points,
                )
                combo_elapsed = time.perf_counter() - combo_started
                iterator.append((i, tuned, row_params, generated_signals, deduped_signals, summary, combo_elapsed))
                if iterator:
                    i, tuned, row_params, generated_signals, deduped_signals, summary, combo_elapsed = iterator.pop()
                    accepted = summary.total_trades >= min_trades
                    score = self._rank(summary) if accepted else -1e9
                    row = OptimizationRow(rank_score=score, search_mode="grid", interval=interval, params=row_params, generated_signals=generated_signals, deduped_signals=deduped_signals, summary=summary)
                    if progress_tracker is not None:
                        progress_tracker.append_progress(
                            evaluated_idx=i,
                            accepted_idx=len(rows) + (1 if accepted else 0),
                            accepted=accepted,
                            search_mode="grid",
                            interval=interval,
                            combo_elapsed_sec=combo_elapsed,
                            row=row,
                        )
                    if accepted:
                        rows.append(row)
                        if progress_tracker is not None:
                            progress_tracker.snapshot_top(rows=sorted(rows, key=lambda r: (r.rank_score, r.summary.net_pnl, r.summary.profit_factor), reverse=True)[:top_n])
                        if best_row is None or row.rank_score > best_row.rank_score:
                            best_row = row
                            pf = "inf" if math.isinf(row.summary.profit_factor) else f"{row.summary.profit_factor:.4f}"
                            _print(f"[BEST {i}/{total}] pnl={row.summary.net_pnl:.8f} pf={pf} wr={row.summary.win_rate:.2%} trades={row.summary.total_trades} dd={row.summary.max_drawdown:.8f} signals={row.generated_signals}/{row.deduped_signals} params={row.params}")
                    if i == 1 or i % progress_every == 0 or i == total:
                        elapsed = time.perf_counter() - t0
                        avg_sec = elapsed / i
                        remaining = max(0.0, (total - i) * avg_sec)
                        best_pnl = best_row.summary.net_pnl if best_row else 0.0
                        best_pf = "inf" if best_row and math.isinf(best_row.summary.profit_factor) else (f"{best_row.summary.profit_factor:.4f}" if best_row else "n/a")
                        _print_progress("GRID", i, total, started_at=started_at, extra=f"avg={avg_sec:.2f}s/comb | last={combo_elapsed:.2f}s | skipped={skipped} | accepted={len(rows)} | best_pnl={best_pnl:.8f} | best_pf={best_pf}")
        else:
            futures = {}
            with ProcessPoolExecutor(max_workers=max(1, int(workers))) as ex:
                for i, values in enumerate(combinations, start=1):
                    tuned = dict(base_params)
                    tuned.update(dict(zip(keys, values)))
                    payload = {
                        "dsn": self.dsn,
                        "pool_dsn": self.pool_dsn,
                        "exchange_id": self.exchange_id,
                        "start": start,
                        "end": end,
                        "interval": interval,
                        "screener_params": tuned,
                        "max_points": max_points,
                        "replay_mode": self.replay_mode,
                        "use_candidate_timestamps": self.use_candidate_timestamps,
                        "use_candidate_symbols": self.use_candidate_symbols,
                        "candidate_liq_multiplier": self.candidate_liq_multiplier,
                    }
                    futures[ex.submit(_run_grid_case_worker, payload)] = (i, tuned)
                for done_count, fut in enumerate(as_completed(futures), start=1):
                    i, tuned = futures[fut]
                    combo_started = time.perf_counter()
                    result = fut.result()
                    combo_elapsed = time.perf_counter() - combo_started
                    generated_signals = result["generated_signals"]
                    deduped_signals = result["deduped_signals"]
                    summary = result["summary"]
                    row_params = {k: tuned[k] for k in keys}
                    accepted = summary.total_trades >= min_trades
                    score = self._rank(summary) if accepted else -1e9
                    row = OptimizationRow(rank_score=score, search_mode="grid", interval=interval, params=row_params, generated_signals=generated_signals, deduped_signals=deduped_signals, summary=summary)
                    if progress_tracker is not None:
                        progress_tracker.append_progress(
                            evaluated_idx=done_count,
                            accepted_idx=len(rows) + (1 if accepted else 0),
                            accepted=accepted,
                            search_mode="grid",
                            interval=interval,
                            combo_elapsed_sec=combo_elapsed,
                            row=row,
                        )
                    if accepted:
                        rows.append(row)
                        if progress_tracker is not None:
                            progress_tracker.snapshot_top(rows=sorted(rows, key=lambda r: (r.rank_score, r.summary.net_pnl, r.summary.profit_factor), reverse=True)[:top_n])
                        if best_row is None or row.rank_score > best_row.rank_score:
                            best_row = row
                            pf = "inf" if math.isinf(row.summary.profit_factor) else f"{row.summary.profit_factor:.4f}"
                            _print(f"[BEST {done_count}/{total}] pnl={row.summary.net_pnl:.8f} pf={pf} wr={row.summary.win_rate:.2%} trades={row.summary.total_trades} dd={row.summary.max_drawdown:.8f} signals={row.generated_signals}/{row.deduped_signals} params={row.params}")
                    if done_count == 1 or done_count % progress_every == 0 or done_count == total:
                        elapsed = time.perf_counter() - t0
                        avg_sec = elapsed / done_count
                        remaining = max(0.0, (total - done_count) * avg_sec)
                        best_pnl = best_row.summary.net_pnl if best_row else 0.0
                        best_pf = "inf" if best_row and math.isinf(best_row.summary.profit_factor) else (f"{best_row.summary.profit_factor:.4f}" if best_row else "n/a")
                        _print_progress("GRID", done_count, total, started_at=started_at, extra=f"avg={avg_sec:.2f}s/comb | last={combo_elapsed:.2f}s | skipped={skipped} | accepted={len(rows)} | best_pnl={best_pnl:.8f} | best_pf={best_pf}")

        rows.sort(key=lambda r: (r.rank_score, r.summary.net_pnl, r.summary.profit_factor), reverse=True)
        if progress_tracker is not None:
            progress_tracker.snapshot_top(rows=rows[:top_n], force=True)
        return rows[:top_n]

    def bayesian_search(
        self,
        *,
        start: datetime,
        end: datetime,
        interval: str,
        base_params: Dict[str, Any],
        grid: Dict[str, Sequence[Any]],
        min_trades: int,
        top_n: int,
        trials: int,
        max_points: Optional[int] = None,
        seed: int = 42,
        study_name: Optional[str] = None,
        progress_tracker: Optional[ProgressCsvTracker] = None,
        resume_rows: Optional[Sequence[OptimizationRow]] = None,
    ) -> List[OptimizationRow]:
        try:
            import optuna
        except Exception as exc:
            raise SystemExit("Bayesian search requires optuna. Install dependencies from requirements.txt first.") from exc

        sampler = optuna.samplers.TPESampler(seed=seed)
        study = optuna.create_study(direction="maximize", sampler=sampler, study_name=study_name)
        rows: List[OptimizationRow] = list(resume_rows or [])
        resume_map: Dict[str, OptimizationRow] = {_params_signature(r.params): r for r in rows}
        for r in rows:
            try:
                study.enqueue_trial(r.params)
            except Exception:
                pass

        _print("=" * 140)
        _print(f"START LIQUIDATION BAYESIAN SEARCH | interval={interval} | trials={trials} | min_trades={min_trades}")
        _print("=" * 140)

        def objective(trial: "optuna.Trial") -> float:
            tuned = dict(base_params)
            chosen: Dict[str, Any] = {}
            for key, values in grid.items():
                chosen[key] = trial.suggest_categorical(key, list(values))
            tuned.update(chosen)
            sig = _params_signature(chosen)
            if sig in resume_map:
                return resume_map[sig].rank_score

            generated_signals, deduped_signals, summary = self.run_case(
                start=start,
                end=end,
                interval=interval,
                screener_params=tuned,
                max_points=max_points,
            )
            score = self._rank(summary) if summary.total_trades >= min_trades else -1e9
            row = OptimizationRow(
                rank_score=score,
                search_mode="bayesian",
                interval=interval,
                params=chosen,
                generated_signals=generated_signals,
                deduped_signals=deduped_signals,
                summary=summary,
            )
            rows.append(row)
            if progress_tracker is not None and summary.total_trades >= min_trades:
                progress_tracker.append_progress(
                    evaluated_idx=len(rows),
                    accepted_idx=len([r for r in rows if r.summary.total_trades >= min_trades]),
                    accepted=True,
                    search_mode="bayesian",
                    interval=interval,
                    combo_elapsed_sec=0.0,
                    row=row,
                )
                accepted_rows = [r for r in rows if r.summary.total_trades >= min_trades]
                accepted_rows.sort(key=lambda r: (r.rank_score, r.summary.net_pnl, r.summary.profit_factor), reverse=True)
                progress_tracker.snapshot_top(rows=accepted_rows[:top_n])
            trial.set_user_attr("generated_signals", generated_signals)
            trial.set_user_attr("deduped_signals", deduped_signals)
            trial.set_user_attr("total_trades", summary.total_trades)
            trial.set_user_attr("net_pnl", summary.net_pnl)
            trial.set_user_attr("profit_factor", summary.profit_factor)
            trial.set_user_attr("max_drawdown", summary.max_drawdown)
            return score

        study.optimize(objective, n_trials=max(1, int(trials)))
        rows.sort(key=lambda r: (r.rank_score, r.summary.net_pnl, r.summary.profit_factor), reverse=True)
        if progress_tracker is not None:
            progress_tracker.snapshot_top(rows=rows[:top_n], force=True)
        return rows[:top_n]

    @staticmethod
    def _rank(summary: ReplaySummary) -> float:
        pf = 10.0 if math.isinf(summary.profit_factor) else summary.profit_factor
        trade_penalty = 0.0 if summary.total_trades >= 20 else (20 - summary.total_trades) * 0.75
        return (
            summary.net_pnl * 1.0
            + pf * 18.0
            + summary.win_rate * 8.0
            + summary.avg_trade * 4.0
            - summary.max_drawdown * 0.40
            - trade_penalty
        )




def _run_grid_case_worker(payload: Dict[str, Any]) -> Dict[str, Any]:
    runner = LiquidationHistoricalReplayRunner(
        dsn=payload["dsn"],
        pool_dsn=payload["pool_dsn"],
        exchange_id=int(payload["exchange_id"]),
    )
    try:
        summary, _events = runner.replay(
            start=payload["start"],
            end=payload["end"],
            interval=payload["interval"],
            screener_params=payload["screener_params"],
            progress_every=10**9,
            max_points=payload.get("max_points"),
            replay_mode=payload.get("replay_mode", "fast"),
            use_candidate_timestamps=payload.get("use_candidate_timestamps", True),
            use_candidate_symbols=payload.get("use_candidate_symbols", True),
            candidate_liq_multiplier=payload.get("candidate_liq_multiplier", 0.35),
        )
        return {
            "generated_signals": summary.generated_signals,
            "deduped_signals": summary.deduped_signals,
            "summary": summary,
            "params": payload["screener_params"],
        }
    finally:
        runner.close()

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
    p = argparse.ArgumentParser(description="True historical optimizer for liquidation screener: regenerate signals from history, then backtest.")
    p.add_argument("--dsn", default=os.getenv("BACKTEST_DSN"), help="SQLAlchemy DSN")
    p.add_argument("--pool-dsn", default=os.getenv("BACKTEST_POOL_DSN"), help="psycopg pool conninfo")
    p.add_argument("--yaml", required=True, help="Path to screeners_liquidation.yaml")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--interval", required=True, help="5m / 15m / 1h")
    p.add_argument("--replay-mode", choices=["fast", "full"], default="fast")
    p.add_argument("--all-timestamps", action="store_true")
    p.add_argument("--all-symbols", action="store_true")
    p.add_argument("--candidate-liq-multiplier", type=float, default=0.35)
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--exchange-id", type=int, default=1)
    p.add_argument("--min-trades", type=int, default=20)
    p.add_argument("--top-n", type=int, default=20)
    p.add_argument("--progress-every", type=int, default=5)
    p.add_argument("--max-points", type=int, help="Optional cap for replay timestamps during debugging")
    p.add_argument("--csv-out", help="Optional path to save top results")
    p.add_argument("--csv-progress-out", help="Optional path to append every accepted result during optimization")
    p.add_argument("--csv-snapshot-out", help="Optional path to continuously rewrite current top-N results during optimization")
    p.add_argument("--snapshot-every", type=int, default=1, help="Rewrite top-N snapshot every N accepted results")
    p.add_argument("--resume", action="store_true", help="Resume optimization from existing progress/snapshot CSV if available")
    p.add_argument("--resume-from-csv", help="Explicit CSV path to resume from (prefers progress CSV with params_json)")
    p.add_argument("--resume-safe", action="store_true", help="Checkpoint every evaluated combination and force-save snapshot on Ctrl+C")
    p.add_argument("--search-mode", choices=["grid", "bayesian"], default="grid")
    p.add_argument("--trials", type=int, default=30, help="Trials for Bayesian search")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--study-name", help="Optional Optuna study name")

    p.add_argument("--volume-spike-k", default="2.0,2.4,2.8,3.2")
    p.add_argument("--liq-relative-k", default="1.4,1.8,2.0,2.4")
    p.add_argument("--confirm-body-min-pct", default="25,30,35,40")
    p.add_argument("--anchor-wick-to-range-min-pct", default="30,35,40,45")
    p.add_argument("--anchor-wick-to-body-min", default="1.2,1.4,1.6,1.8")
    p.add_argument("--anchor-close-recovery-min-pct", default="40,50,55,60")
    p.add_argument("--anchor-to-level-max-pct", default="0.12,0.18,0.24,0.30")
    p.add_argument("--followthrough-max-pct", default="0.08,0.12,0.16,0.20")
    p.add_argument("--min-level-cluster-strength", default="2,3")
    p.add_argument("--level-reaction-min-pct", default="0.25,0.35,0.45")
    p.add_argument("--require-htf-level-overlap", default="true,false")
    p.add_argument("--htf-level-tol-pct", default="0.003,0.004,0.005")
    p.add_argument("--max-anchor-candidates", default="2,3")
    p.add_argument("--trend-extension-max-pct", default="2.0,2.5,3.0")
    return p


def build_grid(args: argparse.Namespace) -> Dict[str, Sequence[Any]]:
    return {
        "volume_spike_k": parse_num_list(args.volume_spike_k),
        "liq_relative_k": parse_num_list(args.liq_relative_k),
        "confirm_body_min_pct": parse_num_list(args.confirm_body_min_pct),
        "anchor_wick_to_range_min_pct": parse_num_list(args.anchor_wick_to_range_min_pct),
        "anchor_wick_to_body_min": parse_num_list(args.anchor_wick_to_body_min),
        "anchor_close_recovery_min_pct": parse_num_list(args.anchor_close_recovery_min_pct),
        "anchor_to_level_max_pct": parse_num_list(args.anchor_to_level_max_pct),
        "followthrough_max_pct": parse_num_list(args.followthrough_max_pct),
        "min_level_cluster_strength": parse_num_list(args.min_level_cluster_strength),
        "level_reaction_min_pct": parse_num_list(args.level_reaction_min_pct),
        "require_htf_level_overlap": parse_num_list(args.require_htf_level_overlap),
        "htf_level_tol_pct": parse_num_list(args.htf_level_tol_pct),
        "max_anchor_candidates": parse_num_list(args.max_anchor_candidates),
        "trend_extension_max_pct": parse_num_list(args.trend_extension_max_pct),
    }


def print_results(rows: Sequence[OptimizationRow]) -> None:
    print("=" * 160)
    print("TOP LIQUIDATION TRUE-HISTORY OPTIMIZATION RESULTS")
    print("=" * 160)
    for i, row in enumerate(rows, start=1):
        s = row.summary
        pf = "inf" if math.isinf(s.profit_factor) else f"{s.profit_factor:.4f}"
        print(
            f"#{i:02d} mode={row.search_mode} rank={row.rank_score:.4f} interval={row.interval} "
            f"signals={row.generated_signals}/{row.deduped_signals} trades={s.total_trades} "
            f"pnl={s.net_pnl:.8f} pf={pf} wr={s.win_rate:.2%} dd={s.max_drawdown:.8f} params={row.params}"
        )
    print("=" * 160)


def save_csv(path: str, rows: Sequence[OptimizationRow]) -> None:
    fieldnames = [
        "search_mode", "rank_score", "interval", "generated_signals", "deduped_signals", "replay_points",
        "total_trades", "wins", "losses", "win_rate", "gross_profit", "gross_loss", "total_fees",
        "net_pnl", "avg_trade", "profit_factor", "max_drawdown",
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
                "search_mode": row.search_mode,
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
    optimizer = LiquidationHistoryOptimizer(
        dsn=args.dsn,
        pool_dsn=args.pool_dsn,
        exchange_id=args.exchange_id,
        replay_mode=args.replay_mode,
        use_candidate_timestamps=not bool(args.all_timestamps),
        use_candidate_symbols=not bool(args.all_symbols),
        candidate_liq_multiplier=float(args.candidate_liq_multiplier),
    )
    rows: List[OptimizationRow] = []
    try:
        grid = build_grid(args)
        if args.resume_safe:
            if not args.csv_progress_out:
                args.csv_progress_out = "artifacts/liquidation_resume_safe_progress.csv"
            if not args.csv_snapshot_out and not args.csv_out:
                args.csv_snapshot_out = "artifacts/liquidation_resume_safe_snapshot.csv"
            args.snapshot_every = 1
            _print(f"RESUME-SAFE enabled | progress={args.csv_progress_out} | snapshot={args.csv_snapshot_out or args.csv_out}")
        progress_tracker = ProgressCsvTracker(
            progress_csv=args.csv_progress_out,
            snapshot_csv=args.csv_snapshot_out or args.csv_out,
            snapshot_every=max(1, int(args.snapshot_every)),
        )
        resume_rows: List[OptimizationRow] = []
        if args.resume or args.resume_safe:
            resume_path = _pick_resume_path(args)
            resume_rows = load_resume_rows(resume_path)
            if resume_rows:
                _print(f"RESUME loaded {len(resume_rows)} rows from: {resume_path}")
            else:
                _print(f"RESUME no prior rows found at: {resume_path}")
        if args.search_mode == "bayesian":
            rows = optimizer.bayesian_search(
                start=parse_dt(args.start),
                end=parse_dt(args.end),
                interval=args.interval,
                base_params=base_params,
                grid=grid,
                min_trades=max(0, int(args.min_trades)),
                top_n=max(1, int(args.top_n)),
                trials=max(1, int(args.trials)),
                max_points=args.max_points,
                seed=int(args.seed),
                study_name=args.study_name,
                progress_tracker=progress_tracker,
                resume_rows=resume_rows,
            )
        else:
            rows = optimizer.grid_search(
                start=parse_dt(args.start),
                end=parse_dt(args.end),
                interval=args.interval,
                base_params=base_params,
                grid=grid,
                min_trades=max(0, int(args.min_trades)),
                top_n=max(1, int(args.top_n)),
                progress_every=max(1, int(args.progress_every)),
                max_points=args.max_points,
                workers=max(1, int(args.workers)),
                progress_tracker=progress_tracker,
                resume_rows=resume_rows,
            )
        print_results(rows)
        if args.csv_out:
            save_csv(args.csv_out, rows)
            print(f"Saved results to: {args.csv_out}")
        if args.csv_progress_out:
            print(f"Saved progress log to: {args.csv_progress_out}")
        if args.csv_snapshot_out or args.csv_out:
            print(f"Saved rolling snapshot to: {args.csv_snapshot_out or args.csv_out}")
    except KeyboardInterrupt:
        _print("INTERRUPTED by user. Saving latest snapshot before exit...")
        accepted_rows = sorted(rows, key=lambda r: (r.rank_score, r.summary.net_pnl, r.summary.profit_factor), reverse=True)
        snapshot_path = args.csv_snapshot_out or args.csv_out
        if snapshot_path and accepted_rows:
            save_csv(snapshot_path, accepted_rows[: max(1, int(args.top_n))])
            _print(f"Saved interrupt snapshot to: {snapshot_path}")
        raise SystemExit(130)
    finally:
        optimizer.close()


if __name__ == "__main__":
    main()
