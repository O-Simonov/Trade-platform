# src/platform/run_screener_oi.py
from __future__ import annotations

import os
import time
import logging
import shutil
from pathlib import Path
from datetime import datetime, timezone, date, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml
from psycopg.errors import UniqueViolation

from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage
from src.platform.screeners.scr_oi_binance import ScrOiBinance, ScreenerSignal
from src.platform.screeners.plotting_oi import save_oi_signal_plot

# Telegram helpers (same style as run_screeners.py)
from src.platform.notifications.telegram import (
    load_dotenv_file,
    resolve_targets_from_env,
    split_long_message,
    send_telegram_message,
    send_telegram_photo,
)

from src.platform.core.utils.candles import interval_to_timedelta

log = logging.getLogger("platform.run_screener_oi")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# -----------------------------
# JSON SAFE (datetime in context)
# -----------------------------
def _jsonable(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, dict):
        return {str(k): _jsonable(val) for k, val in v.items()}
    if isinstance(v, (list, tuple, set)):
        return [_jsonable(x) for x in v]
    return str(v)


def _load_cfg() -> dict:
    cfg_path = os.getenv("OI_SCREENERS_CONFIG", "config/screener_oi.yaml")
    p = Path(cfg_path)
    if not p.exists():
        raise FileNotFoundError(f"OI_SCREENERS_CONFIG not found: {p}")
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}


# -----------------------------
# per-interval overrides
# -----------------------------
def _apply_interval_overrides(params: dict, interval: str) -> dict:
    """
    Возвращает params с применёнными overrides для конкретного interval.

    Поддерживается ключ в YAML:
      per_interval_overrides:
        "5m":
          base_minutes: 240
          trigger_minutes: 20
        "15m":
          base_minutes: 720
          trigger_minutes: 60

    Правило:
      - берём копию base params
      - накладываем overrides[interval] поверх (shallow merge)
      - дополнительно выставляем params["interval"]=interval для удобства логов
    """
    base = dict(params or {})
    overrides = base.get("per_interval_overrides") or {}
    if isinstance(overrides, dict):
        ov = overrides.get(interval) or overrides.get(str(interval)) or {}
        if isinstance(ov, dict) and ov:
            base.update(ov)
    base["interval"] = interval
    base.pop("per_interval_overrides", None)
    return base


def _get_screeners(cfg: dict) -> List[dict]:
    if isinstance(cfg, list):
        return cfg
    if isinstance(cfg, dict) and isinstance(cfg.get("screeners"), list):
        return cfg["screeners"]
    return []


def _fetch_existing_signal_pairs(
    store: PostgreSQLStorage,
    *,
    exchange_id: int,
    screener_id: int,
    timeframe: str,
    pairs: List[Tuple[int, datetime]],
) -> Set[Tuple[int, datetime]]:
    if not pairs:
        return set()

    placeholders = ",".join(["(%s,%s)"] * len(pairs))

    sql = f"""
    SELECT symbol_id, signal_ts
    FROM public.signals
    WHERE exchange_id=%s
      AND screener_id=%s
      AND timeframe=%s
      AND (symbol_id, signal_ts) IN ({placeholders})
    """

    params: list[Any] = [int(exchange_id), int(screener_id), str(timeframe)]
    for sid, ts in pairs:
        params.append(int(sid))
        params.append(ts)

    out: Set[Tuple[int, datetime]] = set()
    with store.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for r in cur.fetchall():
                out.add((int(r[0]), r[1]))
    return out


def _insert_signals_safe(store: PostgreSQLStorage, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    try:
        res = store.insert_signals(rows)
        return int(res) if isinstance(res, int) else len(rows)
    except UniqueViolation:
        log.warning("Signals insert skipped: duplicates (UniqueViolation)")
        return 0


# -----------------------------
# Telegram targets
# -----------------------------
def _resolve_telegram_targets(params: dict) -> list:
    extras_enabled = bool(params.get("telegram_extras_enabled", False))
    max_friends = int(params.get("telegram_max_friends", 10))

    return resolve_targets_from_env(
        include_friends=extras_enabled,
        max_friends=max_friends,
        fallback_friend_token_to_primary=True,
    )


def _fmt_f(v: Any, nd: int = 3) -> str:
    try:
        if v is None:
            return "-"
        x = float(v)
        fmt = f"{{:.{nd}f}}"
        return fmt.format(x)
    except Exception:
        return "-"


def _build_signal_line(s: ScreenerSignal) -> str:
    ctx = s.context or {}
    oi_ret = ctx.get("oi_trigger_ret_pct")
    vol_mult = ctx.get("trigger_vol_mult")
    pr_ret = ctx.get("price_trigger_ret_pct")
    fund = ctx.get("funding_pct")

    return (
        f"{s.symbol} {s.side}  entry={_fmt_f(s.entry_price, 6)}  "
        f"SL={_fmt_f(s.stop_loss, 6)}  TP={_fmt_f(s.take_profit, 6)}\n"
        f"  OI={_fmt_f(oi_ret, 2)}%/{ctx.get('trigger_minutes','?')}m  "
        f"Vol x{_fmt_f(vol_mult, 2)}  Price={_fmt_f(pr_ret, 2)}%  Funding={_fmt_f(fund, 4)}%"
    )


def _build_telegram_message(name: str, interval: str, signals: List[ScreenerSignal]) -> str:
    now = _utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")
    head = f"🔎 {name}  interval={interval}  {now}\n"
    if not signals:
        return head + "No signals."
    lines = []
    for s in signals[:10]:
        lines.append(_build_signal_line(s))
    return head + "\n\n".join(lines)


# -----------------------------
# OI series fetch for plotting
# -----------------------------
def _fetch_oi_series(
    store: PostgreSQLStorage,
    *,
    exchange_id: int,
    symbol_id: int,
    interval: str,
    start_ts: datetime,
    end_ts: datetime,
) -> List[Dict[str, Any]]:
    sql = """
        SELECT ts, open_interest, open_interest_value
        FROM open_interest
        WHERE exchange_id = %s
          AND symbol_id = %s
          AND interval = %s
          AND ts >= %s
          AND ts <= %s
        ORDER BY ts
    """
    with store.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (int(exchange_id), int(symbol_id), str(interval), start_ts, end_ts))
            rows = cur.fetchall() or []
            cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]


# -----------------------------
# plots retention
# -----------------------------
def _cleanup_old_plots(root: Path, *, keep_days: int) -> None:
    if keep_days <= 0:
        return
    if not root.exists():
        return

    cutoff = (_utc_now().date() - timedelta(days=int(keep_days)))

    # root/.../<SYMBOL>/<YYYY-MM-DD>/files.png
    for sym_dir in root.iterdir():
        if not sym_dir.is_dir():
            continue

        for day_dir in sym_dir.iterdir():
            if not day_dir.is_dir():
                continue
            try:
                d = date.fromisoformat(day_dir.name)
            except Exception:
                continue
            if d < cutoff:
                try:
                    shutil.rmtree(day_dir, ignore_errors=True)
                except Exception:
                    pass

        # remove empty symbol folder
        try:
            if sym_dir.exists() and sym_dir.is_dir() and not any(sym_dir.iterdir()):
                sym_dir.rmdir()
        except Exception:
            pass


# -----------------------------
# runner
# -----------------------------
def run_once(store: PostgreSQLStorage, screener_cfg: dict) -> None:
    name = str(screener_cfg.get("name", "")).strip()
    enabled = bool(screener_cfg.get("enabled", False))
    version = str(screener_cfg.get("version", "0.1")).strip() or "0.1"
    params = screener_cfg.get("params") or {}

    if not enabled:
        return

    registry = {
        "scr_oi_binance": ScrOiBinance,
    }
    if name not in registry:
        log.warning("Unknown screener name=%s (skip)", name)
        return

    exchange_id = int(params.get("exchange_id", 1))
    # interval(s): allow either "interval: 5m" or "intervals: [5m, 15m]"
    _raw_intervals = params.get("intervals", params.get("interval", "5m"))
    if isinstance(_raw_intervals, (list, tuple)):
        intervals = [str(x).strip() for x in _raw_intervals if str(x).strip()]
    else:
        s = str(_raw_intervals).strip()
        if "," in s:
            intervals = [x.strip() for x in s.split(",") if x.strip()]
        else:
            intervals = [s or "5m"]
    if not intervals:
        intervals = ["5m"]

    # plot settings
    plots_enabled = bool(params.get("plots_enabled", True))
    plots_dir = Path(params.get("plots_dir", "artifacts/screener_plots"))
    keep_plots_days = int(params.get("keep_plots_days", 2))
    plot_lookback_bars = int(params.get("plot_lookback_bars", 220))

    # telegram settings
    tg_send_messages = bool(params.get("telegram_send_messages", True))
    tg_send_plots = bool(params.get("telegram_send_plots", True))
    tg_silent = bool(params.get("telegram_silent", False))
    screener_id = store.ensure_screener(name=name, version=version, description="Open Interest screener (multi-tf)")
    scr = registry[name]()  # type: ignore

    for interval in intervals:
        interval = str(interval).strip() or "5m"
        params_i = _apply_interval_overrides(params, interval)
        max_signal_age_minutes_i = int(params_i.get("max_signal_age_minutes", 30))
        run_id = store.start_screener_run(screener_id=screener_id, interval=interval)
        t0 = time.time()
        inserted = 0
        total_found = 0
        total_new = 0

        try:
            signals = scr.run(storage=store, exchange_id=exchange_id, interval=interval, params=params_i) or []
            total_found = len(signals)

            # freshness filter
            now_ts = _utc_now()
            fresh: List[ScreenerSignal] = []
            for s in signals:
                age_min = (now_ts - s.signal_ts).total_seconds() / 60.0
                if age_min < -5:  # future signal -> skip
                    continue
                if age_min <= float(max_signal_age_minutes_i):
                    fresh.append(s)

            # dedup
            pairs = [(s.symbol_id, s.signal_ts) for s in fresh]
            existing = _fetch_existing_signal_pairs(store, exchange_id=exchange_id, screener_id=screener_id, timeframe=interval, pairs=pairs)
            new_signals = [s for s in fresh if (s.symbol_id, s.signal_ts) not in existing]
            total_new = len(new_signals)

            # insert rows
            rows: List[Dict[str, Any]] = []
            for s in new_signals:
                signal_day = s.signal_ts.date()
                day_seq = store.next_signal_seq(exchange_id=exchange_id, symbol_id=s.symbol_id, screener_id=screener_id, signal_day=signal_day)

                rows.append(
                    {
                        "exchange_id": int(exchange_id),
                        "symbol_id": int(s.symbol_id),
                        "symbol": str(s.symbol),
                        "screener_id": int(screener_id),
                        "timeframe": str(interval),
                        "signal_ts": s.signal_ts,
                        "side": str(s.side).upper(),
                        "status": "NEW",
                        "source": "screener",
                        "entry_price": float(s.entry_price),
                        "stop_loss": float(s.stop_loss) if s.stop_loss is not None else None,
                        "take_profit": float(s.take_profit) if s.take_profit is not None else None,
                        "exit_price": float(s.exit_price) if s.exit_price is not None else None,
                        "confidence": float(s.confidence) if s.confidence is not None else None,
                        "score": float(s.score) if s.score is not None else None,
                        "reason": str(s.reason) if s.reason is not None else None,
                        "context": _jsonable(s.context) if s.context is not None else None,
                        "signal_day": signal_day,
                        "day_seq": int(day_seq),
                    }
                )

            inserted = _insert_signals_safe(store, rows)

            # telegram
            if inserted > 0 and (tg_send_messages or tg_send_plots):
                targets = _resolve_telegram_targets(params)

                if tg_send_messages:
                    msg = _build_telegram_message(name=name, interval=interval, signals=new_signals)
                    parts = split_long_message(msg)
                    for part in parts:
                        for t in targets:
                            send_telegram_message(part, target=t, disable_notification=bool(tg_silent))

                # plots (save + optional send)
                if bool(params_i.get('plots_enabled', plots_enabled)) or tg_send_plots:
                    td = interval_to_timedelta(interval)
                    # OI key for plotting (can differ from calc key)
                    oi_plot_key = str(
                        params_i.get("oi_plot_key")
                        or params_i.get("oi_value_key")  # backward-compat
                        or "open_interest_value"
                    ).strip()
                    if oi_plot_key not in ("open_interest_value", "open_interest"):
                        oi_plot_key = "open_interest_value"
                    for s in new_signals:
                        signal_day = s.signal_ts.date().isoformat()
                        ts_tag = s.signal_ts.strftime("%Y%m%d_%H%M%S")
                        out_path = plots_dir / name / s.symbol / signal_day / f"{s.symbol}_{interval}_{s.side}_{ts_tag}.png"

                        # candles window for plot
                        center = s.signal_ts
                        lookback = max(50, int(params_i.get('plot_lookback_bars', plot_lookback_bars)))
                        candles = store.fetch_candles_window(
                            exchange_id=int(exchange_id),
                            symbol_id=int(s.symbol_id),
                            interval=str(interval),
                            center_ts=center,
                            lookback=lookback,
                            lookforward=0,
                        ) or []

                        # oi series for plot (same range)
                        start_ts = center - td * lookback
                        end_ts = center + td * 2
                        oi_rows = _fetch_oi_series(
                            store,
                            exchange_id=exchange_id,
                            symbol_id=s.symbol_id,
                            interval=interval,
                            start_ts=start_ts,
                            end_ts=end_ts,
                        )
                        oi_series = []
                        for r in oi_rows:
                            ts = r.get("ts")
                            v = r.get(oi_plot_key)
                            oi_series.append({"ts": ts, "value": v})

                        title_extra = ""
                        try:
                            ctx = s.context or {}
                            funding_pct = ctx.get('funding_pct', None)
                            funding_s = '' if funding_pct is None else f" | Funding {float(funding_pct):.4g}%"
                            title_extra = (
                                f"{oi_plot_key} | "
                                f"OI {ctx.get('oi_trigger_ret_pct', 0):.2f}% | "
                                f"OI$ {ctx.get('oi_value_trigger_ret_pct', 0):.2f}% | "
                                f"Vol x{ctx.get('trigger_vol_mult', 0):.2f}" + funding_s
                            )
                        except Exception:
                            title_extra = ""

                        save_oi_signal_plot(
                            out_path=out_path,
                            symbol=s.symbol,
                            timeframe=interval,
                            candles=candles,
                            signal_ts=s.signal_ts,
                            side=s.side,
                            entry_price=float(s.entry_price),
                            stop_loss=float(s.stop_loss) if s.stop_loss is not None else None,
                            take_profit=float(s.take_profit) if s.take_profit is not None else None,
                            oi_series=oi_series,
                            oi_value_key="value",
                            title_extra=title_extra,
                        )

                        if tg_send_plots:
                            caption = (
                                f"{s.symbol} {s.side} {interval} "
                                f"entry={_fmt_f(s.entry_price, 6)} "
                                f"SL={_fmt_f(s.stop_loss, 6)} "
                                f"TP={_fmt_f(s.take_profit, 6)}"
                            )
                            for t in targets:
                                send_telegram_photo(
                                    str(out_path),
                                    caption=caption,
                                    target=t,
                                    disable_notification=bool(tg_silent),
                                )

                    # retention (keep only last N days)
                    _cleanup_old_plots(plots_dir / name, keep_days=int(params_i.get('keep_plots_days', keep_plots_days)))

            elapsed = time.time() - t0
            store.finish_screener_run(
                run_id=run_id,
                status="OK",
                stats={
                    "found": int(total_found),
                    "fresh": int(len(fresh)),
                    "new": int(total_new),
                    "inserted": int(inserted),
                    "elapsed_sec": float(elapsed),
                },
            )

            log.info(
                "Run %s interval=%s found=%s new=%s inserted=%s elapsed=%.2fs",
                name,
                interval,
                total_found,
                total_new,
                inserted,
                elapsed,
            )
        except Exception as e:
            store.finish_screener_run(run_id=run_id, status="FAILED", error=str(e), stats={"found": int(total_found)})
            raise


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    # env (tokens etc.)
    try:
        load_dotenv_file(".env", override=False)
    except OSError:
        pass

    cfg = _load_cfg()
    restart_min = int(cfg.get("restart_interval_minutes", 5))
    screeners = _get_screeners(cfg)

    log.info("=== RUN OI SCREENER START ===")
    log.info("OI_SCREENERS_CONFIG=%s", os.getenv("OI_SCREENERS_CONFIG", "config/screener_oi.yaml"))
    log.info("Screeners in config: %s", len(screeners))
    log.info("------------------------------------------------------------")

    dsn = os.getenv("PG_DSN") or os.getenv("POSTGRES_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("PG_DSN (or POSTGRES_DSN/DATABASE_URL) env is not set")

    pool = create_pool(dsn)
    store = PostgreSQLStorage(pool)

    while True:
        for sc in screeners:
            run_once(store, sc)

        if restart_min <= 0:
            break

        time.sleep(max(5, restart_min * 60))


if __name__ == "__main__":
    main()