# src/platform/run_screeners_liquidation.py
from __future__ import annotations

import os
import time
import logging
import shutil
import re
from pathlib import Path
from datetime import datetime, timezone, date, timedelta
from typing import Any, Dict, List, Tuple, Set, Optional

import yaml
from psycopg.errors import UniqueViolation

from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage
from src.platform.screeners.scr_liquidation_binance import ScrLiquidationBinance

from src.platform.notifications.telegram import (
    load_dotenv_file,
    resolve_targets_from_env,
    split_long_message,
    send_telegram_message,
    send_telegram_photo,
    send_telegram_document,
)

from src.platform.core.utils.candles import aggregate_candles, interval_to_timedelta, slice_window

log = logging.getLogger("platform.run_screeners_liquidation")

_PLOTS_CLEANUP_LAST: Dict[str, datetime] = {}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_dt_utc(v: Any) -> Optional[datetime]:
    """
    Нормализует value к timezone-aware UTC datetime.
    Поддерживает datetime и ISO-строки (включая суффикс 'Z').
    """
    if v is None:
        return None

    if isinstance(v, datetime):
        if v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v.astimezone(timezone.utc)

    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            s = s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
        except (ValueError, TypeError):
            return None

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    return None


def _jsonable(v: Any) -> Any:
    """JSON-safe для context (datetime/date -> isoformat, dict/list -> рекурсивно)."""
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, dict):
        return {str(k): _jsonable(val) for k, val in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    return str(v)


# -----------------------------
# plots cleanup
# -----------------------------
def _cleanup_plots_dir(plots_dir: Path, *, keep_days: int, now: Optional[datetime] = None) -> Tuple[int, int]:
    """
    Удаляет старые графики из plots_dir.

    Структура:
      plots_dir/<screener>/<symbol>/YYYY-MM-DD/*.png

    keep_days=2 => хранит сегодня и вчера, удаляет позавчера и старше.
    Возвращает (deleted_dirs, deleted_files).
    """
    if keep_days <= 0:
        return 0, 0

    plots_dir = Path(plots_dir)
    if not plots_dir.exists():
        return 0, 0

    now = now or _utc_now()
    cutoff_date = now.date() - timedelta(days=int(keep_days))
    cutoff_ts = datetime.combine(cutoff_date, datetime.min.time(), tzinfo=timezone.utc)

    date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")

    deleted_dirs = 0
    deleted_files = 0

    # удаляем папки дат
    for day_dir in plots_dir.glob("*/*/*"):
        if not day_dir.is_dir():
            continue
        name = day_dir.name
        if not date_re.match(name):
            continue
        try:
            d = datetime.strptime(name, "%Y-%m-%d").date()
        except ValueError:
            continue
        if d <= cutoff_date:
            try:
                shutil.rmtree(day_dir, ignore_errors=True)
                deleted_dirs += 1
            except OSError:
                pass

    # страховка: чистим png по mtime
    for p in plots_dir.rglob("*.png"):
        if not p.is_file():
            continue
        try:
            mt = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
        except OSError:
            continue
        if mt <= cutoff_ts:
            try:
                p.unlink(missing_ok=True)
                deleted_files += 1
            except OSError:
                pass

    return deleted_dirs, deleted_files


# -----------------------------
# YAML helpers
# -----------------------------
def _load_cfg() -> Dict[str, Any]:
    cfg_path = (
        os.getenv("SCREENERS_LIQUIDATION_CONFIG")
        or os.getenv("SCREENERS_CONFIG")
        or "config/screeners_liquidation.yaml"
    )
    p = Path(cfg_path)
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {cfg_path}")
    data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError("Config root must be a mapping")
    return data


def _get_screeners(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = cfg.get("screeners") or []
    return [it for it in items if isinstance(it, dict)] if isinstance(items, list) else []


def _normalize_intervals(v: Any) -> List[str]:
    if v is None:
        return []
    if isinstance(v, str):
        s = v.strip()
        return [s] if s else []
    if isinstance(v, (list, tuple)):
        out: List[str] = []
        for x in v:
            if x is None:
                continue
            s = str(x).strip()
            if s:
                out.append(s)
        return out
    s = str(v).strip()
    return [s] if s else []


def _normalize_float_list(v: Any) -> List[float]:
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        out: List[float] = []
        for x in v:
            if x is None:
                continue
            try:
                out.append(float(x))
            except (ValueError, TypeError):
                continue
        return out
    try:
        return [float(v)]
    except (ValueError, TypeError):
        return []


def _build_interval_liq_pairs(params: Dict[str, Any]) -> List[Tuple[str, Optional[float]]]:
    """
    intervals[i] -> volume_liquid_limit[i]
    Если лимитов меньше — для остальных берём последний.
    """
    intervals = _normalize_intervals(params.get("intervals") or params.get("interval") or "1h")
    if not intervals:
        intervals = ["1h"]

    liq_limits = [x for x in _normalize_float_list(params.get("volume_liquid_limit")) if x > 0]

    if not liq_limits:
        return [(itv, None) for itv in intervals]

    if len(liq_limits) == 1:
        return [(itv, float(liq_limits[0])) for itv in intervals]

    last = float(liq_limits[-1])
    out: List[Tuple[str, Optional[float]]] = []
    for i, itv in enumerate(intervals):
        v = float(liq_limits[i]) if i < len(liq_limits) else last
        out.append((str(itv), v))
    return out


# -----------------------------
# Telegram helpers (твои функции)
# -----------------------------
def _resolve_targets(params: dict) -> list:
    extras_enabled = bool(params.get("telegram_extras_enabled", False))
    max_friends = int(params.get("telegram_max_friends", 10) or 10)
    return resolve_targets_from_env(
        include_friends=extras_enabled,
        max_friends=max_friends,
        fallback_friend_token_to_primary=True,
    )


def _send_telegram_text_if_enabled(*, screener_name: str, timeframe: str, params: dict, rows: List[dict]) -> None:
    if not bool(params.get("telegram_enabled", False)):
        return
    if not bool(params.get("telegram_send_text", False)):
        return
    if not rows:
        return

    mode = str(params.get("telegram_mode", "batch")).strip().lower()
    if mode not in ("batch", "single"):
        mode = "batch"

    max_signals = int(params.get("telegram_max_signals", 20) or 20)
    rows = rows[: max(1, max_signals)]

    targets = _resolve_targets(params)
    if not targets:
        log.warning("Telegram enabled but no targets resolved (check env)")
        return

    def build_msg(rr: List[dict]) -> str:
        lines = [f"📣 {screener_name} | TF={timeframe} | signals={len(rr)}"]
        for row in rr:
            sym = str(row.get("symbol") or "")
            side = str(row.get("side") or "").upper()
            ts = row.get("signal_ts")
            ts_s = ts.isoformat() if isinstance(ts, datetime) else str(ts)
            entry = row.get("entry_price")
            sl = row.get("stop_loss")
            tp = row.get("take_profit")
            lines.append(f"• {sym} {side} | ts={ts_s} | entry={entry} sl={sl} tp={tp}")
        return "\n".join(lines)

    msgs: List[str] = []
    if mode == "single":
        for row in rows:
            msgs.extend(split_long_message(build_msg([row])))
    else:
        msgs = split_long_message(build_msg(rows))

    # noinspection PyBroadException
    try:
        ok = 0
        total = 0
        for msg in msgs:
            for t in targets:
                total += 1
                if send_telegram_message(msg, target=t):
                    ok += 1
        log.info("Telegram text sent ok=%d/%d parts=%d targets=%d", ok, total, len(msgs), len(targets))
    except Exception:
        log.exception("Telegram send failed")


def _send_telegram_plots_if_enabled(
    *, screener_name: str, timeframe: str, params: dict, plot_items: List[Tuple[Path, dict]]
) -> None:
    if not bool(params.get("telegram_enabled", False)):
        return
    if not bool(params.get("telegram_send_plots", False)):
        return
    if not plot_items:
        return

    mode = str(params.get("telegram_plot_mode", "photo")).strip().lower()
    if mode not in ("photo", "document"):
        mode = "photo"

    targets = _resolve_targets(params)
    if not targets:
        log.warning("Telegram plots enabled but no targets resolved (check env)")
        return

    # noinspection PyBroadException
    try:
        ok = 0
        total = 0
        for png_path, row in plot_items:
            sym = str(row.get("symbol") or "")
            side = str(row.get("side") or "").upper()
            ts = row.get("signal_ts")
            ts_s = ts.isoformat() if isinstance(ts, datetime) else str(ts)
            caption = f"{screener_name} | {sym} {timeframe} {side}\nts={ts_s}"

            for t in targets:
                total += 1
                if mode == "document":
                    sent = send_telegram_document(png_path, caption=caption, target=t)
                else:
                    sent = send_telegram_photo(png_path, caption=caption, target=t)
                if sent:
                    ok += 1

        log.info("Telegram plots sent ok=%d/%d items=%d targets=%d", ok, total, len(plot_items), len(targets))
    except Exception:
        log.exception("Telegram plot send failed")


# -----------------------------
# DB dedup helpers
# -----------------------------
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
            for row in cur.fetchall():
                out.add((int(row[0]), row[1]))
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
# main
# -----------------------------
def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        force=True,
    )

    # env (tokens etc.)
    try:
        load_dotenv_file(".env", override=False)
    except OSError:
        pass

    cfg_path = (
        os.getenv("SCREENERS_LIQUIDATION_CONFIG")
        or os.getenv("SCREENERS_CONFIG")
        or "config/screeners_liquidation.yaml"
    )
    log.info("=== RUN SCREENERS LIQUIDATION START ===")
    log.info("CONFIG=%s", cfg_path)

    cfg = _load_cfg()
    restart_interval_minutes = int(cfg.get("restart_interval_minutes", 5))
    items = _get_screeners(cfg)

    log.info("Screeners in config: %d", len(items))
    if not items:
        log.warning("No screeners found in config")
        return

    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise RuntimeError("PG_DSN env not set")

    pool = create_pool(dsn)
    store = PostgreSQLStorage(pool)

    screener_registry = {"scr_liquidation_binance": ScrLiquidationBinance}

    while True:
        total_inserted = 0
        total_skipped_dup = 0
        total_plots = 0

        # noinspection PyBroadException
        try:
            for item in items:
                if not bool(item.get("enabled", False)):
                    continue

                name = str(item.get("name", "")).strip()
                version = str(item.get("version", "0.1"))
                params: Dict[str, Any] = dict(item.get("params") or {})

                if not name:
                    continue
                if name not in screener_registry:
                    log.warning("Skip screener=%s (not implemented yet)", name)
                    continue

                exchange_id = 1
                pairs = _build_interval_liq_pairs(params)
                if not pairs:
                    pairs = [("1h", None)]

                log.info("------------------------------------------------------------")
                log.info("Run screener=%s v=%s mapping=%s", name, version, pairs)

                screener_id = store.ensure_screener(name=name, version=version)
                scr = screener_registry[name]()

                for idx, (interval, liq_limit) in enumerate(pairs):
                    params_i = dict(params)
                    params_i["interval"] = interval

                    cf_map = params_i.get("confirm_lookforward_map") or {}
                    if isinstance(cf_map, dict) and str(interval) in cf_map:
                        try:
                            params_i["confirm_lookforward"] = int(cf_map.get(str(interval)))
                        except (ValueError, TypeError):
                            pass

                    if liq_limit is not None:
                        params_i["volume_liquid_limit"] = float(liq_limit)

                    # volume_change_pct can be scalar OR list mapped by intervals order OR dict {interval: pct}
                    vcp = params.get("volume_change_pct")
                    if isinstance(vcp, list):
                        vlist = _normalize_float_list(vcp)
                        if vlist:
                            pick = float(vlist[idx]) if idx < len(vlist) else float(vlist[-1])
                            params_i["volume_change_pct"] = pick
                    elif isinstance(vcp, dict):
                        key = str(interval)
                        if key in vcp:
                            try:
                                params_i["volume_change_pct"] = float(vcp.get(key))
                            except (ValueError, TypeError):
                                pass

                    signals = scr.run(
                        storage=store,
                        exchange_id=int(exchange_id),
                        interval=str(interval),
                        params=params_i,
                    )
                    if not signals:
                        continue

                    # ✅ СВЕЖЕСТЬ: fresh_signal_minutes_map + close-time (signal_ts + interval)
                    now_ts = _utc_now()

                    fresh_map = params_i.get("fresh_signal_minutes_map") or {}
                    fresh_raw = fresh_map.get(str(interval)) if isinstance(fresh_map, dict) else None
                    try:
                        fresh_minutes = int(fresh_raw) if fresh_raw is not None else int(params_i.get("fresh_signal_minutes", 15))
                    except (ValueError, TypeError):
                        fresh_minutes = 15

                    if fresh_minutes > 0:
                        thr = now_ts - timedelta(minutes=fresh_minutes)
                        try:
                            delta = interval_to_timedelta(str(interval))
                        except (ValueError, TypeError):
                            delta = timedelta(0)

                        filtered = []
                        for sig in signals:
                            dt = _ensure_dt_utc(getattr(sig, "signal_ts", None))
                            if dt is None:
                                continue
                            sig.signal_ts = dt
                            dt_end = dt + delta
                            if dt_end >= thr:
                                filtered.append(sig)

                        if len(filtered) != len(signals):
                            log.info("Skip old signals (> %dm) interval=%s: %d", fresh_minutes, interval, len(signals) - len(filtered))
                        signals = filtered

                    if not signals:
                        continue

                    # dedup по (symbol_id, signal_ts)
                    pairs_key = [(int(sig.symbol_id), sig.signal_ts) for sig in signals]
                    existing = _fetch_existing_signal_pairs(
                        store,
                        exchange_id=exchange_id,
                        screener_id=screener_id,
                        timeframe=interval,
                        pairs=pairs_key,
                    )

                    new_signals = [sig for sig in signals if (int(sig.symbol_id), sig.signal_ts) not in existing]
                    if not new_signals:
                        continue

                    # funding snapshot (best-effort)
                    try:
                        symbol_ids = sorted({int(sig.symbol_id) for sig in new_signals})
                        funding_map = store.fetch_next_funding_bulk(exchange_id=int(exchange_id), symbol_ids=symbol_ids, as_of=now_ts)
                    except Exception:
                        funding_map = {}
                        log.warning("Funding snapshot load failed", exc_info=True)

                    rows: List[Dict[str, Any]] = []
                    packed: List[Dict[str, Any]] = []
                    packed_by_key: Dict[Tuple[int, datetime], Dict[str, Any]] = {}

                    for sig in new_signals:
                        signal_day = sig.signal_ts.date()
                        ctx = _jsonable(dict(sig.context or {}))

                        # Optional fields for signals table (may be absent in screener Signal object)
                        confidence = getattr(sig, "confidence", None)
                        score = getattr(sig, "score", None)
                        reason = getattr(sig, "reason", None)
                        source = getattr(sig, "source", None) or str(name)

                        # If reason not provided explicitly, try to take it from context
                        if not reason:
                            r0 = ctx.get("reason") or ctx.get("signal_reason")
                            if isinstance(r0, str) and r0.strip():
                                reason = r0.strip()

                        f = funding_map.get(int(sig.symbol_id), {}) or {}
                        ctx["funding_time"] = f.get("funding_time")
                        ctx["funding_rate"] = f.get("funding_rate")
                        try:
                            fr = f.get("funding_rate")
                            ctx["funding_pct"] = (float(fr) * 100.0) if fr is not None else None
                        except (ValueError, TypeError):
                            ctx["funding_pct"] = None

                        day_seq = store.next_signal_seq(
                            exchange_id=exchange_id,
                            symbol_id=int(sig.symbol_id),
                            screener_id=int(screener_id),
                            signal_day=signal_day,
                        )
                        ctx["day_seq"] = int(day_seq)

                        row = {
                            "exchange_id": int(exchange_id),
                            "symbol_id": int(sig.symbol_id),
                            "symbol": str(sig.symbol or ""),
                            "screener_id": int(screener_id),
                            "timeframe": str(interval),
                            "signal_ts": sig.signal_ts,
                            "signal_day": signal_day,
                            "day_seq": int(day_seq),
                            "side": str(sig.side).upper(),
                            "status": "NEW",
                            "entry_price": sig.entry_price,
                            "exit_price": sig.exit_price,
                            "stop_loss": sig.stop_loss,
                            "take_profit": sig.take_profit,

                            # required placeholders in SQL insert_signals()
                            "confidence": confidence,
                            "score": score,
                            "reason": reason,
                            "source": source,

                            "context": ctx,
                        }
                        rows.append(row)

                        p = {
                            "symbol_id": int(sig.symbol_id),
                            "symbol": str(sig.symbol or ""),
                            "timeframe": str(interval),
                            "signal_ts": sig.signal_ts,
                            "day_seq": int(day_seq),
                            "side": str(sig.side).upper(),
                            "entry_price": sig.entry_price,
                            "stop_loss": sig.stop_loss,
                            "take_profit": sig.take_profit,
                            "context": ctx,
                        }
                        packed.append(p)
                        packed_by_key[(int(sig.symbol_id), sig.signal_ts)] = p

                    inserted = _insert_signals_safe(store, rows)
                    if inserted <= 0:
                        continue

                    total_inserted += inserted
                    _send_telegram_text_if_enabled(screener_name=name, timeframe=interval, params=params_i, rows=packed)

                    # --------------------------
                    # plots
                    # --------------------------
                    enable_plots = bool(params_i.get("enable_plots", False))
                    if not enable_plots:
                        continue

                    plots_dir = Path(str(params_i.get("plots_dir", "artifacts/screener_plots")))
                    plots_keep_days = int(params_i.get("plots_keep_days", 2) or 0)
                    plots_cleanup_every_minutes = int(params_i.get("plots_cleanup_every_minutes", 60) or 0)

                    if plots_keep_days > 0 and plots_cleanup_every_minutes > 0:
                        key = str(plots_dir)
                        last = _PLOTS_CLEANUP_LAST.get(key)
                        need = (last is None) or ((now_ts - last).total_seconds() >= plots_cleanup_every_minutes * 60)
                        if need:
                            _PLOTS_CLEANUP_LAST[key] = now_ts
                            dd, df = _cleanup_plots_dir(plots_dir, keep_days=plots_keep_days, now=now_ts)
                            if dd or df:
                                log.info("Plots cleanup: deleted_dirs=%d deleted_files=%d keep_days=%d", dd, df, plots_keep_days)

                    from src.platform.screeners.plotting_liquidation import save_signal_plot  # local import

                    lookback = int(params_i.get("plot_lookback", 120) or 120)
                    look_fwd = int(params_i.get("plot_lookforward", 40) or 40)
                    max_plots = int(params_i.get("telegram_max_plot_signals", 20) or 20)
                    send_plots = bool(params_i.get("telegram_send_plots", False))

                    plot_items: List[Tuple[Path, dict]] = []

                    # noinspection PyBroadException
                    try:
                        for sig in new_signals[: max(1, max_plots)]:
                            center_ts = sig.signal_ts

                            # candles (live agg optional)
                            candles = []
                            live_enabled = bool(params_i.get("live_candles_enabled", True))
                            live_prefer_agg = bool(params_i.get("live_prefer_agg", True))
                            live_base_interval = str(params_i.get("live_base_interval", "15m"))
                            live_extra_base_bars = int(params_i.get("live_extra_base_bars", 64) or 64)

                            if live_enabled and live_prefer_agg and live_base_interval and live_base_interval != interval:
                                base_td = interval_to_timedelta(live_base_interval)
                                tgt_td = interval_to_timedelta(interval)
                                base_sec = int(base_td.total_seconds())
                                tgt_sec = int(tgt_td.total_seconds())
                                if base_sec > 0 and tgt_sec > 0 and (tgt_sec % base_sec == 0):
                                    ratio = max(1, int(tgt_sec // base_sec))
                                    base_lb = int(lookback) * ratio + int(live_extra_base_bars)
                                    base_fw = int(look_fwd) * ratio + max(0, int(live_extra_base_bars // 2))

                                    base_candles = store.fetch_candles_window(
                                        exchange_id=exchange_id,
                                        symbol_id=sig.symbol_id,
                                        interval=live_base_interval,
                                        center_ts=center_ts,
                                        lookback=base_lb,
                                        lookforward=base_fw,
                                    )
                                    if base_candles:
                                        agg = aggregate_candles(base_candles, target_interval=interval)
                                        candles = slice_window(
                                            agg,
                                            center_ts=center_ts,
                                            interval=interval,
                                            lookback=int(lookback),
                                            lookforward=int(look_fwd),
                                        )

                            if not candles:
                                candles = store.fetch_candles_window(
                                    exchange_id=exchange_id,
                                    symbol_id=sig.symbol_id,
                                    interval=interval,
                                    center_ts=center_ts,
                                    lookback=int(lookback),
                                    lookforward=int(look_fwd),
                                )

                            if not candles:
                                continue

                            # liq series (используется!)
                            liq_series = None
                            try:
                                start_ts = candles[0]["ts"]
                                end_ts = candles[-1]["ts"] + interval_to_timedelta(interval)
                                liq_series = store.fetch_liquidations_window(
                                    exchange_id=exchange_id,
                                    symbol_id=sig.symbol_id,
                                    interval=interval,
                                    start_ts=start_ts,
                                    end_ts=end_ts,
                                )
                            except Exception:
                                liq_series = None

                            ctx_plot = dict(sig.context or {})
                            touch_ts = ctx_plot.get("touch_ts")

                            day_folder = plots_dir / name / str(sig.symbol) / str(sig.signal_ts.date())
                            day_folder.mkdir(parents=True, exist_ok=True)
                            out_png = day_folder / f"{sig.symbol}_{interval}_{sig.side}_{sig.signal_ts.strftime('%Y%m%d_%H%M%S')}.png"

                            save_signal_plot(
                                out_path=out_png,
                                symbol=str(sig.symbol),
                                timeframe=str(interval),
                                candles=candles,
                                entry_ts=sig.signal_ts,
                                touch_ts=touch_ts,
                                side=str(sig.side),
                                entry_price=float(sig.entry_price),
                                up_level=ctx_plot.get("up_level"),
                                down_level=ctx_plot.get("down_level"),
                                stop_loss=sig.stop_loss,
                                take_profit=sig.take_profit,
                                liquidation_series=liq_series,
                                liq_short_usdt=ctx_plot.get("liq_short_usdt"),
                                liq_long_usdt=ctx_plot.get("liq_long_usdt"),
                                volume_avg=ctx_plot.get("avg_vol"),
                                volume_anchor=ctx_plot.get("volume"),
                                volume_change_pct=ctx_plot.get("volume_change_pct"),
                                volume_threshold=ctx_plot.get("volume_threshold"),
                                anchor_ts=ctx_plot.get("anchor_ts"),
                            )

                            total_plots += 1
                            if send_plots:
                                key2 = (int(sig.symbol_id), sig.signal_ts)
                                row2 = packed_by_key.get(key2)
                                if row2:
                                    plot_items.append((out_png, row2))
                    except Exception:
                        log.exception("Plot generation failed")

                    if plot_items:
                        _send_telegram_plots_if_enabled(screener_name=name, timeframe=interval, params=params_i, plot_items=plot_items)

        except Exception:
            log.exception("RUN SCREENERS FAILED")

        log.info("=== RUN SCREENERS DONE === inserted=%d skipped_dup=%d plots=%d", total_inserted, total_skipped_dup, total_plots)
        log.info("Sleeping for %d minutes before next run...", restart_interval_minutes)
        time.sleep(restart_interval_minutes * 60)


if __name__ == "__main__":
    main()
