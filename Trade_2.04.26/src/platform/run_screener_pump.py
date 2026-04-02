# src/platform/run_screener_pump.py
from __future__ import annotations

import time
import logging
from pathlib import Path
from datetime import datetime, timezone, date, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml
from psycopg.errors import UniqueViolation

from src.platform.config.env import (
    get_environment,
    get_log_level,
    get_pg_dsn,
    require,
)
from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage
from src.platform.screeners.scr_pump_binance import ScrPumpBinance, PumpConfig
from src.platform.notifications import telegram as tg
from src.platform.core.utils.candles import interval_to_timedelta

log = logging.getLogger("platform.run_screener_pump")


def _setup_logging() -> None:
    level_name = get_log_level()
    level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        force=True,
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# -----------------------------
# JSON SAFE (fix datetime in context)
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


def _resolve_cfg_path() -> Path:
    raw = require("PUMP_SCREENERS_CONFIG")
    p = Path(raw).expanduser()

    if not p.is_absolute():
        p = (Path.cwd() / p).resolve()
    else:
        p = p.resolve()

    if not p.exists():
        raise FileNotFoundError(f"PUMP_SCREENERS_CONFIG not found: {p}")

    if not p.is_file():
        raise FileNotFoundError(f"PUMP_SCREENERS_CONFIG is not a file: {p}")

    return p


def _load_cfg(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


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


def _allocate_day_seqs_batch(
    store: PostgreSQLStorage,
    *,
    exchange_id: int,
    screener_id: int,
    items: List[Tuple[int, date]],
) -> Dict[Tuple[int, date], List[int]]:
    if not items:
        return {}

    grouped: Dict[Tuple[int, date], int] = {}
    for symbol_id, signal_day in items:
        key = (int(symbol_id), signal_day)
        grouped[key] = grouped.get(key, 0) + 1

    ordered = sorted(grouped.items(), key=lambda kv: (kv[0][0], kv[0][1]))
    values_sql = ",".join(["(%s,%s,%s,%s,%s)"] * len(ordered))
    sql = f"""
    WITH req(exchange_id, symbol_id, screener_id, signal_day, inc) AS (
        VALUES {values_sql}
    ), upsert AS (
        INSERT INTO public.signals_day_seq (exchange_id, symbol_id, screener_id, signal_day, last_seq)
        SELECT exchange_id, symbol_id, screener_id, signal_day, inc
        FROM req
        ON CONFLICT (exchange_id, symbol_id, screener_id, signal_day)
        DO UPDATE SET last_seq = public.signals_day_seq.last_seq + EXCLUDED.last_seq
        RETURNING symbol_id, signal_day, last_seq
    )
    SELECT symbol_id, signal_day, last_seq
    FROM upsert
    """

    params: List[Any] = []
    for (symbol_id, signal_day), inc in ordered:
        params.extend([int(exchange_id), int(symbol_id), int(screener_id), signal_day, int(inc)])

    out: Dict[Tuple[int, date], List[int]] = {}
    with store.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for symbol_id, signal_day, last_seq in cur.fetchall() or []:
                key = (int(symbol_id), signal_day)
                inc = grouped[key]
                start_seq = int(last_seq) - int(inc) + 1
                out[key] = list(range(start_seq, int(last_seq) + 1))
        conn.commit()
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

    return tg.resolve_targets_from_env(
        include_friends=extras_enabled,
        max_friends=max_friends,
        fallback_friend_token_to_primary=True,
    )


def _fmt_price(x: Any) -> str:
    try:
        if x is None:
            return "—"
        v = float(x)
        s = f"{v:.8f}".rstrip("0").rstrip(".")
        return s if s else str(v)
    except (TypeError, ValueError):
        return "—"


def _fmt_pct(x: Any) -> str:
    try:
        if x is None:
            return "—"
        v = float(x)
        s = f"{v:.3f}".rstrip("0").rstrip(".")
        return s if s else str(v)
    except (TypeError, ValueError):
        return "—"


def _fmt_time_left_seconds(x: Any) -> str:
    try:
        if x is None:
            return "—"
        sec = int(round(float(x)))
        if sec < 0:
            sec = 0
        h = sec // 3600
        m = (sec % 3600) // 60
        s = sec % 60
        if h > 0:
            return f"{h}h {m:02d}m {s:02d}s"
        if m > 0:
            return f"{m}m {s:02d}s"
        return f"{s}s"
    except (TypeError, ValueError):
        return "—"


def _to_local(dt_utc: datetime, tz_name: Optional[str]) -> datetime:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)

    if tz_name:
        try:
            from zoneinfo import ZoneInfo
            return dt_utc.astimezone(ZoneInfo(tz_name))
        except Exception:
            return dt_utc.astimezone()

    return dt_utc.astimezone()


def _fmt_local_ts(dt_utc: datetime, tz_name: Optional[str]) -> str:
    d = _to_local(dt_utc, tz_name)
    return d.strftime("%d.%m.%Y %H:%M %Z")


def _dir_mark(side: str) -> str:
    s = str(side or "").upper()
    if s == "BUY":
        return "🟢↑"
    if s == "SELL":
        return "🔴↓"
    return "➡️"


def _fmt_entry_sl_tp(row: Dict[str, Any]) -> str:
    entry = _fmt_price(row.get("entry_price"))
    sl = _fmt_price(row.get("stop_loss"))
    tp = _fmt_price(row.get("take_profit"))

    sl_pct = row.get("stop_loss_pct")
    tp_pct = row.get("take_profit_pct")

    slp = ""
    tpp = ""
    try:
        if sl_pct is not None and float(sl_pct) > 0:
            slp = f" ({_fmt_pct(sl_pct)}%)"
    except Exception:
        slp = ""
    try:
        if tp_pct is not None and float(tp_pct) > 0:
            tpp = f" ({_fmt_pct(tp_pct)}%)"
    except Exception:
        tpp = ""

    return f"Entry {entry} | SL {sl}{slp} | TP {tp}{tpp}"


def _build_pump_batch_message(
    *,
    screener_name: str,
    timeframe: str,
    rows: List[Dict[str, Any]],
    tz_name: Optional[str],
) -> str:
    header_ts = _fmt_local_ts(_utc_now(), tz_name)
    out: List[str] = [f"🚀 {screener_name} | TF {timeframe} | {header_ts}", ""]

    for i, r in enumerate(rows, start=1):
        symbol = str(r.get("symbol") or "")
        side = str(r.get("side") or "").upper()
        day_seq = int(r.get("day_seq") or 0)

        pump_pct = _fmt_pct(r.get("pump_pct"))
        win_min = r.get("window_minutes")

        funding_pct = _fmt_pct(r.get("funding_pct"))
        funding_left = _fmt_time_left_seconds(r.get("funding_time_left_sec"))

        ts = r.get("signal_ts")
        ts_local = _fmt_local_ts(ts, tz_name) if isinstance(ts, datetime) else str(ts or "—")

        out.append(f"{i}) {_dir_mark(side)} 🪙 {symbol} | {side} | {_fmt_entry_sl_tp(r)}")
        out.append(f"   ΔPrice {pump_pct}% за {win_min}m")
        out.append(f"   💸 Funding {funding_pct}% | next in {funding_left}")
        out.append(f"   ⏱️ {ts_local} | 🧾 #{day_seq}")
        out.append("")

    return "\n".join(out).strip()


def _build_pump_plot_caption(
    *,
    screener_name: str,
    timeframe: str,
    row: Dict[str, Any],
    tz_name: Optional[str],
) -> str:
    symbol = str(row.get("symbol") or "")
    side = str(row.get("side") or "").upper()
    day_seq = int(row.get("day_seq") or 0)

    pump_pct = _fmt_pct(row.get("pump_pct"))
    win_min = row.get("window_minutes")

    funding_pct = _fmt_pct(row.get("funding_pct"))
    funding_left = _fmt_time_left_seconds(row.get("funding_time_left_sec"))

    ts = row.get("signal_ts")
    ts_local = _fmt_local_ts(ts, tz_name) if isinstance(ts, datetime) else str(ts or "—")

    lines = [
        f"{_dir_mark(side)} 🪙 {symbol} | {side} | TF {timeframe} | 🧾 #{day_seq}",
        _fmt_entry_sl_tp(row),
        f"ΔPrice {pump_pct}% за {win_min}m",
        f"Funding {funding_pct}% | next in {funding_left}",
        f"⏱️ {ts_local} | {screener_name}",
    ]
    return "\n".join(lines).strip()


def _send_telegram_text_if_enabled(
    *,
    screener_name: str,
    timeframe: str,
    params: dict,
    packed_rows: List[dict],
) -> None:
    if not bool(params.get("telegram_enabled", False)):
        return
    if not bool(params.get("telegram_send_text", True)):
        return
    if not packed_rows:
        return

    telegram_mode = str(params.get("telegram_mode", "batch")).strip().lower()
    if telegram_mode not in ("batch", "single"):
        telegram_mode = "batch"

    telegram_max_signals = int(params.get("telegram_max_signals", 50))
    tz_name = str(params.get("telegram_timezone", "")).strip() or None

    try:
        targets = _resolve_telegram_targets(params)
        if not targets:
            log.warning("Telegram enabled, but no targets resolved (check env keys)")
            return

        rows = packed_rows[: max(1, telegram_max_signals)]
        dead_targets: Set[str] = set()

        def _send_parts_to_targets(parts: List[str]) -> Tuple[int, int]:
            ok = 0
            total = 0
            for part in parts:
                for t in targets:
                    if getattr(t, "name", "") in dead_targets:
                        continue
                    total += 1
                    sent = bool(tg.send_telegram_message(part, target=t))
                    if sent:
                        ok += 1
                    else:
                        dead_targets.add(getattr(t, "name", "unknown"))
                        log.warning(
                            "Telegram FAILED target=%s chat_id=%s -> disabled for this run",
                            getattr(t, "name", "unknown"),
                            getattr(t, "chat_id", None),
                        )
            return ok, total

        ok_total = 0
        total_total = 0
        parts_total = 0

        if telegram_mode == "single":
            for r in rows:
                msg = _build_pump_batch_message(
                    screener_name=screener_name,
                    timeframe=timeframe,
                    rows=[r],
                    tz_name=tz_name,
                )
                parts = tg.split_long_message(msg)
                parts_total += len(parts)
                ok, total = _send_parts_to_targets(parts)
                ok_total += ok
                total_total += total
        else:
            msg = _build_pump_batch_message(
                screener_name=screener_name,
                timeframe=timeframe,
                rows=rows,
                tz_name=tz_name,
            )
            parts = tg.split_long_message(msg)
            parts_total = len(parts)
            ok_total, total_total = _send_parts_to_targets(parts)

        log.info(
            "Telegram text sent ok=%d/%d mode=%s signals=%d parts=%d targets=%d (alive=%d)",
            ok_total,
            total_total,
            telegram_mode,
            len(rows),
            parts_total,
            len(targets),
            max(0, len(targets) - len(dead_targets)),
        )

    except Exception:
        log.exception("Telegram notify failed")


def _send_telegram_plots_if_enabled(
    *,
    screener_name: str,
    timeframe: str,
    params: dict,
    plot_items: List[Tuple[str, Dict[str, Any]]],
) -> None:
    if not bool(params.get("telegram_enabled", False)):
        return
    if not bool(params.get("telegram_send_plots", False)):
        return
    if not plot_items:
        return

    telegram_plot_mode = str(params.get("telegram_plot_mode", "photo")).strip().lower()
    if telegram_plot_mode not in ("photo", "document"):
        telegram_plot_mode = "photo"

    telegram_max_plot_signals = int(params.get("telegram_max_plot_signals", 20))
    tz_name = str(params.get("telegram_timezone", "")).strip() or None

    try:
        targets = _resolve_telegram_targets(params)
        if not targets:
            log.warning("Telegram plots enabled, but no targets resolved (check env keys)")
            return

        items = plot_items[: max(1, telegram_max_plot_signals)]
        dead_targets: Set[str] = set()
        ok_total = 0
        total_total = 0

        for png_path, row in items:
            caption = _build_pump_plot_caption(
                screener_name=screener_name,
                timeframe=timeframe,
                row=row,
                tz_name=tz_name,
            )

            for t in targets:
                if getattr(t, "name", "") in dead_targets:
                    continue

                total_total += 1
                if telegram_plot_mode == "document":
                    sent = bool(tg.send_telegram_document(png_path, caption=caption, target=t))
                else:
                    sent = bool(tg.send_telegram_photo(png_path, caption=caption, target=t))

                if sent:
                    ok_total += 1
                else:
                    dead_targets.add(getattr(t, "name", "unknown"))
                    log.warning(
                        "Telegram plot FAILED target=%s chat_id=%s -> disabled for this run",
                        getattr(t, "name", "unknown"),
                        getattr(t, "chat_id", None),
                    )

        log.info(
            "Telegram plots sent ok=%d/%d mode=%s plots=%d targets=%d (alive=%d)",
            ok_total,
            total_total,
            telegram_plot_mode,
            len(items),
            len(targets),
            max(0, len(targets) - len(dead_targets)),
        )

    except Exception:
        log.exception("Telegram plots send failed")


def _resolve_intervals(raw_params: dict) -> List[str]:
    cfg = PumpConfig.from_cfg(raw_params)
    intervals = [str(x).strip() for x in (cfg.intervals or []) if str(x).strip()]
    if not intervals:
        intervals = [str(raw_params.get("interval") or "5m").strip()]
    return intervals


def _apply_interval_overrides(raw_params: dict, interval: str) -> dict:
    cfg = PumpConfig.from_cfg(raw_params).for_interval(interval)
    return cfg.as_flat_params()


def _filter_fresh_signals(
    signals: list,
    *,
    now_ts: datetime,
    interval: str,
    params: dict,
) -> list:
    if not signals:
        return []

    td = interval_to_timedelta(interval)
    tf_min = max(1, int(round(td.total_seconds() / 60.0)))

    max_age_min = None
    age_min_raw = params.get("max_signal_age_minutes")
    if age_min_raw not in (None, "", "null"):
        try:
            max_age_min = int(age_min_raw)
        except Exception:
            max_age_min = None

    if max_age_min is None:
        age_candles_raw = params.get("max_signal_age_candles")
        if age_candles_raw not in (None, "", "null"):
            try:
                max_age_min = int(age_candles_raw) * tf_min
            except Exception:
                max_age_min = None

    if max_age_min is None:
        max_age_min = 2 * tf_min

    cutoff = now_ts - timedelta(minutes=max(1, int(max_age_min)))

    out = []
    dropped = 0
    for s in signals:
        ts = getattr(s, "signal_ts", None)
        if isinstance(ts, datetime) and ts >= cutoff:
            out.append(s)
        else:
            dropped += 1

    if dropped > 0:
        log.info("Drop stale signals interval=%s: %d (cutoff=%s)", interval, dropped, cutoff.isoformat())

    return out


def _ping_db(store: PostgreSQLStorage) -> tuple[datetime, int]:
    row_now = store.query_one("SELECT now()")
    now = row_now[0] if row_now else _utc_now()

    row_cnt = store.query_one("SELECT COUNT(*) FROM screeners")
    screeners_total = int(row_cnt[0] if row_cnt else 0)

    return now, screeners_total


def main() -> None:
    _setup_logging()

    environment = get_environment()

    try:
        loaded = int(tg.load_dotenv_file(".env", override=False) or 0)
        if loaded:
            log.info("Loaded .env: %s", str(Path(".env").resolve()))
    except OSError:
        pass
    except Exception:
        log.debug("Failed to load .env", exc_info=True)

    cfg_path = _resolve_cfg_path()

    log.info("=== RUN PUMP SCREENER START ===")
    log.info("Environment: %s", environment)
    log.info("PUMP_SCREENERS_CONFIG=%s", str(cfg_path))

    cfg = _load_cfg(cfg_path)
    restart_interval_minutes = int(cfg.get("restart_interval_minutes", 2))
    items = _get_screeners(cfg)

    log.info("Screeners in config: %d", len(items))
    if not items:
        log.warning("No screeners found in config")
        return

    dsn = get_pg_dsn()
    pool = create_pool(dsn)
    store = PostgreSQLStorage(pool)

    try:
        db_now, screeners_total = _ping_db(store)
        log.info("DB ping OK: now=%s | screeners_total=%d", str(db_now), int(screeners_total))
    except Exception:
        log.exception("DB ping failed")

    screener_registry = {"scr_pump_binance": ScrPumpBinance}

    while True:
        total_inserted = 0
        total_skipped_dup = 0
        total_plots = 0

        try:
            for item in items:
                enabled = bool(item.get("enabled", False))
                name = str(item.get("name", "")).strip()
                version = str(item.get("version", "0.1"))
                params: Dict[str, Any] = dict(item)
                params.pop("enabled", None)
                params.pop("version", None)
                if isinstance(item.get("params"), dict):
                    base_params = dict(item.get("params") or {})
                    merged = dict(params)
                    merged.update(base_params)
                    params = merged

                if not enabled:
                    log.info("Skip screener=%s (disabled)", name)
                    continue
                if not name:
                    log.warning("Skip screener (missing name)")
                    continue
                if name not in screener_registry:
                    log.warning("Skip screener=%s (not implemented yet)", name)
                    continue

                cfg_obj = PumpConfig.from_cfg(params)
                exchange_id = int(getattr(cfg_obj, "exchange_id", params.get("exchange_id", 1)) or 1)
                intervals = _resolve_intervals(params)

                log.info("------------------------------------------------------------")
                log.info("Run screener=%s v=%s intervals=%s", name, version, ",".join(intervals))

                screener_id = store.ensure_screener(name=name, version=version)
                scr = screener_registry[name]()

                for interval in intervals:
                    params_i: Dict[str, Any] = _apply_interval_overrides(params, interval)
                    mode = str(params_i.get("signal_mode") or "both").lower()
                    log.info("  -> interval=%s mode=%s", interval, mode.upper())

                    now_ts = _utc_now()

                    signals = scr.run(storage=store, exchange_id=exchange_id, interval=interval, params=params_i)
                    if not signals:
                        log.info("No signals mode=%s interval=%s", mode, interval)
                        continue

                    signals = _filter_fresh_signals(signals, now_ts=now_ts, interval=interval, params=params_i)
                    if not signals:
                        log.info("No fresh signals mode=%s interval=%s", mode, interval)
                        continue

                    pairs_key = [(int(s.symbol_id), s.signal_ts) for s in signals]
                    existing = _fetch_existing_signal_pairs(
                        store,
                        exchange_id=exchange_id,
                        screener_id=screener_id,
                        timeframe=interval,
                        pairs=pairs_key,
                    )

                    new_signals = [s for s in signals if (int(s.symbol_id), s.signal_ts) not in existing]
                    skipped = len(signals) - len(new_signals)
                    if skipped > 0:
                        total_skipped_dup += skipped
                        log.info("Skip duplicates mode=%s interval=%s: %d", mode, interval, skipped)

                    if not new_signals:
                        log.info("All signals already exist (duplicates) mode=%s interval=%s", mode, interval)
                        continue

                    enable_funding = bool(params_i.get("enable_funding", True))
                    funding_map: Dict[int, Dict[str, Any]] = {}
                    if enable_funding:
                        try:
                            symbol_ids = sorted({int(s.symbol_id) for s in new_signals})
                            funding_map = store.fetch_next_funding_bulk(
                                exchange_id=int(exchange_id),
                                symbol_ids=symbol_ids,
                                as_of=now_ts,
                            )
                        except Exception:
                            funding_map = {}
                            log.warning("Funding snapshot load failed", exc_info=True)

                    rows: List[Dict[str, Any]] = []
                    packed_for_telegram: List[Dict[str, Any]] = []
                    packed_by_key: Dict[Tuple[int, datetime], Dict[str, Any]] = {}

                    sl_pct_cfg = params_i.get("stop_loss_pct")
                    tp_pct_cfg = params_i.get("take_profit_pct")

                    day_seq_ranges = _allocate_day_seqs_batch(
                        store,
                        exchange_id=int(exchange_id),
                        screener_id=int(screener_id),
                        items=[(int(s.symbol_id), s.signal_ts.date()) for s in new_signals],
                    )

                    for s in new_signals:
                        signal_day = s.signal_ts.date()
                        ctx = dict(s.context or {})

                        f = funding_map.get(int(s.symbol_id), {}) or {}
                        f_time = f.get("funding_time")
                        f_rate = f.get("funding_rate")
                        f_pct = (float(f_rate) * 100.0) if f_rate is not None else None

                        is_next = bool(f.get("is_next", False))
                        f_left_sec = None
                        if is_next and isinstance(f_time, datetime):
                            f_left_sec = max(0.0, (f_time - now_ts).total_seconds())

                        ctx["funding_time"] = f_time
                        ctx["funding_rate"] = f_rate
                        ctx["funding_pct"] = f_pct
                        ctx["funding_is_next"] = is_next
                        ctx["funding_time_left_sec"] = f_left_sec
                        ctx["funding_is_estimated"] = bool(f.get("is_estimated", False))

                        seq_key = (int(s.symbol_id), signal_day)
                        seq_list = day_seq_ranges.get(seq_key) or [1]
                        day_seq = int(seq_list.pop(0))
                        ctx["day_seq"] = int(day_seq)

                        rows.append(
                            {
                                "exchange_id": int(exchange_id),
                                "symbol_id": int(s.symbol_id),
                                "symbol": str(s.symbol or ""),
                                "screener_id": int(screener_id),
                                "timeframe": str(interval),
                                "signal_ts": s.signal_ts,
                                "signal_day": signal_day,
                                "day_seq": int(day_seq),
                                "side": str(s.side).upper(),
                                "status": "NEW",
                                "entry_price": s.entry_price,
                                "exit_price": s.exit_price,
                                "stop_loss": s.stop_loss,
                                "take_profit": s.take_profit,
                                "confidence": s.confidence,
                                "score": s.score,
                                "reason": s.reason,
                                "context": _jsonable(ctx),
                                "source": "screener",
                            }
                        )

                        packed = {
                            "symbol": str(s.symbol or ""),
                            "side": str(s.side).upper(),
                            "entry_price": s.entry_price,
                            "stop_loss": s.stop_loss,
                            "take_profit": s.take_profit,
                            "stop_loss_pct": sl_pct_cfg,
                            "take_profit_pct": tp_pct_cfg,
                            "signal_ts": s.signal_ts,
                            "day_seq": int(day_seq),
                            "pump_pct": (ctx.get("pump_pct") if ctx.get("pump_pct") is not None else ctx.get("price_change_pct")),
                            "window_minutes": (ctx.get("window_minutes") or ctx.get("buy_window_minutes") or ctx.get("sell_window_minutes")),
                            "funding_pct": f_pct,
                            "funding_time_left_sec": f_left_sec,
                        }

                        packed_for_telegram.append(packed)
                        packed_by_key[(int(s.symbol_id), s.signal_ts)] = packed

                        try:
                            s.context = ctx
                        except Exception:
                            pass

                    inserted = _insert_signals_safe(store, rows)
                    total_inserted += inserted
                    log.info("Signals inserted mode=%s interval=%s: %d", mode, interval, inserted)

                    def _pump_abs(r: dict) -> float:
                        try:
                            return abs(float(r.get("pump_pct") or 0.0))
                        except Exception:
                            return 0.0

                    packed_for_telegram.sort(key=_pump_abs, reverse=True)

                    if inserted > 0:
                        _send_telegram_text_if_enabled(
                            screener_name=name,
                            timeframe=interval,
                            params=params_i,
                            packed_rows=packed_for_telegram,
                        )

                    enable_plots = bool(params_i.get("enable_plots", True))
                    telegram_send_plots = bool(params_i.get("telegram_send_plots", True))
                    telegram_max_plot_signals = int(params_i.get("telegram_max_plot_signals", 30))

                    if enable_plots and new_signals:
                        from src.platform.screeners.plotting_pump import save_pump_signal_plot

                        plots_dir = Path(str(params_i.get("plots_dir", "artifacts/screener_plots")))
                        plots_dir.mkdir(parents=True, exist_ok=True)

                        lookback = int(params_i.get("plot_lookback", 180))
                        forward_bars = int(params_i.get("plot_lookforward", 30))

                        sigs_for_plots = sorted(
                            list(new_signals),
                            key=lambda s: abs(float(((s.context or {}).get("pump_pct") or (s.context or {}).get("price_change_pct") or 0.0))),
                            reverse=True,
                        )
                        if telegram_send_plots and telegram_max_plot_signals > 0:
                            sigs_for_plots = sigs_for_plots[: max(1, telegram_max_plot_signals)]

                        if params_i.get("oi_interval") or params_i.get("cvd_interval"):
                            log.info(
                                "Pump plot intervals forced to candle interval=%s (oi_interval/cvd_interval config is ignored for consistency)",
                                interval,
                            )

                        saved = 0
                        plot_items_for_tg: List[Tuple[str, Dict[str, Any]]] = []

                        # For pump plots OI/CVD must be taken from DB in the same interval
                        # as the signal candles. Using separate oi_interval/cvd_interval here
                        # makes the chart inconsistent with the screener calculations.
                        oi_itv = str(interval)
                        cvd_itv = str(interval)
                        candles_cache: Dict[Tuple[int, str], List[Dict[str, Any]]] = {}
                        oi_cache: Dict[Tuple[int, str], List[Dict[str, Any]]] = {}
                        cvd_cache: Dict[Tuple[int, str], List[Dict[str, Any]]] = {}

                        for s in sigs_for_plots:
                            try:
                                center_ts = s.signal_ts

                                candles_key = (int(s.symbol_id), int(center_ts.timestamp()))
                                candles = candles_cache.get(candles_key)
                                if candles is None:
                                    candles = store.fetch_candles_window(
                                        exchange_id=exchange_id,
                                        symbol_id=s.symbol_id,
                                        interval=interval,
                                        center_ts=center_ts,
                                        lookback=lookback,
                                        lookforward=forward_bars,
                                    )
                                    candles_cache[candles_key] = candles or []
                                if not candles:
                                    log.warning("No candles for plot: %s %s %s", s.symbol, interval, s.signal_ts)
                                    continue

                                enable_oi = bool(params_i.get("enable_oi", True))
                                enable_cvd = bool(params_i.get("enable_cvd", True))

                                # Same-interval plots: OI/CVD use the same timeframe as candles,
                                # so window sizes are the same in bars and in visible time span.
                                oi_lookback = int(lookback)
                                oi_lookforward = int(forward_bars)
                                cvd_lookback = int(lookback)
                                cvd_lookforward = int(forward_bars)

                                oi_series = None
                                if enable_oi:
                                    try:
                                        oi_key = (int(s.symbol_id), int(center_ts.timestamp()), str(oi_itv), int(oi_lookback), int(oi_lookforward))
                                        oi_series = oi_cache.get(oi_key)
                                        if oi_series is None:
                                            oi_series = store.fetch_open_interest_window(
                                                exchange_id=exchange_id,
                                                symbol_id=s.symbol_id,
                                                interval=oi_itv,
                                                center_ts=center_ts,
                                                lookback=oi_lookback,
                                                lookforward=oi_lookforward,
                                            )
                                            oi_cache[oi_key] = oi_series or []
                                    except Exception:
                                        oi_series = None

                                cvd_series = None
                                if enable_cvd:
                                    try:
                                        cvd_key = (int(s.symbol_id), int(center_ts.timestamp()), str(cvd_itv), int(cvd_lookback), int(cvd_lookforward))
                                        cvd_series = cvd_cache.get(cvd_key)
                                        if cvd_series is None:
                                            cvd_series = store.fetch_cvd_window(
                                                exchange_id=exchange_id,
                                                symbol_id=s.symbol_id,
                                                interval=cvd_itv,
                                                center_ts=center_ts,
                                                lookback=cvd_lookback,
                                                lookforward=cvd_lookforward,
                                            )
                                            cvd_cache[cvd_key] = cvd_series or []
                                    except Exception:
                                        cvd_series = None

                                if enable_oi and oi_series is None:
                                    oi_series = []
                                if enable_cvd and cvd_series is None:
                                    cvd_series = []

                                ctx = dict(s.context or {})

                                day_folder = plots_dir / name / str(s.symbol) / str(s.signal_ts.date())
                                out_png = day_folder / f"{s.symbol}_{interval}_{s.side}_{s.signal_ts.strftime('%Y%m%d_%H%M%S')}.png"
                                out_png.parent.mkdir(parents=True, exist_ok=True)

                                base_avg_price = ctx.get("base_avg_price")
                                base_lookback_bars = (
                                    ctx.get("base_lookback_bars")
                                    or ctx.get("buy_base_lookback_bars")
                                    or ctx.get("sell_base_lookback_bars")
                                )

                                save_pump_signal_plot(
                                    out_path=out_png,
                                    symbol=str(s.symbol),
                                    timeframe=interval,
                                    candles=candles,
                                    signal_ts=s.signal_ts,
                                    side=str(s.side),
                                    entry_price=float(s.entry_price or 0.0),
                                    stop_loss=s.stop_loss,
                                    take_profit=s.take_profit,
                                    pump_start_ts=ctx.get("pump_start_ts"),
                                    pump_end_ts=ctx.get("pump_end_ts"),
                                    confirm_ts=ctx.get("confirm_ts"),
                                    pump_pct=(ctx.get("pump_pct") if ctx.get("pump_pct") is not None else ctx.get("price_change_pct")),
                                    window_minutes=(ctx.get("window_minutes") or ctx.get("buy_window_minutes") or ctx.get("sell_window_minutes")),
                                    oi_delta=ctx.get("oi_delta"),
                                    cvd_delta=ctx.get("cvd_delta"),
                                    oi_series=oi_series,
                                    cvd_series=cvd_series,
                                    base_avg_price=base_avg_price,
                                    base_lookback_bars=base_lookback_bars,
                                    keep_plots_days=params_i.get("keep_plots_days"),
                                    screener_name=name,
                                )

                                saved += 1
                                log.info("Plot saved -> %s", out_png)

                                if telegram_send_plots and inserted > 0:
                                    key = (int(s.symbol_id), s.signal_ts)
                                    row = packed_by_key.get(key)
                                    if row:
                                        plot_items_for_tg.append((str(out_png), row))

                            except Exception:
                                log.exception("Plot failed for %s", s.symbol)

                        total_plots += saved
                        log.info("Plots saved mode=%s interval=%s: %d into %s", mode, interval, saved, plots_dir)

                        if inserted > 0 and telegram_send_plots and plot_items_for_tg:
                            _send_telegram_plots_if_enabled(
                                screener_name=name,
                                timeframe=interval,
                                params=params_i,
                                plot_items=plot_items_for_tg,
                            )

            log.info("------------------------------------------------------------")
            log.info(
                "=== RUN PUMP SCREENER DONE === inserted=%d skipped_dup=%d plots=%d",
                total_inserted,
                total_skipped_dup,
                total_plots,
            )

        except Exception:
            log.exception("RUN PUMP SCREENER FAILED")

        sleep_s = max(1, restart_interval_minutes * 60)
        log.info("Sleeping for %d minutes before next run...", restart_interval_minutes)
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()