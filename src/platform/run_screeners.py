# src/platform/run_screeners.py
from __future__ import annotations

import os
import time
import logging
from pathlib import Path
from datetime import datetime, timezone, date
from typing import Any, Dict, List, Tuple, Set, Optional

import yaml
from psycopg.errors import UniqueViolation

from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage
from src.platform.screeners.scr_liquidation_binance import ScrLiquidationBinance

log = logging.getLogger("platform.run_screeners")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# -----------------------------
# JSON SAFE (fix datetime in context)
# -----------------------------
def _jsonable(v: Any) -> Any:
    """
    Делает объект безопасным для json.dumps():
    - datetime/date -> isoformat()
    - dict/list -> рекурсивно
    - всё остальное -> str()
    """
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, (datetime, date)):
        # ISO формат читаемый и стабильно сериализуется
        return v.isoformat()
    if isinstance(v, dict):
        return {str(k): _jsonable(val) for k, val in v.items()}
    if isinstance(v, (list, tuple, set)):
        return [_jsonable(x) for x in v]
    return str(v)


# -----------------------------
# Telegram formatting helpers
# -----------------------------
def _fmt_price(x: Any) -> str:
    try:
        if x is None:
            return "—"
        v = float(x)
        s = f"{v:.8f}".rstrip("0").rstrip(".")
        return s if s else str(v)
    except Exception:
        return str(x)


def _fmt_usdt(x: Any) -> str:
    try:
        v = float(x or 0)
        v = abs(v)
        # округлим до целых и красиво с пробелами
        i = int(round(v))
        return f"{i:,}".replace(",", " ")
    except Exception:
        return "0"


def _to_local(dt_utc: datetime, tz_name: Optional[str]) -> datetime:
    """
    Переводит UTC время в локальное.
    Если tz_name задан (например 'Europe/Moscow'), пробуем его.
    Иначе используем локальную таймзону ОС.
    """
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)

    if tz_name:
        try:
            from zoneinfo import ZoneInfo

            return dt_utc.astimezone(ZoneInfo(tz_name))
        except Exception:
            # fallback на локальное
            return dt_utc.astimezone()

    return dt_utc.astimezone()


def _fmt_local_ts(dt_utc: datetime, tz_name: Optional[str]) -> str:
    d = _to_local(dt_utc, tz_name)
    # читаемо + с таймзоной
    return d.strftime("%d.%m.%Y %H:%M %Z")


def _build_telegram_batch_message(
    *,
    screener_name: str,
    timeframe: str,
    rows: List[Dict[str, Any]],
    tz_name: Optional[str],
) -> str:
    header_ts = _fmt_local_ts(_utc_now(), tz_name)
    out: List[str] = [f"📡 {screener_name} | TF {timeframe} | {header_ts}", ""]

    for i, r in enumerate(rows, start=1):
        symbol = str(r.get("symbol") or "")
        side = str(r.get("side") or "").upper()
        entry_price = _fmt_price(r.get("entry_price"))
        day_seq = int(r.get("day_seq") or 0)

        liq_s = _fmt_usdt(r.get("liq_short_usdt"))
        liq_l = _fmt_usdt(r.get("liq_long_usdt"))

        ts = r.get("signal_ts")
        if isinstance(ts, str):
            # если вдруг пришло строкой — оставим как есть
            ts_local = ts
        elif isinstance(ts, datetime):
            ts_local = _fmt_local_ts(ts, tz_name)
        else:
            ts_local = "—"

        out.append(f"{i}) 🪙 {symbol} | {side} | Entry {entry_price}")
        out.append(f"   💥 SHORT {liq_s} USDT | LONG {liq_l} USDT")
        out.append(f"   ⏱️ {ts_local} | TF {timeframe} | 🧾 #{day_seq}")
        out.append("")

    return "\n".join(out).strip()


def _resolve_telegram_targets(params: dict) -> list:
    """
    Собираем список получателей:
      - telegram_primary (всегда)
      - telegram_extras (только если telegram_extras_enabled=true)
    """
    targets = []

    primary = params.get("telegram_primary") or {}
    token_env = str(primary.get("token_env", "TELEGRAM_BOT_TOKEN")).strip() or "TELEGRAM_BOT_TOKEN"
    chat_env = str(primary.get("chat_id_env", "TELEGRAM_CHAT_ID")).strip() or "TELEGRAM_CHAT_ID"

    primary_token = (os.getenv(token_env) or "").strip()
    primary_chat = (os.getenv(chat_env) or "").strip()

    if primary_token and primary_chat:
        from src.platform.notifications.telegram import TelegramTarget
        targets.append(TelegramTarget(name="primary", bot_token=primary_token, chat_id=primary_chat))

    extras_enabled = bool(params.get("telegram_extras_enabled", False))
    extras = params.get("telegram_extras") or []

    if extras_enabled and isinstance(extras, list) and primary_token:
        from src.platform.notifications.telegram import TelegramTarget

        for e in extras:
            if not isinstance(e, dict):
                continue
            if not bool(e.get("enabled", True)):
                continue

            name = str(e.get("name", "extra")).strip() or "extra"
            chat_id_env = str(e.get("chat_id_env", "")).strip()
            token_env2 = str(e.get("token_env", "")).strip()

            token = (os.getenv(token_env2).strip() if token_env2 else primary_token) if primary_token else ""
            chat_id = (os.getenv(chat_id_env) or "").strip() if chat_id_env else ""

            if not token or not chat_id:
                continue

            targets.append(TelegramTarget(name=name, bot_token=token, chat_id=chat_id))

    return targets


def _send_telegram_if_enabled(*, screener_name: str, timeframe: str, params: dict, packed_rows: list[dict]) -> None:
    telegram_enabled = bool(params.get("telegram_enabled", False))
    if not telegram_enabled or not packed_rows:
        return

    telegram_mode = str(params.get("telegram_mode", "batch")).strip().lower()
    telegram_max_signals = int(params.get("telegram_max_signals", 20))
    tz_name = str(params.get("telegram_timezone", "")).strip() or None

    try:
        from src.platform.notifications.telegram import (
            send_telegram_message,
            split_long_message,
        )

        targets = _resolve_telegram_targets(params)
        if not targets:
            log.warning("Telegram enabled, but no targets resolved (check env keys)")
            return

        rows = packed_rows[: max(1, telegram_max_signals)]

        # сообщение строишь своей функцией:
        if telegram_mode == "single":
            for r in rows:
                msg = _build_telegram_batch_message(
                    screener_name=screener_name,
                    timeframe=timeframe,
                    rows=[r],
                    tz_name=tz_name,
                )
                for part in split_long_message(msg):
                    for t in targets:
                        send_telegram_message(part, target=t)
        else:
            msg = _build_telegram_batch_message(
                screener_name=screener_name,
                timeframe=timeframe,
                rows=rows,
                tz_name=tz_name,
            )
            for part in split_long_message(msg):
                for t in targets:
                    send_telegram_message(part, target=t)

        log.info("Telegram sent: mode=%s count=%d targets=%d", telegram_mode, len(rows), len(targets))
    except Exception:
        log.exception("Telegram notify failed")

# -----------------------------
# config
# -----------------------------
def _load_cfg() -> dict:
    cfg_path = os.getenv("SCREENERS_CONFIG", "config/screeners.yaml")
    p = Path(cfg_path)
    if not p.exists():
        raise FileNotFoundError(f"SCREENERS_CONFIG not found: {p}")
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}


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


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        force=True,
    )

    cfg_path = os.getenv("SCREENERS_CONFIG", "config/screeners.yaml")
    log.info("=== RUN SCREENERS START ===")
    log.info("SCREENERS_CONFIG=%s", cfg_path)

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

        try:
            for item in items:
                enabled = bool(item.get("enabled", False))
                name = str(item.get("name", "")).strip()
                version = str(item.get("version", "0.1"))
                params: Dict[str, Any] = dict(item.get("params") or {})

                if not enabled:
                    log.info("Skip screener=%s (disabled)", name)
                    continue
                if not name:
                    log.warning("Skip screener (missing name)")
                    continue
                if name not in screener_registry:
                    log.warning("Skip screener=%s (not implemented yet)", name)
                    continue

                exchange_id = 1
                interval = str(params.get("interval", "1h")).strip()

                log.info("------------------------------------------------------------")
                log.info("Run screener=%s v=%s interval=%s", name, version, interval)

                screener_id = store.ensure_screener(name=name, version=version)

                scr = screener_registry[name]()
                signals = scr.run(
                    storage=store,
                    exchange_id=exchange_id,
                    interval=interval,
                    params=params,
                )

                if not signals:
                    log.info("No signals")
                    continue

                pairs = [(int(s.symbol_id), s.signal_ts) for s in signals]
                existing = _fetch_existing_signal_pairs(
                    store,
                    exchange_id=exchange_id,
                    screener_id=screener_id,
                    timeframe=interval,
                    pairs=pairs,
                )

                new_signals = [s for s in signals if (int(s.symbol_id), s.signal_ts) not in existing]
                skipped = len(signals) - len(new_signals)
                if skipped > 0:
                    total_skipped_dup += skipped
                    log.info("Skip duplicates: %d", skipped)

                if not new_signals:
                    log.info("All signals already exist (duplicates)")
                    continue

                rows: List[Dict[str, Any]] = []
                packed_for_telegram: List[Dict[str, Any]] = []

                for s in new_signals:
                    signal_day = s.signal_ts.date()

                    # ✅ копия dict + JSON SAFE (datetime -> string)
                    ctx = _jsonable(dict(s.context or {}))

                    # ✅ номер сигнала за сутки
                    day_seq = store.next_signal_seq(
                        exchange_id=exchange_id,
                        symbol_id=int(s.symbol_id),
                        screener_id=int(screener_id),
                        signal_day=signal_day,
                    )
                    ctx["day_seq"] = int(day_seq)

                    rows.append(
                        {
                            "exchange_id": int(exchange_id),
                            "symbol_id": int(s.symbol_id),
                            "symbol": str(s.symbol or ""),
                            "screener_id": int(screener_id),
                            "timeframe": str(interval),
                            "signal_ts": s.signal_ts,     # datetime ok
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
                            "context": ctx,               # ✅ dict json-safe
                            "source": "screener",
                        }
                    )

                    packed_for_telegram.append(
                        {
                            "symbol": str(s.symbol or ""),
                            "side": str(s.side).upper(),
                            "entry_price": s.entry_price,
                            "signal_ts": s.signal_ts,  # ✅ datetime, для локального времени
                            "day_seq": int(day_seq),
                            "liq_long_usdt": ctx.get("liq_long_usdt"),
                            "liq_short_usdt": ctx.get("liq_short_usdt"),
                        }
                    )

                inserted = _insert_signals_safe(store, rows)
                total_inserted += inserted
                log.info("Signals inserted: %d", inserted)

                # ✅ TELEGRAM отправляем только если реально вставили
                if inserted > 0:
                    def _liq_sum(r: dict) -> float:
                        try:
                            return abs(float(r.get("liq_long_usdt") or 0)) + abs(float(r.get("liq_short_usdt") or 0))
                        except Exception:
                            return 0.0

                    packed_for_telegram.sort(key=_liq_sum, reverse=True)

                    _send_telegram_if_enabled(
                        screener_name=name,
                        timeframe=interval,
                        params=params,
                        packed_rows=packed_for_telegram,
                    )

                # ---- plots ----
                enable_plots = bool(params.get("enable_plots", False))
                if enable_plots and new_signals:
                    from src.platform.screeners.plotting import save_signal_plot

                    plots_dir = Path(str(params.get("plots_dir", "artifacts/screener_plots")))
                    plots_dir.mkdir(parents=True, exist_ok=True)  # ✅ гарантируем базовую папку

                    lookback = int(params.get("plot_lookback", 120))
                    forward_bars = int(params.get("plot_lookforward", 20))

                    saved = 0
                    for s in new_signals:
                        try:
                            candles = store.fetch_candles_window(
                                exchange_id=exchange_id,
                                symbol_id=s.symbol_id,
                                interval=interval,
                                center_ts=s.signal_ts,
                                lookback=lookback,
                                lookforward=forward_bars,
                            )
                            if not candles:
                                log.warning("No candles for plot: %s %s %s", s.symbol, interval, s.signal_ts)
                                continue

                            ctx_plot = dict(s.context or {})
                            touch_ts = ctx_plot.get("touch_ts")

                            liq_series = None
                            try:
                                start_ts = candles[0]["ts"]
                                end_ts = candles[-1]["ts"]
                                liq_series = store.fetch_liquidations_window(
                                    exchange_id=exchange_id,
                                    symbol_id=s.symbol_id,
                                    interval=interval,
                                    start_ts=start_ts,
                                    end_ts=end_ts,
                                )
                            except Exception:
                                liq_series = None

                            day_folder = plots_dir / name / str(s.symbol) / str(s.signal_ts.date())
                            out_png = day_folder / f"{s.symbol}_{interval}_{s.side}_{s.signal_ts.strftime('%Y%m%d_%H%M%S')}.png"

                            # ✅ ВАЖНО: создаём папки перед сохранением файла
                            out_png.parent.mkdir(parents=True, exist_ok=True)

                            save_signal_plot(
                                out_path=out_png,
                                symbol=str(s.symbol),
                                timeframe=interval,
                                candles=candles,
                                entry_ts=s.signal_ts,
                                side=str(s.side),
                                entry_price=float(s.entry_price),
                                touch_ts=touch_ts,
                                up_level=ctx_plot.get("up_level"),
                                down_level=ctx_plot.get("down_level"),
                                liquidation_series=liq_series,
                                # ✅ ВОТ ЭТО ТЫ УБРАЛ — из-за этого графики и не сохранялись
                                liq_short_usdt=ctx_plot.get("liq_short_usdt"),
                                liq_long_usdt=ctx_plot.get("liq_long_usdt"),
                            )

                            saved += 1
                            log.info("Plot saved -> %s", out_png)

                        except Exception:
                            log.exception("Plot failed for %s", s.symbol)

                    total_plots += saved
                    log.info("Plots saved: %d into %s", saved, plots_dir)


            log.info("------------------------------------------------------------")
            log.info(
                "=== RUN SCREENERS DONE === inserted=%d skipped_dup=%d plots=%d",
                total_inserted,
                total_skipped_dup,
                total_plots,
            )

        except Exception:
            log.exception("RUN SCREENERS FAILED")

        log.info("Sleeping for %d minutes before next run...", restart_interval_minutes)
        time.sleep(restart_interval_minutes * 60)


if __name__ == "__main__":
    main()
