# src/platform/exchanges/binance/collector_funding.py
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from src.platform.exchanges.binance.ws import BinanceWS, WS_STREAM_BASE

logger = logging.getLogger("src.platform.exchanges.binance.collector_funding")


# -------------------------
# helpers
# -------------------------

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _from_ms(ms: Any) -> Optional[datetime]:
    try:
        if ms is None:
            return None
        return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)
    except Exception:
        return None


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _classify_http_error(exc: Exception) -> Optional[int]:
    """
    Достаём HTTP code из текста исключения.
    Плюс учитываем бинанс -1003 Too many requests как 429.
    """
    s = str(exc)
    for code in (451, 418, 429, 403, 400):
        if f"HTTP {code}" in s:
            return code
    if "-1003" in s or "Too many requests" in s:
        return 429
    return None


def _normalize_funding_time(dt: Optional[datetime]) -> Optional[datetime]:
    """
    ✅ ВАЖНО: Binance funding time дискретен (обычно 00/08/16 UTC),
    а WS иногда приходит с микро/миллисекундами.
    Нормализуем до начала часа, чтобы REST и WS попадали в один и тот же PK.
    """
    if dt is None:
        return None
    return dt.replace(minute=0, second=0, microsecond=0)


# -------------------------
# DB upsert (hard-safe)
# -------------------------

def _upsert_funding_rows(*, storage, rows: List[dict]) -> int:
    """
    Жёстко гарантируем UPSERT, чтобы:
      - не плодились дубли
      - при уникальном индексе не было падений
    Сначала пробуем storage.upsert_funding(rows). Если он отсутствует/падает,
    делаем прямой SQL UPSERT через storage.pool.

    Поддерживаем 2 схемы:
      A) (exchange_id, symbol_id, funding_time, funding_rate, mark_price, source)
      B) минимальная (exchange_id, symbol_id, funding_time, funding_rate)
    """
    if not rows:
        return 0

    fn = getattr(storage, "upsert_funding", None)
    if callable(fn):
        try:
            n = fn(rows)
            return int(n or 0)
        except Exception as e:
            logger.warning("[Funding] storage.upsert_funding failed -> fallback SQL UPSERT (%s)", e, exc_info=True)

    pool = getattr(storage, "pool", None)
    if pool is None:
        raise RuntimeError("[Funding] storage.pool not found for SQL UPSERT fallback")

    params_full: List[Tuple[Any, ...]] = []
    params_min: List[Tuple[Any, ...]] = []
    for r in rows:
        params_full.append((
            int(r.get("exchange_id")),
            int(r.get("symbol_id")),
            r.get("funding_time"),
            _safe_float(r.get("funding_rate"), 0.0),
            (None if r.get("mark_price") is None else _safe_float(r.get("mark_price"), 0.0)),
            str(r.get("source") or ""),
        ))
        params_min.append((
            int(r.get("exchange_id")),
            int(r.get("symbol_id")),
            r.get("funding_time"),
            _safe_float(r.get("funding_rate"), 0.0),
        ))

    sql_full = """
    INSERT INTO public.funding (exchange_id, symbol_id, funding_time, funding_rate, mark_price, source)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (exchange_id, symbol_id, funding_time)
    DO UPDATE SET
        funding_rate = EXCLUDED.funding_rate,
        mark_price = COALESCE(EXCLUDED.mark_price, public.funding.mark_price),
        source = EXCLUDED.source
    """

    sql_min = """
    INSERT INTO public.funding (exchange_id, symbol_id, funding_time, funding_rate)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (exchange_id, symbol_id, funding_time)
    DO UPDATE SET
        funding_rate = EXCLUDED.funding_rate
    """

    with pool.connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.executemany(sql_full, params_full)
            except Exception:
                # если таблица не имеет mark_price/source — упадём сюда и вставим минимально
                cur.executemany(sql_min, params_min)
        try:
            conn.commit()
        except Exception:
            pass

    return len(rows)


# -------------------------
# DB watermarks
# -------------------------

def _load_watermarks_from_db(*, storage, exchange_id: int) -> Dict[int, datetime]:
    """
    Один запрос:
      symbol_id -> max(funding_time)
    """
    wm: Dict[int, datetime] = {}
    try:
        pool = getattr(storage, "pool", None)
        if pool is None:
            logger.warning("[Funding] storage.pool not found -> DB watermark load skipped")
            return wm

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol_id, MAX(funding_time) AS last_ts
                    FROM public.funding
                    WHERE exchange_id = %s
                    GROUP BY symbol_id
                    """,
                    (int(exchange_id),),
                )
                rows = cur.fetchall() or []
                for sid, last_ts in rows:
                    if sid is None or last_ts is None:
                        continue
                    wm[int(sid)] = _normalize_funding_time(last_ts) or last_ts
    except Exception:
        logger.exception("[Funding] failed to load watermarks from DB")

    return wm


# -------------------------
# parsing (REST fundingRate history)
# -------------------------

def _parse_funding_rows(
    *,
    exchange_id: int,
    symbol_id: int,
    payload: List[dict],
    source: str,
) -> List[dict]:
    """
    Парсим payload от rest.funding_rate(...)

    Ожидаем поля:
      fundingTime (ms), fundingRate, markPrice (optional)
    """
    rows: List[dict] = []
    for r in payload or []:
        if not isinstance(r, dict):
            continue

        ft_ms = r.get("fundingTime") or r.get("time")
        rate = r.get("fundingRate")
        if ft_ms is None or rate is None:
            continue

        funding_time = _normalize_funding_time(_from_ms(ft_ms))
        if funding_time is None:
            continue

        mark_price = r.get("markPrice")

        rows.append(
            {
                "exchange_id": int(exchange_id),
                "symbol_id": int(symbol_id),
                "funding_time": funding_time,
                "funding_rate": _safe_float(rate, 0.0),
                "mark_price": (_safe_float(mark_price, 0.0) if mark_price is not None else None),
                "source": str(source),
            }
        )
    return rows


def _max_funding_time(rows: List[dict]) -> Optional[datetime]:
    best: Optional[datetime] = None
    for r in rows or []:
        ft = r.get("funding_time")
        if isinstance(ft, datetime):
            if best is None or ft > best:
                best = ft
    return best


# -------------------------
# parsing (WS markPriceUpdate)
# -------------------------

def _parse_ws_mark_price_update(
    *,
    exchange_id: int,
    symbol_ids: Dict[str, int],
    msg: dict,
) -> Optional[dict]:
    """
    Binance USD-M Futures markPriceUpdate event:
      {
        "e": "markPriceUpdate",
        "E": ...,
        "s": "BTCUSDT",
        "p": "...",   # mark price
        "r": "...",   # funding rate
        "T": ...      # next funding time
      }

    Мы пишем funding_time = nextFundingTime (T) — будущая точка.
    UPSERT будет обновлять rate/mark_price по одному ключу.
    """
    if not isinstance(msg, dict):
        return None

    ev = msg.get("e") or msg.get("eventType")
    if ev != "markPriceUpdate":
        return None

    sym = (msg.get("s") or "").upper().strip()
    if not sym:
        return None

    sid = symbol_ids.get(sym)
    if sid is None:
        return None

    next_ft_ms = msg.get("T")
    rate = msg.get("r")
    mark_price = msg.get("p")

    if next_ft_ms is None or rate is None:
        return None

    funding_time = _normalize_funding_time(_from_ms(next_ft_ms))
    if funding_time is None:
        return None

    row = {
        "exchange_id": int(exchange_id),
        "symbol_id": int(sid),
        "funding_time": funding_time,
        "funding_rate": _safe_float(rate, 0.0),
        "mark_price": (_safe_float(mark_price, 0.0) if mark_price is not None else None),
        "source": "ws_markPriceUpdate",
    }
    return row


# -------------------------
# REST history runner (pagination-safe)
# -------------------------

def _run_funding_history_paged(
    *,
    storage,
    rest,
    exchange_id: int,
    sym: str,
    sid: int,
    start_dt: datetime,
    end_dt: datetime,
    limit: int,
    max_pages: int,
    page_sleep_sec: float,
    stop_event: threading.Event,
) -> Tuple[int, Optional[datetime]]:
    """
    Надёжная догрузка fundingRate history с пагинацией.
    """
    limit = max(1, min(int(limit), 1000))
    max_pages = max(1, int(max_pages))
    page_sleep_sec = max(0.0, float(page_sleep_sec))

    total_upserts = 0
    best_total: Optional[datetime] = None
    last_progress: Optional[datetime] = None

    if start_dt >= end_dt:
        return 0, None

    for _ in range(max_pages):
        if stop_event.is_set():
            break

        payload = rest.funding_rate(
            symbol=str(sym),
            start_time_ms=_to_ms(start_dt),
            end_time_ms=_to_ms(end_dt),
            limit=int(limit),
        )

        payload_list: List[dict]
        if isinstance(payload, list):
            payload_list = [x for x in payload if isinstance(x, dict)]
        elif isinstance(payload, dict):
            payload_list = [payload]
        else:
            payload_list = []

        rows = _parse_funding_rows(
            exchange_id=int(exchange_id),
            symbol_id=int(sid),
            payload=payload_list,
            source="fundingRate",
        )

        if not rows:
            break

        n = _upsert_funding_rows(storage=storage, rows=rows)
        total_upserts += int(n or 0)

        best = _max_funding_time(rows)
        if best is not None:
            if best_total is None or best > best_total:
                best_total = best

        if last_progress is not None and best is not None and best <= last_progress:
            logger.warning(
                "[Funding][%s] pagination stalled (best=%s <= last=%s) -> stop paging",
                sym,
                best,
                last_progress,
            )
            break
        if best is not None:
            last_progress = best

        if len(rows) < limit:
            break

        if best is None:
            break
        start_dt = best + timedelta(milliseconds=1)
        if start_dt >= end_dt:
            break

        if page_sleep_sec > 0 and stop_event.wait(page_sleep_sec):
            break

    return total_upserts, best_total


# -------------------------
# BACKFILL on start (SMART)
# -------------------------

def _run_backfill_all_symbols(
    *,
    storage,
    rest,
    exchange_id: int,
    symbols: List[Tuple[str, int]],
    watermarks: Dict[int, datetime],
    backfill_days: int,
    overlap_hours: int,
    limit: int,
    max_pages: int,
    page_sleep_sec: float,
    req_sleep_sec: float,
    cooldown_until: Dict[str, datetime],
    stop_event: threading.Event,
) -> None:
    if backfill_days <= 0:
        logger.info("[Funding][Backfill] backfill_days<=0 -> skip")
        return
    if not symbols:
        return

    now = _utc_now()
    min_dt = now - timedelta(days=int(backfill_days))

    logger.info(
        "[Funding][Backfill] start symbols=%d backfill_days=%d overlap_hours=%d min_dt=%s",
        len(symbols),
        backfill_days,
        overlap_hours,
        min_dt.isoformat(),
    )

    done = 0
    total_up = 0
    backoff = 1.0

    for sym, sid in symbols:
        if stop_event.is_set():
            break

        cd = cooldown_until.get(sym)
        if cd is not None and cd > now:
            continue

        try:
            prev_wm = watermarks.get(int(sid))
            if prev_wm is None:
                start_dt = min_dt
            else:
                start_dt = max(min_dt, prev_wm - timedelta(hours=int(overlap_hours)))

            if start_dt >= now:
                done += 1
                continue

            n, best = _run_funding_history_paged(
                storage=storage,
                rest=rest,
                exchange_id=int(exchange_id),
                sym=sym,
                sid=int(sid),
                start_dt=start_dt,
                end_dt=now,
                limit=limit,
                max_pages=max_pages,
                page_sleep_sec=page_sleep_sec,
                stop_event=stop_event,
            )
            total_up += int(n or 0)
            done += 1

            if best is not None:
                prev = watermarks.get(int(sid))
                if prev is None or best > prev:
                    watermarks[int(sid)] = best

            backoff = 1.0

        except Exception as e:
            code = _classify_http_error(e)
            if code in (418, 429):
                logger.warning("[Funding][Backfill] %s HTTP=%s -> cooldown 30m", sym, code, exc_info=True)
                cooldown_until[sym] = _utc_now() + timedelta(minutes=30)
            elif code == 451:
                logger.warning("[Funding][Backfill] %s HTTP=451 -> cooldown 6h", sym, code, exc_info=True)
                cooldown_until[sym] = _utc_now() + timedelta(hours=6)
            else:
                logger.exception("[Funding][Backfill] %s failed", sym)

            stop_event.wait(backoff)
            backoff = min(backoff * 2.0, 30.0)

        if req_sleep_sec > 0 and stop_event.wait(req_sleep_sec):
            break

    logger.info("[Funding][Backfill] done symbols_processed=%d/%d upserts=%d", done, len(symbols), total_up)


# -------------------------
# WS Funding (markPrice@arr)
# -------------------------

def start_funding_ws_collector(
    *,
    storage,
    exchange_id: int,
    symbol_ids: Dict[str, int],
    stop_event: threading.Event,
) -> BinanceWS:
    url = WS_STREAM_BASE + "!markPrice@arr"

    def _on_message(payload: Any) -> None:
        if stop_event.is_set():
            return

        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            payload2 = payload.get("data")
        else:
            payload2 = payload

        rows: List[dict] = []

        if isinstance(payload2, list):
            for item in payload2:
                if not isinstance(item, dict):
                    continue
                row = _parse_ws_mark_price_update(
                    exchange_id=int(exchange_id),
                    symbol_ids=symbol_ids,
                    msg=item,
                )
                if row:
                    rows.append(row)

        elif isinstance(payload2, dict):
            row = _parse_ws_mark_price_update(
                exchange_id=int(exchange_id),
                symbol_ids=symbol_ids,
                msg=payload2,
            )
            if row:
                rows.append(row)

        if rows:
            try:
                _upsert_funding_rows(storage=storage, rows=rows)
            except Exception:
                logger.exception("[Funding][WS] upsert failed rows=%d", len(rows))

    ws = BinanceWS(
        url=url,
        on_message=_on_message,
        name="BinanceFundingWS",
        reconnect_delay_sec=2.0,
        ping_interval=20,
        ping_timeout=10,
    )
    ws.start()
    logger.info("[Funding][WS] started stream=!markPrice@arr")
    return ws


def stop_funding_ws_collector(ws: Optional[BinanceWS]) -> None:
    if ws is None:
        return
    try:
        ws.stop()
    except Exception:
        logger.exception("[Funding][WS] stop failed")


# -------------------------
# main collector
# -------------------------

def run_funding_collector(
    *,
    storage,
    rest,
    exchange_id: int,
    symbol_ids: Dict[str, int],
    cfg: dict,
    stop_event: threading.Event,
) -> None:
    cfg = cfg or {}

    if not bool(cfg.get("enabled", True)):
        logger.info("[Funding] disabled by config")
        return

    use_ws = bool(cfg.get("use_ws", True))
    rest_safety_poll = bool(cfg.get("rest_safety_poll", True))

    backfill_on_start = bool(cfg.get("backfill_on_start", True))
    backfill_days = max(0, int(cfg.get("backfill_days", 10)))

    poll_sec = max(30.0, float(cfg.get("poll_sec", 1800.0)))
    symbols_per_cycle = max(1, int(cfg.get("symbols_per_cycle", 20)))
    req_sleep_sec = max(0.0, float(cfg.get("req_sleep_sec", 0.20)))

    limit = max(1, min(int(cfg.get("limit", 1000)), 1000))
    max_pages = max(1, int(cfg.get("max_pages", 50)))
    page_sleep_sec = max(0.0, float(cfg.get("page_sleep_sec", 0.0)))

    overlap_hours = max(0, int(cfg.get("overlap_hours", 72)))

    symbols = sorted(
        (
            (str(sym).upper().strip(), int(sid))
            for sym, sid in (symbol_ids or {}).items()
            if str(sym).strip()
        ),
        key=lambda x: x[0],
    )
    total = len(symbols)

    logger.info(
        "[Funding] started symbols=%d use_ws=%s rest_safety_poll=%s "
        "backfill_on_start=%s backfill_days=%d rest_poll=%.1fs batch=%d "
        "limit=%d max_pages=%d overlap_hours=%d",
        total,
        use_ws,
        rest_safety_poll,
        backfill_on_start,
        backfill_days,
        poll_sec,
        symbols_per_cycle,
        limit,
        max_pages,
        overlap_hours,
    )

    watermarks = _load_watermarks_from_db(storage=storage, exchange_id=int(exchange_id))
    logger.info("[Funding] loaded watermarks from DB: %d", len(watermarks))

    cooldown_until: Dict[str, datetime] = {}

    if backfill_on_start and backfill_days > 0 and total > 0:
        _run_backfill_all_symbols(
            storage=storage,
            rest=rest,
            exchange_id=int(exchange_id),
            symbols=symbols,
            watermarks=watermarks,
            backfill_days=int(backfill_days),
            overlap_hours=int(overlap_hours),
            limit=limit,
            max_pages=max_pages,
            page_sleep_sec=page_sleep_sec,
            req_sleep_sec=req_sleep_sec,
            cooldown_until=cooldown_until,
            stop_event=stop_event,
        )

    ws_client: Optional[BinanceWS] = None
    if use_ws and total > 0:
        ws_client = start_funding_ws_collector(
            storage=storage,
            exchange_id=int(exchange_id),
            symbol_ids=dict(symbol_ids or {}),
            stop_event=stop_event,
        )

    if not rest_safety_poll:
        logger.info("[Funding] REST safety poll disabled -> WS only mode")
        while not stop_event.is_set():
            stop_event.wait(1.0)
        stop_funding_ws_collector(ws_client)
        return

    idx = 0
    backoff = 1.0

    while not stop_event.is_set():
        try:
            t0 = time.time()
            now = _utc_now()

            if total == 0:
                stop_event.wait(5.0)
                continue

            batch = symbols[idx: idx + symbols_per_cycle]
            if not batch:
                idx = 0
                continue

            upserts = 0
            min_dt = now - timedelta(days=max(1, int(backfill_days)))

            for sym, sid in batch:
                if stop_event.is_set():
                    break

                cd = cooldown_until.get(sym)
                if cd is not None and cd > now:
                    continue

                last_dt = watermarks.get(int(sid))
                if last_dt is None:
                    start_dt = min_dt
                else:
                    start_dt = max(min_dt, last_dt - timedelta(hours=int(overlap_hours)))

                if start_dt >= now:
                    continue

                try:
                    n, best = _run_funding_history_paged(
                        storage=storage,
                        rest=rest,
                        exchange_id=int(exchange_id),
                        sym=sym,
                        sid=int(sid),
                        start_dt=start_dt,
                        end_dt=now,
                        limit=limit,
                        max_pages=max_pages,
                        page_sleep_sec=page_sleep_sec,
                        stop_event=stop_event,
                    )
                    upserts += int(n or 0)
                    if best is not None:
                        prev = watermarks.get(int(sid))
                        if prev is None or best > prev:
                            watermarks[int(sid)] = best

                except Exception as e:
                    code = _classify_http_error(e)
                    if code in (418, 429):
                        logger.warning("[Funding][REST] %s HTTP=%s -> cooldown 30m", sym, code, exc_info=True)
                        cooldown_until[sym] = _utc_now() + timedelta(minutes=30)
                    elif code == 451:
                        logger.warning("[Funding][REST] %s HTTP=451 -> cooldown 6h", sym, code, exc_info=True)
                        cooldown_until[sym] = _utc_now() + timedelta(hours=6)
                    else:
                        logger.exception("[Funding][REST] %s failed", sym)

                if req_sleep_sec > 0 and stop_event.wait(req_sleep_sec):
                    break

            idx += len(batch)
            if idx >= total:
                idx = 0

            logger.info("[Funding][REST] poll batch=%d upserts=%d", len(batch), upserts)

            backoff = 1.0
            dt = time.time() - t0
            sleep = max(0.0, float(poll_sec) - dt)
            stop_event.wait(sleep)

        except Exception as e:
            logger.warning("[Funding] loop error: %s", e, exc_info=True)
            stop_event.wait(backoff)
            backoff = min(backoff * 2.0, 300.0)

    stop_funding_ws_collector(ws_client)


def start_funding_collector(
    *,
    rest,
    storage,
    exchange_id: int,
    symbol_ids: Dict[str, int],
    cfg: dict,
    stop_event: threading.Event,
) -> threading.Thread:
    t = threading.Thread(
        target=run_funding_collector,
        kwargs=dict(
            storage=storage,
            rest=rest,
            exchange_id=int(exchange_id),
            symbol_ids=dict(symbol_ids or {}),
            cfg=dict(cfg or {}),
            stop_event=stop_event,
        ),
        daemon=True,
        name="BinanceFundingCollector",
    )
    t.start()
    logger.info("[Funding] collector thread started")
    return t
