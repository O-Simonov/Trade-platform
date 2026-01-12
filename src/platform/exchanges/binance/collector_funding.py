# src/platform/exchanges/binance/collector_funding.py
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("src.platform.exchanges.binance.collector_funding")


# Funding on Binance USDT-M is every 8 hours (00:00, 08:00, 16:00 UTC)
_FUNDING_INTERVAL_HOURS = 8


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _classify_http_error(exc: Exception) -> Optional[int]:
    """
    Пытаемся достать HTTP code из текста ошибки вида:
      RuntimeError("Binance HTTP 429 GET /...: ...")
    """
    s = str(exc)
    for code in (451, 418, 429, 403, 400):
        if f"HTTP {code}" in s:
            return code
    return None


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _load_watermarks_from_db(*, storage, exchange_id: int) -> Dict[int, datetime]:
    """
    Один запрос вместо N запросов:
      symbol_id -> max(funding_time)
    """
    wm: Dict[int, datetime] = {}
    try:
        pool = getattr(storage, "pool", None)
        if pool is None:
            logger.warning("[Funding] storage.pool not found -> will use in-memory only")
            return wm

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT symbol_id, MAX(funding_time) AS last_ts
                    FROM funding
                    WHERE exchange_id = %s
                    GROUP BY symbol_id
                    """,
                    (int(exchange_id),),
                )
                for sid, last_ts in cur.fetchall():
                    if sid is None or last_ts is None:
                        continue
                    wm[int(sid)] = last_ts
    except Exception:
        logger.exception("[Funding] failed to load watermarks from DB")

    return wm


def _parse_funding_rows(*, exchange_id: int, symbol_id: int, payload: List[dict], source: str) -> List[dict]:
    rows: List[dict] = []
    for r in payload or []:
        ft_ms = r.get("fundingTime") or r.get("time")
        rate = r.get("fundingRate")
        if ft_ms is None or rate is None:
            continue

        funding_time = datetime.fromtimestamp(int(ft_ms) / 1000, tz=timezone.utc)
        mark_price = r.get("markPrice")

        rows.append(
            dict(
                exchange_id=int(exchange_id),
                symbol_id=int(symbol_id),
                funding_time=funding_time,
                funding_rate=_safe_float(rate, 0.0),
                mark_price=(_safe_float(mark_price, 0.0) if mark_price is not None else None),
                source=str(source),
            )
        )
    return rows


def _premium_index_to_row_last_funding(
    *,
    exchange_id: int,
    symbol_id: int,
    data: dict,
    source: str,
) -> Optional[dict]:
    """
    premiumIndex содержит:
      nextFundingTime (будущее)
      lastFundingRate (ставка последнего фандинга)
      markPrice

    Чтобы корректно положить в funding-таблицу, используем:
      funding_time = nextFundingTime - 8h
    """
    if not data:
        return None

    next_ms = data.get("nextFundingTime")
    rate = data.get("lastFundingRate")
    mark = data.get("markPrice")
    if next_ms is None or rate is None:
        return None

    next_dt = datetime.fromtimestamp(int(next_ms) / 1000, tz=timezone.utc)
    funding_time = next_dt - timedelta(hours=_FUNDING_INTERVAL_HOURS)

    return {
        "exchange_id": int(exchange_id),
        "symbol_id": int(symbol_id),
        "funding_time": funding_time,
        "funding_rate": _safe_float(rate, 0.0),
        "mark_price": (_safe_float(mark, 0.0) if mark is not None else None),
        "source": str(source),
    }


def _run_funding_history_one_symbol(
    *,
    storage,
    rest,
    exchange_id: int,
    sym: str,
    sid: int,
    start_dt: datetime,
    end_dt: datetime,
    limit: int,
) -> Tuple[int, Optional[datetime]]:
    """
    Возвращает (upserts, max_funding_time)
    """
    payload = rest.funding_rate(
        symbol=sym,
        start_time_ms=_to_ms(start_dt),
        end_time_ms=_to_ms(end_dt),
        limit=int(limit),
    )
    rows = _parse_funding_rows(exchange_id=exchange_id, symbol_id=sid, payload=payload or [], source="fundingRate")
    if not rows:
        return 0, None

    n = int(storage.upsert_funding(rows) or 0)

    best: Optional[datetime] = None
    for r in rows:
        ft = r.get("funding_time")
        if isinstance(ft, datetime):
            if best is None or ft > best:
                best = ft

    return n, best


def _run_premium_index_snapshot_all(
    *,
    storage,
    rest,
    exchange_id: int,
    symbol_ids: Dict[str, int],
) -> Tuple[int, Dict[int, datetime]]:
    """
    Один вызов premiumIndex без symbol -> список по всем.
    Возвращает (upserts, {symbol_id: funding_time})
    """
    data = rest.premium_index(symbol=None)  # None => all symbols
    if not data:
        return 0, {}

    data_list = [data] if isinstance(data, dict) else list(data)

    rows: List[dict] = []
    wm_updates: Dict[int, datetime] = {}

    for item in data_list:
        sym = str(item.get("symbol") or "").upper().strip()
        if not sym:
            continue
        sid = symbol_ids.get(sym)
        if sid is None:
            continue

        row = _premium_index_to_row_last_funding(
            exchange_id=exchange_id,
            symbol_id=int(sid),
            data=item,
            source="premiumIndex",
        )
        if row:
            rows.append(row)
            ft = row.get("funding_time")
            if isinstance(ft, datetime):
                wm_updates[int(sid)] = ft

    if not rows:
        return 0, {}

    upserts = int(storage.upsert_funding(rows) or 0)
    return upserts, wm_updates


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

    enabled = bool(cfg.get("enabled", True))
    if not enabled:
        logger.info("[Funding] disabled by config")
        return

    poll_sec = float(cfg.get("poll_sec", 900))  # funding меняется редко → 15 минут ок
    symbols_per_cycle = int(cfg.get("symbols_per_cycle", 3))
    req_sleep_sec = float(cfg.get("req_sleep_sec", 0.40))  # чуть медленнее, чтобы не конкурировать со свечами/OI

    seed_on_start = bool(cfg.get("seed_on_start", False))
    seed_days = max(0, int(cfg.get("seed_days", 365)))

    lookback_hours_if_empty = max(24, int(cfg.get("lookback_hours_if_empty", 72)))

    limit = int(cfg.get("limit", 1000))
    limit = max(1, min(limit, 1000))

    also_premium_index = bool(cfg.get("also_premium_index", False))
    premium_poll_sec = float(cfg.get("premium_poll_sec", 1800))  # раз в 30 минут достаточно

    symbols = sorted(symbol_ids.items(), key=lambda x: x[0])  # [(SYMBOL, symbol_id)]
    total = len(symbols)

    logger.info(
        "[Funding] started symbols=%d poll=%.1fs batch=%d seed_on_start=%s seed_days=%d limit=%d premium=%s premium_poll=%.0fs",
        total,
        poll_sec,
        symbols_per_cycle,
        seed_on_start,
        seed_days,
        limit,
        also_premium_index,
        premium_poll_sec,
    )

    # --- Watermarks ---
    watermarks = _load_watermarks_from_db(storage=storage, exchange_id=exchange_id)
    logger.info("[Funding] loaded watermarks from DB: %d", len(watermarks))

    # per-symbol cooldown after 418/429
    cooldown_until: Dict[str, datetime] = {}

    # --- SEED ONCE (только для тех, у кого вообще нет данных) ---
    if seed_on_start and seed_days > 0 and total > 0:
        end_dt = _utc_now()
        seed_from = end_dt - timedelta(days=seed_days)
        total_seed = 0
        seeded = 0

        for sym, sid in symbols:
            if stop_event.is_set():
                break
            if watermarks.get(int(sid)) is not None:
                continue

            try:
                n, max_ft = _run_funding_history_one_symbol(
                    storage=storage,
                    rest=rest,
                    exchange_id=exchange_id,
                    sym=sym,
                    sid=int(sid),
                    start_dt=seed_from,
                    end_dt=end_dt,
                    limit=limit,
                )
                total_seed += int(n or 0)
                seeded += 1
                if max_ft is not None:
                    watermarks[int(sid)] = max_ft

            except Exception as e:
                code = _classify_http_error(e)
                if code in (418, 429):
                    logger.warning("[FundingSeed] %s HTTP=%s (rate/ban)", sym, code, exc_info=True)
                    cooldown_until[sym] = _utc_now() + timedelta(minutes=30)
                elif code == 451:
                    logger.warning("[FundingSeed] %s HTTP=451 restricted", sym, exc_info=True)
                    cooldown_until[sym] = _utc_now() + timedelta(hours=6)
                else:
                    logger.exception("[FundingSeed] %s failed", sym)

            if stop_event.wait(req_sleep_sec):
                break

        logger.info("[FundingSeed] done seeded_symbols=%d upserts=%d", seeded, total_seed)

    # --- LOOP ---
    idx = 0
    backoff = 1.0
    last_premium_ts = 0.0

    while not stop_event.is_set():
        try:
            t0 = time.time()
            now = _utc_now()

            if total == 0:
                stop_event.wait(5.0)
                continue

            # 1) fundingRate history incremental (маленький батч)
            batch = symbols[idx : idx + max(1, symbols_per_cycle)]
            if not batch:
                idx = 0
                continue

            upserts = 0
            for sym, sid in batch:
                if stop_event.is_set():
                    break

                cd = cooldown_until.get(sym)
                if cd is not None and cd > now:
                    continue

                last_dt = watermarks.get(int(sid))
                if last_dt is None:
                    start_dt = now - timedelta(hours=lookback_hours_if_empty)
                else:
                    # небольшой overlap, чтобы не терять точку вокруг границы
                    start_dt = last_dt - timedelta(hours=1)

                try:
                    n, max_ft = _run_funding_history_one_symbol(
                        storage=storage,
                        rest=rest,
                        exchange_id=exchange_id,
                        sym=sym,
                        sid=int(sid),
                        start_dt=start_dt,
                        end_dt=now,
                        limit=limit,
                    )
                    upserts += int(n or 0)
                    if max_ft is not None:
                        prev = watermarks.get(int(sid))
                        if prev is None or max_ft > prev:
                            watermarks[int(sid)] = max_ft

                except Exception as e:
                    code = _classify_http_error(e)
                    if code in (418, 429):
                        logger.warning("[Funding] %s HTTP=%s (rate/ban)", sym, code, exc_info=True)
                        cooldown_until[sym] = _utc_now() + timedelta(minutes=30)
                    elif code == 451:
                        logger.warning("[Funding] %s HTTP=451 restricted", sym, exc_info=True)
                        cooldown_until[sym] = _utc_now() + timedelta(hours=6)
                    else:
                        logger.exception("[Funding] %s failed", sym)

                if stop_event.wait(req_sleep_sec):
                    break

            idx += len(batch)
            if idx >= total:
                idx = 0

            # 2) premiumIndex snapshot (ОДИН запрос, не каждый цикл)
            if also_premium_index and not stop_event.is_set():
                if (time.time() - last_premium_ts) >= float(premium_poll_sec):
                    try:
                        n2, wm2 = _run_premium_index_snapshot_all(
                            storage=storage,
                            rest=rest,
                            exchange_id=exchange_id,
                            symbol_ids=symbol_ids,
                        )
                        upserts += int(n2 or 0)
                        for sid, ft in (wm2 or {}).items():
                            prev = watermarks.get(int(sid))
                            if prev is None or ft > prev:
                                watermarks[int(sid)] = ft
                        last_premium_ts = time.time()
                    except Exception as e:
                        code = _classify_http_error(e)
                        if code in (418, 429):
                            logger.warning("[Funding] premiumIndex HTTP=%s (rate/ban)", code, exc_info=True)
                            # глобальный backoff на premiumIndex
                            last_premium_ts = time.time()  # чтобы не спамить
                        else:
                            logger.exception("[Funding] premiumIndex snapshot failed")

            logger.info("[Funding] poll batch=%d upserts=%d premium=%s", len(batch), upserts, also_premium_index)

            backoff = 1.0
            dt = time.time() - t0
            sleep = max(0.0, float(poll_sec) - dt)
            stop_event.wait(sleep)

        except Exception as e:
            logger.warning("[Funding] error: %s", e, exc_info=True)
            stop_event.wait(backoff)
            backoff = min(backoff * 2.0, 300.0)


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
            symbol_ids=dict(symbol_ids),
            cfg=dict(cfg or {}),
            stop_event=stop_event,
        ),
        daemon=True,
        name="BinanceFundingCollector",
    )
    t.start()
    logger.info("[Funding] collector thread started")
    return t
