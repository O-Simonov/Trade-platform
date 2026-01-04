# src/platform/exchanges/binance/collector_funding.py
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("src.platform.exchanges.binance.collector_funding")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _db_last_funding_time(*, storage, exchange_id: int, symbol_id: int) -> Optional[datetime]:
    try:
        with storage.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT max(funding_time)
                    FROM funding
                    WHERE exchange_id=%s AND symbol_id=%s
                    """,
                    (int(exchange_id), int(symbol_id)),
                )
                r = cur.fetchone()
                return r[0] if r and r[0] else None
    except Exception:
        logger.exception("[Funding] failed to read last funding_time from DB")
        return None


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
                funding_rate=float(rate),
                mark_price=(float(mark_price) if mark_price is not None else None),
                source=str(source),
            )
        )
    return rows


def _premium_index_to_row(*, exchange_id: int, symbol_id: int, data: dict, source: str) -> Optional[dict]:
    if not data:
        return None

    # В премиум-индексе Binance:
    # nextFundingTime - время следующего фандинга
    # lastFundingRate - последняя ставка
    next_ms = data.get("nextFundingTime")
    rate = data.get("lastFundingRate")
    mark = data.get("markPrice")
    if next_ms is None or rate is None:
        return None

    funding_time = datetime.fromtimestamp(int(next_ms) / 1000, tz=timezone.utc)

    return {
        "exchange_id": int(exchange_id),
        "symbol_id": int(symbol_id),
        "funding_time": funding_time,
        "funding_rate": float(rate),
        "mark_price": float(mark) if mark is not None else None,
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
) -> int:
    payload = rest.funding_rate(
        symbol=sym,
        start_time_ms=_to_ms(start_dt),
        end_time_ms=_to_ms(end_dt),
        limit=int(limit),
    )
    rows = _parse_funding_rows(exchange_id=exchange_id, symbol_id=sid, payload=payload or [], source="fundingRate")
    if not rows:
        return 0
    return int(storage.upsert_funding(rows) or 0)


def _run_premium_index_snapshot_all(
    *,
    storage,
    rest,
    exchange_id: int,
    symbol_ids: Dict[str, int],
) -> int:
    """
    Один вызов premiumIndex без symbol -> список по всем.
    """
    data = rest.premium_index(symbol=None)  # важно: None => all symbols
    if not data:
        return 0

    # Binance может вернуть dict (если symbol указан) или list (если нет)
    if isinstance(data, dict):
        data_list = [data]
    else:
        data_list = list(data)

    rows: List[dict] = []
    for item in data_list:
        sym = str(item.get("symbol") or "").upper().strip()
        if not sym:
            continue
        sid = symbol_ids.get(sym)
        if sid is None:
            continue
        row = _premium_index_to_row(exchange_id=exchange_id, symbol_id=int(sid), data=item, source="premiumIndex")
        if row:
            rows.append(row)

    if not rows:
        return 0
    return int(storage.upsert_funding(rows) or 0)


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

    poll_sec = float(cfg.get("poll_sec", 600))
    symbols_per_cycle = int(cfg.get("symbols_per_cycle", 5))
    req_sleep_sec = float(cfg.get("req_sleep_sec", 0.25))

    seed_on_start = bool(cfg.get("seed_on_start", False))
    seed_days = int(cfg.get("seed_days", 365))

    limit = int(cfg.get("limit", 1000))
    also_premium_index = bool(cfg.get("also_premium_index", False))

    symbols = sorted(symbol_ids.items(), key=lambda x: x[0])  # [(SYMBOL, symbol_id)]
    logger.info(
        "[Funding] started symbols=%d poll=%.1fs batch=%d seed_on_start=%s seed_days=%d limit=%d premium=%s",
        len(symbols),
        poll_sec,
        symbols_per_cycle,
        seed_on_start,
        seed_days,
        limit,
        also_premium_index,
    )

    # --- SEED ONCE ---
    if seed_on_start and seed_days > 0:
        end_dt = _utc_now()
        seed_from = end_dt - timedelta(days=seed_days)
        total_seed = 0
        for sym, sid in symbols:
            if stop_event.is_set():
                break
            last_dt = _db_last_funding_time(storage=storage, exchange_id=exchange_id, symbol_id=int(sid))
            if last_dt is not None:
                continue
            try:
                total_seed += _run_funding_history_one_symbol(
                    storage=storage,
                    rest=rest,
                    exchange_id=exchange_id,
                    sym=sym,
                    sid=int(sid),
                    start_dt=seed_from,
                    end_dt=end_dt,
                    limit=limit,
                )
            except Exception:
                logger.exception("[FundingSeed] %s failed", sym)
            if stop_event.wait(req_sleep_sec):
                break

        logger.info("[FundingSeed] done upserts=%d", total_seed)

    # --- LOOP ---
    backoff = 1.0
    idx = 0

    while not stop_event.is_set():
        try:
            t0 = time.time()
            now = _utc_now()

            # 1) history incremental: небольшой батч символов
            batch = symbols[idx : idx + max(1, symbols_per_cycle)]
            if not batch:
                idx = 0
                continue

            upserts = 0
            for sym, sid in batch:
                if stop_event.is_set():
                    break

                last_dt = _db_last_funding_time(storage=storage, exchange_id=exchange_id, symbol_id=int(sid))
                start_dt = (now - timedelta(days=2)) if last_dt is None else (last_dt - timedelta(hours=1))
                try:
                    upserts += _run_funding_history_one_symbol(
                        storage=storage,
                        rest=rest,
                        exchange_id=exchange_id,
                        sym=sym,
                        sid=int(sid),
                        start_dt=start_dt,
                        end_dt=now,
                        limit=limit,
                    )
                except Exception:
                    logger.exception("[Funding] %s failed", sym)

                if stop_event.wait(req_sleep_sec):
                    break

            idx += len(batch)
            if idx >= len(symbols):
                idx = 0

            # 2) premiumIndex snapshot: ОДИН запрос на цикл
            if also_premium_index and not stop_event.is_set():
                try:
                    upserts += _run_premium_index_snapshot_all(
                        storage=storage,
                        rest=rest,
                        exchange_id=exchange_id,
                        symbol_ids=symbol_ids,
                    )
                except Exception:
                    logger.exception("[Funding] premiumIndex snapshot failed")

            logger.info("[Funding] poll batch=%d upserts=%d premium=%s", len(batch), upserts, also_premium_index)

            backoff = 1.0
            dt = time.time() - t0
            sleep = max(0.0, poll_sec - dt)
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
) -> None:
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
