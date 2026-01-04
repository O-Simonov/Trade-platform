# src/platform/exchanges/binance/collector_universe.py
from __future__ import annotations

import inspect
import logging
from typing import Any, Dict, List

logger = logging.getLogger("src.platform.exchanges.binance.collector_universe")


def _call_supported(fn, **kwargs):
    try:
        sig = inspect.signature(fn)
        supported = {k: v for k, v in kwargs.items() if k in sig.parameters}
        return fn(**supported)
    except Exception:
        return fn(**kwargs)


def _count_active_symbols(storage, exchange_id: int) -> int:
    # 1) если есть метод — используем
    if hasattr(storage, "count_active_symbols"):
        try:
            return int(storage.count_active_symbols(exchange_id=int(exchange_id)))  # type: ignore[attr-defined]
        except Exception:
            pass

    # 2) fallback: SQL
    try:
        with storage.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT count(*)
                    FROM symbols
                    WHERE exchange_id=%s AND is_active=true
                    """,
                    (int(exchange_id),),
                )
                r = cur.fetchone()
                return int(r[0] or 0) if r else 0
    except Exception:
        logger.exception("[Universe] failed to count active symbols")
        return 0


def sync_tradable_usdtm_perpetual_symbols(
    *,
    binance_rest,
    storage,
    exchange_id: int,
    min_size: int = 200,
    max_drop_ratio: float = 0.30,
) -> Dict[str, int]:
    """
    Fetch Binance Futures exchangeInfo and upsert ALL tradable USDT-M perpetual symbols.

    Filters:
      - contractType=PERPETUAL
      - quoteAsset=USDT
      - status=TRADING

    Safety:
      - if fetched universe < min_size -> abort (do nothing)
      - if fetched drops too much vs active_db (>max_drop_ratio) -> skip deactivation
    """
    info: Dict[str, Any] = binance_rest.fetch_exchange_info() or {}
    symbols = info.get("symbols") or []

    universe: List[str] = []
    for s in symbols:
        if s.get("contractType") != "PERPETUAL":
            continue
        if s.get("quoteAsset") != "USDT":
            continue
        if s.get("status") != "TRADING":
            continue
        sym = (s.get("symbol") or "").upper().strip()
        if sym:
            universe.append(sym)

    universe = sorted(set(universe))
    fetched = len(universe)
    if fetched < int(min_size):
        logger.warning("[Universe] fetched too small: %d < min_size=%d -> skip", fetched, int(min_size))
        return {}

    active_db = _count_active_symbols(storage, exchange_id=int(exchange_id))
    deactivate_missing = True
    if active_db > 0:
        drop_ratio = float(active_db - fetched) / float(active_db)
        if drop_ratio > float(max_drop_ratio):
            deactivate_missing = False
            logger.warning(
                "[Universe] suspicious drop: active_db=%d fetched=%d drop_ratio=%.2f > %.2f -> deactivation disabled",
                active_db,
                fetched,
                drop_ratio,
                float(max_drop_ratio),
            )

    # upsert + mark active (адаптивно к сигнатуре)
    mapping = _call_supported(
        storage.upsert_symbols,
        exchange_id=int(exchange_id),
        symbols=universe,
        mark_active=True,
    ) or {}

    if deactivate_missing and hasattr(storage, "deactivate_missing_symbols"):
        try:
            deactivated = storage.deactivate_missing_symbols(exchange_id=int(exchange_id), active_symbols=universe)
            if deactivated:
                logger.info("[Universe] deactivated symbols: %d", int(deactivated))
        except Exception:
            logger.exception("[Universe] deactivate_missing_symbols failed (ignored)")

    logger.info(
        "[Universe] upserted=%d deactivate_missing=%s (active_db=%d fetched=%d)",
        len(mapping),
        deactivate_missing,
        active_db,
        fetched,
    )
    return {str(k).upper(): int(v) for k, v in (mapping or {}).items()}
