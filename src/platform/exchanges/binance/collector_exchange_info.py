# src/platform/exchanges/binance/collector_exchange_info.py
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from src.platform.exchanges.binance.filters import parse_symbol_filters

logger = logging.getLogger("src.platform.exchanges.binance.collector_exchange_info")


def _utc_now():
    return datetime.now(timezone.utc)


def sync_exchange_info(
    *,
    binance_rest,
    storage,
    exchange_id: int,
    symbol_ids: dict[str, int],
) -> int:
    """
    Fetch Binance Futures exchangeInfo and store symbol filters.
    Bootstrap only (best-effort).

    Notes:
      - Normalizes symbol key to UPPER().
      - Adds updated_at explicitly.
      - Never raises: logs and returns 0 on errors.
    """
    if not symbol_ids:
        return 0

    try:
        info: dict[str, Any] = binance_rest.fetch_exchange_info() or {}
    except Exception:
        logger.exception("[ExchangeInfo] fetch_exchange_info failed")
        return 0

    symbols = info.get("symbols") or []
    if not symbols:
        logger.warning("[ExchangeInfo] exchangeInfo symbols empty")
        return 0

    now = _utc_now()
    rows: list[dict] = []

    # ensure lookup uses uppercase
    # (symbol_ids обычно уже такой, но делаем безопасно)
    sym_to_id = {str(k).upper().strip(): int(v) for k, v in (symbol_ids or {}).items()}

    for s in symbols:
        try:
            if s.get("contractType") != "PERPETUAL":
                continue

            symbol = str(s.get("symbol") or "").upper().strip()
            if not symbol:
                continue

            sid = sym_to_id.get(symbol)
            if sid is None:
                continue

            parsed = parse_symbol_filters(s) or {}

            row = {
                "exchange_id": int(exchange_id),
                "symbol_id": int(sid),

                "price_tick": parsed.get("price_tick"),
                "qty_step": parsed.get("qty_step"),
                "min_qty": parsed.get("min_qty"),
                "max_qty": parsed.get("max_qty"),
                "min_notional": parsed.get("min_notional"),

                "max_leverage": parsed.get("max_leverage"),
                "margin_type": parsed.get("margin_type"),

                "updated_at": now,  # важно для freshness-логики и idempotency
            }
            rows.append(row)

        except Exception:
            logger.debug("[ExchangeInfo] failed to parse one symbol", exc_info=True)
            continue

    if not rows:
        logger.info("[ExchangeInfo] nothing to upsert (matched=0)")
        return 0

    try:
        n = int(storage.upsert_symbol_filters(rows) or 0)
        logger.info("[ExchangeInfo] upserted=%d rows=%d", n, len(rows))
        return n
    except Exception:
        logger.exception("[ExchangeInfo] upsert_symbol_filters failed rows=%d", len(rows))
        return 0
