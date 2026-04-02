# src/platform/exchanges/binance/collector_universe.py
from __future__ import annotations

import inspect
import logging
import re
from typing import Any, Dict, List

logger = logging.getLogger("src.platform.exchanges.binance.collector_universe")

# строгий whitelist: только A-Z0-9
_RE_VALID_SYMBOL = re.compile(r"^[A-Z0-9]+$")


def _call_supported(fn, **kwargs):
    """
    Calls fn with only supported kwargs if signature is strict,
    otherwise passes everything.
    """
    try:
        sig = inspect.signature(fn)
        params = sig.parameters
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
            return fn(**kwargs)
        supported = {k: v for k, v in kwargs.items() if k in params}
        return fn(**supported)
    except Exception:
        return fn(**kwargs)


def _count_active_symbols(storage, exchange_id: int) -> int:
    if hasattr(storage, "count_active_symbols"):
        try:
            return int(storage.count_active_symbols(exchange_id=int(exchange_id)))  # type: ignore[attr-defined]
        except Exception:
            pass

    pool = getattr(storage, "pool", None)
    if pool is None:
        return 0

    try:
        with pool.connection() as conn:
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


def _normalize_mapping(mapping: Any) -> Dict[str, int]:
    if not isinstance(mapping, dict) or not mapping:
        return {}

    out: Dict[str, int] = {}
    for k, v in mapping.items():
        if isinstance(k, str):
            sym = k.upper().strip()
            if not sym:
                continue
            try:
                sid = int(v)
            except Exception:
                continue
            out[sym] = sid
            continue

        if isinstance(v, str):
            sym = v.upper().strip()
            if not sym:
                continue
            try:
                sid = int(k)
            except Exception:
                continue
            out[sym] = sid
            continue

    return out


def _is_valid_fstream_symbol(sym: str) -> bool:
    """
    Защита от мусора в symbols.symbol.
    Для fstream (Binance USD-M) символы типа BTCUSDT, 1000PEPEUSDT и т.п.
    """
    if not sym:
        return False
    sym = sym.upper().strip()

    # только ASCII
    try:
        sym.encode("ascii")
    except Exception:
        return False

    # только A-Z0-9, без пробелов/прочего
    if _RE_VALID_SYMBOL.fullmatch(sym) is None:
        return False

    # разумная длина, чтобы отсечь странные строки
    if len(sym) < 3 or len(sym) > 25:
        return False

    return True


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
      - if fetched universe < min_size -> abort
      - if fetched drops too much vs active_db (>max_drop_ratio) -> skip deactivation
    """
    info: Dict[str, Any] = binance_rest.fetch_exchange_info() or {}
    symbols_info = info.get("symbols") or []

    universe: List[str] = []
    bad = 0

    for s in symbols_info:
        if s.get("contractType") != "PERPETUAL":
            continue
        if s.get("quoteAsset") != "USDT":
            continue
        if s.get("status") != "TRADING":
            continue

        sym = (s.get("symbol") or "").upper().strip()
        if not _is_valid_fstream_symbol(sym):
            bad += 1
            continue

        universe.append(sym)

    universe = sorted(set(universe))
    fetched = len(universe)

    if bad:
        logger.warning("[Universe] filtered bad symbols=%d", int(bad))

    if fetched < int(min_size):
        logger.warning("[Universe] fetched too small: %d < min_size=%d -> skip", fetched, int(min_size))
        return {}

    active_db = _count_active_symbols(storage, exchange_id=int(exchange_id))

    deactivate_missing = True
    if active_db > 0:
        drop_ratio = (float(active_db - fetched) / float(active_db)) if active_db else 0.0
        drop_ratio = max(0.0, float(drop_ratio))
        if drop_ratio > float(max_drop_ratio):
            deactivate_missing = False
            logger.warning(
                "[Universe] suspicious drop: active_db=%d fetched=%d drop_ratio=%.2f > %.2f -> deactivation disabled",
                active_db,
                fetched,
                drop_ratio,
                float(max_drop_ratio),
            )

    # ВАЖНО: под твою сигнатуру upsert_symbols(..., deactivate_missing=...)
    raw_mapping = _call_supported(
        storage.upsert_symbols,
        exchange_id=int(exchange_id),
        symbols=universe,
        deactivate_missing=bool(deactivate_missing),
    )

    mapping = _normalize_mapping(raw_mapping)

    logger.info(
        "[Universe] upserted=%d deactivate_missing=%s (active_db=%d fetched=%d)",
        len(mapping),
        deactivate_missing,
        active_db,
        fetched,
    )

    return mapping
