# -*- coding: utf-8 -*-
"""
trade_liquidation_full_ready.py

Готовый самодостаточный файл для проекта Trade-platform.
Назначение:
- включить корректную материализацию hedge-ног в БД,
- вести отдельные записи live / live_hedge в position_ledger,
- поддерживать hedge_links,
- синхронизировать positions после обновления ledger.

ВАЖНО:
Этот файл не является "кусочком diff".
Это готовый модуль, который можно положить в проект и импортировать.
Он не требует ручной правки своего содержимого.

Подключение:
1) Скопировать файл в:
   src/platform/traders/trade_liquidation_full_ready.py

2) Добавить ОДНУ строку импорта в запуск:
   from src.platform.traders import trade_liquidation_full_ready  # noqa: F401

После импорта модуль сам пропатчит TradeLiquidation.

Почему не полная замена старого trade_liquidation.py:
- исходник вашего полного файла в этой сессии не был предоставлен,
- поэтому безопаснее дать законченный подключаемый модуль,
  чем "угадать" и сломать рабочую основную стратегию.

Что исправляет:
- если на бирже в hedge mode есть противоположная нога, а в БД её нет,
  создаётся отдельная OPEN запись source='live_hedge'
- если hedge-нога исчезла на бирже, соответствующая запись live_hedge закрывается
- создаётся связь в hedge_links
- после обновления ledger запускается sync в positions / snapshots

Ожидаемый эффект:
- position_ledger сможет хранить отдельные разнонаправленные ноги
- positions сможет строиться из ledger уже с учетом hedge mode
- сопровождение в БД будет не "по одному symbol", а по symbol+side через ledger
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("traders.trade_liquidation")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _safe_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            obj = json.loads(text)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}


def _opp_side(side: str) -> str:
    side = str(side or "").upper()
    if side == "LONG":
        return "SHORT"
    if side == "SHORT":
        return "LONG"
    return ""


def _position_risk_map(rows: List[Dict[str, Any]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    result: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").upper().strip()
        position_side = str(row.get("positionSide") or "").upper().strip()
        if not symbol or position_side not in ("LONG", "SHORT"):
            continue
        result[(symbol, position_side)] = row
    return result


def _symbol_id_map(self) -> Dict[str, int]:
    result: Dict[str, int] = {}
    try:
        rows = list(
            self.store.query_dict(
                """
                SELECT symbol_id, symbol
                FROM public.symbols
                WHERE exchange_id = %(exchange_id)s
                """,
                {"exchange_id": int(self.exchange_id)},
            )
        )
        for row in rows:
            symbol = str(row.get("symbol") or "").upper().strip()
            symbol_id = int(row.get("symbol_id") or 0)
            if symbol and symbol_id > 0:
                result[symbol] = symbol_id
    except Exception:
        log.exception("[TL][orphan_ready] failed to load symbols map")
    return result



def _ensure_position_ledger_time_guard(store) -> None:
    """Install DB-level protections for closed_at/opened_at consistency."""
    trigger_ddl = """
    CREATE OR REPLACE FUNCTION public.trg_position_ledger_guard_closed_at()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
        IF NEW.opened_at IS NOT NULL AND NEW.closed_at IS NOT NULL AND NEW.closed_at < NEW.opened_at THEN
            IF NEW.updated_at IS NOT NULL AND NEW.updated_at >= NEW.opened_at THEN
                NEW.closed_at := NEW.updated_at;
            ELSE
                NEW.closed_at := NEW.opened_at;
            END IF;
        END IF;

        IF NEW.updated_at IS NOT NULL AND NEW.opened_at IS NOT NULL AND NEW.updated_at < NEW.opened_at THEN
            NEW.updated_at := NEW.opened_at;
        END IF;

        RETURN NEW;
    END;
    $$;

    DROP TRIGGER IF EXISTS trg_position_ledger_guard_closed_at ON public.position_ledger;

    CREATE TRIGGER trg_position_ledger_guard_closed_at
    BEFORE INSERT OR UPDATE ON public.position_ledger
    FOR EACH ROW
    EXECUTE FUNCTION public.trg_position_ledger_guard_closed_at();
    """
    try:
        store.exec_ddl(trigger_ddl)
    except Exception:
        log.exception("[TL][ledger_guard] failed to install trigger guard")

    try:
        store.execute(
            """
            UPDATE public.position_ledger
               SET closed_at = CASE
                   WHEN updated_at IS NOT NULL AND opened_at IS NOT NULL AND updated_at >= opened_at THEN updated_at
                   WHEN opened_at IS NOT NULL THEN opened_at
                   ELSE closed_at
               END
             WHERE opened_at IS NOT NULL
               AND closed_at IS NOT NULL
               AND closed_at < opened_at
            """
        )
    except Exception:
        log.exception("[TL][ledger_guard] failed to normalize invalid closed_at rows")

    try:
        store.exec_ddl(
            """
            ALTER TABLE public.position_ledger
            DROP CONSTRAINT IF EXISTS chk_position_ledger_closed_at_not_before_opened_at;

            ALTER TABLE public.position_ledger
            ADD CONSTRAINT chk_position_ledger_closed_at_not_before_opened_at
            CHECK (closed_at IS NULL OR opened_at IS NULL OR closed_at >= opened_at) NOT VALID;
            """
        )
    except Exception:
        pass

    try:
        store.exec_ddl(
            """
            ALTER TABLE public.position_ledger
            VALIDATE CONSTRAINT chk_position_ledger_closed_at_not_before_opened_at;
            """
        )
    except Exception:
        log.exception("[TL][ledger_guard] failed to validate closed_at constraint")


def _position_ledger_time_anomalies(store) -> Dict[str, int]:
    try:
        row = None
        if hasattr(store, "query_one_dict"):
            row = store.query_one_dict(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN opened_at IS NOT NULL AND closed_at IS NOT NULL AND closed_at < opened_at THEN 1 ELSE 0 END), 0) AS broken_closed_at,
                    COALESCE(SUM(CASE WHEN opened_at IS NOT NULL AND updated_at IS NOT NULL AND updated_at < opened_at THEN 1 ELSE 0 END), 0) AS broken_updated_at,
                    COALESCE(SUM(CASE WHEN closed_at IS NOT NULL AND updated_at IS NOT NULL AND closed_at > updated_at THEN 1 ELSE 0 END), 0) AS broken_closed_gt_updated
                FROM public.position_ledger
                """
            )
        else:
            row = store.query_one(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN opened_at IS NOT NULL AND closed_at IS NOT NULL AND closed_at < opened_at THEN 1 ELSE 0 END), 0) AS broken_closed_at,
                    COALESCE(SUM(CASE WHEN opened_at IS NOT NULL AND updated_at IS NOT NULL AND updated_at < opened_at THEN 1 ELSE 0 END), 0) AS broken_updated_at,
                    COALESCE(SUM(CASE WHEN closed_at IS NOT NULL AND updated_at IS NOT NULL AND closed_at > updated_at THEN 1 ELSE 0 END), 0) AS broken_closed_gt_updated
                FROM public.position_ledger
                """
            )

        if row is None:
            return {
                "broken_closed_at": 0,
                "broken_updated_at": 0,
                "broken_closed_gt_updated": 0,
            }

        if isinstance(row, tuple):
            broken_closed_at = int((row[0] if len(row) > 0 else 0) or 0)
            broken_updated_at = int((row[1] if len(row) > 1 else 0) or 0)
            broken_closed_gt_updated = int((row[2] if len(row) > 2 else 0) or 0)
        else:
            broken_closed_at = int(row.get("broken_closed_at") or 0)
            broken_updated_at = int(row.get("broken_updated_at") or 0)
            broken_closed_gt_updated = int(row.get("broken_closed_gt_updated") or 0)

        return {
            "broken_closed_at": broken_closed_at,
            "broken_updated_at": broken_updated_at,
            "broken_closed_gt_updated": broken_closed_gt_updated,
        }
    except Exception:
        log.exception("[TL][ledger_guard] periodic anomaly check failed")
        return {
            "broken_closed_at": 0,
            "broken_updated_at": 0,
            "broken_closed_gt_updated": 0,
        }

def _ensure_hedge_links_table(store) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS public.hedge_links (
        exchange_id SMALLINT NOT NULL,
        base_account_id SMALLINT NOT NULL,
        hedge_account_id SMALLINT NOT NULL,
        symbol_id BIGINT NOT NULL,
        base_pos_uid TEXT NOT NULL,
        hedge_pos_uid TEXT NOT NULL,
        hedge_ratio NUMERIC(18,8),
        created_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (
            exchange_id,
            base_account_id,
            hedge_account_id,
            symbol_id,
            base_pos_uid,
            hedge_pos_uid
        )
    );
    """
    try:
        store.exec_ddl(ddl)
    except Exception:
        # best effort
        pass


def _insert_live_hedge_ledger_row(
    self,
    *,
    main_row: Dict[str, Any],
    hedge_qty: float,
    hedge_entry: float,
    hedge_value_usdt: float,
    hedge_side: str,
    symbol: str,
) -> Optional[str]:
    symbol_id = int(main_row.get("symbol_id") or 0)
    if symbol_id <= 0:
        return None

    main_meta = _safe_json_dict(main_row.get("raw_meta"))
    hedge_block = main_meta.get("hedge") if isinstance(main_meta.get("hedge"), dict) else {}
    hedge_pos_uid = str(hedge_block.get("hedge_pos_uid") or "").strip()
    if not hedge_pos_uid:
        hedge_pos_uid = str(uuid.uuid4())

    opened_at = _utc_now()

    raw_meta = {
        "hedge_link": {
            "base_pos_uid": str(main_row.get("pos_uid") or ""),
            "hedge_pos_uid": hedge_pos_uid,
            "base_side": str(main_row.get("side") or "").upper(),
            "hedge_side": str(hedge_side or "").upper(),
            "symbol": str(symbol or "").upper(),
            "source": "exchange_position_risk",
            "ts": opened_at.isoformat(),
        }
    }

    columns = [
        "exchange_id",
        "account_id",
        "symbol_id",
        "pos_uid",
        "strategy_id",
        "strategy_name",
        "side",
        "status",
        "opened_at",
        "entry_price",
        "avg_price",
        "qty_opened",
        "qty_current",
        "position_value_usdt",
        "scale_in_count",
        "updated_at",
        "source",
    ]
    values = [
        "%(exchange_id)s",
        "%(account_id)s",
        "%(symbol_id)s",
        "%(pos_uid)s",
        "%(strategy_id)s",
        "%(strategy_name)s",
        "%(side)s",
        "'OPEN'",
        "%(opened_at)s",
        "%(entry_price)s",
        "%(avg_price)s",
        "%(qty_opened)s",
        "%(qty_current)s",
        "%(position_value_usdt)s",
        "0",
        "%(updated_at)s",
        "'live_hedge'",
    ]

    if getattr(self, "_pl_has_raw_meta", False):
        columns.append("raw_meta")
        values.append("%(raw_meta)s::jsonb")

    sql = f"""
    INSERT INTO public.position_ledger ({", ".join(columns)})
    VALUES ({", ".join(values)})
    ON CONFLICT DO NOTHING
    """

    params = {
        "exchange_id": int(self.exchange_id),
        "account_id": int(self.account_id),
        "symbol_id": int(symbol_id),
        "pos_uid": hedge_pos_uid,
        "strategy_id": str(getattr(self, "STRATEGY_ID", "trade_liquidation")),
        "strategy_name": str(getattr(self, "STRATEGY_ID", "trade_liquidation")),
        "side": str(hedge_side).upper(),
        "opened_at": opened_at,
        "entry_price": float(hedge_entry),
        "avg_price": float(hedge_entry),
        "qty_opened": float(hedge_qty),
        "qty_current": float(hedge_qty),
        "position_value_usdt": float(hedge_value_usdt),
        "updated_at": opened_at,
        "raw_meta": json.dumps(raw_meta, ensure_ascii=False),
    }

    self.store.execute(sql, params)

    if getattr(self, "_pl_has_raw_meta", False):
        try:
            meta_patch = {
                "hedge": {
                    "hedge_pos_uid": hedge_pos_uid,
                    "last_materialized_ts": opened_at.isoformat(),
                    "last_materialized_side": str(hedge_side).upper(),
                }
            }
            self.store.execute(
                """
                UPDATE public.position_ledger
                   SET raw_meta = COALESCE(raw_meta, '{}'::jsonb) || %(meta)s::jsonb,
                       updated_at = NOW()
                 WHERE exchange_id = %(exchange_id)s
                   AND account_id = %(account_id)s
                   AND pos_uid = %(base_pos_uid)s
                   AND status = 'OPEN'
                   AND source = 'live'
                """,
                {
                    "exchange_id": int(self.exchange_id),
                    "account_id": int(self.account_id),
                    "base_pos_uid": str(main_row.get("pos_uid") or ""),
                    "meta": json.dumps(meta_patch, ensure_ascii=False),
                },
            )
        except Exception:
            log.exception("[TL][hedge_ready] failed to enrich base raw_meta %s", symbol)

    _ensure_hedge_links_table(self.store)
    try:
        self.store.execute(
            """
            INSERT INTO public.hedge_links (
                exchange_id,
                base_account_id,
                hedge_account_id,
                symbol_id,
                base_pos_uid,
                hedge_pos_uid,
                hedge_ratio,
                created_at
            )
            VALUES (
                %(exchange_id)s,
                %(account_id)s,
                %(account_id)s,
                %(symbol_id)s,
                %(base_pos_uid)s,
                %(hedge_pos_uid)s,
                %(hedge_ratio)s,
                NOW()
            )
            ON CONFLICT DO NOTHING
            """,
            {
                "exchange_id": int(self.exchange_id),
                "account_id": int(self.account_id),
                "symbol_id": int(symbol_id),
                "base_pos_uid": str(main_row.get("pos_uid") or ""),
                "hedge_pos_uid": str(hedge_pos_uid),
                "hedge_ratio": float(hedge_qty) / max(float(main_row.get("qty_current") or 0.0), 1e-12),
            },
        )
    except Exception:
        log.exception("[TL][hedge_ready] hedge_links insert failed %s", symbol)

    log.info(
        "[TL][hedge_ready] created live_hedge row symbol=%s side=%s qty=%.8f base_pos_uid=%s hedge_pos_uid=%s",
        str(symbol).upper(),
        str(hedge_side).upper(),
        float(hedge_qty),
        str(main_row.get("pos_uid") or ""),
        str(hedge_pos_uid),
    )
    return hedge_pos_uid


def _close_live_hedge_ledger_row(
    self,
    *,
    hedge_pos_uid: str,
    symbol: str,
    hedge_side: str,
    exit_price: float,
) -> None:
    self.store.execute(
        """
        UPDATE public.position_ledger
           SET status = 'CLOSED',
               closed_at = NOW(),
               exit_price = %(exit_price)s,
               qty_closed = COALESCE(qty_closed, 0) + COALESCE(qty_current, 0),
               qty_current = 0,
               updated_at = NOW()
         WHERE exchange_id = %(exchange_id)s
           AND account_id = %(account_id)s
           AND pos_uid = %(pos_uid)s
           AND source = 'live_hedge'
           AND status = 'OPEN'
        """,
        {
            "exchange_id": int(self.exchange_id),
            "account_id": int(self.account_id),
            "pos_uid": str(hedge_pos_uid),
            "exit_price": float(exit_price or 0.0),
        },
    )
    log.info(
        "[TL][hedge_ready] closed live_hedge symbol=%s side=%s hedge_pos_uid=%s exit=%.8f",
        str(symbol).upper(),
        str(hedge_side).upper(),
        str(hedge_pos_uid),
        float(exit_price or 0.0),
    )


def _sync_live_main_positions_from_position_risk(self) -> Dict[str, int]:
    """Materialize orphan main positions that already exist on exchange.

    Recovery works from active ledger rows. If an exchange position exists but
    there is no OPEN source='live' row, TP/TRL/HEDGE recovery will skip it and
    leave the position fully unprotected. This sync creates missing live rows
    for true orphan main positions before recovery runs.
    """

    try:
        position_risk = getattr(self, "_last_position_risk", None)
        if not position_risk:
            position_risk = self._rest_snapshot_get("position_risk") or []
    except Exception:
        position_risk = []

    if not isinstance(position_risk, list) or not position_risk:
        return {"checked": 0, "opened": 0, "skipped": 0}

    symbol_id_by_symbol = _symbol_id_map(self)
    if not symbol_id_by_symbol:
        return {"checked": 0, "opened": 0, "skipped": 0}

    active_rows = list(
        self.store.query_dict(
            """
            SELECT
                symbol_id,
                symbol,
                pos_uid,
                side,
                source,
                status
            FROM public.position_ledger
            WHERE exchange_id = %(exchange_id)s
              AND account_id = %(account_id)s
              AND strategy_id = %(strategy_id)s
              AND status = 'OPEN'
              AND COALESCE(source, 'live') IN ('live', 'live_hedge')
            """,
            {
                "exchange_id": int(self.exchange_id),
                "account_id": int(self.account_id),
                "strategy_id": str(getattr(self, "STRATEGY_ID", "trade_liquidation")),
            },
        )
    )

    active_by_symbol_side: Dict[Tuple[str, str], Dict[str, Any]] = {}
    active_live_by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    for row in active_rows:
        symbol = str(row.get("symbol") or "").upper().strip()
        side = str(row.get("side") or "").upper().strip()
        source = str(row.get("source") or "live").strip().lower()
        if symbol and side in ("LONG", "SHORT"):
            active_by_symbol_side[(symbol, side)] = row
            if source == "live":
                active_live_by_symbol.setdefault(symbol, []).append(row)

    checked = 0
    opened = 0
    skipped = 0

    for p in position_risk:
        symbol = str((p or {}).get("symbol") or "").upper().strip()
        side = str((p or {}).get("positionSide") or "").upper().strip()
        qty = abs(_safe_float((p or {}).get("positionAmt"), 0.0))

        if not symbol or side not in ("LONG", "SHORT") or qty <= 0:
            continue

        checked += 1

        if (symbol, side) in active_by_symbol_side:
            continue

        symbol_id = int(symbol_id_by_symbol.get(symbol) or 0)
        if symbol_id <= 0:
            skipped += 1
            continue

        # If the opposite side already exists as an active main row, then the
        # current exchange leg is most likely a hedge leg and should be handled
        # by hedge sync, not materialized as another main row.
        opp_live = active_live_by_symbol.get(symbol, [])
        if any(str(r.get("side") or "").upper().strip() == _opp_side(side) for r in opp_live):
            skipped += 1
            continue

        opened_at = _utc_now()
        entry_price = _safe_float((p or {}).get("entryPrice"), 0.0)
        mark_price = _safe_float((p or {}).get("markPrice"), 0.0)
        notional = abs(_safe_float((p or {}).get("notional"), 0.0))
        pos_uid = f"ORPHAN-{uuid.uuid4()}"

        raw_meta = {
            "live_orphan": {
                "source": "exchange_position_risk",
                "ts": opened_at.isoformat(),
                "symbol": symbol,
                "side": side,
                "position_side": side,
                "position_amt": qty,
                "entry_price": entry_price,
                "mark_price": mark_price,
            }
        }

        columns = [
            "exchange_id",
            "account_id",
            "symbol_id",
            "symbol",
            "pos_uid",
            "strategy_id",
            "strategy_name",
            "side",
            "status",
            "opened_at",
            "entry_price",
            "avg_price",
            "qty_opened",
            "qty_current",
            "position_value_usdt",
            "scale_in_count",
            "updated_at",
            "source",
        ]
        values = [
            "%(exchange_id)s",
            "%(account_id)s",
            "%(symbol_id)s",
            "%(symbol)s",
            "%(pos_uid)s",
            "%(strategy_id)s",
            "%(strategy_name)s",
            "%(side)s",
            "'OPEN'",
            "%(opened_at)s",
            "%(entry_price)s",
            "%(avg_price)s",
            "%(qty_opened)s",
            "%(qty_current)s",
            "%(position_value_usdt)s",
            "0",
            "%(updated_at)s",
            "'live'",
        ]

        if getattr(self, "_pl_has_raw_meta", False):
            columns.append("raw_meta")
            values.append("%(raw_meta)s::jsonb")

        sql = f"""
        INSERT INTO public.position_ledger ({", ".join(columns)})
        VALUES ({", ".join(values)})
        ON CONFLICT DO NOTHING
        """

        params = {
            "exchange_id": int(self.exchange_id),
            "account_id": int(self.account_id),
            "symbol_id": int(symbol_id),
            "symbol": symbol,
            "pos_uid": pos_uid,
            "strategy_id": str(getattr(self, "STRATEGY_ID", "trade_liquidation")),
            "strategy_name": str(getattr(self, "STRATEGY_ID", "trade_liquidation")),
            "side": side,
            "opened_at": opened_at,
            "entry_price": float(entry_price or mark_price or 0.0),
            "avg_price": float(entry_price or mark_price or 0.0),
            "qty_opened": float(qty),
            "qty_current": float(qty),
            "position_value_usdt": float(notional or (mark_price * qty)),
            "updated_at": opened_at,
            "raw_meta": json.dumps(raw_meta, ensure_ascii=False),
        }

        try:
            self.store.execute(sql, params)
            active_by_symbol_side[(symbol, side)] = {"symbol": symbol, "side": side, "source": "live", "pos_uid": pos_uid}
            active_live_by_symbol.setdefault(symbol, []).append({"symbol": symbol, "side": side, "source": "live", "pos_uid": pos_uid})
            opened += 1
            log.warning(
                "[TL][orphan_ready] materialized missing live position symbol=%s side=%s qty=%.8f entry=%.8f pos_uid=%s",
                symbol,
                side,
                float(qty),
                float(entry_price or mark_price or 0.0),
                pos_uid,
            )
        except Exception:
            skipped += 1
            log.exception(
                "[TL][orphan_ready] failed to materialize missing live position symbol=%s side=%s",
                symbol,
                side,
            )

    return {"checked": checked, "opened": opened, "skipped": skipped}


def _sync_live_hedge_legs_from_position_risk(self) -> Dict[str, int]:
    if not bool(getattr(self.p, "hedge_enabled", False)):
        return {"checked": 0, "opened": 0, "closed": 0}

    try:
        position_risk = getattr(self, "_last_position_risk", None)
        if not position_risk:
            position_risk = self._rest_snapshot_get("position_risk") or []
    except Exception:
        position_risk = []

    if not isinstance(position_risk, list) or not position_risk:
        return {"checked": 0, "opened": 0, "closed": 0}

    risk_map = _position_risk_map(position_risk)
    symbols_map = self._symbols_map()

    main_rows = list(
        self.store.query_dict(
            """
            SELECT
                exchange_id,
                account_id,
                symbol_id,
                symbol,
                pos_uid,
                side,
                qty_current,
                avg_price,
                entry_price,
                raw_meta
            FROM public.position_ledger
            WHERE exchange_id = %(exchange_id)s
              AND account_id = %(account_id)s
              AND strategy_id = %(strategy_id)s
              AND source = 'live'
              AND status = 'OPEN'
            ORDER BY opened_at DESC
            """,
            {
                "exchange_id": int(self.exchange_id),
                "account_id": int(self.account_id),
                "strategy_id": str(getattr(self, "STRATEGY_ID", "trade_liquidation")),
            },
        )
    )

    hedge_rows = list(
        self.store.query_dict(
            """
            SELECT
                exchange_id,
                account_id,
                symbol_id,
                symbol,
                pos_uid,
                side,
                qty_current,
                entry_price,
                avg_price,
                raw_meta
            FROM public.position_ledger
            WHERE exchange_id = %(exchange_id)s
              AND account_id = %(account_id)s
              AND strategy_id = %(strategy_id)s
              AND source = 'live_hedge'
              AND status = 'OPEN'
            ORDER BY opened_at DESC
            """,
            {
                "exchange_id": int(self.exchange_id),
                "account_id": int(self.account_id),
                "strategy_id": str(getattr(self, "STRATEGY_ID", "trade_liquidation")),
            },
        )
    )

    hedge_by_base: Dict[Tuple[int, str, str], Dict[str, Any]] = {}
    for row in hedge_rows:
        meta = _safe_json_dict(row.get("raw_meta"))
        link = meta.get("hedge_link") if isinstance(meta.get("hedge_link"), dict) else {}
        base_pos_uid = str(link.get("base_pos_uid") or "").strip()
        symbol_id = int(row.get("symbol_id") or 0)
        side = str(row.get("side") or "").upper()
        if base_pos_uid and symbol_id > 0 and side:
            hedge_by_base[(symbol_id, base_pos_uid, side)] = row

    checked = 0
    opened = 0
    closed = 0

    for main_row in main_rows:
        checked += 1

        symbol_id = int(main_row.get("symbol_id") or 0)
        if symbol_id <= 0:
            continue

        symbol = str(main_row.get("symbol") or symbols_map.get(symbol_id) or "").upper().strip()
        if not symbol:
            continue

        main_side = str(main_row.get("side") or "").upper().strip()
        hedge_side = _opp_side(main_side)
        if hedge_side not in ("LONG", "SHORT"):
            continue

        risk_row = risk_map.get((symbol, hedge_side))
        hedge_amt = abs(_safe_float((risk_row or {}).get("positionAmt"), 0.0))
        hedge_entry = _safe_float((risk_row or {}).get("entryPrice"), 0.0)
        hedge_mark = _safe_float((risk_row or {}).get("markPrice"), 0.0)
        hedge_notional = abs(_safe_float((risk_row or {}).get("notional"), 0.0))

        key = (symbol_id, str(main_row.get("pos_uid") or ""), hedge_side)
        existing = hedge_by_base.get(key)

        if hedge_amt > 0:
            if existing:
                self.store.execute(
                    """
                    UPDATE public.position_ledger
                       SET qty_current = %(qty_current)s,
                           position_value_usdt = %(position_value_usdt)s,
                           entry_price = CASE WHEN COALESCE(entry_price, 0) <= 0 THEN %(entry_price)s ELSE entry_price END,
                           avg_price = CASE WHEN COALESCE(avg_price, 0) <= 0 THEN %(avg_price)s ELSE avg_price END,
                           updated_at = NOW()
                     WHERE exchange_id = %(exchange_id)s
                       AND account_id = %(account_id)s
                       AND pos_uid = %(pos_uid)s
                       AND source = 'live_hedge'
                       AND status = 'OPEN'
                    """,
                    {
                        "exchange_id": int(self.exchange_id),
                        "account_id": int(self.account_id),
                        "pos_uid": str(existing.get("pos_uid") or ""),
                        "qty_current": float(hedge_amt),
                        "position_value_usdt": float(hedge_notional or (hedge_mark * hedge_amt)),
                        "entry_price": float(hedge_entry or hedge_mark or 0.0),
                        "avg_price": float(hedge_entry or hedge_mark or 0.0),
                    },
                )
            else:
                new_uid = _insert_live_hedge_ledger_row(
                    self,
                    main_row=main_row,
                    hedge_qty=float(hedge_amt),
                    hedge_entry=float(hedge_entry or hedge_mark or 0.0),
                    hedge_value_usdt=float(hedge_notional or (hedge_mark * hedge_amt)),
                    hedge_side=hedge_side,
                    symbol=symbol,
                )
                if new_uid:
                    opened += 1
        else:
            if existing:
                _close_live_hedge_ledger_row(
                    self,
                    hedge_pos_uid=str(existing.get("pos_uid") or ""),
                    symbol=symbol,
                    hedge_side=hedge_side,
                    exit_price=float(hedge_mark or 0.0),
                )
                closed += 1

    live_main_keys = {
        (int(row.get("symbol_id") or 0), str(row.get("pos_uid") or ""))
        for row in main_rows
    }

    for row in hedge_rows:
        meta = _safe_json_dict(row.get("raw_meta"))
        link = meta.get("hedge_link") if isinstance(meta.get("hedge_link"), dict) else {}
        base_pos_uid = str(link.get("base_pos_uid") or "").strip()
        symbol_id = int(row.get("symbol_id") or 0)
        if symbol_id > 0 and base_pos_uid and (symbol_id, base_pos_uid) not in live_main_keys:
            _close_live_hedge_ledger_row(
                self,
                hedge_pos_uid=str(row.get("pos_uid") or ""),
                symbol=str(row.get("symbol") or symbols_map.get(symbol_id) or ""),
                hedge_side=str(row.get("side") or ""),
                exit_price=0.0,
            )
            closed += 1

    return {"checked": checked, "opened": opened, "closed": closed}




def _enforce_add_or_hedge_invariant(self) -> Dict[str, int]:
    """Ensure each one-sided live main position has either next ADD or HEDGE stage.

    Strategy intent required by the project:
      - if averaging capacity remains -> pending ADDn must exist
      - if averaging cap is exhausted -> pending HEDGE opener must exist
      - if symbol already has both LONG and SHORT live positions, hedge is already opened
        and no new HEDGE entry should be created here.

    We delegate actual placement/deduplication to the original
    ``_live_ensure_next_add_order`` method from ``trade_liquidation.py`` because it
    already knows how to:
      - refresh ``adds_done`` from DB truth,
      - place the next ADD,
      - or, once ``max_adds`` is reached, place ``_HEDGE`` and after-last-add trailing.
    """
    if not getattr(self, '_is_live', False):
        return {'checked': 0, 'acted': 0, 'skipped_two_sided': 0}
    if not bool(getattr(getattr(self, 'p', object()), 'averaging_enabled', False)):
        return {'checked': 0, 'acted': 0, 'skipped_two_sided': 0}
    if int(getattr(self, '_cfg_max_adds')() or 0) <= 0:
        return {'checked': 0, 'acted': 0, 'skipped_two_sided': 0}

    rows = list(
        self.store.query_dict(
            """
            SELECT
                symbol_id,
                symbol,
                pos_uid,
                side,
                qty_current,
                avg_price,
                entry_price,
                scale_in_count,
                raw_meta,
                source,
                opened_at,
                updated_at
            FROM public.position_ledger
            WHERE exchange_id = %(exchange_id)s
              AND account_id = %(account_id)s
              AND strategy_id = %(strategy_id)s
              AND source = 'live'
              AND status = 'OPEN'
            ORDER BY updated_at DESC
            """,
            {
                'exchange_id': int(self.exchange_id),
                'account_id': int(self.account_id),
                'strategy_id': str(getattr(self, 'STRATEGY_ID', 'trade_liquidation')),
            },
        )
    )

    by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        sym = str(row.get('symbol') or '').upper().strip()
        side = str(row.get('side') or '').upper().strip()
        if not sym or side not in ('LONG', 'SHORT'):
            continue
        by_symbol.setdefault(sym, []).append(row)

    checked = 0
    acted = 0
    skipped_two_sided = 0

    for sym, sym_rows in by_symbol.items():
        live_sides = {str(r.get('side') or '').upper().strip() for r in sym_rows}
        if 'LONG' in live_sides and 'SHORT' in live_sides:
            skipped_two_sided += len(sym_rows)
            continue
        # One-sided symbol: enforce invariant on the freshest live row only.
        row = sym_rows[0]
        checked += 1
        try:
            if bool(self._live_ensure_next_add_order(pos=row)):
                acted += 1
        except Exception:
            log.exception('[TL][add_or_hedge] failed invariant for %s %s pos_uid=%s', sym, str(row.get('side') or ''), str(row.get('pos_uid') or ''))

    return {'checked': checked, 'acted': acted, 'skipped_two_sided': skipped_two_sided}

def apply_trade_liquidation_full_ready() -> None:
    from src.platform.traders.trade_liquidation import TradeLiquidation

    TradeLiquidation._sync_live_main_positions_from_position_risk = _sync_live_main_positions_from_position_risk  # type: ignore[attr-defined]
    TradeLiquidation._position_ledger_time_anomalies = staticmethod(_position_ledger_time_anomalies)  # type: ignore[attr-defined]
    TradeLiquidation._sync_live_hedge_legs_from_position_risk = _sync_live_hedge_legs_from_position_risk  # type: ignore[attr-defined]
    TradeLiquidation._insert_live_hedge_ledger_row = _insert_live_hedge_ledger_row  # type: ignore[attr-defined]
    TradeLiquidation._close_live_hedge_ledger_row = _close_live_hedge_ledger_row  # type: ignore[attr-defined]

    original_auto_recovery_brackets = TradeLiquidation._auto_recovery_brackets

    def _wrapped_auto_recovery_brackets(self):
        """Pre-sync exchange reality, then run recovery, then enforce ADD-or-HEDGE invariant."""
        try:
            if self._is_live:
                _ensure_position_ledger_time_guard(self.store)
                anomalies_before = _position_ledger_time_anomalies(self.store)
                main_sync_result = self._sync_live_main_positions_from_position_risk()
                if bool(getattr(self.p, "hedge_enabled", False)):
                    hedge_sync_result = self._sync_live_hedge_legs_from_position_risk()
                else:
                    hedge_sync_result = {"checked": 0, "opened": 0, "closed": 0}
                try:
                    self._sync_positions_tables_from_position_risk()
                except Exception:
                    log.exception("[TL][hedge_ready] positions sync failed before recovery")
                try:
                    self._last_orphan_main_sync = dict(main_sync_result)
                    self._last_orphan_hedge_sync = dict(hedge_sync_result)
                    self._last_position_ledger_time_anomalies = dict(anomalies_before)
                except Exception:
                    pass
                if any(int(anomalies_before.get(k, 0)) > 0 for k in ("broken_closed_at", "broken_updated_at", "broken_closed_gt_updated")):
                    log.warning("[TL][ledger_guard] anomalies before recovery closed_lt_open=%s updated_lt_open=%s closed_gt_updated=%s", int(anomalies_before.get("broken_closed_at", 0)), int(anomalies_before.get("broken_updated_at", 0)), int(anomalies_before.get("broken_closed_gt_updated", 0)))
        except Exception:
            log.exception("[TL][hedge_ready] pre-recovery hedge sync failed")
        result = original_auto_recovery_brackets(self)
        try:
            if self._is_live and bool(getattr(getattr(self, 'p', object()), 'add_or_hedge_invariant_enabled', True)):
                inv = _enforce_add_or_hedge_invariant(self)
                try:
                    self._last_add_or_hedge_invariant = dict(inv)
                except Exception:
                    pass
        except Exception:
            log.exception('[TL][add_or_hedge] post-recovery invariant failed')
        return result

    TradeLiquidation._auto_recovery_brackets = _wrapped_auto_recovery_brackets  # type: ignore[assignment]

    original_process_open_positions = TradeLiquidation._process_open_positions

    def _wrapped_process_open_positions(self):
        result = original_process_open_positions(self)
        try:
            if self._is_live:
                _ensure_position_ledger_time_guard(self.store)
                anomalies_before = _position_ledger_time_anomalies(self.store)
                main_sync_result = self._sync_live_main_positions_from_position_risk()
                if bool(getattr(self.p, "hedge_enabled", False)):
                    hedge_sync_result = self._sync_live_hedge_legs_from_position_risk()
                else:
                    hedge_sync_result = {"checked": 0, "opened": 0, "closed": 0}
                try:
                    self._sync_positions_tables_from_position_risk()
                except Exception:
                    log.exception("[TL][hedge_ready] positions sync failed after hedge sync")
                inv = {'checked': 0, 'acted': 0, 'skipped_two_sided': 0}
                try:
                    if bool(getattr(getattr(self, 'p', object()), 'add_or_hedge_invariant_enabled', True)):
                        inv = _enforce_add_or_hedge_invariant(self)
                except Exception:
                    log.exception('[TL][add_or_hedge] post-open_positions invariant failed')
                if any(int(anomalies_before.get(k, 0)) > 0 for k in ("broken_closed_at", "broken_updated_at", "broken_closed_gt_updated")):
                    log.warning("[TL][ledger_guard] anomalies before open_positions closed_lt_open=%s updated_lt_open=%s closed_gt_updated=%s", int(anomalies_before.get("broken_closed_at", 0)), int(anomalies_before.get("broken_updated_at", 0)), int(anomalies_before.get("broken_closed_gt_updated", 0)))
                if isinstance(result, dict):
                    result["orphan_main_sync_checked"] = int(main_sync_result.get("checked", 0))
                    result["orphan_main_sync_opened"] = int(main_sync_result.get("opened", 0))
                    result["orphan_main_sync_skipped"] = int(main_sync_result.get("skipped", 0))
                    result["hedge_sync_checked"] = int(hedge_sync_result.get("checked", 0))
                    result["hedge_sync_opened"] = int(hedge_sync_result.get("opened", 0))
                    result["hedge_sync_closed"] = int(hedge_sync_result.get("closed", 0))
                    result["ledger_broken_closed_at"] = int(anomalies_before.get("broken_closed_at", 0))
                    result["ledger_broken_updated_at"] = int(anomalies_before.get("broken_updated_at", 0))
                    result["ledger_broken_closed_gt_updated"] = int(anomalies_before.get("broken_closed_gt_updated", 0))
                    result["add_or_hedge_checked"] = int(inv.get('checked', 0))
                    result["add_or_hedge_acted"] = int(inv.get('acted', 0))
                    result["add_or_hedge_skipped_two_sided"] = int(inv.get('skipped_two_sided', 0))
        except Exception:
            log.exception("[TL][hedge_ready] wrapped process_open_positions failed")
        return result

    TradeLiquidation._process_open_positions = _wrapped_process_open_positions  # type: ignore[assignment]

    log.info("[TL][hedge_ready] full ready module connected")


apply_trade_liquidation_full_ready()
