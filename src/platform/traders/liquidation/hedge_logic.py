from __future__ import annotations

import json
import logging
import math
import os
import random
import time
import threading
import uuid
import hmac
import hashlib
import urllib.parse
import re
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR, ROUND_HALF_UP, ROUND_UP, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

import requests

from src.platform.data.storage.postgres.storage import PostgreSQLStorage

from .params import *

log = logging.getLogger("traders.trade_liquidation")

class TradeLiquidationHedgeLogicMixin:

    def _promote_surviving_hedge_to_main_safe(self, *, hedge_pos: dict, symbol: str, symbol_id: int, pr_map: dict, open_live_symbol_keys: set | None = None) -> bool:
        """Safely convert a surviving OPEN live_hedge leg into a new LIVE main row.

        Flow:
          1) confirm original main leg is gone on exchange and there is no OPEN LIVE row for the symbol,
          2) cancel hedge-specific protection orders (HEDGE_TRL / HEDGE_SL),
          3) create a brand new OPEN source='live' ledger row using the surviving hedge exchange qty/entry,
          4) close the old mirror row so it no longer participates in hedge-specific flows.
        """
        if not bool(getattr(self.p, 'hedge_promote_survivor_to_main_enabled', True)):
            return False
        try:
            pos_uid_old = str((hedge_pos or {}).get('pos_uid') or '').strip()
            hedge_side = str((hedge_pos or {}).get('side') or '').upper().strip()
            if not pos_uid_old or hedge_side not in {'LONG', 'SHORT'}:
                return False
            hedge_exchange = pr_map.get((symbol, hedge_side)) or {}
            try:
                hedge_qty = abs(float(hedge_exchange.get('positionAmt') or 0.0))
            except Exception:
                hedge_qty = 0.0
            if hedge_qty <= 0.0:
                return False
            main_side_old = 'SHORT' if hedge_side == 'LONG' else 'LONG'
            main_exchange = pr_map.get((symbol, main_side_old)) or {}
            try:
                main_qty = abs(float(main_exchange.get('positionAmt') or 0.0))
            except Exception:
                main_qty = 0.0
            if main_qty > 0.0:
                return False

            symbol_key = str(symbol).upper().strip()
            open_live_symbol_keys = open_live_symbol_keys or set()
            if (int(symbol_id), symbol_key) in open_live_symbol_keys:
                return False
            existing_live = None
            try:
                existing_live = self.store.fetch_one(
                    """
                    SELECT pos_uid, side
                      FROM position_ledger
                     WHERE exchange_id=%s AND account_id=%s AND strategy_id=%s
                       AND symbol_id=%s AND status='OPEN' AND COALESCE(source,'live')='live'
                     ORDER BY updated_at DESC NULLS LAST, opened_at DESC NULLS LAST
                     LIMIT 1
                    """,
                    (int(self.exchange_id), int(self.account_id), str(self.STRATEGY_ID), int(symbol_id)),
                )
            except Exception:
                existing_live = None
            if existing_live:
                return False

            raw_meta_old = (hedge_pos or {}).get('raw_meta') or {}
            if isinstance(raw_meta_old, str):
                try:
                    raw_meta_old = json.loads(raw_meta_old) if raw_meta_old.strip() else {}
                except Exception:
                    raw_meta_old = {}
            if not isinstance(raw_meta_old, dict):
                raw_meta_old = {}

            entry = 0.0
            for candidate in (
                hedge_exchange.get('entryPrice'),
                hedge_pos.get('avg_price'),
                hedge_pos.get('entry_price'),
                self._get_mark_price(symbol, hedge_side),
            ):
                try:
                    entry = float(candidate or 0.0)
                except Exception:
                    entry = 0.0
                if entry > 0.0:
                    break
            if entry <= 0.0:
                return False

            try:
                tok_old = _coid_token(pos_uid_old, n=20)
                self._cancel_algo_by_client_id_safe(symbol, f'TL_{tok_old}_HEDGE_TRL')
                self._cancel_algo_by_client_id_safe(symbol, f'TL_{tok_old}_HEDGE_SL')
            except Exception:
                log.debug('[TL][HEDGE_PROMOTE] failed to cancel hedge protection for %s %s pos_uid=%s', symbol, hedge_side, pos_uid_old, exc_info=True)

            promoted_pos_uid = str(uuid.uuid4())
            promotion_meta = {
                'promoted_from_hedge': {
                    'enabled': True,
                    'old_live_hedge_pos_uid': pos_uid_old,
                    'symbol': symbol_key,
                    'side': hedge_side,
                    'qty': float(hedge_qty),
                    'entry_price': float(entry),
                    'promoted_ts': float(time.time()),
                }
            }
            merged_meta = dict(raw_meta_old)
            merged_meta.update(promotion_meta)
            val = float(entry * hedge_qty)

            if getattr(self, '_pl_has_raw_meta', False):
                self.store.execute(
                    """
                    INSERT INTO position_ledger (
                        exchange_id, account_id, symbol_id, pos_uid,
                        strategy_id, source, side, status, opened_at,
                        qty_opened, qty_current, entry_price, avg_price,
                        position_value_usdt, scale_in_count, raw_meta, updated_at
                    ) VALUES (
                        %(ex)s, %(acc)s, %(sym)s, %(pos_uid)s,
                        %(sid)s, 'live', %(side)s, 'OPEN', now(),
                        %(qty)s, %(qty)s, %(entry)s, %(entry)s,
                        %(val)s, 0, %(meta)s::jsonb, now()
                    )
                    """,
                    {
                        'ex': int(self.exchange_id), 'acc': int(self.account_id), 'sym': int(symbol_id),
                        'pos_uid': promoted_pos_uid, 'sid': str(self.STRATEGY_ID), 'side': hedge_side,
                        'qty': float(hedge_qty), 'entry': float(entry), 'val': float(val), 'meta': json.dumps(merged_meta),
                    },
                )
                self.store.execute(
                    """
                    UPDATE position_ledger
                       SET status='CLOSED',
                           closed_at=now(),
                           qty_closed=CASE WHEN COALESCE(qty_closed,0) > 0 THEN qty_closed ELSE qty_current END,
                           qty_current=0,
                           updated_at=now(),
                           raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb
                     WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s AND pos_uid=%(pos_uid)s
                    """,
                    {
                        'ex': int(self.exchange_id), 'acc': int(self.account_id), 'sym': int(symbol_id), 'pos_uid': pos_uid_old,
                        'meta': json.dumps({'promotion_close': {'reason': 'surviving_hedge_promoted_to_live_main', 'new_pos_uid': promoted_pos_uid, 'closed_ts': float(time.time())}}),
                    },
                )
            else:
                self.store.execute(
                    """
                    INSERT INTO position_ledger (
                        exchange_id, account_id, symbol_id, pos_uid,
                        strategy_id, source, side, status, opened_at,
                        qty_opened, qty_current, entry_price, avg_price,
                        position_value_usdt, scale_in_count, updated_at
                    ) VALUES (
                        %(ex)s, %(acc)s, %(sym)s, %(pos_uid)s,
                        %(sid)s, 'live', %(side)s, 'OPEN', now(),
                        %(qty)s, %(qty)s, %(entry)s, %(entry)s,
                        %(val)s, 0, now()
                    )
                    """,
                    {
                        'ex': int(self.exchange_id), 'acc': int(self.account_id), 'sym': int(symbol_id),
                        'pos_uid': promoted_pos_uid, 'sid': str(self.STRATEGY_ID), 'side': hedge_side,
                        'qty': float(hedge_qty), 'entry': float(entry), 'val': float(val),
                    },
                )
                self.store.execute(
                    """
                    UPDATE position_ledger
                       SET status='CLOSED',
                           closed_at=now(),
                           qty_closed=CASE WHEN COALESCE(qty_closed,0) > 0 THEN qty_closed ELSE qty_current END,
                           qty_current=0,
                           updated_at=now()
                     WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND symbol_id=%(sym)s AND pos_uid=%(pos_uid)s
                    """,
                    {'ex': int(self.exchange_id), 'acc': int(self.account_id), 'sym': int(symbol_id), 'pos_uid': pos_uid_old},
                )

            open_live_symbol_keys.add((int(symbol_id), symbol_key))
            log.info(
                '[TL][HEDGE_PROMOTE] promoted surviving hedge to LIVE main %s %s old_pos_uid=%s new_pos_uid=%s qty=%.8f entry=%.8f',
                symbol_key,
                hedge_side,
                pos_uid_old,
                promoted_pos_uid,
                float(hedge_qty),
                float(entry),
            )
            return True
        except Exception:
            log.exception('[TL][HEDGE_PROMOTE] failed for %s %s pos_uid=%s', symbol, str((hedge_pos or {}).get('side') or ''), str((hedge_pos or {}).get('pos_uid') or ''))
            return False

    def _ensure_live_hedge_binding(self, *, base_pos: dict, symbol: str, symbol_id: int, hedge_ps: str, hedge_amt: float, hedge_entry: float, mark: float) -> str | None:
        """Ensure there is an OPEN live_hedge mirror row and hedge_links binding for an already-open hedge leg.

        Returns hedge_pos_uid when binding exists/was created, else None.
        """
        try:
            base_pos_uid = str((base_pos or {}).get('pos_uid') or '').strip()
            if not base_pos_uid:
                return None
            ex = int(self.exchange_id)
            acc = int(self.account_id)
            sym_id = int(symbol_id)
            hedge_side = str(hedge_ps).upper()
            qty = abs(float(hedge_amt or 0.0))
            if qty <= 0.0:
                return None
            entry_px = float(hedge_entry or 0.0)
            if entry_px <= 0.0:
                entry_px = float(mark or 0.0)
            if entry_px <= 0.0:
                return None

            # Prefer an already linked OPEN live_hedge row for this base position.
            row = None
            try:
                row = self.store.fetch_one(
                    """
                    SELECT pl.pos_uid
                    FROM position_ledger pl
                    JOIN hedge_links hl
                      ON hl.exchange_id = pl.exchange_id
                     AND hl.symbol_id = pl.symbol_id
                     AND hl.hedge_account_id = pl.account_id
                     AND hl.hedge_pos_uid = pl.pos_uid
                    WHERE pl.exchange_id=%s AND pl.account_id=%s AND pl.symbol_id=%s
                      AND pl.side=%s AND pl.status='OPEN'
                      AND COALESCE(pl.source,'live')='live_hedge'
                      AND hl.base_account_id=%s AND hl.base_pos_uid=%s
                    ORDER BY pl.updated_at DESC NULLS LAST, pl.opened_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (ex, acc, sym_id, hedge_side, acc, base_pos_uid),
                )
            except Exception:
                row = None

            # Fallback: reuse any OPEN live_hedge row for same symbol/side/account.
            if not row:
                try:
                    row = self.store.fetch_one(
                        """
                        SELECT pos_uid
                        FROM position_ledger
                        WHERE exchange_id=%s AND account_id=%s AND symbol_id=%s
                          AND side=%s AND status='OPEN'
                          AND COALESCE(source,'live')='live_hedge'
                        ORDER BY updated_at DESC NULLS LAST, opened_at DESC NULLS LAST
                        LIMIT 1
                        """,
                        (ex, acc, sym_id, hedge_side),
                    )
                except Exception:
                    row = None

            hedge_pos_uid = str((row or {}).get('pos_uid') or '').strip()
            created = False
            if not hedge_pos_uid:
                hedge_pos_uid = str(uuid.uuid4())
                created = True
                if getattr(self, '_pl_has_raw_meta', False):
                    meta = json.dumps({
                        'hedge_mirror': {
                            'base_pos_uid': base_pos_uid,
                            'symbol': str(symbol).upper(),
                            'hedge_side': hedge_side,
                            'adopted_from_exchange': True,
                            'created_ts': float(time.time()),
                        }
                    })
                    self.store.execute(
                        """
                        INSERT INTO position_ledger (
                            exchange_id, account_id, symbol_id, pos_uid,
                            strategy_id, source, side, status, opened_at,
                            qty_opened, qty_current, entry_price, avg_price,
                            position_value_usdt, scale_in_count, raw_meta, updated_at
                        ) VALUES (
                            %(ex)s, %(acc)s, %(sym)s, %(pos_uid)s,
                            %(sid)s, 'live_hedge', %(side)s, 'OPEN', now(),
                            %(qty)s, %(qty)s, %(entry)s, %(entry)s,
                            %(val)s, 0, %(meta)s::jsonb, now()
                        )
                        """,
                        {
                            'ex': ex, 'acc': acc, 'sym': sym_id, 'pos_uid': hedge_pos_uid,
                            'sid': getattr(self, 'STRATEGY_ID', 'trade_liquidation'), 'side': hedge_side,
                            'qty': float(qty), 'entry': float(entry_px), 'val': float(entry_px * qty), 'meta': meta,
                        },
                    )
                else:
                    self.store.execute(
                        """
                        INSERT INTO position_ledger (
                            exchange_id, account_id, symbol_id, pos_uid,
                            strategy_id, source, side, status, opened_at,
                            qty_opened, qty_current, entry_price, avg_price,
                            position_value_usdt, scale_in_count, updated_at
                        ) VALUES (
                            %(ex)s, %(acc)s, %(sym)s, %(pos_uid)s,
                            %(sid)s, 'live_hedge', %(side)s, 'OPEN', now(),
                            %(qty)s, %(qty)s, %(entry)s, %(entry)s,
                            %(val)s, 0, now()
                        )
                        """,
                        {
                            'ex': ex, 'acc': acc, 'sym': sym_id, 'pos_uid': hedge_pos_uid,
                            'sid': getattr(self, 'STRATEGY_ID', 'trade_liquidation'), 'side': hedge_side,
                            'qty': float(qty), 'entry': float(entry_px), 'val': float(entry_px * qty),
                        },
                    )
            else:
                # Keep mirror row in sync with current exchange hedge qty/entry.
                if getattr(self, '_pl_has_raw_meta', False):
                    meta = json.dumps({
                        'hedge_mirror': {
                            'base_pos_uid': base_pos_uid,
                            'symbol': str(symbol).upper(),
                            'hedge_side': hedge_side,
                            'adopted_from_exchange': True,
                            'updated_ts': float(time.time()),
                        }
                    })
                    self.store.execute(
                        """
                        UPDATE position_ledger
                           SET qty_current=%(qty)s,
                               qty_opened=GREATEST(COALESCE(qty_opened,0), %(qty)s),
                               entry_price=CASE WHEN COALESCE(entry_price,0) <= 0 THEN %(entry)s ELSE entry_price END,
                               avg_price=CASE WHEN COALESCE(avg_price,0) <= 0 THEN %(entry)s ELSE avg_price END,
                               position_value_usdt=%(val)s,
                               raw_meta=COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb,
                               updated_at=now(),
                               status='OPEN'
                         WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s
                        """,
                        {'ex': ex, 'acc': acc, 'pos_uid': hedge_pos_uid, 'qty': float(qty), 'entry': float(entry_px), 'val': float(entry_px * qty), 'meta': meta},
                    )
                else:
                    self.store.execute(
                        """
                        UPDATE position_ledger
                           SET qty_current=%(qty)s,
                               qty_opened=GREATEST(COALESCE(qty_opened,0), %(qty)s),
                               entry_price=CASE WHEN COALESCE(entry_price,0) <= 0 THEN %(entry)s ELSE entry_price END,
                               avg_price=CASE WHEN COALESCE(avg_price,0) <= 0 THEN %(entry)s ELSE avg_price END,
                               position_value_usdt=%(val)s,
                               updated_at=now(),
                               status='OPEN'
                         WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s
                        """,
                        {'ex': ex, 'acc': acc, 'pos_uid': hedge_pos_uid, 'qty': float(qty), 'entry': float(entry_px), 'val': float(entry_px * qty)},
                    )

            # Ensure hedge_links row exists for current base -> hedge mapping.
            try:
                self.store.execute(
                    """
                    DELETE FROM hedge_links
                    WHERE exchange_id=%(ex)s AND symbol_id=%(sym)s
                      AND base_account_id=%(acc)s AND hedge_account_id=%(acc)s
                      AND base_pos_uid=%(base)s AND hedge_pos_uid=%(hedge)s
                    """,
                    {'ex': ex, 'sym': sym_id, 'acc': acc, 'base': base_pos_uid, 'hedge': hedge_pos_uid},
                )
            except Exception:
                pass
            self.store.execute(
                """
                INSERT INTO hedge_links (
                    exchange_id, base_account_id, hedge_account_id, symbol_id,
                    base_pos_uid, hedge_pos_uid, hedge_ratio, created_at
                ) VALUES (
                    %(ex)s, %(acc)s, %(acc)s, %(sym)s,
                    %(base)s, %(hedge)s, %(ratio)s, now()
                )
                """,
                {
                    'ex': ex, 'acc': acc, 'sym': sym_id, 'base': base_pos_uid, 'hedge': hedge_pos_uid,
                    'ratio': float(qty / max(abs(float((base_pos or {}).get('qty_current') or (base_pos or {}).get('qty_opened') or qty)), 1e-12)),
                },
            )
            try:
                if bool(created):
                    self._ilog('[TL][HEDGE_BIND] %s %s base_pos_uid=%s hedge_pos_uid=%s created=%s qty=%.8f entry=%.8f', str(symbol).upper(), hedge_side, base_pos_uid, hedge_pos_uid, bool(created), float(qty), float(entry_px))
                else:
                    self._rate_limited_log(logging.DEBUG, 'hedge_binding', f'hedge_binding:{str(symbol).upper()}:{str(hedge_side).upper()}', '[TL][HEDGE_BIND] %s %s base_pos_uid=%s hedge_pos_uid=%s created=%s qty=%.8f entry=%.8f', str(symbol).upper(), hedge_side, base_pos_uid, hedge_pos_uid, bool(created), float(qty), float(entry_px))
            except Exception:
                pass
            return hedge_pos_uid
        except Exception:
            log.exception('[TL][HEDGE_BIND] failed to ensure binding %s %s base_pos_uid=%s', str(symbol).upper(), str(hedge_ps).upper(), str((base_pos or {}).get('pos_uid') or ''))
            return None

    def _get_live_symbol_funding_rate_pct(self, symbol: str) -> float:
        """Return current funding rate in PERCENT for a symbol, best-effort."""
        try:
            if getattr(self, '_binance', None) is None:
                return 0.0
            resp = self._binance.premium_index(symbol=str(symbol).upper())
            row = None
            if isinstance(resp, list):
                for r in resp:
                    if str((r or {}).get('symbol') or '').upper() == str(symbol).upper():
                        row = r
                        break
            elif isinstance(resp, dict):
                row = resp
            if not isinstance(row, dict):
                return 0.0
            rate = row.get('lastFundingRate')
            if rate is None:
                rate = row.get('fundingRate')
            return float(rate or 0.0) * 100.0
        except Exception:
            return 0.0

    def _compute_adaptive_hedge_extra_koff(self, *, main_side: str, ref_price: float, mark_price: float) -> float:
        """Best-effort adaptive hedge boost based on adverse move against MAIN.

        Returns an *extra* koff (additive), capped by hedge_adaptive_max_extra_koff.
        """
        try:
            if not bool(getattr(self.p, 'hedge_adaptive_enabled', True)):
                return 0.0
            ref_px = float(ref_price or 0.0)
            mark_px = float(mark_price or 0.0)
            if ref_px <= 0.0 or mark_px <= 0.0:
                return 0.0
            side_u = str(main_side).upper()
            if side_u == 'LONG':
                adverse_pct = max(0.0, (ref_px - mark_px) / ref_px * 100.0)
            else:
                adverse_pct = max(0.0, (mark_px - ref_px) / ref_px * 100.0)
            trigger_pct = float(getattr(self.p, 'hedge_adaptive_trigger_pct', 0.8) or 0.8)
            step_pct = max(float(getattr(self.p, 'hedge_adaptive_step_pct', 0.8) or 0.8), 1e-9)
            boost_per_step = max(float(getattr(self.p, 'hedge_adaptive_boost_per_step', 0.08) or 0.08), 0.0)
            max_extra = max(float(getattr(self.p, 'hedge_adaptive_max_extra_koff', 0.20) or 0.20), 0.0)
            if adverse_pct < trigger_pct or boost_per_step <= 0.0 or max_extra <= 0.0:
                return 0.0
            steps = int(math.floor((adverse_pct - trigger_pct) / step_pct)) + 1
            extra = min(max_extra, float(steps) * boost_per_step)
            return max(0.0, float(extra))
        except Exception:
            return 0.0

    def _compute_live_hedge_qty_with_funding(self, *, symbol: str, main_side: str, main_qty: float, ref_price: float | None = None, mark_price: float | None = None) -> tuple[float, float, float]:
        """Compute hedge quantity with funding-aware boost and hard risk cap.

        Returns: (hedge_qty, effective_koff, funding_pct)
        """
        base_qty = abs(float(main_qty or 0.0))
        if base_qty <= 0.0:
            return 0.0, 0.0, 0.0

        try:
            hedge_koff = float(getattr(self.p, 'hedge_koff', 1.0) or 1.0)
        except Exception:
            hedge_koff = 1.0
        try:
            hedge_max_ratio = float(getattr(self.p, 'hedge_max_ratio', 1.0) or 1.0)
        except Exception:
            hedge_max_ratio = 1.0
        try:
            funding_limit_pct = float(getattr(self.p, 'hedge_funding_kof_limit', 0.0) or 0.0)
        except Exception:
            funding_limit_pct = 0.0
        try:
            funding_boost = float(getattr(self.p, 'hedge_koff_funding_boost', 1.0) or 1.0)
        except Exception:
            funding_boost = 1.0

        funding_pct = self._get_live_symbol_funding_rate_pct(str(symbol).upper())
        hedge_side = 'SHORT' if str(main_side).upper() == 'LONG' else 'LONG'
        favorable = False
        try:
            if funding_limit_pct > 0.0:
                if hedge_side == 'SHORT' and funding_pct >= funding_limit_pct:
                    favorable = True
                elif hedge_side == 'LONG' and funding_pct <= -funding_limit_pct:
                    favorable = True
        except Exception:
            favorable = False

        eff_koff = float(hedge_koff)
        if favorable and funding_boost > 0.0:
            eff_koff *= float(funding_boost)

        try:
            extra_koff = self._compute_adaptive_hedge_extra_koff(
                main_side=str(main_side).upper(),
                ref_price=float(ref_price or 0.0),
                mark_price=float(mark_price or 0.0),
            )
        except Exception:
            extra_koff = 0.0
        if extra_koff > 0.0:
            eff_koff += float(extra_koff)

        raw_qty = base_qty * eff_koff
        try:
            funding_guard = bool(getattr(self.p, 'hedge_funding_guard_enabled', False))
            funding_soft_cap_ratio = float(getattr(self.p, 'hedge_funding_soft_cap_ratio', 0.25) or 0.25)
        except Exception:
            funding_guard = False
            funding_soft_cap_ratio = 0.25
        if funding_guard:
            try:
                bad_limit = float(getattr(self.p, 'hedge_funding_bad_limit_pct', 0.8) or 0.8)
            except Exception:
                bad_limit = 0.8
            unfavorable = (hedge_side == 'SHORT' and funding_pct <= -abs(bad_limit)) or (hedge_side == 'LONG' and funding_pct >= abs(bad_limit))
            if unfavorable and funding_soft_cap_ratio > 0.0:
                hedge_max_ratio = min(float(hedge_max_ratio), float(funding_soft_cap_ratio))
        max_allowed = base_qty * max(float(hedge_max_ratio), 0.0)
        if max_allowed > 0.0:
            raw_qty = min(raw_qty, max_allowed)
            eff_koff = raw_qty / base_qty if base_qty > 0 else 0.0

        hedge_cap_pct = float(getattr(self.p, 'hedge_position_max_notional_pct_wallet', 0.0) or 0.0)
        if hedge_cap_pct > 0.0:
            wallet_live = _safe_float(self._rest_snapshot_get('wallet_balance_usdt'), 0.0)
            if wallet_live <= 0.0:
                wallet_live = _safe_float(self._wallet_balance_usdt(), 0.0)
            ref_px = abs(float(mark_price or ref_price or 0.0))
            if wallet_live > 0.0 and ref_px > 0.0:
                try:
                    qty_step = float(self._qty_step_for_symbol(symbol) or 0.0)
                except Exception:
                    qty_step = 0.0
                capped_qty = _cap_qty_by_wallet_notional(
                    qty=float(raw_qty),
                    wallet_balance_usdt=float(wallet_live),
                    price=float(ref_px),
                    cap_pct_wallet=float(hedge_cap_pct),
                    qty_step=float(qty_step or 0.0),
                    leverage=float(getattr(self.p, 'leverage', 1.0) or 1.0),
                )
                if capped_qty < float(raw_qty):
                    log.info('[trade_liquidation][LIMIT] capped HEDGE %s %s qty %.8f -> %.8f by hedge_position_max_notional_pct_wallet=%.4f', str(symbol).upper(), str(hedge_side).upper(), float(raw_qty), float(capped_qty), float(hedge_cap_pct))
                raw_qty = float(capped_qty)
                eff_koff = raw_qty / base_qty if base_qty > 0 else 0.0

        return float(raw_qty), float(eff_koff), float(funding_pct)

    def _live_reduce_hedge_market(self, *, symbol: str, hedge_ps: str, qty: float, reason: str = "UNWIND") -> bool:
        """Best-effort partial/full reduce of an open hedge leg."""
        sym_u = str(symbol).upper()
        ps_u = str(hedge_ps).upper()
        try:
            q = float(qty or 0.0)
        except Exception:
            q = 0.0
        if q <= 0.0 or getattr(self, '_binance', None) is None:
            return False
        try:
            step = float(self._qty_step_for_symbol(sym_u) or 0.0)
        except Exception:
            step = 0.0

        def _live_hedge_amt(force_refresh: bool = True) -> float:
            pr = []
            try:
                if force_refresh:
                    pr = self._binance.position_risk()
                    try:
                        self._rest_snapshot_set('position_risk', pr if isinstance(pr, list) else [])
                    except Exception:
                        pass
                else:
                    pr = self._rest_snapshot_get('position_risk') or []
            except Exception:
                pr = []
            if not isinstance(pr, list):
                pr = []
            for row in pr:
                try:
                    if str(row.get('symbol') or '').upper() != sym_u:
                        continue
                    if str(row.get('positionSide') or '').upper() != ps_u:
                        continue
                    amt = float(row.get('positionAmt') or 0.0)
                    return abs(amt)
                except Exception:
                    continue
            return 0.0

        live_amt = _live_hedge_amt(force_refresh=True)
        if live_amt <= 0.0:
            log.info('[TL][HEDGE_UNWIND] skip reduce no live hedge %s %s req_qty=%.8f reason=%s', sym_u, ps_u, float(q), str(reason))
            return False

        q = min(float(q), float(live_amt))
        if step and step > 0.0:
            q = float(_round_qty_to_step(q, step, mode='down'))
        if q <= 0.0:
            return False

        close_side = 'BUY' if ps_u == 'SHORT' else 'SELL'
        try:
            params = dict(symbol=sym_u, side=close_side, type='MARKET', quantity=float(q), positionSide=ps_u)
            try:
                self._binance.new_order(**params)
            except Exception as e1:
                msg1 = str(e1)
                if 'reduceonly' in msg1.lower() and '1106' in msg1:
                    params.pop('reduceOnly', None)
                    self._binance.new_order(**params)
                else:
                    raise
            return True
        except Exception as e:
            msg = str(e)
            if 'ReduceOnly Order is rejected' in msg or '"code":-2022' in msg or 'code: -2022' in msg:
                refreshed_amt = _live_hedge_amt(force_refresh=True)
                log.info('[TL][HEDGE_UNWIND] benign reduce race %s %s req_qty=%.8f live_amt=%.8f reason=%s', sym_u, ps_u, float(q), float(refreshed_amt), str(reason))
                return False
            log.exception('[TL][HEDGE_UNWIND] market reduce failed %s %s qty=%.8f reason=%s', sym_u, ps_u, float(q), str(reason))
            return False

    def _live_get_main_position_amt(self, *, symbol: str, main_ps: str, force_refresh: bool = False) -> float:
        sym_u = str(symbol).upper()
        ps_u = str(main_ps).upper()
        pr = None
        try:
            if force_refresh and getattr(self, '_binance', None) is not None:
                pr = self._binance.position_risk()
                try:
                    self._rest_snapshot_set('position_risk', pr if isinstance(pr, list) else [])
                except Exception:
                    pass
            else:
                pr = self._rest_snapshot_get('position_risk') or []
        except Exception:
            pr = []
        if not isinstance(pr, list) or not pr:
            try:
                if getattr(self, '_binance', None) is not None:
                    pr = self._binance.position_risk()
                    try:
                        self._rest_snapshot_set('position_risk', pr if isinstance(pr, list) else [])
                    except Exception:
                        pass
            except Exception:
                pr = []
        try:
            for r in pr if isinstance(pr, list) else []:
                if str(r.get('symbol') or '').upper() != sym_u:
                    continue
                if str(r.get('positionSide') or '').upper() != ps_u:
                    continue
                amt = float(r.get('positionAmt') or 0.0)
                if abs(amt) > 0.0:
                    return abs(float(amt))
            return 0.0
        except Exception:
            return 0.0

    def _verify_partial_reduce(self, *, symbol: str, position_side: str, before_amt: float, expected_reduce_qty: float, tag: str, pos_uid: str = "", reason: str = "") -> tuple[bool, float, float]:
        """Refresh live position and confirm that size actually decreased after a market reduce."""
        sym_u = str(symbol).upper()
        ps_u = str(position_side).upper()
        try:
            before = abs(float(before_amt or 0.0))
        except Exception:
            before = 0.0
        try:
            expected = abs(float(expected_reduce_qty or 0.0))
        except Exception:
            expected = 0.0
        after = 0.0
        try:
            after = float(self._live_get_position_amt(symbol=sym_u, position_side=ps_u, force_refresh=True) or 0.0)
        except Exception:
            after = 0.0
        actual = max(float(before) - float(after), 0.0)
        verify_abs = max(float(getattr(self.p, 'partial_close_verify_min_abs_qty', 0.0) or 0.0), 0.0)
        verify_ratio = max(float(getattr(self.p, 'partial_close_verify_min_fill_ratio', 0.20) or 0.20), 0.0)
        min_expected = float(expected) * float(verify_ratio) if expected > 0.0 else 0.0
        threshold = max(float(verify_abs), float(min_expected), 1e-12)
        ok = (actual >= threshold) or (before > 0.0 and after <= 1e-12)
        if not ok:
            try:
                self._health_bump('partial_verify_failed', 1)
            except Exception:
                pass
            log.warning('[%s][VERIFY] reduce not confirmed %s %s before=%.8f after=%.8f actual=%.8f expected=%.8f threshold=%.8f', str(tag), sym_u, ps_u, float(before), float(after), float(actual), float(expected), float(threshold))
        try:
            if pos_uid:
                self._persist_partial_reduce_state(
                    pos_uid=str(pos_uid or ''),
                    symbol=sym_u,
                    side=ps_u,
                    phase='verify',
                    before_amt=float(before),
                    expected_reduce_qty=float(expected),
                    after_amt=float(after),
                    actual_reduce_qty=float(actual),
                    verified=bool(ok),
                    reason=str(reason or tag or ''),
                )
        except Exception:
            pass
        return bool(ok), float(after), float(actual)

    def _live_get_position_amt(self, *, symbol: str, position_side: str, force_refresh: bool = False) -> float:
        sym_u = str(symbol).upper()
        ps_u = str(position_side).upper()
        pr = None
        try:
            if force_refresh and getattr(self, '_binance', None) is not None:
                pr = self._binance.position_risk()
                try:
                    self._rest_snapshot_set('position_risk', pr if isinstance(pr, list) else [])
                except Exception:
                    pass
            else:
                pr = self._rest_snapshot_get('position_risk') or []
        except Exception:
            pr = []
        if not isinstance(pr, list) or not pr:
            try:
                if getattr(self, '_binance', None) is not None:
                    pr = self._binance.position_risk()
                    try:
                        self._rest_snapshot_set('position_risk', pr if isinstance(pr, list) else [])
                    except Exception:
                        pass
            except Exception:
                pr = []
        try:
            for r in pr if isinstance(pr, list) else []:
                if str(r.get('symbol') or '').upper() != sym_u:
                    continue
                if str(r.get('positionSide') or '').upper() != ps_u:
                    continue
                amt = float(r.get('positionAmt') or 0.0)
                if abs(amt) > 0.0:
                    return abs(float(amt))
            return 0.0
        except Exception:
            return 0.0

    def _live_reduce_main_market(self, *, symbol: str, main_ps: str, qty: float, reason: str = "MAIN_PARTIAL_TP") -> bool:
        try:
            q = float(qty or 0.0)
        except Exception:
            q = 0.0
        if q <= 0.0 or getattr(self, '_binance', None) is None:
            return False
        sym_u = str(symbol).upper()
        ps_u = str(main_ps).upper()
        live_amt = 0.0
        try:
            live_amt = float(self._live_get_main_position_amt(symbol=sym_u, main_ps=ps_u, force_refresh=True) or 0.0)
        except Exception:
            live_amt = 0.0
        if live_amt <= 0.0:
            log.info('[TL][MAIN_PARTIAL_TP] skip market reduce %s %s qty=%.8f reason=%s live_amt=0', sym_u, ps_u, float(q), str(reason))
            return False
        q = min(float(q), float(live_amt))
        try:
            step = float(self._qty_step_for_symbol(sym_u) or 0.0)
        except Exception:
            step = 0.0
        if step and step > 0.0:
            q = float(_round_qty_to_step(q, step, mode='down'))
        if q <= 0.0:
            return False
        close_side = 'SELL' if ps_u == 'LONG' else 'BUY'
        try:
            self._binance.new_order(symbol=sym_u, side=close_side, type='MARKET', quantity=float(q), positionSide=ps_u)
            return True
        except Exception as e:
            msg = str(e)
            if 'ReduceOnly Order is rejected' in msg or '"code":-2022' in msg or 'code: -2022' in msg:
                refreshed_amt = 0.0
                try:
                    refreshed_amt = float(self._live_get_main_position_amt(symbol=sym_u, main_ps=ps_u, force_refresh=True) or 0.0)
                except Exception:
                    refreshed_amt = 0.0
                log.info('[TL][MAIN_PARTIAL_TP] benign reduce race %s %s req_qty=%.8f live_amt=%.8f reason=%s', sym_u, ps_u, float(q), float(refreshed_amt), str(reason))
                return False
            log.exception('[TL][MAIN_PARTIAL_TP] market reduce failed %s %s qty=%.8f reason=%s', sym_u, ps_u, float(q), str(reason))
            return False

    def _live_manage_main_partial_tp(self, *, p: dict, symbol: str, main_side: str, main_amt: float, entry_px: float, mark: float) -> bool:
        if not bool(getattr(self.p, 'main_partial_tp_enabled', False)):
            return False
        try:
            if float(main_amt or 0.0) <= 0.0 or float(entry_px or 0.0) <= 0.0 or float(mark or 0.0) <= 0.0:
                return False
            rm = p.get('raw_meta') if isinstance(p, dict) else None
            if isinstance(rm, str):
                rm = json.loads(rm) if rm.strip() else {}
            if not isinstance(rm, dict):
                rm = {}
            pm = rm.get('profit') if isinstance(rm.get('profit'), dict) else {}
            initial_qty = float(pm.get('main_qty_initial') or p.get('qty_current') or main_amt or 0.0)
            if initial_qty <= 0.0:
                initial_qty = float(main_amt)
            tp1_done = bool(pm.get('main_tp1_done'))
            tp2_done = bool(pm.get('main_tp2_done'))
            last_ts = float(pm.get('main_last_partial_tp_ts') or 0.0)
            cd = float(getattr(self.p, 'main_partial_tp_cooldown_sec', 30) or 30)
            if last_ts and (time.time() - last_ts) < max(cd, 0.0):
                return False
            if str(main_side).upper() == 'LONG':
                move_pct = (float(mark) - float(entry_px)) / max(float(entry_px), 1e-12) * 100.0
            else:
                move_pct = (float(entry_px) - float(mark)) / max(float(entry_px), 1e-12) * 100.0
            tp1_pct = float(getattr(self.p, 'main_partial_tp1_pct', 2.5) or 2.5)
            tp1_share = float(getattr(self.p, 'main_partial_tp1_share', 0.30) or 0.30)
            tp2_pct = float(getattr(self.p, 'main_partial_tp2_pct', 4.5) or 4.5)
            tp2_share = float(getattr(self.p, 'main_partial_tp2_share', 0.30) or 0.30)
            min_ratio = max(float(getattr(self.p, 'main_partial_tp_min_qty_ratio', 0.15) or 0.15), 0.0)
            action = None
            target_close_qty = 0.0
            if (not tp2_done) and move_pct >= tp2_pct:
                action = 'TP2'
                target_close_qty = float(initial_qty) * float(tp2_share)
            elif (not tp1_done) and move_pct >= tp1_pct:
                action = 'TP1'
                target_close_qty = float(initial_qty) * float(tp1_share)
            if not action or target_close_qty <= 0.0:
                return False
            min_left_qty = float(initial_qty) * float(min_ratio)
            max_close = max(float(main_amt) - min_left_qty, 0.0)
            target_close_qty = min(target_close_qty, max_close)
            if target_close_qty <= 0.0:
                return False
            before_amt = float(main_amt)
            try:
                self._persist_partial_reduce_state(
                    pos_uid=str(p.get('pos_uid') or ''),
                    symbol=str(symbol).upper(),
                    side=str(main_side).upper(),
                    phase='attempt',
                    before_amt=float(before_amt),
                    expected_reduce_qty=float(target_close_qty),
                    reason=str(action),
                )
            except Exception:
                pass
            if not self._live_reduce_main_market(symbol=str(symbol).upper(), main_ps=str(main_side).upper(), qty=float(target_close_qty), reason=action):
                try:
                    self._persist_partial_reduce_state(
                        pos_uid=str(p.get('pos_uid') or ''),
                        symbol=str(symbol).upper(),
                        side=str(main_side).upper(),
                        phase='attempt_failed',
                        before_amt=float(before_amt),
                        expected_reduce_qty=float(target_close_qty),
                        reason=str(action),
                    )
                except Exception:
                    pass
                return False
            verified, live_remaining_qty, actual_reduced = self._verify_partial_reduce(
                symbol=str(symbol).upper(),
                position_side=str(main_side).upper(),
                before_amt=float(before_amt),
                expected_reduce_qty=float(target_close_qty),
                tag='TL][MAIN_PARTIAL_TP',
                pos_uid=str(p.get('pos_uid') or ''),
                reason=str(action),
            )
            if not verified:
                return False
            remaining_qty = max(float(live_remaining_qty), 0.0)
            try:
                if getattr(self, '_pl_has_raw_meta', False):
                    patch = {'profit': {
                        'main_qty_initial': float(initial_qty),
                        'main_tp1_done': bool(tp1_done or action == 'TP1' or action == 'TP2'),
                        'main_tp2_done': bool(tp2_done or action == 'TP2'),
                        'main_last_partial_tp_ts': float(time.time()),
                        'main_last_partial_tp_req_qty': float(target_close_qty),
                        'main_last_partial_tp_fill_qty': float(actual_reduced),
                        'main_last_partial_tp_verify_ts': float(time.time()),
                    }}
                    self.store.execute(
                        """
                        UPDATE position_ledger
                        SET raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb,
                            qty_current=%(qty)s, updated_at = now()
                        WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND status='OPEN';
                        """,
                        {'meta': json.dumps(patch), 'qty': float(remaining_qty), 'ex': int(self.exchange_id), 'acc': int(self.account_id), 'pos_uid': str(p.get('pos_uid') or '')},
                    )
            except Exception:
                pass
            log.info('[TL][MAIN_PARTIAL_TP] %s %s action=%s close_qty=%.8f fill_qty=%.8f remain_live=%.8f move_pct=%.4f entry=%.8f mark=%.8f', symbol, str(main_side).upper(), str(action), float(target_close_qty), float(actual_reduced), float(remaining_qty), float(move_pct), float(entry_px), float(mark))
            return True
        except Exception:
            log.exception('[TL][MAIN_PARTIAL_TP] unexpected error for %s %s', symbol, main_side)
            return False

    def _ensure_immediate_hedge_protection(self, *, base_pos: dict, symbol: str, symbol_id: int, hedge_ps: str, hedge_amt: float, hedge_entry: float, mark: float, allow_trailing: bool = True, allow_sl: bool = True) -> None:
        """Immediately place hedge SL / hedge trailing after hedge open or re-open.

        This closes the one-cycle gap where a just-opened hedge leg had no protection until
        the next hedge manager pass or recovery loop.
        """
        try:
            if float(hedge_amt or 0.0) <= 0.0:
                return
            if not self._has_open_position_live(symbol, str(hedge_ps).upper()):
                return
        except Exception:
            return

        try:
            self._ensure_live_hedge_binding(
                base_pos=base_pos,
                symbol=str(symbol).upper(),
                symbol_id=int(symbol_id),
                hedge_ps=str(hedge_ps).upper(),
                hedge_amt=float(hedge_amt or 0.0),
                hedge_entry=float(hedge_entry or 0.0),
                mark=float(mark or 0.0),
            )
        except Exception:
            log.debug('[TL][HEDGE_PROTECT] ensure binding failed %s %s', symbol, hedge_ps, exc_info=True)

        tok = _coid_token(str((base_pos or {}).get('pos_uid') or ''), n=20)

        # --- Immediate HEDGE_TRL ---
        try:
            if bool(allow_trailing) and bool(getattr(self.p, 'hedge_trailing_enabled', False)):
                cid_htrl = f'TL_{tok}_HEDGE_TRL'
                ex_trl = self._find_known_algo_order_by_client_id(symbol, cid_htrl)
                if ex_trl is None:
                    close_side = 'BUY' if str(hedge_ps).upper() == 'SHORT' else 'SELL'
                    ex_trl = self._find_semantic_open_algo(
                        symbol,
                        client_suffix='_HEDGE_TRL',
                        position_side=str(hedge_ps).upper(),
                        order_type='TRAILING_STOP_MARKET',
                        side=close_side,
                        close_position=False,
                    )
                if ex_trl is None:
                    q = float(hedge_amt or 0.0)
                    try:
                        step = float(self._qty_step_for_symbol(symbol) or 0.0)
                    except Exception:
                        step = 0.0
                    if step > 0.0:
                        q = _round_qty_to_step(q, step, mode='down')
                    if q > 0.0:
                        act_pct = float(getattr(self.p, 'hedge_trailing_activation_pct', 1.5) or 1.5)
                        trail_pct = float(getattr(self.p, 'hedge_trailing_trail_pct', 0.5) or 0.5)
                        buf_pct = float(getattr(self.p, 'hedge_trailing_activation_buffer_pct', 0.25) or 0.25)
                        work_type = str(getattr(self.p, 'hedge_trailing_working_type', 'MARK_PRICE') or 'MARK_PRICE').upper()
                        try:
                            tick_h = float(self._price_tick_for_symbol(symbol) or 0.0)
                        except Exception:
                            tick_h = 0.0
                        hedge_entry_ref = float(hedge_entry or 0.0)
                        if hedge_entry_ref <= 0.0:
                            hedge_entry_ref = float(mark or 0.0)
                        if hedge_entry_ref > 0.0:
                            if str(hedge_ps).upper() == 'SHORT':
                                activation = hedge_entry_ref * (1.0 - act_pct / 100.0)
                                close_side = 'BUY'
                                if float(mark or 0.0) > 0.0 and tick_h > 0.0:
                                    max_act = float(mark) * (1.0 - buf_pct / 100.0)
                                    if activation >= max_act:
                                        activation = float(_round_price_to_tick(max_act - tick_h, tick_h, mode='down'))
                            else:
                                activation = hedge_entry_ref * (1.0 + act_pct / 100.0)
                                close_side = 'SELL'
                                if float(mark or 0.0) > 0.0 and tick_h > 0.0:
                                    min_act = float(mark) * (1.0 + buf_pct / 100.0)
                                    if activation <= min_act:
                                        activation = float(_round_price_to_tick(min_act + tick_h, tick_h, mode='up'))
                            if tick_h > 0.0:
                                activation = float(_round_price_to_tick(activation, tick_h, mode=('down' if str(hedge_ps).upper() == 'SHORT' else 'up')))
                                activation = float(_ensure_tp_trail_side(activation, hedge_entry_ref, tick_h, str(hedge_ps).upper(), kind='hedge_trail_activate'))
                            resp = self._binance.new_order(
                                symbol=symbol,
                                side=close_side,
                                type='TRAILING_STOP_MARKET',
                                quantity=float(q),
                                positionSide=str(hedge_ps).upper(),
                                newClientOrderId=cid_htrl,
                                activationPrice=float(activation),
                                callbackRate=float(trail_pct),
                                workingType=work_type,
                            )
                            try:
                                self.store.upsert_algo_orders([{
                                    'exchange_id': int(self.exchange_id),
                                    'account_id': int(self.account_id),
                                    'client_algo_id': str(cid_htrl),
                                    'algo_id': str((resp or {}).get('algoId') or (resp or {}).get('orderId') or ''),
                                    'symbol': str(symbol),
                                    'side': str(close_side),
                                    'position_side': str(hedge_ps).upper(),
                                    'type': 'TRAILING_STOP_MARKET',
                                    'quantity': float(q),
                                    'trigger_price': float(activation),
                                    'working_type': str(work_type),
                                    'status': 'OPEN',
                                    'strategy_id': str(self.STRATEGY_ID),
                                    'pos_uid': str((base_pos or {}).get('pos_uid') or ''),
                                    'raw_json': dict(resp or {}, **{'kind': 'HEDGE_TRL', 'placed_immediately': True}),
                                }])
                            except Exception:
                                pass
                            try:
                                self._upsert_order_shadow(
                                    pos_uid=str((base_pos or {}).get('pos_uid') or ''),
                                    order_id=(resp or {}).get('orderId'),
                                    client_order_id=cid_htrl,
                                    symbol_id=int(symbol_id),
                                    side=close_side,
                                    order_type='TRAILING_STOP_MARKET',
                                    status='NEW',
                                    qty=float(q),
                                    price=float(activation),
                                    reduce_only=True,
                                )
                            except Exception:
                                pass
                            try:
                                self._mark_local_algo_open(
                                    cid_htrl,
                                    symbol=symbol,
                                    position_side=str(hedge_ps).upper(),
                                    order_type='TRAILING_STOP_MARKET',
                                    side=close_side,
                                    quantity=float(q),
                                    trigger_price=float(activation),
                                    close_position=False,
                                    pos_uid=str((base_pos or {}).get('pos_uid') or ''),
                                )
                            except Exception:
                                pass
                            log.info('[TL][HEDGE_TRL][IMMEDIATE] placed %s %s qty=%.8f activation=%.8f cid=%s', symbol, str(hedge_ps).upper(), float(q), float(activation), cid_htrl)
        except Exception:
            log.exception('[TL][HEDGE_TRL][IMMEDIATE] failed for %s %s', symbol, hedge_ps)

        # --- Immediate HEDGE_SL ---
        try:
            hedge_sl_pct = float(getattr(self.p, 'hedge_stop_loss_pct', 0.0) or 0.0)
            if bool(allow_sl) and hedge_sl_pct > 0.0:
                cid_hsl = f'TL_{tok}_HEDGE_SL'
                sl_side = 'BUY' if str(hedge_ps).upper() == 'SHORT' else 'SELL'
                ex_hsl = self._find_known_algo_order_by_client_id(symbol, cid_hsl)
                if ex_hsl is None:
                    ex_hsl = self._find_semantic_open_algo(
                        symbol,
                        client_suffix='_HEDGE_SL',
                        position_side=str(hedge_ps).upper(),
                        order_type='STOP_MARKET',
                        side=sl_side,
                        close_position=True,
                    )
                if ex_hsl is None:
                    hedge_entry_use = float(hedge_entry or 0.0)
                    if hedge_entry_use <= 0.0:
                        hedge_entry_use = float(mark or 0.0)
                    if hedge_entry_use > 0.0:
                        if str(hedge_ps).upper() == 'SHORT':
                            stop_px = hedge_entry_use * (1.0 + hedge_sl_pct / 100.0)
                        else:
                            stop_px = hedge_entry_use * (1.0 - hedge_sl_pct / 100.0)
                        sl_working_type = str(
                            getattr(self.p, 'hedge_stop_loss_working_type', None)
                            or getattr(self.p, 'hedge_trailing_working_type', None)
                            or getattr(self.p, 'working_type', 'MARK_PRICE')
                            or 'MARK_PRICE'
                        ).upper()
                        try:
                            tick_safe = float(self._price_tick_for_symbol(symbol) or 0.0)
                        except Exception:
                            tick_safe = 0.0
                        try:
                            sl_buf = float(getattr(self.p, 'hedge_stop_loss_trigger_buffer_pct', 0.35) or 0.35)
                        except Exception:
                            sl_buf = 0.35
                        safe_trig = float(stop_px)
                        if float(mark or 0.0) > 0.0 and tick_safe > 0.0:
                            if str(hedge_ps).upper() == 'SHORT':
                                safe_trig = float(self._safe_price_above_mark(safe_trig, float(mark), tick_safe, sl_buf))
                            else:
                                safe_trig = float(self._safe_price_below_mark(safe_trig, float(mark), tick_safe, sl_buf))
                        resp_sl = self._binance.new_algo_order(
                            symbol=symbol,
                            side=sl_side,
                            type='STOP_MARKET',
                            closePosition=True,
                            workingType=sl_working_type,
                            positionSide=str(hedge_ps).upper(),
                            algoType='CONDITIONAL',
                            clientAlgoId=cid_hsl,
                            triggerPrice=self._fmt_price(symbol, safe_trig),
                        )
                        try:
                            self._upsert_algo_order_shadow(resp_sl, pos_uid=str((base_pos or {}).get('pos_uid') or ''), strategy_id='trade_liquidation')
                        except Exception:
                            pass
                        try:
                            self._mark_local_algo_open(
                                cid_hsl,
                                symbol=symbol,
                                position_side=str(hedge_ps).upper(),
                                order_type='STOP_MARKET',
                                side=sl_side,
                                quantity=float(hedge_amt),
                                trigger_price=float(safe_trig),
                                close_position=True,
                                pos_uid=str((base_pos or {}).get('pos_uid') or ''),
                            )
                        except Exception:
                            pass
                        log.info('[TL][HEDGE_SL][IMMEDIATE] placed %s %s qty=%.8f stop=%.8f cid=%s', symbol, str(hedge_ps).upper(), float(hedge_amt), float(safe_trig), cid_hsl)
        except Exception:
            log.exception('[TL][HEDGE_SL][IMMEDIATE] failed for %s %s', symbol, hedge_ps)

    def _maybe_add_main_on_hedge_price_diff(self, *, p: dict, symbol: str, symbol_id: int, main_side: str, main_ps: str, main_amt: float, main_entry: float, hedge_ps: str, hedge_amt: float, hedge_entry: float, hedge_last_open_ts: float) -> bool:
        """Optionally add to MAIN when hedge entry is far from main entry by configured percent.

        The add is executed at most once per hedge open/reopen cycle (tracked by hedge.last_open_ts).
        """
        try:
            if not bool(getattr(self.p, 'main_add_on_hedge_price_diff_enabled', False)):
                return False
            diff_limit_pct = abs(float(getattr(self.p, 'main_add_on_hedge_price_diff_pct', 20.0) or 0.0))
            lot_koff = float(getattr(self.p, 'main_add_on_hedge_price_diff_koff_lot', 2.0) or 0.0)
            if diff_limit_pct <= 0.0 or lot_koff <= 0.0:
                return False
            main_entry = abs(float(main_entry or 0.0))
            hedge_entry = abs(float(hedge_entry or 0.0))
            main_amt = abs(float(main_amt or 0.0))
            hedge_amt = abs(float(hedge_amt or 0.0))
            if main_entry <= 0.0 or hedge_entry <= 0.0 or main_amt <= 0.0 or hedge_amt <= 0.0:
                return False

            diff_pct = abs(hedge_entry - main_entry) / main_entry * 100.0
            if diff_pct < diff_limit_pct:
                return False

            raw_meta = p.get('raw_meta')
            try:
                if isinstance(raw_meta, str):
                    raw_meta = json.loads(raw_meta) if raw_meta.strip() else {}
            except Exception:
                raw_meta = {}
            if not isinstance(raw_meta, dict):
                raw_meta = {}
            hedge_meta = raw_meta.get('hedge') if isinstance(raw_meta.get('hedge'), dict) else {}
            add_meta = hedge_meta.get('main_add_on_entry_diff') if isinstance(hedge_meta.get('main_add_on_entry_diff'), dict) else {}

            prev_open_ts = float(add_meta.get('last_open_ts') or 0.0) if add_meta else 0.0
            prev_hedge_entry = float(add_meta.get('hedge_entry') or 0.0) if add_meta else 0.0
            if hedge_last_open_ts > 0.0 and prev_open_ts >= hedge_last_open_ts:
                return False
            if hedge_last_open_ts <= 0.0 and prev_hedge_entry > 0.0 and abs(prev_hedge_entry - hedge_entry) <= max(hedge_entry * 1e-9, 1e-12):
                return False

            try:
                if not hasattr(self, '_hedge_entry_diff_add_guard') or not isinstance(self._hedge_entry_diff_add_guard, dict):
                    self._hedge_entry_diff_add_guard = {}
                gkey = f"{str(p.get('pos_uid') or '')}:{str(hedge_ps).upper()}:{round(float(hedge_last_open_ts or 0.0), 3)}:{round(float(hedge_entry), 12)}"
                last_ts = float(self._hedge_entry_diff_add_guard.get(gkey) or 0.0)
                if last_ts > 0.0 and (time.time() - last_ts) < 30.0:
                    return False
                self._hedge_entry_diff_add_guard[gkey] = float(time.time())
            except Exception:
                pass

            qty_step = float(self._qty_step_for_symbol(symbol) or 0.0) or 0.0
            add_qty = float(main_amt) * float(lot_koff)
            if qty_step > 0.0:
                add_qty = _round_qty_to_step(add_qty, qty_step, mode='down')

            wallet_live = _safe_float(self._rest_snapshot_get('wallet_balance_usdt'), 0.0)
            if wallet_live <= 0.0:
                wallet_live = _safe_float(self._wallet_balance_usdt(), 0.0)
            add_ref_price = float(main_entry or hedge_entry or 0.0)
            add_cap_pct = float(getattr(self.p, 'averaging_add_max_notional_pct_wallet', 0.0) or 0.0)
            if add_cap_pct > 0.0 and wallet_live > 0.0 and add_ref_price > 0.0:
                capped_add_qty = _cap_qty_by_wallet_notional(
                    qty=float(add_qty),
                    wallet_balance_usdt=float(wallet_live),
                    price=float(add_ref_price),
                    cap_pct_wallet=float(add_cap_pct),
                    qty_step=float(qty_step or 0.0),
                    leverage=float(getattr(self.p, 'leverage', 1.0) or 1.0),
                )
                if capped_add_qty < float(add_qty):
                    log.info('[trade_liquidation][LIMIT] capped MAIN_ADD_ON_HEDGE_DIFF %s %s qty %.8f -> %.8f by averaging_add_max_notional_pct_wallet=%.4f', str(symbol).upper(), str(main_ps).upper(), float(add_qty), float(capped_add_qty), float(add_cap_pct))
                add_qty = float(capped_add_qty)
            main_total_cap_pct = float(getattr(self.p, 'main_position_max_notional_pct_wallet', 0.0) or 0.0)
            if main_total_cap_pct > 0.0 and wallet_live > 0.0 and add_ref_price > 0.0:
                max_total_qty = _cap_qty_by_wallet_notional(
                    qty=float(main_amt) + float(add_qty),
                    wallet_balance_usdt=float(wallet_live),
                    price=float(add_ref_price),
                    cap_pct_wallet=float(main_total_cap_pct),
                    qty_step=float(qty_step or 0.0),
                    leverage=float(getattr(self.p, 'leverage', 1.0) or 1.0),
                )
                allowed_add_qty = max(0.0, float(max_total_qty) - float(main_amt))
                if qty_step > 0.0:
                    allowed_add_qty = _round_qty_to_step(allowed_add_qty, qty_step, mode='down')
                if allowed_add_qty < float(add_qty):
                    log.info('[trade_liquidation][LIMIT] capped MAIN_ADD_ON_HEDGE_DIFF total %s %s qty %.8f -> %.8f by main_position_max_notional_pct_wallet=%.4f', str(symbol).upper(), str(main_ps).upper(), float(add_qty), float(allowed_add_qty), float(main_total_cap_pct))
                add_qty = float(max(0.0, allowed_add_qty))
            if add_qty <= 0.0 or (qty_step > 0.0 and add_qty < qty_step):
                return False

            add_side = 'BUY' if str(main_side).upper() == 'LONG' else 'SELL'
            resp = self._binance.new_order(
                symbol=str(symbol).upper(),
                side=add_side,
                type='MARKET',
                quantity=float(add_qty),
                positionSide=str(main_ps).upper(),
            )

            try:
                if getattr(self, '_pl_has_raw_meta', False):
                    import json as _json
                    meta = {
                        'hedge': {
                            'main_add_on_entry_diff': {
                                'enabled': True,
                                'last_ts': float(time.time()),
                                'last_open_ts': float(hedge_last_open_ts or 0.0),
                                'main_entry': float(main_entry),
                                'hedge_entry': float(hedge_entry),
                                'diff_pct': float(diff_pct),
                                'diff_limit_pct': float(diff_limit_pct),
                                'koff_lot': float(lot_koff),
                                'last_qty': float(add_qty),
                                'last_order_id': str(resp.get('orderId') or resp.get('order_id') or ''),
                                'hedge_side': str(hedge_ps).upper(),
                                'main_side': str(main_ps).upper(),
                            }
                        }
                    }
                    self.store.execute(
                        """
                        UPDATE position_ledger
                        SET raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb,
                            updated_at = now()
                        WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND status='OPEN';
                        """,
                        {
                            'meta': _json.dumps(meta),
                            'ex': int(self.exchange_id),
                            'acc': int(self.account_id),
                            'pos_uid': str(p.get('pos_uid')),
                        },
                    )
            except Exception:
                pass

            log.info('[TL][HEDGE_DIFF_ADD] placed %s main_side=%s hedge_side=%s main_entry=%.8f hedge_entry=%.8f diff_pct=%.4f limit_pct=%.4f add_qty=%.8f koff=%.4f',
                     str(symbol).upper(), str(main_ps).upper(), str(hedge_ps).upper(), float(main_entry), float(hedge_entry), float(diff_pct), float(diff_limit_pct), float(add_qty), float(lot_koff))
            return True
        except Exception:
            log.exception('[TL][HEDGE_DIFF_ADD] failed for %s main_side=%s hedge_side=%s', str(symbol).upper(), str(main_ps).upper(), str(hedge_ps).upper())
            return False

    def _live_manage_hedge_funding_guard(self, *, p: dict, symbol: str, main_side: str, hedge_ps: str, hedge_amt: float, mark: float) -> bool:
        if not bool(getattr(self.p, 'hedge_funding_guard_enabled', False)):
            return False
        try:
            if float(hedge_amt or 0.0) <= 0.0:
                return False
            funding_pct = self._get_live_symbol_funding_rate_pct(str(symbol).upper())
            rm = p.get('raw_meta') if isinstance(p, dict) else None
            if isinstance(rm, str):
                rm = json.loads(rm) if rm.strip() else {}
            if not isinstance(rm, dict):
                rm = {}
            hmeta = rm.get('hedge') if isinstance(rm.get('hedge'), dict) else {}
            last_ts = float(hmeta.get('funding_last_action_ts') or 0.0)
            cd = float(getattr(self.p, 'hedge_funding_cooldown_sec', 300) or 300)
            if last_ts and (time.time() - last_ts) < max(cd, 0.0):
                return False
            bad_limit = abs(float(getattr(self.p, 'hedge_funding_bad_limit_pct', 0.8) or 0.8))
            full_limit = abs(float(getattr(self.p, 'hedge_funding_full_exit_limit_pct', 1.2) or 1.2))
            reduce_share = float(getattr(self.p, 'hedge_funding_reduce_share', 0.25) or 0.25)
            adverse = (str(hedge_ps).upper() == 'SHORT' and funding_pct <= -bad_limit) or (str(hedge_ps).upper() == 'LONG' and funding_pct >= bad_limit)
            toxic = (str(hedge_ps).upper() == 'SHORT' and funding_pct <= -full_limit) or (str(hedge_ps).upper() == 'LONG' and funding_pct >= full_limit)
            if not (adverse or toxic):
                return False
            close_qty = float(hedge_amt) if toxic else float(hedge_amt) * max(min(reduce_share, 1.0), 0.0)
            if close_qty <= 0.0:
                return False
            action = 'FUNDING_FULL_EXIT' if toxic else 'FUNDING_REDUCE'
            if not self._live_reduce_hedge_market(symbol=str(symbol).upper(), hedge_ps=str(hedge_ps).upper(), qty=float(close_qty), reason=action):
                return False
            remain = max(float(hedge_amt) - float(close_qty), 0.0)
            try:
                if getattr(self, '_pl_has_raw_meta', False):
                    patch = {'hedge': {'funding_last_action_ts': float(time.time()), 'last_action_ts': float(time.time())}}
                    self.store.execute(
                        """
                        UPDATE position_ledger
                        SET raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb, updated_at=now()
                        WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND status='OPEN';
                        """,
                        {'meta': json.dumps(patch), 'ex': int(self.exchange_id), 'acc': int(self.account_id), 'pos_uid': str(p.get('pos_uid') or '')},
                    )
                self.store.execute(
                    """
                    UPDATE position_ledger pl SET qty_current=%(qty)s, updated_at=now()
                    WHERE pl.exchange_id=%(ex)s AND pl.account_id=%(acc)s AND pl.status='OPEN' AND COALESCE(pl.source,'live')='live_hedge'
                      AND pl.pos_uid IN (SELECT hl.hedge_pos_uid FROM hedge_links hl WHERE hl.exchange_id=%(ex)s AND hl.base_account_id=%(acc)s AND hl.base_pos_uid=%(base_pos_uid)s);
                    """,
                    {'qty': float(remain), 'ex': int(self.exchange_id), 'acc': int(self.account_id), 'base_pos_uid': str(p.get('pos_uid') or '')},
                )
            except Exception:
                pass
            log.info('[TL][HEDGE_FUNDING_%s] %s %s close_qty=%.8f remain_est=%.8f funding_pct=%.4f mark=%.8f', 'FULL_EXIT' if toxic else 'REDUCE', symbol, str(hedge_ps).upper(), float(close_qty), float(remain), float(funding_pct), float(mark))
            return True
        except Exception:
            log.exception('[TL][HEDGE_FUNDING] unexpected error for %s %s', symbol, hedge_ps)
            return False

    def _live_manage_hedge_unwind(self, *, p: dict, symbol: str, main_side: str, hedge_ps: str, hedge_amt: float, hedge_entry_use: float, mark: float) -> bool:
        """Balanced hedge profit optimization: partial unload and optional full exit when price moves back in favor of MAIN."""
        if not bool(getattr(self.p, 'hedge_unwind_enabled', False)):
            return False
        try:
            if float(hedge_amt or 0.0) <= 0.0 or float(hedge_entry_use or 0.0) <= 0.0 or float(mark or 0.0) <= 0.0:
                return False
            rm = p.get('raw_meta') if isinstance(p, dict) else None
            if isinstance(rm, str):
                rm = json.loads(rm) if rm.strip() else {}
            if not isinstance(rm, dict):
                rm = {}
            hmeta = rm.get('hedge') if isinstance(rm.get('hedge'), dict) else {}

            initial_qty = float(hmeta.get('hedge_qty_initial') or hmeta.get('base_qty') or hedge_amt or 0.0)
            if initial_qty <= 0.0:
                initial_qty = float(hedge_amt)
            step1_done = bool(hmeta.get('unwind_step1_done'))
            step2_done = bool(hmeta.get('unwind_step2_done'))
            full_done = bool(hmeta.get('unwind_full_exit_done'))
            last_action_ts = float(hmeta.get('unwind_last_action_ts') or 0.0)

            cd_sec = float(getattr(self.p, 'hedge_unwind_cooldown_sec', 30) or 30)
            if last_action_ts and (time.time() - last_action_ts) < max(cd_sec, 0.0):
                return False

            if str(main_side).upper() == 'LONG':
                move_pct = (float(mark) - float(hedge_entry_use)) / max(float(hedge_entry_use), 1e-12) * 100.0
            else:
                move_pct = (float(hedge_entry_use) - float(mark)) / max(float(hedge_entry_use), 1e-12) * 100.0

            step1_pct = float(getattr(self.p, 'hedge_unwind_step1_pct', 0.8) or 0.8)
            step1_share = float(getattr(self.p, 'hedge_unwind_step1_share', 0.35) or 0.35)
            step2_pct = float(getattr(self.p, 'hedge_unwind_step2_pct', 1.6) or 1.6)
            step2_share = float(getattr(self.p, 'hedge_unwind_step2_share', 0.35) or 0.35)
            full_exit_pct = float(getattr(self.p, 'hedge_unwind_full_exit_pct', 2.4) or 2.4)
            min_ratio = max(float(getattr(self.p, 'hedge_unwind_min_qty_ratio', 0.15) or 0.15), 0.0)

            try:
                if bool(getattr(self.p, 'debug', False)):
                    log.info('[TL][HEDGE_UNWIND_CHECK] %s %s move_pct=%.4f step1=%.4f step2=%.4f full=%.4f entry=%.8f mark=%.8f', symbol, str(hedge_ps).upper(), float(move_pct), float(step1_pct), float(step2_pct), float(full_exit_pct), float(hedge_entry_use), float(mark))
            except Exception:
                pass

            action = None
            target_close_qty = 0.0
            cur_amt = float(hedge_amt)
            min_left_qty = float(initial_qty) * float(min_ratio)

            if bool(getattr(self.p, 'hedge_unwind_full_exit_enabled', True)) and (not full_done) and move_pct >= full_exit_pct:
                action = 'FULL'
                target_close_qty = cur_amt
            elif (not step2_done) and move_pct >= step2_pct:
                action = 'STEP2'
                target_close_qty = float(initial_qty) * float(step2_share)
            elif (not step1_done) and move_pct >= step1_pct:
                action = 'STEP1'
                target_close_qty = float(initial_qty) * float(step1_share)

            if not action or target_close_qty <= 0.0:
                return False

            if action != 'FULL':
                max_close = max(cur_amt - min_left_qty, 0.0)
                target_close_qty = min(target_close_qty, max_close)
                if target_close_qty <= 0.0:
                    return False

            before_amt = float(cur_amt)
            try:
                self._persist_partial_reduce_state(
                    pos_uid=str(p.get('pos_uid') or ''),
                    symbol=str(symbol).upper(),
                    side=str(hedge_ps).upper(),
                    phase='attempt',
                    before_amt=float(before_amt),
                    expected_reduce_qty=float(target_close_qty),
                    reason=f'UNWIND_{action}',
                )
            except Exception:
                pass
            if not self._live_reduce_hedge_market(symbol=str(symbol).upper(), hedge_ps=str(hedge_ps).upper(), qty=float(target_close_qty), reason=f'UNWIND_{action}'):
                try:
                    self._persist_partial_reduce_state(
                        pos_uid=str(p.get('pos_uid') or ''),
                        symbol=str(symbol).upper(),
                        side=str(hedge_ps).upper(),
                        phase='attempt_failed',
                        before_amt=float(before_amt),
                        expected_reduce_qty=float(target_close_qty),
                        reason=f'UNWIND_{action}',
                    )
                except Exception:
                    pass
                return False

            verified, live_remaining_qty, actual_reduced = self._verify_partial_reduce(
                symbol=str(symbol).upper(),
                position_side=str(hedge_ps).upper(),
                before_amt=float(before_amt),
                expected_reduce_qty=float(target_close_qty),
                tag='TL][HEDGE_UNWIND',
                pos_uid=str(p.get('pos_uid') or ''),
                reason=f'UNWIND_{action}',
            )
            if not verified:
                return False

            remaining_qty = max(float(live_remaining_qty), 0.0)
            try:
                if getattr(self, '_pl_has_raw_meta', False):
                    patch = {'hedge': {
                        'hedge_qty_initial': float(initial_qty),
                        'unwind_step1_done': bool(step1_done or action == 'STEP1' or action == 'STEP2' or action == 'FULL'),
                        'unwind_step2_done': bool(step2_done or action == 'STEP2' or action == 'FULL'),
                        'unwind_full_exit_done': bool(full_done or action == 'FULL'),
                        'unwind_last_action_ts': float(time.time()),
                        'last_action_ts': float(time.time()),
                        'unwind_last_req_qty': float(target_close_qty),
                        'unwind_last_fill_qty': float(actual_reduced),
                        'unwind_last_verify_ts': float(time.time()),
                    }}
                    if action == 'FULL':
                        patch['hedge'].update({'last_close_reason': 'UNWIND_FULL', 'last_close_ts': float(time.time()), 'last_close_px': float(mark), 'reopen_anchor_px': float(mark)})
                    self.store.execute(
                        """
                        UPDATE position_ledger
                        SET raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb,
                            updated_at = now()
                        WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND status='OPEN';
                        """,
                        {'meta': json.dumps(patch), 'ex': int(self.exchange_id), 'acc': int(self.account_id), 'pos_uid': str(p.get('pos_uid') or '')},
                    )
            except Exception:
                pass

            try:
                self.store.execute(
                    """
                    UPDATE position_ledger pl
                       SET qty_current=%(qty)s, updated_at=now()
                     WHERE pl.exchange_id=%(ex)s AND pl.account_id=%(acc)s
                       AND pl.status='OPEN' AND COALESCE(pl.source,'live')='live_hedge'
                       AND pl.pos_uid IN (
                           SELECT hl.hedge_pos_uid
                             FROM hedge_links hl
                            WHERE hl.exchange_id=%(ex)s AND hl.base_account_id=%(acc)s AND hl.base_pos_uid=%(base_pos_uid)s
                       );
                    """,
                    {'qty': float(remaining_qty), 'ex': int(self.exchange_id), 'acc': int(self.account_id), 'base_pos_uid': str(p.get('pos_uid') or '')},
                )
            except Exception:
                pass

            log.info('[TL][HEDGE_UNWIND] %s %s action=%s close_qty=%.8f fill_qty=%.8f remain_live=%.8f move_pct=%.4f entry=%.8f mark=%.8f', symbol, str(hedge_ps).upper(), str(action), float(target_close_qty), float(actual_reduced), float(remaining_qty), float(move_pct), float(hedge_entry_use), float(mark))
            return True
        except Exception:
            log.exception('[TL][HEDGE_UNWIND] unexpected error for %s %s', symbol, hedge_ps)
            return False

    def _live_ensure_hedge_after_last_add(
        self,
        *,
        pos: dict,
        sym: str,
        pos_side: str,
        pos_uid: str,
        prefix: str,
        tok: str,
        adds_done: int,
        max_adds: int,
    ) -> bool:
        """After the last allowed add, place a *conditional market* hedge opener.

        This is NOT the main trailing order. We use a dedicated client id:
          - {prefix}_{tok}_HEDGE

        Trigger is derived from the last add fill and `sl_after_last_add_distance_pct`.
        Quantity is derived from the *exchange* position size (positionRisk.positionAmt)
        multiplied by `hedge_koff` and rounded to qty step.

        We also cancel a legacy hedge order that used `{prefix}_{tok}_TRL` with type STOP_MARKET.
        """
        try:
            dist_pct = float(getattr(self.p, "sl_after_last_add_distance_pct", 0.0) or 0.0)
        except Exception:
            dist_pct = 0.0
        if dist_pct <= 0:
            return False

        if int(adds_done) < int(max_adds) or int(max_adds) <= 0:
            return False

        last_n = int(min(int(adds_done), int(max_adds)))
        last_fill = float(self._live_last_add_fill_price(sym=sym, prefix=prefix, tok=tok, add_n=last_n) or 0.0)
        if last_fill <= 0:
            return False

        # Resolve symbol_id for DB filtering (orders/order_fills store symbol_id)
        symbol_id = 0
        try:
            sym_row = self.store.query_one(
                """
                SELECT symbol_id
                FROM public.symbols
                WHERE exchange_id=%(ex)s AND upper(symbol)=%(sym)s
                LIMIT 1;
                """,
                {"ex": int(self.exchange_id), "sym": str(sym).upper()},
            )
            symbol_id = int(sym_row[0]) if sym_row and sym_row[0] is not None else 0
        except Exception:
            symbol_id = 0
        if symbol_id <= 0:
            return False

        # Compare by DB write time (orders.updated_at), not fill time (order_fills.ts).
        last_add_seen_at = None
        hedge_seen_at = None
        try:
            cid_add = f"{prefix}_{tok}_ADD{int(last_n)}"
            row = self.store.query_one(
                """
                SELECT MAX(updated_at)
                  FROM public.orders
                 WHERE exchange_id=%(ex)s
                   AND account_id=%(acc)s
                   AND symbol_id=%(symbol_id)s
                   AND client_order_id=%(cid)s
                   AND status IN ('FILLED','PARTIALLY_FILLED');
                """,
                {"ex": int(self.exchange_id), "acc": int(self.account_id), "symbol_id": int(symbol_id), "cid": cid_add},
            )
            last_add_seen_at = row[0] if row else None
        except Exception:
            last_add_seen_at = None

        cid_hedge = f"{prefix}_{tok}_HEDGE"
        try:
            row = self.store.query_one(
                """
                SELECT MAX(updated_at)
                  FROM public.algo_orders
                 WHERE exchange_id=%(ex)s
                   AND account_id=%(acc)s
                   AND symbol=%(symbol)s
                   AND client_algo_id LIKE %(cid_like)s;
                """,
                {"ex": int(self.exchange_id), "acc": int(self.account_id), "symbol": str(sym).upper(), "cid_like": f"{cid_hedge}%"},
            )
            hedge_seen_at = row[0] if row else None
        except Exception:
            hedge_seen_at = None

        must_refresh = False
        refresh_reason = None
        try:
            if last_add_seen_at is not None and (hedge_seen_at is None or hedge_seen_at < last_add_seen_at):
                must_refresh = True
                refresh_reason = "after_add"
        except Exception:
            must_refresh = False

        # Anti-churn: if we very recently placed after-last-add hedge, don't cancel/recreate it
        # even if DB snapshots lag behind. This uses hedge_cooldown_sec from config.
        try:
            cd_sec = float(getattr(self.p, "hedge_cooldown_sec", 0.0) or 0.0)
        except Exception:
            cd_sec = 0.0
        if cd_sec and cd_sec > 0 and self._pl_has_raw_meta:
            try:
                rm = pos.get("raw_meta") if isinstance(pos.get("raw_meta"), dict) else {}
                ts_s = ((rm.get("after_last_add_hedge") or {}).get("ts") or ((rm.get("live_entry") or {}).get("after_last_add_hedge") or {}).get("ts"))
                if ts_s:
                    now_ms = self._to_epoch_ms(_utc_now()) or 0
                    ts_ms = self._to_epoch_ms(ts_s) or 0
                    if now_ms and ts_ms and (now_ms - ts_ms) < int(cd_sec * 1000):
                        # don't touch the hedge too often
                        must_refresh = False
            except Exception:
                pass

        side_u = str(pos_side or "").upper()
        close_side = "SELL" if side_u == "LONG" else "BUY"
        hedge_ps = "SHORT" if side_u == "LONG" else "LONG"

        # Trigger price for the pending hedge opener.
        # Default source is the last add fill + configurable buffer.
        # If hedge reopen anchor logic is enabled and we already have a recorded
        # hedge close anchor, the pending reopen order must follow that anchor
        # instead of staying pinned to the historical last add fill.
        try:
            trigger_buf_pct = float(getattr(self.p, "hedge_trigger_buffer_pct", 0.0) or 0.0)
        except Exception:
            trigger_buf_pct = 0.0
        trigger_buf_k = max(float(trigger_buf_pct), 0.0) / 100.0

        def _persist_pending_anchor(field_name: str, field_value: float) -> None:
            try:
                if not getattr(self, "_pl_has_raw_meta", False):
                    return
                uqm = """
                UPDATE position_ledger
                SET raw_meta = jsonb_set(
                        COALESCE(raw_meta,'{}'::jsonb),
                        '{hedge}',
                        COALESCE(COALESCE(raw_meta,'{}'::jsonb)->'hedge','{}'::jsonb) || jsonb_build_object(%(field_name)s, %(field_value)s::double precision),
                        true
                    ),
                    updated_at = now()
                WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND status='OPEN';
                """
                self.store.execute(
                    uqm,
                    {
                        "field_name": str(field_name),
                        "field_value": float(field_value),
                        "ex": int(self.exchange_id),
                        "acc": int(self.account_id),
                        "pos_uid": str(pos_uid),
                    },
                )
            except Exception:
                pass
            try:
                if isinstance(pos, dict):
                    rm_local = pos.get("raw_meta")
                    if isinstance(rm_local, str):
                        import json as _json
                        rm_local = _json.loads(rm_local) if rm_local.strip() else {}
                    if not isinstance(rm_local, dict):
                        rm_local = {}
                    hedge_local = rm_local.get("hedge")
                    if not isinstance(hedge_local, dict):
                        hedge_local = {}
                    hedge_local[str(field_name)] = float(field_value)
                    rm_local["hedge"] = hedge_local
                    pos["raw_meta"] = rm_local
            except Exception:
                pass

        def _anchor_tol(anchor_value: float) -> float:
            try:
                return max(float(tick or 0.0) * 2.0, abs(float(anchor_value or 0.0)) * 0.0002, 1e-8)
            except Exception:
                return 1e-8

        def _anchor_changed(prev_anchor: float, new_anchor: float) -> bool:
            try:
                return abs(float(new_anchor or 0.0) - float(prev_anchor or 0.0)) > _anchor_tol(float(prev_anchor or new_anchor or 0.0))
            except Exception:
                return abs(float(new_anchor or 0.0) - float(prev_anchor or 0.0)) > 1e-8

        def _anchor_move_allowed(prev_anchor: float, new_anchor: float) -> bool:
            try:
                prev_f = float(prev_anchor or 0.0)
                new_f = float(new_anchor or 0.0)
            except Exception:
                return False
            if prev_f <= 0.0 or new_f <= 0.0:
                return False
            tol_local = _anchor_tol(prev_f)
            if side_u == "LONG":
                return new_f > (prev_f + tol_local)
            return new_f < (prev_f - tol_local)

        def _normalize_anchor(prev_anchor: float, candidate_anchor: float) -> float:
            try:
                prev_f = float(prev_anchor or 0.0)
                cand_f = float(candidate_anchor or 0.0)
            except Exception:
                return float(candidate_anchor or 0.0)
            if prev_f <= 0.0 or cand_f <= 0.0:
                return cand_f
            if side_u == "LONG":
                return max(prev_f, cand_f)
            return min(prev_f, cand_f)

        raw_meta = pos.get("raw_meta") if isinstance(pos, dict) else None
        hedge_last_close_px = 0.0
        hedge_reopen_anchor_px = 0.0
        hedge_open_anchor_px = 0.0
        try:
            rm = raw_meta
            if isinstance(rm, str):
                import json as _json
                rm = _json.loads(rm) if rm.strip() else {}
            if isinstance(rm, dict):
                h = rm.get("hedge") or {}
                if isinstance(h, dict):
                    hedge_last_close_px = float(h.get("last_close_px") or 0.0)
                    hedge_reopen_anchor_px = float(h.get("reopen_anchor_px") or 0.0)
                    hedge_open_anchor_px = float(h.get("open_anchor_px") or 0.0)
                    if hedge_reopen_anchor_px <= 0.0:
                        hedge_reopen_anchor_px = float(hedge_last_close_px or 0.0)
        except Exception:
            hedge_last_close_px = 0.0
            hedge_reopen_anchor_px = 0.0
            hedge_open_anchor_px = 0.0

        reopen_anchor_mode = False
        pending_anchor_mode = False
        pending_anchor_kind = ""
        pending_anchor_init = False
        pending_anchor_move_pct = 0.0
        reopen_target_price = 0.0
        anchor_refresh_reason = None
        anchor_changed_this_cycle = False
        try:
            reopen_filter_enabled = bool(getattr(self.p, "hedge_reopen_price_filter_enabled", False))
        except Exception:
            reopen_filter_enabled = False
        try:
            reopen_move_pct = float(getattr(self.p, "hedge_reopen_price_move_pct", 0.0) or 0.0) / 100.0
        except Exception:
            reopen_move_pct = 0.0
        try:
            shift_trigger_pct = float(getattr(self.p, "hedge_reopen_anchor_shift_trigger_pct", 0.0) or 0.0) / 100.0
        except Exception:
            shift_trigger_pct = 0.0
        try:
            shift_step_pct = float(getattr(self.p, "hedge_reopen_anchor_shift_step_pct", 0.0) or 0.0) / 100.0
        except Exception:
            shift_step_pct = 0.0
        try:
            explicit_anchor_order_offset_pct = float(getattr(self.p, "hedge_reopen_order_offset_pct", 0.0) or 0.0) / 100.0
        except Exception:
            explicit_anchor_order_offset_pct = 0.0
        try:
            anchor_reprice_min_pct = float(getattr(self.p, "hedge_reopen_order_min_reprice_pct", 0.0) or 0.0) / 100.0
        except Exception:
            anchor_reprice_min_pct = 0.0
        try:
            anchor_reprice_cooldown_sec = float(getattr(self.p, "hedge_reopen_reprice_cooldown_sec", 0.0) or 0.0)
        except Exception:
            anchor_reprice_cooldown_sec = 0.0

        # Early restore for first-open anchor: on later cycles there may already be a live
        # pending hedge opener on the exchange, while raw_meta in the current position snapshot
        # still lags behind. In that case we must derive the anchor from the existing order
        # BEFORE falling back to last_add_fill, otherwise OPEN_ANCHOR_INIT will repeat forever.
        try:
            if reopen_filter_enabled and hedge_open_anchor_px <= 0.0 and hedge_reopen_anchor_px <= 0.0:
                open_algos_early = self._rest_snapshot_get("open_algo_orders_all") or []
                early_found = None
                for ao in open_algos_early if isinstance(open_algos_early, list) else []:
                    try:
                        if str(ao.get("symbol") or "").upper() != str(sym).upper():
                            continue
                        otype = str(ao.get("orderType") or ao.get("type") or "").upper()
                        if otype not in {"STOP_MARKET", "STOP"}:
                            continue
                        if bool(ao.get("reduceOnly")) or bool(ao.get("closePosition")):
                            continue
                        if str(ao.get("positionSide") or "").upper() != str(hedge_ps).upper():
                            continue
                        if str(ao.get("side") or "").upper() != str(close_side).upper():
                            continue
                        trig_ao = float(ao.get("triggerPrice") or ao.get("stopPrice") or 0.0)
                        if trig_ao <= 0.0:
                            continue
                        early_found = ao
                        break
                    except Exception:
                        continue
                if early_found is not None:
                    cur_f = float(early_found.get("triggerPrice") or early_found.get("stopPrice") or 0.0)
                    _derive_move_pct = max(float(dist_pct or 0.0), 0.0) / 100.0
                    denom = (1.0 - _derive_move_pct) if side_u == "LONG" else (1.0 + _derive_move_pct)
                    if trigger_buf_k > 0.0:
                        denom *= (1.0 - trigger_buf_k) if side_u == "LONG" else (1.0 + trigger_buf_k)
                    derived_open_anchor_px = float(cur_f / denom) if denom > 1e-12 else 0.0
                    derived_open_anchor_px = _normalize_anchor(float(hedge_open_anchor_px or hedge_reopen_anchor_px or hedge_last_close_px or 0.0), derived_open_anchor_px)
                    if derived_open_anchor_px > 0.0:
                        hedge_open_anchor_px = float(derived_open_anchor_px)
                        try:
                            _persist_pending_anchor("open_anchor_px", float(derived_open_anchor_px))
                        except Exception:
                            pass
        except Exception:
            pass

        anchor_px = float(hedge_reopen_anchor_px or hedge_open_anchor_px or hedge_last_close_px or 0.0)
        if reopen_filter_enabled and anchor_px <= 0.0 and float(last_fill or 0.0) > 0.0:
            anchor_px = float(last_fill)
            hedge_open_anchor_px = float(anchor_px)
            pending_anchor_init = True
            pending_anchor_kind = "first_open"
            pending_anchor_move_pct = max(float(dist_pct or 0.0), 0.0) / 100.0
        elif reopen_filter_enabled and anchor_px > 0.0:
            if float(hedge_reopen_anchor_px or hedge_last_close_px or 0.0) > 0.0:
                pending_anchor_kind = "reopen"
                pending_anchor_move_pct = max(float(reopen_move_pct or 0.0), 0.0)
            else:
                pending_anchor_kind = "first_open"
                pending_anchor_move_pct = max(float(dist_pct or 0.0), 0.0) / 100.0

        if reopen_filter_enabled and anchor_px > 0.0:
            reopen_anchor_mode = bool(float(hedge_reopen_anchor_px or hedge_last_close_px or 0.0) > 0.0)
            pending_anchor_mode = True
            if pending_anchor_init and abs(anchor_px) > 0.0:
                _persist_pending_anchor("open_anchor_px", float(anchor_px))
                try:
                    log.info(
                        "[TL][HEDGE][OPEN_ANCHOR_INIT] %s %s initialized first-open anchor from last_add_fill: anchor=%.8f move_pct=%.4f%%",
                        str(sym).upper(),
                        str(side_u).upper(),
                        float(anchor_px),
                        float(pending_anchor_move_pct * 100.0),
                    )
                except Exception:
                    pass
            old_anchor_px = float(anchor_px)
            new_anchor_px = float(anchor_px)
            try:
                mark_anchor = float(self._get_mark_price_live(sym) or 0.0)
            except Exception:
                mark_anchor = 0.0
            if mark_anchor > 0.0 and shift_trigger_pct > 0.0 and shift_step_pct > 0.0:
                if side_u == "LONG":
                    while mark_anchor >= new_anchor_px * (1.0 + shift_trigger_pct):
                        new_anchor_px = new_anchor_px * (1.0 + shift_step_pct)
                else:
                    while mark_anchor <= new_anchor_px * (1.0 - shift_trigger_pct):
                        new_anchor_px = new_anchor_px * (1.0 - shift_step_pct)
            new_anchor_px = _normalize_anchor(old_anchor_px, new_anchor_px)
            if _anchor_move_allowed(old_anchor_px, new_anchor_px):
                _persist_pending_anchor("reopen_anchor_px" if reopen_anchor_mode else "open_anchor_px", float(new_anchor_px))
                try:
                    log.info(
                        "[TL][HEDGE][%s_ANCHOR_SHIFT] %s %s mark=%.8f old_anchor=%.8f new_anchor=%.8f trigger=%.4f%% step=%.4f%%",
                        "REOPEN" if reopen_anchor_mode else "OPEN",
                        str(sym).upper(),
                        str(side_u).upper(),
                        float(mark_anchor),
                        float(anchor_px),
                        float(new_anchor_px),
                        float(shift_trigger_pct * 100.0),
                        float(shift_step_pct * 100.0),
                    )
                except Exception:
                    pass
                anchor_refresh_reason = "anchor_shift"
                anchor_changed_this_cycle = True
            anchor_px = float(new_anchor_px)
            anchor_order_offset_pct = max(
                float(explicit_anchor_order_offset_pct or 0.0),
                float(pending_anchor_move_pct or 0.0),
            )
            if side_u == "LONG":
                reopen_target_price = anchor_px * (1.0 - max(anchor_order_offset_pct, 0.0))
                if trigger_buf_k > 0.0:
                    reopen_target_price *= (1.0 - trigger_buf_k)
            else:
                reopen_target_price = anchor_px * (1.0 + max(anchor_order_offset_pct, 0.0))
                if trigger_buf_k > 0.0:
                    reopen_target_price *= (1.0 + trigger_buf_k)
            stop_price = float(reopen_target_price)
        else:
            if side_u == "LONG":
                stop_price = last_fill * (1.0 - dist_pct / 100.0)
                if trigger_buf_k > 0.0:
                    stop_price *= (1.0 - trigger_buf_k)
            else:
                stop_price = last_fill * (1.0 + dist_pct / 100.0)
                if trigger_buf_k > 0.0:
                    stop_price *= (1.0 + trigger_buf_k)

        # Tick rounding
        tick = 0.0
        try:
            tick = float(self._price_tick_for_symbol(sym) or 0.0)
        except Exception:
            tick = 0.0
        if tick and tick > 0:
            stop_price = float(_round_price_to_tick(stop_price, tick, mode=("down" if close_side == "SELL" else "up")))

        # Avoid immediate trigger wrt mark
        mark = 0.0
        try:
            pr = self._rest_snapshot_get("position_risk") or []
            for r in pr if isinstance(pr, list) else []:
                if str(r.get("symbol") or "").upper() != str(sym).upper():
                    continue
                ps = str(r.get("positionSide") or "").upper()
                if ps and ps in {"LONG", "SHORT"} and ps != side_u:
                    continue
                mark = float(r.get("markPrice") or 0.0)
                if mark > 0:
                    break
        except Exception:
            mark = 0.0
        if mark > 0 and tick and tick > 0:
            try:
                hedge_open_buf = float(getattr(self.p, "hedge_trigger_buffer_pct", 0.30) or 0.30)
            except Exception:
                hedge_open_buf = 0.20
            if close_side == "BUY":
                stop_price = float(self._safe_price_above_mark(stop_price, mark, tick, hedge_open_buf))
            else:
                stop_price = float(self._safe_price_below_mark(stop_price, mark, tick, hedge_open_buf))

        # Quantity: must be based on exchange position size
        qty_main = 0.0
        try:
            pr = self._rest_snapshot_get("position_risk") or []
            for r in pr if isinstance(pr, list) else []:
                if str(r.get("symbol") or "").upper() != str(sym).upper():
                    continue
                ps = str(r.get("positionSide") or "").upper()
                if ps in {"LONG", "SHORT"} and ps != side_u:
                    continue
                amt = float(r.get("positionAmt") or 0.0)
                if abs(amt) > 0:
                    qty_main = abs(float(amt))
                    break
        except Exception:
            qty_main = 0.0
        if qty_main <= 0:
            qty_main = _safe_float(pos.get("qty_current") or 0.0, 0.0)
        if qty_main <= 0:
            return False

        try:
            ref_px_main = float(pos.get('avg_price') or pos.get('entry_price') or last_fill or 0.0)
        except Exception:
            ref_px_main = float(last_fill or 0.0)
        try:
            mark_px_main = float(self._get_mark_price_live(sym) or 0.0)
        except Exception:
            mark_px_main = 0.0
        hedge_qty, hedge_koff, funding_pct = self._compute_live_hedge_qty_with_funding(
            symbol=str(sym).upper(),
            main_side=str(side_u).upper(),
            main_qty=float(qty_main),
            ref_price=float(ref_px_main),
            mark_price=float(mark_px_main),
        )

        # qty step rounding
        try:
            qty_step = float(self._qty_step_for_symbol(sym) or 0.0)
        except Exception:
            qty_step = 0.0
        if qty_step and qty_step > 0:
            hedge_qty = float(_round_qty_to_step(hedge_qty, qty_step, mode="down"))
        if hedge_qty <= 0:
            return False

        try:
            hedge_guard_sec = float(getattr(self.p, "hedge_reissue_cooldown_sec", 120) or 120)
        except Exception:
            hedge_guard_sec = 120.0
        _hedge_payload = {
            "symbol": str(sym).upper(),
            "position_side": str(hedge_ps).upper(),
            "side": str(close_side).upper(),
            "stop": round(float(stop_price), 12),
            "qty": round(float(hedge_qty), 12),
        }
        if self._memo_recent_desired_order("_recent_hedge_desired", cid_hedge, _hedge_payload, hedge_guard_sec):
            return False

        # Find open algos by clientAlgoId
        open_algos = self._rest_snapshot_get("open_algo_orders_all") or []
        found_hedge = None
        found_legacy = None
        legacy_cid = f"{prefix}_{tok}_TRL"
        for ao in open_algos if isinstance(open_algos, list) else []:
            try:
                if str(ao.get("symbol") or "").upper() != str(sym).upper():
                    continue
                cid = str(ao.get("clientAlgoId") or "")
                otype = str(ao.get("orderType") or ao.get("type") or "").upper()
                ps = str(ao.get("positionSide") or "").upper()
                side_ao = str(ao.get("side") or "").upper()
                reduce_only_ao = bool(ao.get("reduceOnly"))
                close_pos_ao = bool(ao.get("closePosition"))
                qty_ao = float(ao.get("quantity") or ao.get("origQty") or 0.0)
                trig_ao = float(ao.get("triggerPrice") or ao.get("stopPrice") or 0.0)
                if cid == cid_hedge:
                    found_hedge = ao
                if cid == legacy_cid and otype in {"STOP_MARKET", "STOP"}:
                    found_legacy = ao
                # Semantic match for the pending hedge opener.
                # It must belong to the hedge side, use the same opening side, and MUST NOT
                # be a reduce-only / close-position main protective order.
                if found_hedge is None and otype in {"STOP_MARKET", "STOP"}:
                    if ps != str(hedge_ps).upper():
                        continue
                    if side_ao != str(close_side).upper():
                        continue
                    if reduce_only_ao or close_pos_ao:
                        continue
                    qty_tol = max(float(qty_step or 0.0) * 2.0, abs(float(hedge_qty or 0.0)) * 0.001, 1e-8)
                    trig_tol = max(float(tick or 0.0) * 2.0, abs(float(stop_price or 0.0)) * 0.0002, 1e-8)
                    if hedge_qty > 0 and qty_ao > 0 and abs(qty_ao - float(hedge_qty)) > qty_tol:
                        continue
                    if stop_price > 0 and trig_ao > 0 and abs(trig_ao - float(stop_price)) > trig_tol:
                        continue
                    found_hedge = ao
            except Exception:
                continue
        if found_hedge is None:
            try:
                row = self.store.query_one(
                    """
                    SELECT client_algo_id, trigger_price, quantity, updated_at
                      FROM public.algo_orders
                     WHERE exchange_id=%(ex)s AND account_id=%(acc)s
                       AND upper(symbol)=%(sym)s
                       AND upper(COALESCE(position_side,'')) = %(ps)s
                       AND upper(COALESCE(side,'')) = %(side)s
                       AND upper(type) IN ('STOP_MARKET','STOP')
                       AND COALESCE(reduce_only, FALSE) = FALSE
                       AND status='OPEN'
                     ORDER BY updated_at DESC
                     LIMIT 1;
                    """,
                    {
                        "ex": int(self.exchange_id),
                        "acc": int(self.account_id),
                        "sym": str(sym).upper(),
                        "ps": str(hedge_ps).upper(),
                        "side": str(close_side).upper(),
                    },
                )
                if row and row[0]:
                    stale_cid = str(row[0] or "")
                    stale_trigger = float(row[1] or 0.0)
                    stale_qty = float(row[2] or 0.0)
                    stale_updated_at = row[3] if len(row) > 3 else None
                    recent_db_hint = False
                    try:
                        recent_hint_sec = float(getattr(self.p, "hedge_db_open_fallback_grace_sec", 15.0) or 15.0)
                    except Exception:
                        recent_hint_sec = 15.0
                    try:
                        now_ms = int(time.time() * 1000)
                        upd_ms = self._to_epoch_ms(stale_updated_at) or 0
                        recent_db_hint = bool(upd_ms and now_ms > 0 and (now_ms - upd_ms) <= int(max(recent_hint_sec, 0.0) * 1000.0))
                    except Exception:
                        recent_db_hint = False

                    if recent_db_hint:
                        found_hedge = {
                            "clientAlgoId": stale_cid,
                            "triggerPrice": float(stale_trigger),
                            "quantity": float(stale_qty),
                        }
                    else:
                        try:
                            self.store.set_algo_order_status(
                                exchange_id=int(self.exchange_id),
                                account_id=int(self.account_id),
                                client_algo_id=str(stale_cid),
                                status="CANCELED",
                                raw_json={
                                    "cancel_reason": "missing_on_exchange",
                                    "cancel_source": "exchange_snapshot_reconcile",
                                    "symbol": str(sym).upper(),
                                    "position_side": str(hedge_ps).upper(),
                                    "trigger_price": float(stale_trigger),
                                    "quantity": float(stale_qty),
                                },
                            )
                        except Exception:
                            pass
                        try:
                            log.info(
                                "[TL][HEDGE] ignore stale DB pending hedge %s %s cid=%s trigger=%.8f qty=%.8f reason=missing_on_exchange",
                                str(sym).upper(),
                                str(side_u).upper(),
                                str(stale_cid),
                                float(stale_trigger),
                                float(stale_qty),
                            )
                        except Exception:
                            pass
            except Exception:
                pass

        # Recovery path for restarts: if reopen filter is enabled but raw_meta has no saved
        # anchor yet, derive it from an already existing pending hedge reopen order.
        # This lets the bot continue trailing the reopen anchor instead of falling back to
        # last_add_fill forever after a restart/recovery.
        if reopen_filter_enabled and anchor_px <= 0.0 and found_hedge is not None:
            try:
                cur = found_hedge.get("triggerPrice") or found_hedge.get("stopPrice")
                cur_f = float(cur) if cur is not None else 0.0
            except Exception:
                cur_f = 0.0
            if cur_f > 0.0:
                try:
                    denom = 1.0
                    _derive_move_pct = max(float(reopen_move_pct or 0.0), 0.0)
                    if hedge_reopen_anchor_px <= 0.0 and hedge_last_close_px <= 0.0:
                        _derive_move_pct = max(float(dist_pct or 0.0), 0.0) / 100.0
                    if side_u == "LONG":
                        denom = (1.0 - _derive_move_pct) * (1.0 - max(trigger_buf_k, 0.0))
                    else:
                        denom = (1.0 + _derive_move_pct) * (1.0 + max(trigger_buf_k, 0.0))
                    derived_anchor_px = float(cur_f / denom) if denom > 1e-12 else 0.0
                except Exception:
                    derived_anchor_px = 0.0
                derived_anchor_px = _normalize_anchor(float(hedge_reopen_anchor_px or hedge_open_anchor_px or hedge_last_close_px or 0.0), derived_anchor_px)
                if derived_anchor_px > 0.0:
                    anchor_px = float(derived_anchor_px)
                    restored_reopen = bool(float(hedge_last_close_px or hedge_reopen_anchor_px or 0.0) > 0.0)
                    if restored_reopen:
                        hedge_last_close_px = float(hedge_last_close_px or derived_anchor_px)
                        hedge_reopen_anchor_px = float(derived_anchor_px)
                        reopen_anchor_mode = True
                        pending_anchor_kind = "reopen"
                        pending_anchor_move_pct = max(float(reopen_move_pct or 0.0), 0.0)
                    else:
                        hedge_open_anchor_px = float(derived_anchor_px)
                        reopen_anchor_mode = False
                        pending_anchor_kind = "first_open"
                        pending_anchor_move_pct = max(float(dist_pct or 0.0), 0.0) / 100.0
                    pending_anchor_mode = True
                    anchor_refresh_reason = anchor_refresh_reason or "anchor_restored_from_pending_order"
                    try:
                        if getattr(self, "_pl_has_raw_meta", False):
                            if restored_reopen:
                                uqm = """
                                UPDATE position_ledger
                                SET raw_meta = jsonb_set(
                                        jsonb_set(
                                            COALESCE(raw_meta,'{}'::jsonb),
                                            '{hedge,reopen_anchor_px}',
                                            to_jsonb(%(anchor_px)s::double precision),
                                            true
                                        ),
                                        '{hedge,last_close_px}',
                                        to_jsonb(%(last_close_px)s::double precision),
                                        true
                                    ),
                                    updated_at = now()
                                WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND status='OPEN';
                                """
                                params = {
                                    "anchor_px": float(derived_anchor_px),
                                    "last_close_px": float(hedge_last_close_px or derived_anchor_px),
                                    "ex": int(self.exchange_id),
                                    "acc": int(self.account_id),
                                    "pos_uid": str(pos_uid),
                                }
                            else:
                                uqm = """
                                UPDATE position_ledger
                                SET raw_meta = jsonb_set(
                                        COALESCE(raw_meta,'{}'::jsonb),
                                        '{hedge,open_anchor_px}',
                                        to_jsonb(%(anchor_px)s::double precision),
                                        true
                                    ),
                                    updated_at = now()
                                WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND status='OPEN';
                                """
                                params = {
                                    "anchor_px": float(derived_anchor_px),
                                    "ex": int(self.exchange_id),
                                    "acc": int(self.account_id),
                                    "pos_uid": str(pos_uid),
                                }
                            self.store.execute(uqm, params)
                    except Exception:
                        pass
                    try:
                        log.info(
                            "[TL][HEDGE][%s_ANCHOR_INIT] %s %s restored anchor from pending hedge order: trigger=%.8f -> anchor=%.8f move_pct=%.4f%% trigger_buf=%.4f%% cid=%s",
                            "REOPEN" if restored_reopen else "OPEN",
                            str(sym).upper(),
                            str(side_u).upper(),
                            float(cur_f),
                            float(derived_anchor_px),
                            float(pending_anchor_move_pct * 100.0),
                            float(trigger_buf_k * 100.0),
                            str(found_hedge.get('clientAlgoId') or ''),
                        )
                    except Exception:
                        pass
                    old_anchor_px = float(anchor_px)
                    new_anchor_px = float(anchor_px)
                    try:
                        mark_anchor = float(self._get_mark_price_live(sym) or 0.0)
                    except Exception:
                        mark_anchor = 0.0
                    if mark_anchor > 0.0 and shift_trigger_pct > 0.0 and shift_step_pct > 0.0:
                        if side_u == "LONG":
                            while mark_anchor >= new_anchor_px * (1.0 + shift_trigger_pct):
                                new_anchor_px = new_anchor_px * (1.0 + shift_step_pct)
                        else:
                            while mark_anchor <= new_anchor_px * (1.0 - shift_trigger_pct):
                                new_anchor_px = new_anchor_px * (1.0 - shift_step_pct)
                    new_anchor_px = _normalize_anchor(old_anchor_px, new_anchor_px)
                    if _anchor_move_allowed(old_anchor_px, new_anchor_px):
                        anchor_px = float(new_anchor_px)
                        hedge_reopen_anchor_px = float(new_anchor_px)
                        try:
                            _persist_pending_anchor("reopen_anchor_px" if restored_reopen else "open_anchor_px", float(new_anchor_px))
                        except Exception:
                            pass
                        anchor_changed_this_cycle = True
                        try:
                            log.info(
                                "[TL][HEDGE][REOPEN_ANCHOR_SHIFT] %s %s mark=%.8f old_anchor=%.8f new_anchor=%.8f trigger=%.4f%% step=%.4f%%",
                                str(sym).upper(),
                                str(side_u).upper(),
                                float(mark_anchor),
                                float(derived_anchor_px),
                                float(new_anchor_px),
                                float(shift_trigger_pct * 100.0),
                                float(shift_step_pct * 100.0),
                            )
                        except Exception:
                            pass
                    # Keep the current exchange trigger for this cycle; subsequent cycles will
                    # compute desired stop_price from the restored anchor and refresh the order only
                    # if the shifted anchor truly implies a different trigger.
                    reopen_target_price = float(cur_f)
                    stop_price = float(cur_f)

        # If hedge POSITION is already open on the exchange (opposite positionSide has non-zero amt),
        # we must NOT place a pending hedge opener again.
        hedge_ps = "SHORT" if side_u == "LONG" else "LONG"
        hedge_amt = 0.0
        try:
            pr = self._rest_snapshot_get("position_risk") or []
            for r in pr if isinstance(pr, list) else []:
                if str(r.get("symbol") or "").upper() != str(sym).upper():
                    continue
                if str(r.get("positionSide") or "").upper() != hedge_ps:
                    continue
                amt = float(r.get("positionAmt") or 0.0)
                if abs(amt) > 0:
                    hedge_amt = abs(float(amt))
                    break
        except Exception:
            hedge_amt = 0.0

        if hedge_amt > 0:
            # Best-effort: cancel pending hedge opener if it exists.
            if found_hedge is not None:
                try:
                    resp_cancel = self._binance.cancel_algo_order(symbol=sym, clientAlgoId=cid_hedge)
                    try:
                        rawj = resp_cancel if isinstance(resp_cancel, dict) else {"result": resp_cancel}
                        if isinstance(rawj, dict):
                            rawj.setdefault("cancel_reason", "hedge_already_open")
                            rawj.setdefault("cancel_source", "bot_rule")
                        self.store.set_algo_order_status(
                            exchange_id=int(self.exchange_id),
                            account_id=int(self.account_id),
                            client_algo_id=str(cid_hedge),
                            status="CANCELED",
                            raw_json=rawj,
                        )
                    except Exception:
                        pass
                except Exception:
                    pass
            return False

        # If hedge exists and trigger already matches, keep it (ignore must_refresh).
        # DB may lag behind exchange; we prefer exchange snapshot here.
        if found_hedge is not None:
            try:
                cur = found_hedge.get("triggerPrice") or found_hedge.get("stopPrice")
                cur_f = float(cur) if cur is not None else 0.0
            except Exception:
                cur_f = 0.0

            # Read main uPnL from positionRisk (main side only)
            main_upnl = 0.0
            try:
                pr = self._rest_snapshot_get("position_risk") or []
                for r in pr if isinstance(pr, list) else []:
                    if str(r.get("symbol") or "").upper() != str(sym).upper():
                        continue
                    if str(r.get("positionSide") or "").upper() != side_u:
                        continue
                    main_upnl = float(r.get("unRealizedProfit") or 0.0)
                    break
            except Exception:
                main_upnl = 0.0

            # If main is already in profit -> pending hedge opener is not needed (optional cancel)
            cancel_when_profit = False
            if cancel_when_profit and main_upnl > 0:
                try:
                    resp_cancel = self._binance.cancel_algo_order(symbol=sym, clientAlgoId=cid_hedge)
                    try:
                        rawj = resp_cancel if isinstance(resp_cancel, dict) else {"result": resp_cancel}
                        if isinstance(rawj, dict):
                            rawj.setdefault("cancel_reason", "main_profit")
                            rawj.setdefault("cancel_source", "bot_rule")
                        self.store.set_algo_order_status(
                            exchange_id=int(self.exchange_id),
                            account_id=int(self.account_id),
                            client_algo_id=str(cid_hedge),
                            status="CANCELED",
                            raw_json=rawj,
                        )
                    except Exception:
                        pass
                    log.info(
                        "[TL][HEDGE][FOLLOW] canceled pending after-last-add hedge for %s %s: main_upnl=%.6f > 0",
                        sym,
                        side_u,
                        main_upnl,
                    )
                except Exception:
                    pass
                return True

            # Follow / reprice: if price moved away from current trigger too much, pull trigger closer to mark
            follow_enabled = False
            try:
                follow_dist = 0.0
            except Exception:
                follow_dist = 0.0
            try:
                follow_pull = 0.0
            except Exception:
                follow_pull = 0.0
            try:
                follow_min_move = 0.0
            except Exception:
                follow_min_move = 0.0
            try:
                follow_cd_sec = 0.0
            except Exception:
                follow_cd_sec = 0.0

            if follow_enabled and follow_dist > 0 and follow_pull > 0 and mark > 0 and cur_f > 0 and main_upnl <= 0:
                # cooldown (stored in raw_meta)
                if follow_cd_sec and follow_cd_sec > 0 and self._pl_has_raw_meta:
                    try:
                        rm = pos.get("raw_meta") if isinstance(pos.get("raw_meta"), dict) else {}
                        ts_s = ((rm.get("after_last_add_hedge") or {}).get("follow_ts") or ((rm.get("live_entry") or {}).get("after_last_add_hedge") or {}).get("follow_ts"))
                        if ts_s:
                            now_ms = self._to_epoch_ms(_utc_now()) or 0
                            ts_ms = self._to_epoch_ms(ts_s) or 0
                            if now_ms and ts_ms and (now_ms - ts_ms) < int(follow_cd_sec * 1000):
                                follow_enabled = False
                    except Exception:
                        pass

                if follow_enabled:
                    try:
                        if close_side == "SELL":
                            # main LONG -> hedge SHORT -> trigger below mark
                            dist_pct_now = (mark - cur_f) / max(mark, 1e-12) * 100.0
                            new_trigger = mark * (1.0 - follow_pull / 100.0)
                        else:
                            # main SHORT -> hedge LONG -> trigger above mark
                            dist_pct_now = (cur_f - mark) / max(mark, 1e-12) * 100.0
                            new_trigger = mark * (1.0 + follow_pull / 100.0)

                        # round to tick
                        if tick and tick > 0:
                            new_trigger = float(
                                _round_price_to_tick(new_trigger, tick, mode=("down" if close_side == "SELL" else "up"))
                            )

                        move_pct = abs(new_trigger - cur_f) / max(mark, 1e-12) * 100.0
                        if dist_pct_now >= follow_dist and (follow_min_move <= 0 or move_pct >= follow_min_move):
                            try:
                                if not hasattr(self, "_recent_hedge_follow_state") or not isinstance(self._recent_hedge_follow_state, dict):
                                    self._recent_hedge_follow_state = {}
                                _f_key = str(cid_hedge)
                                _f_prev = self._recent_hedge_follow_state.get(_f_key) or {}
                                _f_prev_stop = float(_f_prev.get("stop") or 0.0)
                                _f_prev_ts = float(_f_prev.get("ts") or 0.0)
                                _f_cd = float(getattr(self.p, "hedge_follow_guard_sec", 180) or 180)
                                _f_tol = max(float(tick or 0.0) * 2.0, abs(float(new_trigger)) * 0.0002, 1e-8)
                                if _f_prev_ts and (time.time() - _f_prev_ts) < _f_cd and abs(_f_prev_stop - float(new_trigger)) <= _f_tol:
                                    return False
                            except Exception:
                                pass
                            log.info(
                                "[TL][HEDGE][FOLLOW] reprice after-last-add hedge %s %s: mark=%.8f old=%.8f dist=%.4f%% -> new=%.8f (thr=%.4f%% pull=%.4f%%)",
                                sym,
                                side_u,
                                mark,
                                cur_f,
                                dist_pct_now,
                                new_trigger,
                                follow_dist,
                                follow_pull,
                            )
                            stop_price = float(new_trigger)
                            must_refresh = True
                            refresh_reason = "reprice"
                            try:
                                self._recent_hedge_follow_state[str(cid_hedge)] = {"stop": float(new_trigger), "ts": time.time()}
                            except Exception:
                                pass
                        else:
                            # if trigger already close enough to desired stop_price and no refresh requested -> keep
                            tol = max(float(tick or 0.0) * 2.0, abs(stop_price) * 0.0002, 1e-8)
                            if cur_f > 0 and abs(cur_f - float(stop_price)) <= tol and not must_refresh:
                                return False
                    except Exception:
                        pass
            else:
                # Default: if trigger already matches, keep it.
                # For hedge reopen anchor mode, reprice the pending order when the
                # desired trigger moved together with the anchor.
                try:
                    tol = max(float(tick or 0.0) * 2.0, abs(stop_price) * 0.0002, 1e-8)
                    if cur_f > 0 and abs(cur_f - float(stop_price)) <= tol:
                        return False
                    if cur_f > 0 and pending_anchor_mode and abs(cur_f - float(stop_price)) > tol:
                        if not anchor_changed_this_cycle:
                            return False
                        anchor_delta_rel = abs(float(cur_f) - float(stop_price)) / max(abs(float(cur_f)), 1e-12)
                        if anchor_reprice_min_pct > 0.0 and anchor_delta_rel < anchor_reprice_min_pct:
                            return False
                        if anchor_reprice_cooldown_sec > 0.0:
                            try:
                                if not hasattr(self, "_recent_anchor_hedge_reprice_state") or not isinstance(self._recent_anchor_hedge_reprice_state, dict):
                                    self._recent_anchor_hedge_reprice_state = {}
                                _anchor_key = str(cid_hedge)
                                _st = self._recent_anchor_hedge_reprice_state.get(_anchor_key) or {}
                                _last_ts = float(_st.get("ts") or 0.0)
                                _last_stop = float(_st.get("stop") or 0.0)
                                if _last_ts and (time.time() - _last_ts) < anchor_reprice_cooldown_sec and abs(_last_stop - float(stop_price)) <= tol:
                                    return False
                            except Exception:
                                pass
                        must_refresh = True
                        refresh_reason = refresh_reason or anchor_refresh_reason or "anchor_reprice"
                        try:
                            log.info(
                                "[TL][HEDGE][%s_ANCHOR] refresh pending hedge %s %s: current=%.8f -> desired=%.8f anchor=%.8f move_pct=%.4f%% reason=%s",
                                "REOPEN" if reopen_anchor_mode else "OPEN",
                                str(sym).upper(),
                                str(side_u).upper(),
                                float(cur_f),
                                float(stop_price),
                                float(anchor_px if pending_anchor_mode else 0.0),
                                float((max(float(explicit_anchor_order_offset_pct or 0.0), float(pending_anchor_move_pct or 0.0))) * 100.0),
                                str(refresh_reason),
                            )
                        except Exception:
                            pass
                    if cur_f > 0 and not must_refresh:
                        return False
                except Exception:
                    pass

        # Cancel existing hedge (best-effort) only if we really want to refresh it.
        if must_refresh and found_hedge is not None:
            for cid in (cid_hedge,):
                try:
                    resp_cancel = self._binance.cancel_algo_order(symbol=sym, clientAlgoId=cid)
                    try:
                        rawj = resp_cancel if isinstance(resp_cancel, dict) else {"result": resp_cancel}
                        if isinstance(rawj, dict):
                            rawj.setdefault("cancel_reason", refresh_reason or "refresh")
                            rawj.setdefault("cancel_source", "bot_refresh")
                        self.store.set_algo_order_status(
                            exchange_id=int(self.exchange_id),
                            account_id=int(self.account_id),
                            client_algo_id=str(cid),
                            status="CANCELED",
                            raw_json=rawj,
                        )
                    except Exception:
                        pass
                except Exception:
                    try:
                        self._binance.cancel_order(symbol=sym, origClientOrderId=cid)
                    except Exception:
                        pass

        # Cancel legacy hedge stored under _TRL if it's STOP_MARKET
        if found_legacy is not None:
            try:
                resp_cancel = self._binance.cancel_algo_order(symbol=sym, clientAlgoId=legacy_cid)
                try:
                    rawj = resp_cancel if isinstance(resp_cancel, dict) else {"result": resp_cancel}
                    if isinstance(rawj, dict):
                        rawj.setdefault("cancel_reason", "legacy_cleanup")
                        rawj.setdefault("cancel_source", "bot_refresh")
                    self.store.set_algo_order_status(
                        exchange_id=int(self.exchange_id),
                        account_id=int(self.account_id),
                        client_algo_id=str(legacy_cid),
                        status="CANCELED",
                        raw_json=rawj,
                    )
                except Exception:
                    pass
            except Exception:
                pass

        # Hedge position side is opposite
        # (already computed above and reused for semantic exchange/DB matching)

        try:
            try:
                if not hasattr(self, "_recent_hedge_place_state") or not isinstance(self._recent_hedge_place_state, dict):
                    self._recent_hedge_place_state = {}
                _hedge_guard_sec = float(getattr(self.p, "hedge_place_guard_sec", 180) or 180)
                _hedge_key = str(cid_hedge)
                _hedge_prev = self._recent_hedge_place_state.get(_hedge_key) or {}
                _prev_stop = float(_hedge_prev.get("stop") or 0.0)
                _prev_ts = float(_hedge_prev.get("ts") or 0.0)
                _tol = max(float(tick or 0.0) * 2.0, abs(float(stop_price)) * 0.0002, 1e-8)
                if _prev_ts and (time.time() - _prev_ts) < _hedge_guard_sec and abs(_prev_stop - float(stop_price)) <= _tol:
                    return False
            except Exception:
                pass

            place_cid = str(cid_hedge)
            try:
                if pending_anchor_mode:
                    if not hasattr(self, "_recent_anchor_hedge_reprice_state") or not isinstance(self._recent_anchor_hedge_reprice_state, dict):
                        self._recent_anchor_hedge_reprice_state = {}
                    self._recent_anchor_hedge_reprice_state[str(cid_hedge)] = {"ts": float(time.time()), "stop": float(stop_price)}
            except Exception:
                pass
            # Binance can permanently reserve a clientAlgoId after a cancel/reissue cycle.
            # If that happens, using the canonical *_HEDGE id again yields -4116 but no live
            # order appears on exchange. Retry with a short unique suffix and rely on semantic
            # matching + DB LIKE queries for subsequent detection.
            try:
                suffix = format(int(time.time() * 1000) % 65536, '04x')
                alt_cid = f"{cid_hedge}_{suffix}"
                if len(alt_cid) > 36:
                    alt_cid = alt_cid[:36]
            except Exception:
                alt_cid = None

            def _place_once(client_id: str):
                return self._binance.new_order(
                    symbol=sym,
                    side=close_side,
                    type="STOP_MARKET",
                    stopPrice=float(stop_price),
                    quantity=float(hedge_qty),
                    workingType=str(getattr(self.p, "working_type", "MARK_PRICE")),
                    newClientOrderId=client_id,
                    positionSide=hedge_ps,
                )

            try:
                resp = _place_once(place_cid)
            except Exception as e:
                msg = str(e)
                if alt_cid and ("-4116" in msg or "ClientOrderId is duplicated" in msg):
                    place_cid = alt_cid
                    resp = _place_once(place_cid)
                else:
                    raise
            _dup_resp = bool(isinstance(resp, dict) and resp.get("_duplicate"))

            # persist algo hedge order (best-effort)
            try:
                self.store.upsert_algo_orders(
                    [
                        {
                            "exchange_id": int(self.exchange_id),
                            "account_id": int(self.account_id),
                            "client_algo_id": str(place_cid),
                            "algo_id": None if resp is None else str(resp.get("algoId") or resp.get("algoOrderId") or resp.get("orderId") or ""),
                            "symbol": str(sym).upper(),
                            "side": str(close_side),
                            "position_side": str(hedge_ps),
                            "type": "STOP_MARKET",
                            "quantity": float(hedge_qty),
                            "trigger_price": float(stop_price),
                            "working_type": str(getattr(self.p, "working_type", "MARK_PRICE")),
                            "status": "OPEN",
                            "strategy_id": str(self.strategy_id),
                            "pos_uid": str(pos_uid),
                            "raw_json": (resp if isinstance(resp, dict) else {"result": resp, "canonical_client_algo_id": str(cid_hedge)}),
                        }
                    ]
                )
            except Exception:
                pass
        except Exception:
            return False

        # store meta flag (best-effort)
        try:
            if self._pl_has_raw_meta:
                meta = {
                    "after_last_add_hedge": {
                        "add_n": int(last_n),
                        "fill": float(last_fill),
                        "stop": float(stop_price),
                        "pct": float(dist_pct),
                        "qty_main": float(qty_main),
                        "qty_hedge": float(hedge_qty),
                        "client_algo_id": str(place_cid),
                        "canonical_client_algo_id": str(cid_hedge),
                        "ts": _utc_now().isoformat(),
                    }
                }
                self.store.execute(
                    """
                    UPDATE public.position_ledger
                       SET raw_meta = COALESCE(raw_meta, '{}'::jsonb) || %(meta)s::jsonb
                     WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND strategy_id=%(st)s
                       AND symbol_id=%(symbol_id)s AND side=%(side)s AND source='live'
                       AND updated_at = (
                           SELECT MAX(updated_at) FROM public.position_ledger
                            WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND strategy_id=%(st)s
                              AND symbol_id=%(symbol_id)s AND side=%(side)s AND source='live'
                       );
                    """,
                    {
                        "meta": json.dumps(meta),
                        "ex": int(self.exchange_id),
                        "acc": int(self.account_id),
                        "st": str(self.strategy_id),
                        "symbol_id": int(symbol_id),
                        "side": str(side_u),
                    },
                )
        except Exception:
            pass

        try:
            if not hasattr(self, "_recent_hedge_place_state") or not isinstance(self._recent_hedge_place_state, dict):
                self._recent_hedge_place_state = {}
            self._recent_hedge_place_state[str(cid_hedge)] = {"stop": float(stop_price), "ts": time.time()}
            self._recent_hedge_place_state[str(place_cid)] = {"stop": float(stop_price), "ts": time.time()}
        except Exception:
            pass

        if _dup_resp:
            return False
        self.log.info(
            "[trade_liquidation][LIVE][HEDGE] placed after-last-add STOP_MARKET %s %s stop=%.8f (source=%s anchor=%.8f reopen_target=%.8f last_add=%.8f add_n=%s pct=%.4f qty=%.8f koff=%.4f funding_pct=%.4f)",
            str(sym).upper(),
            str(side_u),
            float(stop_price),
            ("reopen_anchor" if reopen_anchor_mode else ("open_anchor" if pending_anchor_mode else "last_add_fill")),
            float(anchor_px if pending_anchor_mode else 0.0),
            float(reopen_target_price if pending_anchor_mode else 0.0),
            float(last_fill),
            str(last_n),
            float(dist_pct),
            float(hedge_qty),
            float(hedge_koff),
            float(funding_pct),
        )
        return True

    def _live_hedge_manage(self) -> Dict[str, int]:
        """Manage hedge positions in LIVE mode when hedge_enabled is True.

        Rule:
          - When the final protection level is reached, open an opposite market hedge sized
                hedge_qty = abs(main_qty) * hedge_koff
            with funding-aware sizing if configured.
          - After hedge opens, manage only hedge trailing, hedge stop-loss, break-even move and reopen logic.
        """
        if (not self._is_live) or self._binance is None:
            return {"checked": 0, "opened": 0, "closed": 0, "skipped": 0}

        if not bool(getattr(self.p, "hedge_enabled", False)):
            return {"checked": 0, "opened": 0, "closed": 0, "skipped": 0}

        hedge_koff = float(getattr(self.p, "hedge_koff", 1.2) or 1.2)

        # Exchange snapshot positions (positionRisk)
        pr = self._rest_snapshot_get("position_risk")
        pr_rows = pr if isinstance(pr, list) else []
        pr_map = {}  # (symbol, positionSide)-> dict
        for r in pr_rows:
            sym = str(r.get("symbol") or "").strip()
            ps = str(r.get("positionSide") or ("BOTH" if not bool(getattr(self.p,"hedge_enabled",False)) else "")).strip().upper()
            if not sym:
                continue
            pr_map[(sym, ps)] = r

        # Ledger OPEN positions for strategy
        led = []
        # NOTE: position_ledger schema can differ between deployments.
        # We only need basic fields + optional raw_meta (for hedge state).
        try:
            sel = "symbol_id, pos_uid, side, status, qty_current, avg_price, entry_price, COALESCE(source,'live') AS source"
            if getattr(self, "_pl_has_raw_meta", False):
                sel += ", raw_meta"
            led = list(
                self.store.query_dict(
                    f"""
                    SELECT {sel}
                    FROM position_ledger
                    WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND strategy_id=%(sid)s AND status = 'OPEN' AND COALESCE(source,'live') IN ('live','live_hedge')
                    ORDER BY opened_at DESC;
                    """,
                    {"ex": int(self.exchange_id), "acc": int(self.account_id), "sid": str(self.STRATEGY_ID)},
                )
            )
        except Exception:
            # fallback without raw_meta
            led = list(
                self.store.query_dict(
                    """
                    SELECT symbol_id, pos_uid, side, status, qty_current, avg_price, entry_price, COALESCE(source,'live') AS source
                    FROM position_ledger
                    WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND strategy_id=%(sid)s AND status = 'OPEN' AND COALESCE(source,'live') IN ('live','live_hedge')
                    ORDER BY opened_at DESC;
                    """,
                    {"ex": int(self.exchange_id), "acc": int(self.account_id), "sid": str(self.STRATEGY_ID)},
                )
            )

        checked = opened = closed = skipped = 0
        sym_map = self._symbols_map()
        _seen_main_rows = set()
        _open_live_symbol_keys = set()
        try:
            for _lp in led:
                if str(_lp.get('source') or 'live').lower().strip() != 'live':
                    continue
                _sid = int(_lp.get('symbol_id') or 0)
                _sym = (sym_map.get(_sid) or '').upper().strip()
                if _sid > 0 and _sym:
                    _open_live_symbol_keys.add((_sid, _sym))
        except Exception:
            pass

        for p in led:
            checked += 1
            symbol_id = int(p["symbol_id"])
            symbol = sym_map.get(symbol_id) or ""
            if not symbol:
                skipped += 1
                continue

            main_side = str(p.get("side") or "").upper()
            if main_side not in {"LONG","SHORT"}:
                skipped += 1
                continue
            src_kind = str(p.get("source") or "live").lower().strip()
            if src_kind == "live":
                main_key = (int(symbol_id), str(main_side).upper())
                if main_key in _seen_main_rows:
                    skipped += 1
                    continue
                _seen_main_rows.add(main_key)
            # IMPORTANT: hedge manager must be driven only by the base/main strategy rows.
            # OPEN/CLOSED live_hedge rows are auxiliary mirror records for the opposite leg.
            # If we process them here as if they were independent main positions, the loop starts
            # managing the hedge-of-a-hedge, which leads to missing or flapping HEDGE_TRL orders
            # after hedge reopen/recovery (for example ETHUSDT LONG in the user's logs).
            if src_kind == "live_hedge":
                # Auxiliary hedge mirror rows must not drive hedge-of-a-hedge logic.
                # If the original main leg has disappeared but this hedge leg survived,
                # safely re-create it as a new LIVE main row and retire the mirror row.
                promoted = False
                try:
                    promoted = bool(self._promote_surviving_hedge_to_main_safe(
                        hedge_pos=p,
                        symbol=symbol,
                        symbol_id=symbol_id,
                        pr_map=pr_map,
                        open_live_symbol_keys=_open_live_symbol_keys,
                    ))
                except Exception:
                    promoted = False
                if promoted:
                    opened += 1
                    closed += 1
                    skipped += 1
                    continue
                # If the hedge leg itself is already gone on exchange, close the stale mirror row.
                try:
                    hedge_ps_aux = str(main_side).upper()
                    hedge_pos_aux = pr_map.get((symbol, hedge_ps_aux))
                    hedge_amt_aux = 0.0
                    if hedge_pos_aux:
                        try:
                            hedge_amt_aux = abs(float(hedge_pos_aux.get("positionAmt") or 0.0))
                        except Exception:
                            hedge_amt_aux = 0.0
                    if float(hedge_amt_aux or 0.0) <= 0.0 and str(p.get("status") or "OPEN").upper() == "OPEN":
                        try:
                            self._close_position_exchange(
                                p,
                                exit_price=float(self._get_mark_price(symbol, hedge_ps_aux) or self._get_mark_price(symbol, main_side) or 0.0),
                                realized_pnl=0.0,
                                close_time_ms=None,
                                reason="stale_live_hedge_mirror",
                                timeframe="hedge_reconcile",
                            )
                        except Exception:
                            log.debug(
                                "[TL][hedge_ready] failed to auto-close stale live_hedge mirror sym=%s side=%s pos_uid=%s",
                                symbol,
                                hedge_ps_aux,
                                str(p.get("pos_uid") or ""),
                                exc_info=True,
                            )
                except Exception:
                    pass
                skipped += 1
                continue


            pl_status = str(p.get("status") or "OPEN").upper().strip()
            if pl_status not in {"OPEN", "CLOSED"}:
                pl_status = "OPEN"

            qty = abs(float(p.get("qty_current") or 0.0))
            if pl_status == "OPEN" and qty <= 0:
                skipped += 1
                continue

            entry = _safe_float(p.get("avg_price"), default=_safe_float(p.get("entry_price"), default=0.0))
            if pl_status == "OPEN" and entry <= 0:
                skipped += 1
                continue

            # Determine hedge direction. EMA validation and EMA-based exits are intentionally disabled
            # to avoid conflicts with hedge stop-loss, trailing and reopen logic.
            hedge_side = "SHORT" if main_side == "LONG" else "LONG"
            need_ema = None
            ema_dir_open = None
            ema_dir_close = None
            hedge_ok = True

            # Detect existing hedge position on exchange
            # In hedge mode we should have positionSide LONG/SHORT.
            main_ps = main_side
            hedge_ps = hedge_side
            main_pos = pr_map.get((symbol, main_ps))
            hedge_pos = pr_map.get((symbol, hedge_ps))

            hedge_amt = 0.0
            hedge_entry = 0.0
            if hedge_pos:
                try:
                    hedge_amt = abs(float(hedge_pos.get("positionAmt") or 0.0))
                except Exception:
                    hedge_amt = 0.0
                try:
                    hedge_entry = abs(float(hedge_pos.get("entryPrice") or hedge_pos.get("avgEntryPrice") or 0.0))
                except Exception:
                    hedge_entry = 0.0

            # If main is already closed in ledger, still manage hedge only if it exists on exchange
            if pl_status != "OPEN" and float(hedge_amt or 0.0) <= 0.0:
                skipped += 1
                continue

            # Main position info (for PnL controls) + hedge base qty from ledger raw_meta
            main_amt = 0.0
            main_entry_ex = 0.0
            main_upnl_ex: Optional[float] = None
            if main_pos:
                try:
                    main_amt = abs(float(main_pos.get("positionAmt") or 0.0))
                except Exception:
                    main_amt = 0.0
                try:
                    main_entry_ex = abs(float(main_pos.get("entryPrice") or main_pos.get("avgEntryPrice") or 0.0))
                except Exception:
                    main_entry_ex = 0.0
                try:
                    v = main_pos.get("unRealizedProfit")
                    if v is not None:
                        main_upnl_ex = float(v)
                except Exception:
                    main_upnl_ex = None

            hedge_upnl_ex: Optional[float] = None
            if hedge_pos:
                try:
                    v = hedge_pos.get("unRealizedProfit")
                    if v is not None:
                        hedge_upnl_ex = float(v)
                except Exception:
                    hedge_upnl_ex = None

            hedge_base_qty: Optional[float] = None
            hedge_last_action_ts: float = 0.0
            hedge_last_open_ts: float = 0.0
            hedge_last_close_ts: float = 0.0
            hedge_last_close_px: float = 0.0
            hedge_reopen_anchor_px: float = 0.0
            hedge_be_best_px: float = 0.0
            hedge_be_armed: bool = False
            raw_meta = p.get("raw_meta") if isinstance(p, dict) else None
            if raw_meta is not None:
                try:
                    rm = raw_meta
                    if isinstance(rm, str):
                        import json as _json
                        rm = _json.loads(rm) if rm.strip() else {}
                    if isinstance(rm, dict):
                        h = rm.get("hedge") or {}
                        if isinstance(h, dict):
                            if h.get("base_qty") is not None:
                                try:
                                    hedge_base_qty = float(h.get("base_qty"))
                                except Exception:
                                    hedge_base_qty = None
                            try:
                                hedge_last_action_ts = float(h.get("last_action_ts") or 0.0)
                            except Exception:
                                hedge_last_action_ts = 0.0
                            try:
                                hedge_last_open_ts = float(h.get("last_open_ts") or 0.0)
                            except Exception:
                                hedge_last_open_ts = 0.0
                            try:
                                hedge_last_close_ts = float(h.get("last_close_ts") or 0.0)
                            except Exception:
                                hedge_last_close_ts = 0.0
                            try:
                                hedge_last_close_px = float(h.get("last_close_px") or 0.0)
                            except Exception:
                                hedge_last_close_px = 0.0
                            try:
                                hedge_reopen_anchor_px = float(h.get("reopen_anchor_px") or 0.0)
                            except Exception:
                                hedge_reopen_anchor_px = 0.0
                            try:
                                hedge_be_best_px = float(h.get("be_best_px") or 0.0)
                            except Exception:
                                hedge_be_best_px = 0.0
                            try:
                                hedge_be_armed = bool(h.get("be_armed"))
                            except Exception:
                                hedge_be_armed = False
                            if hedge_reopen_anchor_px <= 0.0:
                                hedge_reopen_anchor_px = float(hedge_last_close_px or 0.0)
                except Exception:
                    hedge_base_qty = None

            mark = 0.0
            try:
                mark = float(self._get_mark_price(symbol, hedge_ps) or self._get_mark_price(symbol, main_ps) or 0.0)
            except Exception:
                mark = 0.0
            if mark <= 0:
                skipped += 1
                continue

            # Repair/ensure internal hedge binding for a live hedge leg already open on exchange.
            # Without this, HEDGE_SL / HEDGE_TRL managers may skip a perfectly real hedge position
            # just because live_hedge / hedge_links rows were not created yet.
            try:
                if float(hedge_amt or 0.0) > 0.0:
                    self._ensure_live_hedge_binding(
                        base_pos=p,
                        symbol=str(symbol).upper(),
                        symbol_id=int(symbol_id),
                        hedge_ps=str(hedge_ps).upper(),
                        hedge_amt=float(hedge_amt or 0.0),
                        hedge_entry=float(hedge_entry or 0.0),
                        mark=float(mark or 0.0),
                    )
            except Exception:
                log.exception('[TL][HEDGE_BIND] unexpected error while ensuring binding %s %s base_pos_uid=%s', str(symbol).upper(), str(hedge_ps).upper(), str(p.get('pos_uid') or ''))

            # If hedge position has just been closed externally (e.g., by HEDGE_SL/HEDGE_TRL on exchange),
            # persist last_close_ts + last_close_px into position_ledger.raw_meta so that reopen filters
            # can work after restarts.
            try:
                if float(hedge_amt or 0.0) <= 0.0 and float(hedge_last_open_ts or 0.0) > 0.0:
                    # Detect transition: we have a recorded last_open_ts newer than last_close_ts
                    # (meaning hedge was open at least once) but currently exchange hedge_amt==0.
                    if float(hedge_last_open_ts or 0.0) > float(hedge_last_close_ts or 0.0):
                        if getattr(self, "_pl_has_raw_meta", False):
                            import json as _json
                            meta = {"hedge": {"last_close_reason": "ALGO", "last_close_ts": float(time.time()), "last_close_px": float(mark), "reopen_anchor_px": float(mark), "last_action_ts": float(time.time())}}
                            uqm = '''
                            UPDATE position_ledger
                            SET raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb,
                                updated_at = now()
                            WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s
                            '''
                            self.store.execute(uqm, {"meta": _json.dumps(meta), "ex": int(self.exchange_id), "acc": int(self.account_id), "pos_uid": str(pos_uid)})
                        hedge_last_close_ts = float(time.time())
                        hedge_last_close_px = float(mark)
                        hedge_reopen_anchor_px = float(mark)
            except Exception:
                pass

            # Compute unrealized PnL (prefer exchange fields; fallback to mark/entry)
            def _calc_upnl(side: str, entry_px: float, amt: float, upnl_ex: Optional[float]) -> float:
                if upnl_ex is not None:
                    try:
                        return float(upnl_ex)
                    except Exception:
                        pass
                if entry_px <= 0 or amt <= 0:
                    return 0.0
                if side == "LONG":
                    return (mark - entry_px) * amt
                return (entry_px - mark) * amt

            main_entry_use = float(main_entry_ex or entry or 0.0)
            main_upnl = _calc_upnl(main_side, main_entry_use, float(main_amt or qty), main_upnl_ex)
            hedge_upnl = 0.0
            if hedge_amt > 0:
                hedge_entry_use = float(hedge_entry or 0.0)
                if hedge_entry_use <= 0 and hedge_pos:
                    try:
                        hedge_entry_use = abs(float(hedge_pos.get("entryPrice") or hedge_pos.get("avgEntryPrice") or 0.0))
                    except Exception:
                        hedge_entry_use = 0.0
                hedge_upnl = _calc_upnl(hedge_side, hedge_entry_use, float(hedge_amt), hedge_upnl_ex)

            combined_upnl = float(main_upnl) + float(hedge_upnl)

            try:
                if pl_status == 'OPEN' and float(hedge_amt or 0.0) > 0.0 and float(main_amt or qty or 0.0) > 0.0:
                    self._maybe_add_main_on_hedge_price_diff(
                        p=p,
                        symbol=symbol,
                        symbol_id=int(symbol_id),
                        main_side=str(main_side).upper(),
                        main_ps=str(main_ps).upper(),
                        main_amt=float(main_amt or qty or 0.0),
                        main_entry=float(main_entry_use or entry or 0.0),
                        hedge_ps=str(hedge_ps).upper(),
                        hedge_amt=float(hedge_amt or 0.0),
                        hedge_entry=float(hedge_entry_use or hedge_entry or 0.0),
                        hedge_last_open_ts=float(hedge_last_open_ts or 0.0),
                    )
            except Exception:
                pass

            # Profit optimizer for MAIN: partial TP before managing hedge exits/trl.
            try:
                if pl_status == 'OPEN' and float(main_amt or qty or 0.0) > 0.0:
                    self._live_manage_main_partial_tp(
                        p=p,
                        symbol=symbol,
                        main_side=main_side,
                        main_amt=float(main_amt or qty or 0.0),
                        entry_px=float(main_entry_use or entry or 0.0),
                        mark=float(mark),
                    )
            except Exception:
                pass

            # ------------------------------------------------------------------
            # Hedge trailing stop (reduce-only) after hedge position is OPEN
            # ------------------------------------------------------------------
            if hedge_amt > 0 and bool(getattr(self.p, "hedge_trailing_enabled", False)):
                try:
                    # Strict ownership guard:
                    # HEDGE_TRL must belong to a real OPEN hedge leg that is linked to the current
                    # base position. Without this, historical CLOSED base rows can recreate
                    # HEDGE_TRL on the main positionSide and duplicate the normal main TRL.
                    hedge_leg_owned = False
                    try:
                        row = self.store.fetch_one(
                            """
                            SELECT 1 AS ok
                            FROM position_ledger pl
                            JOIN hedge_links hl
                              ON hl.exchange_id = pl.exchange_id
                             AND hl.symbol_id = pl.symbol_id
                             AND hl.base_account_id = pl.account_id
                             AND hl.hedge_account_id = pl.account_id
                             AND hl.hedge_pos_uid = pl.pos_uid
                            WHERE pl.exchange_id = %s
                              AND pl.account_id = %s
                              AND pl.symbol_id = %s
                              AND pl.side = %s
                              AND pl.status = 'OPEN'
                              AND COALESCE(pl.source, 'live') = 'live_hedge'
                              AND hl.base_pos_uid = %s
                            LIMIT 1
                            """,
                            (
                                int(self.exchange_id),
                                int(self.account_id),
                                int(symbol_id),
                                str(hedge_ps),
                                str(p.get('pos_uid') or ''),
                            ),
                        )
                        hedge_leg_owned = bool(row)
                    except Exception:
                        hedge_leg_owned = False

                    # Fallback: if the explicit hedge_links row is not visible yet (for example,
                    # immediately after a hedge re-open was detected from exchange state), allow
                    # the dedicated HEDGE_TRL manager to work when there is any OPEN live_hedge row
                    # on the same symbol/side for this account. This prevents a reopened hedge leg
                    # from staying without its own trailing protection just because the link row was
                    # created later than the exchange position snapshot.
                    if not hedge_leg_owned:
                        try:
                            row_any = self.store.fetch_one(
                                """
                                SELECT pl.pos_uid
                                FROM position_ledger pl
                                WHERE pl.exchange_id = %s
                                  AND pl.account_id = %s
                                  AND pl.symbol_id = %s
                                  AND pl.side = %s
                                  AND pl.status = 'OPEN'
                                  AND COALESCE(pl.source, 'live') = 'live_hedge'
                                ORDER BY pl.updated_at DESC NULLS LAST, pl.opened_at DESC NULLS LAST
                                LIMIT 1
                                """,
                                (
                                    int(self.exchange_id),
                                    int(self.account_id),
                                    int(symbol_id),
                                    str(hedge_ps),
                                ),
                            )
                            hedge_leg_owned = bool(row_any)
                        except Exception:
                            pass

                    # Guard: only manage HEDGE_TRL when hedge position is really open on exchange
                    # and we have a live_hedge row linked to this exact base position.
                    if (not hedge_leg_owned) or (not self._has_open_position_live(symbol, str(hedge_ps))):
                        tok = _coid_token(str(p.get('pos_uid') or ''), n=20)
                        cid_htrl = f"TL_{tok}_HEDGE_TRL"
                        try:
                            log.info("[TL][HEDGE_TRL][DECISION] skip reason=no_open_hedge_or_binding sym=%s side=%s base_pos_uid=%s hedge_leg_owned=%s hedge_live=%s", symbol, hedge_ps, str(p.get('pos_uid') or ''), bool(hedge_leg_owned), bool(self._has_open_position_live(symbol, str(hedge_ps))))
                        except Exception:
                            pass
                        self._dlog("skip HEDGE_TRL: no open hedge position sym=%s side=%s", symbol, hedge_ps)
                        try:
                            cache = getattr(self, "_hedge_trl_local_open", None)
                            if isinstance(cache, dict):
                                cache.pop(str(cid_htrl), None)
                        except Exception:
                            pass
                        self._cancel_algo_by_client_id_safe(symbol, cid_htrl)
                        raise StopIteration()

                    act_pct = float(getattr(self.p, "hedge_trailing_activation_pct", 1.5) or 1.5)
                    trail_pct = float(getattr(self.p, "hedge_trailing_trail_pct", 0.5) or 0.5)
                    buf_pct = float(getattr(self.p, "hedge_trailing_activation_buffer_pct", 0.25) or 0.25)
                    place_retries = int(getattr(self.p, "hedge_trailing_place_retries", 3) or 3)
                    work_type = str(getattr(self.p, "hedge_trailing_working_type", "MARK_PRICE") or "MARK_PRICE").upper()
                    reissue_on_qty = bool(getattr(self.p, "hedge_trailing_reissue_on_qty_change", True))
                    reissue_cd = float(getattr(self.p, "hedge_trailing_reissue_cooldown_sec", 30) or 30)
                    qty_tol = float(getattr(self.p, "hedge_trailing_qty_tolerance", 1e-8) or 1e-8)

                    # Binance constraints
                    if trail_pct < 0.1:
                        trail_pct = 0.1
                    if trail_pct > 10.0:
                        trail_pct = 10.0

                    # Build stable clientOrderId for hedge trailing
                    tok = _coid_token(str(p.get('pos_uid') or ''), n=20)
                    cid_htrl = f"TL_{tok}_HEDGE_TRL"

                    # DB-side dedupe: openAlgoOrders may not always return our hedge TRL promptly/consistently.
                    # If we already have an OPEN record in algo_orders for this client_algo_id, do not place again
                    # unless we are explicitly reissuing on hedge_qty change.
                    db_trl = None
                    try:
                        db_trl = self.store.fetch_one(
                            """
                            SELECT client_algo_id, quantity, updated_at
                            FROM algo_orders
                            WHERE exchange_id=%s AND account_id=%s AND client_algo_id=%s AND status='OPEN'
                            ORDER BY updated_at DESC
                            LIMIT 1
                            """,
                            (int(self.exchange_id), int(self.account_id), str(cid_htrl)),
                        )
                    except Exception:
                        db_trl = None


                    # Round qty to LOT_SIZE
                    q = float(hedge_amt)
                    try:
                        step = float(self._qty_step_for_symbol(symbol) or 0.0)
                    except Exception:
                        step = 0.0
                    if step and step > 0:
                        q = _round_qty_to_step(q, step, mode="down")

                    if q > 0:

                        # Strong dedupe for hedge trailing. Binance openAlgoOrders can lag, and on some
                        # accounts the same clientAlgoId may still be accepted again. So we combine:
                        #   1) in-process recent-place guard
                        #   2) exact clientAlgoId match in all available algo snapshots
                        #   3) semantic match: symbol + positionSide + TRAILING_STOP_MARKET + *_HEDGE_TRL
                        existing = None
                        try:
                            if not hasattr(self, "_hedge_trl_place_guard_ts"):
                                self._hedge_trl_place_guard_ts = {}
                        except Exception:
                            pass

                        place_guard_sec = float(getattr(self.p, "hedge_trailing_place_guard_sec", 20) or 20)
                        recent_place_ts = 0.0
                        try:
                            recent_place_ts = float(getattr(self, "_hedge_trl_place_guard_ts", {}).get(cid_htrl, 0.0) or 0.0)
                        except Exception:
                            recent_place_ts = 0.0

                        oo_rows = []
                        try:
                            for snap_name in ("open_algo_orders_all", "open_algo_orders"):
                                snap = self._rest_snapshot_get(snap_name)
                                if isinstance(snap, list):
                                    oo_rows.extend(snap)
                        except Exception:
                            oo_rows = []

                        try:
                            want_sym = str(symbol).upper()
                            want_ps = str(hedge_ps).upper()
                            # 1) exact client id match
                            for r in oo_rows:
                                if str(r.get("symbol") or "").upper() != want_sym:
                                    continue
                                cid = str(r.get("clientAlgoId") or r.get("clientOrderId") or "").strip()
                                if cid != cid_htrl:
                                    continue
                                existing = r
                                break
                            # 2) semantic match for same live hedge leg
                            if existing is None:
                                for r in oo_rows:
                                    if str(r.get("symbol") or "").upper() != want_sym:
                                        continue
                                    cid = str(r.get("clientAlgoId") or r.get("clientOrderId") or "").strip()
                                    if not cid.endswith("_HEDGE_TRL"):
                                        continue
                                    if want_ps and str(r.get("positionSide") or "").upper() not in {want_ps, "BOTH", ""}:
                                        continue
                                    otype = str(r.get("orderType") or r.get("type") or "").upper()
                                    if otype and otype != "TRAILING_STOP_MARKET":
                                        continue
                                    existing = r
                                    break
                        except Exception:
                            existing = None

                        # Fallback dedupe: Binance may not return some algo types in openAlgoOrders.
                        db_htrl = None
                        try:
                            db_htrl = self.store.fetch_one(
                                """SELECT status, quantity, trigger_price, updated_at
                                   FROM algo_orders
                                   WHERE exchange_id=%s AND account_id=%s AND client_algo_id=%s
                                   ORDER BY updated_at DESC
                                   LIMIT 1""",
                                (int(self.exchange_id), int(self.account_id), str(cid_htrl)),
                            )
                        except Exception:
                            db_htrl = None
                        have_db_open = bool(db_htrl and str(db_htrl.get("status") or "").upper() == "OPEN")

                        try:
                            existing_qty = float((existing or {}).get("quantity") or (existing or {}).get("origQty") or 0.0) if isinstance(existing, dict) else 0.0
                        except Exception:
                            existing_qty = 0.0
                        try:
                            existing_trig = float((existing or {}).get("activatePrice") or (existing or {}).get("activationPrice") or (existing or {}).get("triggerPrice") or 0.0) if isinstance(existing, dict) else 0.0
                        except Exception:
                            existing_trig = 0.0
                        try:
                            log.info("[TL][HEDGE_TRL][DECISION] sym=%s side=%s base_pos_uid=%s hedge_amt=%.8f existing=%s existing_qty=%.8f existing_trigger=%.8f db_open=%s", symbol, str(hedge_ps).upper(), str(p.get('pos_uid') or ''), float(q), bool(existing is not None), float(existing_qty), float(existing_trig), bool(have_db_open))
                        except Exception:
                            pass

                        # In-memory dedupe for exchanges/accounts where openAlgoOrders does not
                        # reliably return TRAILING_STOP_MARKET hedge orders. Once we successfully
                        # place HEDGE_TRL, keep a local "open" marker until hedge leg disappears
                        # or we explicitly reissue/cancel it.
                        local_htrl = None
                        try:
                            cache = getattr(self, "_hedge_trl_local_open", None)
                            if not isinstance(cache, dict):
                                self._hedge_trl_local_open = {}
                                cache = self._hedge_trl_local_open
                            local_htrl = cache.get(str(cid_htrl))
                        except Exception:
                            local_htrl = None

                        local_open_same = False
                        if isinstance(local_htrl, dict):
                            try:
                                local_open_same = (
                                    str(local_htrl.get("symbol") or "").upper() == str(symbol).upper()
                                    and str(local_htrl.get("position_side") or "").upper() == str(hedge_ps).upper()
                                    and abs(float(local_htrl.get("quantity") or 0.0) - float(q)) <= max(qty_tol, 0.0)
                                )
                            except Exception:
                                local_open_same = False

                        # Determine if we need to (re)issue
                        # Exchange/openAlgoOrders is the source of truth here. A stale DB OPEN marker
                        # must not block HEDGE_TRL placement, otherwise startup/main manager can skip
                        # a genuinely missing hedge trailing and only recovery will restore it one cycle later.
                        need_place = (existing is None) and (not local_open_same)
                        if need_place and have_db_open:
                            try:
                                log.info(
                                    "[TL][HEDGE_TRL][DECISION] stale_db_open_missing_exchange sym=%s side=%s base_pos_uid=%s cid=%s",
                                    symbol, str(hedge_ps).upper(), str(p.get('pos_uid') or ''), cid_htrl
                                )
                            except Exception:
                                pass
                        need_reissue = False
                        if (not need_place) and reissue_on_qty:
                            try:
                                src = existing
                                if src is None and db_htrl is not None:
                                    src = {"quantity": db_htrl.get("quantity")}
                                ex_q = float((src or {}).get("origQty") or (src or {}).get("quantity") or 0.0)
                            except Exception:
                                ex_q = 0.0
                            if abs(ex_q - q) > max(qty_tol, 0.0):
                                need_reissue = True

                        # Cooldown tracking for reissue to avoid churn
                        if not hasattr(self, "_hedge_trl_last_reissue_ts"):
                            self._hedge_trl_last_reissue_ts = {}
                        last_ts = float(self._hedge_trl_last_reissue_ts.get(cid_htrl, 0.0) or 0.0)
                        now_ts = time.time()

                        if need_reissue and (now_ts - last_ts) < reissue_cd:
                            need_reissue = False

                        if need_reissue:
                            # cancel existing before reissue
                            try:
                                self._binance.cancel_algo_order(symbol=symbol, clientAlgoId=cid_htrl)
                                try:
                                    cache = getattr(self, "_hedge_trl_local_open", None)
                                    if isinstance(cache, dict):
                                        cache.pop(str(cid_htrl), None)
                                except Exception:
                                    pass
                                self._hedge_trl_last_reissue_ts[cid_htrl] = now_ts
                                log.info("[TL][HEDGE_TRL] canceled for reissue %s %s cid=%s", symbol, hedge_ps, cid_htrl)

                                # Persist cancel into algo_orders (for audit)
                                try:
                                    self.store.upsert_algo_orders([
                                        {
                                            "exchange_id": int(self.exchange_id),
                                            "account_id": int(self.account_id),
                                            "client_algo_id": str(cid_htrl),
                                            "algo_id": str((existing or {}).get("orderId") or (existing or {}).get("algoId") or ""),
                                            "symbol": str(symbol),
                                            "side": str(existing.get("side") or ""),
                                            "position_side": str(hedge_ps).upper(),
                                            "type": "TRAILING_STOP_MARKET",
                                            "quantity": float(q),
                                            "trigger_price": 0.0,
                                            "working_type": str(work_type),
                                            "status": "CANCELED",
                                            "strategy_id": str(self.STRATEGY_ID),
                                            "pos_uid": str(p.get("pos_uid") or ""),
                                            "raw_json": {"cancel_reason": "reissue_qty_change", "cancel_source": "bot"},
                                        }
                                    ])
                                except Exception:
                                    pass
                            except Exception:
                                log.exception("[TL][HEDGE_TRL] failed to cancel for reissue %s %s cid=%s", symbol, hedge_ps, cid_htrl)

                            need_place = True

                        if need_place:
                            # Last local guard before sending REST request. Prevent duplicates inside the same
                            # cycle / shortly after a successful placement when exchange snapshots lag behind.
                            if recent_place_ts and (now_ts - recent_place_ts) < place_guard_sec:
                                need_place = False

                        if need_place:
                            # Activation must be computed from the hedge average entry price:
                            #   LONG  -> above entry by configured pct
                            #   SHORT -> below entry by configured pct
                            # Then we apply a mark-price safety buffer to avoid immediate trigger rejection,
                            # but keep activation on the correct side of the entry.
                            hedge_entry_ref = float(hedge_entry or 0.0)
                            if hedge_entry_ref <= 0.0 and hedge_pos:
                                try:
                                    hedge_entry_ref = abs(float(hedge_pos.get("entryPrice") or hedge_pos.get("avgEntryPrice") or 0.0))
                                except Exception:
                                    hedge_entry_ref = 0.0
                            if hedge_entry_ref <= 0.0:
                                raise RuntimeError(f"HEDGE_TRL: no valid hedge entry for {symbol} {hedge_ps}")

                            try:
                                tick_h = float(self._price_tick_for_symbol(symbol) or 0.0)
                            except Exception:
                                tick_h = 0.0

                            if str(hedge_ps).upper() == "SHORT":
                                activation = float(hedge_entry_ref) * (1.0 - act_pct / 100.0)
                                close_side = "BUY"
                                if float(mark or 0.0) > 0.0 and tick_h > 0.0:
                                    max_act = float(mark) * (1.0 - buf_pct / 100.0)
                                    if activation >= max_act:
                                        activation = float(_round_price_to_tick(max_act - tick_h, tick_h, mode="down"))
                            else:
                                activation = float(hedge_entry_ref) * (1.0 + act_pct / 100.0)
                                close_side = "SELL"
                                if float(mark or 0.0) > 0.0 and tick_h > 0.0:
                                    min_act = float(mark) * (1.0 + buf_pct / 100.0)
                                    if activation <= min_act:
                                        activation = float(_round_price_to_tick(min_act + tick_h, tick_h, mode="up"))

                            if tick_h > 0.0:
                                activation = float(_round_price_to_tick(
                                    activation,
                                    tick_h,
                                    mode=("down" if str(hedge_ps).upper() == "SHORT" else "up"),
                                ))
                                activation = float(_ensure_tp_trail_side(
                                    activation,
                                    hedge_entry_ref,
                                    tick_h,
                                    str(hedge_ps).upper(),
                                    kind="hedge_trail_activate",
                                ))

                            placed = False
                            last_err = None
                            for _ in range(max(1, place_retries)):
                                try:
                                    resp = self._binance.new_order(
                                        symbol=symbol,
                                        side=close_side,
                                        type="TRAILING_STOP_MARKET",
                                        quantity=float(q),
                                        positionSide=str(hedge_ps).upper(),
                                        newClientOrderId=cid_htrl,
                                        activationPrice=float(activation),
                                        callbackRate=float(trail_pct),
                                        workingType=work_type,
                                    )

                                    # Persist into algo_orders (TRAILING_STOP_MARKET is an algo order in Binance futures)
                                    try:
                                        self.store.upsert_algo_orders([
                                            {
                                                "exchange_id": int(self.exchange_id),
                                                "account_id": int(self.account_id),
                                                "client_algo_id": str(cid_htrl),
                                                "algo_id": str((resp or {}).get("algoId") or (resp or {}).get("orderId") or ""),
                                                "symbol": str(symbol),
                                                "side": str(close_side),
                                                "position_side": str(hedge_ps).upper(),
                                                "type": "TRAILING_STOP_MARKET",
                                                "quantity": float(q),
                                                "trigger_price": float(activation),
                                                "working_type": str(work_type),
                                                "status": "OPEN",
                                                "strategy_id": str(self.STRATEGY_ID),
                                                "pos_uid": str(p.get("pos_uid") or ""),
                                                "raw_json": dict(resp or {}, **{"kind": "HEDGE_TRL"}),
                                            }
                                        ])
                                    except Exception:
                                        pass
                                    # Shadow row in orders (optional, but helps audit)
                                    try:
                                        self._upsert_order_shadow(
                                            pos_uid=str(p.get("pos_uid") or ""),
                                            order_id=(resp or {}).get("orderId"),
                                            client_order_id=cid_htrl,
                                            symbol_id=int(symbol_id),
                                            side=close_side,
                                            order_type="TRAILING_STOP_MARKET",
                                            status="NEW",
                                            qty=float(q),
                                            price=float(activation),
                                            reduce_only=True,
                                        )
                                    except Exception:
                                        pass

                                    self._hedge_trl_last_reissue_ts[cid_htrl] = now_ts
                                    try:
                                        self._hedge_trl_place_guard_ts[cid_htrl] = now_ts
                                    except Exception:
                                        pass
                                    try:
                                        cache = getattr(self, "_hedge_trl_local_open", None)
                                        if not isinstance(cache, dict):
                                            self._hedge_trl_local_open = {}
                                            cache = self._hedge_trl_local_open
                                        cache[str(cid_htrl)] = {
                                            "symbol": str(symbol).upper(),
                                            "position_side": str(hedge_ps).upper(),
                                            "side": str(close_side).upper(),
                                            "quantity": float(q),
                                            "activation_price": float(activation),
                                            "callback_rate": float(trail_pct),
                                            "placed_at": float(now_ts),
                                            "pos_uid": str(p.get("pos_uid") or ""),
                                        }
                                    except Exception:
                                        pass
                                    try:
                                        snap = self._rest_snapshot_get("open_algo_orders_all")
                                        if isinstance(snap, list):
                                            snap.append({
                                                "symbol": str(symbol).upper(),
                                                "clientAlgoId": str(cid_htrl),
                                                "positionSide": str(hedge_ps).upper(),
                                                "orderType": "TRAILING_STOP_MARKET",
                                                "type": "TRAILING_STOP_MARKET",
                                                "side": str(close_side).upper(),
                                                "quantity": float(q),
                                                "callbackRate": float(trail_pct),
                                                "activatePrice": float(activation),
                                            })
                                    except Exception:
                                        pass
                                    log.info(
                                        "[TL][HEDGE_TRL] placed %s %s qty=%.8f activation=%.8f cb=%.4f cid=%s",
                                        symbol, str(hedge_ps).upper(), float(q), float(activation), float(trail_pct), cid_htrl
                                    )
                                    placed = True
                                    break
                                except Exception as e:
                                    last_err = e
                                    time.sleep(0.2)
                            if not placed and last_err is not None:
                                log.warning("[TL][HEDGE_TRL] failed to place %s %s cid=%s err=%s", symbol, hedge_ps, cid_htrl, str(last_err))

                except StopIteration:
                    # Guarded skip
                    pass

                except Exception:
                    log.exception("[TL][HEDGE_TRL] unexpected error for %s %s", symbol, hedge_ps)

            # Side-specific cleanup: if hedge leg is truly flat or hedge trailing disabled, cancel dangling HEDGE_TRL.
            # Do NOT cancel while there is still an OPEN hedge leg on exchange for this side.
            if (not bool(getattr(self.p, "hedge_trailing_enabled", False))) or (hedge_amt <= 0 and (not self._has_open_position_live(symbol, str(hedge_ps)))):
                try:
                    tok = _coid_token(str(p.get('pos_uid') or ''), n=20)
                    cid_htrl = f"TL_{tok}_HEDGE_TRL"
                    self._cancel_algo_by_client_id_safe(symbol, cid_htrl)
                except Exception:
                    pass

            # ------------------------------------------------------------------
            # Hedge hard stop-loss (reduce-only) from hedge ENTRY price
            # Close hedge on adverse move ONLY by configured pct from hedge entry
            # ------------------------------------------------------------------
            if hedge_amt > 0:
                try:
                    hedge_sl_pct = float(getattr(self.p, "hedge_stop_loss_pct", 0.0) or 0.0)
                except Exception:
                    hedge_sl_pct = 0.0

                if hedge_sl_pct and hedge_sl_pct > 0:
                    try:
                        # Strict ownership guard: only the current OPEN base row linked to a real OPEN
                        # live_hedge row may manage HEDGE_SL. This prevents duplicate HEDGE_SL orders
                        # from historical/stale rows of the same symbol.
                        row_hsl = self.store.fetch_one(
                            """
                            SELECT 1 AS ok
                            FROM position_ledger pl
                            JOIN hedge_links hl
                              ON hl.exchange_id = pl.exchange_id
                             AND hl.symbol_id = pl.symbol_id
                             AND hl.base_account_id = pl.account_id
                             AND hl.hedge_account_id = pl.account_id
                             AND hl.hedge_pos_uid = pl.pos_uid
                            WHERE pl.exchange_id = %s
                              AND pl.account_id = %s
                              AND pl.symbol_id = %s
                              AND pl.side = %s
                              AND pl.status = 'OPEN'
                              AND COALESCE(pl.source, 'live') = 'live_hedge'
                              AND hl.base_pos_uid = %s
                            LIMIT 1
                            """,
                            (
                                int(self.exchange_id),
                                int(self.account_id),
                                int(symbol_id),
                                str(hedge_ps),
                                str(p.get('pos_uid') or ''),
                            ),
                        )
                        hsl_owned = bool(row_hsl)
                    except Exception:
                        hsl_owned = False

                    if not hsl_owned:
                        try:
                            row_any_hsl = self.store.fetch_one(
                                """
                                SELECT pl.pos_uid
                                FROM position_ledger pl
                                WHERE pl.exchange_id = %s
                                  AND pl.account_id = %s
                                  AND pl.symbol_id = %s
                                  AND pl.side = %s
                                  AND pl.status = 'OPEN'
                                  AND COALESCE(pl.source, 'live') = 'live_hedge'
                                ORDER BY pl.updated_at DESC NULLS LAST, pl.opened_at DESC NULLS LAST
                                LIMIT 1
                                """,
                                (int(self.exchange_id), int(self.account_id), int(symbol_id), str(hedge_ps)),
                            )
                            hsl_owned = bool(row_any_hsl)
                        except Exception:
                            pass

                    tok = _coid_token(str(p.get('pos_uid') or ''), n=20)
                    cid_hsl = f"TL_{tok}_HEDGE_SL"

                    if (not hsl_owned) or (not self._has_open_position_live(symbol, str(hedge_ps))):
                        try:
                            log.info("[TL][HEDGE_SL][DECISION] skip reason=no_open_hedge_or_binding sym=%s side=%s base_pos_uid=%s hsl_owned=%s hedge_live=%s", symbol, hedge_ps, str(p.get('pos_uid') or ''), bool(hsl_owned), bool(self._has_open_position_live(symbol, str(hedge_ps))))
                        except Exception:
                            pass
                        try:
                            self._dlog("skip HEDGE_SL: no linked live hedge sym=%s side=%s base_pos_uid=%s", symbol, hedge_ps, str(p.get('pos_uid') or ''))
                        except Exception:
                            pass
                        try:
                            self._cancel_algo_by_client_id_safe(symbol, cid_hsl)
                        except Exception:
                            pass
                    else:
                        try:
                            hedge_entry_use = float(hedge_entry or 0.0)
                            hedge_entry_source = 'arg'
                            try:
                                live_hedge_pos = self._get_live_position(symbol, str(hedge_ps).upper())
                            except Exception:
                                live_hedge_pos = None
                            try:
                                live_hedge_entry = abs(float((live_hedge_pos or {}).get('entryPrice') or (live_hedge_pos or {}).get('avgEntryPrice') or (live_hedge_pos or {}).get('avgPrice') or 0.0))
                            except Exception:
                                live_hedge_entry = 0.0
                            if live_hedge_entry > 0:
                                hedge_entry_use = live_hedge_entry
                                hedge_entry_source = 'live_position'
                            mp0 = float(self._get_mark_price_live(symbol) or 0.0)
                            if hedge_entry_use <= 0:
                                hedge_entry_use = mp0
                                hedge_entry_source = 'mark_fallback'

                            if str(hedge_ps).upper() == 'SHORT':
                                stop_px = hedge_entry_use * (1.0 + hedge_sl_pct / 100.0)
                                sl_side = 'BUY'
                            else:
                                stop_px = hedge_entry_use * (1.0 - hedge_sl_pct / 100.0)
                                sl_side = 'SELL'

                            try:
                                be_pct = float(getattr(self.p, "hedge_stop_loss_be_pct", 0.0) or 0.0)
                                be_act_pct = float(getattr(self.p, "hedge_stop_loss_be_activation_pct", 0.0) or 0.0)
                            except Exception:
                                be_pct, be_act_pct = 0.0, 0.0

                            be_applied = False
                            if be_pct > 0 and be_act_pct > 0 and hedge_entry_use > 0:
                                try:
                                    be_pct_min = float(getattr(self.p, "hedge_stop_loss_be_pct_min", 0.0) or 0.0)
                                    be_pct_max = float(getattr(self.p, "hedge_stop_loss_be_pct_max", 100.0) or 100.0)
                                    be_act_min = float(getattr(self.p, "hedge_stop_loss_be_activation_pct_min", 0.0) or 0.0)
                                    be_act_max = float(getattr(self.p, "hedge_stop_loss_be_activation_pct_max", 100.0) or 100.0)
                                except Exception:
                                    be_pct_min, be_pct_max, be_act_min, be_act_max = 0.0, 100.0, 0.0, 100.0

                                be_pct = max(be_pct_min, min(be_pct, be_pct_max))
                                be_act_pct = max(be_act_min, min(be_act_pct, be_act_max))

                                mp = mp0
                                if mp > 0:
                                    if str(hedge_ps).upper() == "SHORT":
                                        act_px = hedge_entry_use * (1.0 - be_act_pct / 100.0)
                                        if mp <= act_px:
                                            be_stop = hedge_entry_use * (1.0 - be_pct / 100.0)
                                            stop_px = min(stop_px, be_stop)
                                            be_applied = True
                                    else:
                                        act_px = hedge_entry_use * (1.0 + be_act_pct / 100.0)
                                        if mp >= act_px:
                                            be_stop = hedge_entry_use * (1.0 + be_pct / 100.0)
                                            stop_px = max(stop_px, be_stop)
                                            be_applied = True

                            try:
                                step = float(self._qty_step_for_symbol(symbol) or 0.0)
                            except Exception:
                                step = 0.0
                            cfg_tol = float(getattr(self.p, 'hedge_trailing_qty_tolerance', 1e-8) or 1e-8)
                            qty_tol = max(cfg_tol, step * 0.5 if step > 0 else cfg_tol)
                            reissue_on_qty = bool(getattr(self.p, 'hedge_trailing_reissue_on_qty_change', True))
                            reissue_cd = float(getattr(self.p, 'hedge_trailing_reissue_cooldown_sec', 30) or 30)

                            ex_hsl = self._find_known_algo_order_by_client_id(symbol, cid_hsl)
                            if ex_hsl is None:
                                ex_hsl = self._find_semantic_open_algo(
                                    symbol,
                                    client_suffix="_HEDGE_SL",
                                    position_side=str(hedge_ps).upper(),
                                    order_type="STOP_MARKET",
                                    side=sl_side,
                                    close_position=True,
                                )
                            ex_open = bool(ex_hsl is not None)
                            ex_close_position = False
                            try:
                                ex_close_position = bool(ex_hsl is not None and str(ex_hsl.get("closePosition") or "").strip().lower() in {"true", "1", "yes", "y", "on"})
                            except Exception:
                                ex_close_position = False
                            try:
                                ex_qty = float((ex_hsl or {}).get("quantity") or (ex_hsl or {}).get("origQty") or 0.0)
                            except Exception:
                                ex_qty = 0.0
                            try:
                                ex_trig = float((ex_hsl or {}).get("triggerPrice") or (ex_hsl or {}).get("stopPrice") or 0.0)
                            except Exception:
                                ex_trig = 0.0

                            db_hsl = self.store.get_algo_order(exchange_id=self.exchange_id, account_id=self.account_id, client_algo_id=cid_hsl)
                            db_open = bool(db_hsl and str(db_hsl.get('status') or '').upper() == 'OPEN')
                            is_open_now = bool(ex_open or db_open)

                            db_close_position = False
                            try:
                                if db_hsl is not None:
                                    db_close_position = bool(db_hsl.get('close_position'))
                            except Exception:
                                db_close_position = False

                            db_qty = None
                            try:
                                if db_hsl is not None:
                                    db_qty = float(db_hsl.get('quantity') or 0.0)
                            except Exception:
                                db_qty = None
                            db_trig = None
                            try:
                                if db_hsl is not None:
                                    db_trig = float(db_hsl.get('trigger_price') or 0.0)
                            except Exception:
                                db_trig = None
                            if ex_open:
                                if ex_qty > 0:
                                    db_qty = ex_qty
                                if ex_trig > 0:
                                    db_trig = ex_trig

                            try:
                                tick = self._price_tick_for_symbol(symbol)
                                tol_px = float(tick) * 0.5 if tick is not None and float(tick) > 0 else 1e-8
                            except Exception:
                                tick = 0.0
                                tol_px = 1e-8
                            try:
                                desired_trig_base = float(self._fmt_price(symbol, stop_px))
                            except Exception:
                                desired_trig_base = float(stop_px)

                            # Build a *placement-safe* trigger separately from the stable/base trigger.
                            # We compare open HEDGE_SL orders against an acceptable band [base .. safe]
                            # instead of against the transient mark-adjusted trigger itself; otherwise
                            # the SL can flap between two nearby prices when mark hovers near the stop.
                            try:
                                live_mark_cmp = float(mp0 or self._get_mark_price_live(symbol) or 0.0)
                            except Exception:
                                live_mark_cmp = float(mp0 or 0.0)
                            try:
                                sl_buf_cmp = float(getattr(self.p, 'hedge_stop_loss_trigger_buffer_pct', 0.35) or 0.35)
                            except Exception:
                                sl_buf_cmp = 0.35

                            desired_trig_safe = float(desired_trig_base)
                            if live_mark_cmp > 0 and tick and float(tick) > 0:
                                if str(hedge_ps).upper() == 'SHORT':
                                    desired_trig_safe = float(self._safe_price_above_mark(desired_trig_base, live_mark_cmp, float(tick), sl_buf_cmp))
                                else:
                                    desired_trig_safe = float(self._safe_price_below_mark(desired_trig_base, live_mark_cmp, float(tick), sl_buf_cmp))

                            desired_trig = float(desired_trig_base)

                            try:
                                trig_tol_pct = float(getattr(self.p, 'hedge_stop_loss_compare_tolerance_pct', 0.08) or 0.08)
                            except Exception:
                                trig_tol_pct = 0.08
                            trig_tol_abs = max(float(tol_px), abs(float(desired_trig_base)) * (float(trig_tol_pct) / 100.0), 1e-8)
                            trig_low = min(float(desired_trig_base), float(desired_trig_safe))
                            trig_high = max(float(desired_trig_base), float(desired_trig_safe))
                            trig_mismatch = False
                            if is_open_now and db_trig is not None:
                                db_trig_f = float(db_trig)
                                trig_mismatch = bool(db_trig_f < (trig_low - trig_tol_abs) or db_trig_f > (trig_high + trig_tol_abs))
                            be_already = False
                            try:
                                if is_open_now and db_trig is not None and hedge_entry_use > 0:
                                    if str(hedge_ps).upper() == 'SHORT' and float(db_trig) < float(hedge_entry_use):
                                        be_already = True
                                    elif str(hedge_ps).upper() != 'SHORT' and float(db_trig) > float(hedge_entry_use):
                                        be_already = True
                            except Exception:
                                be_already = False
                            if be_applied and be_already:
                                trig_mismatch = False
                            if be_already and (db_trig is not None) and float(db_trig) > 0:
                                desired_trig = float(db_trig)
                                trig_mismatch = False

                            qty_mismatch = False
                            if is_open_now and (not (ex_close_position or db_close_position)) and reissue_on_qty and db_qty is not None:
                                try:
                                    db_qty_f = float(self._fmt_qty(symbol, float(db_qty)))
                                    want_qty_f = float(self._fmt_qty(symbol, float(hedge_amt)))
                                    qty_mismatch = bool(abs(db_qty_f - want_qty_f) > qty_tol)
                                except Exception:
                                    qty_mismatch = bool(abs(float(db_qty) - float(hedge_amt)) > qty_tol)
                            need_reissue = bool((not is_open_now) or qty_mismatch or trig_mismatch)

                            # Strong anti-duplicate guard for HEDGE_SL.
                            # Exchange openAlgoOrders snapshots can lag for several seconds; during that window
                            # DB/local shadow may still say OPEN and the desired trigger may already match.
                            # In that case we must keep the current hedge SL and never cancel/recreate it.
                            sig_key = f"{symbol}|{str(hedge_ps).upper()}|HEDGE_SL"
                            sig_val = (round(float(desired_trig), 12), round(float(hedge_amt), 12))
                            cache = getattr(self, '_hedge_sl_sig_cache', None)
                            if not isinstance(cache, dict):
                                cache = {}
                                self._hedge_sl_sig_cache = cache
                            last_sig = cache.get(sig_key)

                            recently_placed_same = False
                            try:
                                place_cache = getattr(self, '_hedge_sl_place_guard', None)
                                if not isinstance(place_cache, dict):
                                    place_cache = {}
                                    self._hedge_sl_place_guard = place_cache
                                prev = place_cache.get(sig_key) or {}
                                prev_sig = prev.get('sig')
                                prev_ts = float(prev.get('ts') or 0.0)
                                guard_sec = float(getattr(self.p, 'hedge_stop_loss_reissue_cooldown_sec', 30) or 30)
                                if prev_sig == sig_val and prev_ts and (time.time() - prev_ts) < max(guard_sec, 30.0):
                                    recently_placed_same = True
                            except Exception:
                                recently_placed_same = False

                            try:
                                log.info('[TL][HEDGE_SL][DECISION] sym=%s side=%s base_pos_uid=%s hedge_amt=%.8f hedge_entry=%.8f entry_source=%s desired_trigger=%.8f existing_trigger=%.8f ex_open=%s db_open=%s trig_mismatch=%s', symbol, str(hedge_ps).upper(), str(p.get('pos_uid') or ''), float(hedge_amt), float(hedge_entry_use), str(hedge_entry_source), float(desired_trig), float(db_trig or 0.0), bool(ex_open), bool(db_open), bool(trig_mismatch))
                            except Exception:
                                pass

                            # Final idempotency guard: when an equivalent HEDGE_SL is already open,
                            # never cancel/recreate it just because recovery state is noisy in the first cycle.
                            if is_open_now and (not qty_mismatch) and (not trig_mismatch):
                                need_reissue = False
                            elif ex_open and ex_close_position and not trig_mismatch:
                                need_reissue = False
                            elif db_open and db_close_position and not trig_mismatch:
                                need_reissue = False
                            elif last_sig == sig_val and (is_open_now or recently_placed_same):
                                need_reissue = False
                            elif ex_open and ex_close_position and ex_trig > 0 and abs(float(ex_trig) - float(desired_trig)) <= trig_tol_abs:
                                need_reissue = False

                            if (not need_reissue) and is_open_now:
                                try:
                                    self._hedge_sl_sig_cache[sig_key] = (round(float(desired_trig), 12), round(float(hedge_amt), 12))
                                except Exception:
                                    pass

                            if need_reissue:
                                now_ts = time.time()
                                if not hasattr(self, '_hsl_last_ts'):
                                    self._hsl_last_ts = {}
                                last_ts = float(self._hsl_last_ts.get(cid_hsl, 0.0))
                                if (now_ts - last_ts) >= reissue_cd:
                                    try:
                                        self._cancel_algo_by_client_id_safe(symbol, cid_hsl)
                                    except Exception:
                                        pass

                                    sl_working_type = str(
                                        getattr(self.p, 'hedge_stop_loss_working_type', None)
                                        or getattr(self.p, 'hedge_trailing_working_type', None)
                                        or getattr(self.p, 'working_type', 'MARK_PRICE')
                                        or 'MARK_PRICE'
                                    ).upper()

                                    try:
                                        live_mark = float(mp0 or self._get_mark_price_live(symbol) or 0.0)
                                    except Exception:
                                        live_mark = float(mp0 or 0.0)
                                    safe_trig = float(desired_trig_safe if desired_trig_safe > 0 else desired_trig)
                                    try:
                                        tick_safe = float(self._price_tick_for_symbol(symbol) or 0.0)
                                    except Exception:
                                        tick_safe = 0.0
                                    try:
                                        sl_buf = float(getattr(self.p, 'hedge_stop_loss_trigger_buffer_pct', 0.35) or 0.35)
                                    except Exception:
                                        sl_buf = 0.35
                                    if live_mark > 0 and tick_safe > 0:
                                        if str(hedge_ps).upper() == 'SHORT':
                                            safe_trig = float(self._safe_price_above_mark(float(desired_trig), live_mark, tick_safe, sl_buf))
                                        else:
                                            safe_trig = float(self._safe_price_below_mark(float(desired_trig), live_mark, tick_safe, sl_buf))
                                    try:
                                        resp_sl = self._binance.new_algo_order(
                                            symbol=symbol,
                                            side=sl_side,
                                            type='STOP_MARKET',
                                            closePosition=True,
                                            workingType=sl_working_type,
                                            positionSide=hedge_ps,
                                            algoType='CONDITIONAL',
                                            clientAlgoId=cid_hsl,
                                            triggerPrice=self._fmt_price(symbol, safe_trig),
                                        )
                                    except Exception as e:
                                        msg = str(e)
                                        if '-2021' in msg or 'immediately trigger' in msg.lower():
                                            if live_mark > 0 and tick_safe > 0:
                                                widen_buf = max(sl_buf, 0.8)
                                                if str(hedge_ps).upper() == 'SHORT':
                                                    safe_trig = float(self._safe_price_above_mark(safe_trig, live_mark, tick_safe, widen_buf))
                                                else:
                                                    safe_trig = float(self._safe_price_below_mark(safe_trig, live_mark, tick_safe, widen_buf))
                                            resp_sl = self._binance.new_algo_order(
                                                symbol=symbol,
                                                side=sl_side,
                                                type='STOP_MARKET',
                                                closePosition=True,
                                                workingType=sl_working_type,
                                                positionSide=hedge_ps,
                                                algoType='CONDITIONAL',
                                                clientAlgoId=cid_hsl,
                                                triggerPrice=self._fmt_price(symbol, safe_trig),
                                            )
                                        else:
                                            raise
                                    desired_trig = float(safe_trig)
                                    self._upsert_algo_order_shadow(resp_sl, pos_uid=str(p.get('pos_uid') or ''), strategy_id='trade_liquidation')
                                    try:
                                        self._mark_local_algo_open(
                                            cid_hsl,
                                            symbol=symbol,
                                            position_side=str(hedge_ps).upper(),
                                            order_type='STOP_MARKET',
                                            side=sl_side,
                                            quantity=float(hedge_amt),
                                            trigger_price=float(desired_trig),
                                            close_position=True,
                                            pos_uid=str(p.get('pos_uid') or ''),
                                        )
                                    except Exception:
                                        pass
                                    self._hsl_last_ts[cid_hsl] = now_ts
                                    self._hedge_sl_sig_cache[sig_key] = (round(float(desired_trig), 12), round(float(hedge_amt), 12))
                                    try:
                                        place_cache = getattr(self, '_hedge_sl_place_guard', None)
                                        if not isinstance(place_cache, dict):
                                            place_cache = {}
                                            self._hedge_sl_place_guard = place_cache
                                        place_cache[sig_key] = {'sig': sig_val, 'ts': float(now_ts), 'cid': str(cid_hsl)}
                                    except Exception:
                                        pass
                                    log.info('[TL][HEDGE_SL] placed %s %s qty=%.8f stop=%.8f entry=%.8f entry_source=%s pct=%.4f be=%s be_pct=%.4f act_pct=%.4f cid=%s', symbol, str(hedge_ps).upper(), float(hedge_amt), float(stop_px), float(hedge_entry_use), str(hedge_entry_source), float(hedge_sl_pct), bool(be_applied), float(be_pct or 0.0), float(be_act_pct or 0.0), cid_hsl)
                        except Exception:
                            log.exception('[TL][HEDGE_SL] unexpected error for %s %s', symbol, hedge_ps)

            # Side-specific cleanup: if hedge leg is flat now, cancel any dangling HEDGE_SL
            if hedge_amt <= 0:
                try:
                    tok = _coid_token(str(p.get('pos_uid') or ''), n=20)
                    cid_hsl = f"TL_{tok}_HEDGE_SL"
                    self._cancel_algo_by_client_id_safe(symbol, cid_hsl)
                except Exception:
                    pass

            # Balanced profit optimization: unload hedge when price moves back in favor of MAIN.
            if hedge_amt > 0:
                try:
                    hedge_entry_use_unwind = float(hedge_entry or 0.0)
                    if hedge_entry_use_unwind <= 0.0 and hedge_pos:
                        hedge_entry_use_unwind = abs(float(hedge_pos.get('entryPrice') or hedge_pos.get('avgEntryPrice') or 0.0))
                except Exception:
                    hedge_entry_use_unwind = 0.0
                try:
                    if self._live_manage_hedge_unwind(
                        p=p,
                        symbol=symbol,
                        main_side=main_side,
                        hedge_ps=hedge_ps,
                        hedge_amt=float(hedge_amt),
                        hedge_entry_use=float(hedge_entry_use_unwind),
                        mark=float(mark),
                    ):
                        try:
                            refreshed = self._rest_snapshot_get('position_risk') or []
                            for rr in refreshed if isinstance(refreshed, list) else []:
                                if str(rr.get('symbol') or '').upper() != str(symbol).upper():
                                    continue
                                if str(rr.get('positionSide') or '').upper() != str(hedge_ps).upper():
                                    continue
                                hedge_amt = abs(float(rr.get('positionAmt') or 0.0))
                                hedge_pos = rr
                                break
                        except Exception:
                            pass
                except Exception:
                    pass

            # Funding drag guard for an already open hedge
            if hedge_amt > 0:
                try:
                    if self._live_manage_hedge_funding_guard(
                        p=p, symbol=symbol, main_side=main_side, hedge_ps=hedge_ps, hedge_amt=float(hedge_amt), mark=float(mark)
                    ):
                        try:
                            refreshed = self._rest_snapshot_get('position_risk') or []
                            for rr in refreshed if isinstance(refreshed, list) else []:
                                if str(rr.get('symbol') or '').upper() != str(symbol).upper():
                                    continue
                                if str(rr.get('positionSide') or '').upper() != str(hedge_ps).upper():
                                    continue
                                hedge_amt = abs(float(rr.get('positionAmt') or 0.0))
                                hedge_pos = rr
                                break
                        except Exception:
                            pass
                except Exception:
                    pass

            # Close hedge if main position is already positive unrealized PnL
            main_positive = (float(main_upnl) > 0.0)

            # Compute would-be SL price (same as default SL)
            if main_side == "LONG":
                sl_price = entry * (1.0 - _pct_to_mult(self.p.stop_loss_pct))
                sl_hit = (mark <= sl_price)
            else:
                sl_price = entry * (1.0 + _pct_to_mult(self.p.stop_loss_pct))
                sl_hit = (mark >= sl_price)

            # 1) Open hedge instead of SL
            # Open hedge instead of SL (or reopen after prior close) when conditions met
            try:
                hedge_cooldown_sec = float(getattr(self.p, "hedge_cooldown_sec", 0.0) or 0.0)
            except Exception:
                hedge_cooldown_sec = 0.0
            open_trigger = bool(sl_hit) if pl_status == "OPEN" else False
            if pl_status == "OPEN" and (not open_trigger) and bool(getattr(self.p, "hedge_reopen_enabled", True)):
                if float(main_upnl) < 0.0:
                    open_trigger = True

            
            # Optional extra filter for hedge re-entry after a hedge close (SL/TRL/ALGO).
            # Re-entry direction is defined relative to the MAIN leg:
            #   - main LONG  -> re-enter hedge only if price moved BELOW the last hedge exit anchor
            #   - main SHORT -> re-enter hedge only if price moved ABOVE the last hedge exit anchor
            # When price moves in favor of the MAIN leg, the anchor is shifted toward the MAIN.
            try:
                if (not bool(sl_hit)) and bool(getattr(self.p, "hedge_reopen_price_filter_enabled", False)):
                    anchor_px = float(hedge_reopen_anchor_px or hedge_last_close_px or 0.0)
                    move_pct = float(getattr(self.p, "hedge_reopen_price_move_pct", 0.0) or 0.0) / 100.0
                    shift_trigger_pct = float(getattr(self.p, "hedge_reopen_anchor_shift_trigger_pct", 0.0) or 0.0) / 100.0
                    shift_step_pct = float(getattr(self.p, "hedge_reopen_anchor_shift_step_pct", 0.0) or 0.0) / 100.0
                    if anchor_px > 0.0:
                        new_anchor_px = float(anchor_px)
                        if shift_trigger_pct > 0.0 and shift_step_pct > 0.0:
                            if str(main_side).upper() == "LONG":
                                while float(mark) >= new_anchor_px * (1.0 + shift_trigger_pct):
                                    new_anchor_px = new_anchor_px * (1.0 + shift_step_pct)
                            else:
                                while float(mark) <= new_anchor_px * (1.0 - shift_trigger_pct):
                                    new_anchor_px = new_anchor_px * (1.0 - shift_step_pct)
                        if abs(new_anchor_px - anchor_px) > 1e-12:
                            try:
                                _persist_pending_anchor("reopen_anchor_px", float(new_anchor_px))
                            except Exception:
                                pass
                            hedge_reopen_anchor_px = float(new_anchor_px)
                            anchor_px = float(new_anchor_px)
                        if str(main_side).upper() == "LONG":
                            target_px = anchor_px * (1.0 - max(move_pct, 0.0))
                            if not (float(mark) <= target_px):
                                open_trigger = False
                        else:
                            target_px = anchor_px * (1.0 + max(move_pct, 0.0))
                            if not (float(mark) >= target_px):
                                open_trigger = False
            except Exception:
                pass
            
            allow_first_hedge_open = True
            adds_done_for_hedge = 0
            max_adds_for_hedge = 0
            try:
                if bool(getattr(self.p, "hedge_open_only_after_max_adds", True)):
                    avg_enabled_cfg = bool(getattr(self.p, "averaging_enabled", False))
                    max_adds_for_hedge = int(self._cfg_max_adds() or 0)
                    if avg_enabled_cfg and max_adds_for_hedge > 0:
                        try:
                            adds_done_for_hedge = int(p.get("scale_in_count", 0) or 0)
                        except Exception:
                            adds_done_for_hedge = 0
                        try:
                            pos_uid = str(p.get("pos_uid") or "")
                            prefix = str(getattr(self.p, "client_order_id_prefix", "TL") or "TL")
                            if pos_uid:
                                adds_done_live = int(self._live_refresh_scale_in_count(pos_uid=pos_uid, prefix=prefix, sym=symbol, pos_side=main_side) or 0)
                                if adds_done_live > adds_done_for_hedge:
                                    adds_done_for_hedge = adds_done_live
                        except Exception:
                            pass
                        if int(adds_done_for_hedge) < int(max_adds_for_hedge):
                            allow_first_hedge_open = False
            except Exception:
                allow_first_hedge_open = True

            if hedge_amt <= 0 and open_trigger and (not allow_first_hedge_open):
                try:
                    self._rate_limited_log(logging.DEBUG, 'hedge_deferred', f'hedge_deferred:{str(symbol).upper()}:{str(main_side).upper()}', '[TL][HEDGE_OPEN_DEFERRED_UNTIL_MAX_ADDS] %s %s adds_done=%s/%s upnl=%.8f sl_hit=%s', symbol, str(main_side).upper(), int(adds_done_for_hedge), int(max_adds_for_hedge), float(main_upnl or 0.0), bool(sl_hit))
                except Exception:
                    pass

            if hedge_amt <= 0 and open_trigger and allow_first_hedge_open:
                # cooldown check (open)
                if hedge_cooldown_sec and hedge_last_action_ts and (time.time() - float(hedge_last_action_ts)) < float(hedge_cooldown_sec):
                    skipped += 1
                    continue

                # qty and rounding
                qty_step = float(self._qty_step_for_symbol(symbol) or 0.0) or 0.0
                # hedge qty: use stored base qty if available (same as first open), otherwise compute from current main qty
                if hedge_base_qty is not None and float(hedge_base_qty) > 0:
                    hedge_qty = float(hedge_base_qty)
                else:
                    try:
                        ref_px_open = float(avg_entry or entry_price or 0.0)
                    except Exception:
                        ref_px_open = 0.0
                    hedge_qty, _eff_koff, _funding_pct = self._compute_live_hedge_qty_with_funding(
                        symbol=str(symbol).upper(),
                        main_side=str(main_side).upper(),
                        main_qty=float(qty),
                        ref_price=float(ref_px_open),
                        mark_price=float(mark),
                    )
                if qty_step and qty_step > 0:
                    hedge_qty = _round_qty_to_step(hedge_qty, qty_step, mode="down")
                if hedge_qty <= 0 or (qty_step and hedge_qty < qty_step):
                    skipped += 1
                    continue

                side_ord = "SELL" if hedge_side == "SHORT" else "BUY"
                try:
                    resp = self._binance.new_order(
                        symbol=symbol,
                        side=side_ord,
                        type="MARKET",
                        quantity=float(hedge_qty),
                        positionSide=hedge_ps,
                    )
                    opened += 1
                    # persist hedge base qty (same for reopens) into position_ledger.raw_meta if available
                    try:
                        if getattr(self, "_pl_has_raw_meta", False):
                            import json as _json
                            reason = "SL" if bool(sl_hit) else "REOPEN"
                            meta = {"hedge": {"base_qty": float(hedge_qty), "last_open_reason": str(reason), "last_open_ts": float(time.time()), "last_action_ts": float(time.time()), "be_best_px": float(mark), "be_armed": False}}
                            uqm = '''
                            UPDATE position_ledger
                            SET raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb,
                                updated_at = now()
                            WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND status='OPEN';
                            '''
                            self.store.execute(
                                uqm,
                                {
                                    "meta": _json.dumps(meta),
                                    "ex": int(self.exchange_id),
                                    "acc": int(self.account_id),
                                    "pos_uid": str(p.get("pos_uid")),
                                },
                            )
                    except Exception:
                        pass
                    log.info("[TL][HEDGE] opened %s %s qty=%.8f (main=%s qty=%.8f) mark=%.6f sl=%.6f ema_open=%s ema_close=%s",
                             symbol, hedge_ps, float(hedge_qty), main_side, float(qty), float(mark), float(sl_price), str(ema_dir_open), str(ema_dir_close))

                    # Immediately protect a freshly opened / reopened hedge leg in the same cycle.
                    # Previously SL + HEDGE_TRL were only attached on the next manager pass,
                    # leaving a timing window right after market entry.
                    try:
                        hedge_entry_now = 0.0
                        try:
                            hedge_entry_now = abs(float((resp or {}).get('avgPrice') or (resp or {}).get('price') or 0.0))
                        except Exception:
                            hedge_entry_now = 0.0
                        if hedge_entry_now <= 0.0:
                            try:
                                refreshed = self._rest_snapshot_get('position_risk') or []
                                for rr in refreshed if isinstance(refreshed, list) else []:
                                    if str(rr.get('symbol') or '').upper() != str(symbol).upper():
                                        continue
                                    if str(rr.get('positionSide') or '').upper() != str(hedge_ps).upper():
                                        continue
                                    hedge_entry_now = abs(float(rr.get('entryPrice') or rr.get('avgEntryPrice') or 0.0))
                                    break
                            except Exception:
                                hedge_entry_now = 0.0
                        if hedge_entry_now <= 0.0:
                            hedge_entry_now = float(mark or 0.0)
                        self._ensure_immediate_hedge_protection(
                            base_pos=p,
                            symbol=str(symbol).upper(),
                            symbol_id=int(symbol_id),
                            hedge_ps=str(hedge_ps).upper(),
                            hedge_amt=float(hedge_qty),
                            hedge_entry=float(hedge_entry_now),
                            mark=float(mark or 0.0),
                        )
                    except Exception:
                        log.exception('[TL][HEDGE] immediate protection failed %s %s base_pos_uid=%s', symbol, hedge_ps, str(p.get('pos_uid') or ''))
                except Exception as e:
                    log.exception("[TL][HEDGE] failed to open hedge for %s: %s", symbol, e)
                    skipped += 1
                continue  # next position

            # 2) Close hedge if exists and conditions met
            if hedge_amt > 0:
                close_reason = None

                # close hedge if main became profitable (optional via config)
                if bool(getattr(self.p, "hedge_close_on_main_positive", True)) and main_positive:
                    close_reason = "MAIN_POSITIVE"

                # a) close hedge at/near entry after it has first moved into profit enough
                if False and close_reason is None and bool(getattr(self.p, "hedge_close_on_entry_enabled", False)):
                    try:
                        be_act_pct = float(getattr(self.p, "hedge_close_on_entry_profit_activation_pct", 0.0) or 0.0) / 100.0
                    except Exception:
                        be_act_pct = 0.0
                    try:
                        be_buf_pct = float(getattr(self.p, "hedge_close_on_entry_buffer_pct", 0.0) or 0.0) / 100.0
                    except Exception:
                        be_buf_pct = 0.0
                    hedge_entry_ref = float(hedge_entry_use or 0.0)
                    hedge_upnl_now = float(hedge_upnl or 0.0)
                    hedge_pnl_ok = hedge_upnl_now >= 0.0
                    if hedge_entry_ref > 0.0:
                        best_px = float(hedge_be_best_px or hedge_entry_ref or 0.0)
                        armed_now = bool(hedge_be_armed)
                        if str(hedge_side).upper() == "SHORT":
                            if best_px <= 0.0:
                                best_px = hedge_entry_ref
                            if float(mark) < best_px:
                                best_px = float(mark)
                            trigger_px = hedge_entry_ref * (1.0 - max(be_act_pct, 0.0))
                            if float(best_px) <= trigger_px:
                                armed_now = True
                            entry_close_px = hedge_entry_ref * (1.0 - max(be_buf_pct, 0.0))
                            if armed_now and hedge_pnl_ok and float(mark) >= entry_close_px:
                                close_reason = "ENTRY_BE"
                        else:
                            if best_px <= 0.0:
                                best_px = hedge_entry_ref
                            if float(mark) > best_px:
                                best_px = float(mark)
                            trigger_px = hedge_entry_ref * (1.0 + max(be_act_pct, 0.0))
                            if float(best_px) >= trigger_px:
                                armed_now = True
                            entry_close_px = hedge_entry_ref * (1.0 + max(be_buf_pct, 0.0))
                            if armed_now and hedge_pnl_ok and float(mark) <= entry_close_px:
                                close_reason = "ENTRY_BE"
                        if abs(best_px - float(hedge_be_best_px or 0.0)) > 1e-12 or bool(armed_now) != bool(hedge_be_armed):
                            try:
                                if getattr(self, "_pl_has_raw_meta", False):
                                    import json as _json
                                    meta = {"hedge": {"be_best_px": float(best_px), "be_armed": bool(armed_now)}}
                                    uqm = """
                                    UPDATE position_ledger
                                    SET raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb,
                                        updated_at = now()
                                    WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND status='OPEN';
                                    """
                                    self.store.execute(
                                        uqm,
                                        {
                                            "meta": _json.dumps(meta),
                                            "ex": int(self.exchange_id),
                                            "acc": int(self.account_id),
                                            "pos_uid": str(p.get("pos_uid")),
                                        },
                                    )
                            except Exception:
                                pass
                            hedge_be_best_px = float(best_px)
                            hedge_be_armed = bool(armed_now)

                # b) close on significant level
                if False and bool(getattr(self.p, "hedge_close_on_level", True)):
                    tf_main = str(p.get("timeframe") or "15m")
                    level = self._compute_significant_level(symbol_id, side=main_side, entry_ref=float(entry), timeframe=tf_main)
                    tol = float(getattr(self.p, "hedge_level_tolerance_pct", 0.10) or 0.10) / 100.0
                    if level > 0:
                        if main_side == "LONG":
                            # close SHORT hedge when price reaches support level (or slightly above)
                            if mark <= level * (1.0 + tol):
                                close_reason = "LEVEL"
                        else:
                            # close LONG hedge when price reaches resistance level (or slightly below)
                            if mark >= level * (1.0 - tol):
                                close_reason = "LEVEL"

                # b) close on EMA flip against hedge
                if False and close_reason is None and bool(getattr(self.p, "hedge_close_on_ema_flip", True)):
                    # Close hedge when EMA direction flips against hedge direction.
                    # NOTE: we only close the hedge if it's currently profitable (hedge_upnl > 0),
                    # to avoid "locking in" a loss on fast whipsaws.
                    if ema_dir_close and ema_dir_close in {"UP", "DOWN"}:
                        if ema_dir_close != need_ema:
                            if float(hedge_upnl) > 0.0:
                                close_reason = "EMA_FLIP"
                                self.log.info(
                                    f"[TL][hedge] EMA flip -> close hedge: {symbol} hedge_side={hedge_side} need_ema={need_ema} ema_dir_close={ema_dir_close} hedge_upnl={float(hedge_upnl):.6f}"
                                )
                            else:
                                self.log.debug(
                                    f"[TL][hedge] EMA flip detected but hedge_upnl<=0 -> keep hedge: {symbol} hedge_side={hedge_side} need_ema={need_ema} ema_dir_close={ema_dir_close} hedge_upnl={float(hedge_upnl):.6f}"
                                )
                        else:
                            self.log.debug(
                                f"[TL][hedge] EMA direction matches hedge -> keep hedge: {symbol} hedge_side={hedge_side} need_ema={need_ema} ema_dir_close={ema_dir_close}"
                            )
                    else:
                        self.log.debug(
                            f"[TL][hedge] EMA close direction not available -> skip ema_flip close: {symbol} hedge_side={hedge_side} need_ema={need_ema} ema_dir_close={ema_dir_close}"
                        )

                if close_reason:
                    # cooldown check (close)
                    if hedge_cooldown_sec and hedge_last_action_ts and (time.time() - float(hedge_last_action_ts)) < float(hedge_cooldown_sec):
                        skipped += 1
                        continue
                    close_side = "BUY" if hedge_side == "SHORT" else "SELL"
                    qty_step = float(self._qty_step_for_symbol(symbol) or 0.0) or 0.0
                    close_qty = float(hedge_amt)
                    if qty_step and qty_step > 0:
                        close_qty = _round_qty_to_step(close_qty, qty_step, mode="down")
                    if close_qty <= 0 or (qty_step and close_qty < qty_step):
                        skipped += 1
                        continue

                    def _stamp_hedge_close_attempt(reason: str, close_px: float, synthetic_closed: bool = False) -> None:
                        try:
                            if getattr(self, "_pl_has_raw_meta", False):
                                import json as _json
                                meta = {
                                    "hedge": {
                                        "base_qty": float(hedge_base_qty) if (hedge_base_qty is not None and float(hedge_base_qty) > 0) else float(hedge_amt),
                                        "last_close_reason": str(reason),
                                        "last_close_ts": float(time.time()),
                                        "last_close_px": float(close_px),
                                        "reopen_anchor_px": float(close_px),
                                        "last_action_ts": float(time.time()),
                                        "be_armed": False,
                                        "be_best_px": 0.0,
                                        "close_inflight": False if synthetic_closed else True,
                                        "close_inflight_ts": float(time.time()),
                                    }
                                }
                                uqm = """
                                UPDATE position_ledger
                                SET raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb,
                                    updated_at = now()
                                WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND status='OPEN';
                                """
                                self.store.execute(
                                    uqm,
                                    {
                                        "meta": _json.dumps(meta),
                                        "ex": int(self.exchange_id),
                                        "acc": int(self.account_id),
                                        "pos_uid": str(p.get("pos_uid")),
                                    },
                                )
                        except Exception:
                            pass

                    try:
                        # Stamp the close attempt before the exchange request so we do not spam duplicate
                        # close orders while the ledger is waiting for reconciliation.
                        _stamp_hedge_close_attempt(str(close_reason), float(mark), synthetic_closed=False)
                        close_params = dict(
                            symbol=symbol,
                            side=close_side,
                            type="MARKET",
                            quantity=float(close_qty),
                            positionSide=hedge_ps,
                        )
                        try:
                            # In Hedge Mode Binance typically forbids reduceOnly together with positionSide.
                            # We therefore close hedge legs without reduceOnly by default.
                            resp = self._binance.new_order(**close_params)
                        except Exception as e1:
                            msg1 = str(e1)
                            if '"code":-1106' in msg1 and 'reduceonly' in msg1.lower():
                                retry_params = dict(close_params)
                                retry_params.pop("reduceOnly", None)
                                resp = self._binance.new_order(**retry_params)
                            else:
                                raise
                        closed += 1
                        _stamp_hedge_close_attempt(str(close_reason), float(mark), synthetic_closed=False)
                        log.info("[TL][HEDGE] closed %s %s qty=%.8f reason=%s mark=%.6f ema_close=%s ema_open=%s",
                                 symbol, hedge_ps, float(close_qty), str(close_reason), float(mark), str(ema_dir_close), str(ema_dir_open))

                        # Optional: after hedge close, add to the still-open main leg by the same
                        # currently open main volume. This effectively doubles the main exposure
                        # from its live size at the moment of hedge close.
                        try:
                            add_enabled = False
                            add_max_count = 0
                        except Exception:
                            add_enabled = False
                            add_max_count = 0

                        if add_enabled and add_max_count > 0 and float(main_amt) > 0.0:
                            try:
                                rm_pos = p.get("raw_meta")
                                if isinstance(rm_pos, str):
                                    rm_pos = json.loads(rm_pos) if rm_pos.strip() else {}
                                if not isinstance(rm_pos, dict):
                                    rm_pos = {}
                            except Exception:
                                rm_pos = {}

                            hedge_meta = rm_pos.get("hedge") if isinstance(rm_pos.get("hedge"), dict) else {}
                            add_meta = hedge_meta.get("main_add_on_hedge_close") if isinstance(hedge_meta.get("main_add_on_hedge_close"), dict) else {}
                            add_count_done = int(add_meta.get("count") or 0)

                            # In-memory guard against duplicate add placement while exchange/ledger
                            # snapshots are still catching up after the hedge close market order.
                            add_guard_ok = True
                            try:
                                if not hasattr(self, "_hedge_close_add_guard") or not isinstance(self._hedge_close_add_guard, dict):
                                    self._hedge_close_add_guard = {}
                                guard_key = f"{str(p.get('pos_uid') or '')}:{int(add_count_done + 1)}"
                                last_guard_ts = float(self._hedge_close_add_guard.get(guard_key) or 0.0)
                                if last_guard_ts > 0.0 and (time.time() - last_guard_ts) < 30.0:
                                    add_guard_ok = False
                                else:
                                    self._hedge_close_add_guard[guard_key] = time.time()
                            except Exception:
                                add_guard_ok = True

                            if add_guard_ok and add_count_done < add_max_count:
                                main_qty_to_add = float(main_amt)
                                if qty_step and qty_step > 0:
                                    main_qty_to_add = _round_qty_to_step(main_qty_to_add, qty_step, mode="down")

                                if main_qty_to_add > 0.0 and (not qty_step or main_qty_to_add >= qty_step):
                                    add_side = "BUY" if main_side == "LONG" else "SELL"
                                    try:
                                        add_resp = self._binance.new_order(
                                            symbol=symbol,
                                            side=add_side,
                                            type="MARKET",
                                            quantity=float(main_qty_to_add),
                                            positionSide=main_ps,
                                        )
                                        try:
                                            if getattr(self, "_pl_has_raw_meta", False):
                                                import json as _json
                                                add_meta_patch = {
                                                    "hedge": {
                                                        "main_add_on_hedge_close": {
                                                            "enabled": True,
                                                            "count": int(add_count_done) + 1,
                                                            "max_count": int(add_max_count),
                                                            "last_ts": float(time.time()),
                                                            "last_qty": float(main_qty_to_add),
                                                            "last_reason": str(close_reason),
                                                            "last_hedge_side": str(hedge_ps).upper(),
                                                            "last_main_side": str(main_ps).upper(),
                                                            "last_order_id": str(add_resp.get("orderId") or add_resp.get("order_id") or ""),
                                                        }
                                                    }
                                                }
                                                self.store.execute(
                                                    """
                                                    UPDATE position_ledger
                                                    SET raw_meta = COALESCE(raw_meta,'{}'::jsonb) || %(meta)s::jsonb,
                                                        updated_at = now()
                                                    WHERE exchange_id=%(ex)s AND account_id=%(acc)s AND pos_uid=%(pos_uid)s AND status='OPEN';
                                                    """,
                                                    {
                                                        "meta": _json.dumps(add_meta_patch),
                                                        "ex": int(self.exchange_id),
                                                        "acc": int(self.account_id),
                                                        "pos_uid": str(p.get("pos_uid")),
                                                    },
                                                )
                                        except Exception:
                                            pass
                                        log.info(
                                            "[TL][HEDGE][MAIN_ADD] added to main after hedge close %s %s qty=%.8f count=%s/%s reason=%s",
                                            symbol,
                                            str(main_ps).upper(),
                                            float(main_qty_to_add),
                                            int(add_count_done) + 1,
                                            int(add_max_count),
                                            str(close_reason),
                                        )
                                    except Exception as add_e:
                                        log.exception(
                                            "[TL][HEDGE][MAIN_ADD] failed to add to main after hedge close for %s %s qty=%.8f: %s",
                                            symbol,
                                            str(main_ps).upper(),
                                            float(main_qty_to_add),
                                            add_e,
                                        )
                                else:
                                    log.info(
                                        "[TL][HEDGE][MAIN_ADD] skipped after hedge close for %s %s: computed qty too small qty=%.8f step=%.8f",
                                        symbol,
                                        str(main_ps).upper(),
                                        float(main_qty_to_add),
                                        float(qty_step or 0.0),
                                    )
                            elif add_enabled and add_count_done >= add_max_count:
                                log.info(
                                    "[TL][HEDGE][MAIN_ADD] limit reached for %s %s: count=%s max=%s",
                                    symbol,
                                    str(main_ps).upper(),
                                    int(add_count_done),
                                    int(add_max_count),
                                )
                    except Exception as e:
                        msg = str(e)
                        if 'ReduceOnly Order is rejected' in msg or '"code":-2022' in msg:
                            # The hedge is already closed (or a close is already fully matched) on the exchange.
                            # Treat it as an idempotent close and let the normal ledger reconciliation mark qty=0.
                            _stamp_hedge_close_attempt(f"{close_reason}_ALREADY_CLOSED", float(mark), synthetic_closed=True)
                            closed += 1
                            log.info("[TL][HEDGE] close already satisfied for %s %s qty=%.8f reason=%s mark=%.6f",
                                     symbol, hedge_ps, float(close_qty), str(close_reason), float(mark))
                        elif '"code":-1106' in msg and 'reduceonly' in msg.lower():
                            # Binance rejected an unnecessary reduceOnly flag; treat as config/API-shape issue for this request.
                            log.info("[TL][HEDGE] close retried without reduceOnly for %s %s qty=%.8f reason=%s",
                                     symbol, hedge_ps, float(close_qty), str(close_reason))
                        else:
                            log.exception("[TL][HEDGE] failed to close hedge for %s: %s", symbol, e)
                            skipped += 1

        return {"checked": checked, "opened": opened, "closed": closed, "skipped": skipped}

