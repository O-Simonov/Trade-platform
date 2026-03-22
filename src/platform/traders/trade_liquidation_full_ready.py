# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Подключаемый модуль-патч для TradeLiquidation.

ВАЖНО:
- этот файл нужно сохранять как src/platform/traders/trade_liquidation_full_ready.py
- в конце src/platform/traders/trade_liquidation.py должно быть:

    from src.platform.traders.trade_liquidation_full_ready import apply_trade_liquidation_full_ready
    apply_trade_liquidation_full_ready(TradeLiquidation)

- сам модуль не должен импортировать TradeLiquidation из trade_liquidation.py,
  иначе возникает circular import.
"""

import json
import logging
import uuid

def _round_price_to_tick(price: float, tick: float, *, mode: str = "nearest") -> float:
    if tick <= 0:
        return float(price)
    n = float(price) / float(tick)
    if mode == 'up':
        import math
        return float(math.ceil(n) * tick)
    if mode == 'down':
        import math
        return float(math.floor(n) * tick)

    return float(round(n) * tick)

def _ensure_tp_trail_side(level: float, entry: float, tick: float, pos_side: str, *, kind: str) -> float:
    level = float(level)
    entry = float(entry)
    tick = abs(float(tick or 0.0))
    side = str(pos_side or '').upper()
    if entry <= 0:
        return level
    if tick <= 0:
        tick = max(abs(entry) * 1e-8, 1e-8)
    if side == 'LONG':
        if level <= entry:
            return entry + tick
        return level
    if side == 'SHORT':
        if level >= entry:
            return entry - tick
        return level
    return level


def _coid_token_local(pos_uid: str, n: int = 20) -> str:
    import hashlib
    raw = str(pos_uid or '').encode('utf-8', errors='ignore')
    return hashlib.sha1(raw).hexdigest()[: max(4, int(n))]

def _algo_trigger_price(row: Optional[Dict[str, Any]]) -> float:
    if not isinstance(row, dict):
        return 0.0
    for key in ("triggerPrice", "stopPrice", "activatePrice", "activationPrice"):
        try:
            val = float(row.get(key) or 0.0)
        except Exception:
            val = 0.0
        if val > 0.0:
            return val
    return 0.0


def _algo_quantity(row: Optional[Dict[str, Any]]) -> float:
    if not isinstance(row, dict):
        return 0.0
    for key in ("origQty", "quantity", "qty"):
        try:
            val = abs(float(row.get(key) or 0.0))
        except Exception:
            val = 0.0
        if val > 0.0:
            return val
    return 0.0


def _reissue_tolerance(self, want_price: float, tick: float) -> float:
    try:
        avg_eps_pct = float(getattr(self.p, "avg_eps_pct", 0.02) or 0.02)
    except Exception:
        avg_eps_pct = 0.02
    try:
        price_eps_ticks = int(getattr(self.p, "price_eps_ticks", 2) or 2)
    except Exception:
        price_eps_ticks = 2
    return max(abs(float(want_price or 0.0)) * (avg_eps_pct / 100.0), abs(float(tick or 0.0)) * float(price_eps_ticks), 1e-12)


def _main_order_ids(self, pos_uid: str) -> Dict[str, str]:
    prefix = str(getattr(self.p, "client_order_id_prefix", "TL") or "TL").strip() or "TL"
    tok = _coid_token_local(pos_uid, n=20)
    return {
        "tp": f"{prefix}_{tok}_TP",
        "trl": f"{prefix}_{tok}_TRL",
        "hedge_tp": f"{prefix}_{tok}_HEDGE_TP",
        "hedge_trl": f"{prefix}_{tok}_HEDGE_TRL",
    }


def _find_algo_any(self, symbol: str, cid: str, *, suffix: str, position_side: str, order_type: str, side: str, close_position: Optional[bool] = None):
    row = None
    try:
        row = self._find_known_algo_order_by_client_id(symbol, cid)
    except Exception:
        row = None
    if row is None:
        try:
            row = self._find_semantic_open_algo(
                symbol,
                client_suffix=suffix,
                position_side=position_side,
                order_type=order_type,
                side=side,
                close_position=close_position,
            )
        except Exception:
            row = None
    return row


def _place_main_tp(self, *, symbol: str, side: str, close_side: str, cid_tp: str, tp_price: float) -> None:
    resp = self._binance.new_order(
        symbol=symbol,
        side=close_side,
        type="TAKE_PROFIT_MARKET",
        stopPrice=float(tp_price),
        closePosition=True,
        workingType=str(getattr(self.p, "working_type", "MARK_PRICE") or "MARK_PRICE"),
        newClientOrderId=cid_tp,
        positionSide=str(side).upper(),
    )
    try:
        self._upsert_algo_order_shadow(resp, pos_uid=None, strategy_id=self.STRATEGY_ID)
    except Exception:
        pass
    try:
        self._mark_local_algo_open(
            cid_tp,
            symbol=symbol,
            position_side=str(side).upper(),
            order_type="TAKE_PROFIT_MARKET",
            side=close_side,
            trigger_price=float(tp_price),
            close_position=True,
        )
    except Exception:
        pass


def _place_main_trailing(self, *, symbol: str, side: str, close_side: str, cid_trl: str, qty: float, activation: float) -> None:
    resp = self._binance.new_order(
        symbol=symbol,
        side=close_side,
        type="TRAILING_STOP_MARKET",
        quantity=float(qty),
        activationPrice=float(activation),
        callbackRate=float(getattr(self.p, "trailing_trail_pct", 0.6) or 0.6),
        workingType=str(getattr(self.p, "working_type", "MARK_PRICE") or "MARK_PRICE"),
        newClientOrderId=cid_trl,
        positionSide=str(side).upper(),
    )
    try:
        self._upsert_algo_order_shadow(resp, pos_uid=None, strategy_id=self.STRATEGY_ID)
    except Exception:
        pass
    try:
        self._mark_local_algo_open(
            cid_trl,
            symbol=symbol,
            position_side=str(side).upper(),
            order_type="TRAILING_STOP_MARKET",
            side=close_side,
            quantity=float(qty),
            trigger_price=float(activation),
            close_position=False,
        )
    except Exception:
        pass


def _hedge_order_spec(self, *, symbol: str, hedge_side: str, hedge_entry: float, hedge_qty: float, pos_uid: str, mark: float) -> Optional[Dict[str, Any]]:
    if hedge_entry <= 0 or hedge_qty <= 0:
        return None
    ids = _main_order_ids(self, pos_uid)
    hedge_side = str(hedge_side or '').upper()
    close_side = 'BUY' if hedge_side == 'SHORT' else 'SELL'
    try:
        tick = float(self._price_tick_for_symbol(symbol) or 0.0)
    except Exception:
        tick = 0.0
    try:
        tp_pct = float(getattr(self.p, 'hedge_take_profit_pct', 0.0) or 0.0)
    except Exception:
        tp_pct = 0.0
    try:
        act_pct = float(getattr(self.p, 'hedge_trailing_activation_pct', 1.5) or 1.5)
    except Exception:
        act_pct = 1.5
    try:
        buf_pct = float(getattr(self.p, 'hedge_trailing_activation_buffer_pct', 0.25) or 0.25)
    except Exception:
        buf_pct = 0.25
    if hedge_side == 'SHORT':
        tp_price = hedge_entry * (1.0 - tp_pct / 100.0) if tp_pct > 0 else 0.0
        activation = hedge_entry * (1.0 - act_pct / 100.0)
        if mark > 0 and tick > 0:
            max_act = mark * (1.0 - buf_pct / 100.0)
            if activation >= max_act:
                activation = self._safe_price_below_mark(activation, mark, tick, buf_pct)
    else:
        tp_price = hedge_entry * (1.0 + tp_pct / 100.0) if tp_pct > 0 else 0.0
        activation = hedge_entry * (1.0 + act_pct / 100.0)
        if mark > 0 and tick > 0:
            min_act = mark * (1.0 + buf_pct / 100.0)
            if activation <= min_act:
                activation = self._safe_price_above_mark(activation, mark, tick, buf_pct)
    if tick > 0:
        if tp_price > 0:
            tp_price = float(_ensure_tp_trail_side(
                _round_price_to_tick(tp_price, tick, mode=('down' if hedge_side == 'SHORT' else 'up')),
                hedge_entry,
                tick,
                hedge_side,
                kind='hedge_tp',
            ))
        activation = float(_ensure_tp_trail_side(
            _round_price_to_tick(activation, tick, mode=('down' if hedge_side == 'SHORT' else 'up')),
            hedge_entry,
            tick,
            hedge_side,
            kind='hedge_trail_activate',
        ))
    return {
        'cid_tp': ids['hedge_tp'],
        'cid_trl': ids['hedge_trl'],
        'close_side': close_side,
        'tp_price': float(tp_price or 0.0),
        'activation': float(activation or 0.0),
        'qty': float(hedge_qty),
        'tick': float(tick or 0.0),
    }


def _ensure_live_main_tp_trailing(self, pos: Dict[str, Any], avg_price: float) -> bool:
    if not getattr(self, '_is_live', False) or getattr(self, '_binance', None) is None:
        return False
    symbol = str(pos.get('symbol') or '').upper().strip()
    side = str(pos.get('side') or '').upper().strip()
    pos_uid = str(pos.get('pos_uid') or '').strip()
    if not symbol or side not in ('LONG', 'SHORT') or not pos_uid or avg_price <= 0:
        return False
    close_side = 'SELL' if side == 'LONG' else 'BUY'
    ids = _main_order_ids(self, pos_uid)
    try:
        tick = float(self._price_tick_for_symbol(symbol) or 0.0)
    except Exception:
        tick = 0.0
    mark = float(self._get_mark_price_live(symbol, side) or 0.0)
    tp_pct = float(getattr(self.p, 'take_profit_pct', 0.0) or 0.0)
    if side == 'LONG':
        tp_price = avg_price * (1.0 + tp_pct / 100.0)
        activation = avg_price * (1.0 + float(getattr(self.p, 'trailing_activation_pct', 0.0) or 0.0) / 100.0)
        if mark > 0 and tick > 0:
            activation = self._safe_price_above_mark(activation, mark, tick, float(getattr(self.p, 'trailing_activation_buffer_pct', 0.2) or 0.2))
    else:
        tp_price = avg_price * (1.0 - tp_pct / 100.0)
        activation = avg_price * (1.0 - float(getattr(self.p, 'trailing_activation_pct', 0.0) or 0.0) / 100.0)
        if mark > 0 and tick > 0:
            activation = self._safe_price_below_mark(activation, mark, tick, float(getattr(self.p, 'trailing_activation_buffer_pct', 0.2) or 0.2))
    if tick > 0:
        tp_price = float(_ensure_tp_trail_side(_round_price_to_tick(tp_price, tick, mode=('up' if side == 'LONG' else 'down')), avg_price, tick, side, kind='tp'))
        activation = float(_ensure_tp_trail_side(_round_price_to_tick(activation, tick, mode=('up' if side == 'LONG' else 'down')), avg_price, tick, side, kind='trail_activate'))
    changed = False
    tp_existing = _find_algo_any(self, symbol, ids['tp'], suffix='_TP', position_side=side, order_type='TAKE_PROFIT_MARKET', side=close_side, close_position=True)
    tp_need = True
    if isinstance(tp_existing, dict):
        cur_tp = _algo_trigger_price(tp_existing)
        tp_need = not (cur_tp > 0 and abs(cur_tp - tp_price) <= _reissue_tolerance(self, tp_price, tick))
        if tp_need:
            self._cancel_algo_by_client_id_safe(symbol, str(tp_existing.get('clientAlgoId') or ids['tp']))
            changed = True
    if tp_need:
        _place_main_tp(self, symbol=symbol, side=side, close_side=close_side, cid_tp=ids['tp'], tp_price=tp_price)
        changed = True
    if bool(getattr(self.p, 'trailing_enabled', False)):
        qty = float(self._get_position_amt_live(symbol, side) or abs(float(pos.get('qty_current') or 0.0)) or 0.0)
        trl_existing = _find_algo_any(self, symbol, ids['trl'], suffix='_TRL', position_side=side, order_type='TRAILING_STOP_MARKET', side=close_side)
        trl_need = qty > 0
        if isinstance(trl_existing, dict) and qty > 0:
            cur_act = _algo_trigger_price(trl_existing)
            cur_qty = _algo_quantity(trl_existing)
            qty_tol = max(float(getattr(self.p, 'hedge_trailing_qty_tolerance', 1e-8) or 1e-8), 1e-12)
            trl_need = not (
                cur_act > 0 and abs(cur_act - activation) <= _reissue_tolerance(self, activation, tick)
                and cur_qty > 0 and abs(cur_qty - qty) <= qty_tol
            )
            if trl_need:
                self._cancel_algo_by_client_id_safe(symbol, str(trl_existing.get('clientAlgoId') or ids['trl']))
                changed = True
        if qty > 0 and trl_need:
            _place_main_trailing(self, symbol=symbol, side=side, close_side=close_side, cid_trl=ids['trl'], qty=qty, activation=activation)
            changed = True
    return changed


def _live_reanchor_tp_and_trailing_after_avg_change(self, pos: Dict[str, Any], avg_price: float) -> bool:
    try:
        return bool(_ensure_live_main_tp_trailing(self, pos, float(avg_price or 0.0)))
    except Exception:
        log.exception('[TL][REANCHOR] failed for pos_uid=%s', str((pos or {}).get('pos_uid') or ''))
        return False


def _ensure_live_hedge_tp_trailing(self, *, base_pos: Dict[str, Any], symbol: str, hedge_side: str, hedge_entry: float, hedge_qty: float) -> bool:
    if not getattr(self, '_is_live', False) or getattr(self, '_binance', None) is None:
        return False
    pos_uid = str(base_pos.get('pos_uid') or '').strip()
    if not pos_uid:
        return False
    mark = float(self._get_mark_price_live(symbol, hedge_side) or 0.0)
    spec = _hedge_order_spec(self, symbol=symbol, hedge_side=hedge_side, hedge_entry=float(hedge_entry), hedge_qty=float(hedge_qty), pos_uid=pos_uid, mark=mark)
    if not spec:
        return False
    changed = False
    close_side = str(spec['close_side'])
    tick = float(spec['tick'])
    qty_tol = max(float(getattr(self.p, 'hedge_trailing_qty_tolerance', 1e-8) or 1e-8), 1e-12)
    if float(spec['tp_price']) > 0 and bool(getattr(self.p, 'hedge_take_profit_enabled', True)):
        tp_existing = _find_algo_any(self, symbol, spec['cid_tp'], suffix='_HEDGE_TP', position_side=hedge_side, order_type='TAKE_PROFIT_MARKET', side=close_side)
        tp_need = True
        if isinstance(tp_existing, dict):
            cur_tp = _algo_trigger_price(tp_existing)
            cur_qty = _algo_quantity(tp_existing)
            tp_match = cur_tp > 0 and abs(cur_tp - float(spec['tp_price'])) <= _reissue_tolerance(self, float(spec['tp_price']), tick)
            qty_match = cur_qty > 0 and abs(cur_qty - float(spec['qty'])) <= qty_tol
            tp_need = not (tp_match and qty_match)
            if tp_need:
                self._cancel_algo_by_client_id_safe(symbol, str(tp_existing.get('clientAlgoId') or spec['cid_tp']))
                changed = True
        if tp_need:
            resp = self._binance.new_order(
                symbol=symbol,
                side=close_side,
                type='TAKE_PROFIT_MARKET',
                stopPrice=float(spec['tp_price']),
                quantity=float(spec['qty']),
                workingType=str(getattr(self.p, 'hedge_take_profit_working_type', getattr(self.p, 'hedge_trailing_working_type', 'MARK_PRICE')) or 'MARK_PRICE'),
                newClientOrderId=str(spec['cid_tp']),
                positionSide=str(hedge_side).upper(),
            )
            try:
                self._upsert_algo_order_shadow(resp, pos_uid=pos_uid, strategy_id=self.STRATEGY_ID)
            except Exception:
                pass
            try:
                self._mark_local_algo_open(str(spec['cid_tp']), symbol=symbol, position_side=str(hedge_side).upper(), order_type='TAKE_PROFIT_MARKET', side=close_side, quantity=float(spec['qty']), trigger_price=float(spec['tp_price']), close_position=False, pos_uid=pos_uid)
            except Exception:
                pass
            changed = True
    if bool(getattr(self.p, 'hedge_trailing_enabled', False)) and float(spec['activation']) > 0 and float(spec['qty']) > 0:
        trl_existing = _find_algo_any(self, symbol, spec['cid_trl'], suffix='_HEDGE_TRL', position_side=hedge_side, order_type='TRAILING_STOP_MARKET', side=close_side)
        trl_need = True
        reissue_on_avg = bool(getattr(self.p, 'hedge_trailing_reissue_on_avg_entry_change', True))
        if isinstance(trl_existing, dict):
            cur_act = _algo_trigger_price(trl_existing)
            cur_qty = _algo_quantity(trl_existing)
            act_match = cur_act > 0 and abs(cur_act - float(spec['activation'])) <= _reissue_tolerance(self, float(spec['activation']), tick)
            qty_match = cur_qty > 0 and abs(cur_qty - float(spec['qty'])) <= qty_tol
            trl_need = not ((act_match if reissue_on_avg else True) and qty_match)
            if trl_need:
                self._cancel_algo_by_client_id_safe(symbol, str(trl_existing.get('clientAlgoId') or spec['cid_trl']))
                changed = True
        if trl_need:
            resp = self._binance.new_order(
                symbol=symbol,
                side=close_side,
                type='TRAILING_STOP_MARKET',
                quantity=float(spec['qty']),
                positionSide=str(hedge_side).upper(),
                newClientOrderId=str(spec['cid_trl']),
                activationPrice=float(spec['activation']),
                callbackRate=float(getattr(self.p, 'hedge_trailing_trail_pct', 0.5) or 0.5),
                workingType=str(getattr(self.p, 'hedge_trailing_working_type', 'MARK_PRICE') or 'MARK_PRICE'),
            )
            try:
                self._upsert_algo_order_shadow(resp, pos_uid=pos_uid, strategy_id=self.STRATEGY_ID)
            except Exception:
                pass
            try:
                self._mark_local_algo_open(str(spec['cid_trl']), symbol=symbol, position_side=str(hedge_side).upper(), order_type='TRAILING_STOP_MARKET', side=close_side, quantity=float(spec['qty']), trigger_price=float(spec['activation']), close_position=False, pos_uid=pos_uid)
            except Exception:
                pass
            changed = True
    return changed


def _maintain_live_hedge_exit_orders(self) -> Dict[str, int]:
    stats = {'checked': 0, 'changed': 0, 'canceled': 0}
    if not getattr(self, '_is_live', False) or not bool(getattr(self.p, 'hedge_enabled', False)):
        return stats

    rows = list(self.store.query_dict(
        """
        SELECT symbol_id, symbol, pos_uid, side, qty_current, avg_price, entry_price, status, raw_meta, updated_at
        FROM public.position_ledger
        WHERE exchange_id = %(exchange_id)s
          AND account_id = %(account_id)s
          AND strategy_id = %(strategy_id)s
          AND COALESCE(source, 'live') = 'live'
          AND status = 'OPEN'
        ORDER BY updated_at DESC NULLS LAST, opened_at DESC NULLS LAST
        """,
        {
            'exchange_id': int(self.exchange_id),
            'account_id': int(self.account_id),
            'strategy_id': str(getattr(self, 'STRATEGY_ID', 'trade_liquidation')),
        },
    ))

    pr_map = _position_risk_map(list(self._rest_snapshot_get('position_risk') or []))
    open_algos = list(self._rest_snapshot_get('open_algo_orders_all') or [])

    desired_by_pair: Dict[Tuple[str, str], Dict[str, Any]] = {}
    seen_pairs: set[Tuple[str, str]] = set()

    for row in rows:
        symbol = str(row.get('symbol') or '').upper().strip()
        side = str(row.get('side') or '').upper().strip()
        pos_uid = str(row.get('pos_uid') or '').strip()
        if not symbol or side not in ('LONG', 'SHORT') or not pos_uid:
            continue

        pair = (symbol, side)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)

        main_pos = pr_map.get((symbol, side))
        if not isinstance(main_pos, dict):
            continue
        try:
            main_qty = abs(float(main_pos.get('positionAmt') or 0.0))
        except Exception:
            main_qty = 0.0
        if main_qty <= 0:
            continue

        hedge_side = _opp_side(side)
        hedge_pos = pr_map.get((symbol, hedge_side))
        if not isinstance(hedge_pos, dict):
            continue
        try:
            hedge_qty = abs(float(hedge_pos.get('positionAmt') or 0.0))
        except Exception:
            hedge_qty = 0.0
        try:
            hedge_entry = abs(float(hedge_pos.get('entryPrice') or hedge_pos.get('avgEntryPrice') or 0.0))
        except Exception:
            hedge_entry = 0.0
        if hedge_qty <= 0 or hedge_entry <= 0:
            continue

        desired_by_pair[(symbol, hedge_side)] = {
            'row': row,
            'hedge_entry': float(hedge_entry),
            'hedge_qty': float(hedge_qty),
            'spec': _hedge_order_spec(
                self,
                symbol=symbol,
                hedge_side=hedge_side,
                hedge_entry=float(hedge_entry),
                hedge_qty=float(hedge_qty),
                pos_uid=pos_uid,
                mark=float(self._get_mark_price_live(symbol, hedge_side) or 0.0),
            ),
        }

    # Cancel stale or duplicate hedge exit algos before ensuring desired ones.
    for algo in open_algos:
        if not isinstance(algo, dict):
            continue
        symbol = str(algo.get('symbol') or '').upper().strip()
        cid = str(algo.get('clientAlgoId') or '').strip()
        if not symbol or not cid:
            continue
        cid_u = cid.upper()
        is_h_tp = cid_u.endswith('_HEDGE_TP')
        is_h_trl = cid_u.endswith('_HEDGE_TRL')
        if not (is_h_tp or is_h_trl):
            continue
        ps = str(algo.get('positionSide') or '').upper().strip()
        pair = (symbol, ps)
        desired = desired_by_pair.get(pair)
        cancel = False
        if not desired or not isinstance(desired.get('spec'), dict):
            cancel = True
        else:
            spec = desired['spec']
            want_cid = str(spec['cid_tp'] if is_h_tp else spec['cid_trl'])
            if cid != want_cid:
                cancel = True
            else:
                want_type = 'TAKE_PROFIT_MARKET' if is_h_tp else 'TRAILING_STOP_MARKET'
                have_type = str(algo.get('orderType') or algo.get('type') or '').upper().strip()
                if have_type and have_type != want_type:
                    cancel = True
        if cancel:
            try:
                removed = self._cancel_algo_by_client_id_safe(symbol, cid, str(algo.get('algoId') or ''))
                if removed:
                    stats['canceled'] += 1
                    stats['changed'] += 1
                    log.info('[TL][HEDGE_EXIT] canceled stale hedge algo %s %s cid=%s', symbol, ps or '?', cid)
                else:
                    log.warning('[TL][HEDGE_EXIT] stale hedge algo still open after cancel attempt %s %s cid=%s', symbol, ps or '?', cid)
            except Exception:
                log.exception('[TL][HEDGE_EXIT] failed to cancel stale hedge algo %s %s cid=%s', symbol, ps or '?', cid)

    # Cancel legacy closePosition hedge TP/TRL that belong to the active hedge side but use non-hedge suffixes.
    for (symbol, hedge_side), payload in desired_by_pair.items():
        spec = payload.get('spec') or {}
        close_side = str((spec or {}).get('close_side') or ('BUY' if hedge_side == 'SHORT' else 'SELL')).upper()
        for suffix, order_type in (('_TP', 'TAKE_PROFIT_MARKET'), ('_TRL', 'TRAILING_STOP_MARKET')):
            try:
                legacy = self._find_semantic_open_algo(
                    symbol,
                    client_suffix=suffix,
                    position_side=hedge_side,
                    order_type=order_type,
                    side=close_side,
                )
            except Exception:
                legacy = None
            if isinstance(legacy, dict):
                legacy_cid = str(legacy.get('clientAlgoId') or '').strip()
                if legacy_cid and '_HEDGE_' not in legacy_cid.upper():
                    try:
                        removed = self._cancel_algo_by_client_id_safe(symbol, legacy_cid, str(legacy.get('algoId') or ''))
                        if removed:
                            stats['canceled'] += 1
                            stats['changed'] += 1
                            log.info('[TL][HEDGE_EXIT] canceled legacy hedge semantic duplicate %s %s cid=%s', symbol, hedge_side, legacy_cid)
                        else:
                            log.warning('[TL][HEDGE_EXIT] legacy hedge semantic duplicate still open after cancel attempt %s %s cid=%s', symbol, hedge_side, legacy_cid)
                    except Exception:
                        log.exception('[TL][HEDGE_EXIT] failed to cancel legacy hedge semantic duplicate %s %s cid=%s', symbol, hedge_side, legacy_cid)

    for (symbol, hedge_side), payload in desired_by_pair.items():
        row = payload.get('row') or {}
        spec = payload.get('spec') or {}
        pos_uid = str(row.get('pos_uid') or '').strip()
        hedge_entry = float(payload.get('hedge_entry') or 0.0)
        hedge_qty = float(payload.get('hedge_qty') or 0.0)
        if not pos_uid or not isinstance(spec, dict) or hedge_entry <= 0 or hedge_qty <= 0:
            continue
        stats['checked'] += 1
        try:
            if _ensure_live_hedge_tp_trailing(
                self,
                base_pos=row,
                symbol=symbol,
                hedge_side=hedge_side,
                hedge_entry=hedge_entry,
                hedge_qty=hedge_qty,
            ):
                stats['changed'] += 1
        except Exception:
            log.exception('[TL][HEDGE_EXIT] failed for %s %s pos_uid=%s', symbol, hedge_side, pos_uid)
    return stats

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
    side = str(side or "").upper().strip()
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
            return {"broken_closed_at": 0, "broken_updated_at": 0, "broken_closed_gt_updated": 0}
        if isinstance(row, tuple):
            return {
                "broken_closed_at": int((row[0] if len(row) > 0 else 0) or 0),
                "broken_updated_at": int((row[1] if len(row) > 1 else 0) or 0),
                "broken_closed_gt_updated": int((row[2] if len(row) > 2 else 0) or 0),
            }
        return {
            "broken_closed_at": int(row.get("broken_closed_at") or 0),
            "broken_updated_at": int(row.get("broken_updated_at") or 0),
            "broken_closed_gt_updated": int(row.get("broken_closed_gt_updated") or 0),
        }
    except Exception:
        log.exception("[TL][ledger_guard] periodic anomaly check failed")
        return {"broken_closed_at": 0, "broken_updated_at": 0, "broken_closed_gt_updated": 0}


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
    hedge_pos_uid = str(hedge_block.get("hedge_pos_uid") or "").strip() or str(uuid.uuid4())
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
        "exchange_id","account_id","symbol_id","pos_uid","strategy_id","strategy_name","side","status",
        "opened_at","entry_price","avg_price","qty_opened","qty_current","position_value_usdt",
        "scale_in_count","updated_at","source",
    ]
    values = [
        "%(exchange_id)s","%(account_id)s","%(symbol_id)s","%(pos_uid)s","%(strategy_id)s","%(strategy_name)s",
        "%(side)s","'OPEN'","%(opened_at)s","%(entry_price)s","%(avg_price)s","%(qty_opened)s","%(qty_current)s",
        "%(position_value_usdt)s","0","%(updated_at)s","'live_hedge'",
    ]
    if getattr(self, "_pl_has_raw_meta", False):
        columns.append("raw_meta")
        values.append("%(raw_meta)s::jsonb")

    sql = f"INSERT INTO public.position_ledger ({', '.join(columns)}) VALUES ({', '.join(values)}) ON CONFLICT DO NOTHING"
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
                exchange_id, base_account_id, hedge_account_id, symbol_id,
                base_pos_uid, hedge_pos_uid, hedge_ratio, created_at
            )
            VALUES (
                %(exchange_id)s, %(account_id)s, %(account_id)s, %(symbol_id)s,
                %(base_pos_uid)s, %(hedge_pos_uid)s, %(hedge_ratio)s, NOW()
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
        str(symbol).upper(), str(hedge_side).upper(), float(hedge_qty),
        str(main_row.get("pos_uid") or ""), str(hedge_pos_uid),
    )
    return hedge_pos_uid


def _close_live_hedge_ledger_row(self, *, hedge_pos_uid: str, symbol: str, hedge_side: str, exit_price: float) -> None:
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
        str(symbol).upper(), str(hedge_side).upper(), str(hedge_pos_uid), float(exit_price or 0.0),
    )


def _promote_live_hedge_to_live_main(
    self,
    *,
    hedge_row: Dict[str, Any],
    symbol: str,
    side: str,
    qty_current: float,
    entry_price: float,
    position_value_usdt: float,
) -> bool:
    meta = _safe_json_dict(hedge_row.get("raw_meta"))
    link = meta.get("hedge_link") if isinstance(meta.get("hedge_link"), dict) else {}
    promoted_meta = dict(meta)
    promoted_meta["promoted_from_live_hedge"] = {
        "ts": _utc_now().isoformat(),
        "old_base_pos_uid": str(link.get("base_pos_uid") or ""),
        "old_hedge_pos_uid": str(hedge_row.get("pos_uid") or ""),
        "symbol": str(symbol).upper(),
        "side": str(side).upper(),
    }
    promoted_meta.pop("hedge_link", None)
    promoted_meta.pop("hedge", None)

    try:
        params = {
            "exchange_id": int(self.exchange_id),
            "account_id": int(self.account_id),
            "pos_uid": str(hedge_row.get("pos_uid") or ""),
            "qty_current": float(qty_current),
            "entry_price": float(entry_price),
            "avg_price": float(entry_price),
            "position_value_usdt": float(position_value_usdt),
            "raw_meta": json.dumps(promoted_meta, ensure_ascii=False),
        }
        if getattr(self, "_pl_has_raw_meta", False):
            self.store.execute(
                """
                UPDATE public.position_ledger
                   SET source = 'live',
                       qty_current = %(qty_current)s,
                       qty_opened = CASE
                           WHEN COALESCE(qty_opened, 0) <= 0 THEN %(qty_current)s
                           ELSE qty_opened
                       END,
                       entry_price = CASE
                           WHEN COALESCE(%(entry_price)s, 0) > 0 THEN %(entry_price)s
                           ELSE entry_price
                       END,
                       avg_price = CASE
                           WHEN COALESCE(%(avg_price)s, 0) > 0 THEN %(avg_price)s
                           ELSE avg_price
                       END,
                       position_value_usdt = %(position_value_usdt)s,
                       raw_meta = %(raw_meta)s::jsonb,
                       updated_at = NOW()
                 WHERE exchange_id = %(exchange_id)s
                   AND account_id = %(account_id)s
                   AND pos_uid = %(pos_uid)s
                   AND source = 'live_hedge'
                   AND status = 'OPEN'
                """,
                params,
            )
        else:
            self.store.execute(
                """
                UPDATE public.position_ledger
                   SET source = 'live',
                       qty_current = %(qty_current)s,
                       qty_opened = CASE
                           WHEN COALESCE(qty_opened, 0) <= 0 THEN %(qty_current)s
                           ELSE qty_opened
                       END,
                       entry_price = CASE
                           WHEN COALESCE(%(entry_price)s, 0) > 0 THEN %(entry_price)s
                           ELSE entry_price
                       END,
                       avg_price = CASE
                           WHEN COALESCE(%(avg_price)s, 0) > 0 THEN %(avg_price)s
                           ELSE avg_price
                       END,
                       position_value_usdt = %(position_value_usdt)s,
                       updated_at = NOW()
                 WHERE exchange_id = %(exchange_id)s
                   AND account_id = %(account_id)s
                   AND pos_uid = %(pos_uid)s
                   AND source = 'live_hedge'
                   AND status = 'OPEN'
                """,
                params,
            )
        try:
            self.store.execute(
                """
                DELETE FROM public.hedge_links
                 WHERE exchange_id = %(exchange_id)s
                   AND hedge_account_id = %(account_id)s
                   AND hedge_pos_uid = %(pos_uid)s
                """,
                {
                    "exchange_id": int(self.exchange_id),
                    "account_id": int(self.account_id),
                    "pos_uid": str(hedge_row.get("pos_uid") or ""),
                },
            )
        except Exception:
            pass

        log.info(
            "[TL][hedge_ready] promoted live_hedge -> live main symbol=%s side=%s pos_uid=%s",
            str(symbol).upper(),
            str(side).upper(),
            str(hedge_row.get("pos_uid") or ""),
        )
        return True
    except Exception:
        log.exception(
            "[TL][hedge_ready] failed to promote live_hedge -> live main symbol=%s side=%s pos_uid=%s",
            str(symbol).upper(),
            str(side).upper(),
            str(hedge_row.get("pos_uid") or ""),
        )
        return False


def _sync_live_main_positions_from_position_risk(self) -> Dict[str, int]:
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
            SELECT symbol_id, symbol, pos_uid, side, source, status
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

    checked = opened = skipped = 0

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
            "exchange_id","account_id","symbol_id","symbol","pos_uid","strategy_id","strategy_name","side","status",
            "opened_at","entry_price","avg_price","qty_opened","qty_current","position_value_usdt","scale_in_count","updated_at","source",
        ]
        values = [
            "%(exchange_id)s","%(account_id)s","%(symbol_id)s","%(symbol)s","%(pos_uid)s","%(strategy_id)s","%(strategy_name)s","%(side)s","'OPEN'",
            "%(opened_at)s","%(entry_price)s","%(avg_price)s","%(qty_opened)s","%(qty_current)s","%(position_value_usdt)s","0","%(updated_at)s","'live'",
        ]
        if getattr(self, "_pl_has_raw_meta", False):
            columns.append("raw_meta")
            values.append("%(raw_meta)s::jsonb")
        sql = f"INSERT INTO public.position_ledger ({', '.join(columns)}) VALUES ({', '.join(values)}) ON CONFLICT DO NOTHING"
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
                symbol, side, float(qty), float(entry_price or mark_price or 0.0), pos_uid,
            )
        except Exception:
            skipped += 1
            log.exception("[TL][orphan_ready] failed to materialize missing live position symbol=%s side=%s", symbol, side)

    return {"checked": checked, "opened": opened, "skipped": skipped}


def _sync_live_hedge_legs_from_position_risk(self) -> Dict[str, int]:
    if not bool(getattr(self.p, "hedge_enabled", False)):
        return {"checked": 0, "opened": 0, "closed": 0, "promoted": 0}
    try:
        position_risk = getattr(self, "_last_position_risk", None)
        if not position_risk:
            position_risk = self._rest_snapshot_get("position_risk") or []
    except Exception:
        position_risk = []
    if not isinstance(position_risk, list) or not position_risk:
        return {"checked": 0, "opened": 0, "closed": 0, "promoted": 0}

    risk_map = _position_risk_map(position_risk)
    symbols_map = self._symbols_map()

    main_rows = list(
        self.store.query_dict(
            """
            SELECT exchange_id, account_id, symbol_id, symbol, pos_uid, side, qty_current, avg_price, entry_price, raw_meta
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
            SELECT exchange_id, account_id, symbol_id, symbol, pos_uid, side, qty_current, entry_price, avg_price, raw_meta
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
        side = str(row.get("side") or "").upper().strip()
        if base_pos_uid and symbol_id > 0 and side:
            hedge_by_base[(symbol_id, base_pos_uid, side)] = row

    checked = opened = closed = promoted = 0

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

    live_main_keys = {(int(row.get("symbol_id") or 0), str(row.get("pos_uid") or "")) for row in main_rows}

    for row in hedge_rows:
        meta = _safe_json_dict(row.get("raw_meta"))
        link = meta.get("hedge_link") if isinstance(meta.get("hedge_link"), dict) else {}
        base_pos_uid = str(link.get("base_pos_uid") or "").strip()
        symbol_id = int(row.get("symbol_id") or 0)
        symbol = str(row.get("symbol") or symbols_map.get(symbol_id) or "").upper().strip()
        side = str(row.get("side") or "").upper().strip()
        if not symbol_id or not base_pos_uid:
            continue
        if (symbol_id, base_pos_uid) in live_main_keys:
            continue

        risk_row = risk_map.get((symbol, side))
        hedge_amt = abs(_safe_float((risk_row or {}).get("positionAmt"), 0.0))
        hedge_entry = _safe_float((risk_row or {}).get("entryPrice"), 0.0)
        hedge_mark = _safe_float((risk_row or {}).get("markPrice"), 0.0)
        hedge_notional = abs(_safe_float((risk_row or {}).get("notional"), 0.0))

        if hedge_amt > 0:
            if _promote_live_hedge_to_live_main(
                self,
                hedge_row=row,
                symbol=symbol,
                side=side,
                qty_current=float(hedge_amt),
                entry_price=float(hedge_entry or hedge_mark or _safe_float(row.get("entry_price"), 0.0)),
                position_value_usdt=float(hedge_notional or (hedge_mark * hedge_amt)),
            ):
                promoted += 1
        else:
            _close_live_hedge_ledger_row(
                self,
                hedge_pos_uid=str(row.get("pos_uid") or ""),
                symbol=symbol,
                hedge_side=side,
                exit_price=float(hedge_mark or 0.0),
            )
            closed += 1

    return {"checked": checked, "opened": opened, "closed": closed, "promoted": promoted}


def _enforce_add_or_hedge_invariant(self) -> Dict[str, int]:
    if not getattr(self, "_is_live", False):
        return {"checked": 0, "acted": 0, "skipped_two_sided": 0}
    if not bool(getattr(getattr(self, "p", object()), "averaging_enabled", False)):
        return {"checked": 0, "acted": 0, "skipped_two_sided": 0}
    if int(getattr(self, "_cfg_max_adds")() or 0) <= 0:
        return {"checked": 0, "acted": 0, "skipped_two_sided": 0}

    rows = list(
        self.store.query_dict(
            """
            SELECT symbol_id, symbol, pos_uid, side, qty_current, avg_price, entry_price,
                   scale_in_count, raw_meta, source, opened_at, updated_at
            FROM public.position_ledger
            WHERE exchange_id = %(exchange_id)s
              AND account_id = %(account_id)s
              AND strategy_id = %(strategy_id)s
              AND source = 'live'
              AND status = 'OPEN'
            ORDER BY updated_at DESC
            """,
            {
                "exchange_id": int(self.exchange_id),
                "account_id": int(self.account_id),
                "strategy_id": str(getattr(self, "STRATEGY_ID", "trade_liquidation")),
            },
        )
    )

    by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        sym = str(row.get("symbol") or "").upper().strip()
        side = str(row.get("side") or "").upper().strip()
        if sym and side in ("LONG", "SHORT"):
            by_symbol.setdefault(sym, []).append(row)

    checked = acted = skipped_two_sided = 0
    for sym, sym_rows in by_symbol.items():
        live_sides = {str(r.get("side") or "").upper().strip() for r in sym_rows}
        if "LONG" in live_sides and "SHORT" in live_sides:
            skipped_two_sided += len(sym_rows)
            continue
        row = sym_rows[0]
        checked += 1
        try:
            if bool(self._live_ensure_next_add_order(pos=row)):
                acted += 1
        except Exception:
            log.exception("[TL][add_or_hedge] failed invariant for %s %s pos_uid=%s", sym, str(row.get("side") or ""), str(row.get("pos_uid") or ""))

    return {"checked": checked, "acted": acted, "skipped_two_sided": skipped_two_sided}


def apply_trade_liquidation_full_ready(TradeLiquidation) -> None:

    TradeLiquidation._sync_live_main_positions_from_position_risk = _sync_live_main_positions_from_position_risk  # type: ignore[attr-defined]
    TradeLiquidation._position_ledger_time_anomalies = staticmethod(_position_ledger_time_anomalies)  # type: ignore[attr-defined]
    TradeLiquidation._sync_live_hedge_legs_from_position_risk = _sync_live_hedge_legs_from_position_risk  # type: ignore[attr-defined]
    TradeLiquidation._insert_live_hedge_ledger_row = _insert_live_hedge_ledger_row  # type: ignore[attr-defined]
    TradeLiquidation._close_live_hedge_ledger_row = _close_live_hedge_ledger_row  # type: ignore[attr-defined]
    TradeLiquidation._promote_live_hedge_to_live_main = _promote_live_hedge_to_live_main  # type: ignore[attr-defined]
    TradeLiquidation._live_reanchor_tp_and_trailing_after_avg_change = _live_reanchor_tp_and_trailing_after_avg_change  # type: ignore[attr-defined]
    TradeLiquidation._maintain_live_hedge_exit_orders = _maintain_live_hedge_exit_orders  # type: ignore[attr-defined]

    original_auto_recovery_brackets = TradeLiquidation._auto_recovery_brackets

    def _wrapped_auto_recovery_brackets(self):
        try:
            if self._is_live:
                _ensure_position_ledger_time_guard(self.store)
                anomalies_before = _position_ledger_time_anomalies(self.store)
                main_sync_result = self._sync_live_main_positions_from_position_risk()
                hedge_sync_result = self._sync_live_hedge_legs_from_position_risk() if bool(getattr(self.p, "hedge_enabled", False)) else {"checked": 0, "opened": 0, "closed": 0, "promoted": 0}
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
        except Exception:
            log.exception("[TL][hedge_ready] pre-recovery hedge sync failed")

        result = original_auto_recovery_brackets(self)

        try:
            if self._is_live and bool(getattr(self.p, "hedge_enabled", False)):
                hedge_exit = self._maintain_live_hedge_exit_orders()
                try:
                    self._last_hedge_exit_maintenance = dict(hedge_exit)
                except Exception:
                    pass
        except Exception:
            log.exception("[TL][HEDGE_EXIT] recovery maintenance failed")

        try:
            if self._is_live and bool(getattr(getattr(self, "p", object()), "add_or_hedge_invariant_enabled", True)):
                inv = _enforce_add_or_hedge_invariant(self)
                try:
                    self._last_add_or_hedge_invariant = dict(inv)
                except Exception:
                    pass
        except Exception:
            log.exception("[TL][add_or_hedge] post-recovery invariant failed")
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
                hedge_sync_result = self._sync_live_hedge_legs_from_position_risk() if bool(getattr(self.p, "hedge_enabled", False)) else {"checked": 0, "opened": 0, "closed": 0, "promoted": 0}
                try:
                    self._sync_positions_tables_from_position_risk()
                except Exception:
                    log.exception("[TL][hedge_ready] positions sync failed after hedge sync")
                inv = {"checked": 0, "acted": 0, "skipped_two_sided": 0}
                try:
                    if bool(getattr(getattr(self, "p", object()), "add_or_hedge_invariant_enabled", True)):
                        inv = _enforce_add_or_hedge_invariant(self)
                except Exception:
                    log.exception("[TL][add_or_hedge] post-open_positions invariant failed")
                hedge_exit = {"checked": 0, "changed": 0}
                try:
                    if bool(getattr(self.p, "hedge_enabled", False)):
                        hedge_exit = self._maintain_live_hedge_exit_orders()
                        try:
                            self._last_hedge_exit_maintenance = dict(hedge_exit)
                        except Exception:
                            pass
                except Exception:
                    log.exception("[TL][HEDGE_EXIT] process_open_positions maintenance failed")
                if isinstance(result, dict):
                    result["orphan_main_sync_checked"] = int(main_sync_result.get("checked", 0))
                    result["orphan_main_sync_opened"] = int(main_sync_result.get("opened", 0))
                    result["orphan_main_sync_skipped"] = int(main_sync_result.get("skipped", 0))
                    result["hedge_sync_checked"] = int(hedge_sync_result.get("checked", 0))
                    result["hedge_sync_opened"] = int(hedge_sync_result.get("opened", 0))
                    result["hedge_sync_closed"] = int(hedge_sync_result.get("closed", 0))
                    result["hedge_sync_promoted"] = int(hedge_sync_result.get("promoted", 0))
                    result["ledger_broken_closed_at"] = int(anomalies_before.get("broken_closed_at", 0))
                    result["ledger_broken_updated_at"] = int(anomalies_before.get("broken_updated_at", 0))
                    result["ledger_broken_closed_gt_updated"] = int(anomalies_before.get("broken_closed_gt_updated", 0))
                    result["add_or_hedge_checked"] = int(inv.get("checked", 0))
                    result["add_or_hedge_acted"] = int(inv.get("acted", 0))
                    result["add_or_hedge_skipped_two_sided"] = int(inv.get("skipped_two_sided", 0))
                    result["hedge_exit_checked"] = int(hedge_exit.get("checked", 0))
                    result["hedge_exit_changed"] = int(hedge_exit.get("changed", 0))
        except Exception:
            log.exception("[TL][hedge_ready] wrapped process_open_positions failed")
        return result

    TradeLiquidation._process_open_positions = _wrapped_process_open_positions  # type: ignore[assignment]
    log.info("[TL][hedge_ready] full ready module connected")

