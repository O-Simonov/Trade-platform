#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_close_positions_manual.py

Ручное закрытие фьючерсных позиций Binance USDM по рынку
для выбранного списка символов.

Что делает:
1) Загружает API ключи из .env
2) Получает список открытых позиций
3) Печатает все открытые символы с направлением позиции
4) Берёт список CLOSE_SYMBOLS из настроек ниже
5) Закрывает только выбранные позиции MARKET-ордером
6) Поддерживает dry-run для безопасной проверки

Поддерживаемые переменные окружения:
- BINANCE_BASE_MAIN_API_KEY
- BINANCE_BASE_MAIN_API_SECRET

Запуск:
    python .\run_close_positions_manual.py
"""

from __future__ import annotations

import hashlib
import hmac
import os
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests


# =============================================================================
# НАСТРОЙКИ
# =============================================================================

CLOSE_SYMBOLS: List[str] = [
    # "APTUSDT",
    # "CHZUSDT",
]

DRY_RUN: bool = True
AUTO_CONFIRM: bool = False
PRINT_ALL_OPEN_POSITIONS: bool = True

RECV_WINDOW: int = 5000
BASE_URL: str = "https://fapi.binance.com"

ENV_CANDIDATES = [
    ".env",
    "../.env",
    "../../.env",
]


def load_dotenv_file() -> Optional[Path]:
    for candidate in ENV_CANDIDATES:
        p = Path(candidate).resolve()
        if not p.exists():
            continue
        for raw_line in p.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)
        return p
    return None


def to_decimal(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def normalize_symbol_list(symbols: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in symbols:
        if item is None:
            continue
        sym = str(item).strip().upper()
        if not sym:
            continue
        if sym not in seen:
            out.append(sym)
            seen.add(sym)
    return out


def format_decimal(value: Decimal, places: int = 8) -> str:
    q = Decimal("1").scaleb(-places)
    try:
        return str(value.quantize(q))
    except Exception:
        return str(value)


def chunked_line(title: str = "", sep: str = "-") -> str:
    if title:
        return f"{sep * 8} {title} {sep * 8}"
    return sep * 40


class BinanceFuturesRest:
    def __init__(self, api_key: str, api_secret: str, base_url: str = BASE_URL, recv_window: int = RECV_WINDOW):
        self.api_key = api_key
        self.api_secret = api_secret.encode("utf-8")
        self.base_url = base_url.rstrip("/")
        self.recv_window = recv_window
        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": api_key})
        self._time_offset_ms = 0

    def sync_time(self) -> int:
        r = self.session.get(f"{self.base_url}/fapi/v1/time", timeout=(10, 30))
        r.raise_for_status()
        data = r.json()
        server_time = int(data["serverTime"])
        local_time = int(time.time() * 1000)
        self._time_offset_ms = server_time - local_time
        return self._time_offset_ms

    def _timestamp(self) -> int:
        return int(time.time() * 1000 + self._time_offset_ms)

    def _sign(self, params: Dict[str, Any]) -> str:
        # Подпись должна считаться по тем же параметрам и в том же порядке,
        # в каком они реально идут в query string.
        qs = "&".join(f"{k}={params[k]}" for k in params)
        return hmac.new(self.api_secret, qs.encode("utf-8"), hashlib.sha256).hexdigest()

    def _signed_params(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        p = dict(params or {})
        p["recvWindow"] = self.recv_window
        p["timestamp"] = self._timestamp()
        p["signature"] = self._sign(p)
        return p

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        signed: bool = False,
        retry_on_timestamp: bool = True,
    ) -> Any:
        url = f"{self.base_url}{path}"
        request_params = self._signed_params(params) if signed else dict(params or {})
        last_error: Optional[Exception] = None

        for attempt in range(2):
            try:
                if method.upper() == "GET":
                    r = self.session.get(url, params=request_params, timeout=(10, 60))
                elif method.upper() == "POST":
                    r = self.session.post(url, params=request_params, timeout=(10, 60))
                else:
                    raise ValueError(f"Unsupported method: {method}")

                if r.status_code == 200:
                    return r.json()

                txt = r.text
                if signed and retry_on_timestamp and r.status_code == 400 and '"code":-1021' in txt and attempt == 0:
                    self.sync_time()
                    request_params = self._signed_params(params)
                    continue

                raise RuntimeError(f"binance http {r.status_code} {method} {path}: {txt}")
            except Exception as exc:
                last_error = exc
                if signed and retry_on_timestamp and attempt == 0:
                    try:
                        self.sync_time()
                        request_params = self._signed_params(params)
                        continue
                    except Exception:
                        pass
                break

        if last_error:
            raise last_error
        raise RuntimeError(f"request failed: {method} {path}")

    def get_position_risk(self) -> List[Dict[str, Any]]:
        data = self._request("GET", "/fapi/v2/positionRisk", signed=True)
        if not isinstance(data, list):
            raise RuntimeError("Unexpected /fapi/v2/positionRisk response")
        return data

    def get_exchange_info(self) -> Dict[str, Any]:
        data = self._request("GET", "/fapi/v1/exchangeInfo", signed=False)
        if not isinstance(data, dict):
            raise RuntimeError("Unexpected /fapi/v1/exchangeInfo response")
        return data

    def create_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        data = self._request("POST", "/fapi/v1/order", params=params, signed=True)
        if not isinstance(data, dict):
            raise RuntimeError("Unexpected /fapi/v1/order response")
        return data


@dataclass
class PositionInfo:
    symbol: str
    position_side: str
    qty: Decimal
    entry_price: Decimal
    mark_price: Decimal
    unrealized_pnl: Decimal

    @property
    def direction(self) -> str:
        if self.qty > 0:
            return "LONG"
        if self.qty < 0:
            return "SHORT"
        return "FLAT"

    @property
    def abs_qty(self) -> Decimal:
        return abs(self.qty)

    @property
    def close_side(self) -> Optional[str]:
        if self.qty > 0:
            return "SELL"
        if self.qty < 0:
            return "BUY"
        return None


def build_step_size_map(exchange_info: Dict[str, Any]) -> Dict[str, Decimal]:
    result: Dict[str, Decimal] = {}
    for s in exchange_info.get("symbols", []):
        symbol = s.get("symbol")
        if not symbol:
            continue
        step = None
        for flt in s.get("filters", []):
            if flt.get("filterType") == "LOT_SIZE":
                step = flt.get("stepSize")
                break
        if step:
            result[symbol] = to_decimal(step, "0")
    return result


def quantize_qty(qty: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return qty
    units = (qty / step).to_integral_value(rounding=ROUND_DOWN)
    return units * step


def load_open_positions(rest: BinanceFuturesRest) -> List[PositionInfo]:
    raw = rest.get_position_risk()
    positions: List[PositionInfo] = []
    for row in raw:
        amt = to_decimal(row.get("positionAmt"))
        if amt == 0:
            continue
        positions.append(
            PositionInfo(
                symbol=str(row.get("symbol")),
                position_side=str(row.get("positionSide") or "BOTH"),
                qty=amt,
                entry_price=to_decimal(row.get("entryPrice")),
                mark_price=to_decimal(row.get("markPrice")),
                unrealized_pnl=to_decimal(row.get("unRealizedProfit")),
            )
        )
    return positions


def print_open_positions(positions: List[PositionInfo]) -> None:
    print(chunked_line("ОТКРЫТЫЕ ПОЗИЦИИ"))
    if not positions:
        print("Открытых позиций нет.")
        return

    for p in sorted(positions, key=lambda x: (x.symbol, x.position_side)):
        print(
            {
                "symbol": p.symbol,
                "direction": p.direction,
                "positionSide": p.position_side,
                "qty": str(p.qty),
                "entryPrice": str(p.entry_price),
                "markPrice": str(p.mark_price),
                "unrealizedPnl": str(p.unrealized_pnl),
            }
        )


def filter_positions_to_close(positions: List[PositionInfo], close_symbols: List[str]) -> List[PositionInfo]:
    if not close_symbols:
        return []
    wanted = set(close_symbols)
    return [p for p in positions if p.symbol in wanted and p.qty != 0]


def ask_confirmation(targets: List[PositionInfo]) -> bool:
    print(chunked_line("ПОДТВЕРЖДЕНИЕ"))
    print("Будут закрыты позиции:")
    for p in targets:
        print(f"  - {p.symbol} | {p.direction} | positionSide={p.position_side} | qty={p.abs_qty}")
    answer = input("Подтвердить закрытие? Введите YES: ").strip()
    return answer == "YES"


def close_positions(rest: BinanceFuturesRest, positions: List[PositionInfo], step_sizes: Dict[str, Decimal], dry_run: bool) -> None:
    print(chunked_line("ЗАКРЫТИЕ ПОЗИЦИЙ"))
    if not positions:
        print("Нет позиций для закрытия.")
        return

    success = 0
    errors = 0

    for p in positions:
        close_side = p.close_side
        if close_side is None:
            print(f"[SKIP] {p.symbol}: qty=0")
            continue

        step = step_sizes.get(p.symbol, Decimal("0"))
        qty = quantize_qty(p.abs_qty, step) if step > 0 else p.abs_qty

        if qty <= 0:
            print(f"[SKIP] {p.symbol}: после округления qty=0")
            continue

        order_params: Dict[str, Any] = {
            "symbol": p.symbol,
            "side": close_side,
            "type": "MARKET",
            "quantity": format_decimal(qty, 8).rstrip("0").rstrip("."),
        }

        # В hedge mode Binance не принимает reduceOnly вместе с positionSide=LONG/SHORT
        # для обычного MARKET-ордера на закрытие.
        if p.position_side in {"LONG", "SHORT"}:
            order_params["positionSide"] = p.position_side
        else:
            order_params["reduceOnly"] = "true"

        print(
            {
                "symbol": p.symbol,
                "direction": p.direction,
                "positionSide": p.position_side,
                "closeSide": close_side,
                "qty": str(qty),
                "dryRun": dry_run,
            }
        )

        if dry_run:
            continue

        try:
            resp = rest.create_order(order_params)
            print(f"[OK] {p.symbol}: orderId={resp.get('orderId')} status={resp.get('status')}")
            success += 1
        except Exception as exc:
            print(f"[ERROR] {p.symbol}: {exc}")
            errors += 1

    if not dry_run:
        print({"closed_ok": success, "closed_errors": errors})


def main() -> int:
    env_path = load_dotenv_file()
    if env_path:
        print(f"Loaded .env: {env_path}")
    else:
        print("WARNING: .env не найден, используются только уже выставленные переменные окружения.")

    api_key = os.getenv("BINANCE_BASE_MAIN_API_KEY", "").strip()
    api_secret = os.getenv("BINANCE_BASE_MAIN_API_SECRET", "").strip()

    if not api_key or not api_secret:
        print("ERROR: Не найдены BINANCE_BASE_MAIN_API_KEY / BINANCE_BASE_MAIN_API_SECRET")
        return 1

    close_symbols = normalize_symbol_list(CLOSE_SYMBOLS)

    print(chunked_line("НАСТРОЙКИ"))
    print({"CLOSE_SYMBOLS": close_symbols, "DRY_RUN": DRY_RUN, "AUTO_CONFIRM": AUTO_CONFIRM})

    rest = BinanceFuturesRest(api_key=api_key, api_secret=api_secret)

    try:
        offset = rest.sync_time()
        print(f"time sync ok: offset_ms={offset}")
    except Exception as exc:
        print(f"WARNING: time sync failed: {exc}")

    try:
        positions = load_open_positions(rest)
    except Exception as exc:
        print(f"ERROR: не удалось получить позиции: {exc}")
        return 2

    if PRINT_ALL_OPEN_POSITIONS:
        print_open_positions(positions)

    if not close_symbols:
        print(chunked_line("ИТОГ"))
        print("Список CLOSE_SYMBOLS пуст. Укажи символы в коде и запусти снова.")
        return 0

    targets = filter_positions_to_close(positions, close_symbols)

    print(chunked_line("ВЫБРАНО ДЛЯ ЗАКРЫТИЯ"))
    if not targets:
        print("Среди открытых позиций нет символов из CLOSE_SYMBOLS.")
        return 0

    for p in targets:
        print(
            {
                "symbol": p.symbol,
                "direction": p.direction,
                "positionSide": p.position_side,
                "qty": str(p.qty),
                "entryPrice": str(p.entry_price),
                "markPrice": str(p.mark_price),
                "unrealizedPnl": str(p.unrealized_pnl),
            }
        )

    if not DRY_RUN and not AUTO_CONFIRM:
        if not ask_confirmation(targets):
            print("Отменено пользователем.")
            return 0

    try:
        exchange_info = rest.get_exchange_info()
        step_sizes = build_step_size_map(exchange_info)
    except Exception as exc:
        print(f"WARNING: не удалось получить exchangeInfo: {exc}")
        step_sizes = {}

    close_positions(rest=rest, positions=targets, step_sizes=step_sizes, dry_run=DRY_RUN)

    print(chunked_line("ГОТОВО"))
    if DRY_RUN:
        print("Это был dry-run. Для реального закрытия поставь DRY_RUN = False.")
    else:
        print("Скрипт завершён. Проверь строки [OK]/[ERROR] выше.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
