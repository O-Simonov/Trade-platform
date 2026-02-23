# src/platform/run_balance.py
from __future__ import annotations

import os
import time
import json
import hmac
import hashlib
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
import requests
from urllib.parse import urlencode

# websocket-client package (pip name: websocket-client)
import websocket  # type: ignore

from src.platform.data.storage.postgres.pool import create_pool


log = logging.getLogger("platform.run_balance")


# =============================================================================
# Small utils
# =============================================================================

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _load_dotenv_if_present() -> Optional[Path]:
    """
    Very small .env loader (no extra deps). Loads KEY=VALUE lines into os.environ
    only if a key is not already set.
    """
    # We intentionally *search upwards* because this file can be executed from
    # different working directories (PowerShell, services, IDE).  The project
    # root usually contains a ".env" next to "config/" and "src/".
    here = Path(__file__).resolve()
    env_path: Optional[Path] = None
    for parent in [here.parent] + list(here.parents):
        cand = parent / ".env"
        if cand.exists():
            env_path = cand
            break
    if env_path is None:
        return None
    try:
        for line in env_path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v
        return env_path
    except Exception:
        return None


def _get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None:
        return default
    v = v.strip()
    return v if v else default


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class InstanceConfig:
    exchange: str
    account: str
    asset: str = "USDT"
    poll_sec: float = 10.0

    timeout_sec: float = 20.0
    retry_max: int = 1
    debug_requests: bool = False

    # In WS mode, WS payload doesn't include available balance.
    # We do a lightweight REST "heartbeat" snapshot every N seconds to refresh it.
    rest_heartbeat_sec: float = 60.0

    write_heartbeat_sec: float = 60.0  # write cached state to DB every N seconds (ws/hybrid)
    # If True, in WS mode we may write an occasional REST snapshot as a fallback
    # (e.g., if WS stream is silent for a long time). Default is False because
    # you asked for WS-driven balance history.
    rest_fallback_write: bool = False

    # mode:
    #   - "rest"   : periodic REST /fapi/v2/balance
    #   - "ws"     : Binance Futures user-data stream (fstream), write on ACCOUNT_UPDATE events
    #               + lightweight REST heartbeat to refresh fields missing in WS payload
    #   - "hybrid" : user-data stream + periodic REST every poll_sec seconds (fills fields
    #               missing in WS payload, like availableBalance)
    mode: str = "rest"

    api_key_env: Optional[str] = None
    api_secret_env: Optional[str] = None


def _parse_instances(cfg_path: Path) -> List[InstanceConfig]:
    raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    items = raw.get("instances") or []
    out: List[InstanceConfig] = []
    for i, it in enumerate(items):
        if not isinstance(it, dict):
            raise ValueError(f"instances[{i}] must be a mapping")
        out.append(
            InstanceConfig(
                exchange=str(it.get("exchange", "binance")).strip(),
                account=str(it.get("account", "")).strip(),
                asset=str(it.get("asset", "USDT")).strip().upper(),
                poll_sec=float(it.get("poll_sec", 10)),
                timeout_sec=float(it.get("timeout_sec", 20)),
                retry_max=int(it.get("retry_max", 1)),
                debug_requests=bool(it.get("debug_requests", False)),
                rest_heartbeat_sec=float(it.get("rest_heartbeat_sec", 60)),
                write_heartbeat_sec=float(it.get("write_heartbeat_sec", 0.0)),
                rest_fallback_write=bool(it.get("rest_fallback_write", False)),
                mode=str(it.get("mode", "rest")).strip().lower(),
                api_key_env=it.get("api_key_env"),
                api_secret_env=it.get("api_secret_env"),
            )
        )
    return out


# =============================================================================
# DB helper (raw psycopg pool via create_pool)
# =============================================================================

DDL_ACCOUNT_STATE = """
CREATE TABLE IF NOT EXISTS account_state (
  exchange_id INT NOT NULL,
  account_id  INT NOT NULL,
  ts          TIMESTAMPTZ NOT NULL,
  wallet_balance NUMERIC,
  equity         NUMERIC,
  available_balance NUMERIC,
  unrealized_pnl NUMERIC
);
"""

DDL_ACCOUNT_BALANCE_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS account_balance_snapshots (
  exchange_id INT NOT NULL,
  account_id  INT NOT NULL,
  ts          TIMESTAMPTZ NOT NULL,
  wallet_balance NUMERIC,
  equity         NUMERIC,
  available_balance NUMERIC,
  margin_used NUMERIC,
  unrealized_pnl NUMERIC,
  source TEXT NOT NULL
);
"""


def _db_fetch_one(pool, sql: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
            row = cur.fetchone()
            if row is None:
                return None
            cols = [d.name for d in cur.description]
            return dict(zip(cols, row))

def _db_fetch_all(pool, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
            rows = cur.fetchall()
            if not rows:
                return []
            cols = [d.name for d in cur.description]
            return [dict(zip(cols, r)) for r in rows]


def _db_get_columns(pool, table: str, schema: str = "public") -> set[str]:
    rows = _db_fetch_all(
        pool,
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %(s)s AND table_name = %(t)s
        """,
        {"s": schema, "t": table},
    )
    return {r["column_name"] for r in rows}


def _pick_first_existing(cols: set[str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in cols:
            return c
    return None


def _db_exec(pool, sql: str, params: Optional[Dict[str, Any]] = None) -> None:
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
        conn.commit()


def _ensure_tables(pool) -> None:
    _db_exec(pool, DDL_ACCOUNT_STATE)
    _db_exec(pool, DDL_ACCOUNT_BALANCE_SNAPSHOTS)


def _resolve_exchange_account_ids(pool, exchange_name: str, account_name: str) -> Tuple[int, int]:
    # Different project versions may have different column names for exchange/account display names.
    ex_cols = _db_get_columns(pool, "exchanges")
    ex_name_col = _pick_first_existing(ex_cols, ["name", "exchange", "code", "slug"])
    if not ex_name_col:
        raise RuntimeError(f"Cannot resolve exchange by name: no suitable column in exchanges. columns={sorted(ex_cols)}")

    acc_cols = _db_get_columns(pool, "accounts")
    acc_name_col = _pick_first_existing(acc_cols, ["name", "account", "account_name", "code", "slug"])
    if not acc_name_col:
        raise RuntimeError(f"Cannot resolve account by name: no suitable column in accounts. columns={sorted(acc_cols)}")

    r1 = _db_fetch_one(pool, f"SELECT exchange_id FROM exchanges WHERE {ex_name_col} = %(n)s", {"n": exchange_name})
    if not r1:
        raise RuntimeError(
            f"exchange not found in DB: {exchange_name!r} (table exchanges, column {ex_name_col})"
        )

    r2 = _db_fetch_one(pool, f"SELECT account_id FROM accounts WHERE exchange_id = %(ex_id)s AND {acc_name_col} = %(n)s", {"ex_id": int(r1["exchange_id"]), "n": account_name})
    if not r2:
        raise RuntimeError(
            f"account not found in DB: {account_name!r} (table accounts, column {acc_name_col})"
        )

    return int(r1["exchange_id"]), int(r2["account_id"])


# =============================================================================
# Binance USDⓈ-M Futures REST (fapi)
# =============================================================================

class BinanceFuturesRest:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str = "https://fapi.binance.com",
        timeout_sec: float = 20.0,
        retry_max: int = 1,
        debug_requests: bool = False,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret.encode("utf-8")
        self._base_url = base_url.rstrip("/")
        self._timeout = float(timeout_sec)
        self._retry_max = int(retry_max)
        self._debug = bool(debug_requests)

        self._sess = requests.Session()
        self._sess.headers.update({"X-MBX-APIKEY": self._api_key})

        # time sync
        self._time_offset_ms = 0

    def sync_time(self) -> None:
        # /fapi/v1/time => {"serverTime":...}
        try:
            t0 = int(time.time() * 1000)
            r = self._sess.get(self._base_url + "/fapi/v1/time", timeout=self._timeout)
            r.raise_for_status()
            server_ms = int(r.json().get("serverTime"))
            t1 = int(time.time() * 1000)
            local_ms = (t0 + t1) // 2
            self._time_offset_ms = server_ms - local_ms
            log.info("[BINANCE REST] time sync ok: offset_ms=%s", self._time_offset_ms)
        except Exception as e:
            log.warning("[BINANCE REST] time sync failed: %s", e)

    def _ts_ms(self) -> int:
        return int(time.time() * 1000) + int(self._time_offset_ms)

    def _sign(self, params: Dict[str, Any]) -> Dict[str, Any]:
        # Binance expects query string to be HMAC SHA256 over the exact query string.
        q = urlencode(params, doseq=True)
        sig = hmac.new(self._api_secret, q.encode("utf-8"), hashlib.sha256).hexdigest()
        out = dict(params)
        out["signature"] = sig
        return out

    def _request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None, signed: bool = False) -> Any:
        params = params or {}
        last_exc: Optional[Exception] = None

        for attempt in range(self._retry_max + 1):
            try:
                q = dict(params)
                if signed:
                    q.setdefault("recvWindow", 5000)
                    q["timestamp"] = self._ts_ms()
                    q = self._sign(q)

                if self._debug:
                    log.info("[BINANCE REST] %s %s signed=%s params=%s", method, path, "Y" if signed else "N", {k: ("<redacted>" if k=="signature" else v) for k,v in q.items()})

                url = self._base_url + path
                r = self._sess.request(method, url, params=q, timeout=self._timeout)
                if self._debug:
                    log.info("[BINANCE REST] HTTP %s for %s", r.status_code, path)
                if r.status_code >= 400:
                    raise RuntimeError(f"binance http {r.status_code} {method} {path}: {r.text}")
                return r.json()
            except Exception as e:
                last_exc = e
                if attempt < self._retry_max:
                    time.sleep(0.25 * (attempt + 1))
        assert last_exc is not None
        raise last_exc

    def fetch_usdm_balance(self) -> List[Dict[str, Any]]:
        # GET /fapi/v2/balance (signed)
        data = self._request("GET", "/fapi/v2/balance", params={}, signed=True)
        if not isinstance(data, list):
            raise RuntimeError(f"unexpected balance payload: {type(data)}")
        return data


    # -------------------------------------------------------------------------
    # Orders / trades bootstrap helpers (USDⓈ-M Futures)
    # -------------------------------------------------------------------------

    def fetch_open_orders(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        # GET /fapi/v1/openOrders (signed)
        params: Dict[str, Any] = {}
        if symbol:
            params["symbol"] = symbol
        data = self._request("GET", "/fapi/v1/openOrders", params=params, signed=True)
        if not isinstance(data, list):
            raise RuntimeError(f"unexpected openOrders payload: {type(data)}")
        return data

    def fetch_user_trades(
        self,
        *,
        symbol: str,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        # GET /fapi/v1/userTrades (signed) -- requires symbol
        params: Dict[str, Any] = {"symbol": symbol, "limit": int(limit)}
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)
        data = self._request("GET", "/fapi/v1/userTrades", params=params, signed=True)
        if not isinstance(data, list):
            raise RuntimeError(f"unexpected userTrades payload: {type(data)}")
        return data

    def fetch_all_orders(
        self,
        *,
        symbol: str,
        start_time_ms: Optional[int] = None,
        end_time_ms: Optional[int] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        # GET /fapi/v1/allOrders (signed) -- requires symbol
        params: Dict[str, Any] = {"symbol": symbol, "limit": int(limit)}
        if start_time_ms is not None:
            params["startTime"] = int(start_time_ms)
        if end_time_ms is not None:
            params["endTime"] = int(end_time_ms)
        data = self._request("GET", "/fapi/v1/allOrders", params=params, signed=True)
        if not isinstance(data, list):
            raise RuntimeError(f"unexpected allOrders payload: {type(data)}")
        return data
    # user-data stream listenKey management
    def create_listen_key(self) -> str:
        data = self._request("POST", "/fapi/v1/listenKey", params={}, signed=False)
        lk = data.get("listenKey")
        if not lk:
            raise RuntimeError("no listenKey in response")
        return str(lk)

    def keepalive_listen_key(self, listen_key: str) -> None:
        self._request("PUT", "/fapi/v1/listenKey", params={"listenKey": listen_key}, signed=False)

    def close_listen_key(self, listen_key: str) -> None:
        try:
            self._request("DELETE", "/fapi/v1/listenKey", params={"listenKey": listen_key}, signed=False)
        except Exception:
            pass


# =============================================================================
# Binance Futures user-data WS (fstream)
# =============================================================================

class BinanceFuturesUserStream:
    """
    Connects to wss://fstream.binance.com/ws/<listenKey> (USDⓈ-M Futures user stream).
    Emits ACCOUNT_UPDATE and ORDER_TRADE_UPDATE events via callbacks.
    """

    def __init__(
        self,
        rest: BinanceFuturesRest,
        asset: str,
        on_account_update,
        on_order_trade_update=None,
        *,
        debug_messages: bool = False,
    ) -> None:
        self._rest = rest
        self._asset = asset.upper()
        self._on_account_update = on_account_update
        self._on_order_trade_update = on_order_trade_update
        self._debug_messages = debug_messages

        self._stop = threading.Event()
        self._ws: Optional[websocket.WebSocketApp] = None
        self._listen_key: Optional[str] = None
        self._ws_url: Optional[str] = None
        self._lock = threading.Lock()

        self._t_ws: Optional[threading.Thread] = None
        self._t_keep: Optional[threading.Thread] = None


        # WS callbacks (for listenKey rotation rebuild)
        self._cb_on_open = None
        self._cb_on_message = None
        self._cb_on_error = None
        self._cb_on_close = None
    def start(self) -> None:
        self._listen_key = self._rest.create_listen_key()
        ws_url = f"wss://fstream.binance.com/ws/{self._listen_key}"
        with self._lock:
            self._ws_url = ws_url
        log.info("[BINANCE WS] connect: %s", ws_url)

        def on_message(_ws, message: str):
            if self._debug_messages:
                # Avoid huge spam: truncate long payloads.
                msg = message if len(message) <= 2000 else (message[:2000] + "...<truncated>")
                log.debug("[BINANCE WS] message: %s", msg)
            try:
                obj = json.loads(message)
            except Exception:
                return
            et = obj.get("e")
            if et == "ACCOUNT_UPDATE":
                self._on_account_update(obj)

            elif et == "ORDER_TRADE_UPDATE" and self._on_order_trade_update is not None:
                self._on_order_trade_update(obj)

            elif et == "listenKeyExpired":
                log.warning("[BINANCE WS] listenKeyExpired event received; rotating listenKey")
                self._rotate_listen_key()
        def on_error(_ws, err):
            log.warning("[BINANCE WS] error: %s", err)

        def on_close(_ws, status, msg):
            log.warning("[BINANCE WS] closed: status=%s msg=%s", status, msg)

        def on_open(_ws):
            log.info("[BINANCE WS] opened")

        # Store callbacks so we can rebuild WS app on listenKey rotation.
        self._cb_on_open = on_open
        self._cb_on_message = on_message
        self._cb_on_error = on_error
        self._cb_on_close = on_close

        self._ws = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        self._t_ws = threading.Thread(target=self._run_ws, name="binance_ws", daemon=True)
        self._t_ws.start()

        self._t_keep = threading.Thread(target=self._run_keepalive, name="binance_ws_keepalive", daemon=True)
        self._t_keep.start()


    def _rotate_listen_key(self) -> None:
        """Rotate listenKey and force WS reconnect."""
        try:
            old = self._listen_key
            new_key = self._rest.create_listen_key()
            new_url = f"wss://fstream.binance.com/ws/{new_key}"
            with self._lock:
                self._listen_key = new_key
                self._ws_url = new_url
            # Best-effort close old listenKey
            if old:
                try:
                    self._rest.close_listen_key(old)
                except Exception:
                    pass
            # Force reconnect: close current socket; ws thread will rebuild using new url.
            try:
                if self._ws:
                    self._ws.close()
            except Exception:
                pass
            log.info("[BINANCE WS] rotated listenKey")
        except Exception as e:
            log.warning("[BINANCE WS] rotate listenKey failed: %s", e)

    def stop(self) -> None:
        self._stop.set()
        try:
            if self._ws:
                self._ws.close()
        except Exception:
            pass
        if self._listen_key:
            self._rest.close_listen_key(self._listen_key)

    def _run_ws(self) -> None:
        # ping interval built-in
        while not self._stop.is_set():
            try:
                # Rebuild app if listenKey rotated
                with self._lock:
                    url = self._ws_url
                if url and (self._ws is None or getattr(self._ws, "url", None) != url):
                    self._ws = websocket.WebSocketApp(
                        url,
                        on_open=self._cb_on_open,
                        on_message=self._cb_on_message,
                        on_error=self._cb_on_error,
                        on_close=self._cb_on_close,
                    )
                assert self._ws is not None
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                log.warning("[BINANCE WS] run_forever failed: %s", e)
            if not self._stop.is_set():
                time.sleep(2.0)


    def _run_keepalive(self) -> None:
        # Binance requires keepalive at least once per 60 minutes; do every 30m
        while not self._stop.is_set():
            time.sleep(30 * 60)
            if self._stop.is_set():
                break
            try:
                if self._listen_key:
                    self._rest.keepalive_listen_key(self._listen_key)
                    log.info("[BINANCE WS] listenKey keepalive OK")
            except Exception as e:
                log.warning("[BINANCE WS] listenKey keepalive failed: %s", e)
                # If keepalive fails, rotate listenKey and reconnect.
                self._rotate_listen_key()


# =============================================================================
# Writer
# =============================================================================

class AccountBalanceWriter:
    def __init__(self, pool, cfg: InstanceConfig, exchange_id: int, account_id: int) -> None:
        self._pool = pool
        self.cfg = cfg
        self.exchange_id = exchange_id
        self.account_id = account_id

        # keys
        key_env = cfg.api_key_env or f"BINANCE_{cfg.account.upper()}_API_KEY"
        sec_env = cfg.api_secret_env or f"BINANCE_{cfg.account.upper()}_API_SECRET"
        api_key = _get_env(key_env)
        api_secret = _get_env(sec_env)
        if not api_key or not api_secret:
            raise RuntimeError(
                f"Missing Binance API keys in environment. Expected {key_env} and {sec_env}. "
                "Check your .env and that it is loaded."
            )

        self._rest = BinanceFuturesRest(
            api_key=api_key,
            api_secret=api_secret,
            timeout_sec=cfg.timeout_sec,
            retry_max=cfg.retry_max,
            debug_requests=cfg.debug_requests or bool(int(os.getenv("BINANCE_REST_DEBUG_REQUESTS", "0"))),
        )
        self._rest.sync_time()

        self._stop = threading.Event()
        self._thread = threading.Thread(target=self.run, name=f"balance_writer.{cfg.exchange}.{cfg.account}", daemon=True)

        self._last_write_ts: Optional[datetime] = None
        self._last_rest_ts: Optional[datetime] = None
        self._last_avail: Optional[float] = None
        self._last_wallet: Optional[float] = None
        self._last_equity: Optional[float] = None
        self._last_upnl: Optional[float] = None
        self._last_ws_ts: Optional[datetime] = None
        self._ws: Optional[BinanceFuturesUserStream] = None


        self._symbol_cache: Dict[str, int] = {}
        self._load_symbol_cache()
    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        try:
            if self._ws:
                self._ws.stop()
        except Exception:
            pass

    # ---- DB write

    def _write_snapshot(
        self,
        ts: datetime,
        wallet: Optional[float],
        equity: Optional[float],
        avail: Optional[float],
        margin_used: Optional[float],
        upnl: Optional[float],
        source: str,
    ) -> None:
        _db_exec(
            self._pool,
            """
            INSERT INTO account_balance_snapshots
              (exchange_id, account_id, ts, wallet_balance, equity, available_balance, margin_used, unrealized_pnl, source)
            VALUES
              (%(ex)s, %(ac)s, %(ts)s, %(w)s, %(e)s, %(av)s, %(mu)s, %(up)s, %(src)s)
            """,
            {
                "ex": self.exchange_id,
                "ac": self.account_id,
                "ts": ts,
                "w": wallet,
                "e": equity,
                "av": avail,
                "mu": margin_used,
                "up": upnl,
                "src": source,
            },
        )
        _db_exec(
            self._pool,
            """
            INSERT INTO account_state
              (exchange_id, account_id, ts, wallet_balance, equity, available_balance, unrealized_pnl)
            VALUES
              (%(ex)s, %(ac)s, %(ts)s, %(w)s, %(e)s, %(av)s, %(up)s)
            """,
            {"ex": self.exchange_id, "ac": self.account_id, "ts": ts, "w": wallet, "e": equity, "av": avail, "up": upnl},
        )
        self._last_write_ts = ts

    # ---- REST parse

    def _rest_fetch(self, *, cache_only: bool = False) -> Tuple[datetime, float, float, float, float]:
        """Fetch USDⓈ-M Futures balance via REST and return (ts, wallet, equity, avail, upnl).

        Notes:
          - In WS mode we typically **do not** write REST snapshots into DB.
          - REST is used mostly as a heartbeat + to refresh `available_balance`, which WS doesn't provide.
        """
        data = self._rest.fetch_usdm_balance()
        asset = self.cfg.asset.upper()
        row = next((x for x in data if str(x.get("asset", "")).upper() == asset), None)
        if not row:
            raise RuntimeError(f"asset {asset} not found in /fapi/v2/balance response")

        wallet = _safe_float(row.get("crossWalletBalance"))
        if wallet is None:
            wallet = _safe_float(row.get("balance"))
        if wallet is None:
            wallet = _safe_float(row.get("walletBalance"))

        # "crossMarginBalance" (when present) ~= wallet + unrealized PnL for cross; fall back to "balance"/wallet
        equity = _safe_float(row.get("crossMarginBalance"))
        if equity is None:
            equity = _safe_float(row.get("balance"))
        if equity is None:
            equity = wallet

        avail = _safe_float(row.get("availableBalance"))
        upnl = _safe_float(row.get("crossUnPnl"))
        ts = _utc_now()
        if wallet is None or equity is None or avail is None or upnl is None:
            raise RuntimeError(f"cannot parse balance fields from row keys={list(row.keys())} row={row}")
        if cache_only:
            # Update in-memory cache but do NOT write to DB.
            self._last_rest_ts = ts
            self._last_wallet = wallet
            self._last_equity = equity
            self._last_avail = avail
            self._last_upnl = upnl
            return ts, wallet, equity, avail, upnl

        return ts, wallet, equity, avail, upnl

    def _rest_refresh_cache(self) -> None:
        # Refresh in-memory cache only (no DB writes).
        self._rest_fetch(cache_only=True)

    def _rest_fetch_and_write(self) -> None:
        ts, wallet, equity, avail, upnl = self._rest_fetch()
        # marginUsed is not present in /fapi/v2/balance reliably; leave None
        self._write_snapshot(ts, wallet, equity, avail, None, upnl, source="binance_usdm_rest")

        # also refresh cache
        self._last_rest_ts = ts
        self._last_avail = avail
        self._last_wallet = wallet
        self._last_equity = equity
        self._last_upnl = upnl

        if os.getenv("DEBUG_BALANCE_WRITER", "0") == "1":
            log.info(
                "[debug] wrote snapshot: ts=%s wallet=%s equity=%s avail=%s upnl=%s",
                ts.isoformat(),
                wallet,
                equity,
                avail,
                upnl,
            )

    # ---- WS parse

    def _on_ws_account_update(self, obj: Dict[str, Any]) -> None:
        """
        ACCOUNT_UPDATE event format:
          {
            "e":"ACCOUNT_UPDATE",
            "E":..., "T":...,
            "a":{
              "m":"ORDER",
              "B":[{"a":"USDT","wb":"...","cw":"...","bc":"..."}],
              "P":[{"s":"BTCUSDT","pa":"...","ep":"...","up":"..."} ...]
            }
          }
        """
        try:
            a = obj.get("a") or {}
            balances = a.get("B") or []
            positions = a.get("P") or []
            asset = self.cfg.asset.upper()

            b = next((x for x in balances if str(x.get("a", "")).upper() == asset), None)
            wallet = _safe_float(b.get("wb")) if b else None  # walletBalance
            if wallet is None and b:
                wallet = _safe_float(b.get("cw"))  # crossWalletBalance (fallback)
            # WS doesn't give availableBalance directly. We can approximate:
            # equity = wallet + sum(upnl across positions)
            upnl = 0.0
            for p in positions:
                v = _safe_float(p.get("up"))
                if v is not None:
                    upnl += v
            equity = (wallet + upnl) if (wallet is not None) else None

            # Available balance is not present in WS payload; keep last REST value if available.
            avail = self._last_avail
            if avail is None and wallet is not None:
                avail = wallet

            # timestamp: prefer event time (E) or transaction time (T)
            ts_ms = obj.get("E") or obj.get("T")
            ts = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc) if ts_ms else _utc_now()

            # Guard NOT NULL constraints in DB
            if wallet is None or equity is None:
                return

            self._write_snapshot(ts, wallet, equity, avail, None, upnl, source="binance_usdm_ws")

            # cache
            self._last_ws_ts = ts
            self._last_wallet = wallet
            self._last_equity = equity
            self._last_upnl = upnl

            if os.getenv("DEBUG_BALANCE_WRITER", "0") == "1":
                log.info(
                    "[debug] wrote WS snapshot: ts=%s wallet=%s equity=%s avail=%s upnl=%s",
                    ts.isoformat(),
                    wallet,
                    equity,
                    avail,
                    upnl,
                )
        except Exception as e:
            log.warning("[BINANCE WS] parse/write failed: %s", e)

    # ---- Orders/Trades via WS user stream

    def _load_symbol_cache(self) -> None:
        try:
            rows = _db_fetch_all(
                self._pool,
                "SELECT symbol_id, symbol FROM public.symbols WHERE exchange_id = %(ex)s",
                {"ex": self.exchange_id},
            )
            self._symbol_cache = {str(r["symbol"]).upper(): int(r["symbol_id"]) for r in rows if r.get("symbol") and r.get("symbol_id") is not None}
        except Exception as e:
            log.warning("Failed to load symbols cache: %s", e)
            self._symbol_cache = {}

    def _ensure_symbol_id(self, symbol: str) -> int:
        sym = str(symbol).upper()
        if sym in self._symbol_cache:
            return self._symbol_cache[sym]
        # Insert if missing (keeps system resilient when new symbols appear)
        row = _db_fetch_one(
            self._pool,
            '''
            INSERT INTO public.symbols(exchange_id, symbol, is_active, last_seen_at)
            VALUES (%(ex)s, %(sym)s, true, now())
            ON CONFLICT (exchange_id, symbol)
            DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at
            RETURNING symbol_id
            ''',
            {"ex": self.exchange_id, "sym": sym},
        )
        if not row or row.get("symbol_id") is None:
            raise RuntimeError(f"Cannot resolve symbol_id for {sym}")
        sid = int(row["symbol_id"])
        self._symbol_cache[sym] = sid
        return sid

    
    # ---- REST bootstrap (orders / fills) before WS starts

    def _bootstrap_rest_orders_and_fills(self) -> None:
        """Seed orders and recent fills via REST so DB is consistent before WS updates."""
        if os.getenv("BOOTSTRAP_ON_START", "1") != "1":
            return

        hours = int(os.getenv("BOOTSTRAP_HOURS", "24") or "24")
        symbols_limit = int(os.getenv("BOOTSTRAP_SYMBOLS_LIMIT", "50") or "50")
        do_all_orders = os.getenv("BOOTSTRAP_ALL_ORDERS", "0") == "1"

        start_ms = int(time.time() * 1000) - hours * 3600 * 1000
        log.info("[BOOTSTRAP] REST seed start: hours=%s symbols_limit=%s allOrders=%s", hours, symbols_limit, do_all_orders)
        open_orders: List[Dict[str, Any]] = []


        # 1) Open orders (no symbol required)
        try:
            open_orders = self._rest.fetch_open_orders()
            upserts: List[Dict[str, Any]] = []
            for o in open_orders:
                sym = str(o.get("symbol") or "").upper()
                if not sym:
                    continue
                sid = self._ensure_symbol_id(sym)
                order_id = str(o.get("orderId") or "").strip()
                if not order_id:
                    continue
                client_order_id = o.get("clientOrderId")
                side = o.get("side")
                type_ = o.get("type")
                reduce_only = o.get("reduceOnly")
                price = _safe_float(o.get("price"))
                qty = _safe_float(o.get("origQty"))
                filled_qty = _safe_float(o.get("executedQty"))
                status = o.get("status")
                ts_ms = o.get("updateTime") or o.get("time")
                ts_ms = int(ts_ms) if ts_ms is not None else None
                avg_price = _safe_float(o.get("avgPrice"))
                self._upsert_order(
                    order_id=order_id,
                    symbol_id=sid,
                    client_order_id=client_order_id,
                    side=side,
                    type_=type_,
                    reduce_only=bool(reduce_only) if reduce_only is not None else None,
                    price=price,
                    qty=qty,
                    filled_qty=filled_qty,
                    status=status,
                    ts_ms=ts_ms,
                    raw_json_obj={"rest_open_order": o},
                    avg_price=avg_price,
                    source="rest_bootstrap",
                )
            log.info("[BOOTSTRAP] openOrders: %s", len(open_orders))
        except Exception as e:
            log.warning("[BOOTSTRAP] openOrders failed: %s", e, exc_info=True)

        
        # 2) Recent user trades (fills) per selected symbols (active set + openOrders + recent orders)
        try:
            # Prefer symbols that are actually relevant for this account (reduces API calls, reaches SOLUSDT etc.).
            selected: List[str] = []

            # (a) Symbols from open orders (already fetched above)
            open_syms: set[str] = set()
            try:
                for o in open_orders if isinstance(open_orders, list) else []:
                    s = str(o.get('symbol') or '').upper()
                    if s:
                        open_syms.add(s)
            except Exception:
                open_syms = set()

            # (b) Symbols that appear in recent orders in DB for this account
            db_syms: List[str] = []
            try:
                rows = _db_fetch_all(
                    self._pool,
                    '''
                    SELECT s.symbol, COUNT(*) AS n
                    FROM public.orders o
                    JOIN public.symbols s
                      ON s.symbol_id = o.symbol_id AND s.exchange_id = o.exchange_id
                    WHERE o.exchange_id = %(ex)s AND o.account_id = %(ac)s
                      AND o.created_at >= now() - ( %(hours)s * interval '1 hour' )
                    GROUP BY s.symbol
                    ORDER BY n DESC
                    LIMIT %(lim)s
                    ''',
                    {'ex': self.exchange_id, 'ac': self.account_id, 'hours': hours, 'lim': symbols_limit},
                )
                db_syms = [str(r['symbol']).upper() for r in rows if r.get('symbol')]
            except Exception as e:
                log.warning('[BOOTSTRAP] recent orders symbols query failed: %s', e)

            # Merge with priority: open orders first, then recent DB symbols
            seen: set[str] = set()
            for s in list(open_syms) + db_syms:
                s = str(s).upper()
                if not s or s in seen:
                    continue
                selected.append(s)
                seen.add(s)
                if len(selected) >= symbols_limit:
                    break

            # (c) Backfill from symbols.last_seen_at if we still need more
            if len(selected) < symbols_limit:
                sym_rows = _db_fetch_all(
                    self._pool,
                    '''
                    SELECT symbol
                    FROM public.symbols
                    WHERE exchange_id = %(ex)s AND is_active IS TRUE
                    ORDER BY last_seen_at DESC NULLS LAST
                    LIMIT %(lim)s
                    ''',
                    {'ex': self.exchange_id, 'lim': max(symbols_limit * 5, symbols_limit)},
                )
                for r in sym_rows:
                    s = str(r.get('symbol') or '').upper()
                    if not s or s in seen:
                        continue
                    selected.append(s)
                    seen.add(s)
                    if len(selected) >= symbols_limit:
                        break

            log.info('[BOOTSTRAP] userTrades symbols selected: %s (e.g. %s)', len(selected), ','.join(selected[:10]))

            total_fills = 0
            for sym in selected:
                try:
                    trades = self._rest.fetch_user_trades(symbol=sym, start_time_ms=start_ms, limit=1000)
                    if not trades:
                        continue
                    sid = self._ensure_symbol_id(sym)
                    inserted_sym = 0
                    for t in trades:
                        # Binance /fapi/v1/userTrades uses `id` as unique trade id
                        trade_id = str(t.get('id') or '').strip() or None
                        if not trade_id:
                            continue
                        order_id = str(t.get('orderId') or '').strip() or None

                        # OMS invariant: fill_uid == trade_id
                        self._insert_fill(
                            fill_uid=str(trade_id),
                            symbol_id=sid,
                            order_id=order_id,
                            trade_id=str(trade_id),
                            client_order_id=None,
                            price=_safe_float(t.get('price')),
                            qty=_safe_float(t.get('qty')),
                            realized_pnl=_safe_float(t.get('realizedPnl')),
                            ts_ms=int(t.get('time') or t.get('timestamp') or self._rest._ts_ms()),
                            source='rest_bootstrap',
                        )
                        inserted_sym += 1
                        total_fills += 1

                    log.info('[BOOTSTRAP] userTrades %s: got=%d inserted=%d', sym, len(trades), inserted_sym)
                except Exception as e:
                    log.warning('[BOOTSTRAP] userTrades failed for %s: %s', sym, e, exc_info=True)
                    continue

            log.info('[BOOTSTRAP] userTrades fills inserted: %s', total_fills)
        except Exception as e:
            log.warning('[BOOTSTRAP] userTrades bootstrap failed: %s', e, exc_info=True)

# 3) Optional: recent allOrders history to sync statuses (can be heavy)
        if do_all_orders:
            try:
                sym_rows = _db_fetch_all(
                    self._pool,
                    '''
                    SELECT symbol
                    FROM public.symbols
                    WHERE exchange_id = %(ex)s AND is_active IS TRUE
                    ORDER BY last_seen_at DESC NULLS LAST
                    LIMIT %(lim)s
                    ''',
                    {"ex": self.exchange_id, "lim": symbols_limit},
                )
                symbols = [str(r["symbol"]).upper() for r in sym_rows if r.get("symbol")]
                total = 0
                for sym in symbols:
                    try:
                        orders = self._rest.fetch_all_orders(symbol=sym, start_time_ms=start_ms, limit=1000)
                        for o in orders:
                            sid = self._ensure_symbol_id(sym)
                            order_id = str(o.get("orderId") or "").strip()
                            if not order_id:
                                continue
                            self._upsert_order(
                                order_id=order_id,
                                symbol_id=sid,
                                client_order_id=o.get("clientOrderId"),
                                side=o.get("side"),
                                type_=o.get("type"),
                                reduce_only=bool(o.get("reduceOnly")) if o.get("reduceOnly") is not None else None,
                                price=_safe_float(o.get("price")),
                                qty=_safe_float(o.get("origQty")),
                                filled_qty=_safe_float(o.get("executedQty")),
                                status=o.get("status"),
                                ts_ms=int(o.get("updateTime") or o.get("time") or start_ms),
                                raw_json_obj={"rest_all_order": o},
                                avg_price=_safe_float(o.get("avgPrice")),
                                source="rest_bootstrap",
                            )
                            total += 1
                    except Exception as e:
                        log.warning("[BOOTSTRAP] allOrders failed for %s: %s", sym, e)
                log.info("[BOOTSTRAP] allOrders upserts: %s", total)
            except Exception as e:
                log.warning("[BOOTSTRAP] allOrders bootstrap failed: %s", e, exc_info=True)

        log.info("[BOOTSTRAP] REST seed done")

    def _reconcile_orders_rest_once(self) -> None:
        """Lightweight periodic reconcile via REST to avoid WS gaps.

        - refreshes openOrders and upserts them into public.orders
        - optionally refreshes allOrders for symbols that currently have open orders
        """
        every = float(os.getenv("RECONCILE_ORDERS_EVERY_SEC", "300") or 300)
        if every <= 0:
            return
        open_orders: List[Dict[str, Any]] = []

        try:
            open_orders = self._rest.fetch_open_orders()
            log.info("[RECONCILE] openOrders: %s", len(open_orders))
            symbols: set[str] = set()
            for o in open_orders:
                sym = str(o.get("symbol") or "").strip()
                if not sym:
                    continue
                symbols.add(sym)
                sid = self._ensure_symbol_id(sym)
                order_id = str(o.get("orderId") or "").strip()
                if not order_id:
                    continue
                client_order_id = str(o.get("clientOrderId") or "").strip() or None
                side = str(o.get("side") or "").strip() or None
                type_ = str(o.get("type") or "").strip() or None
                reduce_only = o.get("reduceOnly")
                price = _safe_float(o.get("price"))
                qty = _safe_float(o.get("origQty"))
                filled_qty = _safe_float(o.get("executedQty"))
                status = str(o.get("status") or "").strip() or None
                ts_ms = o.get("updateTime") or o.get("time") or None
                self._upsert_order(
                    order_id=order_id,
                    symbol_id=int(sid),
                    client_order_id=client_order_id,
                    side=side,
                    type_=type_,
                    reduce_only=bool(reduce_only) if reduce_only is not None else None,
                    price=price,
                    qty=qty,
                    filled_qty=filled_qty,
                    status=status,
                    ts_ms=int(ts_ms) if ts_ms is not None else None,
                    raw_json_obj=o,
                    avg_price=_safe_float(o.get("avgPrice")),
                    source="rest_reconcile",
                )
        except Exception as e:
            log.warning("[RECONCILE] openOrders failed: %s", e, exc_info=True)
            return

        if os.getenv("RECONCILE_ALL_ORDERS", "0") != "1":
            return

        hours = float(os.getenv("RECONCILE_HOURS", "24") or 24)
        start_ms = int((time.time() - hours * 3600) * 1000)

        # Also reconcile symbols that have potentially stale NEW/PARTIALLY_FILLED orders in DB
        # (e.g. duplicate/old averaging orders) even if they are no longer present in openOrders.
        try:
            stale_lim = int(os.getenv("RECONCILE_STALE_SYMBOLS_LIMIT", "30") or 30)
            if stale_lim > 0:
                stale_rows = _db_fetch_all(
                    self._pool,
                    """
                    SELECT s.symbol
                    FROM public.orders o
                    JOIN public.symbols s
                      ON s.exchange_id = o.exchange_id AND s.symbol_id = o.symbol_id
                    WHERE o.exchange_id = %(ex)s AND o.account_id = %(ac)s
                      AND o.status IN ('NEW','PARTIALLY_FILLED')
                      AND o.updated_at >= now() - (%(hours)s * interval '1 hour')
                    GROUP BY s.symbol
                    ORDER BY MAX(o.updated_at) DESC
                    LIMIT %(lim)s
                    """,
                    {"ex": self.exchange_id, "ac": self.account_id, "hours": hours, "lim": stale_lim},
                )
                for r in stale_rows:
                    sym = str(r.get("symbol") or "").strip()
                    if sym:
                        symbols.add(sym.upper())
        except Exception as e:
            log.warning("[RECONCILE] stale symbols lookup failed: %s", e)



        # Only for symbols with active open orders to keep it light.
        for sym in sorted(symbols):
            try:
                orders = self._rest.fetch_all_orders(symbol=sym, start_time_ms=start_ms)
                up = 0
                for o in orders:
                    sid = self._ensure_symbol_id(sym)
                    order_id = str(o.get("orderId") or "").strip()
                    if not order_id:
                        continue
                    client_order_id = str(o.get("clientOrderId") or "").strip() or None
                    side = str(o.get("side") or "").strip() or None
                    type_ = str(o.get("type") or "").strip() or None
                    reduce_only = o.get("reduceOnly")
                    price = _safe_float(o.get("price"))
                    qty = _safe_float(o.get("origQty"))
                    filled_qty = _safe_float(o.get("executedQty"))
                    status = str(o.get("status") or "").strip() or None
                    ts_ms = o.get("updateTime") or o.get("time") or None
                    self._upsert_order(
                        order_id=order_id,
                        symbol_id=int(sid),
                        client_order_id=client_order_id,
                        side=side,
                        type_=type_,
                        reduce_only=bool(reduce_only) if reduce_only is not None else None,
                        price=price,
                        qty=qty,
                        filled_qty=filled_qty,
                        status=status,
                        ts_ms=int(ts_ms) if ts_ms is not None else None,
                        raw_json_obj=o,
                        avg_price=_safe_float(o.get("avgPrice")),
                        source="rest_reconcile",
                    )
                    up += 1
                if up:
                    log.info("[RECONCILE] allOrders %s: upserted=%s", sym, up)
            except Exception as e:
                log.warning("[RECONCILE] allOrders failed for %s: %s", sym, e)

        # Optional GC: mark very old TL averaging orders that are still NEW/PARTIALLY_FILLED in DB,
        # but are NOT present in current openOrders, as CANCELED. This prevents stale duplicates
        # from inflating active_adds and breaking scale-in limits.
        try:
            gc_age_h = float(os.getenv("RECONCILE_GC_AGE_HOURS", "12") or 12)
            gc_lim = int(os.getenv("RECONCILE_GC_LIMIT", "500") or 500)
            if gc_age_h > 0 and gc_lim > 0:
                open_ids = {str(o.get("orderId") or "").strip() for o in (open_orders or []) if str(o.get("orderId") or "").strip()}
                pat = r"TL\_%\_ADD%"
                stale = _db_fetch_all(
                    self._pool,
                    """
                    SELECT order_id
                    FROM public.orders
                    WHERE exchange_id = %(ex)s AND account_id = %(ac)s
                      AND strategy_id = 'trade_liquidation'
                      AND status IN ('NEW','PARTIALLY_FILLED')
                      AND order_id IS NOT NULL AND btrim(order_id) <> ''
                      AND client_order_id LIKE %(pat)s ESCAPE '\\'
                      AND updated_at < now() - (%(age)s * interval '1 hour')
                    ORDER BY updated_at ASC
                    LIMIT %(lim)s
                    """,
                    {"ex": self.exchange_id, "ac": self.account_id, "pat": pat, "age": gc_age_h, "lim": gc_lim},
                )
                to_cancel = [str(r.get("order_id") or "").strip() for r in stale if str(r.get("order_id") or "").strip() and str(r.get("order_id") or "").strip() not in open_ids]
                if to_cancel:
                    _db_exec(
                        self._pool,
                        """
                        UPDATE public.orders
                        SET status = 'CANCELED',
                            updated_at = now(),
                            source = 'reconcile_gc'
                        WHERE exchange_id = %(ex)s AND account_id = %(ac)s
                          AND strategy_id = 'trade_liquidation'
                          AND status IN ('NEW','PARTIALLY_FILLED')
                          AND order_id = ANY(%(ids)s)
                        """,
                        {"ex": self.exchange_id, "ac": self.account_id, "ids": to_cancel},
                    )
                    log.info("[RECONCILE] GC canceled stale TL orders: %s", len(to_cancel))
        except Exception as e:
            log.warning("[RECONCILE] GC failed: %s", e)

    def _insert_order_event(
        self,
        *,
        order_id: str,
        symbol_id: int,
        client_order_id: Optional[str],
        status: str,
        side: Optional[str],
        type_: Optional[str],
        reduce_only: Optional[bool],
        price: Optional[float],
        qty: Optional[float],
        filled_qty: Optional[float],
        ts_ms: int,
        raw_json_text: str,
        strategy_id: str = "unknown",
        pos_uid: Optional[str] = None,
        source: str = "ws_user",
    ) -> None:
        _db_exec(
            self._pool,
            '''
            INSERT INTO public.order_events(
              exchange_id, account_id, order_id, symbol_id, client_order_id,
              status, side, type, reduce_only, price, qty, filled_qty,
              source, ts_ms, recv_ts, raw_json, strategy_id, pos_uid
            )
            VALUES (
              %(ex)s, %(ac)s, %(oid)s, %(sid)s, %(coid)s,
              %(st)s, %(side)s, %(typ)s, %(ro)s, %(price)s, %(qty)s, %(fqty)s,
              %(src)s, %(ts_ms)s, now(), %(raw)s, %(strategy)s, %(pos_uid)s
            )
            ON CONFLICT DO NOTHING
            ''',
            {
                "ex": self.exchange_id,
                "ac": self.account_id,
                "oid": order_id,
                "sid": symbol_id,
                "coid": client_order_id,
                "st": status,
                "side": side,
                "typ": type_,
                "ro": reduce_only,
                "price": price,
                "qty": qty,
                "fqty": filled_qty,
                "src": source,
                "ts_ms": int(ts_ms),
                "raw": raw_json_text,
                "strategy": strategy_id or "unknown",
                "pos_uid": pos_uid,
            },
        )

    def _upsert_order(
        self,
        *,
        order_id: str,
        symbol_id: int,
        client_order_id: Optional[str],
        side: Optional[str],
        type_: Optional[str],
        reduce_only: Optional[bool],
        price: Optional[float],
        qty: Optional[float],
        filled_qty: Optional[float],
        status: Optional[str],
        ts_ms: Optional[int],
        raw_json_obj: Dict[str, Any],
        avg_price: Optional[float],
        source: str = "ws_user",
    ) -> None:
        strategy_id = "unknown"
        pos_uid = None
        if client_order_id:
            # Prefer metadata from existing real orders with the same client_order_id (common when one client id
            # is reused across multiple exchange orders).
            r = _db_fetch_one(
                self._pool,
                '''
                SELECT strategy_id, pos_uid
                FROM public.orders
                WHERE exchange_id = %(ex)s AND account_id = %(ac)s
                  AND client_order_id = %(coid)s
                  AND btrim(order_id) <> ''
                  AND (strategy_id IS NOT NULL AND strategy_id <> 'unknown'
                       OR (pos_uid IS NOT NULL AND pos_uid <> ''))
                ORDER BY updated_at DESC NULLS LAST
                LIMIT 1
                ''',
                {"ex": self.exchange_id, "ac": self.account_id, "coid": client_order_id},
            )
            if r:
                strategy_id = str(r.get("strategy_id") or "unknown")
                pos_uid = r.get("pos_uid")
            else:
                # Fallback: if a placeholder exists, pick metadata from it.
                r2 = _db_fetch_one(
                    self._pool,
                    '''
                    SELECT strategy_id, pos_uid
                    FROM public.orders
                    WHERE exchange_id = %(ex)s AND account_id = %(ac)s
                      AND (order_id IS NULL OR btrim(order_id) = '')
                      AND client_order_id = %(coid)s
                    LIMIT 1
                    ''',
                    {"ex": self.exchange_id, "ac": self.account_id, "coid": client_order_id},
                )
                if r2:
                    strategy_id = str(r2.get("strategy_id") or "unknown")
                    pos_uid = r2.get("pos_uid")

        raw_json_text = json.dumps(raw_json_obj, ensure_ascii=False)

        _db_exec(
            self._pool,
            '''
            INSERT INTO public.orders(
              exchange_id, account_id, order_id, symbol_id,
              strategy_id, pos_uid, client_order_id,
              side, type, reduce_only, price, qty, filled_qty,
              status, created_at, updated_at, source, ts_ms, raw_json, avg_price
            )
            VALUES (
              %(ex)s, %(ac)s, %(oid)s, %(sid)s,
              %(strategy)s, %(pos_uid)s, %(coid)s,
              %(side)s, %(typ)s, %(ro)s, %(price)s, %(qty)s, %(fqty)s,
              %(st)s, now(), now(), %(src)s, %(ts_ms)s, %(raw)s::jsonb, %(avg_price)s
            )
            ON CONFLICT (exchange_id, account_id, order_id)
            DO UPDATE SET
              symbol_id = EXCLUDED.symbol_id,
              strategy_id = COALESCE(NULLIF(EXCLUDED.strategy_id,'unknown'), public.orders.strategy_id),
              pos_uid = COALESCE(EXCLUDED.pos_uid, public.orders.pos_uid),
              client_order_id = COALESCE(EXCLUDED.client_order_id, public.orders.client_order_id),
              side = COALESCE(EXCLUDED.side, public.orders.side),
              type = COALESCE(EXCLUDED.type, public.orders.type),
              reduce_only = COALESCE(EXCLUDED.reduce_only, public.orders.reduce_only),
              price = COALESCE(EXCLUDED.price, public.orders.price),
              qty = COALESCE(EXCLUDED.qty, public.orders.qty),
              filled_qty = COALESCE(EXCLUDED.filled_qty, public.orders.filled_qty),
              status = COALESCE(EXCLUDED.status, public.orders.status),
              updated_at = now(),
              ts_ms = COALESCE(EXCLUDED.ts_ms, public.orders.ts_ms),
              raw_json = COALESCE(EXCLUDED.raw_json, public.orders.raw_json),
              avg_price = COALESCE(EXCLUDED.avg_price, public.orders.avg_price)
            ''',
            {
                "ex": self.exchange_id,
                "ac": self.account_id,
                "oid": order_id,
                "sid": symbol_id,
                "strategy": strategy_id or "unknown",
                "pos_uid": pos_uid,
                "coid": client_order_id,
                "side": side,
                "typ": type_,
                "ro": reduce_only,
                "price": price,
                "qty": qty,
                "fqty": filled_qty,
                "st": status,
                "src": source,
                "ts_ms": int(ts_ms) if ts_ms is not None else None,
                "raw": raw_json_text,
                "avg_price": avg_price,
            },
        )

        if client_order_id:
            _db_exec(
                self._pool,
                '''
                DELETE FROM public.orders
                WHERE exchange_id = %(ex)s AND account_id = %(ac)s
                  AND (order_id IS NULL OR btrim(order_id) = '')
                  AND client_order_id = %(coid)s
                ''',
                {"ex": self.exchange_id, "ac": self.account_id, "coid": client_order_id},
            )

    def _merge_orphan_placeholders_once(self, *, limit: int = 50) -> int:
        """Merge "placeholder" orders (order_id='') into real orders by client_order_id.

        Strategy code may insert placeholders *after* Binance WS already created real orders.
        In that case, the WS path can't delete/merge the placeholder retroactively.

        This reconciler:
          - finds placeholders for this account
          - for each client_order_id, picks the most recently updated real order
          - copies strategy_id/pos_uid onto the real order if missing/unknown
          - deletes the placeholder rows
        """

        rows = _db_fetch_all(
            self._pool,
            '''
            SELECT client_order_id, strategy_id, pos_uid
            FROM public.orders
            WHERE exchange_id = %(ex)s AND account_id = %(ac)s
              AND (order_id IS NULL OR btrim(order_id) = '')
              AND client_order_id IS NOT NULL
            ORDER BY updated_at DESC NULLS LAST
            LIMIT %(lim)s
            ''',
            {"ex": self.exchange_id, "ac": self.account_id, "lim": int(limit)},
        )
        if not rows:
            return 0

        merged = 0
        for r in rows:
            coid = str(r.get("client_order_id") or "").strip()
            if not coid:
                continue

            real = _db_fetch_one(
                self._pool,
                '''
                SELECT order_id
                FROM public.orders
                WHERE exchange_id = %(ex)s AND account_id = %(ac)s
                  AND client_order_id = %(coid)s
                  AND btrim(order_id) <> ''
                ORDER BY updated_at DESC NULLS LAST
                LIMIT 1
                ''',
                {"ex": self.exchange_id, "ac": self.account_id, "coid": coid},
            )
            if not real:
                continue

            real_oid = str(real.get("order_id") or "").strip()
            if not real_oid:
                continue

            ph_strategy = r.get("strategy_id")
            ph_pos_uid = r.get("pos_uid")

            _db_exec(
                self._pool,
                '''
                UPDATE public.orders
                SET
                  strategy_id = CASE
                    WHEN public.orders.strategy_id IS NULL OR public.orders.strategy_id = 'unknown'
                      THEN %(strategy)s
                    ELSE public.orders.strategy_id
                  END,
                  pos_uid = COALESCE(public.orders.pos_uid, %(pos_uid)s),
                  updated_at = now()
                WHERE exchange_id = %(ex)s AND account_id = %(ac)s AND order_id = %(oid)s
                ''',
                {
                    "ex": self.exchange_id,
                    "ac": self.account_id,
                    "oid": real_oid,
                    "strategy": ph_strategy or "unknown",
                    "pos_uid": ph_pos_uid,
                },
            )

            _db_exec(
                self._pool,
                '''
                DELETE FROM public.orders
                WHERE exchange_id = %(ex)s AND account_id = %(ac)s
                  AND (order_id IS NULL OR btrim(order_id) = '')
                  AND client_order_id = %(coid)s
                ''',
                {"ex": self.exchange_id, "ac": self.account_id, "coid": coid},
            )
            merged += 1

        if merged:
            log.info("[merge] merged %s placeholder order(s)", merged)
        return merged


    def _propagate_order_meta_once(self) -> None:
        """Propagate strategy_id/pos_uid across orders that share the same client_order_id.

        Some strategies reuse a client_order_id for a chain of orders. In that case we want every related order row
        to have the same strategy_id/pos_uid once any of them has it.
        """
        _db_exec(
            self._pool,
            '''
            UPDATE public.orders o
            SET
              strategy_id = COALESCE(NULLIF(o.strategy_id,'unknown'), src.strategy_id),
              pos_uid     = COALESCE(NULLIF(o.pos_uid,''), src.pos_uid),
              updated_at  = now()
            FROM (
              SELECT client_order_id,
                     max(strategy_id) FILTER (WHERE strategy_id IS NOT NULL AND strategy_id <> 'unknown') AS strategy_id,
                     max(pos_uid)      FILTER (WHERE pos_uid IS NOT NULL AND pos_uid <> '')              AS pos_uid
              FROM public.orders
              WHERE exchange_id = %(ex)s AND account_id = %(ac)s
                AND client_order_id IS NOT NULL AND client_order_id <> ''
              GROUP BY client_order_id
            ) src
            WHERE o.exchange_id = %(ex)s AND o.account_id = %(ac)s
              AND o.client_order_id = src.client_order_id
              AND (o.strategy_id IS NULL OR o.strategy_id = 'unknown' OR o.pos_uid IS NULL OR o.pos_uid = '')
              AND (src.strategy_id IS NOT NULL OR src.pos_uid IS NOT NULL)
            ''',
            {"ex": self.exchange_id, "ac": self.account_id},
        )

        # If an order originates from the exchange UI / API without strategy tagging,
        # it often has a client_order_id starting with 'web_'. Mark those as 'exchange'
        # instead of leaving them as 'unknown' so dashboards are cleaner.
        _db_exec(
            self._pool,
            '''
            UPDATE public.orders
            SET strategy_id = 'exchange', updated_at = now()
            WHERE exchange_id = %(ex)s AND account_id = %(ac)s
              AND client_order_id LIKE %(web_pattern)s ESCAPE '\\'
              AND (strategy_id IS NULL OR strategy_id = 'unknown')
            ''',
            {"ex": self.exchange_id, "ac": self.account_id, "web_pattern": r"web\_%"},
        )


    def _insert_fill(
        self,
        *,
        fill_uid: str,
        symbol_id: int,
        order_id: Optional[str],
        trade_id: Optional[str],
        client_order_id: Optional[str],
        price: Optional[float],
        qty: Optional[float],
        realized_pnl: Optional[float],
        ts_ms: int,
        source: str = "ws_user",
    ) -> None:
        ts = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
        _db_exec(
            self._pool,
            '''
            INSERT INTO public.order_fills(
              exchange_id, account_id, fill_uid, symbol_id,
              order_id, trade_id, client_order_id,
              price, qty, realized_pnl, ts, source
            )
            VALUES (
              %(ex)s, %(ac)s, %(uid)s, %(sid)s,
              %(oid)s, %(tid)s, %(coid)s,
              %(price)s, %(qty)s, %(rpnl)s, %(ts)s, %(src)s
            )
            ON CONFLICT (exchange_id, account_id, fill_uid)
            DO NOTHING
            ''',
            {
                "ex": self.exchange_id,
                "ac": self.account_id,
                "uid": fill_uid,
                "sid": symbol_id,
                "oid": order_id,
                "tid": trade_id,
                "coid": client_order_id,
                "price": price,
                "qty": qty,
                "rpnl": realized_pnl,
                "ts": ts,
                "src": source,
            },
        )

    def _on_ws_order_trade_update(self, obj: Dict[str, Any]) -> None:
        try:
            o = obj.get("o") or {}
            symbol = o.get("s")
            if not symbol:
                return
            symbol_id = self._ensure_symbol_id(str(symbol))

            order_id = str(o.get("i") or "").strip()
            client_order_id = str(o.get("c") or "").strip() or None

            status = str(o.get("X") or "").strip()
            side = str(o.get("S") or "").strip() or None
            type_ = str(o.get("o") or "").strip() or None
            reduce_only = o.get("R")
            if reduce_only is not None:
                reduce_only = bool(reduce_only)

            price = _safe_float(o.get("p"))
            if price is not None and abs(price) < 1e-18:
                price = None
            qty = _safe_float(o.get("q"))
            filled_qty = _safe_float(o.get("z"))
            avg_price = _safe_float(o.get("ap"))

            ts_ms = o.get("T") or obj.get("E") or obj.get("T")
            if ts_ms is None:
                ts_ms = int(time.time() * 1000)

            if order_id:
                self._insert_order_event(
                    order_id=order_id,
                    symbol_id=symbol_id,
                    client_order_id=client_order_id,
                    status=status or "UNKNOWN",
                    side=side,
                    type_=type_,
                    reduce_only=reduce_only,
                    price=price,
                    qty=qty,
                    filled_qty=filled_qty if filled_qty is not None else 0.0,
                    ts_ms=int(ts_ms),
                    raw_json_text=json.dumps(obj, ensure_ascii=False),
                )

                self._upsert_order(
                    order_id=order_id,
                    symbol_id=symbol_id,
                    client_order_id=client_order_id,
                    side=side,
                    type_=type_,
                    reduce_only=reduce_only,
                    price=price,
                    qty=qty,
                    filled_qty=filled_qty,
                    status=status or None,
                    ts_ms=int(ts_ms) if ts_ms is not None else None,
                    raw_json_obj=obj,
                    avg_price=avg_price,
                )

            exec_type = str(o.get("x") or "").strip()
            if exec_type == "TRADE" and order_id:
                trade_id = str(o.get("t") or "").strip() or None
                if trade_id:
                    last_qty = _safe_float(o.get("l"))
                    last_price = _safe_float(o.get("L"))
                    realized_pnl = _safe_float(o.get("rp"))
                    # OMS invariant: fill_uid == trade_id
                    self._insert_fill(
                        fill_uid=str(trade_id),
                        symbol_id=symbol_id,
                        order_id=order_id,
                        trade_id=str(trade_id),
                        client_order_id=client_order_id,
                        price=last_price,
                        qty=last_qty,
                        realized_pnl=realized_pnl,
                        ts_ms=int(ts_ms),
                    )


        except Exception as e:
            log.warning("[BINANCE WS] order/trade parse/write failed: %s", e, exc_info=True)
    # ---- main loop

    def run(self) -> None:
        mode = (self.cfg.mode or "rest").lower()
        poll_sec = max(1.0, float(self.cfg.poll_sec))
        heartbeat_sec = max(0.0, float(getattr(self.cfg, "rest_heartbeat_sec", 60.0) or 0.0))
        debug = os.getenv("DEBUG_BALANCE_WRITER", "0") == "1"

        # placeholder merge cadence (helps when strategy inserts placeholders after WS already has real orders)
        merge_every_sec = float(os.getenv("MERGE_PLACEHOLDERS_EVERY_SEC", "60") or 60)
        next_merge = time.time() + 5.0

        # propagate strategy_id/pos_uid across same client_order_id
        propagate_every_sec = float(os.getenv("PROPAGATE_ORDER_META_EVERY_SEC", "60") or 60)
        next_propagate = time.time() + 10.0

        # periodic REST reconcile for order statuses (WS gap protection)
        reconcile_every_sec = float(os.getenv("RECONCILE_ORDERS_EVERY_SEC", "300") or 300)
        next_reconcile = time.time() + 15.0


        if mode in ("ws", "hybrid"):
            # REST bootstrap first: seed orders/fills so DB is consistent before WS updates
            self._bootstrap_rest_orders_and_fills()
            # Also reconcile any placeholders against already-known real orders.
            try:
                self._merge_orphan_placeholders_once(limit=200)
            except Exception:
                log.warning("[merge] placeholder reconcile failed", exc_info=True)
            try:
                self._propagate_order_meta_once()
            except Exception:
                log.warning("[meta] propagate failed", exc_info=True)
            self._ws = BinanceFuturesUserStream(self._rest, self.cfg.asset, self._on_ws_account_update, self._on_ws_order_trade_update)
            self._ws.start()

        # For "ws": write when events arrive; do occasional REST heartbeat to refresh available balance.
        # For "hybrid": always do REST snapshot every poll_sec seconds.
        next_rest = time.time()  # allow immediate seed/heartbeat
        next_write = time.time() if (self.cfg.write_heartbeat_sec and self.cfg.write_heartbeat_sec > 0) else 1e30

        while not self._stop.is_set():
            try:
                now = time.time()

                do_rest_write = False
                do_rest_cache = False
                if mode == "rest":
                    do_rest_write = True
                elif mode == "hybrid":
                    do_rest_write = now >= next_rest
                elif mode == "ws":
                    # In WS mode we normally *don't* write REST snapshots.
                    # We only refresh available balance cache (WS payload doesn't include it).
                    if heartbeat_sec > 0 and now >= next_rest:
                        do_rest_cache = True

                    # Optional fallback: if WS is silent for a long time, allow a REST snapshot write.
                    if self.cfg.rest_fallback_write:
                        ws_age = None
                        if self._last_ws_ts is not None:
                            ws_age = (_utc_now() - self._last_ws_ts).total_seconds()
                        if ws_age is None:
                            do_rest_write = now >= (next_rest + poll_sec)
                        else:
                            do_rest_write = ws_age >= max(3.0 * poll_sec, 30.0)

                if do_rest_cache:
                    if debug:
                        log.info("[debug] heartbeat REST: refresh avail (%s)", self.cfg.asset)
                    self._rest_fetch(cache_only=True)
                    next_rest = time.time() + heartbeat_sec if heartbeat_sec > 0 else time.time() + poll_sec

                # Periodically reconcile placeholders even in WS mode.
                if merge_every_sec > 0 and time.time() >= next_merge:
                    try:
                        self._merge_orphan_placeholders_once(limit=200)
                    except Exception:
                        log.warning("[merge] placeholder reconcile failed", exc_info=True)
                    next_merge = time.time() + merge_every_sec

                # Propagate strategy_id/pos_uid across orders sharing client_order_id.
                if propagate_every_sec > 0 and time.time() >= next_propagate:
                    try:
                        self._propagate_order_meta_once()
                    except Exception:
                        log.warning("[meta] propagate failed", exc_info=True)
                    next_propagate = time.time() + propagate_every_sec

                # Periodic REST reconcile for orders (WS gap protection).
                if reconcile_every_sec > 0 and time.time() >= next_reconcile:
                    try:
                        self._reconcile_orders_rest_once()
                    except Exception:
                        log.warning("[RECONCILE] failed", exc_info=True)
                    next_reconcile = time.time() + reconcile_every_sec


                # write-heartbeat: persist cached state even if WS is quiet
                if (self.cfg.write_heartbeat_sec and self.cfg.write_heartbeat_sec > 0) and time.time() >= next_write:
                    if self._last_wallet is not None:
                        ts_now = _utc_now()
                        self._write_snapshot(
                            ts_now,
                            self._last_wallet,
                            self._last_equity,
                            self._last_avail,
                            None,
                            self._last_upnl,
                            source="binance_usdm_cache",
                        )
                        if debug:
                            log.info(
                                "[debug] wrote heartbeat snapshot: ts=%s wallet=%s equity=%s avail=%s upnl=%s",
                                ts_now.isoformat(),
                                self._last_wallet,
                                self._last_equity,
                                self._last_avail,
                                self._last_upnl,
                            )
                    next_write = time.time() + float(self.cfg.write_heartbeat_sec)

                if do_rest_write:
                    if debug:
                        log.info("[debug] polling REST: fetch_balance(%s)", self.cfg.asset)
                    self._rest_fetch_and_write()
                    next_rest = time.time() + poll_sec

            except Exception as e:
                log.error("poll error: %s", e, exc_info=True)
                # don't spin
                time.sleep(2.0)

            # write-heartbeat: persist last known state even if no REST heartbeat
            if (self.cfg.write_heartbeat_sec and self.cfg.write_heartbeat_sec > 0) and time.time() >= next_write:
                if self._last_wallet is not None:
                    ts_now = _utc_now()
                    self._write_snapshot(
                        ts_now,
                        self._last_wallet,
                        self._last_equity,
                        self._last_avail,
                        None,
                        self._last_upnl,
                        source="binance_usdm_cache",
                    )
                    if debug:
                        log.info(
                            "[debug] wrote heartbeat snapshot: ts=%s wallet=%s equity=%s avail=%s upnl=%s",
                            ts_now.isoformat(),
                            self._last_wallet,
                            self._last_equity,
                            self._last_avail,
                            self._last_upnl,
                        )
                next_write = time.time() + float(self.cfg.write_heartbeat_sec)

            # sleep short; WS runs in its own thread
            time.sleep(0.25)

        try:
            if self._ws:
                self._ws.stop()
        except Exception:
            pass


# =============================================================================
# Entrypoint
# =============================================================================

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    log.info("=== RUN BALANCE WRITER START ===")

    env_loaded = _load_dotenv_if_present()
    if env_loaded:
        log.info("Loaded .env: %s", env_loaded)

    dsn = os.getenv("PG_DSN", "").strip()
    if not dsn:
        raise RuntimeError("PG_DSN env var is required")

    cfg_path = Path(os.getenv("INSTANCES_CONFIG", "config/balance_instances.yaml")).resolve()
    instances = _parse_instances(cfg_path)
    log.info("Config: %s | instances=%s", cfg_path, len(instances))

    pool = create_pool(dsn)
    log.info("PostgreSQL storage initialized")

    _ensure_tables(pool)
    row = _db_fetch_one(pool, "SELECT now() AS now")
    log.info("DB ping OK: now=%s", row["now"] if row else None)

    writers: List[AccountBalanceWriter] = []
    for ic in instances:
        ex_id, acc_id = _resolve_exchange_account_ids(pool, ic.exchange, ic.account)
        w = AccountBalanceWriter(pool, ic, ex_id, acc_id)
        log.info(
            "Writer ready: exchange=%s account=%s exchange_id=%s account_id=%s poll_sec=%s mode=%s",
            ic.exchange,
            ic.account,
            ex_id,
            acc_id,
            ic.poll_sec,
            ic.mode,
        )
        writers.append(w)

    log.info("Starting %s balance writer(s)", len(writers))
    for w in writers:
        w.start()

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        log.info("Stopping...")
    finally:
        for w in writers:
            w.stop()
        try:
            pool.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()