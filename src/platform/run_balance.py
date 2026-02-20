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
    Emits ACCOUNT_UPDATE events via callback.
    """

    def __init__(self, rest: BinanceFuturesRest, asset: str, on_account_update, *, debug_messages: bool = False) -> None:
        self._rest = rest
        self._asset = asset.upper()
        self._on_account_update = on_account_update
        self._debug_messages = debug_messages

        self._stop = threading.Event()
        self._ws: Optional[websocket.WebSocketApp] = None
        self._listen_key: Optional[str] = None

        self._t_ws: Optional[threading.Thread] = None
        self._t_keep: Optional[threading.Thread] = None

    def start(self) -> None:
        self._listen_key = self._rest.create_listen_key()
        ws_url = f"wss://fstream.binance.com/ws/{self._listen_key}"
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

        def on_error(_ws, err):
            log.warning("[BINANCE WS] error: %s", err)

        def on_close(_ws, status, msg):
            log.warning("[BINANCE WS] closed: status=%s msg=%s", status, msg)

        def on_open(_ws):
            log.info("[BINANCE WS] opened")

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
        assert self._ws is not None
        # ping interval built-in
        while not self._stop.is_set():
            try:
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

    # ---- main loop

    def run(self) -> None:
        mode = (self.cfg.mode or "rest").lower()
        poll_sec = max(1.0, float(self.cfg.poll_sec))
        heartbeat_sec = max(0.0, float(getattr(self.cfg, "rest_heartbeat_sec", 60.0) or 0.0))
        debug = os.getenv("DEBUG_BALANCE_WRITER", "0") == "1"

        if mode in ("ws", "hybrid"):
            self._ws = BinanceFuturesUserStream(self._rest, self.cfg.asset, self._on_ws_account_update)
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