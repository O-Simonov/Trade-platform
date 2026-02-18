from __future__ import annotations

import os
import time
import signal
import logging
import threading
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from src.platform.exchanges.registry import build_exchange
from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage


# -----------------------------------------------------------------------------
# helpers
# -----------------------------------------------------------------------------
def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Config file not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return data or {}


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")


def _resolve_cfg_path(project_root: Path) -> Path:
    """Resolve config path from $INSTANCES_CONFIG.

    Supports:
      - absolute path
      - relative path (resolved from project_root)
    """
    raw = (os.getenv("INSTANCES_CONFIG") or "").strip()
    if not raw:
        return project_root / "config" / "balance_instances.yaml"

    p = Path(raw)
    if p.is_absolute():
        return p
    return (project_root / p).resolve()


# -----------------------------------------------------------------------------
# poller thread: writes BOTH account_state + account_balance_snapshots
# -----------------------------------------------------------------------------
class AccountBalanceWriter(threading.Thread):
    """Polls exchange account state and writes to DB.

    Tables:
      - account_state (last known state)
      - account_balance_snapshots (history)

    It relies on the exchange adapter method:
      exchange.fetch_account_state(account=...)
    """

    def __init__(
        self,
        *,
        exchange_name: str,
        account: str,
        exchange_id: int,
        account_id: int,
        storage: PostgreSQLStorage,
        poll_sec: float,
        debug: bool,
        binance_timeout_sec: float | None = None,
        binance_retry_max: int | None = None,
        binance_debug_requests: bool = False,
    ):
        super().__init__(daemon=True, name=f"BalanceWriter-{exchange_name}-{account}")
        self.exchange_name = exchange_name
        self.account = account
        self.exchange_id = int(exchange_id)
        self.account_id = int(account_id)
        self.storage = storage
        self.poll_sec = float(poll_sec)
        self.debug = bool(debug)

        self._stop = threading.Event()

        # Binance REST adapter reads these env vars at init time.
        if self.exchange_name.lower() == "binance":
            if binance_timeout_sec is not None:
                os.environ["BINANCE_REST_TIMEOUT_SEC"] = str(float(binance_timeout_sec))
            if binance_retry_max is not None:
                os.environ["BINANCE_REST_RETRY_MAX"] = str(int(binance_retry_max))
            if binance_debug_requests:
                os.environ["BINANCE_REST_DEBUG_REQUESTS"] = "1"

        self._ex = build_exchange(exchange_name)

        # For balance we don't need symbol_ids, but bind keeps IDs consistent
        # and lets the adapter know exchange_id.
        try:
            self._ex.bind(storage=storage, exchange_id=self.exchange_id, symbol_ids={})
        except Exception:
            pass

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        log = logging.getLogger(f"balance_writer.{self.exchange_name}.{self.account}")

        log.info(
            "Writer start: exchange=%s account=%s exchange_id=%s account_id=%s poll_sec=%.1f",
            self.exchange_name,
            self.account,
            self.exchange_id,
            self.account_id,
            self.poll_sec,
        )

        from datetime import datetime, timezone

        while not self._stop.is_set():
            t0 = time.time()
            try:
                if self.debug:
                    log.info("[debug] polling: fetch_account_state()")

                st = self._ex.fetch_account_state(account=self.account)
                if not isinstance(st, dict):
                    raise RuntimeError(f"fetch_account_state returned non-dict: {type(st)}")

                # 1) current state
                self.storage.upsert_account_state(
                    exchange_id=self.exchange_id,
                    account_id=self.account_id,
                    state=st,
                )

                # 2) snapshot history
                snap_row = {
                    "exchange_id": self.exchange_id,
                    "account_id": self.account_id,
                    "ts": datetime.now(timezone.utc),
                    "wallet_balance": float(st.get("wallet_balance") or 0.0),
                    "equity": float(st.get("equity") or 0.0),
                    "available_balance": float(st.get("available_balance") or 0.0),
                    "margin_used": float(st.get("margin_used") or 0.0),
                    "unrealized_pnl": float(st.get("unrealized_pnl") or 0.0),
                    "source": str(st.get("source") or "rest"),
                }
                self.storage.insert_account_balance_snapshots([snap_row])

                if self.debug:
                    log.info(
                        "[debug] wallet=%.4f equity=%.4f avail=%.4f margin_used=%.4f upnl=%.4f",
                        snap_row["wallet_balance"],
                        snap_row["equity"],
                        snap_row["available_balance"],
                        snap_row["margin_used"],
                        snap_row["unrealized_pnl"],
                    )

            except Exception as e:
                log.error("poll error: %s", e, exc_info=self.debug)

            # stop-aware sleep
            elapsed = time.time() - t0
            sleep_for = max(0.2, self.poll_sec - elapsed)
            end_at = time.time() + sleep_for
            while not self._stop.is_set() and time.time() < end_at:
                time.sleep(0.2)

        log.info("Writer stopped")


# -----------------------------------------------------------------------------
# main
# -----------------------------------------------------------------------------
def main() -> None:
    load_dotenv()

    debug = _env_bool("DEBUG_BALANCE_WRITER", False)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    log = logging.getLogger("platform.run_instances")

    log.info("=== RUN BALANCE WRITER START ===")

    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise SystemExit("PG_DSN env var is required")

    project_root = Path(__file__).resolve().parents[2]  # .../Trade-platform
    cfg_path = _resolve_cfg_path(project_root)
    cfg = _load_yaml(cfg_path)
    instances_cfg = cfg.get("instances") or []
    log.info("Config: %s | instances=%s", str(cfg_path), len(instances_cfg))
    if not instances_cfg:
        raise SystemExit(f"No instances defined in {cfg_path} (key: instances)")

    pool = create_pool(dsn)
    store = PostgreSQLStorage(pool)
    log.info("PostgreSQL storage initialized")

    writers: list[AccountBalanceWriter] = []

    for icfg in instances_cfg:
        exchange_name = str(icfg.get("exchange") or "").strip()
        account = str(icfg.get("account") or "").strip()
        if not exchange_name or not account:
            log.warning("Skip instance: exchange/account not set: %s", icfg)
            continue

        poll_sec = float(icfg.get("poll_sec") or icfg.get("balance_snapshot_sec") or 60)

        # Ensure exchange/account ids exist in DB.
        ids = store.ensure_exchange_account_symbol_map(exchange=exchange_name, account=account, symbols=[])
        exchange_id = int(ids["_exchange_id"])
        account_id = int(ids["_account_id"])

        w = AccountBalanceWriter(
            exchange_name=exchange_name,
            account=account,
            exchange_id=exchange_id,
            account_id=account_id,
            storage=store,
            poll_sec=poll_sec,
            debug=debug,
            binance_timeout_sec=(float(icfg.get("timeout_sec")) if icfg.get("timeout_sec") is not None else None),
            binance_retry_max=(int(icfg.get("retry_max")) if icfg.get("retry_max") is not None else None),
            binance_debug_requests=bool(icfg.get("debug_requests") or False),
        )
        writers.append(w)

        log.info(
            "Writer ready: exchange=%s account=%s exchange_id=%s account_id=%s poll_sec=%.1f",
            exchange_name,
            account,
            exchange_id,
            account_id,
            poll_sec,
        )

    if not writers:
        raise SystemExit("No valid writers to run (check instances config)")

    stop_flag = threading.Event()

    def _stop(*_args: Any) -> None:
        if stop_flag.is_set():
            return
        stop_flag.set()
        log.info("Stopping writers...")
        for w in writers:
            w.stop()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    log.info("Starting %d balance writer(s)", len(writers))
    for w in writers:
        w.start()

    try:
        while not stop_flag.is_set():
            time.sleep(0.5)
    finally:
        _stop()
        for w in writers:
            w.join(timeout=5.0)
        log.info("=== RUN BALANCE WRITER STOP ===")


if __name__ == "__main__":
    main()
