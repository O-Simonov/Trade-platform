# src/platform/exchanges/binance/ws.py
from __future__ import annotations

import json
import random
import threading
import logging
from typing import Callable, Optional, Any

import websocket  # websocket-client

log = logging.getLogger("binance.ws")

WS_STREAM_BASE = "wss://fstream.binance.com/stream?streams="
WS_WS_BASE = "wss://fstream.binance.com/ws/"


class BinanceWS:
    """
    Надёжный WS:
    ✅ 1 поток на соединение
    ✅ reconnect внутри потока
    ✅ backoff + jitter
    ✅ ping_interval / ping_timeout
    ✅ stop() прерывает sleep
    ✅ JSON decode (dict/list) для on_message
    """

    def __init__(
        self,
        *,
        name: str,
        url: str,
        on_message: Callable[[Any], None],
        on_open: Optional[Callable[[], None]] = None,
        on_close: Optional[Callable[[], None]] = None,
        ping_interval: int = 30,
        ping_timeout: int = 20,
        reconnect_min_delay: float = 1.0,
        reconnect_max_delay: float = 30.0,
        parse_json: bool = True,
    ):
        self.name = str(name)
        self.url = str(url)

        self._on_message_cb = on_message
        self._on_open_cb = on_open
        self._on_close_cb = on_close

        self.ping_interval = max(10, int(ping_interval))
        self.ping_timeout = max(5, int(ping_timeout))

        self.reconnect_min_delay = max(0.2, float(reconnect_min_delay))
        self.reconnect_max_delay = max(self.reconnect_min_delay, float(reconnect_max_delay))

        self.parse_json = bool(parse_json)

        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._ws: Optional[websocket.WebSocketApp] = None

        self._opened_once = False
        self._opened_lock = threading.Lock()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_loop, name=self.name, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        try:
            if self._ws:
                self._ws.keep_running = False
                self._ws.close()
        except Exception:
            pass

    def _run_loop(self) -> None:
        delay = self.reconnect_min_delay

        while not self._stop.is_set():
            try:
                with self._opened_lock:
                    self._opened_once = False

                self._ws = websocket.WebSocketApp(
                    self.url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )

                self._ws.run_forever(
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_timeout,
                    ping_payload="ping",
                    skip_utf8_validation=True,
                )

            except Exception:
                log.exception("[%s] WS LOOP EXCEPTION", self.name)

            if self._stop.is_set():
                break

            with self._opened_lock:
                opened = self._opened_once
            if opened:
                delay = self.reconnect_min_delay

            jitter = random.uniform(0.0, 0.35)
            sleep_s = min(self.reconnect_max_delay, delay) * (1.0 + jitter)

            log.warning("[%s] reconnect in %.2fs ...", self.name, sleep_s)

            if self._stop.wait(sleep_s):
                break

            delay = min(self.reconnect_max_delay, max(self.reconnect_min_delay, delay * 1.7))

    def _on_open(self, ws) -> None:
        log.info("[%s] WS CONNECTED", self.name)
        with self._opened_lock:
            self._opened_once = True
        try:
            if self._on_open_cb:
                self._on_open_cb()
        except Exception:
            log.exception("[%s] on_open callback failed", self.name)

    def _on_message(self, ws, message: str) -> None:
        payload: Any = message
        if self.parse_json:
            try:
                payload = json.loads(message)
            except Exception:
                payload = message  # fallback

        try:
            self._on_message_cb(payload)
        except Exception:
            log.exception("[%s] on_message callback failed", self.name)

    def _on_error(self, ws, error: Any) -> None:
        log.error("[%s] WS ERROR: %s", self.name, error)

    def _on_close(self, ws, close_status_code, close_msg) -> None:
        log.warning("[%s] WS CLOSED code=%s msg=%s", self.name, close_status_code, close_msg)
        try:
            if self._on_close_cb:
                self._on_close_cb()
        except Exception:
            log.exception("[%s] on_close callback failed", self.name)
