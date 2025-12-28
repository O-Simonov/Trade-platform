# src/platform/exchanges/binance/ws.py
from __future__ import annotations

import json
import threading
import time
import logging
import websocket
from typing import Callable, Optional

log = logging.getLogger("binance.ws")

WS_STREAM_BASE = "wss://fstream.binance.com/stream?streams="
WS_WS_BASE = "wss://fstream.binance.com/ws"


class BinanceWS(threading.Thread):
    def __init__(
        self,
        *,
        url: str,
        on_message,
        name: str = "BinanceWS",
        on_open: Optional[Callable[[], None]] = None,
        on_close: Optional[Callable[[], None]] = None,
    ):
        super().__init__(daemon=True, name=name)
        self.url = url
        self.on_message_cb = on_message
        self._ws: websocket.WebSocketApp | None = None
        self._stop = threading.Event()

        self.connected = threading.Event()
        self._on_open_hook = on_open
        self._on_close_hook = on_close

    def run(self):
        log.info("[%s] connecting → %s", self.name, self.url)

        def _on_open(_ws):
            self.connected.set()
            log.info("[%s] WS CONNECTED", self.name)
            try:
                if self._on_open_hook:
                    self._on_open_hook()
            except Exception:
                log.exception("[%s] on_open hook failed", self.name)

        def _on_close(_ws, *_a):
            self.connected.clear()
            log.warning("[%s] WS CLOSED", self.name)
            try:
                if self._on_close_hook:
                    self._on_close_hook()
            except Exception:
                log.exception("[%s] on_close hook failed", self.name)

        def _on_error(_ws, err):
            # error не всегда означает close, поэтому connected не трогаем тут
            log.error("[%s] WS ERROR: %s", self.name, err)

        while not self._stop.is_set():
            try:
                self._ws = websocket.WebSocketApp(
                    self.url,
                    on_open=_on_open,
                    on_message=lambda ws, msg: self._handle(msg),
                    on_error=_on_error,
                    on_close=_on_close,
                )
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                self.connected.clear()
                log.exception("[%s] WS exception: %s", self.name, e)

            # небольшой backoff
            for _ in range(10):
                if self._stop.is_set():
                    break
                time.sleep(0.2)

    def stop(self):
        self._stop.set()
        self.connected.clear()
        try:
            if self._ws:
                self._ws.close()
        except Exception:
            pass

    def _handle(self, msg: str):
        try:
            data = json.loads(msg)
        except Exception:
            return

        # combined stream -> {"stream":"...", "data":{...}}
        if isinstance(data, dict) and "data" in data and "stream" in data:
            payload = data.get("data")
        else:
            payload = data

        if payload is None:
            return

        self.on_message_cb(payload)
