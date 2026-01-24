# src/platform/notifications/telegram.py
from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from typing import Optional, List

import requests

log = logging.getLogger("platform.notifications.telegram")

TELEGRAM_MAX_LEN = 3900  # безопасный лимит (<4096)


@dataclass(frozen=True)
class TelegramTarget:
    name: str
    bot_token: str
    chat_id: str


def split_long_message(text: str, max_len: int = TELEGRAM_MAX_LEN) -> List[str]:
    """
    Разбивает длинный текст под лимит Telegram.
    Старается резать по пустым строкам/переводам строк.
    """
    s = (text or "").strip()
    if not s:
        return []

    if len(s) <= max_len:
        return [s]

    parts: List[str] = []
    buf = ""

    for chunk in s.split("\n\n"):
        cand = (buf + "\n\n" + chunk).strip() if buf else chunk.strip()
        if len(cand) <= max_len:
            buf = cand
        else:
            if buf:
                parts.append(buf)
                buf = ""

            # если сам chunk огромный - режем по строкам
            if len(chunk) > max_len:
                line_buf = ""
                for line in chunk.splitlines():
                    cand2 = (line_buf + "\n" + line).strip() if line_buf else line
                    if len(cand2) <= max_len:
                        line_buf = cand2
                    else:
                        if line_buf:
                            parts.append(line_buf)
                        line_buf = line[:max_len]
                if line_buf:
                    parts.append(line_buf)
            else:
                buf = chunk.strip()

    if buf:
        parts.append(buf)

    return [p for p in parts if p.strip()]


def _default_target() -> Optional[TelegramTarget]:
    token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
    if not token or not chat_id:
        return None
    return TelegramTarget(name="primary", bot_token=token, chat_id=chat_id)


def send_telegram_message(
    text: str,
    *,
    target: Optional[TelegramTarget] = None,
    bot_token: Optional[str] = None,
    chat_id: Optional[str] = None,
    disable_preview: bool = True,
) -> bool:
    """
    Отправляет сообщение в Telegram.
    Можно передать либо target, либо bot_token/chat_id.
    Если ничего не передано — берёт TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID из env.
    """
    text = (text or "").strip()
    if not text:
        return False

    if target is None:
        if bot_token and chat_id:
            target = TelegramTarget(name="inline", bot_token=str(bot_token), chat_id=str(chat_id))
        else:
            target = _default_target()

    if target is None:
        log.warning("Telegram target not configured (missing token/chat_id)")
        return False

    url = f"https://api.telegram.org/bot{target.bot_token}/sendMessage"
    payload = {
        "chat_id": target.chat_id,
        "text": text,
        "disable_web_page_preview": bool(disable_preview),
    }

    try:
        r = requests.post(url, json=payload, timeout=15)
        if r.status_code != 200:
            log.error("Telegram send failed: %s %s", r.status_code, r.text[:300])
            return False
        return True
    except Exception:
        log.exception("Telegram send exception")
        return False


def broadcast_telegram_message(text: str, *, targets: List[TelegramTarget]) -> int:
    """
    Рассылает одно сообщение в несколько получателей.
    Возвращает количество успешных отправок.
    """
    ok = 0
    for t in targets:
        if send_telegram_message(text, target=t):
            ok += 1
    return ok
