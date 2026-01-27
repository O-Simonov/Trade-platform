# src/platform/notifications/telegram.py
from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from typing import Optional, List

import requests

log = logging.getLogger("platform.notifications.telegram")

TELEGRAM_MAX_LEN = 3900  # безопасный лимит (<4096)


# -------------------------
# models
# -------------------------
@dataclass(frozen=True)
class TelegramTarget:
    name: str
    bot_token: str
    chat_id: str


# -------------------------
# tiny .env loader (без зависимостей)
# -------------------------
def load_dotenv_file(path: str = ".env", *, override: bool = False) -> int:
    """
    Мини-лоадер .env:
      KEY=VALUE
      # comment
    Не требует python-dotenv.
    Возвращает количество установленных переменных.
    """
    try:
        if not os.path.exists(path):
            return 0

        n = 0
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue

                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip().strip('"').strip("'")

                if not key:
                    continue
                if (key in os.environ) and (not override):
                    continue

                os.environ[key] = val
                n += 1

        if n > 0:
            log.info("Loaded .env vars: %d (path=%s)", n, path)
        return n
    except Exception:
        log.exception("Failed to load .env file: %s", path)
        return 0


def _env(name: str) -> str:
    v = os.getenv(name)
    return v.strip() if isinstance(v, str) else ""


# -------------------------
# message split
# -------------------------
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


# -------------------------
# targets resolving
# -------------------------
def resolve_targets_from_env(
    *,
    include_friends: bool = True,
    max_friends: int = 10,
    fallback_friend_token_to_primary: bool = True,
) -> List[TelegramTarget]:
    """
    Собирает targets только из env (как ты хочешь):

    MAIN:
      TELEGRAM_BOT_TOKEN
      TELEGRAM_CHAT_ID

    FRIENDS:
      TELEGRAM_FRIEND_BOT_TOKEN_1
      TELEGRAM_FRIEND_CHAT_ID_1
      TELEGRAM_FRIEND_BOT_TOKEN_2
      TELEGRAM_FRIEND_CHAT_ID_2
      ...

    Если fallback_friend_token_to_primary=True и FRIEND_BOT_TOKEN_i пустой,
    то будет использован основной TELEGRAM_BOT_TOKEN (если он есть).
    """
    targets: List[TelegramTarget] = []

    primary_token = _env("TELEGRAM_BOT_TOKEN")
    primary_chat = _env("TELEGRAM_CHAT_ID")

    if primary_token and primary_chat:
        targets.append(TelegramTarget(name="primary", bot_token=primary_token, chat_id=primary_chat))

    if not include_friends:
        return targets

    for i in range(1, max(1, int(max_friends)) + 1):
        chat = _env(f"TELEGRAM_FRIEND_CHAT_ID_{i}")
        token = _env(f"TELEGRAM_FRIEND_BOT_TOKEN_{i}")

        if not chat:
            continue

        if not token and fallback_friend_token_to_primary:
            token = primary_token

        if not token:
            continue

        targets.append(TelegramTarget(name=f"friend_{i}", bot_token=token, chat_id=chat))

    return targets


# -------------------------
# send
# -------------------------
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
            # fallback: primary from env
            token = _env("TELEGRAM_BOT_TOKEN")
            cid = _env("TELEGRAM_CHAT_ID")
            if token and cid:
                target = TelegramTarget(name="primary", bot_token=token, chat_id=cid)

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
