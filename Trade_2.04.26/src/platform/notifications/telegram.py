# src/platform/notifications/telegram.py
from __future__ import annotations

import os
import time
import logging
from dataclasses import dataclass
from typing import Optional, List, Tuple

import requests
from requests import Response
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger("platform.notifications.telegram")


TELEGRAM_MAX_LEN = 3900  # безопасный лимит (<4096)

_DEFAULT_CONNECT_TIMEOUT = 10
_DEFAULT_READ_TIMEOUT = 45
_DEFAULT_MEDIA_READ_TIMEOUT = 90
_DEFAULT_RETRIES = 2


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    if not raw:
        return int(default)
    try:
        v = int(raw)
        return v if v > 0 else int(default)
    except Exception:
        return int(default)


def _timeout_pair(*, media: bool = False) -> Tuple[int, int]:
    connect_timeout = _env_int('TELEGRAM_CONNECT_TIMEOUT', _DEFAULT_CONNECT_TIMEOUT)
    base_read = _DEFAULT_MEDIA_READ_TIMEOUT if media else _DEFAULT_READ_TIMEOUT
    read_timeout = _env_int('TELEGRAM_READ_TIMEOUT', base_read)
    return connect_timeout, read_timeout


def _build_session() -> requests.Session:
    retries = Retry(
        total=_env_int('TELEGRAM_RETRIES', _DEFAULT_RETRIES),
        connect=_env_int('TELEGRAM_RETRIES', _DEFAULT_RETRIES),
        read=_env_int('TELEGRAM_RETRIES', _DEFAULT_RETRIES),
        status=_env_int('TELEGRAM_RETRIES', _DEFAULT_RETRIES),
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({'POST'}),
        raise_on_status=False,
        respect_retry_after_header=True,
    )

    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    session.mount('https://', adapter)
    session.mount('http://', adapter)

    if _env('TELEGRAM_DISABLE_PROXY').lower() in {'1', 'true', 'yes', 'on'}:
        session.trust_env = False

    return session


def _log_telegram_http_error(prefix: str, resp: Response, target: TelegramTarget) -> None:
    body = ''
    try:
        body = (resp.text or '')[:500]
    except Exception:
        body = '<no-body>'

    log.error(
        '%s failed target=%s chat_id=%s status=%s body=%s',
        prefix,
        target.name,
        target.chat_id,
        resp.status_code,
        body,
    )


def _sleep_before_retry(attempt: int) -> None:
    if attempt <= 0:
        return
    time.sleep(min(2.0 * attempt, 5.0))


def _post_telegram(
    url: str,
    *,
    target: TelegramTarget,
    json: Optional[dict] = None,
    data: Optional[dict] = None,
    files: Optional[dict] = None,
    media: bool = False,
    action_name: str = 'Telegram request',
) -> bool:
    session = _build_session()
    timeout = _timeout_pair(media=media)
    attempts = max(1, _env_int('TELEGRAM_ATTEMPTS', 2))

    try:
        for attempt in range(1, attempts + 1):
            try:
                r = session.post(url, json=json, data=data, files=files, timeout=timeout)
                if r.status_code == 200:
                    return True

                _log_telegram_http_error(action_name, r, target)

                if r.status_code not in (429, 500, 502, 503, 504):
                    return False
            except requests.exceptions.Timeout:
                log.warning(
                    '%s timeout target=%s chat_id=%s attempt=%d/%d timeout=%s',
                    action_name, target.name, target.chat_id, attempt, attempts, timeout
                )
            except requests.exceptions.RequestException:
                log.exception(
                    '%s request exception target=%s chat_id=%s attempt=%d/%d',
                    action_name, target.name, target.chat_id, attempt, attempts
                )

            if attempt < attempts:
                _sleep_before_retry(attempt)

        return False
    finally:
        session.close()


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
    Собирает targets из env.

    Поддерживаемые направления:

    LEGACY/PRIMARY:
      TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID

    MAIN (личка):
      TELEGRAM_MAIN_BOT_TOKEN + TELEGRAM_MAIN_CHAT_ID

    CHANNEL:
      TELEGRAM_TOKEN + TELEGRAM_CHANNEL

    FRIENDS:
      TELEGRAM_FRIEND_BOT_TOKEN_1 + TELEGRAM_FRIEND_CHAT_ID_1
      ...

    ВАЖНО:
    - если заданы и MAIN, и CHANNEL, сообщения будут отправлены в ОБА направления;
    - если задан LEGACY/PRIMARY, он тоже участвует в рассылке;
    - дубликаты token/chat_id автоматически убираются.
    """
    targets: List[TelegramTarget] = []
    seen: set[tuple[str, str]] = set()

    def _add_target(name: str, token: str, chat: str) -> None:
        token = str(token or '').strip()
        chat = str(chat or '').strip()
        if not token or not chat:
            return
        key = (token, chat)
        if key in seen:
            return
        seen.add(key)
        targets.append(TelegramTarget(name=name, bot_token=token, chat_id=chat))

    primary_token = _env("TELEGRAM_BOT_TOKEN")
    primary_chat = _env("TELEGRAM_CHAT_ID")
    main_token = _env("TELEGRAM_MAIN_BOT_TOKEN")
    main_chat = _env("TELEGRAM_MAIN_CHAT_ID")
    channel_token = _env("TELEGRAM_TOKEN")
    channel_chat = _env("TELEGRAM_CHANNEL")

    _add_target("primary", primary_token, primary_chat)
    _add_target("main", main_token, main_chat)
    _add_target("channel", channel_token, channel_chat)

    if not include_friends:
        return targets

    friend_fallback_token = primary_token or main_token or channel_token

    for i in range(1, max(1, int(max_friends)) + 1):
        chat = _env(f"TELEGRAM_FRIEND_CHAT_ID_{i}")
        token = _env(f"TELEGRAM_FRIEND_BOT_TOKEN_{i}")

        if not chat:
            continue

        if not token and fallback_friend_token_to_primary:
            token = friend_fallback_token

        if not token:
            continue

        _add_target(f"friend_{i}", token, chat)

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
    disable_notification: bool = False,
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
        "disable_notification": bool(disable_notification),
    }

    return _post_telegram(
        url,
        target=target,
        json=payload,
        media=False,
        action_name='Telegram sendMessage',
    )


# -------------------------
# send media (photo/document)
# -------------------------
def _truncate_caption(text: str, max_len: int = 1024) -> str:
    s = (text or "").strip()
    if not s:
        return ""
    return s if len(s) <= max_len else (s[: max_len - 1] + "…")


def send_telegram_photo(
    photo_path: str,
    *,
    caption: str = "",
    target: Optional[TelegramTarget] = None,
    bot_token: Optional[str] = None,
    chat_id: Optional[str] = None,
    disable_notification: bool = False,
) -> bool:
    """
    Отправляет PNG/JPG как фото в Telegram (sendPhoto).
    photo_path: путь к файлу на диске.
    """
    photo_path = str(photo_path or "").strip()
    if not photo_path or (not os.path.exists(photo_path)):
        log.warning("Telegram photo not found: %s", photo_path)
        return False

    if target is None:
        if bot_token and chat_id:
            target = TelegramTarget(name="inline", bot_token=str(bot_token), chat_id=str(chat_id))
        else:
            token = _env("TELEGRAM_BOT_TOKEN")
            cid = _env("TELEGRAM_CHAT_ID")
            if token and cid:
                target = TelegramTarget(name="primary", bot_token=token, chat_id=cid)

    if target is None:
        log.warning("Telegram target not configured (missing token/chat_id)")
        return False

    url = f"https://api.telegram.org/bot{target.bot_token}/sendPhoto"
    data = {
        "chat_id": target.chat_id,
        "caption": _truncate_caption(caption),
        "disable_notification": bool(disable_notification),
    }

    try:
        with open(photo_path, "rb") as f:
            files = {"photo": f}
            return _post_telegram(
                url,
                target=target,
                data=data,
                files=files,
                media=True,
                action_name='Telegram sendPhoto',
            )
    except Exception:
        log.exception("Telegram sendPhoto exception target=%s chat_id=%s path=%s", target.name, target.chat_id, photo_path)
        return False


def send_telegram_document(
    file_path: str,
    *,
    caption: str = "",
    target: Optional[TelegramTarget] = None,
    bot_token: Optional[str] = None,
    chat_id: Optional[str] = None,
    disable_notification: bool = False,
) -> bool:
    """
    Отправляет файл как документ в Telegram (sendDocument).
    Полезно, если PNG слишком большой или нужно сохранить «как файл».
    """
    file_path = str(file_path or "").strip()
    if not file_path or (not os.path.exists(file_path)):
        log.warning("Telegram document not found: %s", file_path)
        return False

    if target is None:
        if bot_token and chat_id:
            target = TelegramTarget(name="inline", bot_token=str(bot_token), chat_id=str(chat_id))
        else:
            token = _env("TELEGRAM_BOT_TOKEN")
            cid = _env("TELEGRAM_CHAT_ID")
            if token and cid:
                target = TelegramTarget(name="primary", bot_token=token, chat_id=cid)

    if target is None:
        log.warning("Telegram target not configured (missing token/chat_id)")
        return False

    url = f"https://api.telegram.org/bot{target.bot_token}/sendDocument"
    data = {
        "chat_id": target.chat_id,
        "caption": _truncate_caption(caption),
        "disable_notification": bool(disable_notification),
    }

    try:
        with open(file_path, "rb") as f:
            files = {"document": f}
            return _post_telegram(
                url,
                target=target,
                data=data,
                files=files,
                media=True,
                action_name='Telegram sendDocument',
            )
    except Exception:
        log.exception("Telegram sendDocument exception target=%s chat_id=%s path=%s", target.name, target.chat_id, file_path)
        return False


def broadcast_telegram_photo(photo_path: str, *, caption: str = "", targets: List[TelegramTarget]) -> int:
    ok = 0
    for t in targets:
        if send_telegram_photo(photo_path, caption=caption, target=t):
            ok += 1
    return ok


def broadcast_telegram_document(file_path: str, *, caption: str = "", targets: List[TelegramTarget]) -> int:
    ok = 0
    for t in targets:
        if send_telegram_document(file_path, caption=caption, target=t):
            ok += 1
    return ok


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
