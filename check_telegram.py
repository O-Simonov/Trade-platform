#!/usr/bin/env python3
from __future__ import annotations

import json
import mimetypes
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib import error, parse, request


PROJECT_ROOT = Path(__file__).resolve().parent


@dataclass
class TelegramTarget:
    name: str
    token_key: str
    chat_key: str
    token: str
    chat_id: str


@dataclass
class TargetCheckResult:
    name: str
    token_key: str
    chat_key: str
    token_present: bool
    chat_present: bool
    token_masked: str
    chat_masked: str

    getme_ok: bool = False
    getchat_ok: bool = False
    sendmessage_ok: bool = False
    sendphoto_ok: Optional[bool] = None

    bot_username: str = ""
    chat_type: str = ""
    chat_title: str = ""
    error_stage: str = ""
    error_description: str = ""


def find_env_file(start: Path) -> Optional[Path]:
    candidates = [start / ".env"]
    candidates.extend(parent / ".env" for parent in start.parents)
    for path in candidates:
        if path.exists() and path.is_file():
            return path
    return None


def parse_env_file(path: Path) -> Dict[str, str]:
    data: Dict[str, str] = {}

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()

        if not line or line.startswith("#"):
            continue

        if line.startswith("export "):
            line = line[len("export "):].strip()

        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if not key:
            continue

        if value and value[0] not in ('"', "'"):
            value = re.split(r"\s+#", value, maxsplit=1)[0].strip()

        if (
            len(value) >= 2
            and value[0] == value[-1]
            and value[0] in ('"', "'")
        ):
            value = value[1:-1]

        data[key] = value

    return data


def load_env() -> Tuple[Optional[Path], Dict[str, str]]:
    env_path = find_env_file(PROJECT_ROOT)
    parsed: Dict[str, str] = {}

    if env_path:
        parsed = parse_env_file(env_path)
        for k, v in parsed.items():
            os.environ[k] = v

    return env_path, parsed


def _env(key: str) -> str:
    return os.getenv(key, "").strip()


def mask_token(token: str) -> str:
    if not token:
        return "<EMPTY>"
    if len(token) <= 10:
        return "*" * len(token)
    return f"{token[:6]}...{token[-4:]}"


def mask_chat_id(chat_id: str) -> str:
    if not chat_id:
        return "<EMPTY>"
    if len(chat_id) <= 6:
        return chat_id
    return f"{chat_id[:4]}...{chat_id[-2:]}"


def print_section(title: str) -> None:
    print(f"\n{'=' * 20} {title} {'=' * 20}")


def print_kv(key: str, value: str) -> None:
    print(f"{key:<30} {value}")


def telegram_request(
    method: str,
    token: str,
    data: Optional[Dict[str, str]] = None,
    files: Optional[Dict[str, Path]] = None,
    timeout: int = 20,
) -> Dict:
    url = f"https://api.telegram.org/bot{token}/{method}"

    if files:
        boundary = "----TelegramCheckBoundary7MA4YWxkTrZu0gW"
        body = build_multipart_body(data or {}, files, boundary)
        req = request.Request(
            url,
            data=body,
            method="POST",
            headers={
                "Content-Type": f"multipart/form-data; boundary={boundary}",
            },
        )
    else:
        encoded = parse.urlencode(data or {}).encode("utf-8")
        req = request.Request(
            url,
            data=encoded,
            method="POST",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )

    try:
        with request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw)
    except error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw)
            if isinstance(payload, dict):
                return payload
            return {"ok": False, "description": raw}
        except Exception:
            return {"ok": False, "description": f"HTTP {e.code}: {raw}"}
    except Exception as e:
        return {"ok": False, "description": str(e)}


def build_multipart_body(
    data: Dict[str, str],
    files: Dict[str, Path],
    boundary: str,
) -> bytes:
    chunks: list[bytes] = []
    boundary_bytes = boundary.encode("utf-8")

    for key, value in data.items():
        chunks.extend(
            [
                b"--" + boundary_bytes + b"\r\n",
                f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode("utf-8"),
                str(value).encode("utf-8"),
                b"\r\n",
            ]
        )

    for field_name, file_path in files.items():
        filename = file_path.name
        mime_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        file_bytes = file_path.read_bytes()

        chunks.extend(
            [
                b"--" + boundary_bytes + b"\r\n",
                (
                    f'Content-Disposition: form-data; name="{field_name}"; '
                    f'filename="{filename}"\r\n'
                ).encode("utf-8"),
                f"Content-Type: {mime_type}\r\n\r\n".encode("utf-8"),
                file_bytes,
                b"\r\n",
            ]
        )

    chunks.append(b"--" + boundary_bytes + b"--\r\n")
    return b"".join(chunks)


def resolve_all_telegram_targets(max_friends: int = 10) -> List[TelegramTarget]:
    targets: List[TelegramTarget] = []
    seen: set[tuple[str, str]] = set()

    pairs = [
        ("PRIMARY", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"),
        ("MAIN", "TELEGRAM_MAIN_BOT_TOKEN", "TELEGRAM_MAIN_CHAT_ID"),
        ("CHANNEL", "TELEGRAM_TOKEN", "TELEGRAM_CHANNEL"),
    ]

    for name, token_key, chat_key in pairs:
        token = _env(token_key)
        chat_id = _env(chat_key)
        if token and chat_id:
            sig = (token, chat_id)
            if sig not in seen:
                targets.append(
                    TelegramTarget(
                        name=name,
                        token_key=token_key,
                        chat_key=chat_key,
                        token=token,
                        chat_id=chat_id,
                    )
                )
                seen.add(sig)

    for i in range(1, max_friends + 1):
        token_key = f"TELEGRAM_FRIEND_BOT_TOKEN_{i}"
        chat_key = f"TELEGRAM_FRIEND_CHAT_ID_{i}"
        token = _env(token_key)
        chat_id = _env(chat_key)
        if token and chat_id:
            sig = (token, chat_id)
            if sig not in seen:
                targets.append(
                    TelegramTarget(
                        name=f"FRIEND_{i}",
                        token_key=token_key,
                        chat_key=chat_key,
                        token=token,
                        chat_id=chat_id,
                    )
                )
                seen.add(sig)

    return targets


def iter_all_slots(max_friends: int = 10) -> List[Tuple[str, str, str]]:
    slots = [
        ("PRIMARY", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"),
        ("MAIN", "TELEGRAM_MAIN_BOT_TOKEN", "TELEGRAM_MAIN_CHAT_ID"),
        ("CHANNEL", "TELEGRAM_TOKEN", "TELEGRAM_CHANNEL"),
    ]
    for i in range(1, max_friends + 1):
        slots.append(
            (
                f"FRIEND_{i}",
                f"TELEGRAM_FRIEND_BOT_TOKEN_{i}",
                f"TELEGRAM_FRIEND_CHAT_ID_{i}",
            )
        )
    return slots


def print_env_values(parsed_env: Dict[str, str], max_friends: int = 10) -> None:
    print_section("ENV VALUES")

    keys = [
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_CHAT_ID",
        "TELEGRAM_MAIN_BOT_TOKEN",
        "TELEGRAM_MAIN_CHAT_ID",
        "TELEGRAM_TOKEN",
        "TELEGRAM_CHANNEL",
    ]

    for i in range(1, max_friends + 1):
        keys.append(f"TELEGRAM_FRIEND_BOT_TOKEN_{i}")
        keys.append(f"TELEGRAM_FRIEND_CHAT_ID_{i}")

    for key in keys:
        value = _env(key)
        if "TOKEN" in key:
            shown = mask_token(value)
        else:
            shown = mask_chat_id(value)

        source = "from .env" if key in parsed_env else ("from environment" if value else "-")
        print_kv(key, f"{shown}   [{source}]")


def print_resolved_targets(targets: List[TelegramTarget]) -> None:
    print_section("RESOLVED TARGETS")

    if not targets:
        print("[FAIL] Не найдено ни одного полного направления (token + chat_id).")
        return

    for idx, target in enumerate(targets, start=1):
        print_kv(f"target[{idx}].name", target.name)
        print_kv(f"target[{idx}].token_key", target.token_key)
        print_kv(f"target[{idx}].chat_key", target.chat_key)
        print_kv(f"target[{idx}].token", mask_token(target.token))
        print_kv(f"target[{idx}].chat_id", mask_chat_id(target.chat_id))
        print("-" * 64)


def short_error(payload: Dict) -> str:
    if not isinstance(payload, dict):
        return str(payload)
    desc = str(payload.get("description", "")).strip()
    code = payload.get("error_code")
    if code and desc:
        return f"{code}: {desc}"
    if desc:
        return desc
    return json.dumps(payload, ensure_ascii=False)


def check_one_target(target: TelegramTarget, image_path: Optional[Path]) -> TargetCheckResult:
    result = TargetCheckResult(
        name=target.name,
        token_key=target.token_key,
        chat_key=target.chat_key,
        token_present=bool(target.token),
        chat_present=bool(target.chat_id),
        token_masked=mask_token(target.token),
        chat_masked=mask_chat_id(target.chat_id),
    )

    print_section(f"{target.name} | CHECK getMe")
    getme = telegram_request("getMe", target.token)
    print(json.dumps(getme, ensure_ascii=False, indent=2))

    if not getme.get("ok"):
        result.error_stage = "getMe"
        result.error_description = short_error(getme)
        print("\n[ERROR] Бот не прошёл getMe.")
        return result

    result.getme_ok = True
    bot = getme.get("result", {}) or {}
    result.bot_username = str(bot.get("username", "") or "")

    print("\n[OK] Бот доступен.")
    print_kv("target", target.name)
    print_kv("token_key", target.token_key)
    print_kv("chat_key", target.chat_key)
    print_kv("bot_username", result.bot_username or "—")

    print_section(f"{target.name} | CHECK getChat")
    getchat = telegram_request("getChat", target.token, data={"chat_id": target.chat_id})
    print(json.dumps(getchat, ensure_ascii=False, indent=2))

    if not getchat.get("ok"):
        result.error_stage = "getChat"
        result.error_description = short_error(getchat)
        print("\n[ERROR] Не удалось получить чат.")
        return result

    result.getchat_ok = True
    chat = getchat.get("result", {}) or {}
    result.chat_type = str(chat.get("type", "") or "")
    result.chat_title = str(chat.get("title", "") or chat.get("username", "") or chat.get("first_name", "") or "")

    print("\n[OK] Чат доступен.")
    print_kv("chat_type", result.chat_type or "—")
    print_kv("chat_title", result.chat_title or "—")

    print_section(f"{target.name} | TEST sendMessage")
    sendmsg = telegram_request(
        "sendMessage",
        target.token,
        data={
            "chat_id": target.chat_id,
            "text": f"✅ Test message from check_telegram.py [{target.name}]",
        },
    )
    print(json.dumps(sendmsg, ensure_ascii=False, indent=2))

    if not sendmsg.get("ok"):
        result.error_stage = "sendMessage"
        result.error_description = short_error(sendmsg)
        print("\n[ERROR] sendMessage не прошёл.")
        return result

    result.sendmessage_ok = True
    print("\n[OK] Тестовое сообщение отправлено.")

    if image_path:
        print_section(f"{target.name} | TEST sendPhoto")
        sendphoto = telegram_request(
            "sendPhoto",
            target.token,
            data={
                "chat_id": target.chat_id,
                "caption": f"📈 Test photo from check_telegram.py [{target.name}]",
            },
            files={"photo": image_path},
        )
        print(json.dumps(sendphoto, ensure_ascii=False, indent=2))

        if not sendphoto.get("ok"):
            result.sendphoto_ok = False
            result.error_stage = "sendPhoto"
            result.error_description = short_error(sendphoto)
            print("\n[ERROR] sendPhoto не прошёл.")
            return result

        result.sendphoto_ok = True
        print("\n[OK] Тестовое фото отправлено.")
    else:
        result.sendphoto_ok = None

    return result


def build_results_for_all_slots(
    resolved_targets: List[TelegramTarget],
    image_path: Optional[Path],
    max_friends: int = 10,
) -> List[TargetCheckResult]:
    resolved_map = {(t.token_key, t.chat_key): t for t in resolved_targets}
    results: List[TargetCheckResult] = []

    for name, token_key, chat_key in iter_all_slots(max_friends=max_friends):
        token = _env(token_key)
        chat_id = _env(chat_key)

        if not token and not chat_id:
            results.append(
                TargetCheckResult(
                    name=name,
                    token_key=token_key,
                    chat_key=chat_key,
                    token_present=False,
                    chat_present=False,
                    token_masked="<EMPTY>",
                    chat_masked="<EMPTY>",
                    error_stage="env",
                    error_description="token и chat_id пустые",
                )
            )
            continue

        if not token or not chat_id:
            missing = []
            if not token:
                missing.append("token")
            if not chat_id:
                missing.append("chat_id")
            results.append(
                TargetCheckResult(
                    name=name,
                    token_key=token_key,
                    chat_key=chat_key,
                    token_present=bool(token),
                    chat_present=bool(chat_id),
                    token_masked=mask_token(token),
                    chat_masked=mask_chat_id(chat_id),
                    error_stage="env",
                    error_description=f"неполная пара: нет {', '.join(missing)}",
                )
            )
            continue

        target = resolved_map.get((token_key, chat_key))
        if target is None:
            target = TelegramTarget(
                name=name,
                token_key=token_key,
                chat_key=chat_key,
                token=token,
                chat_id=chat_id,
            )

        results.append(check_one_target(target, image_path=image_path))

    return results


def status_text(r: TargetCheckResult, with_photo: bool) -> str:
    if r.error_stage == "env":
        return f"ENV FAIL ({r.error_description})"

    if not r.getme_ok:
        return f"GETME FAIL ({r.error_description or 'unknown'})"

    if not r.getchat_ok:
        return f"GETCHAT FAIL ({r.error_description or 'unknown'})"

    if not r.sendmessage_ok:
        return f"MESSAGE FAIL ({r.error_description or 'unknown'})"

    if with_photo:
        if r.sendphoto_ok is True:
            return "OK"
        if r.sendphoto_ok is False:
            return f"PHOTO FAIL ({r.error_description or 'unknown'})"
        return "PHOTO NOT TESTED"

    return "OK"


def print_summary(results: List[TargetCheckResult], with_photo: bool) -> None:
    print_section("SUMMARY BY TARGET")

    header = (
        f"{'TARGET':<12} {'BOT':<18} {'CHAT':<14} {'TYPE':<10} "
        f"{'MSG':<6} {'PHOTO':<8} STATUS"
    )
    print(header)
    print("-" * len(header))

    for r in results:
        bot = r.bot_username[:18] if r.bot_username else "—"
        chat = r.chat_masked
        chat_type = r.chat_type[:10] if r.chat_type else "—"
        msg = "OK" if r.sendmessage_ok else "FAIL"
        if with_photo:
            if r.sendphoto_ok is True:
                photo = "OK"
            elif r.sendphoto_ok is False:
                photo = "FAIL"
            else:
                photo = "N/T"
        else:
            photo = "N/T"

        print(
            f"{r.name:<12} {bot:<18} {chat:<14} {chat_type:<10} "
            f"{msg:<6} {photo:<8} {status_text(r, with_photo)}"
        )

    print_section("FAIL DETAILS")
    failed = [r for r in results if status_text(r, with_photo) != "OK"]
    if not failed:
        print("Все направления прошли успешно.")
        return

    for r in failed:
        print_kv("target", r.name)
        print_kv("token_key", r.token_key)
        print_kv("chat_key", r.chat_key)
        print_kv("token", r.token_masked)
        print_kv("chat_id", r.chat_masked)
        print_kv("stage", r.error_stage or "—")
        print_kv("reason", r.error_description or "—")
        print("-" * 64)


def main() -> int:
    env_path, parsed_env = load_env()

    print_section("ENV FILE")
    if env_path:
        print_kv("Found .env", str(env_path))
    else:
        print("[WARN] Файл .env не найден.")

    print_env_values(parsed_env, max_friends=10)

    resolved_targets = resolve_all_telegram_targets(max_friends=10)
    print_resolved_targets(resolved_targets)

    image_path: Optional[Path] = None
    if len(sys.argv) > 1:
        candidate = Path(sys.argv[1]).expanduser().resolve()
        if candidate.exists() and candidate.is_file():
            image_path = candidate
        else:
            print(f"\n[WARN] Файл для фото не найден: {candidate}")

    results = build_results_for_all_slots(
        resolved_targets=resolved_targets,
        image_path=image_path,
        max_friends=10,
    )

    print_summary(results, with_photo=bool(image_path))

    print_section("TOTAL")
    total_slots = len(results)
    ok_slots = sum(1 for r in results if status_text(r, bool(image_path)) == "OK")
    print_kv("slots_total", str(total_slots))
    print_kv("slots_ok", str(ok_slots))
    print_kv("slots_failed", str(total_slots - ok_slots))
    print("\nГотово.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())