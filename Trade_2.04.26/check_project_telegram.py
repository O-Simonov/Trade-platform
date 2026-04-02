#!/usr/bin/env python3
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple


PROJECT_ROOT = Path(__file__).resolve().parent


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
            os.environ.setdefault(k, v)

    return env_path, parsed


def first_non_empty(*keys: str) -> Tuple[Optional[str], Optional[str]]:
    for key in keys:
        value = os.getenv(key, "").strip()
        if value:
            return key, value
    return None, None


def mask(value: str, keep_start: int = 6, keep_end: int = 4) -> str:
    if not value:
        return "<EMPTY>"
    if len(value) <= keep_start + keep_end:
        return "*" * len(value)
    return f"{value[:keep_start]}...{value[-keep_end:]}"


def print_header(title: str) -> None:
    print(f"\n{'=' * 20} {title} {'=' * 20}")


def print_kv(key: str, value: str) -> None:
    print(f"{key:<35} {value}")


def find_yaml_files(root: Path) -> List[Path]:
    results: List[Path] = []
    for pattern in ("*.yml", "*.yaml"):
        results.extend(root.rglob(pattern))
    return sorted(results)


def parse_simple_yaml(path: Path) -> Dict[str, str]:
    """
    Примитивный парсер key: value для диагностики.
    """
    result: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue

        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()

        if not key:
            continue

        if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
            value = value[1:-1]

        result[key] = value

    return result


def as_bool(value: Optional[str]) -> Optional[bool]:
    if value is None:
        return None
    v = value.strip().lower()
    if v in {"true", "1", "yes", "on"}:
        return True
    if v in {"false", "0", "no", "off"}:
        return False
    return None


def find_interesting_yaml_configs(root: Path) -> List[Tuple[Path, Dict[str, str]]]:
    interesting = []
    for path in find_yaml_files(root):
        try:
            data = parse_simple_yaml(path)
        except Exception:
            continue

        if any(k.startswith("telegram_") for k in data.keys()):
            interesting.append((path, data))
    return interesting


def find_chart_like_files(root: Path) -> List[Path]:
    exts = {".png", ".jpg", ".jpeg", ".webp"}
    candidates: List[Path] = []
    skip_parts = {".venv", "__pycache__", ".git"}

    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if any(part in skip_parts for part in path.parts):
            continue
        if path.suffix.lower() in exts:
            candidates.append(path)

    return sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)


def find_code_mentions(root: Path, patterns: List[str]) -> Dict[str, List[Tuple[Path, int, str]]]:
    matches: Dict[str, List[Tuple[Path, int, str]]] = {p: [] for p in patterns}
    code_exts = {".py", ".yaml", ".yml", ".env", ".txt", ".md", ".ini"}

    skip_parts = {".venv", "__pycache__", ".git"}
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if any(part in skip_parts for part in path.parts):
            continue
        if path.suffix.lower() not in code_exts and path.name != ".env":
            continue

        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception:
            continue

        for i, line in enumerate(lines, 1):
            for pattern in patterns:
                if pattern.lower() in line.lower():
                    matches[pattern].append((path, i, line.strip()))
    return matches


def main() -> int:
    env_path, parsed_env = load_env()

    print_header("ENV")
    print_kv("Project root", str(PROJECT_ROOT))
    print_kv("Found .env", str(env_path) if env_path else "<NOT FOUND>")

    env_keys = [
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_CHAT_ID",
        "TELEGRAM_MAIN_BOT_TOKEN",
        "TELEGRAM_MAIN_CHAT_ID",
        "TELEGRAM_TOKEN",
        "TELEGRAM_CHANNEL",
    ]

    for key in env_keys:
        value = os.getenv(key, "").strip()
        shown = mask(value) if "TOKEN" in key else (mask(value, 4, 2) if value else "<EMPTY>")
        source = "from .env" if key in parsed_env else ("from environment" if value else "-")
        print_kv(key, f"{shown}   [{source}]")

    token_key, token = first_non_empty(
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_MAIN_BOT_TOKEN",
        "TELEGRAM_TOKEN",
    )
    chat_key, chat_id = first_non_empty(
        "TELEGRAM_CHAT_ID",
        "TELEGRAM_MAIN_CHAT_ID",
        "TELEGRAM_CHANNEL",
    )

    print_header("RESOLVED TARGET")
    print_kv("Resolved token key", token_key or "<NOT FOUND>")
    print_kv("Resolved chat key", chat_key or "<NOT FOUND>")
    print_kv("Resolved token", mask(token) if token else "<EMPTY>")
    print_kv("Resolved chat_id", mask(chat_id, 4, 2) if chat_id else "<EMPTY>")

    print_header("YAML CONFIGS")
    yaml_configs = find_interesting_yaml_configs(PROJECT_ROOT)
    if not yaml_configs:
        print("Конфиги с telegram_* не найдены.")
    else:
        for path, data in yaml_configs:
            print(f"\n--- {path}")
            for field in (
                "telegram_enabled",
                "telegram_send_text",
                "telegram_send_plots",
                "telegram_send_images",
                "telegram_chat_id",
                "telegram_channel",
            ):
                if field in data:
                    print_kv(field, data[field])

    print_header("RECENT IMAGE FILES")
    images = find_chart_like_files(PROJECT_ROOT)
    if not images:
        print("Файлы графиков/картинок не найдены.")
    else:
        for path in images[:20]:
            print_kv("image", str(path))

    print_header("CODE MENTIONS")
    patterns = [
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_CHAT_ID",
        "TELEGRAM_MAIN_BOT_TOKEN",
        "TELEGRAM_MAIN_CHAT_ID",
        "TELEGRAM_TOKEN",
        "TELEGRAM_CHANNEL",
        "sendMessage",
        "sendPhoto",
        "telegram_enabled",
        "telegram_send_text",
        "telegram_send_plots",
    ]
    mentions = find_code_mentions(PROJECT_ROOT, patterns)

    for pattern, rows in mentions.items():
        print(f"\n[{pattern}]")
        if not rows:
            print("  not found")
            continue
        for path, line_no, line in rows[:10]:
            print(f"  {path}:{line_no}: {line}")

    print_header("SUMMARY")

    problems = []

    if not env_path:
        problems.append("Файл .env не найден.")
    if not token or not chat_id:
        problems.append("Не удалось определить итоговые Telegram token/chat_id.")
    if token_key == "TELEGRAM_MAIN_BOT_TOKEN" and os.getenv("TELEGRAM_TOKEN", "").strip():
        problems.append(
            "Сейчас по приоритету выбирается MAIN-бот, хотя в .env есть ещё TELEGRAM_TOKEN. "
            "Нужно убедиться, что проект должен слать именно через MAIN, а не через канал."
        )
    if not images:
        problems.append("В проекте не найдено ни одного файла графика/изображения.")
    if not yaml_configs:
        problems.append("Не найдены yaml-конфиги с telegram_* настройками.")

    if problems:
        print("Возможные проблемы:")
        for p in problems:
            print(f"- {p}")
    else:
        print("Явных проблем по структуре проекта не найдено.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())