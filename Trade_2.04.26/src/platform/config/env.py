# src/platform/config/env.py
from __future__ import annotations

import os
from typing import Optional, Tuple


def require(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(f"Environment variable {name} is required")
    return value.strip()


def optional(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


def get_binance_base() -> Tuple[Optional[str], Optional[str]]:
    return (
        optional("BINANCE_BASE_MAIN_API_KEY"),
        optional("BINANCE_BASE_MAIN_API_SECRET"),
    )


def get_binance_hedge() -> Tuple[Optional[str], Optional[str]]:
    return (
        optional("BINANCE_HEDGE_MAIN_API_KEY"),
        optional("BINANCE_HEDGE_MAIN_API_SECRET"),
    )


def get_telegram_main_config() -> Tuple[str, str]:
    bot_token = (
        optional("TELEGRAM_MAIN_BOT_TOKEN")
        or optional("TELEGRAM_BOT_TOKEN")
        or optional("TELEGRAM_TOKEN")
    )

    chat_id = (
        optional("TELEGRAM_MAIN_CHAT_ID")
        or optional("TELEGRAM_CHAT_ID")
        or optional("TELEGRAM_CHANNEL")
    )

    if not bot_token or not chat_id:
        raise RuntimeError("Telegram config is not set")

    return bot_token, chat_id


def get_pg_dsn() -> str:
    return optional("PG_DSN") or require("POSTGRES_DSN")


def get_marketdata_config() -> str:
    return require("MARKETDATA_CONFIG")


def get_marketdata_instance() -> str:
    return optional("MARKETDATA_INSTANCE", "default") or "default"


def get_log_level() -> str:
    return (optional("LOG_LEVEL", "INFO") or "INFO").upper()


def get_environment() -> str:
    return optional("ENVIRONMENT", "local") or "local"
