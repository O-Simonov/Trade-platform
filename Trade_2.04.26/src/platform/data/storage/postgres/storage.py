from __future__ import annotations

import re
import json
import hashlib
import time
import logging
from datetime import date, datetime, timezone, timedelta
from typing import Any, Iterable, Sequence, Mapping, List, Tuple, Dict, Optional

from psycopg.types.json import Jsonb
from psycopg import sql as _sql
from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)
log = logging.getLogger("storage.sql")

from .repositories.base import *
from .repositories.orders_repo import OrdersRepositoryMixin
from .repositories.fills_repo import FillsRepositoryMixin
from .repositories.positions_repo import PositionsRepositoryMixin
from .repositories.market_data_repo import MarketDataRepositoryMixin
from .repositories.signals_repo import SignalsRepositoryMixin
from .repositories.analytics_repo import AnalyticsRepositoryMixin
from .repositories.account_repo import AccountRepositoryMixin

class PostgreSQLStorage(
    BasePostgresRepositoryMixin,
    OrdersRepositoryMixin,
    FillsRepositoryMixin,
    PositionsRepositoryMixin,
    MarketDataRepositoryMixin,
    SignalsRepositoryMixin,
    AnalyticsRepositoryMixin,
    AccountRepositoryMixin,
):
    """PostgreSQL storage — registry, market state, OMS helpers, retention, exchangeInfo, positions, balances snapshots, orders, trades, stats."""
    pass
