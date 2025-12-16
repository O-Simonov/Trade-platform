# src/platform/run_smoke.py
import logging
import os
import time

from src.platform.data.storage.postgres.pool import create_pool
from src.platform.data.storage.postgres.storage import PostgreSQLStorage


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logger = logging.getLogger("platform.smoke")

    logger.info("=== PLATFORM SMOKE START ===")

    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise SystemExit("PG_DSN env var is required")

    pool = create_pool(dsn)
    store = PostgreSQLStorage(pool)

    logger.info("PostgreSQL pool OK")
    logger.info("Storage OK")
    logger.info("Entering idle loop")

    try:
        while True:
            time.sleep(5)
            logger.info("smoke alive")
    except KeyboardInterrupt:
        logger.info("Shutdown requested")


if __name__ == "__main__":
    main()
