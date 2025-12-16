import argparse
from datetime import date, timedelta
from src.platform.ledger.stats_daily import StrategyStatsDaily
from src.platform.data.storage.postgres.pool import get_conn

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="dfrom", required=True)
    args = ap.parse_args()

    d0 = date.fromisoformat(args.dfrom)
    d1 = date.today()

    conn = get_conn()
    stats = StrategyStatsDaily(conn)

    d = d0
    while d <= d1:
        stats.compute_and_upsert("binance", d.isoformat())
        d += timedelta(days=1)

    conn.close()

if __name__ == "__main__":
    main()
