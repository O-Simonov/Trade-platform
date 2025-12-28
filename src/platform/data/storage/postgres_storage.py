def upsert_symbol_filters(self, rows: list[dict]) -> int:
    if not rows:
        return 0

    sql = """
    INSERT INTO symbol_filters (
        exchange_id, symbol_id, symbol,
        min_qty, step_size, min_notional, max_qty,
        tick_size, price_precision, qty_precision,
        is_trading, raw_json, updated_at
    )
    VALUES (
        %(exchange_id)s, %(symbol_id)s, %(symbol)s,
        %(min_qty)s, %(step_size)s, %(min_notional)s, %(max_qty)s,
        %(tick_size)s, %(price_precision)s, %(qty_precision)s,
        %(is_trading)s, %(raw_json)s, %(updated_at)s
    )
    ON CONFLICT (exchange_id, symbol_id)
    DO UPDATE SET
        min_qty = EXCLUDED.min_qty,
        step_size = EXCLUDED.step_size,
        min_notional = EXCLUDED.min_notional,
        max_qty = EXCLUDED.max_qty,
        tick_size = EXCLUDED.tick_size,
        price_precision = EXCLUDED.price_precision,
        qty_precision = EXCLUDED.qty_precision,
        is_trading = EXCLUDED.is_trading,
        raw_json = EXCLUDED.raw_json,
        updated_at = EXCLUDED.updated_at;
    """

    with self.conn.cursor() as cur:
        cur.executemany(sql, rows)

    self.conn.commit()
    return len(rows)
