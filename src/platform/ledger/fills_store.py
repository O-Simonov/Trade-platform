class FillsStore:
    def __init__(self, conn):
        self.conn = conn

    def upsert(self, f):
        sql = """
        INSERT INTO trade_fills (
          trade_id, exchange, symbol, order_id, side, position_side,
          price, qty, quote_qty, commission, commission_asset,
          realized_pnl, is_maker, executed_at, strategy_name, pos_uid
        )
        VALUES (%(trade_id)s,%(exchange)s,%(symbol)s,%(order_id)s,%(side)s,%(position_side)s,
                %(price)s,%(qty)s,%(quote_qty)s,%(commission)s,%(commission_asset)s,
                %(realized_pnl)s,%(is_maker)s,%(executed_at)s,%(strategy_name)s,%(pos_uid)s)
        ON CONFLICT (trade_id) DO NOTHING;
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, f.__dict__)
        self.conn.commit()
