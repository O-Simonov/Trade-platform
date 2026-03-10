-- Verify hedge-mode accounting after migration + restart
\d+ positions
\d+ position_snapshots
\d+ hedge_links

SELECT exchange_id, account_id, symbol, side, status, qty_current, pos_uid, source
FROM position_ledger
WHERE status = 'OPEN'
ORDER BY symbol, side, source, opened_at;

SELECT exchange_id, account_id, symbol_id, side, status, qty, pos_uid, source, updated_at
FROM positions
WHERE status = 'OPEN'
ORDER BY symbol_id, side, updated_at DESC;

SELECT exchange_id, base_account_id, hedge_account_id, symbol_id, base_pos_uid, hedge_pos_uid, hedge_ratio, created_at
FROM hedge_links
ORDER BY created_at DESC;

SELECT p.symbol,
       pl.side AS ledger_side,
       pl.pos_uid AS ledger_pos_uid,
       pl.qty_current AS ledger_qty,
       ps.side AS positions_side,
       ps.pos_uid AS positions_pos_uid,
       ps.qty AS positions_qty,
       ps.source AS positions_source
FROM position_ledger pl
LEFT JOIN positions ps
  ON ps.exchange_id = pl.exchange_id
 AND ps.account_id = pl.account_id
 AND ps.pos_uid = pl.pos_uid
 AND ps.side = pl.side
JOIN symbols p
  ON p.exchange_id = pl.exchange_id
 AND p.symbol_id = pl.symbol_id
WHERE pl.status = 'OPEN'
ORDER BY p.symbol, pl.side, pl.opened_at;
