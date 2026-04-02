# Averaging levels patch summary

## src/platform/traders/liquidation/filters.py

```python
def _cfg_averaging_levels_method(self) -> str:
        """Read averaging levels method with backward-compatible keys.

        Supported values:
          - pivot
          - combined (pivot + volume confirmation)
        Unknown values fall back to ``pivot``.
        """
        try:
            raw = None
            for key in ("averaging_levels_method", "levels_method"):
                try:
                    raw = getattr(self.p, key, None)
                except Exception:
                    raw = None
                if raw not in (None, ""):
                    break
            method = str(raw or "pivot").strip().lower()
            if method not in {"pivot", "combined"}:
                return "pivot"
            return method
        except Exception:
            return "pivot"

    def _load_averaging_candles(self, symbol_id: int, timeframe: str) -> list[dict]:
        try:
            lv_tf = str(getattr(self.p, "averaging_levels_tf", timeframe) or timeframe)
            lookback_h = int(getattr(self.p, "averaging_levels_lookback_hours", 168) or 168)
            q = """
            SELECT open_time, high, low, close, volume
            FROM candles
            WHERE exchange_id=%(ex)s AND symbol_id=%(sym)s AND interval=%(tf)s
              AND open_time >= (NOW() AT TIME ZONE 'UTC') - (%(h)s || ' hours')::interval
            ORDER BY open_time ASC;
            """
            return list(self.store.query_dict(q, {"ex": int(self.exchange_id), "sym": int(symbol_id), "tf": lv_tf, "h": int(lookback_h)}))
        except Exception:
            return []

    def _extract_pivot_levels(self, rows: list[dict]) -> tuple[list[float], list[float]]:
        try:
            left = int(getattr(self.p, "averaging_pivot_left", 3) or 3)
            right = int(getattr(self.p, "averaging_pivot_right", 3) or 3)
            lows = [float((r or {}).get("low") or 0.0) for r in rows]
            highs = [float((r or {}).get("high") or 0.0) for r in rows]
            if len(lows) < (left + right + 5):
                return [], []
            piv_lows: list[float] = []
            piv_highs: list[float] = []
            for i in range(left, len(lows) - right):
                w_low = lows[i-left:i+right+1]
                cur_low = lows[i]
                if cur_low > 0 and cur_low == min(w_low):
                    piv_lows.append(cur_low)
                w_high = highs[i-left:i+right+1]
                cur_high = highs[i]
                if cur_high > 0 and cur_high == max(w_high):
                    piv_highs.append(cur_high)
            return piv_lows, p
```

```python
def _pick_averaging_level(
        self,
        *,
        symbol_id: int,
        side: str,
        ref_price: float,
        timeframe: str,
        level_index: int = 1,
    ) -> Optional[float]:
        """Pick averaging level according to configured method.

        Methods:
          - pivot: nearest qualifying pivot level
          - combined: nearest pivot with nearby volume-profile confirmation
        Returns ``None`` when no suitable level is found; caller should apply fallback.
        """
        try:
            side_u = str(side or "").upper()
            if ref_price <= 0 or side_u not in {"LONG", "SHORT"}:
                return None
            rows = self._load_averaging_candles(symbol_id, timeframe)
            if not rows:
                return None
            piv_lows, piv_highs = self._extract_pivot_levels(rows)
            method = self._cfg_averaging_levels_method()
            min_dist_pct = float(self._cfg_averaging_min_level_distance_pct())
            dist_limit_pct = max(0.01, float(min_dist_pct))
            level_index = max(1, int(level_index or 1))

            if side_u == "LONG":
                candidates = sorted({x for x in piv_lows if x > 0 and x < ref_price * (1.0 - dist_limit_pct/100.0)}, reverse=True)
            else:
                candidates = sorted({x for x in piv_highs if x > ref_price * (1.0 + dist_limit_pct/100.0)})

            if not candidates:
                return None

            if method == "pivot":
                return float(candidates[level_index - 1]) if len(candidates) >= level_index else None

            # combined = pivot + volume confirmation
            tol_pct = float(getattr(self.p, "averaging_level_tolerance_pct", 0.15) or 0.15)
            cluster_tol_pct = float(getattr(self.p, "averaging_cluster_tolerance_pct", 0.25) or 0.25)
            confirm_pct = max(tol_pct, cluster_tol_pct)
            vp_nodes = self._extract_volume_profile_nodes(rows)
            if not vp_nodes:
                return None

            confirmed: list[float] = []
            for lvl in candidates:
                if any(abs(lvl - node) / max(abs(lvl), 1e-9) * 100.0 <= confirm_pct for node in vp_nodes):
                    confirmed.append(float(lvl))
            return float(confirmed[level_index - 1]) if len(confirmed) >= level_index else None
        except Exception:
            return None

    def _parse_add_n(self, client_id: str) -> int:
        """Extract ADD number from client_order_id (.._ADD1/.._ADD2..)."""
        try:
            m = _ADD_RE.search(str(client_id or ""))
            i
```

## src/platform/traders/liquidation/candidate_finder.py

```python
def _compute_significant_level(self, symbol_id: int, *, side: str, entry_ref: float, timeframe: str) -> float:
        """Compute a significant support/resistance level for averaging.

        Uses the configured averaging method:
          - pivot: nearest qualifying pivot level
          - combined: nearest pivot with nearby volume-profile confirmation

        Returns 0.0 if cannot compute.
        """
        try:
            if entry_ref <= 0:
                return 0.0
            min_dist_pct = float(self._cfg_averaging_min_level_distance_pct())
            dist_limit_pct = max(0.01, float(min_dist_pct))
            level = self._pick_averaging_level(
                symbol_id=int(symbol_id),
                side=str(side or ""),
                ref_price=float(entry_ref),
                timeframe=str(timeframe or getattr(self.p, "averaging_levels_tf", "4h") or "4h"),
                level_index=1,
            )
            if level and level > 0:
                return float(level)

            side_u = str(side or "").upper()
            if side_u == "LONG":
                return float(entry_ref) * (1.0 - (dist_limit_pct / 100.0))
            return float(entry_ref) * (1.0 + (dist_limit_pct / 100.0))
        except Exception:
            return 0.0


```

## src/platform/traders/liquidation/entry_rules.py

```python
if mult > 1.0:
                    min_dist_pct = float(self._cfg_averaging_min_level_distance_pct())
                    dist_limit_pct = max(0.01, float(min_dist_pct))
                    entry_ref = float(avg_price or entry_estimate or 0)
                    level = 0.0
                    if entry_ref > 0:
                        level = float(self._pick_averaging_level(
                            symbol_id=int(symbol_id),
                            side=str(ledger_side or ""),
                            ref_price=float(entry_ref),
                            timeframe=str(timeframe or getattr(self.p, "averaging_levels_tf", "4h") or "4h"),
                            level_index=1,
                        ) or 0.0)
                        if level <= 0:
                            if ledger_side == "LONG":
                                level = entry_ref * (1.0 - (dist_limit_pct / 100.0))
                            else:
                                level = entry_ref * (1.0 + (dist_limit_pct / 100.0))

                    if level > 0:
                        # compute add qty via multiplier: new_qty = cur_qty * mult => add = cur*(mult-1)
                        add_qty = float(qty) * (mult - 1.0)
                        add_qty = _round_qty_to_step(add_qty, qty_step, mode="down")
                        if add_qty >= float(qty_step):
                            cid_add = f"{prefix}_{tok}_ADD1"
                            # Use TAKE_PROFIT_MARKET as conditional entry: BUY triggers when mark <= stopPrice; SELL triggers when mark >= stopPrice
                            add_side = entry_side
                            add_type = "TAKE_PROFIT_MARKET"
                            stop_px = _round_price_to_tick(level, float(tick_size or 0.0), mode="down" if add_s
```

## src/platform/traders/liquidation/order_builder.py

```python
picked: Optional[float] = None
        try:
            sid = int(pos.get("symbol_id") or 0)
            if sid > 0:
                picked = self._pick_averaging_level(
                    symbol_id=sid,
                    side=str(side or ""),
                    ref_price=float(ref_price),
                    timeframe=str(getattr(self.p, "averaging_levels_tf", pos.get("timeframe") or "4h") or pos.get("timeframe") or "4h"),
                    level_index=int(next_n),
                )
        except Exception:
            picked = None

        if picked is not None and picked > 0:
            level = _dec(str(picked))
        else:
            # fallback spacing from reference
            pct = _dec(str(min_dist_pct)) / _dec("100")
            if pct <= 0:
                pct = _dec("0.10")
            if side == "LONG":
                level = ref_price * (_dec("1") - pct * _dec(str(next_n)))
            else:
                level = ref_price * (_dec("1") + pct * _dec(str(next_
```
