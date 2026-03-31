# Команды для оптимизации OI-скринера

Ниже команды даны для Windows PowerShell и Linux/macOS shell.

## 1) Переменные окружения

Нужно задать 2 строки подключения:

- `BACKTEST_DSN` — для SQLAlchemy / data feed
- `BACKTEST_POOL_DSN` — для psycopg pool / storage

### PowerShell

```powershell
$env:BACKTEST_DSN="postgresql+psycopg2://USER:PASSWORD@HOST:5432/DBNAME"
$env:BACKTEST_POOL_DSN="dbname=DBNAME user=USER password=PASSWORD host=HOST port=5432"
```

### Linux/macOS

```bash
export BACKTEST_DSN='postgresql+psycopg2://USER:PASSWORD@HOST:5432/DBNAME'
export BACKTEST_POOL_DSN='dbname=DBNAME user=USER password=PASSWORD host=HOST port=5432'
```

---

## 2) Быстрый прогон по 5m

```bash
python -m src.platform.backtester.optimize_oi_screener_history \
  --yaml config/screener_oi.yaml \
  --start 2026-02-01T00:00:00+00:00 \
  --end   2026-03-01T00:00:00+00:00 \
  --interval 5m \
  --exchange-id 1 \
  --min-trades 20 \
  --top-n 20 \
  --progress-every 5 \
  --csv-out artifacts/opt/oi_5m_top.csv
```

---

## 3) Сбалансированная оптимизация для 5m

```bash
python -m src.platform.backtester.optimize_oi_screener_history \
  --yaml config/screener_oi.yaml \
  --start 2026-02-01T00:00:00+00:00 \
  --end   2026-03-01T00:00:00+00:00 \
  --interval 5m \
  --exchange-id 1 \
  --min-trades 25 \
  --top-n 30 \
  --progress-every 5 \
  --buy-oi-rise-pct "2.0,2.5,3.0" \
  --oi-value-confirm-pct "1.5,2.0,2.5" \
  --buy-volume-mult "1.8,2.2,2.6" \
  --buy-volume-spike-mult "3.0,4.0,5.0" \
  --buy-price-rise-pct "1.0,1.5,2.0" \
  --buy-min-green-candles "2,3" \
  --price-sideways-max-pct "4.0,6.0,8.0" \
  --price-downtrend-min-pct "0.3,0.5,0.8" \
  --base-volume-max-mult "3.0,4.0,5.0" \
  --min-base-quote-volume "150000,250000,400000" \
  --min-oi-value "500000,1500000,2500000" \
  --buy-min-oi-last-rise-pct "0.8,1.2,1.5" \
  --max-oi-drawdown-from-peak-pct "0.8,1.0,1.2" \
  --min-signal-score "50,55,60" \
  --use-atr-normalization "true" \
  --atr-period "14" \
  --buy-price-rise-atr-mult "0.7,0.8,0.9" \
  --buy-price-max-rise-atr-mult "1.8,2.2,2.8" \
  --stop-loss-pct "1.5,2.0,2.5" \
  --take-profit-pct "4.0,5.0,6.0" \
  --buy-require-last-green "true" \
  --enable-funding "true" \
  --buy-require-funding-decreasing "true,false" \
  --buy-funding-max-pct "-0.02,-0.01,-0.001,-0.0001" \
  --csv-out artifacts/opt/oi_5m_balanced.csv
```

---

## 4) Оптимизация для 15m

```bash
python -m src.platform.backtester.optimize_oi_screener_history \
  --yaml config/screener_oi.yaml \
  --start 2026-01-15T00:00:00+00:00 \
  --end   2026-03-15T00:00:00+00:00 \
  --interval 15m \
  --exchange-id 1 \
  --min-trades 15 \
  --top-n 25 \
  --progress-every 5 \
  --buy-oi-rise-pct "2.0,2.5,3.0" \
  --oi-value-confirm-pct "1.5,2.0,2.5" \
  --buy-volume-mult "1.8,2.2,2.6" \
  --buy-volume-spike-mult "3.0,4.0,5.0" \
  --min-base-quote-volume "250000,400000,600000" \
  --min-oi-value "1500000,2500000,4000000" \
  --buy-min-oi-last-rise-pct "1.2,1.5,1.8" \
  --max-oi-drawdown-from-peak-pct "0.8,1.0,1.2" \
  --min-signal-score "55,58,62" \
  --buy-price-rise-atr-mult "0.7,0.8,0.9" \
  --buy-price-max-rise-atr-mult "2.0,2.2,2.5" \
  --csv-out artifacts/opt/oi_15m_balanced.csv
```

---

## 5) Оптимизация для 1h

```bash
python -m src.platform.backtester.optimize_oi_screener_history \
  --yaml config/screener_oi.yaml \
  --start 2025-12-01T00:00:00+00:00 \
  --end   2026-03-15T00:00:00+00:00 \
  --interval 1h \
  --exchange-id 1 \
  --min-trades 10 \
  --top-n 25 \
  --progress-every 5 \
  --buy-oi-rise-pct "2.0,2.5,3.0,3.5" \
  --oi-value-confirm-pct "1.5,2.0,2.5,3.0" \
  --buy-volume-mult "1.8,2.2,2.6" \
  --buy-volume-spike-mult "3.0,4.0,5.0" \
  --min-base-quote-volume "400000,800000,1200000" \
  --min-oi-value "2500000,5000000,8000000" \
  --buy-min-oi-last-rise-pct "1.5,2.0,2.5" \
  --max-oi-drawdown-from-peak-pct "1.0,1.2,1.5" \
  --min-signal-score "55,60,65" \
  --buy-price-rise-atr-mult "0.8,0.9,1.0" \
  --buy-price-max-rise-atr-mult "2.2,2.8,3.2" \
  --csv-out artifacts/opt/oi_1h_balanced.csv
```

---

## 6) Узкий быстрый search вокруг уже хороших значений

Когда найдёшь первый рабочий диапазон, переходи на узкую сетку.

```bash
python -m src.platform.backtester.optimize_oi_screener_history \
  --yaml config/screener_oi.yaml \
  --start 2026-02-01T00:00:00+00:00 \
  --end   2026-03-01T00:00:00+00:00 \
  --interval 15m \
  --exchange-id 1 \
  --min-trades 20 \
  --top-n 20 \
  --progress-every 5 \
  --buy-oi-rise-pct "2.2,2.4,2.6" \
  --oi-value-confirm-pct "1.8,2.0,2.2" \
  --buy-volume-mult "2.0,2.2,2.4" \
  --min-base-quote-volume "300000,400000,500000" \
  --min-oi-value "2000000,2500000,3000000" \
  --buy-min-oi-last-rise-pct "1.3,1.5,1.7" \
  --max-oi-drawdown-from-peak-pct "0.9,1.0,1.1" \
  --min-signal-score "56,58,60" \
  --buy-price-rise-atr-mult "0.75,0.8,0.85" \
  --buy-price-max-rise-atr-mult "2.0,2.2,2.4" \
  --csv-out artifacts/opt/oi_15m_narrow.csv
```

---

## 7) Что смотреть в результатах

Сначала сортируй не только по `net_pnl`, а по комбинации:

- `profit_factor`
- `max_drawdown`
- `win_rate`
- `total_trades`
- `net_pnl`

Практически хорошие кандидаты обычно такие:

- есть не слишком мало сделок
- drawdown не выбивается
- profit factor стабильно > 1
- top-результаты повторяются на соседних диапазонах параметров, а не в одной "магической" точке

---

## 8) Важно

Оптимизатор после патча теперь делает настоящий replay по историческим timestamp внутри диапазона и передаёт `replay_as_of_ts` в скринер. Это намного честнее, но и заметно тяжелее по времени, чем старый вариант.
