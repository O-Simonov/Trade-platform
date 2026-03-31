# Команды для оптимизации pump-скринера

Ниже уже подготовлены готовые пресеты сеток под `5m`, `15m` и `1h`.

## Пресеты

- `balanced` — основной боевой пресет для первичного поиска
- `wide` — широкий поиск, когда ещё нет хорошей области параметров
- `narrow` — узкий дожим вокруг уже найденных хороших значений
- `conservative` — поиск параметров вокруг консервативного pump-профиля
- `manual` — старый режим, где сетка задаётся флагами CLI вручную

## Быстрый старт

```bash
export BACKTEST_DSN='postgresql+psycopg2://USER:PASSWORD@HOST:5432/DBNAME'
export BACKTEST_POOL_DSN='dbname=DBNAME user=USER password=PASSWORD host=HOST port=5432'
```

## 1) Основной запуск для 5m

```bash
python -m src.platform.backtester.optimize_pump_screener_history \
  --yaml config/screener_pump.yaml \
  --start 2026-02-15T00:00:00+00:00 \
  --end   2026-03-15T00:00:00+00:00 \
  --interval 5m \
  --preset balanced \
  --min-trades 25 \
  --top-n 25 \
  --progress-every 5 \
  --csv-out artifacts/opt/pump_5m_balanced.csv
```

## 2) Основной запуск для 15m

```bash
python -m src.platform.backtester.optimize_pump_screener_history \
  --yaml config/screener_pump.yaml \
  --start 2026-01-15T00:00:00+00:00 \
  --end   2026-03-15T00:00:00+00:00 \
  --interval 15m \
  --preset balanced \
  --min-trades 18 \
  --top-n 25 \
  --progress-every 5 \
  --csv-out artifacts/opt/pump_15m_balanced.csv
```

## 3) Основной запуск для 1h

```bash
python -m src.platform.backtester.optimize_pump_screener_history \
  --yaml config/screener_pump.yaml \
  --start 2025-12-01T00:00:00+00:00 \
  --end   2026-03-15T00:00:00+00:00 \
  --interval 1h \
  --preset balanced \
  --min-trades 10 \
  --top-n 25 \
  --progress-every 5 \
  --csv-out artifacts/opt/pump_1h_balanced.csv
```

## 4) Консервативный запуск

```bash
python -m src.platform.backtester.optimize_pump_screener_history \
  --yaml config/screener_pump_conservative.yaml \
  --start 2026-01-15T00:00:00+00:00 \
  --end   2026-03-15T00:00:00+00:00 \
  --interval 15m \
  --preset conservative \
  --min-trades 12 \
  --top-n 20 \
  --progress-every 5 \
  --csv-out artifacts/opt/pump_15m_conservative.csv
```

## 5) Узкий дожим после первого прогона

```bash
python -m src.platform.backtester.optimize_pump_screener_history \
  --yaml config/screener_pump.yaml \
  --start 2026-01-15T00:00:00+00:00 \
  --end   2026-03-15T00:00:00+00:00 \
  --interval 15m \
  --preset narrow \
  --min-trades 18 \
  --top-n 20 \
  --progress-every 5 \
  --csv-out artifacts/opt/pump_15m_narrow.csv
```

## 6) Широкий поиск, когда рынок сильно изменился

```bash
python -m src.platform.backtester.optimize_pump_screener_history \
  --yaml config/screener_pump.yaml \
  --start 2026-02-01T00:00:00+00:00 \
  --end   2026-03-15T00:00:00+00:00 \
  --interval 5m \
  --preset wide \
  --min-trades 20 \
  --top-n 20 \
  --progress-every 5 \
  --csv-out artifacts/opt/pump_5m_wide.csv
```

## Практика запуска

1. Сначала `balanced`.
2. Потом `narrow` на лучшем таймфрейме.
3. Для строгого профиля отдельно прогонять `conservative` вместе с `config/screener_pump_conservative.yaml`.
4. `wide` использовать только когда надо заново найти рабочую область, потому что он тяжелее.

## Важно

Количество комбинаций у пресетов большое. `wide` особенно тяжёлый. Для первого запуска лучше ограничивать период и при необходимости использовать `--max-points`.
