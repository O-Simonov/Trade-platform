# 🚀 Trade Platform

Алготрейдинг-платформа для работы с Binance Futures:
- сбор маркет-данных (candles, OI, funding)
- торговые стратегии (liquidation trader)
- риск-менеджмент
- хранение в PostgreSQL

---

## ⚙️ Возможности

### 📊 Market Data
- Candles (klines)
- Open Interest
- Ticker (price/volume)
- Funding rates (с backfill)
- Retention cleanup

### 🤖 Trading
- Liquidation strategy
- Entry / Exit rules
- Hedge logic
- Risk adapter
- Paper / Live режим

### 🗄 Storage
- PostgreSQL
- Репозитории:
  - orders
  - fills
  - positions
  - signals
  - market_data
  - analytics

---

## 🧱 Архитектура

```
src/platform/
│
├── traders/
│   └── liquidation/
│       ├── params.py
│       ├── entry_rules.py
│       ├── exit_rules.py
│       ├── hedge_logic.py
│       ├── order_builder.py
│       ├── recovery.py
│       └── runner.py
│
├── exchanges/binance/
│   ├── rest.py
│   ├── collector_candles.py
│   ├── collector_oi.py
│   └── collector_funding.py
│
└── data/storage/postgres/repositories/
```

---

## 🚀 Быстрый старт

### 1. Установка

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

### 2. Настройка `.env`

```
BINANCE_API_KEY=your_key
BINANCE_API_SECRET=your_secret
POSTGRES_DSN=postgresql://user:pass@localhost:5432/db
LOG_LEVEL=INFO
```

---

### 3. Запуск Market Data

```bash
python -m src.platform.run_market_data
```

---

### 4. Запуск трейдера

```bash
python -m src.platform.run_trade_liquidation
```

---

## ⚠️ Особенности Binance

- возможны 403 ошибки
- HTML ответы вместо JSON

Решение:
- fail-fast в rest.py
- cooldown в collector_funding.py

---

## 🛠 Конфигурация

```yaml
funding:
  enabled: true
  backfill_on_start: true
  backfill_days: 3
  limit: 500
  batch: 3
```

---

## 🧪 Режимы

| Режим | Описание |
|------|--------|
| paper | симуляция |
| live  | реальные ордера |

---

## 📈 Roadmap

- Adaptive rate limiting
- WebSocket market data
- Strategy plugins
- Backtesting engine

---

## 🤝 Contributing

1. Fork
2. Создать ветку
3. Commit
4. PR

---

## 📜 License

MIT

---

## 👨‍💻 Автор

Oleg
