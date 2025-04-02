# 🕯️ CandleCollector

**CandleCollector** is a key component of a larger AlgoTrading system I'm building in Python. I decided to publish this standalone module on GitHub as it solves a fundamental challenge in algorithmic trading: **automated and efficient collection of OHLCV (candlestick) data** from various crypto exchanges.

> Why? Because exchanges like Binance, KuCoin, and Coinbase impose strict request limits. Without a robust data layer, frequent API calls can lead to throttling or bans.

---

## Why CandleCollector?

In an automated trading pipeline, price data is everything — but pulling fresh candles on-the-fly isn't scalable. This tool tackles that by:

- Fetching historical and real-time candlestick data from popular crypto exchanges
- Allowing users to **store the data locally or in a database (e.g. MongoDB)**
- Continuously updating historical records to **avoid redundant external requests**

---

## 🛠 Features

- Fetch OHLCV data by:
- Fixed limit (e.g. last 100 candles)
- Date range (`since` → `until`)
- Supports major exchanges: `binance`, `kucoin`, `coinbase`
- Multiple timeframes: `4h`, `8h`, `12h`, `1d`, `1w`, `1month`
- Modular collectors: supports Crypto and Forex (via API key)
- Retry system with rate limit awareness
- MongoDB export capability (planned/branchable)
- CSV export capability
- Extendable and production-ready

---

## Setup

```bash
git clone https://github.com/bettercallsalar/CandleCollector.git
cd CandleCollector
pip install -r requirements.txt
python main.py
```

BetterCallSalar
