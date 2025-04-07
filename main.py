#!/usr/bin/env python3
import time
import threading
from datetime import datetime, timezone

from data.market_data_collector import MarketDataCollector
from db.mongo_storage import MongoDBHandler

# Initialize your MarketDataCollector (for crypto, forex key is not used here)
collector = MarketDataCollector(api_key=None)

# Initialize the MongoDB handler.
# The database name is created dynamically, e.g., "binance_BTC_USDT" (with "/" replaced by "_").
exchange = 'binance'
symbol = 'BTC/USDT'
db_name = exchange + "_" + symbol.replace("/", "_")
mongo_handler = MongoDBHandler(uri="mongodb://localhost:27017/", db_name=db_name)

# Define the list of timeframes in descending order (largest first)
timeframes = ['1M', '1w', '1d', '12h', '8h', '4h', '1h', '30m', '15m', '5m']

# Define the historical date range for data fetching.
# Ensure timezone-aware datetime objects.
since = datetime(2017, 1, 1, tzinfo=timezone.utc)
until = datetime.now(timezone.utc)

def update_timeframe(tf):
    try:
        print(f"Fetching candles for timeframe {tf}...")
        df = collector.crypto.fetch_by_date(
            exchange_name=exchange,
            symbol=symbol,
            timeframe=tf,
            since=since,
            until=until
        )
        num_candles = len(df)
        print(f"Fetched {num_candles} candles for timeframe {tf}.")
        records = df.to_dict("records")
        mongo_handler.upsert_ohlcv(records, exchange, symbol, tf)
        print(f"Upserted {len(records)} candles for timeframe {tf}.")
    except Exception as e:
        print(f"Error fetching or upserting data for timeframe {tf}: {e}")

def update_all_timeframes():
    threads = []
    for tf in timeframes:
        thread = threading.Thread(target=update_timeframe, args=(tf,))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    while True:
        current_time = datetime.now(timezone.utc).isoformat()
        print(f"\nStarting data update cycle at {current_time}")
        update_all_timeframes()
        print("Cycle complete. Waiting 60 seconds before next cycle...")
        time.sleep(60)
