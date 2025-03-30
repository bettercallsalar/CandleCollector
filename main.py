# main.py
import os
import sys
from logging_config import ColoredFormatter
ColoredFormatter.setup_logging(level="INFO")  # Initialize logging

from datetime import datetime, timezone
from market_data_collector import MarketDataCollector
from mongo_storage import MongoDBHandler
from price_data_updater import PriceDataUpdater

def prompt_user(prompt_text, default_value=None):
    # Only prompt if sys.stdin is a tty
    if sys.stdin.isatty():
        inp = input(prompt_text)
        if not inp and default_value is not None:
            return default_value
        return inp
    return default_value

def main():
    # Get configuration values from environment variables, or prompt if not set.
    exchange = os.environ.get("EXCHANGE") or prompt_user("Enter exchange (default: binance): ", "binance")
    symbol = os.environ.get("SYMBOL") or prompt_user("Enter symbol (default: BTC/USDT): ", "BTC/USDT")
    timeframes_str = os.environ.get("TIMEFRAMES") or prompt_user("Enter timeframes as comma-separated values (default: 1w,3d,1d,12h,8h,6h,4h,2h,1h,30m,15m,5m,3m,1m): ", "1w,3d,1d,12h,8h,6h,4h,2h,1h,30m,15m,5m,3m,1m")
    mongo_uri = os.environ.get("MONGODB_URI") or prompt_user("Enter MongoDB connection URI (default: mongodb://localhost:27017/): ", "mongodb://localhost:27017/")

    # Parse the timeframes into a list.
    timeframes = [tf.strip() for tf in timeframes_str.split(",") if tf.strip()]

    # Build crypto_tests: one dict per timeframe.
    crypto_tests = [{"exchange": exchange, "symbol": symbol, "timeframe": tf} for tf in timeframes]

    historical_periods = [
        ("all_years", datetime(2010, 1, 1, tzinfo=timezone.utc), datetime.now(timezone.utc))
    ]

    print("\nConfiguration:")
    print(f"Exchange: {exchange}")
    print(f"Symbol: {symbol}")
    print(f"Timeframes: {timeframes}")
    print(f"MongoDB URI: {mongo_uri}\n")

    # Initialize your MarketDataCollector (assuming your code sets up crypto/forex collectors).
    collector = MarketDataCollector()

    # Create a dynamic database name based on exchange and symbol.
    # Replace "/" with "_" in symbol to avoid issues.
    db_name = exchange + "_" + symbol.replace("/", "_")
    mongo_handler = MongoDBHandler(uri=mongo_uri, db_name=db_name)

    # Initialize and run the PriceDataUpdater.
    updater = PriceDataUpdater(collector, mongo_handler, crypto_tests, historical_periods, real_time_sleep=60)
    updater.run()

if __name__ == "__main__":
    main()
