# main.py
from logging_config import ColoredFormatter
ColoredFormatter.setup_logging(level="INFO")  # Initialize logging

from datetime import datetime, timezone
from market_data_collector import MarketDataCollector
from mongo_storage import MongoDBHandler
from price_data_updater import PriceDataUpdater

def prompt_user(prompt_text, default_value=None):
    inp = input(prompt_text)
    if not inp and default_value is not None:
        return default_value
    return inp

def main():
    print("=== AlgoTrade Updater Configuration ===")
    
    # Ask for exchange (default: binance)
    exchange = prompt_user("Enter exchange (default: binance): ", "binance")
    
    # Ask for symbol (default: BTC/USDT)
    symbol = prompt_user("Enter symbol (default: BTC/USDT): ", "BTC/USDT")
    
    # Ask for timeframes (comma-separated). Defaults below include a wide range.
    default_timeframes = "1w,3d,1d,12h,8h,6h,4h,2h,1h,30m,15m,5m,3m,1m"
    tf_input = prompt_user(f"Enter timeframes as comma-separated values (default: {default_timeframes}): ", default_timeframes)
    timeframes = [tf.strip() for tf in tf_input.split(",") if tf.strip()]
    
    # Construct crypto_tests list (one dict per timeframe)
    crypto_tests = [{"exchange": exchange, "symbol": symbol, "timeframe": tf} for tf in timeframes]
    
    # Ask for MongoDB connection URI.
    default_mongo_uri = "mongodb://localhost:27017/"
    mongo_uri = prompt_user(f"Enter MongoDB connection URI (default: {default_mongo_uri}): ", default_mongo_uri)
    
    # Define historical period (you could also ask user for a start date, but we'll use a default here)
    historical_periods = [
        ("all_years", datetime(2016, 3, 25, tzinfo=timezone.utc), datetime.now(timezone.utc))
    ]
    
    print("\nConfiguration:")
    print(f"Exchange: {exchange}")
    print(f"Symbol: {symbol}")
    print(f"Timeframes: {timeframes}")
    print(f"MongoDB URI: {mongo_uri}\n")
    
    # Initialize your MarketDataCollector (assuming your code sets up crypto/forex collectors)
    collector = MarketDataCollector(api_key='064de06f729b4dfab083cecede14b530')
    
    # Initialize MongoDB handler with provided URI
    mongo_handler = MongoDBHandler(uri=mongo_uri, db_name=exchange+"_"+symbol)
    
    # Initialize and run the PriceDataUpdater
    updater = PriceDataUpdater(collector, mongo_handler, crypto_tests, historical_periods, real_time_sleep=60)
    updater.run()

if __name__ == "__main__":
    main()
