# real_time_updater.py (updated with per-timeframe sleep intervals)
import time
import pandas as pd
from datetime import datetime, timedelta, timezone
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def real_time_updater(collector, mongo_handler, crypto_tests, default_sleep_interval: int = 60, sleep_intervals: dict = None) -> None:
    """
    Continuously fetches the latest OHLCV data for each crypto test case and upserts
    it into the specific collection for that exchange/symbol/timeframe.
    
    The sleep interval for each test is determined by the timeframe via a provided dictionary.
    If a timeframe is not specified in the dictionary, a default sleep interval is used.
    
    :param collector: Instance of your MarketDataCollector.
    :param mongo_handler: Instance of your MongoDBHandler.
    :param crypto_tests: List of test cases (each a dict with keys: "exchange", "symbol", "timeframe").
    :param default_sleep_interval: Default sleep interval (in seconds) if none is provided for a timeframe.
    :param sleep_intervals: Dictionary mapping timeframe strings to sleep intervals in seconds.
    """
    # Define default sleep intervals per timeframe if not provided.
    if sleep_intervals is None:
        sleep_intervals = {
            "1m": 30,   
            "3m": 30,   
            "5m": 60,   
            "15m": 120,  
            "30m": 180,  
            "1h": 300,
            "2h": 600,
            "4h": 900,
            "6h": 900,
            "8h": 1200,
            "12h": 1800,
            "1d": 3600,
            "3d": 3600,
            "1w": 7200
        }
    
    while True:
        now = datetime.now(timezone.utc)
        for test in crypto_tests:
            exchange = test["exchange"]
            symbol = test["symbol"]
            timeframe = test["timeframe"]
            try:
                # Get the latest stored candle timestamp for this test case.
                latest = mongo_handler.get_latest_timestamp(exchange, symbol, timeframe)
                # Determine the expected candle duration (in seconds) from the exchange.
                exchange_inst = collector.crypto.check_exchange(exchange)
                duration_sec = exchange_inst.parse_timeframe(timeframe)
                
                if latest is None:
                    # If no data exists, fetch an initial candle.
                    df = collector.crypto.fetch_by_limit(
                        exchange_name=exchange,
                        symbol=symbol,
                        limit=1,
                        timeframe=timeframe
                    )
                else:
                    gap_sec = (now - latest).total_seconds()
                    if gap_sec > duration_sec:
                        logger.info(f"Gap ({gap_sec:.2f} sec) for {symbol} on {exchange} ({timeframe}) exceeds interval ({duration_sec} sec). Fetching missing candles.")
                        # Fetch all candles between latest+1ms and now.
                        new_since = latest + timedelta(milliseconds=1)
                        df = collector.crypto.fetch_by_date(
                            exchange_name=exchange,
                            symbol=symbol,
                            timeframe=timeframe,
                            since=new_since,
                            until=now
                        )
                    else:
                        logger.info(f"Gap ({gap_sec:.2f} sec) for {symbol} on {exchange} ({timeframe}) is within interval ({duration_sec} sec). Fetching latest candle.")
                        df = collector.crypto.fetch_by_limit(
                            exchange_name=exchange,
                            symbol=symbol,
                            limit=1,
                            timeframe=timeframe
                        )
                # Convert fetched data to dictionary and ensure timestamps are datetime objects.
                records = df.to_dict("records")
                for r in records:
                    if not isinstance(r["timestamp"], datetime):
                        r["timestamp"] = pd.to_datetime(r["timestamp"]).to_pydatetime()
                # Upsert the new data into the corresponding collection.
                mongo_handler.upsert_ohlcv(records, exchange, symbol, timeframe)
                logger.info(f"Real-time update: Upserted {len(records)} new candle(s) for {symbol} on {exchange} into collection {mongo_handler._collection_name(exchange, symbol, timeframe)}.")
            except Exception as e:
                logger.error(f"Real-time update error for {symbol} on {exchange} ({timeframe}): {e}")
            
            # Sleep for this test's specific sleep interval.
            interval_sleep = sleep_intervals.get(timeframe, default_sleep_interval)
            logger.info(f"Sleeping for {interval_sleep} seconds for {symbol} on {exchange} ({timeframe}).")
            time.sleep(interval_sleep)
        # Optionally, you might add a global sleep here if desired.
