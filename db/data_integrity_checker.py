# data_integrity_checker.py
import logging
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class DataIntegrityChecker:
    def __init__(self, collector, mongo_handler, tolerance_sec: int = 5):
        """
        :param collector: Instance of MarketDataCollector.
        :param mongo_handler: Instance of MongoDBHandler.
        :param tolerance_sec: Tolerance (in seconds) to allow slight timing variations.
        """
        self.collector = collector
        self.mongo_handler = mongo_handler
        self.tolerance_sec = tolerance_sec

    def check_and_fetch_missing(self, exchange: str, symbol: str, timeframe: str, 
                                  period_start: datetime, period_end: datetime) -> None:
        """
        Checks for missing candles in the given period for the specified exchange, symbol, and timeframe.
        If gaps are detected (i.e. a gap larger than the expected candle duration plus a tolerance),
        it fetches the missing candles and upserts them into MongoDB.
        
        :param exchange: Exchange name (e.g. "binance")
        :param symbol: Trading pair symbol (e.g. "BTC/USDT")
        :param timeframe: Candle timeframe (e.g. "1m", "5m", "1h", etc.)
        :param period_start: Start datetime of the period to check (timezone-aware)
        :param period_end: End datetime of the period to check (timezone-aware)
        """
        # Get the collection data for this pair/timeframe.
        coll = self.mongo_handler.get_collection(exchange, symbol, timeframe)
        stored_docs = list(coll.find({"timestamp": {"$gte": period_start, "$lte": period_end}}).sort("timestamp", 1))
        
        # Extract stored timestamps
        stored_times: List[datetime] = []
        for doc in stored_docs:
            ts = doc["timestamp"]
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            stored_times.append(ts)
        
        # Use the exchange's parse_timeframe to determine expected duration (in seconds)
        exchange_inst = self.collector.crypto.check_exchange(exchange)
        candle_duration = exchange_inst.parse_timeframe(timeframe)
        
        missing_intervals: List[Tuple[datetime, datetime]] = []
        # If no data exists, consider the entire period missing.
        if not stored_times:
            missing_intervals.append((period_start, period_end))
        else:
            # Check for a gap between period_start and the first stored candle.
            first_time = stored_times[0]
            if (first_time - period_start).total_seconds() > candle_duration + self.tolerance_sec:
                missing_intervals.append((period_start, first_time - timedelta(milliseconds=1)))
            # Check gaps between consecutive stored candles.
            for i in range(len(stored_times) - 1):
                current = stored_times[i]
                next_time = stored_times[i + 1]
                if (next_time - current).total_seconds() > candle_duration + self.tolerance_sec:
                    missing_intervals.append((current + timedelta(milliseconds=1), next_time - timedelta(milliseconds=1)))
            # Check the gap between the last stored candle and period_end.
            last_time = stored_times[-1]
            if (period_end - last_time).total_seconds() > candle_duration + self.tolerance_sec:
                missing_intervals.append((last_time + timedelta(milliseconds=1), period_end))
        
        if missing_intervals:
            for interval in missing_intervals:
                start_interval, end_interval = interval
                logger.info(f"Missing candles detected for {symbol} on {exchange} ({timeframe}) from {start_interval} to {end_interval}. Fetching missing data.")
                try:
                    df_missing = self.collector.crypto.fetch_by_date(
                        exchange_name=exchange,
                        symbol=symbol,
                        timeframe=timeframe,
                        since=start_interval,
                        until=end_interval
                    )
                    records = df_missing.to_dict("records")
                    for r in records:
                        if not hasattr(r["timestamp"], "tzinfo") or r["timestamp"].tzinfo is None:
                            r["timestamp"] = pd.to_datetime(r["timestamp"]).to_pydatetime().replace(tzinfo=timezone.utc)
                    self.mongo_handler.upsert_ohlcv(records, exchange, symbol, timeframe)
                    logger.info(f"Upserted {len(records)} missing candles for {symbol} on {exchange} ({timeframe}) from {start_interval} to {end_interval}.")
                except Exception as e:
                    logger.error(f"Error fetching missing candles for {symbol} on {exchange} ({timeframe}) from {start_interval} to {end_interval}: {e}")
        else:
            logger.info(f"No missing candles detected for {symbol} on {exchange} ({timeframe}) between {period_start} and {period_end}.")
