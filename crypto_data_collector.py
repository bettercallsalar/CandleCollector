# crypto_data_collector.py
import logging
import time
import pandas as pd
import ccxt
from dateutil.parser import parse
from datetime import datetime, timezone

from base_data_collector import BaseDataCollector

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class CryptoDataCollector(BaseDataCollector):
    
    def __init__(self, exchange_names=None):
        super().__init__()  # Call base class initializer if needed
        self.exchanges = {}
        exchange_names = exchange_names or [
            'binance', 'kraken', 'bitstamp', 'bitfinex', 'coinbase', 'poloniex'
        ]
        for name in exchange_names:
            try:
                self.exchanges[name] = getattr(ccxt, name)()
                logger.info(f"Initialized exchange: {name}")
            except Exception as e:
                logger.error(f"Failed to initialize {name}: {e}")

    def safe_fetch_ohlcv(self, exchange: ccxt.Exchange, symbol: str, timeframe: str, since: int, limit: int):
        """
        Fetches OHLCV data using the exchange's API with a retry mechanism.
        This method now uses the safe_retry method from BaseDataCollector.
        
        :param exchange: The exchange instance.
        :param symbol: The trading pair symbol (e.g., 'BTC/USDT').
        :param timeframe: The timeframe for the candles (e.g., '1h').
        :param since: Starting timestamp in milliseconds.
        :param limit: Maximum number of candles to fetch in one call.
        :return: The OHLCV data as returned by the exchange.
        :raises RuntimeError: If maximum retry attempts are exceeded.
        """
        def fetch_func():
            return exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)
        
        return self.safe_retry(fetch_func, max_attempts=3, delay_seconds=3)

    def check_exchange(self, exchange_name:str) -> ccxt.Exchange:
        exchange = self.exchanges.get(exchange_name)
        if not exchange:
            raise ValueError(f"Exchange '{exchange_name}' not initialized.")
        return exchange
    
    def fetch_by_limit(self, exchange_name: str, symbol: str, limit: int, timeframe: str ='1d') -> pd.DataFrame:
        """
        Concrete implementation of the abstract method from BaseDataCollector.
        """
        exchange = self.check_exchange(exchange_name)
        
        all_ohlcv = []
        max_limit = 1000
        since = exchange.milliseconds() - (limit * exchange.parse_timeframe(timeframe) * 1000)

        while len(all_ohlcv) < limit:
            fetch_limit = min(max_limit, limit - len(all_ohlcv))
            ohlcv = self.safe_fetch_ohlcv(exchange, symbol, timeframe, since, fetch_limit)
            if not ohlcv:
                break

            all_ohlcv += ohlcv
            last_timestamp = ohlcv[-1][0]
            logger.info(f"Fetched up to: {pd.to_datetime(last_timestamp, unit='ms')}")
            since = last_timestamp + 1
            # Respect rate limit
            time.sleep(max(exchange.rateLimit / 1000, 1))

            if len(ohlcv) < fetch_limit:
                break

        df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df

    def fetch_by_date(self, exchange_name: str, symbol: str, timeframe : str ='1h', since : str | datetime = None, until : str | datetime = None) -> pd.DataFrame:
        """
        Concrete implementation of the abstract method from BaseDataCollector.
        """
        exchange = self.check_exchange(exchange_name)

        # Convert 'since' and 'until' to ms
        if isinstance(since, str):
            since = parse(since).replace(tzinfo=timezone.utc)
        if isinstance(until, str):
            until = parse(until).replace(tzinfo=timezone.utc)

        if isinstance(since, datetime):
            since = int(since.timestamp() * 1000)
        if until and isinstance(until, datetime):
            until = int(until.timestamp() * 1000)

        all_ohlcv = []
        limit = 1000
        timeframe_ms = exchange.parse_timeframe(timeframe) * 1000
        
        if until:
            total_expected = (until - since) // timeframe_ms
        else:
            total_expected = float('inf')
        logger.info(f"Expected: {total_expected} candles")
        
        while len(all_ohlcv) < total_expected:
            fetch_limit = min(limit, total_expected - len(all_ohlcv))
            logger.info(f"Fetching {fetch_limit} rows")
            ohlcv = self.safe_fetch_ohlcv(exchange, symbol, timeframe, since, fetch_limit)
            if not ohlcv:
                break

            all_ohlcv += ohlcv
            last_timestamp = ohlcv[-1][0]
            logger.info(f"Fetched up to: {pd.to_datetime(last_timestamp, unit='ms')}")
            since = last_timestamp + 1
            time.sleep(max(exchange.rateLimit / 1000, 1))
            if len(ohlcv) < fetch_limit:
                break
            
        df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        logger.info(f"Fetched: {len(df)} candles")
        return df
