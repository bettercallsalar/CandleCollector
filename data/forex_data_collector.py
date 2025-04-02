import logging
import pandas as pd
import requests
from datetime import datetime, timedelta
import time
from base_data_collector import BaseDataCollector

# Set up logging for the module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ForexDataCollector(BaseDataCollector):
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.twelvedata.com/time_series"

    def _get_timedelta_from_interval(self, interval : str) -> timedelta | None:
        mapping = {
            "1min": timedelta(minutes=1),
            "5min": timedelta(minutes=5),
            "15min": timedelta(minutes=15),
            "30min": timedelta(minutes=30),
            "1h": timedelta(hours=1),
            "2h": timedelta(hours=2),
            "4h": timedelta(hours=4),
            "1day": timedelta(days=1),
        }
        return mapping.get(interval)

    def fetch_by_limit(self, base : str ='EUR', quote : str = 'USD', interval : str = '1h', limit : int = 100) -> pd.DataFrame:
        symbol = f"{base}/{quote}"
        params = {
            "symbol": symbol,
            "interval": interval,
            "outputsize": limit,
            "apikey": self.api_key,
            "format": "JSON"
        }
        response = requests.get(self.base_url, params=params)
        data = response.json()
        if "values" not in data:
            raise ValueError(f"[!] Error fetching data: {data}")
        df = pd.DataFrame(data["values"])
        df.rename(columns={
            'datetime': 'timestamp',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume'
        }, inplace=True)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(by='timestamp').reset_index(drop=True)
        return df

    def fetch_by_date(self, base : str = 'EUR', quote : str = 'USD', interval : str = '1h', start_date : str | datetime = None, end_date : str | datetime = None)-> pd.DataFrame:
        symbol = f"{base}/{quote}"
        interval_td = self._get_timedelta_from_interval(interval)
        if not interval_td:
            raise ValueError(f"Unsupported interval: {interval}")
        
        # Calculate expected number of rows
        dt_start = pd.to_datetime(start_date)
        dt_end = pd.to_datetime(end_date)
        expected = int((dt_end - dt_start) / interval_td)
        logger.info(f"Expected: {expected} candles")

        all_data = []
        batch_start = dt_start

        while batch_start < dt_end:
            batch_end = min(batch_start + interval_td * 4999, dt_end)
            logger.info(f"[+] Fetching: {batch_start} to {batch_end}")

            params = {
                "symbol": symbol,
                "interval": interval,
                "start_date": batch_start.strftime('%Y-%m-%d %H:%M:%S'),
                "end_date": batch_end.strftime('%Y-%m-%d %H:%M:%S'),
                "apikey": self.api_key,
                "format": "JSON"
            }

            response = requests.get(self.base_url, params=params)
            data = response.json()
            if "values" not in data:
                raise ValueError(f"[!] Error fetching data: {data}")

            df = pd.DataFrame(data["values"])
            df.rename(columns={'datetime': 'timestamp'}, inplace=True)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values(by='timestamp')
            all_data.append(df)

            # Advance to next window
            batch_start = batch_end + interval_td
            time.sleep(1)  # prevent rate-limit

        final_df = pd.concat(all_data).drop_duplicates(subset='timestamp').reset_index(drop=True)
        logger.info(f"Fetched: {len(final_df)} candles")
        return final_df
    
    def fill_missing_candles(self, df: pd.DataFrame, interval: str) -> pd.DataFrame:
        """
        Ensures a continuous OHLCV time series by forward-filling missing candles.
        """
        interval_td = self._get_timedelta_from_interval(interval)
        if not interval_td:
            raise ValueError(f"Unsupported interval: {interval}")

        # Set timestamp as index
        df = df.copy()
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)

        # Create full expected time index
        full_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq=interval_td)
        
        # Reindex the dataframe and forward-fill missing candles
        df = df.reindex(full_range)
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)

        df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].fillna(method='ffill')
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'timestamp'}, inplace=True)

        logger.info(f"[✓] Final rows after filling: {len(df)}")
        return df
