# import_historical.py (updated)
import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def import_full_historical(collector, mongo_handler, crypto_tests, periods) -> None:
    """
    For each crypto test case and period, check the latest candle in MongoDB and
    fetch only data newer than that timestamp. Convert the data to dictionaries and
    upsert them into the corresponding collection.
    """
    for test in crypto_tests:
        exchange = test["exchange"]
        symbol = test["symbol"]
        timeframe = test["timeframe"]
        for period_label, period_since, period_until in periods:
            try:
                # Check the DB for the latest timestamp in this collection.
                latest = mongo_handler.get_latest_timestamp(exchange, symbol, timeframe)
                if latest is not None:
                    # If the latest is within the period, adjust 'since' accordingly.
                    # If the latest is after the period, skip this period.
                    if latest >= period_until:
                        continue
                    adjusted_since = latest + timedelta(milliseconds=1)
                else:
                    adjusted_since = period_since
                    logger.info(f"Importing {symbol} on {exchange} ({timeframe}) from {adjusted_since} for period {period_label} (no existing data).")
                
                df = collector.crypto.fetch_by_date(
                    exchange_name=exchange,
                    symbol=symbol,
                    timeframe=timeframe,
                    since=adjusted_since,
                    until=period_until
                )
                records = df.to_dict("records")
                # Ensure each record's timestamp is a Python datetime object.
                for r in records:
                    if not isinstance(r["timestamp"], datetime):
                        r["timestamp"] = pd.to_datetime(r["timestamp"]).to_pydatetime()
                mongo_handler.upsert_ohlcv(records, exchange, symbol, timeframe)
                logger.info(f"Imported {len(records)} candles for {symbol} ({period_label}).")
            except Exception as e:
                logger.error(f"Error importing historical data for {symbol} on {exchange} for period {period_label}: {e}")
