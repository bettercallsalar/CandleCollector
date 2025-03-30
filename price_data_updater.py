# price_data_updater.py
import logging
from import_historical import import_full_historical
from real_time_updater import real_time_updater

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class PriceDataUpdater:
    """
    This class coordinates the full historical import and then starts the continuous real-time updater.
    It assumes that you have:
      - an instance of MarketDataCollector (collector)
      - an instance of MongoDBHandler (mongo_handler)
      - a list of crypto test cases (each with keys: exchange, symbol, timeframe)
      - a list of historical periods (tuples of (label, since, until))
    """
    def __init__(self, collector, mongo_handler, crypto_tests, historical_periods, real_time_sleep: int = 60):
        """
        :param collector: Instance of MarketDataCollector.
        :param mongo_handler: Instance of MongoDBHandler.
        :param crypto_tests: List of dictionaries, each with keys "exchange", "symbol", "timeframe".
        :param historical_periods: List of tuples: (period_label, since, until).
        :param real_time_sleep: Sleep interval in seconds between real-time updater cycles.
        """
        self.collector = collector
        self.mongo_handler = mongo_handler
        self.crypto_tests = crypto_tests
        self.historical_periods = historical_periods
        self.real_time_sleep = real_time_sleep

    def run(self):
        """
        Runs the full historical import first, then starts the continuous real-time updater.
        """
        try:
            logger.info("Starting full historical import...")
            import_full_historical(self.collector, self.mongo_handler, self.crypto_tests, self.historical_periods)
            logger.info("Historical import completed.")
        except Exception as e:
            logger.error("Historical import failed: %s", e)
            # Optionally, you could choose to exit here if historical data is critical.
        
        logger.info("Starting real-time updater...")
        # This function runs indefinitely. You might add signal handling for graceful shutdown later.
        real_time_updater(self.collector, self.mongo_handler, self.crypto_tests, sleep_interval=self.real_time_sleep)
