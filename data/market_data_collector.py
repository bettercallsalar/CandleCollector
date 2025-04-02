from forex_data_collector  import ForexDataCollector
from crypto_data_collector import CryptoDataCollector
from db.data_exporter import DataExporter
class MarketDataCollector:
    def __init__(self, api_key=None):
        self.crypto = CryptoDataCollector()
        self.forex = ForexDataCollector(api_key=api_key) if api_key else None
        self.exporter = DataExporter()
