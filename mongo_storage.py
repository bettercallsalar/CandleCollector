# mongo_storage.py
import pymongo
import logging
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from datetime import datetime
from typing import List, Dict, Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class MongoDBHandler:
    def __init__(self, uri: str = "mongodb://localhost:27017", db_name: str = "algo_trade"):
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[db_name]
    
    def _collection_name(self, exchange: str, symbol: str, timeframe: str) -> str:
        # Normalize values: lowercase, remove '/' from symbol
        exchange_norm = exchange.lower()
        symbol_norm = symbol.replace("/", "").lower()
        timeframe_norm = timeframe.lower()
        return f"{exchange_norm}_{symbol_norm}_{timeframe_norm}"
    
    def get_collection(self, exchange: str, symbol: str, timeframe: str):
        """
        Returns the collection for the specified exchange, symbol, and timeframe.
        Creates a unique index on the timestamp field.
        """
        collection_name = self._collection_name(exchange, symbol, timeframe)
        coll = self.db[collection_name]
        # Create a unique index on timestamp
        coll.create_index([("timestamp", 1)], unique=True)
        return coll

    def upsert_ohlcv(self, data: List[Dict[str, Any]], exchange: str, symbol: str, timeframe: str) -> None:
        """
        Upsert a list of OHLCV documents into the specific MongoDB collection.
        Each document must contain at least: timestamp, open, high, low, close, volume.
        """
        if not data:
            return
        
        collection = self.get_collection(exchange, symbol, timeframe)
        operations = []
        for doc in data:
            query = {"timestamp": doc["timestamp"]}
            operations.append(
                UpdateOne(query, {"$set": doc}, upsert=True)
            )
        try:
            result = collection.bulk_write(operations, ordered=False)
            logger.info(f"Upserted {result.upserted_count} documents, modified {result.modified_count} in collection {collection.name}.")
        except BulkWriteError as bwe:
            logger.error("Bulk write error in %s: %s", collection.name, bwe.details)

    def get_latest_timestamp(self, exchange: str, symbol: str, timeframe: str) -> datetime | None:
        """
        Returns the latest timestamp from the collection for the given exchange, symbol, and timeframe.
        If no document exists, returns None.
        Ensures the returned datetime is timezone-aware (UTC).
        """
        coll = self.get_collection(exchange, symbol, timeframe)
        doc = coll.find_one(sort=[("timestamp", -1)])
        if doc:
            ts = doc["timestamp"]
            # If ts is naive, assume it's in UTC and convert it
            if ts.tzinfo is None:
                from datetime import timezone
                ts = ts.replace(tzinfo=timezone.utc)
            return ts
        return None