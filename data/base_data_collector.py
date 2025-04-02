# base_data_collector.py
import logging
from abc import ABC, abstractmethod
from typing import Callable, Any
logger = logging.getLogger(__name__)

class BaseDataCollector(ABC):
    """
    Abstract base class that outlines the structure for all data collectors.
    Enforces the presence of fetch_by_limit and fetch_by_date methods.
    """
    
    @abstractmethod
    def fetch_by_limit(self, *args, **kwargs):
        """
        Fetch data based on a fixed limit of records.
        Derived classes should implement this with their own logic.
        """
        pass
    
    @abstractmethod
    def fetch_by_date(self, *args, **kwargs):
        """
        Fetch data for a specified date/time range.
        Derived classes should implement this with their own logic.
        """
        pass
    
    def safe_retry(
        self, 
        func: Callable[...,Any], 
        max_attempts: int = 3, 
        delay_seconds: int = 3, 
        *args: tuple, 
        **kwargs: dict
    ) -> Any:
        """
        Example shared retry method that derived classes can use for API calls.
        This approach encapsulates common retry logic in one place.

        :param func: The function (API call) to attempt.
        :param max_attempts: How many times to retry.
        :param delay_seconds: How long to wait between retries.
        :param args: Positional arguments to pass to the function.
        :param kwargs: Keyword arguments to pass to the function.
        :return: The result of the function call if successful.
        :raises Exception: If all attempts fail, raise the last encountered exception.
        """
        last_exception: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.warning(f"Attempt {attempt}/{max_attempts} failed: {e}")
                last_exception = e
                if attempt < max_attempts:
                    import time
                    time.sleep(delay_seconds)
        # If we reach here, all attempts have failed
        raise last_exception