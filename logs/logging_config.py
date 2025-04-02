# logging_config.py
import logging
import os
import sys

try:
    # For Windows support of ANSI escape sequences
    from colorama import init as colorama_init
    colorama_init()
except ImportError:
    pass  # colorama is optional on Unix-based systems

class ColoredFormatter(logging.Formatter):
    # ANSI escape sequences for colors
    COLORS = {
        'DEBUG': '\033[94m',    # Blue
        'INFO': '\033[92m',     # Green
        'WARNING': '\033[93m',  # Yellow
        'ERROR': '\033[91m',    # Red
        'CRITICAL': '\033[1;91m'  # Bold Red
    }
    RESET = '\033[0m'

    def format(self, record):
        color = self.COLORS.get(record.levelname, self.RESET)
        message = super().format(record)
        return f"{color}{message}{self.RESET}"

    def setup_logging(level=None):
        """
        Configures the root logger with colored output for the console.
        The log level can be set via the LOG_LEVEL environment variable or passed directly.
        """
        if level is None:
            level_name = os.environ.get("LOG_LEVEL", "WARNING")
            level = getattr(logging, level_name.upper(), logging.WARNING)
            
        logger = logging.getLogger()
        logger.setLevel(level)
        
        # Avoid adding multiple handlers in interactive environments
        if logger.hasHandlers():
            logger.handlers.clear()

        handler = logging.StreamHandler(sys.stdout)
        formatter = ColoredFormatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
