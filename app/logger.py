"""
app/logger.py

Shared logging configuration for the sea-temperature app.
Import and use like:

    from app.logger import get_logger
    log = get_logger(__name__)
"""

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Return a logger with consistent formatting.
    Safe to call multiple times — logging module deduplicates handlers.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s  %(levelname)-8s %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False

    return logger
