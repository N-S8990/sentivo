"""Logging configuration for the application."""

import logging
import sys


def setup_logging(level=logging.INFO):
    """
    Configures a standardized logger that writes to stdout.
    Tames noisy third-party libraries so the signal chain stays readable.
    """
    log_format = "%(asctime)s - %(name)-12s - %(levelname)-8s - %(message)s"

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    formatter = logging.Formatter(log_format)
    handler.setFormatter(formatter)

    root_logger.addHandler(handler)

    # quieten chatty libraries
    logging.getLogger("kafka").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("prawcore").setLevel(logging.WARNING)
