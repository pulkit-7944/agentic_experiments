# src/nuvyn_bldr/core/logging_config.py

import logging
import sys
from nuvyn_bldr.core.config import settings


def setup_logging():
    """
    Sets up a standardized logging configuration for the entire application.

    This function should be called once at the start of the application,
    typically in main.py. It configures a handler that streams logs to
    the console with a consistent format. The log level is read from the
    application's settings.
    """
    # Get the log level from our settings, default to INFO if not set
    log_level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)

    # Define the format for our log messages
    log_formatter = logging.Formatter(
        "%(asctime)s - [%(levelname)s] - [%(name)s] - %(message)s"
    )

    # Create a handler to stream logs to the console (stdout)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(log_formatter)

    # Get the root logger and configure it
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear existing handlers to avoid duplicate logs
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
        
    root_logger.addHandler(handler)

    logging.info(f"Logging configured with level: {logging.getLevelName(log_level)}")