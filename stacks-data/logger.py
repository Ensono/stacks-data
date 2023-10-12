"""Logging configuration."""
import logging
import colorlog


def setup_logger(name: str = "", log_level: int = logging.INFO) -> logging.Logger:
    """Set up a colored logger with customizable log level and formatting.

    Args:
        name: The name of the logger. Defaults to an empty string.
        log_level: The desired log level for the logger. Should be one of the constants
            defined in the 'logging' module (e.g., logging.DEBUG, logging.INFO). Defaults to logging.INFO.

    Returns:
        A configured logger instance ready to use.
    """
    formatter = colorlog.ColoredFormatter(
        fmt="%(log_color)s%(asctime)s %(levelname)s%(reset)s%(blue)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        reset=True,
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "red,bg_white",
        },
        secondary_log_colors={},
        style="%",
    )

    handler = colorlog.StreamHandler()
    handler.setFormatter(formatter)

    logger = colorlog.getLogger(name)
    logger.addHandler(handler)
    logger.setLevel(log_level)

    return logger
