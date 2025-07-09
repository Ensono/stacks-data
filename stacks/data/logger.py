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


def add_handler(fmt: str, logger: logging.Logger) -> None:
    """Add a new StreamHandler with the specified format to the provided logger.

    Args:
        fmt: The format string to be used by the logging formatter.
        logger: The logger instance to which the handler will be added.
    """
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(datefmt="%Y-%m-%d %H:%M:%S", fmt=fmt)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """Creates a base logger.

    Args:
        name: The name of the logger.

    Returns:
        Logger instance.
    """
    fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        add_handler(fmt, logger)

    return logger
