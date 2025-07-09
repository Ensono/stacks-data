import logging


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


def get_logger(name: str, log_level: int | str = logging.INFO) -> logging.Logger:
    """Creates a base logger.

    Args:
        name: The name of the logger.
        log_level: The logging level to set for the logger. Defaults to logging.INFO.

    Returns:
        Logger instance.
    """
    fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    if not logger.handlers:
        add_handler(fmt, logger)

    return logger
