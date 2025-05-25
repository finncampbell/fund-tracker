import os
import logging
from logging.handlers import RotatingFileHandler

def get_logger(
    name: str,
    log_file: str,
    level: int = logging.INFO,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5
) -> logging.Logger:
    """
    Return a logger that writes to `log_file`, rotating at `max_bytes`.
    """
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # If handlers already configured, skip re-adding
    if not logger.handlers:
        handler = RotatingFileHandler(
            filename=log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        fmt = logging.Formatter(
            fmt="%(asctime)s %(name)s %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)

    return logger
