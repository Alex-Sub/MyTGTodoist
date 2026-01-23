import os
import sys

from loguru import logger


def setup_logging() -> None:
    from src.config import settings

    log_dir = os.path.dirname(settings.log_path)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    logger.remove()
    logger.add(sys.stdout, level="INFO")
    logger.add(settings.log_path, rotation="10 MB", level="INFO")
