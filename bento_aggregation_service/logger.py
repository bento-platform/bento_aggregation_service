import logging

from fastapi import Depends
from functools import lru_cache
from typing import Annotated

from .config import ConfigDependency

__all__ = [
    "get_logger",
    "LoggerDependency",
]

log_config_to_log_level = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}

logging.basicConfig(level=logging.DEBUG)


@lru_cache
def get_logger(config: ConfigDependency) -> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(log_config_to_log_level[config.log_level])
    return logger


LoggerDependency = Annotated[logging.Logger, Depends(get_logger)]
