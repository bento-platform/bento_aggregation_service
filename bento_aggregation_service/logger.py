import logging

from .constants import LOG_LEVEL

logging.basicConfig(level=logging.NOTSET)

__all__ = [
    "logger",
]

logger = logging.getLogger(__name__)

log_level_conversion = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}

logger.setLevel(log_level_conversion[LOG_LEVEL])
