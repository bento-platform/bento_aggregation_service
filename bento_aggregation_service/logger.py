import structlog

from bento_lib.logging.structured.configure import configure_structlog_from_bento_config, configure_structlog_uvicorn
from fastapi import Depends
from functools import lru_cache
from typing import Annotated

from .config import ConfigDependency
from .constants import BENTO_SERVICE_KIND

__all__ = [
    "get_logger",
    "LoggerDependency",
]


@lru_cache
def get_logger(config: ConfigDependency) -> structlog.stdlib.BoundLogger:
    configure_structlog_from_bento_config(config)
    configure_structlog_uvicorn()  # If using uvicorn to serve the app

    return structlog.stdlib.get_logger(f"{BENTO_SERVICE_KIND}.logger")


LoggerDependency = Annotated[structlog.stdlib.BoundLogger, Depends(get_logger)]
