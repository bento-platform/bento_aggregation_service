from bento_lib.service_info.manager import ServiceManager
from fastapi import Depends
from functools import lru_cache
from typing import Annotated

from .config import ConfigDependency
from .logger import LoggerDependency

__all__ = [
    "ServiceManagerDependency",
]


@lru_cache()
def get_service_manager(config: ConfigDependency, logger: LoggerDependency) -> ServiceManager:
    return ServiceManager(logger, config.request_timeout, config.service_registry_url, not config.bento_debug)


ServiceManagerDependency = Annotated[ServiceManager, Depends(get_service_manager)]
