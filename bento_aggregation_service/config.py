from bento_lib.config.pydantic import BentoBaseConfig
from fastapi import Depends
from functools import lru_cache
from typing import Annotated

from .constants import SERVICE_TYPE

__all__ = [
    "Config",
    "get_config",
    "ConfigDependency",
]


class Config(BentoBaseConfig):
    service_id: str = str(":".join(list(SERVICE_TYPE.values())[:2]))

    request_timeout: int = 180  # seconds

    # Other services - settings and flags
    use_gohan: bool = False
    katsu_url: str
    service_registry_url: str  # used for fetching list of data services, so we can get data type providers


@lru_cache()
def get_config() -> Config:
    return Config()


ConfigDependency = Annotated[Config, Depends(get_config)]
