import json

from fastapi import Depends
from functools import lru_cache
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings, EnvSettingsSource, PydanticBaseSettingsSource, SettingsConfigDict
from typing import Annotated, Any, Literal

from .constants import SERVICE_TYPE

__all__ = [
    "Config",
    "get_config",
    "ConfigDependency",
]


class CorsOriginsParsingSource(EnvSettingsSource):
    def prepare_field_value(self, field_name: str, field: FieldInfo, value: Any, value_is_complex: bool) -> Any:
        if field_name == "cors_origins":
            return tuple(x.strip() for x in value.split(";"))
        return json.loads(value)


class Config(BaseSettings):
    bento_debug: bool = False

    service_id: str = str(":".join(list(SERVICE_TYPE.values())[:2]))
    service_url: str = "http://127.0.0.1:5000"  # base URL to construct object URIs from

    request_timeout: int = 180  # seconds

    bento_authz_service_url: str  # Bento authorization service base URL
    authz_enabled: bool = True

    # Other services - settings and flags
    use_gohan: bool = False
    katsu_url: str
    service_registry_url: str  # used for fetching list of data services, so we can get data type providers

    cors_origins: tuple[str, ...] = ()

    log_level: Literal["debug", "info", "warning", "error"] = "debug"

    # Make Config instances hashable + immutable
    model_config = SettingsConfigDict(frozen=True)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (CorsOriginsParsingSource(settings_cls),)


@lru_cache()
def get_config() -> Config:
    return Config()


ConfigDependency = Annotated[Config, Depends(get_config)]
