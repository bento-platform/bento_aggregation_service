import aiohttp
import asyncio
import contextlib
import logging

from bento_lib.types import GA4GHServiceInfo
from fastapi import Depends
from functools import lru_cache
from typing import Annotated, AsyncIterator
from urllib.parse import urljoin

from .config import Config, ConfigDependency
from .logger import LoggerDependency
from .models import DataType

__all__ = [
    "ServiceManager",
    "ServiceManagerDependency",
]


class ServiceManager:
    def __init__(self, config: Config, logger: logging.Logger):
        self._logger: logging.Logger = logger

        self._service_registry_url: str = config.service_registry_url.rstrip("/")
        self._timeout: int = config.request_timeout
        self._verify_ssl: bool = not config.bento_debug

        self._service_list: list[GA4GHServiceInfo] = []

    @contextlib.asynccontextmanager
    async def _http_session(
        self,
        existing: aiohttp.ClientSession | None = None,
    ) -> AsyncIterator[aiohttp.ClientSession]:
        # Don't use the FastAPI dependency for the HTTP session, since this object is long-lasting.

        if existing:
            yield existing
            return

        session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(verify_ssl=self._verify_ssl),
            timeout=aiohttp.ClientTimeout(total=self._timeout),
        )

        try:
            yield session
        finally:
            await session.close()

    async def fetch_service_list(
        self,
        existing_session: aiohttp.ClientSession | None = None,
        headers: dict[str, str] | None = None,
    ) -> list[GA4GHServiceInfo]:
        if self._service_list:
            return self._service_list

        session: aiohttp.ClientSession
        async with self._http_session(existing_session) as session:
            url = urljoin(self._service_registry_url, "/api/service-registry/services")
            r = await session.get(url, headers=headers)

            if not r.ok:
                self._logger.error(
                    f"Recieved error response from service registry while fetching service list: "
                    f"{r.status} {await r.json()}")
                self._service_list = []
                return []

            service_list: list[GA4GHServiceInfo]
            if service_list := await r.json():
                self._service_list = service_list
                return service_list
            else:
                self._logger.warning("Got empty service list response from service registry")
                return []

    async def fetch_data_types(
        self,
        existing_session: aiohttp.ClientSession | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, DataType]:
        headers["content-type"] = "application/json"
        services = await self.fetch_service_list(headers=headers)
        data_services = [s for s in services if s.get("bento", {}).get("dataService")]

        async def _get_data_types_for_service(s: aiohttp.ClientSession, ds: GA4GHServiceInfo) -> tuple[DataType, ...]:
            service_base_url = ds["url"]
            dt_url = service_base_url.rstrip("/") + "/data-types"

            r = await s.get(dt_url, headers=headers)
            if not r.ok:
                self._logger.error(f"Recieved error from data-types URL {dt_url}: {r.status} {await r.json()}")
                return ()

            payload = await r.json()
            service_dts: list[GA4GHServiceInfo] = payload
            return tuple(
                DataType.model_validate({"service_base_url": service_base_url, "data_type_listing": sdt})
                for sdt in service_dts
            )

        session: aiohttp.ClientSession
        async with self._http_session(existing=existing_session) as session:
            dts: tuple[tuple[DataType, ...], ...] = await asyncio.gather(
                *(_get_data_types_for_service(session, ds) for ds in data_services))

        types: dict[str, DataType] = {}
        for dts_item in dts:
            dt = {dt.data_type_listing.id: dt for dt in dts_item}
            types.update(dt)
        return types


@lru_cache()
def get_service_manager(config: ConfigDependency, logger: LoggerDependency) -> ServiceManager:
    return ServiceManager(config, logger)


ServiceManagerDependency = Annotated[ServiceManager, Depends(get_service_manager)]
