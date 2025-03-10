from __future__ import annotations

import asyncio

from bento_lib.service_info.helpers import build_service_info_from_pydantic_config
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import __version__
from .config import ConfigDependency, get_config
from .constants import BENTO_SERVICE_KIND, SERVICE_TYPE
from .logger import LoggerDependency
from .search.handlers.datasets import dataset_search_router


application = FastAPI()

# TODO: Find a way to DI this
config_for_setup = get_config()

application.add_middleware(
    CORSMiddleware,
    allow_origins=config_for_setup.cors_origins,
    allow_headers=["Authorization"],
    allow_credentials=True,
    allow_methods=["*"],
)

application.include_router(dataset_search_router)


async def _git_stdout(*args) -> str:
    git_proc = await asyncio.create_subprocess_exec(
        "git", *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    res, _ = await git_proc.communicate()
    return res.decode().rstrip()


@application.get("/service-info")
async def service_info(config: ConfigDependency, logger: LoggerDependency):
    return await build_service_info_from_pydantic_config(
        config,
        logger,
        {
            "serviceKind": BENTO_SERVICE_KIND,
            "gitRepository": "https://github.com/bento-platform/bento_aggregation_service",
        },
        SERVICE_TYPE,
        __version__,
    )
