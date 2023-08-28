from __future__ import annotations

import asyncio
import bento_aggregation_service

from bento_lib.types import GA4GHServiceInfo
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import ConfigDependency, get_config
from .constants import (
    BENTO_SERVICE_KIND,
    SERVICE_TYPE,
    SERVICE_NAME,
)
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
        "git", *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    res, _ = await git_proc.communicate()
    return res.decode().rstrip()


@application.get("/service-info")
async def service_info(config: ConfigDependency, logger: LoggerDependency):
    info: GA4GHServiceInfo = {
        "id": config.service_id,
        "name": SERVICE_NAME,  # TODO: Should be globally unique?
        "type": SERVICE_TYPE,
        "description": "Aggregation service for a Bento platform node.",
        "organization": {
            "name": "C3G",
            "url": "https://www.computationalgenomics.ca"
        },
        "contactUrl": "mailto:info@c3g.ca",
        "version": bento_aggregation_service.__version__,
        "bento": {
            "serviceKind": BENTO_SERVICE_KIND,
        },
        "environment": "prod",
    }

    if not config.bento_debug:
        return info

    info["environment"] = "dev"

    try:
        if res_tag := await _git_stdout("describe", "--tags", "--abbrev=0"):
            # noinspection PyTypeChecker
            info["bento"]["gitTag"] = res_tag
        if res_branch := await _git_stdout("branch", "--show-current"):
            # noinspection PyTypeChecker
            info["bento"]["gitBranch"] = res_branch
        if res_commit := await _git_stdout("rev-parse", "HEAD"):
            # noinspection PyTypeChecker
            info["bento"]["gitCommit"] = res_commit

    except Exception as e:
        logger.warning(f"Could not retrieve git information: {type(e).__name__}")

    return info
