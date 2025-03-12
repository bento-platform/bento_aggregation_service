from __future__ import annotations

from bento_lib.apps.fastapi import BentoFastAPI

from . import __version__
from .authz import authz_middleware
from .config import get_config
from .constants import BENTO_SERVICE_KIND, SERVICE_TYPE
from .logger import get_logger
from .search.handlers.datasets import dataset_search_router


BENTO_SERVICE_INFO = {
    "serviceKind": BENTO_SERVICE_KIND,
    "dataService": False,
    "workflowProvider": False,
    "gitRepository": "https://github.com/bento-platform/bento_aggregation_service",
}


# TODO: Find a way to DI this
config_for_setup = get_config()
logger_for_setup = get_logger(config_for_setup)
application = BentoFastAPI(
    authz_middleware,
    config_for_setup,
    logger_for_setup,
    BENTO_SERVICE_INFO,
    SERVICE_TYPE,
    __version__,
    configure_structlog_access_logger=True,  # Set up custom access log middleware to replace the default Uvicorn one
)

application.include_router(dataset_search_router)
