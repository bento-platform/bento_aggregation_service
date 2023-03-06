from __future__ import annotations

import asyncio
import bento_aggregation_service
import tornado.gen
import tornado.ioloop
import tornado.web

from bento_lib.types import GA4GHServiceInfo
from tornado.web import RequestHandler, url

from .constants import (
    BENTO_SERVICE_KIND,
    SERVICE_ID,
    SERVICE_TYPE,
    SERVICE_NAME,
    PORT,
    BASE_PATH,
    CHORD_DEBUG,
    CHORD_URL_SET,
    DEBUGGER_PORT,
)
from .logger import logger
from .search.handlers.datasets import DatasetsSearchHandler
from .search.handlers.private_dataset import PrivateDatasetSearchHandler


# noinspection PyAbstractClass,PyAttributeOutsideInit
class ServiceInfoHandler(RequestHandler):
    SERVICE_INFO: GA4GHServiceInfo = {
        "id": SERVICE_ID,
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
    }

    async def _git_stdout(*args) -> str:
        git_proc = await asyncio.create_subprocess_exec(
            "git", *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        res, _ = await git_proc.communicate()
        return res.decode().rstrip()

    async def get(self):
        # Spec: https://github.com/ga4gh-discovery/ga4gh-service-info

        if not CHORD_DEBUG:
            self.write({**self.SERVICE_INFO, "environment": "prod"})
            return

        bento_info = {
            **self.SERVICE_INFO,
            "environment": "dev",
        }

        try:
            if res_tag := await self._git_stdout("describe", "--tags", "--abbrev=0"):
                # noinspection PyTypeChecker
                bento_info["gitTag"] = res_tag
            if res_branch := await self._git_stdout("branch", "--show-current"):
                # noinspection PyTypeChecker
                bento_info["gitBranch"] = res_branch
            if res_commit := await self._git_stdout("rev-parse", "HEAD"):
                # noinspection PyTypeChecker
                bento_info["gitCommit"] = res_commit

        except Exception as e:
            logger.warning(f"Could not retrieve git information: {type(e).__name__}")

        self.write(bento_info)


class Application(tornado.web.Application):
    def __init__(self, base_path: str):
        super().__init__([
            url(f"{base_path}/service-info", ServiceInfoHandler),
            url(f"{base_path}/dataset-search", DatasetsSearchHandler),
            url(f"{base_path}/private/dataset-search/([a-zA-Z0-9\\-_]+)", PrivateDatasetSearchHandler),
        ])


application = Application(BASE_PATH)


def run():  # pragma: no cover
    if not CHORD_URL_SET:
        logger.critical("CHORD_URL is not set, terminating...")
        exit(1)

    if CHORD_DEBUG:
        try:
            # noinspection PyPackageRequirements,PyUnresolvedReferences
            import debugpy
            debugpy.listen(("0.0.0.0", DEBUGGER_PORT))
            logger.info("debugger attached")
        except ImportError:
            logger.info("debugpy not found")

    application.listen(PORT)
    tornado.ioloop.IOLoop.current().start()
