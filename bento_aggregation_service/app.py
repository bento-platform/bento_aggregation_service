from __future__ import annotations

import bento_aggregation_service
import tornado.gen
import tornado.ioloop
import tornado.web

from datetime import datetime
from tornado.web import RequestHandler, url

from .constants import (
    SERVICE_ID,
    SERVICE_TYPE,
    SERVICE_NAME,
    PORT,
    BASE_PATH,
    CHORD_DEBUG,
    CHORD_URL_SET,
    DEBUGGER_PORT,
)
from .search.handlers.datasets import DatasetsSearchHandler
from .search.handlers.private_dataset import PrivateDatasetSearchHandler


# noinspection PyAbstractClass,PyAttributeOutsideInit
class ServiceInfoHandler(RequestHandler):
    async def get(self):
        # Spec: https://github.com/ga4gh-discovery/ga4gh-service-info
        self.write({
            "id": SERVICE_ID,
            "name": SERVICE_NAME,  # TODO: Should be globally unique?
            "type": SERVICE_TYPE,
            "description": "Aggregation service for a Bento platform node.",
            "organization": {
                "name": "C3G",
                "url": "https://www.computationalgenomics.ca"
            },
            "contactUrl": "mailto:david.lougheed@mail.mcgill.ca",
            "version": bento_aggregation_service.__version__
        })


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
        print(f"[{SERVICE_NAME} {datetime.utcnow()}] CHORD_URL is not set, terminating...")
        exit(1)

    if CHORD_DEBUG:
        try:
            # noinspection PyPackageRequirements,PyUnresolvedReferences
            import debugpy
            debugpy.listen(("0.0.0.0", DEBUGGER_PORT))
            print("debugger attached")
        except ImportError:
            print("debugpy not found")

    application.listen(PORT)
    tornado.ioloop.IOLoop.current().start()
