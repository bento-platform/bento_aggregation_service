import bento_aggregation_service
import tornado.gen
import tornado.ioloop
import tornado.web

from datetime import datetime
from tornado.httpserver import HTTPServer
from tornado.netutil import bind_unix_socket
from tornado.web import RequestHandler, url

from .constants import (
    SERVICE_ID,
    SERVICE_TYPE,
    SERVICE_NAME,
    BASE_PATH,
    SERVICE_SOCKET,
    CHORD_URL_SET,
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
                "url": "http://www.computationalgenomics.ca"
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

    server = HTTPServer(application)
    server.add_socket(bind_unix_socket(SERVICE_SOCKET))
    tornado.ioloop.IOLoop.current().start()
