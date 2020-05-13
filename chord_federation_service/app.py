import chord_federation_service
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
    INITIALIZE_IMMEDIATELY,
    CHORD_URLS_SET,
    BASE_PATH,
    SERVICE_SOCKET,
)
from .db import peer_db
from .peers.handlers import PeerHandler, PeerRefreshHandler
from .peers.manager import PeerManager
from .search.dataset_search import DatasetSearchHandler, PrivateDatasetSearchHandler
from .search.federated_dataset_search import FederatedDatasetSearchHandler
from .search.search import SearchHandler


# noinspection PyAbstractClass,PyAttributeOutsideInit
class ServiceInfoHandler(RequestHandler):
    async def get(self):
        # Spec: https://github.com/ga4gh-discovery/ga4gh-service-info
        self.write({
            "id": SERVICE_ID,
            "name": SERVICE_NAME,  # TODO: Should be globally unique?
            "type": SERVICE_TYPE,
            "description": "Federation service for a CHORD application.",
            "organization": {
                "name": "C3G",
                "url": "http://www.computationalgenomics.ca"
            },
            "contactUrl": "mailto:david.lougheed@mail.mcgill.ca",
            "version": chord_federation_service.__version__
        })


def post_start_hook(peer_manager: PeerManager):
    peer_manager.get_peers()
    print(f"[{SERVICE_NAME} {datetime.utcnow()}] Post-start hook finished", flush=True)


# noinspection PyAbstractClass
class PostStartHookHandler(RequestHandler):
    async def get(self):
        """
        Handles post-start hook which pings the node registry with the current node's information.
        :return:
        """
        print(f"[{SERVICE_NAME} {datetime.utcnow()}] Post-start hook invoked via URL request", flush=True)
        post_start_hook(self.application.peer_manager)
        self.clear()
        self.set_status(204)


class Application(tornado.web.Application):
    def __init__(self, db, base_path: str):
        self.db = db
        self.peer_manager = PeerManager(self.db)

        if INITIALIZE_IMMEDIATELY:
            post_start_hook(self.peer_manager)

        super(Application, self).__init__([
            url(f"{base_path}/service-info", ServiceInfoHandler),
            url(f"{base_path}/private/post-start-hook", PostStartHookHandler),
            url(f"{base_path}/peers", PeerHandler),
            url(f"{base_path}/private/peers/refresh", PeerRefreshHandler),
            url(f"{base_path}/dataset-search", DatasetSearchHandler),
            url(f"{base_path}/private/dataset-search/([a-zA-Z0-9\\-_]+)", PrivateDatasetSearchHandler),
            url(f"{base_path}/federated-dataset-search", FederatedDatasetSearchHandler),
            url(f"{base_path}/search-aggregate/([a-zA-Z0-9\\-_/]+)", SearchHandler),
        ])


application = Application(peer_db, BASE_PATH)


def run():
    if not CHORD_URLS_SET:
        print(f"[{SERVICE_NAME} {datetime.utcnow()}] No CHORD URLs given, terminating...")
        exit(1)

    server = HTTPServer(application)
    server.add_socket(bind_unix_socket(SERVICE_SOCKET))
    tornado.ioloop.IOLoop.instance().start()
