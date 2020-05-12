import chord_federation_service
import tornado.gen
import tornado.ioloop
import tornado.web

from tornado.httpserver import HTTPServer
from tornado.netutil import bind_unix_socket
from tornado.web import RequestHandler, url

from .constants import SERVICE_ID, SERVICE_TYPE, SERVICE_NAME, CHORD_URLS_SET, BASE_PATH, SERVICE_SOCKET
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

        if self.get_argument("update_peers", "true") == "true":
            # Hack to force lists to update when the CHORD dashboard is loaded
            await self.application.peer_manager.get_peers()

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


class Application(tornado.web.Application):
    def __init__(self, db, base_path: str):
        self.db = db
        self.peer_manager = PeerManager(self.db)

        super(Application, self).__init__([
            url(f"{base_path}/service-info", ServiceInfoHandler),
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
        print(f"[{SERVICE_NAME}] No CHORD URLs given, terminating...")
        exit(1)

    server = HTTPServer(application)
    server.add_socket(bind_unix_socket(SERVICE_SOCKET))
    tornado.ioloop.IOLoop.instance().start()
