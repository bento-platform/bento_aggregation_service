from tornado.web import RequestHandler
from .search import perform_search


__all__ = ["FederatedDatasetsSearchHandler"]


# noinspection PyAbstractClass,PyAttributeOutsideInit
class FederatedDatasetsSearchHandler(RequestHandler):
    def initialize(self, peer_manager):
        self.peer_manager = peer_manager

    async def options(self):
        self.set_status(204)
        await self.finish()

    async def post(self):
        await perform_search(self, "federation/dataset-search", "POST", dataset_search=True)
