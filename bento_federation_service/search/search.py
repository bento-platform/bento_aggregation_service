import sys
import tornado.gen

from bento_lib.responses.errors import bad_request_error
from datetime import datetime
from tornado.httpclient import AsyncHTTPClient
from tornado.netutil import Resolver
from tornado.queues import Queue
from tornado.web import RequestHandler
from typing import Optional

from ..constants import MAX_BUFFER_SIZE, SERVICE_NAME, WORKERS
from ..utils import peer_fetch, ServiceSocketResolver, get_request_json, get_new_peer_queue, get_auth_header

AsyncHTTPClient.configure(None, max_buffer_size=MAX_BUFFER_SIZE, resolver=ServiceSocketResolver(resolver=Resolver()))


__all__ = ["SearchHandler"]


# noinspection PyAbstractClass,PyAttributeOutsideInit
class SearchHandler(RequestHandler):
    def initialize(self, peer_manager):
        self.peer_manager = peer_manager

    async def search_worker(self, peer_queue: Queue, search_path: str, auth_header: Optional[str], responses: list):
        client = AsyncHTTPClient()

        async for peer in peer_queue:
            if peer is None:  # Exit signal
                return

            try:
                responses.append((peer, await peer_fetch(
                    client,
                    peer,
                    f"api/{search_path}",
                    request_body=self.request.body,
                    auth_header=auth_header,
                )))

            except Exception as e:
                # TODO: Less broad of an exception
                responses.append((peer, None))
                print(f"[{SERVICE_NAME} {datetime.now()}] Connection issue or timeout with peer {peer}.\n"
                      f"    Error: {str(e)}", flush=True, file=sys.stderr)

            finally:
                peer_queue.task_done()

    async def options(self, _search_path: str):
        self.set_status(204)
        await self.finish()

    async def post(self, search_path: str):
        # TODO: NO SPEC FOR THIS YET SO I JUST MADE SOME STUFF UP

        request = get_request_json(self.request.body)
        if request is None:
            # TODO: Better / more compliant error message
            self.set_status(400)
            await self.finish(bad_request_error("Invalid request format (missing body)"))
            return

        auth_header = get_auth_header(self.request.headers)

        peer_queue = get_new_peer_queue(await self.peer_manager.get_peers())
        responses = []
        workers = tornado.gen.multi([self.search_worker(peer_queue, search_path, auth_header, responses)
                                     for _ in range(WORKERS)])
        await peer_queue.join()

        try:
            self.write({"results": {n: r["results"] for n, r in responses}})

        except KeyError:
            self.clear()
            self.set_status(400)
            self.write(bad_request_error())  # TODO: What message to send?

        await self.finish()

        # Trigger exit for all workers
        for _ in range(WORKERS):
            peer_queue.put_nowait(None)

        # Wait for workers to exit
        await workers
