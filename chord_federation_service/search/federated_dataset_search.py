import sys
import tornado.gen

from chord_lib.responses.errors import bad_request_error
from chord_lib.search.queries import convert_query_to_ast_and_preprocess
from datetime import datetime
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.netutil import Resolver
from tornado.queues import Queue
from tornado.web import RequestHandler

from ..constants import MAX_BUFFER_SIZE, SERVICE_NAME, WORKERS
from ..utils import peer_fetch, ServiceSocketResolver, get_request_json, get_new_peer_queue


AsyncHTTPClient.configure(None, max_buffer_size=MAX_BUFFER_SIZE, resolver=ServiceSocketResolver(resolver=Resolver()))


__all__ = ["FederatedDatasetSearchHandler"]


# noinspection PyAbstractClass
class FederatedDatasetSearchHandler(RequestHandler):
    @staticmethod
    async def search_worker(peer_queue: Queue, request_body: bytes, responses: list):
        client = AsyncHTTPClient()

        async for peer in peer_queue:
            if peer is None:
                # Exit signal
                return

            try:
                responses.append((peer, await peer_fetch(client, peer, "api/federation/dataset-search",
                                                         request_body=request_body, method="POST")))

            except HTTPError as e:
                # TODO: Less broad of an exception
                responses.append((peer, None))
                print(f"[{SERVICE_NAME} {datetime.now()}] Connection issue or timeout with peer {peer}.\n"
                      f"    Error: {str(e)}", flush=True)

            finally:
                peer_queue.task_done()

    async def options(self):
        self.set_status(204)
        await self.finish()

    async def post(self):
        request = get_request_json(self.request.body)
        if request is None or "data_type_queries" not in request or "join_query" not in request:
            # TODO: Expand out request error messages
            print(f"[{SERVICE_NAME} {datetime.now()}] Request error", flush=True, file=sys.stderr)
            self.set_status(400)
            self.write(bad_request_error("Invalid request format (missing body or data_type_queries or join_query)"))
            return

        try:
            # Check for query errors

            # Try compiling join query to make sure it works (if it's not null, i.e. unspecified)
            if request["join_query"] is not None:
                convert_query_to_ast_and_preprocess(request["join_query"])

            for q in request["data_type_queries"].values():
                # Try compiling each query to make sure it works
                convert_query_to_ast_and_preprocess(q)

            # Federate out requests

            peer_queue = get_new_peer_queue(await self.application.peer_manager.get_peers())
            responses = []
            workers = tornado.gen.multi([self.search_worker(peer_queue, self.request.body, responses)
                                         for _ in range(WORKERS)])
            await peer_queue.join()

            try:
                self.write({"results": {n: r["results"] if r is not None else None for n, r in responses}})

            except KeyError as e:
                print(f"[{SERVICE_NAME} {datetime.now()}] Key error: {str(e)}", flush=True, file=sys.stderr)
                self.clear()
                self.set_status(400)
                self.write(bad_request_error())  # TODO: What message to send here?

            await self.finish()

            # Trigger exit for all workers
            for _ in range(WORKERS):
                peer_queue.put_nowait(None)

            # Wait for workers to exit
            await workers

        except (TypeError, ValueError, SyntaxError) as e:  # errors from query processing
            print(f"[{SERVICE_NAME} {datetime.now()}] TypeError / ValueError / SyntaxError: {str(e)}", flush=True,
                  file=sys.stderr)
            self.set_status(400)
            await self.finish(bad_request_error(f"Query processing error: {str(e)}"))  # TODO: Better message
