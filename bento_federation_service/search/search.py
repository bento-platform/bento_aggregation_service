import sys
import tornado.gen

from bento_lib.responses.errors import bad_request_error
from bento_lib.search.queries import convert_query_to_ast_and_preprocess
from datetime import datetime
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.queues import Queue
from tornado.web import RequestHandler
from typing import Optional

from ..constants import CHORD_URL, SERVICE_NAME, WORKERS
from ..utils import peer_fetch, get_request_json, iterable_to_queue, get_auth_header


__all__ = [
    "perform_search",
    "SearchHandler",
]


async def _search_worker(peer_queue: Queue, search_path: str, request_body: Optional[bytes], method: str,
                         auth_header: Optional[str], responses: list):
    client = AsyncHTTPClient()

    async for peer in peer_queue:
        if peer is None:
            # Exit signal
            return

        try:
            responses.append((peer, await peer_fetch(
                client,
                peer,
                f"api/{search_path}",
                request_body=request_body,
                method=method,

                # Only pass the bearer token to our own node (otherwise it could be hijacked)
                # TODO: How to share auth?
                auth_header=(auth_header if peer == CHORD_URL else None),
            )))

        except HTTPError as e:
            # TODO: Less broad of an exception
            responses.append((peer, None))
            print(f"[{SERVICE_NAME} {datetime.now()}] Connection issue or timeout with peer {peer}.\n"
                  f"    Error: {str(e)}", flush=True)

        finally:
            peer_queue.task_done()


async def perform_search(handler: RequestHandler, search_path: str, method: str, dataset_search: bool = False):
    # TODO: Implement federated GET search

    request = get_request_json(handler.request.body)

    if request is None or (dataset_search and "data_type_queries" not in request or "join_query" not in request):
        print(f"[{SERVICE_NAME} {datetime.now()}] Request error: Invalid request {request}", flush=True,
              file=sys.stderr)
        handler.set_status(400)
        await handler.finish(
            bad_request_error(
                "Invalid request format (missing body or data_type_queries or join_query)"
                if dataset_search else "Invalid request format (missing body)"
            ))
        return

    try:
        if dataset_search:
            # Check for query errors if we know that this request should be of a specific
            # (dataset search) format.

            # Try compiling join query to make sure it works (if it's not null, i.e. unspecified)
            if request["join_query"] is not None:
                convert_query_to_ast_and_preprocess(request["join_query"])

            for q in request["data_type_queries"].values():
                # Try compiling each query to make sure it works
                convert_query_to_ast_and_preprocess(q)

        auth_header = get_auth_header(handler.request.headers)

        # noinspection PyUnresolvedReferences
        peer_queue = iterable_to_queue(await handler.peer_manager.get_peers())
        responses = []
        workers = tornado.gen.multi([
            _search_worker(peer_queue, search_path, handler.request.body, method, auth_header, responses)
            for _ in range(WORKERS)
        ])
        await peer_queue.join()

        try:
            await handler.finish({"results": {n: r["results"] if r is not None else None for n, r in responses}})

        except KeyError as e:
            print(f"[{SERVICE_NAME} {datetime.now()}] Key error: {str(e)}", flush=True, file=sys.stderr)
            handler.clear()
            handler.set_status(400)
            await handler.finish(bad_request_error())  # TODO: What message to send?

        # Trigger exit for all workers
        for _ in range(WORKERS):
            peer_queue.put_nowait(None)

        # Wait for workers to exit
        await workers

    except (TypeError, ValueError, SyntaxError) as e:  # errors from query processing
        print(f"[{SERVICE_NAME} {datetime.now()}] TypeError / ValueError / SyntaxError: {str(e)}", flush=True,
              file=sys.stderr)
        handler.set_status(400)
        await handler.finish(bad_request_error(f"Query processing error: {str(e)}"))  # TODO: Better message


# noinspection PyAbstractClass,PyAttributeOutsideInit
class SearchHandler(RequestHandler):
    def initialize(self, peer_manager):
        self.peer_manager = peer_manager

    async def options(self, _search_path: str):
        self.set_status(204)
        await self.finish()

    async def get(self, search_path: str):
        await perform_search(self, search_path, "GET")

    async def post(self, search_path: str):
        await perform_search(self, search_path, "POST")
