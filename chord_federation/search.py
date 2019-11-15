import asyncio
import json
import tornado.gen

from chord_lib.search.data_structure import check_ast_against_data_structure
from chord_lib.search.queries import convert_query_to_ast_and_preprocess
from datetime import datetime
from itertools import chain
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.queues import Queue
from tornado.web import RequestHandler

from typing import Iterable, Optional

from .constants import TIMEOUT, WORKERS, CHORD_URL


def get_request_json(request_body: bytes) -> Optional[dict]:
    request = None

    try:
        request = json.loads(request_body)
    except json.JSONDecodeError:
        pass

    # TODO: Validate against a JSON schema or OpenAPI
    if not isinstance(request, dict):
        request = None

    return request


def get_new_peer_queue(peers: Iterable) -> Queue:
    peer_queue = Queue()
    for peer in peers:
        peer_queue.put_nowait(peer)

    return peer_queue


async def peer_fetch(client: AsyncHTTPClient, peer: str, path_fragment: str, request_body: Optional[bytes] = None,
                     method: str = "POST"):
    r = await client.fetch(f"{peer}{path_fragment}", request_timeout=TIMEOUT, method=method, body=request_body,
                           headers={"Content-Type": "application/json"}, raise_error=True)
    return json.loads(r.body)


# noinspection PyAbstractClass
class SearchHandler(RequestHandler):
    async def search_worker(self, peer_queue: Queue, search_path: str, responses: list):
        client = AsyncHTTPClient()

        async for peer in peer_queue:
            if peer is None:  # Exit signal
                return

            try:
                responses.append(await peer_fetch(client, peer, f"api/{search_path}", self.request.body))

            except Exception as e:
                # TODO: Less broad of an exception
                responses.append(None)
                print("[CHORD Federation {}] Connection issue or timeout with peer {}.\n"
                      "    Error: {}".format(datetime.now(), peer, str(e)), flush=True)

            finally:
                peer_queue.task_done()

    async def options(self):
        self.set_status(204)
        await self.finish()

    async def post(self, search_path: str):
        # TODO: NO SPEC FOR THIS YET SO I JUST MADE SOME STUFF UP

        request = get_request_json(self.request.body)
        if request is None:
            # TODO: Better / more compliant error message
            self.set_status(400)
            return

        c = self.application.db.cursor()
        peer_queue = get_new_peer_queue(await self.application.peer_manager.get_peers(c))
        self.application.db.commit()

        responses = []
        workers = tornado.gen.multi([self.search_worker(peer_queue, search_path, responses) for _ in range(WORKERS)])
        await peer_queue.join()
        good_responses = tuple(r for r in responses if r is not None)

        try:
            self.write({
                "results": list(chain.from_iterable((r["results"] for r in good_responses))),
                "peers": {"responded": len(good_responses), "total": len(responses)}
            })

        except KeyError:
            # TODO: Better / more compliant error message
            self.clear()
            self.set_status(400)

        await self.finish()

        # Trigger exit for all workers
        for _ in range(WORKERS):
            peer_queue.put_nowait(None)

        # Wait for workers to exit
        await workers


class QueryError(Exception):
    pass


async def empty_list():
    return []


# noinspection PyAbstractClass
class DatasetSearchHandler(RequestHandler):  # TODO: Move to another dedicated service?
    """
    Aggregates tables into datasets and runs a query against the data.
    """

    async def options(self):
        self.set_status(204)
        await self.finish()

    async def post(self):
        request = get_request_json(self.request.body)
        if request is None or "data_type_queries" not in request or "join_query" not in request:
            # TODO: Better / more compliant error message
            self.set_status(400)
            return

        # Format: {"data_type": ["#eq", ...]}
        data_type_queries = request["data_type_queries"]

        # Format: normal query, using data types for join conditions
        join_query = request["join_query"]

        results = []

        try:
            query_ast = convert_query_to_ast_and_preprocess(join_query)

            for q in data_type_queries.values():
                # Try compiling each query to make sure it works
                convert_query_to_ast_and_preprocess(q)

            client = AsyncHTTPClient()

            # TODO: Local query using sockets?

            # TODO: One API call?
            datasets, table_ownerships = await asyncio.gather(
                peer_fetch(client, CHORD_URL, "api/metadata/api/datasets", method="GET"),
                peer_fetch(client, CHORD_URL, "api/metadata/api/table_ownership", method="GET")
            )

            datasets_dict = {d["dataset_id"]: d for d in datasets}
            dataset_objects_dict = {d["dataset_id"]: {} for d in datasets}

            dataset_object_schema = {
                "type": "object",
                "properties": {}
            }

            for t in table_ownerships:  # TODO: Query worker
                if t["dataset"] not in datasets_dict:
                    # TODO: error
                    continue

                if "tables" not in datasets[t["dataset"]]:
                    datasets[t["dataset"]]["tables"] = []

                if t["data_type"] not in dataset_object_schema["properties"]:
                    dataset_object_schema["properties"][t["data_type"]] = {
                        "type": "array",
                        "items": await peer_fetch(
                            client,
                            CHORD_URL,
                            f"api/{t['service_artifact']}/private/tables/{t['table_id']}/search",
                            request_body=json.dumps({"query": data_type_queries[t["data_type"]]}),
                            method="POST"
                        ) if t["data_type"] in data_type_queries else {}
                    }

                if t["data_type"] not in dataset_objects_dict[t["dataset"]]:
                    dataset_objects_dict[t["dataset"]][t["data_type"]] = await peer_fetch(
                        client,
                        CHORD_URL,
                        f"api/{t['service_artifact']}/private/tables/{t['table_id']}/search",
                        request_body=json.dumps({"query": data_type_queries[t["data_type"]]}),
                        method="POST"
                    ) if t["data_type"] in data_type_queries else []

            for d, s in dataset_objects_dict.items():  # TODO: Worker
                result = check_ast_against_data_structure(query_ast, s, dataset_object_schema)
                if result:
                    results.append({"id": d})

            self.write({"results": results})

        except HTTPError:
            # Metadata service error
            self.set_status(500)

        except (TypeError, ValueError, SyntaxError) as e:  # errors from query processing
            # TODO: Better / more compliant error message
            print(str(e))
            self.set_status(400)


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
                responses.append(await peer_fetch(client, peer, "api/federation/dataset-search",
                                                  request_body=request_body, method="POST"))
            except HTTPError as e:
                # TODO: Less broad of an exception
                responses.append(None)
                print("[CHORD Federation {}] Connection issue or timeout with peer {}.\n"
                      "    Error: {}".format(datetime.now(), peer, str(e)), flush=True)

            finally:
                peer_queue.task_done()

    async def options(self):
        self.set_status(204)
        await self.finish()

    async def post(self):
        request = get_request_json(self.request.body)
        if request is None or "data_type_queries" not in request or "join_query" not in request:
            # TODO: Better / more compliant error message
            self.set_status(400)
            return

        try:
            # Check for query errors

            # Try compiling join query to make sure it works
            convert_query_to_ast_and_preprocess(request["join_query"])

            for q in request["data_type_queries"].values():
                # Try compiling each query to make sure it works
                convert_query_to_ast_and_preprocess(q)

            # Federate out requests

            c = self.application.db.cursor()
            peer_queue = get_new_peer_queue(await self.application.peer_manager.get_peers(c))
            self.application.db.commit()

            responses = []
            workers = tornado.gen.multi([self.search_worker(peer_queue, self.request.body, responses)
                                         for _ in range(WORKERS)])
            await peer_queue.join()
            good_responses = tuple(r for r in responses if r is not None)

            try:
                self.write({
                    "results": list(chain.from_iterable((r["results"] for r in good_responses))),
                    "peers": {"responded": len(good_responses), "total": len(responses)}
                })

            except KeyError:
                # TODO: Better / more compliant error message
                self.clear()
                self.set_status(400)

            await self.finish()

            # Trigger exit for all workers
            for _ in range(WORKERS):
                peer_queue.put_nowait(None)

            # Wait for workers to exit
            await workers

        except (TypeError, ValueError, SyntaxError):  # errors from query processing
            # TODO: Better / more compliant error message
            self.set_status(400)
            await self.finish()
