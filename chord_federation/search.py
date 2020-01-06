import asyncio
import json
import socket
import tornado.gen

from chord_lib.search.data_structure import check_ast_against_data_structure
from chord_lib.search.queries import convert_query_to_ast_and_preprocess
from datetime import datetime
from itertools import chain
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.netutil import Resolver
from tornado.queues import Queue
from tornado.web import RequestHandler

from typing import Iterable, Optional

from .constants import CHORD_HOST, TIMEOUT, WORKERS, SOCKET_INTERNAL, SOCKET_INTERNAL_DOMAIN

SOCKET_INTERNAL_URL = f"http://{SOCKET_INTERNAL_DOMAIN}/"


class ServiceSocketResolver(Resolver):
    # noinspection PyAttributeOutsideInit
    def initialize(self, resolver):  # tornado Configurable init
        self.resolver = resolver

    def close(self):
        self.resolver.close()

    async def resolve(self, host, port, *args, **kwargs):
        print("resolve", host)
        if host == SOCKET_INTERNAL_DOMAIN:
            print("sock resolved")
            return [(socket.AF_UNIX, SOCKET_INTERNAL)]

        r = await self.resolver.resolve(host, port, *args, **kwargs)
        print("else resolved", r)
        return r


AsyncHTTPClient.configure(None, resolver=ServiceSocketResolver(resolver=Resolver()))


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
    print("peer fetch", peer, path_fragment)
    r = await client.fetch(f"{peer}{path_fragment}", request_timeout=TIMEOUT, method=method, body=request_body,
                           headers={"Content-Type": "application/json", "Host": CHORD_HOST}, raise_error=True)
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

    async def options(self, _search_path: str):
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
        if request is None or "data_type_queries" not in request:
            # TODO: Better / more compliant error message
            self.set_status(400)
            return

        # Format: {"data_type": ["#eq", ...]}
        data_type_queries = request["data_type_queries"]

        # Format: normal query, using data types for join conditions
        join_query = request.get("join_query", None)

        results = []

        try:
            join_query_ast = convert_query_to_ast_and_preprocess(join_query) if join_query is not None else None

            for q in data_type_queries.values():
                # Try compiling each query to make sure it works
                convert_query_to_ast_and_preprocess(q)

            client = AsyncHTTPClient()

            # TODO: Local query using sockets?

            # TODO: Reduce API call with combined renderers?
            # TODO: Handle pagination
            # Use Unix socket resolver
            projects, table_ownerships = await asyncio.gather(
                peer_fetch(client, SOCKET_INTERNAL_URL, "api/metadata/api/projects", method="GET"),
                peer_fetch(client, SOCKET_INTERNAL_URL, "api/metadata/api/table_ownership", method="GET")
            )

            datasets_dict = {d["identifier"]: d for p in projects["results"] for d in p["datasets"]}
            dataset_objects_dict = {d: {} for d in datasets_dict.keys()}

            dataset_object_schema = {
                "type": "object",
                "properties": {}
            }

            # Include metadata table explicitly
            # TODO: This should probably be auto-produced by the metadata service

            tables_with_metadata = table_ownerships["results"] + [{
                "table_id": d,
                "dataset": d,
                "data_type": "phenopacket",  # TODO: Don't hard-code?
                "service_artifact": "metadata",
            } for d in datasets_dict.keys()]

            for t in tables_with_metadata:  # TODO: Query worker
                table_dataset_id = t["dataset"]
                table_data_type = t["data_type"]

                if table_dataset_id not in datasets_dict:
                    # TODO: error
                    continue

                if table_data_type not in dataset_object_schema["properties"]:
                    dataset_object_schema["properties"][table_data_type] = {
                        "type": "array",
                        "items": (await peer_fetch(
                            client,
                            SOCKET_INTERNAL_URL,  # Use Unix socket resolver
                            f"api/{t['service_artifact']}/data-types/{table_data_type}/schema",
                            method="GET"
                        )) if table_data_type in data_type_queries else {}
                    }

                if table_data_type not in dataset_objects_dict[table_dataset_id]:
                    dataset_objects_dict[table_dataset_id][table_data_type] = (await peer_fetch(
                        client,
                        SOCKET_INTERNAL_URL,  # Use Unix socket resolver
                        f"api/{t['service_artifact']}/private/tables/{t['table_id']}/search",
                        request_body=json.dumps({"query": data_type_queries[table_data_type]}),
                        method="POST"
                    ))["results"] if table_data_type in data_type_queries else []

            print("[CHORD Federation {}] Done fetching individual service search results.".format(datetime.now()),
                  flush=True)

            for d, s in dataset_objects_dict.items():  # TODO: Worker
                # d: dataset identifier
                # s: dict of data types and corresponding table matches
                # Append result if:
                #  - No join query was specified and there is at least one matching table present in the dataset; or
                #  - A join query is present and evaluates to True against the dataset.
                # Need to mark this query as internal, since the federation service "gets" extra privileges here
                # (joined data isn't explicitly exposed.)
                if ((join_query_ast is None and any(len(dtr) > 0 for dtr in s.values())) or
                        check_ast_against_data_structure(join_query_ast, s, dataset_object_schema, internal=True)):
                    results.append(datasets_dict[d])  # TODO: Make sure all information here is public-level.

            self.write({"results": results})

        except HTTPError as e:
            # Metadata service error
            print("Error from service: {}".format(e))
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

            print("search worker", peer)

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

            # Try compiling join query to make sure it works (if it's not null, i.e. unspecified)
            if request["join_query"] is not None:
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
