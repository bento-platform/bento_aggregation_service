import asyncio
import itertools
import json
import socket
import tornado.gen

from chord_lib.search.data_structure import check_ast_against_data_structure
from chord_lib.search.queries import convert_query_to_ast_and_preprocess
from datetime import datetime
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.netutil import Resolver
from tornado.queues import Queue
from tornado.web import RequestHandler

from typing import List, Iterable, Optional, Set, Tuple

from .constants import CHORD_HOST, WORKERS, SOCKET_INTERNAL, SOCKET_INTERNAL_DOMAIN
from .utils import peer_fetch

SOCKET_INTERNAL_URL = f"http://{SOCKET_INTERNAL_DOMAIN}/"


# TODO: Try to use OverrideResolver instead
class ServiceSocketResolver(Resolver):
    # noinspection PyAttributeOutsideInit
    def initialize(self, resolver):  # tornado Configurable init
        self.resolver = resolver

    def close(self):
        self.resolver.close()

    async def resolve(self, host, port, *args, **kwargs):
        if host == SOCKET_INTERNAL_DOMAIN:
            return [(socket.AF_UNIX, SOCKET_INTERNAL)]
        return await self.resolver.resolve(host, port, *args, **kwargs)


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


# noinspection PyAbstractClass
class SearchHandler(RequestHandler):
    async def search_worker(self, peer_queue: Queue, search_path: str, responses: list):
        client = AsyncHTTPClient()

        async for peer in peer_queue:
            if peer is None:  # Exit signal
                return

            try:
                responses.append((peer, await peer_fetch(client, peer, f"api/{search_path}", self.request.body)))

            except Exception as e:
                # TODO: Less broad of an exception
                responses.append((peer, None))
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

        peer_queue = get_new_peer_queue(await self.application.peer_manager.get_peers())
        responses = []
        workers = tornado.gen.multi([self.search_worker(peer_queue, search_path, responses) for _ in range(WORKERS)])
        await peer_queue.join()

        try:
            self.write({"results": {n: r["results"] for n, r in responses}})

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


DATASET_SEARCH_HEADERS = {"Host": CHORD_HOST}


def _linked_fields_to_join_query_fragment(field_1: Tuple[str, List[str]], field_2: Tuple[str, List[str]]):
    return ["#eq", ["#resolve", field_1[0], "[item]", field_1[1]], ["#resolve", field_2[0], "[item]", field_2[1]]]


def _linked_field_set_to_join_query_rec(pairs):
    if len(pairs) == 1:
        return _linked_fields_to_join_query_fragment(*pairs[0])

    return ["#and",
            _linked_fields_to_join_query_fragment(*pairs[0]),
            _linked_fields_to_join_query_fragment(*pairs[1:])]


def _linked_field_sets_to_join_query(linked_field_sets, data_type_set: Set[str]) -> Optional[List]:
    if len(linked_field_sets) == 0:
        return None

    # TODO: This blows up combinatorially, oh well.
    pairs = tuple(p for p in itertools.combinations(linked_field_sets[0].items(), 2)
                  if p[0][0] in data_type_set and p[1][0] in data_type_set)

    print(itertools.combinations(linked_field_sets[0].items(), 2), pairs, flush=True)

    if len(pairs) == 0:
        return None  # TODO: Somehow tell the user no join was applied or return NO RESULTS if None and 2+ data types?

    if len(linked_field_sets) == 1:
        return _linked_field_set_to_join_query_rec(pairs)

    return ["#and",
            _linked_field_set_to_join_query_rec(pairs),
            _linked_field_sets_to_join_query(linked_field_sets[1:], data_type_set)]


def get_dataset_results(data_type_queries, join_query, data_type_results, datasets_dict, dataset_id,
                        dataset_object_schema, results):
    # dataset_id: dataset identifier
    # data_type_results: dict of data types and corresponding table matches

    print(dataset_id)

    # Only include useful linked field sets, i.e. 2+ fields
    linked_field_sets = [lfs["fields"] for lfs in datasets_dict[dataset_id].get("linked_field_sets", [])
                         if len(lfs["fields"]) > 1]
    print(linked_field_sets)
    if join_query is None:
        # Could re-return None; pass set of all data types to filter out combinations
        join_query = _linked_field_sets_to_join_query(linked_field_sets, set(data_type_queries.keys()))
    print(join_query)

    # TODO: Avoid re-compiling a fixed join query
    join_query_ast = convert_query_to_ast_and_preprocess(join_query) if join_query is not None else None
    print(join_query_ast)

    # Append result if:
    #  - No join query was specified and there is at least one matching table present in the dataset; or
    #  - A join query is present and evaluates to True against the dataset.
    # Need to mark this query as internal, since the federation service "gets" extra privileges here
    # (joined data isn't explicitly exposed.)
    if ((join_query_ast is None and any(len(dtr) > 0 for dtr in data_type_results.values())) or
            (join_query_ast is not None and
             check_ast_against_data_structure(join_query_ast, data_type_results, dataset_object_schema,
                                              internal=True))):
        # Append results to aggregator list
        print((join_query_ast is None and any(len(dtr) > 0 for dtr in data_type_results.values())))
        results.append(datasets_dict[dataset_id])  # TODO: Make sure all information here is public-level.

    print("----")


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
            for q in data_type_queries.values():
                # Try compiling each query to make sure it works
                convert_query_to_ast_and_preprocess(q)

            client = AsyncHTTPClient()

            # TODO: Local query using sockets?

            # TODO: Reduce API call with combined renderers?
            # TODO: Handle pagination
            # Use Unix socket resolver
            projects, table_ownerships = await asyncio.gather(
                peer_fetch(client, SOCKET_INTERNAL_URL, "api/metadata/api/projects", method="GET",
                           extra_headers=DATASET_SEARCH_HEADERS),
                peer_fetch(client, SOCKET_INTERNAL_URL, "api/metadata/api/table_ownership", method="GET",
                           extra_headers=DATASET_SEARCH_HEADERS)
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
                            method="GET",
                            extra_headers=DATASET_SEARCH_HEADERS
                        )) if table_data_type in data_type_queries else {}
                    }

                if table_data_type not in dataset_objects_dict[table_dataset_id]:
                    dataset_objects_dict[table_dataset_id][table_data_type] = (await peer_fetch(
                        client,
                        SOCKET_INTERNAL_URL,  # Use Unix socket resolver
                        f"api/{t['service_artifact']}/private/tables/{t['table_id']}/search",
                        request_body=json.dumps({"query": data_type_queries[table_data_type]}),
                        method="POST",
                        extra_headers=DATASET_SEARCH_HEADERS
                    ))["results"] if table_data_type in data_type_queries else []

            print("[CHORD Federation {}] Done fetching individual service search results.".format(datetime.now()),
                  flush=True)

            for dataset_id, data_type_results in dataset_objects_dict.items():  # TODO: Worker
                get_dataset_results(data_type_queries, join_query, data_type_results, datasets_dict, dataset_id,
                                    dataset_object_schema, results)

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

            try:
                responses.append((peer, await peer_fetch(client, peer, "api/federation/dataset-search",
                                                         request_body=request_body, method="POST")))

            except HTTPError as e:
                # TODO: Less broad of an exception
                responses.append((peer, None))
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

            peer_queue = get_new_peer_queue(await self.application.peer_manager.get_peers())
            responses = []
            workers = tornado.gen.multi([self.search_worker(peer_queue, self.request.body, responses)
                                         for _ in range(WORKERS)])
            await peer_queue.join()

            try:
                self.write({"results": {n: r["results"] for n, r in responses}})

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
