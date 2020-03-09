import asyncio
import itertools
import json

from chord_lib.responses.errors import bad_request_error, internal_server_error
from chord_lib.search.data_structure import check_ast_against_data_structure
from chord_lib.search.queries import convert_query_to_ast_and_preprocess, Query
from datetime import datetime
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.netutil import Resolver
from tornado.web import RequestHandler

from typing import Dict, List, Optional, Set, Tuple

from ..constants import CHORD_HOST, MAX_BUFFER_SIZE, SERVICE_NAME, SOCKET_INTERNAL_URL
from ..utils import peer_fetch, ServiceSocketResolver, get_request_json


AsyncHTTPClient.configure(None, max_buffer_size=MAX_BUFFER_SIZE, resolver=ServiceSocketResolver(resolver=Resolver()))


__all__ = ["DatasetSearchHandler"]


DATASET_SEARCH_HEADERS = {"Host": CHORD_HOST}


FieldSpec = List[str]
DataTypeAndField = Tuple[str, FieldSpec]
DictOfDataTypesAndFields = Dict[str, FieldSpec]
LinkedFieldSetList = List[DictOfDataTypesAndFields]


def _linked_fields_to_join_query_fragment(field_1: DataTypeAndField, field_2: DataTypeAndField) -> Query:
    return ["#eq", ["#resolve", field_1[0], "[item]", *field_1[1]], ["#resolve", field_2[0], "[item]", *field_2[1]]]


def _linked_field_set_to_join_query_rec(pairs: tuple) -> Query:
    if len(pairs) == 1:
        return _linked_fields_to_join_query_fragment(*pairs[0])

    return ["#and",
            _linked_fields_to_join_query_fragment(*pairs[0]),
            _linked_field_set_to_join_query_rec(pairs[1:])]


def _linked_field_sets_to_join_query(linked_field_sets: LinkedFieldSetList, data_type_set: Set[str]) -> Optional[Query]:
    if len(linked_field_sets) == 0:
        return None

    # TODO: This blows up combinatorially, oh well.
    pairs = tuple(p for p in itertools.combinations(linked_field_sets[0].items(), 2)
                  if p[0][0] in data_type_set and p[1][0] in data_type_set)

    if len(pairs) == 0:
        return None  # TODO: Somehow tell the user no join was applied or return NO RESULTS if None and 2+ data types?

    if len(linked_field_sets) == 1:
        return _linked_field_set_to_join_query_rec(pairs)

    return ["#and",
            _linked_field_set_to_join_query_rec(pairs),
            _linked_field_sets_to_join_query(linked_field_sets[1:], data_type_set)]


def _augment_resolves(query: Query, prefix: Tuple[str, ...]) -> Query:
    if not isinstance(query, list) or len(query) == 0 or len(query[0]) == 0 or query[0][0] != "#":
        return query

    if query[0] == "#resolve":
        return ["#resolve", *prefix, *query[1:]]

    return [query[0], *(_augment_resolves(q, prefix) for q in query[1:])]


def get_dataset_results(
    data_type_queries: Dict[str, Query],
    dataset_join_query: Query,
    data_type_results: Dict[str, list],
    dataset: dict,
    dataset_object_schema: dict,
    results: list
):
    # TODO: Check dataset, table-level authorizations

    # dataset_id: dataset identifier
    # data_type_results: dict of data types and corresponding table matches

    # TODO: Avoid re-compiling a fixed join query
    join_query_ast = convert_query_to_ast_and_preprocess(dataset_join_query) if dataset_join_query is not None else None

    print(f"[{SERVICE_NAME} {datetime.now()}] Compiled join query: {join_query_ast}", flush=True)

    # Append result if:
    #  - No join query was specified,
    #      and there is at least one matching table present in the dataset,
    #      and only one data type is being searched; or
    #  - A join query is present and evaluates to True against the dataset.
    # Need to mark this query as internal, since the federation service "gets" extra privileges here
    # (joined data isn't explicitly exposed.)
    # TODO: Optimize by not fetching if the query isn't going anywhere (i.e. no linked field sets, 2+ data types)
    if ((join_query_ast is None and any(len(dtr) > 0 for dtr in data_type_results.values())
         and len(data_type_queries) == 1) or
            (join_query_ast is not None and
             check_ast_against_data_structure(join_query_ast, data_type_results, dataset_object_schema,
                                              internal=True))):
        # Append results to aggregator list
        results.append(dataset)  # TODO: Make sure all information here is public-level.


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
            self.set_status(400)
            self.write(bad_request_error("Invalid request format (missing body or data_type_queries)"))
            return

        # Format: {"data_type": ["#eq", ...]}
        data_type_queries: Dict[str, Query] = request["data_type_queries"]

        # Format: normal query, using data types for join conditions
        join_query = request.get("join_query", None)

        results = []

        try:
            for q in data_type_queries.values():
                # Try compiling each query to make sure it works. Any exceptions thrown will get caught below.
                convert_query_to_ast_and_preprocess(q)

            client = AsyncHTTPClient()

            # TODO: Reduce API call with combined renderers?
            # TODO: Handle pagination
            # TODO: Why fetch projects instead of datasets?
            # Use Unix socket resolver
            projects, table_ownerships = await asyncio.gather(
                peer_fetch(client, SOCKET_INTERNAL_URL, "api/metadata/api/projects", method="GET",
                           extra_headers=DATASET_SEARCH_HEADERS),
                peer_fetch(client, SOCKET_INTERNAL_URL, "api/metadata/api/table_ownership", method="GET",
                           extra_headers=DATASET_SEARCH_HEADERS)
            )

            datasets_dict: Dict[str, dict] = {d["identifier"]: d for p in projects["results"] for d in p["datasets"]}
            dataset_objects_dict: Dict[str, Dict[str, list]] = {d: {} for d in datasets_dict.keys()}

            dataset_object_schema = {
                "type": "object",
                "properties": {}
            }

            dataset_join_queries: Dict[str, Query] = {d: None for d in datasets_dict.keys()}

            # Include metadata table explicitly
            # TODO: This should probably be auto-produced by the metadata service

            tables_with_metadata: List[Dict[str, str]] = table_ownerships["results"] + [{
                "table_id": d,
                "dataset": d,
                "data_type": "phenopacket",  # TODO: Don't hard-code?
                "service_artifact": "metadata",
            } for d in datasets_dict.keys()]

            for t in tables_with_metadata:  # TODO: Query worker
                table_dataset_id = t["dataset"]
                table_data_type = t["data_type"]

                # Check if we were able to fetch the dataset description for the dataset ID specified by the table
                # ownership entry; if not, log an error and (for now) just skip the table ownership relationship.
                if table_dataset_id not in datasets_dict:
                    # TODO: error
                    print(f"[{SERVICE_NAME} {datetime.now()}] Dataset {table_dataset_id} from table not found in "
                          f"metadata service")
                    continue

                if table_data_type not in dataset_objects_dict[table_dataset_id]:
                    dataset_objects_dict[table_dataset_id][table_data_type] = []

                linked_field_sets: LinkedFieldSetList = [
                    lfs["fields"]
                    for lfs in datasets_dict[table_dataset_id].get("linked_field_sets", [])
                    if len(lfs["fields"]) > 1  # Only include useful linked field sets, i.e. 2+ fields
                ]

                dataset_join_query = join_query

                if dataset_join_query is None:
                    # Could re-return None; pass set of all data types to filter out combinations
                    dataset_join_query = _linked_field_sets_to_join_query(linked_field_sets,
                                                                          set(data_type_queries.keys()))

                if dataset_join_query is not None:  # still isn't None...
                    # TODO: Pre-filter data_type_results to avoid a billion index combinations - return specific set of
                    #  combos
                    # TODO: Allow passing a non-empty index fixation to search to save time and start somewhere
                    # TODO: Or should search filter the data object (including sub-arrays) as it goes, returning it at
                    #  the end?

                    # Combine the join query with data type queries to be able to link across fixed [item]s
                    for dt, q in data_type_queries.items():
                        dataset_join_query = ["#and", _augment_resolves(q, (dt, "[item]")), join_query]

                    print(f"[{SERVICE_NAME} {datetime.now()}] Generated join query: {dataset_join_query}", flush=True)

                    if table_data_type not in dataset_object_schema["properties"]:
                        # Fetch schema for data type if needed
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

                    # TODO: We should only fetch items that match including sub-items (e.g. limited calls) by using
                    #  all index combinations that match and combining them... something like that

                    dataset_objects_dict[table_dataset_id][table_data_type].extend((await peer_fetch(
                        client,
                        SOCKET_INTERNAL_URL,  # Use Unix socket resolver
                        f"api/{t['service_artifact']}/private/tables/{t['table_id']}/search",
                        request_body=json.dumps({"query": data_type_queries[table_data_type]}),
                        method="POST",
                        extra_headers=DATASET_SEARCH_HEADERS
                    ))["results"] if table_data_type in data_type_queries else [])

                else:
                    # Don't need to fetch results for joining; just check individual tables (which is much faster)
                    # using the public discovery endpoint.

                    if table_data_type in data_type_queries:
                        if await peer_fetch(
                            client,
                            SOCKET_INTERNAL_URL,  # Use Unix socket resolver
                            f"api/{t['service_artifact']}/tables/{t['table_id']}/search",
                            request_body=json.dumps({"query": data_type_queries[table_data_type]}),
                            method="POST",
                            extra_headers=DATASET_SEARCH_HEADERS
                        ):  # True return value, i.e. the query matched something
                            # Here, the array of 1 True is a dummy value to give a positive result
                            dataset_objects_dict[table_dataset_id][table_data_type] = [True]

                dataset_join_queries[table_dataset_id] = dataset_join_query

            print(f"[{SERVICE_NAME} {datetime.now()}] Done fetching individual service search results.", flush=True)

            # Aggregate datasets into results list if they satisfy the queries
            for dataset_id, data_type_results in dataset_objects_dict.items():  # TODO: Worker
                get_dataset_results(
                    data_type_queries,
                    dataset_join_queries[dataset_id],
                    data_type_results,
                    datasets_dict[dataset_id],
                    dataset_object_schema,
                    results
                )

            self.write({"results": results})

        except HTTPError as e:
            # Metadata service error
            print(f"[{SERVICE_NAME} {datetime.now()}] Error from service: {str(e)}", flush=True)  # TODO: Better message
            self.set_status(500)
            self.write(internal_server_error(f"Error from service: {str(e)}"))

        except (TypeError, ValueError, SyntaxError) as e:  # errors from query processing
            # TODO: Better / more compliant error message
            # TODO: Move these up?
            # TODO: Not guaranteed to be actually query-processing errors
            print(str(e))
            self.set_status(400)
            self.write(bad_request_error(f"Query processing error: {str(e)}"))  # TODO: Better message
