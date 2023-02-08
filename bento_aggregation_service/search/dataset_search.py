import itertools
import json
import tornado.gen

from bento_lib.search.queries import Query
from tornado.httpclient import AsyncHTTPClient
from tornado.queues import Queue

from typing import Dict, List, Optional, Set, Tuple

from bento_aggregation_service.constants import WORKERS, USE_GOHAN
from bento_aggregation_service.logger import logger
from bento_aggregation_service.search import query_utils
from bento_aggregation_service.utils import bento_fetch, iterable_to_queue
from .constants import DATASET_SEARCH_HEADERS


__all__ = [
    "run_search_on_dataset",
]


FieldSpec = List[str]
DataTypeAndField = Tuple[str, FieldSpec]
DictOfDataTypesAndFields = Dict[str, FieldSpec]
LinkedFieldSetList = List[DictOfDataTypesAndFields]


def _linked_fields_to_join_query_fragment(field_1: DataTypeAndField, field_2: DataTypeAndField) -> Query:
    """
    Given two tuples of (data type, field path) representing the fields to join on,
    return an equality expression for the fields.
    :param field_1: The first field definition to join on.
    :param field_2: The second field definition to join on.
    :return: The constructed join equality expression.
    """
    return ["#eq", ["#resolve", field_1[0], "[item]", *field_1[1]], ["#resolve", field_2[0], "[item]", *field_2[1]]]


def _linked_field_set_to_join_query_rec(pairs: tuple) -> Query:
    """
    Recursive function to generate a join query between pairs of fields
    """
    if len(pairs) == 1:
        return _linked_fields_to_join_query_fragment(*pairs[0])

    return ["#and",
            _linked_fields_to_join_query_fragment(*pairs[0]),
            _linked_field_set_to_join_query_rec(pairs[1:])]


def _linked_field_sets_to_join_query(linked_field_sets: LinkedFieldSetList, data_type_set: Set[str]) -> Optional[Query]:
    """
    Recursive function to add joins between linked fields.
    It recurses through the sets of linked fields.
    """
    if len(linked_field_sets) == 0:
        logger.debug("No useful linked field sets present")
        return None

    # TODO: This blows up combinatorially, oh well.

    # For each linked field set, make all relevant pairs of fields (here,
    # relevant means the data set is in the query.)

    # This does not deduplicate pairs if there are overlaps between the linked
    # field sets, so a bit of extra work might be performed.

    # Just take the first linked field set, since we are recursing later.

    pairs = tuple(
        p for p in itertools.combinations(linked_field_sets[0].items(), 2)
        if p[0][0] in data_type_set and p[1][0] in data_type_set)

    if len(pairs) == 0:
        logger.debug("No useful ID pairs present")
        return None  # TODO: Somehow tell the user no join was applied or return NO RESULTS if None and 2+ data types?

    if len(linked_field_sets) == 1:
        return _linked_field_set_to_join_query_rec(pairs)

    # Recurse on the next linked field set, building up the #and query.

    return ["#and",
            _linked_field_set_to_join_query_rec(pairs),
            _linked_field_sets_to_join_query(linked_field_sets[1:], data_type_set)]


def _get_dataset_linked_field_sets(dataset: dict) -> LinkedFieldSetList:
    return [
        lfs["fields"]
        for lfs in dataset.get("linked_field_sets", [])
        if len(lfs["fields"]) > 1  # Only include useful linked field sets, i.e. 2+ fields
    ]


def _augment_resolves(query: Query, prefix: Tuple[str, ...]) -> Query:
    """
    Recursive function that prepends every #resolve list in the query AST
    with the given prefix (a data-type such as `phenopacket`).
    This is used for "in memory" joins
    """
    if not isinstance(query, list) or len(query) == 0 or len(query[0]) == 0 or query[0][0] != "#":
        return query

    if query[0] == "#resolve":
        return ["#resolve", *prefix, *query[1:]]

    return [query[0], *(_augment_resolves(q, prefix) for q in query[1:])]


def _combine_join_and_data_type_queries(join_query: Query, data_type_queries: Dict[str, Query]) -> Query:
    if join_query is None:
        return None

    # Otherwise,  still isn't None...
    # TODO: Pre-filter data_type_results to avoid a billion index combinations - return specific set of combos
    # TODO: Allow passing a non-empty index fixation to search to save time and start somewhere
    # TODO: Or should search filter the data object (including sub-arrays) as it goes, returning it at the end?

    # Combine the join query with data type queries to be able to link across fixed [item]s
    for dt, q in data_type_queries.items():
        join_query = ["#and", _augment_resolves(q, (dt, "[item]")), join_query]

    logger.debug(f"Generated join query: {join_query}")

    return join_query


def _get_array_resolve_paths(query: Query) -> List[str]:
    """
    Collect string representations array resolve paths without the trailing [item] resolution from a query. This can
    facilitate determining which index combinations will appear; and can be used as a step in filtering results by
    index combination.
    :param query: Query to collect array resolves from
    :return: List of index combination-compatible array resolve paths.
    """

    if isinstance(query, list) and len(query) > 1:
        r = []

        if query[0] == "#resolve":  # Resolve expression; items make up a resolve path
            r = []
            path = "_root"
            for ri in query[1:]:
                if ri == "[item]":
                    r.append(path)
                path = f"{path}.{ri}"

        else:  # Expression where items are other expressions/literals
            for e in query[1:]:
                r.extend(_get_array_resolve_paths(e))

        return r

    return []


async def _fetch_table_definition_worker(table_queue: Queue, auth_header: Optional[str],
                                         table_ownerships_and_records: List[Tuple[dict, dict]]):
    """
    Impure function.
    Fetches the searcheable schema for each table by querying their corresponding
    service.
    The result is stored in the table_ownerships_and_records var as a tuple of
    table_ownership (table_id and hosting service), and table definition (data_type, schema,...)
    """
    client = AsyncHTTPClient()

    async for t in table_queue:
        if t is None:
            # Exit signal
            return

        service_kind = t["service_artifact"]  # TODO: this should eventually be migrated to service_kind or something...

        try:
            # - Gohan compatibility
            # TODO: formalize/clean this up
            if USE_GOHAN and service_kind == "variant":
                service_kind = "gohan"

            # Setup up pre-requisites
            url = f"api/{service_kind}/tables/{t['table_id']}"

            logger.debug(f"Table URL fragment: {url}")

            # TODO: Don't fetch schema except for first time?
            table_record = await bento_fetch(
                client,
                url,
                method="GET",
                auth_header=auth_header,  # Required, otherwise may hit a 403 error
                extra_headers=DATASET_SEARCH_HEADERS
            )

            if isinstance(table_record, dict):
                table_ownerships_and_records.append((t, table_record))
            else:
                logger.error(f"Encountered malformatted table record from {service_kind} (will skip): {table_record}")

        except Exception as e:
            logger.error(f"Encountered error while trying to fetch table record from {service_kind}: {e}")

        finally:
            table_queue.task_done()


async def _table_search_worker(
    table_queue: Queue,
    dataset_join_query: Query,
    data_type_queries: Dict[str, Query],
    target_linked_fields: Optional[DictOfDataTypesAndFields],
    include_internal_results: bool,
    auth_header: Optional[str],
    dataset_object_schema: dict,
    dataset_linked_fields_results: List[set],
):
    """
    Impure async function.
    The following arguments are mutated by the function:
    - dataset_object_schema
    - dataset_linked_fields_results
    WARNING: I have tried to use a set() and flags to make the intersections
    between the results returned by each async function. It led to unnexpected
    results due to the fact that each async run seems to have its own context
    and not share a common state with the other concurrent runs. It looks like
    this is only resolved in the context of the caller of this function after
    the queue of workers has emptied. So, change this at your own risk and make
    sure to test.
    Note: could we use a pure function pattern here and have each worker actually
    return a value. The caller would have to await the return value from all the
    workers, but we would avoid the issue of shared objects which look like they
    are not shared reference but rely on intermediary copies (not sure about what
    the issue is, but it looks "dangerous")
    """
    client = AsyncHTTPClient()

    async for table_pair in table_queue:
        if table_pair is None:
            # Exit signal
            return

        try:
            table_ownership, table_record = table_pair
            table_data_type = table_record["data_type"]
            # True is a value used instead of the AST string to return the whole
            # datatype related data without any filtering. For perf. reasons
            # this is unneeded when doing a search
            is_querying_data_type = (table_data_type in data_type_queries
                                     and not data_type_queries[table_data_type] is True)

            # Don't need to fetch results for joining if the join query is None; just check
            # individual tables (which is much faster) using the public discovery endpoint.
            private = dataset_join_query is not None or include_internal_results

            if dataset_join_query is not None and table_data_type not in dataset_object_schema["properties"]:
                # Since we have a join query, we need to create a superstructure containing
                # different search results and a schema to match.

                # Set schema for data type if needed
                dataset_object_schema["properties"][table_data_type] = {
                    "type": "array",
                    "items": table_record["schema"] if is_querying_data_type else {}
                }

            # If data type is not being queried, its results are irrelevant
            if not is_querying_data_type:
                continue

            # Setup up search pre-requisites
            # - defaults:
            path_fragment = (
                f"api/{table_ownership['service_artifact']}{'/private' if private else ''}/tables"
                f"/{table_record['id']}/search"
            )
            url_args = (("query", json.dumps(data_type_queries[table_data_type])),)

            # WIP: if no linked fields have been defined, the search can still be made?
            if target_linked_fields:
                url_args += (("field", json.dumps(target_linked_fields[table_data_type])),
                             ("output", "values_list"))

            # - Gohan compatibility
            # TODO: formalize/clean this up
            # TODO: cleanup note: json.loads(json.dumps()) seems dubious, url_args was a tuple and becomes a list
            is_using_gohan = USE_GOHAN and table_ownership["service_artifact"] == "gohan"
            if is_using_gohan:
                # reset path_fragment:
                path_fragment = "api/gohan/variants/get/by/variantId"

                # reset url_args:
                # - construct based on search query
                supplemental_url_args = [["getSampleIdsOnly", "true"]]
                # - transform custom Query to list of lists to simplify
                #   the gohan query parameter construction
                tmpjson = json.dumps({"tmpkey": data_type_queries[table_data_type]})
                reloaded_converted = json.loads(tmpjson)["tmpkey"]
                # - generate query parameters from list of query tree objects
                gohan_query_params = query_utils.construct_gohan_query_params(reloaded_converted, supplemental_url_args)
                url_args = gohan_query_params

            # Run the search
            r = await bento_fetch(
                client,
                path_fragment=path_fragment,
                url_args=url_args,
                method="GET",
                auth_header=auth_header,  # Required in some cases to not get a 403
                extra_headers=DATASET_SEARCH_HEADERS,
            )

            if private:
                ids = r["results"]
                if is_using_gohan:
                    # the gohan results object has to be flattened to a list of sample_id from
                    # [{
                    #   "assembly":...,
                    #   "calls": [{
                    #               "sample_id": "HG00106",
                    #               "genotype_type": "HETEROZYGOUS"
                    #             },...
                    #           ]
                    # },...]
                    ids = [
                        call["sample_id"]
                        for r in ids
                        for call in r["calls"]
                    ]
                # We have a results array to account for
                results = {id_ for id_ in ids if id_ is not None}
            else:
                # Here, the array of 1 True is a dummy value to give a positive result
                results = {r} if r else set()

            dataset_linked_fields_results.append(results)

        finally:
            table_queue.task_done()


def _get_linked_field_for_query(
    linked_field_sets: LinkedFieldSetList,
    data_type_queries: Dict[str, Query]
) -> Optional[DictOfDataTypesAndFields]:
    """
    Given the linked field sets that are defined for a given Dataset, and a
    query definition, returns the first set of linked fields that is
    suitable for performing the joins between the data_types.
    In a typical Bento instance, only one linked field set is defined (over
    biosamples id), but this function makes this choice in an independent way.
    """
    dt = set(data_type_queries.keys())
    for linked_fields in linked_field_sets:
        if dt.issubset(linked_fields.keys()):
            return linked_fields
    return None


async def run_search_on_dataset(
    dataset_object_schema: dict,
    dataset: dict,
    join_query: Query,
    data_type_queries: Dict[str, Query],
    exclude_from_auto_join: Tuple[str, ...],
    include_internal_results: bool,
    auth_header: Optional[str] = None,
) -> Dict[str, list]:
    linked_field_sets: LinkedFieldSetList = _get_dataset_linked_field_sets(dataset)
    target_linked_field: Optional[DictOfDataTypesAndFields] = _get_linked_field_for_query(
        linked_field_sets, data_type_queries)

    logger.debug(f"Linked field sets: {linked_field_sets}")
    logger.debug(f"Dataset: {dataset['id']}")

    # Pairs of table ownership records, from the metadata service, and table properties,
    # from each data service to which the table belongs
    table_ownerships_and_records: List[Tuple[dict, dict]] = []

    table_ownership_queue = iterable_to_queue(dataset["table_ownership"])   # queue containing table ids

    table_definition_workers = tornado.gen.multi([
        _fetch_table_definition_worker(table_ownership_queue, auth_header, table_ownerships_and_records)
        for _ in range(WORKERS)
    ])
    await table_ownership_queue.join()

    try:
        logger.debug(f"Table ownership and records: {table_ownerships_and_records}")

        table_data_types: Set[str] = {t[1]["data_type"] for t in table_ownerships_and_records}

        # Set of data types excluded from building the join query
        # exclude_from_auto_join: a list of data types that will get excluded from the join query even if there are
        #   tables present, effectively functioning as a 'full join' where the excluded data types are not guaranteed
        #   to match
        excluded_data_types: Set[str] = set(exclude_from_auto_join)

        if excluded_data_types:
            logger.debug(f"Pre-excluding data types from join: {excluded_data_types}")

        for dt, dt_q in filter(lambda dt2: dt2[0] not in table_data_types, data_type_queries.items()):
            # If there are no tables of a particular data type, we don't get the schema. If this
            # happens, return no results unless the query is hard-coded to be True, in which
            # case put in a fake schema.
            # TODO: Come up with something more elegant/intuitive here - a way to resolve data types?
            # TODO: This may sometimes return the wrong result - should check for resolves instead

            # This CANNOT be simplified to "if not dt_q:"; other truth-y values don't have the
            # same meaning (sorry Guido).
            if dt_q is not True:
                return {dt2: [] for dt2 in data_type_queries}

            # Give it a boilerplate array schema and result set; there won't be anything there anyway
            dataset_object_schema["properties"][dt] = {"type": "array"}
            excluded_data_types.add(dt)

            logger.debug(f"Excluding data type from join: {dt}")

        if join_query is None:
            # Could re-return None; pass set of all data types (keys of the data type queries)
            # to filter out combinations
            join_query = _linked_field_sets_to_join_query(
                linked_field_sets, set(data_type_queries) - excluded_data_types)

        # Combine the join query with the data type queries, fixing resolves to be consistent
        join_query = _combine_join_and_data_type_queries(join_query, data_type_queries)

    finally:
        # Trigger exit for all table definition workers
        for _ in range(WORKERS):
            table_ownership_queue.put_nowait(None)

        # Wait for table definition workers to exit
        await table_definition_workers

    # ------------------------- Start running search on tables -------------------------

    # Special case: when the query contains constraints only on phenopackets,
    # searching for the matching biosamples id first will exclude any phenopacket
    # for which no biosample is defined. In that case, querying directly
    # for phenopackets is the correct way of collecting the data.
    # When there is a combination of field, the query is the SQL equivalent to an
    # INNER JOIN: when there is no join possible between the tables, there is
    # no result that can be displayed.

    # In the next flag, the list comprehension with filtering for lists is used
    # to take care of the case where a data_type is associated with the value
    # `True`, as no query is performed in that case.
    query_is_phenopacket_only = (
        "phenopacket" in data_type_queries and
        isinstance(data_type_queries["phenopacket"], list) and
        len([k for k, val in data_type_queries.items() if isinstance(val, list)]) == 1
    )

    if query_is_phenopacket_only:
        request_body = {
            "query": data_type_queries["phenopacket"],
            "output": "bento_search_result"
        }

    else:
        dataset_linked_fields_results: List[set] = []

        table_pairs_queue = iterable_to_queue(table_ownerships_and_records)

        table_search_workers = tornado.gen.multi([
            _table_search_worker(
                table_pairs_queue,
                join_query,
                data_type_queries,
                target_linked_field,
                include_internal_results,
                auth_header,
                dataset_object_schema,
                dataset_linked_fields_results,
            )
            for _ in range(WORKERS)
        ])

        await table_pairs_queue.join()

        # Trigger exit for all table search workers
        for _ in range(WORKERS):
            table_pairs_queue.put_nowait(None)

        # Wait for table search workers to exit
        await table_search_workers

        # Compute the intersection between the sets of results
        results = dataset_linked_fields_results[0]
        for r in dataset_linked_fields_results:
            results.intersection_update(r)

        if not include_internal_results:
            return {
                "results": list(results),
            }

        # edge case: no result, no extra query
        if len(results) == 0:
            return {
                "results": []
            }

        request_body = {
            "query": [
                "#in",
                ["#resolve", *target_linked_field["phenopacket"]],
                ["#list", *results],
            ],
            "output": "bento_search_result"
        }

    table_id = next((t[1]["id"] for t in table_ownerships_and_records if t[1]["data_type"] == "phenopacket"), None)

    # TODO: what if no phenopacket service?
    # Make this code more generic... Maybe, `format` and final `data-type` should
    # be extracted from the request. If these are absent, then fetch results from
    # every service.
    r = await bento_fetch(
        AsyncHTTPClient(),
        path_fragment=f"api/metadata/private/tables/{table_id}/search",
        request_body=json.dumps(request_body),
        method="POST",  # required to avoid exceeding GET parameters limit size with the list of ids
        auth_header=auth_header,  # Required in some cases to not get a 403
        extra_headers=DATASET_SEARCH_HEADERS,
    )
    return r
