import itertools
import json
import logging
from urllib.parse import urljoin

from aiohttp import ClientSession
from bento_lib.search.queries import Query

from bento_aggregation_service.config import Config
from bento_aggregation_service.search import query_utils
from bento_aggregation_service.service_manager import ServiceManager


__all__ = [
    "run_search_on_dataset",
]


FieldSpec = list[str]
DataTypeAndField = tuple[str, FieldSpec]
DictOfDataTypesAndFields = dict[str, FieldSpec]
LinkedFieldSetList = list[DictOfDataTypesAndFields]


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


def _linked_field_sets_to_join_query(
    linked_field_sets: LinkedFieldSetList,
    data_type_set: set[str],
    logger: logging.Logger,
) -> Query | None:
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
        logger.debug(f"No useful ID pairs present ({data_type_set=})")
        return None  # TODO: Somehow tell the user no join was applied or return NO RESULTS if None and 2+ data types?

    if len(linked_field_sets) == 1:
        return _linked_field_set_to_join_query_rec(pairs)

    # Recurse on the next linked field set, building up the #and query.

    return [
        "#and",
        _linked_field_set_to_join_query_rec(pairs),
        _linked_field_sets_to_join_query(linked_field_sets[1:], data_type_set, logger),
    ]


def _get_dataset_linked_field_sets(dataset: dict) -> LinkedFieldSetList:
    return [
        lfs["fields"]
        for lfs in dataset.get("linked_field_sets", [])
        if len(lfs["fields"]) > 1  # Only include useful linked field sets, i.e. 2+ fields
    ]


def _augment_resolves(query: Query, prefix: tuple[str, ...]) -> Query:
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


def _combine_join_and_data_type_queries(
    join_query: Query | None,
    data_type_queries: dict[str, Query],
    logger: logging.Logger,
) -> Query | None:
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


def _get_array_resolve_paths(query: Query) -> list[str]:
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


async def _run_search(
    dataset_id: str,
    dataset_join_query: Query,
    data_type_queries: dict[str, Query],
    target_linked_fields: DictOfDataTypesAndFields | None,
    include_internal_results: bool,
    dataset_object_schema: dict,
    dataset_linked_fields_results: list[set],
    config: Config,
    http_session: ClientSession,
    service_manager: ServiceManager,
    headers: dict[str, str]
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

    data_type_entries = await service_manager.fetch_data_types(headers=headers)

    for data_type in data_type_queries.keys():
        # True is a value used instead of the AST string to return the whole
        # datatype related data without any filtering. For perf. reasons
        # this is unneeded when doing a search
        is_querying_data_type = not data_type_queries[data_type] is True

        # Don't need to fetch results for joining if the join query is None; just check
        # individual tables (which is much faster) using the public discovery endpoint.
        private = dataset_join_query is not None or include_internal_results

        data_type_entry = data_type_entries[data_type]

        if dataset_join_query is not None and data_type not in dataset_object_schema["properties"]:
            # Since we have a join query, we need to create a superstructure containing
            # different search results and a schema to match.

            # Set schema for data type if needed
            dataset_object_schema["properties"][data_type] = {
                "type": "array",
                "items": data_type_entry.data_type_listing.item_schema if is_querying_data_type else {}
            }

        # If data type is not being queried, its results are irrelevant
        if not is_querying_data_type:
            continue

        # Setup up search pre-requisites
        # - defaults:
        search_path = f"{data_type_entry.service_base_url}/private/datasets/{dataset_id}/search"
        url_args = [
            ("query", json.dumps(data_type_queries[data_type])),
            ("data_type", data_type)
        ]

        # WIP: if no linked fields have been defined, the search can still be made?
        if target_linked_fields:
            url_args.extend((
                ("field", json.dumps(target_linked_fields[data_type])),
                ("output", "values_list"),
            ))

        # - Gohan compatibility
        # TODO: formalize/clean this up
        # TODO: cleanup note: json.loads(json.dumps()) seems dubious, url_args was a tuple and becomes a list
        is_using_gohan = config.use_gohan and data_type == "variant"

        if is_using_gohan:
            # reset path_fragment:
            search_path = urljoin(data_type_entry.service_base_url, "/api/gohan/variants/get/by/variantId")

            # reset url_args:
            # - construct based on search query
            supplemental_url_args = [("getSampleIdsOnly", "true")]
            # - transform custom Query to list of lists to simplify
            #   the gohan query parameter construction
            reloaded_converted = json.loads(json.dumps(data_type_queries[data_type]))
            # - generate query parameters from list of query tree objects
            url_args = query_utils.construct_gohan_query_params(reloaded_converted, supplemental_url_args)

        # Run the search
        res = await http_session.get(search_path, params=url_args, headers=headers)
        r = await res.json()

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


def _get_linked_field_for_query(
    linked_field_sets: LinkedFieldSetList,
    data_type_queries: dict[str, Query]
) -> DictOfDataTypesAndFields | None:
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
    data_type_queries: dict[str, Query],
    exclude_from_auto_join: tuple[str, ...],
    include_internal_results: bool,
    config: Config,
    http_session: ClientSession,
    logger: logging.Logger,
    service_manager: ServiceManager,
    headers: dict[str, str]
) -> dict[str, list]:
    linked_field_sets: LinkedFieldSetList = _get_dataset_linked_field_sets(dataset)
    target_linked_field: DictOfDataTypesAndFields | None = _get_linked_field_for_query(
        linked_field_sets, data_type_queries)

    dataset_id: str = dataset["identifier"]

    logger.debug(f"Linked field sets: {linked_field_sets}")
    logger.debug(f"Dataset: {dataset_id}")

    # Set of data types excluded from building the join query
    # exclude_from_auto_join: a list of data types that will get excluded from the join query even if there are
    #   tables present, effectively functioning as a 'full join' where the excluded data types are not guaranteed
    #   to match
    excluded_data_types: set[str] = set(exclude_from_auto_join)

    if excluded_data_types:
        logger.debug(f"Pre-excluding data types from join: {excluded_data_types}")

    if join_query is None:
        # Could re-return None; pass set of all data types (keys of the data type queries)
        # to filter out combinations
        join_query = _linked_field_sets_to_join_query(
            linked_field_sets, set(data_type_queries) - excluded_data_types, logger)

    # Combine the join query with the data type queries, fixing resolves to be consistent
    join_query = _combine_join_and_data_type_queries(join_query, data_type_queries, logger)

    # ------------------------- Start running search across data types -------------------------

    # Special case: when the query contains constraints only on phenopackets,
    # searching for the matching biosamples id first will exclude any phenopacket
    # for which no biosample is defined. In that case, querying directly
    # for phenopackets is the correct way of collecting the data.
    # When there is a combination of field, the query is the SQL equivalent to an
    # INNER JOIN: when there is no join possible between the tables, there is
    # no result that can be displayed.

    # TODO: This whole thing is a hack and should be removed

    # In the next flag, the list comprehension with filtering for lists is used
    # to take care of the case where a data_type is associated with the value
    # `True`, as no query is performed in that case.
    query_is_phenopacket_only = (
        "phenopacket" in data_type_queries and
        isinstance(data_type_queries["phenopacket"], list) and
        len([k for k, val in data_type_queries.items() if isinstance(val, list)]) == 1
    )

    # TODO: no weird override logic for specific data types!

    if query_is_phenopacket_only:
        request_body = {
            "data_type": "phenopacket",
            "query": data_type_queries["phenopacket"],
            "output": "bento_search_result"
        }

    else:
        dataset_linked_fields_results: list[set] = []

        await _run_search(
            dataset_id,
            join_query,
            data_type_queries,
            target_linked_field,
            include_internal_results,
            dataset_object_schema,
            dataset_linked_fields_results,
            config,
            http_session,
            service_manager,
            headers
        )

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
            "data_type": "phenopacket",
            "query": [
                "#in",
                ["#resolve", *target_linked_field["phenopacket"]],
                ["#list", *results],
            ],
            "output": "bento_search_result"
        }

    # TODO: what if no phenopacket service?
    # Make this code more generic... Maybe, `format` and final `data-type` should
    # be extracted from the request. If these are absent, then fetch results from
    # every service.

    # POST required to avoid exceeding GET parameters limit size with the list of ids
    r = await http_session.post(
        f"{config.katsu_url.rstrip('/')}/private/datasets/{dataset_id}/search",
        json=request_body,
        headers=headers,
    )
    return await r.json()
