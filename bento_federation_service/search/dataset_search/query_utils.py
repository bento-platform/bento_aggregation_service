from bento_lib.search.queries import convert_query_to_ast_and_preprocess, Query
from typing import Any, Dict, Iterable, Optional, Tuple

from bento_federation_service.utils import get_request_json


__all__ = [
    "get_query_parts",
    "test_queries",
]


def get_query_parts(request_body: bytes) -> Tuple[Optional[Dict[str, Query]], Optional[Query],
                                                  Tuple[str, ...], Dict[str, Dict[str, Any]]]:
    request = get_request_json(request_body)
    if request is None:
        return None, None, (), {}

    # Format: {"data_type": ["#eq", ...]}
    data_type_queries: Optional[Dict[str, Query]] = request.get("data_type_queries")

    # Format: normal query, using data types for join conditions
    join_query: Optional[Query] = request.get("join_query")

    # Format: list of data types to use as part of a full-join-ish thing instead of an inner-join-ish thing
    exclude_from_auto_join: Tuple[str] = request.get("exclude_from_auto_join", ())

    # Format: dict of arguments to pass via URL to the nested data type search queries
    # e.g. a list of fields to return in response (Katsu)
    #      {"phenopackets": {"fields": ["id", "subject", "biosamples", "diseases"]}}
    data_type_args: Dict[str, Dict[str, Any]] = request.get("data_type_args", {})

    return data_type_queries, join_query, exclude_from_auto_join, data_type_args


def test_queries(queries: Iterable[Query]) -> None:
    """
    Throws an error if a query in the iterable cannot be compiled.
    :param queries: Iterable of queries to attempt compilation of.
    :return: None
    """
    for q in queries:
        # Try compiling each query to make sure it works.
        convert_query_to_ast_and_preprocess(q)
