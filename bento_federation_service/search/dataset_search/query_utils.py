from bento_lib.search.queries import convert_query_to_ast_and_preprocess, Query
from typing import Dict, Iterable, List, Optional, Tuple

from bento_federation_service.utils import get_request_json


__all__ = [
    "get_query_parts",
    "test_queries",
]


def get_query_parts(request_body: bytes) -> Tuple[Optional[Dict[str, Query]], Optional[Query], Tuple[str, ...]]:
    request = get_request_json(request_body)
    if request is None:
        return None, None, ()

    # Format: {"data_type": ["#eq", ...]}
    data_type_queries: Optional[Dict[str, Query]] = request.get("data_type_queries")

    # Format: normal query, using data types for join conditions
    join_query: Optional[Query] = request.get("join_query")

    # Format: list of data types to use as part of a full-join-ish thing instead of an inner-join-ish thing
    exclude_from_auto_join: Tuple[str] = request.get("exclude_from_auto_join", ())

    return data_type_queries, join_query, exclude_from_auto_join


def test_queries(queries: Iterable[Query]) -> None:
    """
    Throws an error if a query in the iterable cannot be compiled.
    :param queries: Iterable of queries to attempt compilation of.
    :return: None
    """
    for q in queries:
        # Try compiling each query to make sure it works.
        convert_query_to_ast_and_preprocess(q)


def print_tree(tabs, t):
    for i in t:
        if isinstance(i, list):
            print_tree(tabs + 1, i)
        else:
            print(f"{tabs*'   '}{i}")


def simple_resolve_tree(t, finalists: list):
    counter = 0
    for i in t:
        if isinstance(i, list):
            simple_resolve_tree(i, finalists)
        if counter == len(t) - 1 and (isinstance(i, str) or isinstance(i, int)):
            finalists.append(str(i))
        counter += 1
    return finalists


def pair_up_simple_list(t: List[List[str]]):
    counter = 0
    pairs = []
    for i in t:
        if counter % 2 == 0:
            pairs.append([t[counter], t[counter+1]])
        counter += 1
    return pairs


def rename_gohan_compatible(list_pairs):
    for p in list_pairs:
        if p[0] == "assembly_id":
            p[0] = "assemblyId"
        elif p[0] == "start":
            p[0] = "lowerBound"
        elif p[0] == "end":
            p[0] = "upperBound"
        elif p[0] == "genotype_type":
            p[0] = "genotype"


def prune_non_gohan_paramters(list_pairs):
    for p in list_pairs:
        if p[0] == "sample_id":
            list_pairs.remove(p)


def construct_gohan_query_params(ast: list, supplemental_args: List[List[str]]):
    # somehow convert AST to a simple list of lists/strings/ints
    # converted_ast = [] # temp

    # resolve simple key/value pairs
    simple_list = []
    simple_resolve_tree(ast, simple_list)

    # pair up simple list and concat with extra arg pairs
    pairs = pair_up_simple_list(simple_list) + supplemental_args
    # prune unnecessary paramers
    prune_non_gohan_paramters(pairs)
    # ensure gohan query param nameing convention matches up
    rename_gohan_compatible(pairs)

    return tuple(tuple(x) for x in pairs)
