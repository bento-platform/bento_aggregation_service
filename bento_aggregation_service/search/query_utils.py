from __future__ import annotations

from bento_lib.search.queries import convert_query_to_ast_and_preprocess, Query
from fastapi import Request
from typing import Iterable


__all__ = [
    "service_request_headers",
    "test_queries",
]


def forward_auth_if_available(request: Request) -> dict[str, str]:
    auth = request.headers.get("Authorization")
    return {
        **({"Authorization": auth} if auth is not None else {}),
    }


def service_request_headers(request: Request) -> dict[str, str]:
    return {
        **forward_auth_if_available(request),
        "Content-Type": "application/json",
    }


def test_queries(queries: Iterable[Query]) -> None:
    """
    Throws an error if a query in the iterable cannot be compiled.
    :param queries: Iterable of queries to attempt compilation of.
    :return: None
    """
    for q in queries:
        # Try compiling each query to make sure it works.
        convert_query_to_ast_and_preprocess(q)


def simple_resolve_tree(t, finalists: list[str]) -> list[str]:
    counter = 0
    for i in t:
        if isinstance(i, list):
            simple_resolve_tree(i, finalists)
        if counter == len(t) - 1 and (isinstance(i, str) or isinstance(i, int)):
            finalists.append(str(i))
        counter += 1
    return finalists


def pair_up_simple_list(t: list[str]) -> list[list[str]]:
    counter = 0
    pairs = []
    for _ in t:
        if counter % 2 == 0:
            pairs.append([t[counter], t[counter + 1]])
        counter += 1
    return pairs


def rename_gohan_compatible(list_pairs: list[list[str]]):
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


def construct_gohan_query_params(ast: list, supplemental_args: list[tuple[str, str]]):
    # somehow convert AST to a simple list of lists/strings/ints
    # converted_ast = [] # temp

    # resolve simple key/value pairs
    simple_list: list[str] = []
    simple_resolve_tree(ast, simple_list)

    # pair up simple list and concat with extra arg pairs
    pairs = pair_up_simple_list(simple_list) + supplemental_args
    # prune unnecessary paramers
    prune_non_gohan_paramters(pairs)
    # ensure gohan query param nameing convention matches up
    rename_gohan_compatible(pairs)

    return tuple(tuple(x) for x in pairs)
