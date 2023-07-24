from __future__ import annotations

import logging

from bento_lib.search.data_structure import check_ast_against_data_structure
from bento_lib.search.queries import convert_query_to_ast_and_preprocess, Query
from collections.abc import Iterable

from typing import Any, Optional

from bento_aggregation_service.config import Config


__all__ = [
    "process_dataset_results",
]


class Kept:
    def __init__(self, data: Any):
        # noinspection PyUnresolvedReferences
        self.data = data.data if isinstance(data, Kept) else data

    def __getitem__(self, item):
        return self.data[item]

    def __iter__(self):
        yield from self.data

    def __str__(self):
        return f"Kept <{str(self.data)}>"

    def __repr__(self):
        return str(self)


def _is_list(x: Any):
    return isinstance(x, list)


def _filter_kept(data_structure: Any, ic_path: list[str]) -> Any:
    """
    Goes through a data structure and only keeps array items that are tagged with the Kept class as they occur along the
    index combination path we're following. Recurses on every element of arrays.
    :param data_structure: The data structure to start following the index combination path at.
    :param ic_path: The index combination path elements to follow; e.g. biosamples.[item].id split by "." into a list.
    :return: The filtered data structure.
    """

    is_kept = isinstance(data_structure, Kept)

    if not ic_path:  # At the base level, so filter lists without recursing.
        if _is_list(data_structure) or (is_kept and _is_list(data_structure.data)):
            return [i for i in data_structure if isinstance(i, Kept)]

        return data_structure

    if _is_list(data_structure) or (is_kept and _is_list(data_structure.data)):
        return [Kept(_filter_kept(i.data, ic_path[1:])) for i in data_structure if isinstance(i, Kept)]

    ds = {
        **data_structure,
        ic_path[0]: _filter_kept(data_structure[ic_path[0]], ic_path[1:]),
    }

    if is_kept:
        return Kept(ds)

    return ds


def _base_strip_kept(data_structure: Any) -> Any:
    """
    Strips the Kept class off of a wrapped data structure if one exists.
    :param data_structure: The possibly-wrapped data structure.
    :return: The unwrapped data structure.
    """
    return data_structure.data if isinstance(data_structure, Kept) else data_structure


def _strip_kept(data_structure: Any, ic_path: list[str]) -> Any:
    """
    Goes through a data structure and strips any data wrapped in a Kept class as they occur along the index combination
    path we're following. Recurses on every element of arrays.
    :param data_structure: The data structure to start following the index combination path at.
    :param ic_path: The index combination path elements to follow; e.g. _root.biosamples.[item].id split by "."
    :return: The data structure, stripped of Kept wrapping.
    """

    data_structure = _base_strip_kept(data_structure)

    if not ic_path:  # At the base level, so strip off any if it's an array; otherwise do nothing.
        if _is_list(data_structure):
            return [_base_strip_kept(i) for i in data_structure]

        return data_structure

    # Otherwise, we have more to resolve; call the recursive Kept-stripping utility instead.

    if _is_list(data_structure):
        return [_strip_kept(i, ic_path[1:]) for i in data_structure]

    return {
        **data_structure,
        ic_path[0]: _strip_kept(data_structure[ic_path[0]], ic_path[1:]),
    }


def _filter_results_by_index_combinations(
    dataset_results: dict[str, list],
    index_combinations: tuple[dict, ...],
    ic_paths_to_filter: list[str],
) -> dict[str, list]:
    # TODO: This stuff is slow

    ic_paths_to_filter_set = set(ic_paths_to_filter)

    for index_combination in index_combinations:
        resolved_versions = {}

        for path, index in sorted(index_combination.items(), key=lambda pair: len(pair[0])):
            if path not in ic_paths_to_filter_set:
                continue

            path_array_parts = path.split(".[item]")

            resolved_path = ""
            current_path = ""
            for p in path_array_parts[:-1]:
                current_path += p
                resolved_path += resolved_versions[current_path]

            resolved_path += f"{path_array_parts[-1]}.[{index}]"
            resolved_versions[path] = resolved_path

        for resolved_path in resolved_versions.values():
            path_parts = resolved_path.split(".")[1:]
            ds: Any = dataset_results
            for pp in path_parts:
                arr = pp[0] == "["
                idx = int(pp[1:-1]) if arr else pp
                if arr:
                    ds[idx] = Kept(ds[idx]) if not isinstance(ds[idx], Kept) else ds[idx]
                ds = ds[idx]

    sorted_icps = sorted(ic_paths_to_filter, key=lambda icp: len(icp))

    for ic_path in sorted_icps:
        dataset_results = _filter_kept(dataset_results, ic_path.split(".")[1:])

    for ic_path in sorted_icps:
        dataset_results = _strip_kept(dataset_results, ic_path.split(".")[1:])

    return dataset_results


def _filter_results_by_index_combinations_if_set(
    dataset_results: dict[str, list],
    index_combinations: tuple[dict, ...] | None,
    ic_paths_to_filter: list[str],
) -> dict[str, list]:
    if index_combinations is None:
        return dataset_results

    return _filter_results_by_index_combinations(dataset_results, index_combinations, ic_paths_to_filter)


def process_dataset_results(
    data_type_queries: dict[str, Query],
    dataset_join_query: Query,
    dataset_results: dict[str, list],
    dataset: dict,
    dataset_object_schema: dict,
    include_internal_data: bool,
    # dependencies
    config: Config,
    logger: logging.Logger,
    # args w/ defaults
    ic_paths_to_filter: Optional[list[str]] = None,
    always_yield: bool = False,
):
    # TODO: Check dataset, table-level authorizations

    # dataset_results: dict of data types and corresponding table matches

    # TODO: Avoid re-compiling a fixed join query
    join_query_ast = convert_query_to_ast_and_preprocess(dataset_join_query) if dataset_join_query is not None else None

    logger.debug(f"Compiled join query: {join_query_ast}")

    # Truth-y if:
    #  - include_internal_data = False and check_ast_against_data_structure returns True
    #  - include_internal_data = True and check_ast_against_data_structure doesn't return an empty iterable
    ic = None
    if join_query_ast is not None:
        ic = check_ast_against_data_structure(
            join_query_ast, dataset_results, dataset_object_schema,
            internal=True,
            return_all_index_combinations=include_internal_data,
            secure_errors=not config.bento_debug,
            # Schema validation adds a lot of slowdown but helps debugging:
            skip_schema_validation=not config.bento_debug,
        )

        if isinstance(ic, Iterable):
            ic = tuple(ic)

    # Append result if:
    #  - No join query was specified,
    #      and there is at least one matching table present in the dataset,
    #      and only one data type is being searched; or
    #  - A join query is present and evaluates to True against the dataset.
    # Need to mark this query as internal, since the federation service "gets" extra privileges here
    # (joined data isn't explicitly exposed.)
    # TODO: Derive this from before; don't re-calculate
    # TODO: This is a bad solution - see elsewhere where this is discussed
    included_data_types = set(dt for dt, q in data_type_queries.items() if q is not True)
    # TODO: Optimize by not fetching if the query isn't going anywhere (i.e. no linked field sets, 2+ data types)
    if ((join_query_ast is None and any(len(dtr) > 0 for dtr in dataset_results.values())
         and len(included_data_types) == 1) or (join_query_ast is not None and ic)):
        yield {
            **dataset,
            **({"results": _filter_results_by_index_combinations_if_set(dataset_results, ic, ic_paths_to_filter)}
               if include_internal_data else {}),
        }  # TODO: Make sure all information here is public-level if include_internal_data is False.

    if always_yield:  # If true, yield even for empty search results
        yield {
            **dataset,
            **({"results": {dt: [] for dt in data_type_queries}} if include_internal_data else {}),
        }
