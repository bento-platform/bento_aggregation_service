from __future__ import annotations

import asyncio
import logging
import traceback

from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientResponseError
from bento_lib.search.queries import Query
from fastapi import APIRouter, Request, status
from fastapi.exceptions import HTTPException
from pydantic import BaseModel
from urllib.parse import urljoin

from bento_aggregation_service.config import Config, ConfigDependency
from bento_aggregation_service.http_session import HTTPSessionDependency
from bento_aggregation_service.logger import LoggerDependency
from bento_aggregation_service.service_manager import (
    ServiceManager,
    ServiceManagerDependency,
)

from ..dataset_search import run_search_on_dataset
from ..query_utils import service_request_headers, test_queries


__all__ = [
    "dataset_search_router",
]

dataset_search_router = APIRouter()


async def search_worker(
    # Input dataset list
    datasets: list[dict],
    # Input values
    dataset_object_schema: dict,
    join_query,
    data_type_queries,
    exclude_from_auto_join: tuple[str, ...],
    # Dependencies
    config: Config,
    http_session: ClientSession,
    logger: logging.Logger,
    service_manager: ServiceManager,
    headers: dict[str, str],
    # Flags
    include_internal_results: bool = False,
):
    async def _search_dataset(dataset: dict) -> tuple[str, dict[str, list] | None]:
        dataset_id = dataset["identifier"]
        try:
            dataset_results = await run_search_on_dataset(
                dataset_object_schema,
                dataset,
                join_query,
                data_type_queries,
                exclude_from_auto_join,
                include_internal_results,
                config,
                http_session,
                logger,
                service_manager,
                headers,
            )
            return dataset_id, dataset_results

        except ClientResponseError as e:  # Thrown from run_search_on_dataset
            # Metadata service error
            # TODO: Better message
            # TODO: Set error code outside worker?
            logger.error(f"Error from dataset search: {str(e)}")
            return dataset_id, None

    return {**asyncio.gather(*(_search_dataset(ds) for ds in datasets))}


class DatasetSearchRequest(BaseModel):
    # Format: {"data_type": ["#eq", ...]}
    data_type_queries: dict[str, Query]

    # Format: normal query, using data types for join conditions
    join_query: Query | None = None

    # Format: list of data types to use as part of a full-join-ish thing instead of an inner-join-ish thing
    exclude_from_auto_join: tuple[str, ...] = ()


@dataset_search_router.post("/dataset-search")
async def all_datasets_search_handler(
    request: Request,
    search_req: DatasetSearchRequest,
    config: ConfigDependency,
    http_session: HTTPSessionDependency,
    logger: LoggerDependency,
    service_manager: ServiceManagerDependency,
):
    try:
        # Try compiling each query to make sure it works. Any exceptions thrown will get caught below.
        test_queries(search_req.data_type_queries.values())
    except (TypeError, ValueError, SyntaxError) as e:  # errors from query processing
        # TODO: Better / more compliant error message
        err = f"Query processing error: {str(e)}"  # TODO: Better message
        logger.error(err)
        traceback.print_exc()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=err)

    results = []

    try:
        # TODO: Handle pagination
        # TODO: Why fetch projects instead of datasets? Is it to avoid "orphan" datasets? Is that even possible?

        logger.debug("fetching projects from Katsu")
        headers = service_request_headers(request=request)
        res = await http_session.get(
            urljoin(config.katsu_url, "api/projects"),
            headers=headers,
            raise_for_status=True,
        )

        projects = await res.json()

        datasets_dict: dict[str, dict] = {d["identifier"]: d for p in projects["results"] for d in p["datasets"]}

        dataset_object_schema = {"type": "object", "properties": {}}

        # Spawn workers to handle asynchronous requests to various datasets
        dataset_objects_dict = await search_worker(
            # dataset dictionaries:
            list(datasets_dict.values()),
            # search request / query-related:
            dataset_object_schema,
            search_req.join_query,
            search_req.data_type_queries,
            search_req.exclude_from_auto_join,
            # dependencies:
            config,
            http_session,
            logger,
            service_manager,
            headers,
        )

        logger.info("Done fetching individual service search results.")

        # Aggregate datasets into results list if they satisfy the queries
        for dataset_id, dataset_results in dataset_objects_dict.items():
            if len(dataset_results) > 0:
                d = datasets_dict[dataset_id]
                results.append({**d, "results": {}})

        return {"results": results}

    except ClientResponseError as e:
        # Metadata service error
        # TODO: Better message
        err = f"Error from service: {str(e)}"
        logger.error(err)
        # TODO: include traceback in error
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=err)


@dataset_search_router.post("/dataset-search/{dataset_id}")
async def dataset_search_handler(
    request: Request,
    search_req: DatasetSearchRequest,
    dataset_id: str,
    config: ConfigDependency,
    http_session: HTTPSessionDependency,
    logger: LoggerDependency,
    service_manager: ServiceManagerDependency,
):
    try:
        # Try compiling each query to make sure it works. Any exceptions thrown will get caught below.
        test_queries(search_req.data_type_queries.values())
    except (TypeError, ValueError, SyntaxError) as e:  # errors from query processing
        # TODO: Better / more compliant error message
        err = f"Query processing error: {str(e)}"
        traceback.print_exc()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=err)

    try:
        logger.debug(f"fetching dataset {dataset_id} from Katsu")
        headers = service_request_headers(request)
        res = await http_session.get(
            urljoin(config.katsu_url, f"api/datasets/{dataset_id}"),
            headers=headers,
            raise_for_status=True,
        )

        dataset = await res.json()

        # TODO: Handle dataset 404 properly

        dataset_object_schema = {"type": "object", "properties": {}}

        dataset_results = await run_search_on_dataset(
            dataset_object_schema,
            dataset,
            search_req.join_query,
            search_req.data_type_queries,
            search_req.exclude_from_auto_join,
            include_internal_results=True,
            config=config,
            http_session=http_session,
            logger=logger,
            service_manager=service_manager,
            headers=headers,
        )

        return {**dataset, **dataset_results}

    except ClientResponseError as e:
        # Metadata service error
        # TODO: Better message
        err = f"Error from service: {str(e)}"
        logger.error(err)
        traceback.print_exc()  # TODO: log instead of printing manually
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=err)
