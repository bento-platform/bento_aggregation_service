from __future__ import annotations

from aiohttp.client_exceptions import ClientResponseError
from bento_lib.search.queries import Query
from fastapi import APIRouter, Request, status
from fastapi.exceptions import HTTPException
from pydantic import BaseModel
from urllib.parse import urljoin

from bento_aggregation_service.authz import authz_middleware
from bento_aggregation_service.config import ConfigDependency
from bento_aggregation_service.http_session import HTTPSessionDependency
from bento_aggregation_service.logger import LoggerDependency
from bento_aggregation_service.service_manager import ServiceManagerDependency

from ..dataset_search import run_search_on_dataset
from ..query_utils import service_request_headers, test_queries


__all__ = [
    "dataset_search_router",
]

dataset_search_router = APIRouter()


class DatasetSearchRequest(BaseModel):
    # Format: {"data_type": ["#eq", ...]}
    data_type_queries: dict[str, Query]

    # Format: normal query, using data types for join conditions
    join_query: Query | None = None

    # Format: list of data types to use as part of a full-join-ish thing instead of an inner-join-ish thing
    exclude_from_auto_join: tuple[str, ...] = ()


@dataset_search_router.post("/dataset-search/{dataset_id}", dependencies=[authz_middleware.dep_public_endpoint()])
async def dataset_search_handler(
    request: Request,
    search_req: DatasetSearchRequest,
    dataset_id: str,
    config: ConfigDependency,
    http_session: HTTPSessionDependency,
    logger: LoggerDependency,
    service_manager: ServiceManagerDependency,
):
    """
    Executes a search on the specified data types of a dataset in the Bento instance. Authorization is open because
    requests are more-or-less immediately proxied to data services with the authorization header. These services will
    then check access themselves, and these searches are all-or-nothing (no censoring fallback) so if we get a 403 from
    one service, the whole request will fail.
    :param request: FastAPI Request object
    :param search_req: Search request object (see above model definition: DatasetSearchRequest)
    :param dataset_id: Dataset ID from URL
    :param config: Injected service configuration instance
    :param http_session: Injected shared aiohttp session instance
    :param logger: Injected structlog.stdlib.BoundLogger instance
    :param service_manager: Injected Bento service manager singleton instance
    :return:
    """

    # Bind request parameters to all logging done inside this request handler
    logger = logger.bind(dataset_id=dataset_id, search_req=search_req)

    try:
        # Try compiling each query to make sure it works. Any exceptions thrown will get caught below.
        test_queries(search_req.data_type_queries.values())
    except (TypeError, ValueError, SyntaxError) as e:  # errors from query processing
        # TODO: Better / more compliant error message
        await logger.aexception("query processing error", exc_info=e)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Query processing error: {str(e)}")

    try:
        await logger.adebug("fetching dataset from Katsu")
        headers = service_request_headers(request)

        async with http_session.get(
            urljoin(config.katsu_url, f"api/datasets/{dataset_id}"),
            headers=headers,
            raise_for_status=True,
        ) as res:
            dataset = await res.json()

        # TODO: Handle dataset 404 properly

        dataset_object_schema = {"type": "object", "properties": {}}

        dataset_results = await run_search_on_dataset(
            dataset_object_schema,
            dataset,
            search_req.join_query,
            search_req.data_type_queries,
            search_req.exclude_from_auto_join,
            # dependencies:
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
        await logger.aexception("error from service", exc_info=e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error from service: {str(e)}")
