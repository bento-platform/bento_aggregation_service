import sys
import tornado.gen
import traceback

from bento_lib.responses.errors import bad_request_error, internal_server_error
from bento_lib.search.queries import Query
from datetime import datetime
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.queues import Queue
from tornado.web import RequestHandler

from typing import Dict, Optional, Tuple

from bento_federation_service.constants import CHORD_URL, SERVICE_NAME, WORKERS
from bento_federation_service.utils import peer_fetch, get_auth_header

from ..constants import DATASET_SEARCH_HEADERS
from ..dataset_search import run_search_on_dataset
from ..process_dataset_results import process_dataset_results
from ..query_utils import get_query_parts, test_queries


__all__ = [
    "DatasetsSearchHandler",
]


# noinspection PyAbstractClass
class DatasetsSearchHandler(RequestHandler):  # TODO: Move to another dedicated service?
    """
    Aggregates tables into datasets and runs a query against the data. Does not reveal internal object-level data.
    """

    include_internal_results = False

    @classmethod
    async def search_worker(
        cls,

        # Input queue
        dataset_queue: Queue,

        # Input values
        dataset_object_schema: dict,
        join_query,
        data_type_queries,
        exclude_from_auto_join: Tuple[str, ...],
        auth_header: Optional[str],

        # Output references
        dataset_objects_dict: dict,
        dataset_join_queries: dict,
    ):
        async for dataset in dataset_queue:
            if dataset is None:
                # Exit signal
                return

            try:
                dataset_id = dataset["identifier"]

                dataset_results, dataset_join_query, _ = await run_search_on_dataset(
                    dataset_object_schema,
                    dataset,
                    join_query,
                    data_type_queries,
                    exclude_from_auto_join,
                    cls.include_internal_results,
                    auth_header,
                )

                dataset_objects_dict[dataset_id] = dataset_results
                dataset_join_queries[dataset_id] = dataset_join_query

            except HTTPError as e:  # Thrown from run_search_on_dataset
                # Metadata service error
                # TODO: Better message
                # TODO: Set error code outside worker?
                print(f"[{SERVICE_NAME} {datetime.now()}] [ERROR] Error from dataset search: {str(e)}", file=sys.stderr,
                      flush=True)

            finally:
                dataset_queue.task_done()

    async def options(self):
        self.set_status(204)
        await self.finish()

    async def post(self):
        data_type_queries, join_query, exclude_from_auto_join = get_query_parts(self.request.body)
        if not data_type_queries:
            self.set_status(400)
            self.write(bad_request_error("Invalid request format (missing body or data_type_queries)"))
            return

        results = []

        auth_header = get_auth_header(self.request.headers)

        try:
            # Try compiling each query to make sure it works. Any exceptions thrown will get caught below.
            test_queries(data_type_queries.values())

            client = AsyncHTTPClient()

            # TODO: Handle pagination
            # TODO: Why fetch projects instead of datasets? Is it to avoid "orphan" datasets? Is that even possible?
            # Use Unix socket resolver

            projects = await peer_fetch(
                client,
                CHORD_URL,
                "api/metadata/api/projects",
                method="GET",
                auth_header=auth_header,
                extra_headers=DATASET_SEARCH_HEADERS
            )

            datasets_dict: Dict[str, dict] = {d["identifier"]: d for p in projects["results"] for d in p["datasets"]}
            dataset_objects_dict: Dict[str, Dict[str, list]] = {d: {} for d in datasets_dict}

            dataset_object_schema = {
                "type": "object",
                "properties": {}
            }

            dataset_join_queries: Dict[str, Query] = {d: None for d in datasets_dict}

            dataset_queue = Queue()
            for dataset in datasets_dict.values():
                dataset_queue.put_nowait(dataset)

            # Spawn workers to handle asynchronous requests to various datasets
            search_workers = tornado.gen.multi([
                self.search_worker(
                    dataset_queue,

                    dataset_object_schema,
                    join_query,
                    data_type_queries,
                    exclude_from_auto_join,
                    auth_header,

                    dataset_objects_dict,
                    dataset_join_queries,
                )
                for _ in range(WORKERS)
            ])
            await dataset_queue.join()

            print(f"[{SERVICE_NAME} {datetime.now()}] Done fetching individual service search results.", flush=True)

            # Aggregate datasets into results list if they satisfy the queries
            for dataset_id, dataset_results in dataset_objects_dict.items():  # TODO: Worker
                results.extend(process_dataset_results(
                    data_type_queries,
                    dataset_join_queries[dataset_id],
                    dataset_results,
                    datasets_dict[dataset_id],
                    dataset_object_schema,
                    include_internal_data=False
                ))

            self.write({"results": results})

            await self.finish()

            # Trigger exit for all search workers
            for _ in range(WORKERS):
                dataset_queue.put_nowait(None)

            # Wait for workers to exit
            await search_workers

        except HTTPError as e:
            # Metadata service error
            # TODO: Better message
            print(f"[{SERVICE_NAME} {datetime.now()}] [ERROR] Error from service: {str(e)}", file=sys.stderr,
                  flush=True)
            self.set_status(500)
            self.write(internal_server_error(f"Error from service: {str(e)}"))

        except (TypeError, ValueError, SyntaxError) as e:  # errors from query processing
            # TODO: Better / more compliant error message
            # TODO: Move these up?
            # TODO: Not guaranteed to be actually query-processing errors
            self.set_status(400)
            self.write(bad_request_error(f"Query processing error: {str(e)}"))  # TODO: Better message
            print(f"[{SERVICE_NAME} {datetime.now()}] [ERROR] Encountered query processing error: {str(e)}",
                  file=sys.stderr, flush=True)
            traceback.print_exc()
