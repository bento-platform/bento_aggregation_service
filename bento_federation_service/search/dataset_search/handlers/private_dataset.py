import sys
import traceback

from bento_lib.responses.errors import bad_request_error, internal_server_error
from datetime import datetime
from tornado.httpclient import AsyncHTTPClient, HTTPError
from tornado.web import RequestHandler

from bento_federation_service.constants import CHORD_URL, SERVICE_NAME
from bento_federation_service.utils import peer_fetch, get_auth_header

from ..constants import DATASET_SEARCH_HEADERS
from ..dataset_search import run_search_on_dataset
from ..process_dataset_results import process_dataset_results
from ..query_utils import get_query_parts, test_queries


# noinspection PyAbstractClass
class PrivateDatasetSearchHandler(RequestHandler):
    """
    Searches a specific dataset's tables, showing full object-level results. Unlike DatasetsSearchHandler, does not
    search across multiple datasets.
    """

    include_internal_results = True

    async def options(self, _dataset_id: str):
        self.set_status(204)
        await self.finish()

    async def post(self, dataset_id: str):
        data_type_queries, join_query, exclude_from_auto_join, fields = get_query_parts(self.request.body)
        if not data_type_queries:
            self.set_status(400)
            self.write(bad_request_error("Invalid request format (missing body or data_type_queries)"))
            return

        auth_header = get_auth_header(self.request.headers)

        try:
            # Try compiling each query to make sure it works. Any exceptions thrown will get caught below.
            test_queries(data_type_queries.values())

            client = AsyncHTTPClient()

            # TODO: Handle dataset 404 properly

            dataset = await peer_fetch(
                client,
                CHORD_URL,
                f"api/metadata/api/datasets/{dataset_id}",
                method="GET",
                auth_header=auth_header,
                extra_headers=DATASET_SEARCH_HEADERS
            )

            dataset_object_schema = {
                "type": "object",
                "properties": {}
            }

            dataset_results, dataset_join_query, ic_paths_to_filter = await run_search_on_dataset(
                dataset_object_schema,
                dataset,
                join_query,
                data_type_queries,
                fields,
                exclude_from_auto_join,
                self.include_internal_results,
                auth_header,
            )

            self.write(next(process_dataset_results(
                data_type_queries,
                dataset_join_query,
                dataset_results,
                dataset,
                dataset_object_schema,
                include_internal_data=True,
                ic_paths_to_filter=ic_paths_to_filter,
                always_yield=True,
            )))

            self.set_header("Content-Type", "application/json")

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
