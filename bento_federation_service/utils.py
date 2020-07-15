import json

from tornado.httpclient import AsyncHTTPClient
from tornado.queues import Queue
from typing import Iterable, Optional, Union
from urllib.parse import urljoin

from .constants import CHORD_DEBUG, SERVICE_NAME, TIMEOUT


__all__ = [
    "peer_fetch",
    "get_request_json",
    "iterable_to_queue",
    "get_auth_header",
]


RequestBody = Optional[Union[bytes, str]]


async def peer_fetch(client: AsyncHTTPClient, peer: str, path_fragment: str, request_body: RequestBody = None,
                     method: str = "POST", auth_header: Optional[str] = None, extra_headers: Optional[dict] = None):
    if CHORD_DEBUG:
        print(f"[{SERVICE_NAME}] [DEBUG] {method} to {urljoin(peer, path_fragment)}: {request_body}", flush=True)

    if isinstance(request_body, str):
        # Convert str to bytes with only accepted charset: UTF-8
        request_body = request_body.encode("UTF-8")

    r = await client.fetch(
        urljoin(peer, path_fragment),
        request_timeout=TIMEOUT,
        method=method,
        body=request_body,
        headers={
            **({} if request_body is None else {"Content-Type": "application/json; charset=UTF-8"}),
            **({"Authorization": auth_header} if auth_header else {}),
            **(extra_headers or {}),
        },
        raise_error=True
    )

    return json.loads(r.body) if r.code != 204 else None


def get_request_json(request_body: bytes) -> Optional[dict]:
    try:
        request = json.loads(request_body)
        # TODO: Validate against a JSON schema or OpenAPI
        return request if isinstance(request, dict) else None
    except json.JSONDecodeError:
        pass

    # Otherwise, return None implicitly


def iterable_to_queue(iterable: Iterable) -> Queue:
    queue = Queue()
    for item in iterable:
        queue.put_nowait(item)

    return queue


# TODO: Replace with bento_lib
def get_auth_header(headers: dict) -> Optional[str]:
    return headers.get("X-Authorization", headers.get("Authorization"))
