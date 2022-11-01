import json

from tornado.escape import url_escape
from tornado.httpclient import AsyncHTTPClient
from tornado.queues import Queue
from typing import Iterable, Optional, Tuple, Union
from urllib.parse import urljoin

from .constants import CHORD_DEBUG, CHORD_URL, SERVICE_NAME, TIMEOUT


__all__ = [
    "bento_fetch",
    "get_request_json",
    "iterable_to_queue",
    "get_auth_header",
]


RequestBody = Optional[Union[bytes, str]]


async def bento_fetch(client: AsyncHTTPClient, path_fragment: str, request_body: RequestBody = None,
                      method: str = "POST", auth_header: Optional[str] = None, extra_headers: Optional[dict] = None,
                      url_args: Tuple[Tuple[str, str]] = ()):
    if CHORD_DEBUG:
        print(f"[{SERVICE_NAME}] [DEBUG] {method} to {urljoin(CHORD_URL, path_fragment)}: {request_body}", flush=True)

    if isinstance(request_body, str):
        # Convert str to bytes with only accepted charset: UTF-8
        request_body = request_body.encode("UTF-8")

    arg_str = ""
    if url_args:
        arg_str = "?" + "&".join(f"{k}={url_escape(v)}" for k, v in url_args)

    r = await client.fetch(
        urljoin(CHORD_URL, path_fragment) + arg_str,
        request_timeout=TIMEOUT,
        method=method,
        body=request_body, validate_cert=(not CHORD_DEBUG),
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