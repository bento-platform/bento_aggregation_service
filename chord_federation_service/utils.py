import json
import socket

from tornado.httpclient import AsyncHTTPClient
from tornado.netutil import Resolver
from tornado.queues import Queue
from typing import Iterable, Optional, Union
from urllib.parse import urljoin

from .constants import CHORD_DEBUG, SOCKET_INTERNAL, SOCKET_INTERNAL_DOMAIN, SERVICE_NAME, TIMEOUT


__all__ = [
    "peer_fetch",
    "ServiceSocketResolver",
    "get_request_json",
    "get_new_peer_queue",
]


RequestBody = Optional[Union[bytes, str]]


async def peer_fetch(client: AsyncHTTPClient, peer: str, path_fragment: str, request_body: RequestBody = None,
                     method: str = "POST", extra_headers: Optional[dict] = None):
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
            **({} if extra_headers is None else extra_headers)
        },
        raise_error=True
    )

    return json.loads(r.body) if r.code != 204 else None


# TODO: Try to use OverrideResolver instead
class ServiceSocketResolver(Resolver):
    # noinspection PyAttributeOutsideInit
    def initialize(self, resolver):  # tornado Configurable init
        self.resolver = resolver

    def close(self):
        self.resolver.close()

    async def resolve(self, host, port, *args, **kwargs):
        if host == SOCKET_INTERNAL_DOMAIN:
            return [(socket.AF_UNIX, SOCKET_INTERNAL)]
        return await self.resolver.resolve(host, port, *args, **kwargs)


def get_request_json(request_body: bytes) -> Optional[dict]:
    request = None

    try:
        request = json.loads(request_body)
    except json.JSONDecodeError:
        pass

    # TODO: Validate against a JSON schema or OpenAPI
    if not isinstance(request, dict):
        request = None

    return request


def get_new_peer_queue(peers: Iterable) -> Queue:
    peer_queue = Queue()
    for peer in peers:
        peer_queue.put_nowait(peer)

    return peer_queue
