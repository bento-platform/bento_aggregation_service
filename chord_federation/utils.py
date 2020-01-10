import json

from tornado.httpclient import AsyncHTTPClient
from typing import Optional
from urllib.parse import urljoin

from .constants import TIMEOUT


__all__ = ["peer_fetch"]


async def peer_fetch(client: AsyncHTTPClient, peer: str, path_fragment: str, request_body: Optional[bytes] = None,
                     method: str = "POST", extra_headers: Optional[dict] = None):
    r = await client.fetch(
        urljoin(peer, path_fragment),
        request_timeout=TIMEOUT,
        method=method,
        body=request_body,
        headers={
            **({} if request_body is None else {"Content-Type": "application/json"}),
            **({} if extra_headers is None else extra_headers)
        },
        raise_error=True
    )
    return json.loads(r.body) if r.code != 204 else None
