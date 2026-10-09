"""DNS-pinned HTTP connections for registered command peers."""

from __future__ import annotations

from typing import TYPE_CHECKING

import httpx

from execution_plane_client.errors import UnsafeDestination

if TYPE_CHECKING:
    from execution_plane_client.destinations import DestinationPolicy


class PinnedTransport(httpx.AsyncBaseTransport):
    """Use approved IPs while preserving original Host and TLS hostname/SNI."""

    def __init__(self, policy: DestinationPolicy, peer_id: str, inner: httpx.AsyncBaseTransport) -> None:
        """Own one connection pool per peer, preventing cross-peer SNI pool reuse."""
        self._policy = policy
        self._peer_id = peer_id
        self._inner = inner

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        """Connect to the checked numeric address, never re-resolve the peer name."""
        if request.method not in {"POST", "GET"}:
            raise UnsafeDestination
        destination = self._policy.validate(str(request.url), self._peer_id, query=request.method == "GET")
        addresses = await self._policy.resolve(destination)
        pinned = httpx.Request(
            request.method,
            request.url.copy_with(host=addresses[0]),
            headers={**request.headers, "host": destination.host_header},
            stream=request.stream,
            extensions={**request.extensions, "sni_hostname": destination.hostname},
        )
        return await self._inner.handle_async_request(pinned)

    async def aclose(self) -> None:
        """Close this peer's bounded pool."""
        await self._inner.aclose()
