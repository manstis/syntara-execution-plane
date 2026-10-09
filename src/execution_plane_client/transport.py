"""Bounded authenticated HTTPS transport; HTTP receipt is not command acceptance."""

from __future__ import annotations

import asyncio
import json
import ssl
from dataclasses import dataclass
from time import monotonic
from typing import TYPE_CHECKING, Literal
from uuid import UUID  # noqa: TC003 - SQLModel resolves TransportReceipt fields at runtime.

import httpx
from pydantic import ValidationError

from execution_plane_client.crypto import canonical_bytes
from execution_plane_client.destinations import DestinationPolicy, PinnedTransport
from execution_plane_client.errors import (
    CommandNotFound,
    PermanentRejection,
    RetryableDelivery,
    UnsafeDestination,
)
from execution_plane_client.observers import ExchangeObserver, NullObserver
from execution_plane_client.protocol import (
    CapabilitiesRead,
    ChildPage,
    CommandRead,
    CommandScope,
    MessageEnvelope,
    WireModel,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from execution_plane_client.security import AssertionSigner, PeerConfig

_MAX_RESOURCE_PAGE = 200
_MAX_WIRE_BYTES = 1024 * 1024
_RETRY_STATUS = 429
_SERVER_ERROR_STATUS = 500
_REDIRECT_MIN = 300
_REDIRECT_MAX = 400
_SUCCESS_MIN = 200
_NOT_FOUND = 404


class TransportReceipt(WireModel):
    """A successful transport response, independent of the asynchronous ACK."""

    received: Literal[True]
    message_id: UUID
    duplicate: bool = False


@dataclass(frozen=True)
class TLSConfig:
    """Required trusted CA and client certificate files supplied by a participant."""

    ca_file: str
    certificate_file: str
    private_key_file: str

    def create_context(self) -> ssl.SSLContext:
        """Enable CA/hostname verification and present the participant certificate."""
        context = ssl.create_default_context(cafile=self.ca_file)
        context.load_cert_chain(self.certificate_file, self.private_key_file)
        return context


def strict_json(data: bytes) -> object:
    """Reject duplicate keys rather than accepting parser-dependent identities."""

    def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in items:
            if key in value:
                error = "Duplicate JSON object key"
                raise ValueError(error)
            value[key] = item
        return value

    def constant(_value: str) -> object:
        error = "Non-finite JSON value"
        raise ValueError(error)

    return json.loads(data, object_pairs_hook=pairs, parse_constant=constant)


def _check_response(response: httpx.Response, method: str) -> None:
    """Classify status without persisting or logging arbitrary peer response text."""
    status = response.status_code
    if _REDIRECT_MIN <= status < _REDIRECT_MAX:
        raise UnsafeDestination
    if status == _RETRY_STATUS or status >= _SERVER_ERROR_STATUS:
        raise RetryableDelivery
    if status == _NOT_FOUND and method == "GET":
        raise CommandNotFound
    if not _SUCCESS_MIN <= status < _REDIRECT_MIN:
        raise PermanentRejection
    if response.headers.get("content-encoding", "identity") != "identity":
        raise RetryableDelivery


class HttpCommandTransport:
    """One bounded pool per peer with no redirects, proxies or stored bearer tokens."""

    def __init__(
        self,
        *,
        signer: AssertionSigner,
        peers: Mapping[str, PeerConfig],
        tls: TLSConfig | None = None,
        transports: Mapping[str, httpx.AsyncBaseTransport] | None = None,
        policy: DestinationPolicy | None = None,
        observer: ExchangeObserver | None = None,
        timeout_seconds: float = 10,
    ) -> None:
        """Inject in-process transports explicitly for tests; normal peers require mTLS."""
        self._signer = signer
        self._peers = dict(peers)
        self._policy = policy or DestinationPolicy(peers)
        self._observer = observer or NullObserver()
        self._timeout = timeout_seconds
        self._clients: dict[str, httpx.AsyncClient] = {}
        for peer_id in peers:
            if peer_id == signer.local_id:
                continue
            injected = transports.get(peer_id) if transports is not None else None
            if injected is None:
                if tls is None:
                    error = "Peer transport requires client TLS credentials"
                    raise ValueError(error)
                inner = httpx.AsyncHTTPTransport(
                    verify=tls.create_context(),
                    trust_env=False,
                    limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
                )
                injected = PinnedTransport(self._policy, peer_id, inner)
            self._clients[peer_id] = httpx.AsyncClient(
                transport=injected,
                follow_redirects=False,
                trust_env=False,
                timeout=httpx.Timeout(timeout_seconds),
            )

    async def _request(
        self,
        peer_id: str,
        method: str,
        url: str,
        token: str,
        body: bytes | None = None,
    ) -> object:
        self._policy.validate(url, peer_id, query=method == "GET")
        if body is not None and len(body) > _MAX_WIRE_BYTES:
            raise PermanentRejection
        started = monotonic()
        try:
            async with asyncio.timeout(self._timeout):
                async with self._clients[peer_id].stream(
                    method,
                    url,
                    content=body,
                    headers={
                        "authorization": f"Bearer {token}",
                        "content-type": "application/json",
                        "accept-encoding": "identity",
                    },
                ) as response:
                    status = response.status_code
                    self._observer.record("transport.response", {"peer_id": peer_id, "status": status})
                    _check_response(response, method)
                    data = bytearray()
                    async for chunk in response.aiter_bytes():
                        data.extend(chunk)
                        if len(data) > _MAX_WIRE_BYTES:
                            raise RetryableDelivery
                    return strict_json(bytes(data))
        except (httpx.HTTPError, TimeoutError, OSError):
            self._observer.record("transport.retry", {"peer_id": peer_id, "reason": "network"})
            raise RetryableDelivery from None
        except (ValueError, UnicodeError, RecursionError):
            self._observer.record("transport.retry", {"peer_id": peer_id, "reason": "response"})
            raise RetryableDelivery from None
        finally:
            self._observer.record("transport.latency", {"peer_id": peer_id, "seconds": monotonic() - started})

    async def send(self, message: MessageEnvelope) -> TransportReceipt:
        """Return receipt only after validating the response's exact message identity."""
        data = await self._request(
            message.destination_id,
            "POST",
            message.command.target_url,
            self._signer.sign(message),
            canonical_bytes(message),
        )
        try:
            receipt = TransportReceipt.model_validate(data)
        except ValidationError:
            raise RetryableDelivery from None
        if receipt.message_id != message.message_id:
            raise RetryableDelivery
        return receipt

    async def _query(self, peer_id: str, scope: CommandScope, suffix: str) -> object:
        peer = self._peers.get(peer_id)
        if peer is None:
            raise UnsafeDestination
        url = peer.query_base_url.rstrip("/") + suffix
        path = httpx.URL(url).raw_path.decode("ascii")
        return await self._request(peer_id, "GET", url, self._signer.sign_query(peer_id, scope, path))

    async def get_command(self, peer_id: str, scope: CommandScope, command_id: UUID) -> CommandRead:
        """Fetch a scoped, safe status projection."""
        data = await self._query(peer_id, scope, f"/commands/{command_id}")
        try:
            return CommandRead.model_validate(data)
        except ValidationError:
            raise RetryableDelivery from None

    async def get_children(
        self,
        peer_id: str,
        scope: CommandScope,
        command_id: UUID,
        after: int = 0,
    ) -> ChildPage:
        """Recover one original outcome per bounded page, including its private result."""
        data = await self._query(peer_id, scope, f"/commands/{command_id}/children?after={after}&limit=1&recover=true")
        try:
            return ChildPage.model_validate(data)
        except ValidationError:
            raise RetryableDelivery from None

    async def get_resource(
        self,
        peer_id: str,
        scope: CommandScope,
        collection: str,
        *,
        resource_id: UUID | None = None,
        limit: int = 50,
    ) -> object:
        """Query only registered read collections through the pinned signed transport."""
        if collection not in {"work-items", "clusters", "execution-targets"} or not 1 <= limit <= _MAX_RESOURCE_PAGE:
            error = "Invalid resource collection or page bounds"
            raise ValueError(error)
        suffix = f"/{collection}/{resource_id}" if resource_id else f"/{collection}?limit={limit}"
        return await self._query(peer_id, scope, suffix)

    async def get_capabilities(self, peer_id: str, scope: CommandScope | None = None) -> CapabilitiesRead:
        """Inspect installed revisions with authenticated initiating-client identity."""
        scope = scope or CommandScope(client_id=self._signer.local_id, project_id=None, all_projects=True)
        data = await self._query(peer_id, scope, "/capabilities")
        try:
            return CapabilitiesRead.model_validate(data)
        except ValidationError:
            raise RetryableDelivery from None

    async def aclose(self) -> None:
        """Close all peer pools when participant workers shut down."""
        for client in self._clients.values():
            await client.aclose()
