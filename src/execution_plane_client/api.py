"""Mountable authenticated command receipt, status, children and capabilities."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Annotated, Protocol
from uuid import UUID  # noqa: TC003 - FastAPI resolves route annotations at runtime.

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.security import HTTPBearer
from pydantic import ValidationError

from execution_plane_client.errors import CommandNotFound, ProtocolConflict, UnauthorizedPeer, UnsupportedCommand
from execution_plane_client.protocol import CapabilitiesRead, ChildPage, CommandRead, MessageEnvelope
from execution_plane_client.security import AuthenticatedIdentity  # noqa: TC001 - FastAPI dependency annotation.
from execution_plane_client.transport import TransportReceipt

if TYPE_CHECKING:
    from execution_plane_client.catalog import CapabilityRegistry
    from execution_plane_client.handlers import CompletionRecorder
    from execution_plane_client.protocol import CommandScope
    from execution_plane_client.security import AssertionVerifier

type CertificateAuthenticator = Callable[[Request], Awaitable[str]]
_BODY_LIMIT = 1024 * 1024


class CommandRuntime(Protocol):
    """Persistence-neutral operations required by the shared FastAPI router."""

    @property
    def verifier(self) -> AssertionVerifier:
        """Verify signed peer assertions."""
        ...

    @property
    def certificate_auth(self) -> CertificateAuthenticator:
        """Authenticate the peer certificate on an inbound request."""
        ...

    @property
    def capabilities(self) -> CapabilityRegistry:
        """Advertise the handlers installed in this process."""
        ...

    @property
    def sessions(self) -> Callable[[], object] | None:
        """Create a participant-owned session where domain persistence is needed."""
        ...

    @property
    def work_completion_recorder(self) -> CompletionRecorder | None:
        """Record work completion using the selected transport implementation."""
        ...

    async def receive_command(self, message: MessageEnvelope, identity: AuthenticatedIdentity) -> TransportReceipt:
        """Accept one authenticated command envelope."""
        ...

    async def get_command(self, scope: CommandScope, command_id: UUID, *, peer_id: str) -> CommandRead:
        """Query one authorized command projection."""
        ...

    async def get_children(
        self,
        scope: CommandScope,
        command_id: UUID,
        after: int,
        limit: int,
        *,
        peer_id: str,
        include_recovery: bool,
    ) -> ChildPage:
        """Query bounded child outcomes, optionally including recovery envelopes."""
        ...

    async def run(self) -> None:
        """Run selected transport workers until the owning application stops it."""
        ...

    async def close(self) -> None:
        """Release selected transport and runtime resources."""
        ...


def strict_json(data: bytes) -> object:
    """Reject duplicate object keys and non-finite numbers in inbound envelopes."""

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


def _token(request: Request) -> str:
    header = request.headers.get("authorization", "")
    scheme, separator, token = header.partition(" ")
    if scheme.lower() != "bearer" or not separator or not token:
        raise HTTPException(status_code=401, detail="Service assertion required")
    return token


def _path(request: Request) -> str:
    raw_bytes: bytes = request.scope.get("raw_path", request.url.path.encode())
    query_bytes: bytes = request.scope.get("query_string", b"")
    raw = raw_bytes.decode("ascii")
    query = query_bytes.decode("ascii")
    return raw + ("?" + query if query else "")


def _check_certificate(identity: AuthenticatedIdentity, certificate_peer: str) -> None:
    if identity.peer_id != certificate_peer:
        raise UnauthorizedPeer


def _http_error(error: Exception) -> HTTPException:
    if isinstance(error, UnauthorizedPeer):
        return HTTPException(status_code=403, detail="Peer authorization failed")
    if isinstance(error, ProtocolConflict):
        return HTTPException(status_code=409, detail="Command identity conflict")
    if isinstance(error, CommandNotFound):
        return HTTPException(status_code=404, detail="Command not found")
    return HTTPException(status_code=422, detail="Invalid or unsupported command")


async def authenticate_query(
    request: Request, service: CommandRuntime, certificate_peer: str | None = None
) -> AuthenticatedIdentity:
    """Authenticate exact query target and scope with independent TLS and signing trust."""
    peer = certificate_peer or await service.certificate_auth(request)
    try:
        identity = service.verifier.verify_query(_token(request), _path(request))
        _check_certificate(identity, peer)
        return identity
    except UnauthorizedPeer as error:
        raise _http_error(error) from None


def _envelope_schema() -> dict[str, object]:
    """Inline the finite envelope schema while retaining strict streaming receipt."""
    schema = MessageEnvelope.model_json_schema()
    definitions = schema.pop("$defs", {})

    def expand(value: object) -> object:
        if isinstance(value, list):
            return [expand(item) for item in value]
        if isinstance(value, dict):
            if "$ref" in value:
                return expand(definitions[str(value["$ref"]).rsplit("/", 1)[-1]])
            return {key: expand(item) for key, item in value.items()}
        return value

    return {key: expand(value) for key, value in schema.items()}


def create_router(  # noqa: C901 - Four independently authenticated routes share injected dependencies.
    service: CommandRuntime | Callable[[], CommandRuntime],
    prefix: str = "/v1",
    *,
    router: APIRouter | None = None,
) -> APIRouter:
    """Mount shared command and query routes without assuming participant persistence."""
    router = router or APIRouter(prefix=prefix)
    router.dependencies.append(Depends(HTTPBearer(auto_error=False)))

    def current() -> CommandRuntime:
        return service() if callable(service) else service

    async def certificate_identity(request: Request) -> str:
        return await current().certificate_auth(request)

    certificate_dependency = Depends(certificate_identity)

    async def query_identity(
        request: Request,
        certificate_peer: str = certificate_dependency,
    ) -> AuthenticatedIdentity:
        return await authenticate_query(request, current(), certificate_peer)

    identity_dependency = Depends(query_identity)

    @router.post(
        "/commands",
        response_model=TransportReceipt,
        status_code=202,
        openapi_extra={
            "requestBody": {"required": True, "content": {"application/json": {"schema": _envelope_schema()}}}
        },
    )
    async def receive_command(
        request: Request,
        certificate_peer: str = certificate_dependency,
    ) -> TransportReceipt:
        data = bytearray()
        async for chunk in request.stream():
            data.extend(chunk)
            if len(data) > _BODY_LIMIT:
                raise HTTPException(status_code=413, detail="Command too large")
        try:
            message = MessageEnvelope.model_validate(strict_json(bytes(data)))
            identity = current().verifier.verify(_token(request), message)
            _check_certificate(identity, certificate_peer)
            return await current().receive_command(message, identity)
        except (ValidationError, ValueError, ProtocolConflict, UnsupportedCommand, UnauthorizedPeer) as error:
            raise _http_error(error) from None

    @router.get("/commands/{command_id}", response_model=CommandRead)
    async def get_command(
        command_id: UUID,
        identity: AuthenticatedIdentity = identity_dependency,
    ) -> CommandRead:
        try:
            return await current().get_command(identity.scope, command_id, peer_id=identity.peer_id)
        except (UnauthorizedPeer, CommandNotFound) as error:
            raise _http_error(error) from None

    @router.get("/commands/{command_id}/children", response_model=ChildPage)
    async def get_children(
        command_id: UUID,
        *,
        after: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        recover: bool = False,
        identity: AuthenticatedIdentity = identity_dependency,
    ) -> ChildPage:
        try:
            return await current().get_children(
                identity.scope,
                command_id,
                after,
                limit,
                peer_id=identity.peer_id,
                include_recovery=recover,
            )
        except (UnauthorizedPeer, CommandNotFound) as error:
            raise _http_error(error) from None

    @router.get("/capabilities", response_model=CapabilitiesRead)
    async def capabilities(_identity: AuthenticatedIdentity = identity_dependency) -> CapabilitiesRead:
        return current().capabilities.advertise()

    return router
