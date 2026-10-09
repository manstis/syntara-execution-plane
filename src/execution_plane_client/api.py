"""Mountable authenticated command receipt, status, children and capabilities."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from time import perf_counter
from typing import TYPE_CHECKING, Annotated
from uuid import UUID  # noqa: TC003 - FastAPI resolves route annotations at runtime.

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.security import HTTPBearer
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from execution_plane_client.errors import CommandNotFound, ProtocolConflict, UnauthorizedPeer, UnsupportedCommand
from execution_plane_client.protocol import CapabilitiesRead, ChildPage, CommandRead, MessageEnvelope
from execution_plane_client.security import AuthenticatedIdentity  # noqa: TC001 - FastAPI dependency annotation.
from execution_plane_client.transport import TransportReceipt, strict_json

if TYPE_CHECKING:
    from execution_plane_client.service import MediatorService

type CertificateAuthenticator = Callable[[Request], Awaitable[str]]
type SessionDependency = Callable[..., AsyncIterator[AsyncSession]]
_BODY_LIMIT = 1024 * 1024


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
    request: Request, service: MediatorService, certificate_peer: str | None = None
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
    service: MediatorService | Callable[[], MediatorService],
    prefix: str = "/v1",
    *,
    router: APIRouter | None = None,
    session_dependency: SessionDependency | None = None,
) -> APIRouter:
    """Mount routes with participant-supplied certificate and database dependencies."""
    router = router or APIRouter(prefix=prefix)
    router.dependencies.append(Depends(HTTPBearer(auto_error=False)))

    def current() -> MediatorService:
        return service() if callable(service) else service

    async def certificate_identity(request: Request) -> str:
        return await current().certificate_auth(request)

    async def journal_session() -> AsyncIterator[AsyncSession]:
        async with current().sessions() as session:
            yield session

    session_dependency = (
        session_dependency or (None if callable(service) else service.session_dependency) or journal_session
    )
    certificate_dependency = Depends(certificate_identity)
    database_dependency = Depends(session_dependency)

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
        session: AsyncSession = database_dependency,
    ) -> TransportReceipt:
        started = perf_counter()
        data = bytearray()
        async for chunk in request.stream():
            data.extend(chunk)
            if len(data) > _BODY_LIMIT:
                raise HTTPException(status_code=413, detail="Command too large")
        try:
            message = MessageEnvelope.model_validate(strict_json(bytes(data)))
            identity = current().verifier.verify(_token(request), message)
            _check_certificate(identity, certificate_peer)
            async with session.begin():
                receipt = await current().ingress.receive(message, identity, session)
        except (ValidationError, ValueError, ProtocolConflict, UnsupportedCommand, UnauthorizedPeer) as error:
            raise _http_error(error) from None
        if message.command.command_type == "command.accepted" and message.command.parent_command_id is not None:
            parent = await current().journal.get(session, message.scope, message.command.parent_command_id, "outbound")
            if parent is not None and parent.accepted_at is not None:
                current().observer.record(
                    "acceptance.observed",
                    {
                        "command_id": str(parent.command_id),
                        "latency_seconds": max(0.0, (parent.accepted_at - parent.created_at).total_seconds()),
                    },
                )
        current().observer.record(
            "receipt.committed",
            {
                "message_id": str(receipt.entry.message_id),
                "command_id": str(receipt.entry.command_id),
                "duplicate": receipt.duplicate,
                "latency_seconds": perf_counter() - started,
            },
        )
        return TransportReceipt(received=True, message_id=receipt.entry.message_id, duplicate=receipt.duplicate)

    @router.get("/commands/{command_id}", response_model=CommandRead)
    async def get_command(
        command_id: UUID,
        identity: AuthenticatedIdentity = identity_dependency,
        session: AsyncSession = database_dependency,
    ) -> CommandRead:
        try:
            return await current().queries.get(session, identity.scope, command_id, peer_id=identity.peer_id)
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
        session: AsyncSession = database_dependency,
    ) -> ChildPage:
        try:
            return await current().queries.children(
                session, identity.scope, command_id, after, limit, peer_id=identity.peer_id, include_recovery=recover
            )
        except (UnauthorizedPeer, CommandNotFound) as error:
            raise _http_error(error) from None

    @router.get("/capabilities", response_model=CapabilitiesRead)
    async def capabilities(_identity: AuthenticatedIdentity = identity_dependency) -> CapabilitiesRead:
        return current().capabilities.advertise()

    return router
