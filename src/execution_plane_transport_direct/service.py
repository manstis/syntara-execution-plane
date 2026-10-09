"""Direct receipt runtime for immediate command and completion handling."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from execution_plane_client.api import CommandRuntime, create_router
from execution_plane_client.catalog import CapabilityRegistry, CommandCatalog
from execution_plane_client.client import MediatorClient
from execution_plane_client.completion import completion_message
from execution_plane_client.dispatch import ExecutionPlaneCommands
from execution_plane_client.errors import CommandExecutionError, ProtocolConflict
from execution_plane_client.handlers import (
    CompletionRecorder,
    ExternalHandler,
    HandlerContext,
    HandlingResult,
    TransactionalHandler,
)
from execution_plane_client.ingress import CommandIngress
from execution_plane_client.protocol import CommandStatus
from execution_plane_client.transport import Transport, TransportReceipt
from execution_plane_transport_direct.queries import InMemoryCommandQueryProvider

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
    from uuid import UUID

    from fastapi import APIRouter, Request
    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.handlers import CommandRegistry
    from execution_plane_client.ingress import ResolvedCommand
    from execution_plane_client.protocol import ChildPage, CommandRead, CommandScope, MessageEnvelope
    from execution_plane_client.queries import CommandStateProvider
    from execution_plane_client.security import AssertionVerifier, AuthenticatedIdentity, PeerConfig


def _now() -> datetime:
    return datetime.now(UTC)


class DirectCommandRuntime(CommandRuntime):
    """Receive, execute and query commands without Journal storage or workers."""

    def __init__(
        self,
        *,
        client: MediatorClient,
        catalog: CommandCatalog,
        registry: CommandRegistry,
        local_id: str,
        local_role: str,
        transport: Transport,
        verifier: AssertionVerifier,
        certificate_auth: Callable[[Request], Awaitable[str]],
        capabilities: CapabilityRegistry,
        queries: CommandStateProvider,
        sessions: Callable[[], AsyncSession] | None = None,
        completion_recorder: CompletionRecorder | None = None,
        clock: Callable[[], datetime] = _now,
    ) -> None:
        """Retain the selected transport and participant-owned protocol adapters."""
        self.client = client
        self._ingress = CommandIngress(catalog, registry, local_id=local_id, local_role=local_role)
        self._local_id = local_id
        self.transport = transport
        self._verifier = verifier
        self._certificate_auth = certificate_auth
        self._capabilities = capabilities
        self.queries = queries
        self._sessions = sessions
        self._work_completion_recorder = completion_recorder
        self._clock = clock
        self._tasks: set[asyncio.Task[None]] = set()
        self._receipt_locks: dict[tuple[str, UUID], asyncio.Lock] = {}
        self._stopping = asyncio.Event()
        self._closed = False

    @property
    def verifier(self) -> AssertionVerifier:
        """Expose the configured assertion verifier to the shared API router."""
        return self._verifier

    @property
    def certificate_auth(self) -> Callable[[Request], Awaitable[str]]:
        """Expose certificate authentication to the shared API router."""
        return self._certificate_auth

    @property
    def capabilities(self) -> CapabilityRegistry:
        """Expose registered command capabilities to the shared API router."""
        return self._capabilities

    @property
    def sessions(self) -> Callable[[], AsyncSession] | None:
        """Expose the optional domain session factory to the shared API router."""
        return self._sessions

    @property
    def work_completion_recorder(self) -> CompletionRecorder | None:
        """Expose the selected completion bridge to command handlers."""
        return self._work_completion_recorder

    @work_completion_recorder.setter
    def work_completion_recorder(self, recorder: CompletionRecorder | None) -> None:
        """Allow the EP factory to attach a recorder after transport construction."""
        self._work_completion_recorder = recorder

    @property
    def commands(self) -> ExecutionPlaneCommands:
        """Expose concise typed command submission through the selected transport."""
        return ExecutionPlaneCommands(self.client)

    def create_router(self, prefix: str = "/v1", *, router: APIRouter | None = None) -> APIRouter:
        """Mount the shared command and callback receiver."""
        return create_router(self, prefix, router=router)

    async def receive_command(self, message: MessageEnvelope, identity: AuthenticatedIdentity) -> TransportReceipt:
        """Run the handler directly and acknowledge only after its transaction commits."""
        resolved = self._ingress.resolve(message, identity)
        key = (message.scope.client_id, message.message_id)
        lock = self._receipt_locks.setdefault(key, asyncio.Lock())
        async with lock:
            if self.queries.has_received(message):
                return TransportReceipt(received=True, message_id=message.message_id, duplicate=True)
            result = await self._handle(message, resolved, record_receipt=True)
            if result.observe_after_seconds is not None and callable(getattr(resolved.handler, "observe", None)):
                self._schedule(self._observe(message, resolved, result.observe_after_seconds))
            return TransportReceipt(received=True, message_id=message.message_id, duplicate=False)

    def _context(self, message: MessageEnvelope) -> HandlerContext:
        """Create the shared handler context with only client-owned capabilities."""
        return HandlerContext(
            scope=message.scope,
            local_id=self._local_id,
            peer_id=message.source_id,
            dispatcher=self.transport,
            clock=self._clock,
            completion_recorder=self.work_completion_recorder,
        )

    async def _handle(  # noqa: C901, PLR0912, PLR0915 - Runtime handler types have distinct transaction paths.
        self,
        message: MessageEnvelope,
        resolved: ResolvedCommand,
        *,
        record_receipt: bool = False,
        method_name: str = "handle",
    ) -> HandlingResult:
        """Run one handler transaction and stage its completion before commit."""
        definition = resolved.definition
        handler = resolved.handler
        if handler is None:
            raise ProtocolConflict
        context = self._context(message)
        result = HandlingResult(status=CommandStatus.RUNNING)
        after_commit: list[Callable[[], Awaitable[None]]] = []
        child: MessageEnvelope | None = None
        async with self._transaction() as session:
            if isinstance(handler, TransactionalHandler):
                if message.command.command_type == "work.execute" and await self.queries.is_cancelled(
                    message.scope, message.command_id, session=session
                ):
                    result = HandlingResult(status=CommandStatus.CANCELLED, completion_required=True)
                else:
                    try:
                        async with self._savepoint(session):
                            result = await getattr(handler, method_name)(message, context, session)
                    except CommandExecutionError as error:
                        result = HandlingResult(
                            status=CommandStatus.FAILED,
                            error_code=error.code,
                            completion_required=definition.kind == "action",
                        )
            elif isinstance(handler, ExternalHandler):
                if session is None:
                    raise ProtocolConflict
                effect = await handler.prepare(message, context, session)
                outcome = await handler.execute(effect)
                result = await handler.finish(effect, outcome, context, session)
            else:
                raise ProtocolConflict

            if result.after_commit is not None:
                after_commit.append(result.after_commit)
            if result.completion_required and definition.kind == "action":
                normalized = replace(result, status=result.status or CommandStatus.SUCCEEDED)
                child = completion_message(message, normalized, sequence=1, occurred_at=self._clock())
                staged = getattr(self.transport, "stage", None)
                if staged is not None:
                    after_commit.append(await staged(session, child))
                else:
                    child_to_send = child
                    if child_to_send is None:
                        raise ProtocolConflict

                    async def send_child() -> None:
                        await self.transport.send(child_to_send)

                    after_commit.append(send_child)

        if record_receipt and not self.queries.record_received(message, local_id=self._local_id):
            raise ProtocolConflict
        if definition.kind == "completion":
            self.queries.mark_status(
                message.scope,
                message.command_id,
                result.status or CommandStatus.SUCCEEDED,
                resource=result.resource,
                error_code=result.error_code,
            )
        elif definition.kind == "action":
            status = result.status or CommandStatus.SUCCEEDED
            result = replace(result, status=status)
            self.queries.mark_status(
                message.scope,
                message.command_id,
                status,
                resource=result.resource,
                error_code=result.error_code,
            )
            if result.completion_required:
                if child is None:
                    raise ProtocolConflict
                self.queries.append_child(message, child, result)
        if result.after_commit is not None:
            # The result hook may publish a separate domain completion (for example
            # a cancelled work item) after the transaction that persisted it.
            await result.after_commit()
        for action in after_commit:
            if action is not result.after_commit:
                await action()
        return result

    async def _observe(
        self,
        message: MessageEnvelope,
        resolved: ResolvedCommand,
        delay: float,
    ) -> None:
        """Repeat explicitly requested domain observations until a result is terminal."""
        handler = resolved.handler
        if handler is None:
            return
        while not self._closed:
            await asyncio.sleep(delay)
            result = await self._handle(message, resolved, method_name="observe")
            if result.observe_after_seconds is None:
                return
            delay = result.observe_after_seconds

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncSession | None]:
        """Supply a null session for in-memory consumers or own one DB transaction."""
        if self._sessions is None:
            yield None
            return
        async with self._sessions() as session, session.begin():
            yield session

    @staticmethod
    @asynccontextmanager
    async def _savepoint(session: AsyncSession | None) -> AsyncIterator[None]:
        """Rollback partial domain changes when a handler returns a command error."""
        if session is None:
            yield
            return
        async with session.begin_nested():
            yield

    def _schedule(self, work: Awaitable[None]) -> None:
        async def run() -> None:
            await work

        task: asyncio.Task[None] = asyncio.create_task(run())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def get_command(self, scope: CommandScope, command_id: UUID, *, peer_id: str) -> CommandRead:
        """Query local command state through the injected provider."""
        return await self.queries.get(scope, command_id, peer_id=peer_id)

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
        """Query bounded child outcomes through the injected provider."""
        return await self.queries.children(
            scope,
            command_id,
            after,
            limit,
            peer_id=peer_id,
            include_recovery=include_recovery,
        )

    async def start(self) -> None:
        """Mark this runtime available; accepted commands schedule work immediately."""
        if self._closed:
            error = "Ephemeral command service is closed"
            raise RuntimeError(error)

    async def run(self) -> None:
        """Keep the runtime alive until its owner cancels it or calls close."""
        await self.start()
        try:
            await self._stopping.wait()
        finally:
            await self.close()

    async def close(self) -> None:
        """Cancel local tasks and release the selected transport."""
        if self._closed:
            return
        self._closed = True
        self._stopping.set()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.transport.aclose()


def build_direct_service(
    *,
    local_id: str,
    local_role: str,
    command_url: str,
    peers: Mapping[str, PeerConfig],
    registry: CommandRegistry,
    transport: Transport,
    verifier: AssertionVerifier,
    certificate_auth: Callable[[Request], Awaitable[str]],
    queries: CommandStateProvider | None = None,
    default_peer_id: str | None = None,
    max_in_memory_commands: int = 10_000,
    sessions: Callable[[], AsyncSession] | None = None,
    clock: Callable[[], datetime] = _now,
) -> DirectCommandRuntime:
    """Build a direct participant runtime without Journal storage or workers."""
    catalog = CommandCatalog()
    client = MediatorClient(
        catalog,
        registry,
        transport,
        local_id=local_id,
        local_role=local_role,
        command_url=command_url,
        peers=peers,
        default_peer_id=default_peer_id,
    )
    return DirectCommandRuntime(
        client=client,
        catalog=catalog,
        registry=registry,
        local_id=local_id,
        local_role=local_role,
        transport=transport,
        verifier=verifier,
        certificate_auth=certificate_auth,
        capabilities=CapabilityRegistry(registry),
        queries=queries or InMemoryCommandQueryProvider(max_commands=max_in_memory_commands),
        sessions=sessions,
        clock=clock,
    )
