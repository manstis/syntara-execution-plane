"""Journal-backed command runtime and participant composition."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from execution_plane_client.api import CommandRuntime, create_router
from execution_plane_client.catalog import CapabilityRegistry, CommandCatalog
from execution_plane_client.client import MediatorClient
from execution_plane_client.dispatch import ExecutionPlaneCommands
from execution_plane_client.observers import ExchangeObserver, NullObserver
from execution_plane_client.transport import TransportReceipt
from execution_plane_transport_journal.claims import JournalClaims
from execution_plane_transport_journal.configuration import ExchangeOptions
from execution_plane_transport_journal.ingress import CommandReceiptHandler
from execution_plane_transport_journal.notifications import JournalNotifications
from execution_plane_transport_journal.queries import JournalSessionCommandQueryProvider
from execution_plane_transport_journal.transport import JournalTransport
from execution_plane_transport_journal.workers.delivery import JournalDeliveryWorker
from execution_plane_transport_journal.workers.handling import JournalHandlingWorker
from execution_plane_transport_journal.workers.reconciliation import ReconciliationWorker

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from uuid import UUID

    from fastapi import APIRouter
    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.api import CertificateAuthenticator
    from execution_plane_client.handlers import CommandRegistry, CompletionRecorder, HandledHook, ReceiptPolicy
    from execution_plane_client.protocol import ChildPage, CommandRead, CommandScope, MessageEnvelope
    from execution_plane_client.queries import SessionCommandQueryProvider
    from execution_plane_client.security import AssertionVerifier, AuthenticatedIdentity, PeerConfig
    from execution_plane_transport_direct.transport import HttpCommandTransport
    from execution_plane_transport_journal.store import ExchangeJournalStore


@dataclass
class JournalCommandRuntime(CommandRuntime):
    """Own exchange workers while the participant retains its database engine."""

    client: MediatorClient
    ingress: CommandReceiptHandler
    delivery: JournalDeliveryWorker
    handling: JournalHandlingWorker
    reconciliation: ReconciliationWorker
    queries: SessionCommandQueryProvider
    _capabilities: CapabilityRegistry
    _verifier: AssertionVerifier
    _certificate_auth: CertificateAuthenticator
    _sessions: Callable[[], AsyncSession]
    transport: JournalTransport
    http_transport: HttpCommandTransport
    journal: ExchangeJournalStore
    _work_completion_recorder: CompletionRecorder | None = None
    observer: ExchangeObserver = field(default_factory=NullObserver)
    _tasks: list[asyncio.Task[None]] = field(default_factory=list, init=False, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    @property
    def capabilities(self) -> CapabilityRegistry:
        """Expose registered command capabilities to the shared API router."""
        return self._capabilities

    @property
    def verifier(self) -> AssertionVerifier:
        """Expose the configured assertion verifier to the shared API router."""
        return self._verifier

    @property
    def certificate_auth(self) -> CertificateAuthenticator:
        """Expose certificate authentication to the shared API router."""
        return self._certificate_auth

    @property
    def sessions(self) -> Callable[[], AsyncSession]:
        """Expose the participant's session factory to Journal workers and API routes."""
        return self._sessions

    @property
    def work_completion_recorder(self) -> CompletionRecorder | None:
        """Expose the selected completion bridge to command handlers."""
        return self._work_completion_recorder

    @property
    def commands(self) -> ExecutionPlaneCommands:
        """Expose concise typed EP command submission over this participant's client."""
        return ExecutionPlaneCommands(self.client)

    def create_router(self, prefix: str = "/v1", *, router: APIRouter | None = None) -> APIRouter:
        """Mount the shared transport-neutral command router."""
        return create_router(self, prefix, router=router)

    async def receive_command(self, message: MessageEnvelope, identity: AuthenticatedIdentity) -> TransportReceipt:
        """Persist the authenticated receipt before the router returns HTTP 202."""
        async with self.sessions() as session, session.begin():
            receipt = await self.ingress.receive(message, identity, session)
        self.observer.record(
            "receipt.committed",
            {
                "message_id": str(receipt.entry.message_id),
                "command_id": str(receipt.entry.command_id),
                "duplicate": receipt.duplicate,
            },
        )
        return TransportReceipt(received=True, message_id=receipt.entry.message_id, duplicate=receipt.duplicate)

    async def get_command(self, scope: CommandScope, command_id: UUID, *, peer_id: str) -> CommandRead:
        """Read command status through the configured participant query provider."""
        async with self.sessions() as session:
            return await self.queries.get(session, scope, command_id, peer_id=peer_id)

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
        """Read child outcomes through the configured participant query provider."""
        async with self.sessions() as session:
            return await self.queries.children(
                session,
                scope,
                command_id,
                after,
                limit,
                peer_id=peer_id,
                include_recovery=include_recovery,
            )

    async def run(self) -> None:
        """Supervise commit wakeups, workers and timed recovery together."""
        notifications = JournalNotifications(
            self.journal.schema, self.sessions, self.delivery.wakeup, self.handling.wakeup, observer=self.observer
        )
        try:
            async with asyncio.TaskGroup() as group:
                self._tasks = [
                    group.create_task(worker.run())
                    for worker in (notifications, self.delivery, self.handling, self.reconciliation)
                ]
        finally:
            await self.close()

    async def close(self) -> None:
        """Cancel/await work and close HTTP pools without falsely completing claims."""
        if self._closed:
            return
        self._closed = True
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.transport.aclose()


def _now() -> datetime:
    return datetime.now(UTC)


def build_journal_service(
    *,
    store: ExchangeJournalStore,
    sessions: Callable[[], AsyncSession],
    local_peer: PeerConfig,
    peers: Mapping[str, PeerConfig],
    registry: CommandRegistry,
    transport: HttpCommandTransport,
    verifier: AssertionVerifier,
    certificate_auth: CertificateAuthenticator,
    receipt_hook: ReceiptPolicy | None = None,
    handled_hook: HandledHook | None = None,
    completion_recorder: CompletionRecorder | None = None,
    queries: SessionCommandQueryProvider | None = None,
    clock: Callable[[], datetime] = _now,
    observer: ExchangeObserver | None = None,
    options: ExchangeOptions | None = None,
    default_peer_id: str | None = None,
) -> JournalCommandRuntime:
    """Compose the same implementation using explicit participant-owned dependencies."""
    options = options or ExchangeOptions()
    claims = JournalClaims(
        store,
        lease_seconds=options.lease_seconds,
        retry_base_seconds=options.retry_base_seconds,
        retry_max_seconds=options.retry_max_seconds,
    )
    catalog = CommandCatalog()
    ingress = CommandReceiptHandler(
        store,
        catalog,
        registry,
        local_id=local_peer.peer_id,
        local_role=local_peer.role,
        receipt_hook=receipt_hook,
        observer=observer,
    )
    journal_transport = JournalTransport(store, sessions, transport)
    client = MediatorClient(
        catalog,
        registry,
        journal_transport,
        local_id=local_peer.peer_id,
        local_role=local_peer.role,
        command_url=local_peer.command_url,
        peers=peers,
        default_peer_id=default_peer_id,
    )
    return JournalCommandRuntime(
        client=client,
        ingress=ingress,
        delivery=JournalDeliveryWorker(
            store,
            sessions,
            transport,
            registry,
            clock=clock,
            claims=claims,
            observer=observer,
            poll_seconds=options.poll_seconds,
            batch_size=options.batch_size,
        ),
        handling=JournalHandlingWorker(
            store,
            sessions,
            registry,
            local_id=local_peer.peer_id,
            handled_hook=handled_hook,
            completion_recorder=completion_recorder,
            clock=clock,
            claims=claims,
            observer=observer,
            poll_seconds=options.poll_seconds,
            batch_size=options.batch_size,
        ),
        reconciliation=ReconciliationWorker(
            store,
            sessions,
            transport,
            ingress,
            peers,
            clock=clock,
            observer=observer,
            poll_seconds=options.reconciliation_seconds,
            batch_size=options.batch_size,
        ),
        queries=queries or JournalSessionCommandQueryProvider(store),
        _capabilities=CapabilityRegistry(registry),
        _verifier=verifier,
        _certificate_auth=certificate_auth,
        _sessions=sessions,
        transport=journal_transport,
        http_transport=transport,
        journal=store,
        _work_completion_recorder=completion_recorder,
        observer=observer or NullObserver(),
    )
