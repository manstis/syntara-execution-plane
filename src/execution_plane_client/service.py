"""Participant composition and lifecycle for the shared exchange implementation."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from execution_plane_client.api import create_router
from execution_plane_client.catalog import CapabilityRegistry, CommandCatalog
from execution_plane_client.client import MediatorClient
from execution_plane_client.configuration import ExchangeOptions, PeerTrust
from execution_plane_client.crypto import AESGCMCipher
from execution_plane_client.dispatch import ExecutionPlaneCommands
from execution_plane_client.ingress import CommandReceiptHandler
from execution_plane_client.journal.claims import JournalClaims
from execution_plane_client.journal.notifications import JournalNotifications
from execution_plane_client.journal.store import ExchangeJournalStore
from execution_plane_client.observers import ExchangeObserver, NullObserver
from execution_plane_client.queries import JournalQueryProvider
from execution_plane_client.security import AssertionSigner, AssertionVerifier
from execution_plane_client.transport import HttpCommandTransport, TLSConfig
from execution_plane_client.workers.delivery import JournalDeliveryWorker
from execution_plane_client.workers.handling import JournalHandlingWorker
from execution_plane_client.workers.reconciliation import ReconciliationWorker

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from fastapi import APIRouter
    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.api import CertificateAuthenticator, SessionDependency
    from execution_plane_client.handlers import CommandRegistry
    from execution_plane_client.ingress import ReceiptHook
    from execution_plane_client.journal.schema import JournalSchema
    from execution_plane_client.queries import CommandQueryProvider
    from execution_plane_client.security import PeerConfig
    from execution_plane_client.workers.handling import HandledHook


@dataclass
class MediatorService:
    """Own exchange workers while the participant retains its database engine."""

    client: MediatorClient
    ingress: CommandReceiptHandler
    delivery: JournalDeliveryWorker
    handling: JournalHandlingWorker
    reconciliation: ReconciliationWorker
    queries: CommandQueryProvider
    capabilities: CapabilityRegistry
    verifier: AssertionVerifier
    certificate_auth: CertificateAuthenticator
    sessions: Callable[[], AsyncSession]
    transport: HttpCommandTransport
    journal: ExchangeJournalStore
    observer: ExchangeObserver = field(default_factory=NullObserver)
    session_dependency: SessionDependency | None = None
    _tasks: list[asyncio.Task[None]] = field(default_factory=list, init=False, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    @property
    def commands(self) -> ExecutionPlaneCommands:
        """Expose concise typed EP command submission over this participant's client."""
        return ExecutionPlaneCommands(self.client)

    def create_router(self, prefix: str = "/v1", *, router: APIRouter | None = None) -> APIRouter:
        """Mount shared routes, preserving participant router dependencies."""
        return create_router(self, prefix, router=router)

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


def build_service(
    *,
    store: ExchangeJournalStore,
    sessions: Callable[[], AsyncSession],
    local_peer: PeerConfig,
    peers: Mapping[str, PeerConfig],
    registry: CommandRegistry,
    transport: HttpCommandTransport,
    verifier: AssertionVerifier,
    certificate_auth: CertificateAuthenticator,
    receipt_hook: ReceiptHook | None = None,
    handled_hook: HandledHook | None = None,
    queries: CommandQueryProvider | None = None,
    session_dependency: SessionDependency | None = None,
    clock: Callable[[], datetime] = _now,
    observer: ExchangeObserver | None = None,
    options: ExchangeOptions | None = None,
    default_peer_id: str | None = None,
) -> MediatorService:
    """Compose the same implementation using explicit participant-owned dependencies."""
    options = options or ExchangeOptions()
    claims = JournalClaims(
        store,
        lease_seconds=options.lease_seconds,
        acceptance_seconds=options.acceptance_seconds,
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
    client = MediatorClient(
        store,
        catalog,
        registry,
        transport,
        local_id=local_peer.peer_id,
        local_role=local_peer.role,
        command_url=local_peer.command_url,
        peers=peers,
        default_peer_id=default_peer_id,
    )
    return MediatorService(
        client,
        ingress,
        JournalDeliveryWorker(
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
        JournalHandlingWorker(
            store,
            sessions,
            registry,
            local_id=local_peer.peer_id,
            handled_hook=handled_hook,
            clock=clock,
            claims=claims,
            observer=observer,
            poll_seconds=options.poll_seconds,
            batch_size=options.batch_size,
        ),
        ReconciliationWorker(
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
        queries or JournalQueryProvider(store),
        CapabilityRegistry(registry),
        verifier,
        certificate_auth,
        sessions,
        transport,
        store,
        observer or NullObserver(),
        session_dependency,
    )


class JournalServiceBuilder:
    """Build a participant's shared durable service from its local security inputs."""

    def __init__(
        self,
        *,
        sessions: Callable[[], AsyncSession],
        trust: PeerTrust,
        local_peer_id: str,
        expected_role: str,
        schema: JournalSchema,
        journal_key_id: str,
        journal_keys: Mapping[str, bytes],
        signing_key_id: str,
        signing_private_key: str,
        tls: TLSConfig,
        certificate_auth: CertificateAuthenticator,
        options: ExchangeOptions | None = None,
        observer: ExchangeObserver | None = None,
        clock: Callable[[], datetime] = _now,
        default_peer_id: str | None = None,
    ) -> None:
        """Create the journal and retain participant-owned transport configuration."""
        local = trust.peers.get(local_peer_id)
        if local is None:
            error = f"Local peer {local_peer_id!r} is not registered"
            raise ValueError(error)
        if local.role != expected_role:
            error = f"Local peer must have role {expected_role!r}, not {local.role!r}"
            raise ValueError(error)
        if local.command_url is None:
            error = "A durable service requires its own command URL"
            raise ValueError(error)
        if default_peer_id is not None and (default_peer_id not in trust.peers or default_peer_id == local_peer_id):
            error = "The default command peer must be a registered remote peer"
            raise ValueError(error)
        self.journal = ExchangeJournalStore(
            schema,
            AESGCMCipher(active_key_id=journal_key_id, keys=journal_keys),
        )
        self.sessions = sessions
        self.trust = trust
        self.local_peer = local
        self.signing_key_id = signing_key_id
        self.signing_private_key = signing_private_key
        self.tls = tls
        self.certificate_auth = certificate_auth
        self.options = options or ExchangeOptions()
        self.observer = observer or NullObserver()
        self.clock = clock
        self.default_peer_id = default_peer_id
        self._built = False

    def build(
        self,
        registry: CommandRegistry,
        *,
        receipt_hook: ReceiptHook | None = None,
        handled_hook: HandledHook | None = None,
        queries: CommandQueryProvider | None = None,
        session_dependency: SessionDependency | None = None,
    ) -> MediatorService:
        """Create signer, verifier, transport and the shared worker service once."""
        if self._built:
            error = "A journal service builder can only build one service"
            raise RuntimeError(error)
        signer = AssertionSigner(
            local_id=self.local_peer.peer_id,
            issuer=self.local_peer.issuer,
            key_id=self.signing_key_id,
            private_key=self.signing_private_key,
            peers=self.trust.peers,
        )
        transport = HttpCommandTransport(
            signer=signer,
            peers=self.trust.peers,
            tls=self.tls,
            timeout_seconds=self.options.request_timeout_seconds,
            observer=self.observer,
        )
        verifier = AssertionVerifier(
            local_id=self.local_peer.peer_id,
            audience=self.local_peer.audience,
            command_url=self.local_peer.command_url,
            peers=self.trust.peers,
        )
        service = build_service(
            store=self.journal,
            sessions=self.sessions,
            local_peer=self.local_peer,
            peers=self.trust.peers,
            registry=registry,
            transport=transport,
            verifier=verifier,
            certificate_auth=self.certificate_auth,
            receipt_hook=receipt_hook,
            handled_hook=handled_hook,
            queries=queries,
            session_dependency=session_dependency,
            clock=self.clock,
            observer=self.observer,
            options=self.options,
            default_peer_id=self.default_peer_id,
        )
        self._built = True
        return service
