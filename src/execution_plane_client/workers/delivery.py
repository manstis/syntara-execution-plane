"""Fenced polling delivery with capability checks outside caller transactions."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Protocol
from uuid import uuid4

from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.errors import PermanentRejection, RetryableDelivery, UnsafeDestination, UnsupportedCommand
from execution_plane_client.journal.claims import DeliveryOutcome, JournalClaims
from execution_plane_client.observers import NullObserver
from execution_plane_client.workers.scheduling import run_claimed

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.handlers import CommandRegistry
    from execution_plane_client.journal.claims import JournalClaim
    from execution_plane_client.journal.store import ExchangeJournalStore
    from execution_plane_client.observers import ExchangeObserver
    from execution_plane_client.protocol import CapabilitiesRead, CommandScope, MessageEnvelope
    from execution_plane_client.transport import TransportReceipt


class DeliveryTransport(Protocol):
    """Transport operations needed after a delivery claim commits."""

    async def send(self, message: MessageEnvelope) -> TransportReceipt:
        """Submit an exact persisted envelope and validate HTTP receipt."""
        ...

    async def get_capabilities(self, peer_id: str, scope: CommandScope | None = None) -> CapabilitiesRead:
        """Observe installed peer revisions under the initiating client's grant."""
        ...


def _now() -> datetime:
    return datetime.now(UTC)


class JournalDeliveryWorker:
    """Own only transport retries; domain and provider execution are separate."""

    def __init__(
        self,
        store: ExchangeJournalStore,
        sessions: Callable[[], AsyncSession],
        transport: DeliveryTransport,
        registry: CommandRegistry,
        *,
        claims: JournalClaims | None = None,
        catalog: CommandCatalog | None = None,
        clock: Callable[[], datetime] = _now,
        observer: ExchangeObserver | None = None,
        poll_seconds: float = 1,
        batch_size: int = 10,
        capability_ttl_seconds: float = 60,
    ) -> None:
        """Inject participant sessions, HTTP transport, policy and a testable clock."""
        self.wakeup = asyncio.Event()
        self._store = store
        self._sessions = sessions
        self._transport = transport
        self._registry = registry
        self._claims = claims or JournalClaims(store)
        self._catalog = catalog or CommandCatalog()
        self._clock = clock
        self._observer = observer or NullObserver()
        self._owner = uuid4()
        self._poll_seconds = poll_seconds
        self._batch_size = batch_size
        self._capability_ttl = capability_ttl_seconds
        self._capabilities: dict[tuple[str, str], tuple[datetime, CapabilitiesRead]] = {}

    async def _check_capability(self, message: MessageEnvelope) -> None:
        definition = self._catalog.definition(message.command.command_type, message.command.command_revision)
        if definition.kind == "acceptance":
            # Receipt of an authenticated protocol command already establishes this
            # mandatory control capability; do not delay its ACK on another query.
            return
        if definition.completion_type is not None:
            self._registry.get(definition.completion_type, definition.revision)
        key = (message.destination_id, message.scope.client_id)
        cached = self._capabilities.get(key)
        now = self._clock()
        if cached is None or cached[0] <= now:
            capabilities = await self._transport.get_capabilities(message.destination_id, message.scope)
            self._capabilities[key] = (now + timedelta(seconds=self._capability_ttl), capabilities)
        else:
            capabilities = cached[1]
        if definition.revision not in capabilities.commands.get(definition.command_type, []):
            raise UnsupportedCommand

    async def _deliver(self, claim: JournalClaim) -> None:
        try:
            await self._check_capability(claim.message)
            await self._transport.send(claim.message)
            outcome = DeliveryOutcome("receipt")
        except RetryableDelivery:
            outcome = DeliveryOutcome("retry", "transport_uncertain")
        except UnsupportedCommand:
            outcome = DeliveryOutcome("rejected", "unsupported_revision")
        except UnsafeDestination:
            outcome = DeliveryOutcome("rejected", "unsafe_destination")
        except PermanentRejection:
            outcome = DeliveryOutcome("rejected", "peer_rejected")
        async with self._sessions() as session, session.begin():
            changed = await self._claims.record_delivery(session, claim, outcome, self._clock())
        self._observer.record(
            "delivery.result",
            {
                "command_id": str(claim.message.command_id),
                "message_id": str(claim.message.message_id),
                "attempt": claim.attempt,
                "outcome": outcome.kind,
                "applied": changed,
            },
        )

    async def _claim(self, limit: int) -> list[JournalClaim]:
        async with self._sessions() as session, session.begin():
            return await self._claims.claim_due_delivery(session, self._owner, self._clock(), limit)

    async def _next_due(self) -> datetime | None:
        async with self._sessions() as session:
            return await self._claims.next_delivery_at(session)

    async def run_once(self) -> int:
        """Commit claims before any HTTP; each response joins its own short transaction."""
        claims = await self._claim(self._batch_size)
        await asyncio.gather(*(self._deliver(claim) for claim in claims))
        return len(claims)

    async def run(self) -> None:
        """Send on commit hints or durable deadlines, keeping free capacity active."""
        await run_claimed(
            self._claim,
            self._deliver,
            self._next_due,
            self.wakeup,
            clock=self._clock,
            limit=self._batch_size,
            poll_seconds=self._poll_seconds,
        )
