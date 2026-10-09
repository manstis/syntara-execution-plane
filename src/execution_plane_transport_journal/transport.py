"""Journal-backed outbound queue with authenticated HTTP query delegation."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from execution_plane_client.errors import ProtocolConflict
from execution_plane_client.handlers import CommandDispatcher
from execution_plane_client.transport import DispatchReceipt, DispatchState, TransactionalDispatcher, Transport

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.protocol import (
        CapabilitiesRead,
        ChildPage,
        CommandRead,
        CommandScope,
        MessageEnvelope,
    )
    from execution_plane_transport_direct.transport import HttpCommandTransport
    from execution_plane_transport_journal.records import EnsuredOutbound
    from execution_plane_transport_journal.store import ExchangeJournalStore


class JournalTransport(Transport, TransactionalDispatcher, CommandDispatcher):
    """Persist outbound intent locally and delegate authenticated reads to HTTP."""

    def __init__(
        self, store: ExchangeJournalStore, sessions: Callable[[], AsyncSession], http: HttpCommandTransport
    ) -> None:
        """Retain the participant's one Journal and its selected underlying HTTP transport."""
        self._store = store
        self._sessions = sessions
        self._http = http

    async def send(self, message: MessageEnvelope) -> DispatchReceipt:
        """Commit a dispatch intent and report it queued for the delivery worker."""
        async with self._sessions() as session, session.begin():
            entry = await self._store.append_outbound(session, message)
        return DispatchReceipt(message_id=entry.message_id, state=DispatchState.QUEUED)

    async def stage(self, session: object, message: MessageEnvelope) -> Callable[[], Awaitable[None]]:
        """Append outbound intent to the caller transaction for post-commit delivery."""
        await self._store.append_outbound(cast("AsyncSession", session), message)

        async def wake_delivery() -> None:
            return None

        return wake_delivery

    async def submit_in_transaction(
        self,
        session: AsyncSession,
        *,
        scope: CommandScope,
        command_id: UUID,
        command_factory: Callable[[], Awaitable[MessageEnvelope]],
        context_factory: Callable[[], Mapping[str, object]] | None = None,
        correlation_key: str | None = None,
    ) -> EnsuredOutbound:
        """Ensure frozen intent and optional private context within the caller transaction."""

        async def create_entry() -> JournalEntry:
            message = await command_factory()
            if message.scope != scope or message.command_id != command_id:
                raise ProtocolConflict
            return await self._store.append_outbound(session, message)

        return await self._store.ensure_outbound(
            session,
            scope,
            command_id,
            command_factory=create_entry,
            context_factory=context_factory,
            correlation_key=correlation_key,
        )

    async def get_command(self, peer_id: str, scope: CommandScope, command_id: UUID) -> CommandRead:
        """Read remote command status over the configured authenticated HTTP transport."""
        return await self._http.get_command(peer_id, scope, command_id)

    async def get_children(
        self,
        peer_id: str,
        scope: CommandScope,
        command_id: UUID,
        after: int = 0,
    ) -> ChildPage:
        """Read remote completion children over the configured HTTP transport."""
        return await self._http.get_children(peer_id, scope, command_id, after)

    async def get_capabilities(
        self,
        peer_id: str,
        scope: CommandScope | None = None,
    ) -> CapabilitiesRead:
        """Read remote command capabilities over the configured HTTP transport."""
        return await self._http.get_capabilities(peer_id, scope)

    async def get_resource(
        self,
        peer_id: str,
        scope: CommandScope,
        collection: str,
        *,
        resource_id: UUID | None = None,
        limit: int = 50,
    ) -> object:
        """Read a remote resource projection over the configured HTTP transport."""
        return await self._http.get_resource(
            peer_id,
            scope,
            collection,
            resource_id=resource_id,
            limit=limit,
        )

    async def aclose(self) -> None:
        """Close the selected underlying transport."""
        await self._http.aclose()


if TYPE_CHECKING:
    from uuid import UUID

    from execution_plane_transport_journal.records import JournalEntry
