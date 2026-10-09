"""Scoped safe status projections and explicitly authorized outcome recovery."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import func, select

from execution_plane_client.errors import CommandNotFound, UnauthorizedPeer
from execution_plane_client.protocol import ChildPage, CommandRead
from execution_plane_client.queries import SessionCommandQueryProvider

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.protocol import CommandScope
    from execution_plane_transport_journal.records import JournalEntry
    from execution_plane_transport_journal.store import ExchangeJournalStore


class JournalSessionCommandQueryProvider(SessionCommandQueryProvider):
    """Implement session-bound command queries from persisted Journal projections."""

    def __init__(self, store: ExchangeJournalStore) -> None:
        """Use the participant's explicit journal store."""
        self._store = store

    async def _entry(
        self,
        session: AsyncSession,
        scope: CommandScope,
        command_id: UUID,
        peer_id: str | None,
    ) -> JournalEntry:
        for direction in ("inbound", "outbound"):
            entry = await self._store.get(session, scope, command_id, direction)
            if entry is not None and (peer_id is None or peer_id in {entry.source_id, entry.destination_id}):
                return entry
        raise CommandNotFound

    async def get(
        self,
        session: AsyncSession,
        scope: CommandScope,
        command_id: UUID,
        *,
        peer_id: str | None = None,
    ) -> CommandRead:
        """Exclude request payloads, results, ciphertext, credentials and actor context."""
        entry = await self._entry(session, scope, command_id, peer_id)
        table = self._store.schema.table
        latest = await session.scalar(
            select(func.max(table.c.parent_sequence)).where(
                table.c.client_id == entry.client_id,
                table.c.parent_command_id == entry.command_id,
            )
        )
        return CommandRead(
            command_id=entry.command_id,
            command_type=entry.command_type,
            status=entry.command_status,
            parent_command_id=entry.parent_command_id,
            resource=entry.resource,
            latest_child_sequence=latest or 0,
            delivery_state=entry.delivery_state,
            handling_state=entry.handling_state,
            error_code=entry.command_error_code or entry.handling_error_code or entry.delivery_error_code,
            reconciliation_required=entry.reconciliation_required,
            created_at=entry.created_at,
            handled_at=entry.handled_at,
        )

    async def children(
        self,
        session: AsyncSession,
        scope: CommandScope,
        command_id: UUID,
        after: int = 0,
        limit: int = 50,
        *,
        peer_id: str | None = None,
        include_recovery: bool = False,
    ) -> ChildPage:
        """Return private completion envelopes only to their original recipient."""
        parent = await self._entry(session, scope, command_id, peer_id)
        if include_recovery and (parent.direction != "inbound" or peer_id != parent.source_id):
            raise UnauthorizedPeer
        if include_recovery:
            limit = min(limit, 1)
        return await self._store.list_children(
            session,
            scope,
            command_id,
            after,
            limit,
            peer_id=peer_id,
            include_recovery=include_recovery,
        )
