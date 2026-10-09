"""Adapt Journal-backed EP command queries to the shared provider protocol."""

from __future__ import annotations

from typing import TYPE_CHECKING

from execution_plane_client.queries import CommandQueryProvider

if TYPE_CHECKING:
    from collections.abc import Callable
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.protocol import ChildPage, CommandRead, CommandScope
    from execution_plane_transport_journal.queries import JournalSessionCommandQueryProvider


class EPJournalCommandQueryProvider(CommandQueryProvider):
    """Bind Journal's caller-session projection to EP's session-free query API."""

    def __init__(self, sessions: Callable[[], AsyncSession], provider: JournalSessionCommandQueryProvider) -> None:
        """Bind an EP domain session factory to Journal's session-based projection."""
        self._sessions = sessions
        self._provider = provider

    async def get(self, scope: CommandScope, command_id: UUID, *, peer_id: str) -> CommandRead:
        """Read one command projection through a short-lived domain session."""
        async with self._sessions() as session:
            return await self._provider.get(session, scope, command_id, peer_id=peer_id)

    async def children(
        self,
        scope: CommandScope,
        command_id: UUID,
        after: int = 0,
        limit: int = 50,
        *,
        peer_id: str,
        include_recovery: bool = False,
    ) -> ChildPage:
        """Read a bounded child page through a short-lived domain session."""
        async with self._sessions() as session:
            return await self._provider.children(
                session,
                scope,
                command_id,
                after,
                limit,
                peer_id=peer_id,
                include_recovery=include_recovery,
            )
