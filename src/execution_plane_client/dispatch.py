"""Small typed command surface for participants that submit Execution Plane work."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid5

from execution_plane_client.commands import CommandIdentity, CommandRoute
from execution_plane_client.commands.control import CancelCommand

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.client import MediatorClient
    from execution_plane_client.commands import ResourceRevision
    from execution_plane_client.commands.clusters import ClusterProvision
    from execution_plane_client.commands.work import WorkExecute
    from execution_plane_client.journal.records import JournalEntry
    from execution_plane_client.protocol import CommandScope, ExecutionPolicy, MessageEnvelope


class ExecutionPlaneCommands:
    """Submit typed EP commands without repeating route and envelope identity setup."""

    def __init__(self, client: MediatorClient) -> None:
        """Use the mediator client's configured default peer and journal."""
        self._client = client

    @staticmethod
    def _identity(command_id: UUID) -> CommandIdentity:
        """Derive the stable initial message identity for a logical command."""
        return CommandIdentity(
            command_id=command_id,
            message_id=uuid5(command_id, "submit"),
            occurred_at=datetime.now(UTC),
        )

    async def work_execute(
        self,
        session: AsyncSession,
        *,
        command_id: UUID,
        scope: CommandScope,
        payload: WorkExecute,
        execution_policy: ExecutionPolicy | None = None,
    ) -> JournalEntry:
        """Append a Script work command in the caller's transaction."""
        return await self._client.work_execute(
            session,
            identity=self._identity(command_id),
            route=self._client.route(scope),
            payload=payload,
            execution_policy=execution_policy,
        )

    async def cluster_provision(
        self,
        session: AsyncSession,
        *,
        command_id: UUID,
        scope: CommandScope,
        payload: ClusterProvision,
    ) -> JournalEntry:
        """Append a cluster and default-target provision command."""
        return await self._client.cluster_provision(
            session,
            identity=self._identity(command_id),
            route=self._client.route(scope),
            payload=payload,
        )

    async def cluster_delete(
        self,
        session: AsyncSession,
        *,
        command_id: UUID,
        scope: CommandScope,
        payload: ResourceRevision,
    ) -> JournalEntry:
        """Append a revisioned cluster deletion command."""
        return await self._client.cluster_delete(
            session,
            identity=self._identity(command_id),
            route=self._client.route(scope),
            payload=payload,
        )

    async def cancel_command(
        self,
        session: AsyncSession,
        *,
        command_id: UUID,
        target: MessageEnvelope,
    ) -> JournalEntry:
        """Cancel a previously submitted command using its frozen route and scope."""
        route = CommandRoute(
            source_id=target.source_id,
            destination_id=target.destination_id,
            scope=target.scope,
            target_url=target.command.target_url,
            callback_url=target.command.callback_url,
        )
        return await self._client.cancel_command(
            session,
            identity=self._identity(command_id),
            route=route,
            payload=CancelCommand(target_command_id=target.command_id),
        )
