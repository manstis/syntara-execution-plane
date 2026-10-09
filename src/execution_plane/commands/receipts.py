"""EP domain authorization within the shared journal receipt transaction."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from execution_plane.models.cluster import Cluster
from execution_plane_client.errors import UnauthorizedPeer

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.journal.store import ExchangeJournalStore
    from execution_plane_client.protocol import MessageEnvelope
    from execution_plane_client.security import AuthenticatedIdentity


class EPReceiptHook:
    """Validate domain grants; durable intent belongs to the shared journal."""

    def __init__(self, journal: ExchangeJournalStore) -> None:
        """Read resource ownership from the participant journal."""
        self._journal = journal

    async def __call__(self, message: MessageEnvelope, identity: AuthenticatedIdentity, session: AsyncSession) -> None:
        """Validate grants atomically with receipt and its resource reservation."""
        del identity
        payload = message.command.payload
        if message.command.command_type.startswith("cluster.") and not message.scope.all_projects:
            grants = payload.get("project_ids")
            if (
                "project_ids" in payload or message.command.command_type in {"cluster.register", "cluster.provision"}
            ) and grants != [str(message.scope.project_id)]:
                raise UnauthorizedPeer
        if message.command.command_type == "execution_target.register":
            cluster_id = UUID(str(payload["cluster_id"]))
            owner = await self._journal.intents.owner(session, "cluster", cluster_id)
            cluster = await session.get(Cluster, cluster_id)
            if owner is None or owner.client_id != message.scope.client_id or cluster is None:
                raise UnauthorizedPeer
            if (
                not message.scope.all_projects
                and cluster.project_ids is not None
                and message.scope.project_id not in cluster.project_ids
            ):
                raise UnauthorizedPeer
