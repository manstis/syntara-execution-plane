"""In-memory resource revision adapter used by direct EP receipt."""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

from sqlalchemy import func, select

from execution_plane_client.errors import ProtocolConflict, UnauthorizedPeer
from execution_plane_client.resources import ResourceIntent, ResourceIntentStore, desired_hash

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.protocol import MessageEnvelope


class InMemoryResourceIntentStore(ResourceIntentStore):
    """Provide resource revision fencing without Journal storage."""

    def __init__(self) -> None:
        """Start with no received resource revisions."""
        self._resources: dict[tuple[str, str, str], ResourceIntent] = {}

    async def resource(self, session: AsyncSession, message: MessageEnvelope) -> ResourceIntent:
        """Reserve the highest desired revision for one client-owned resource."""
        payload = message.command.payload
        try:
            resource_type = message.command.command_type.partition(".")[0]
            resource_key = str(payload["resource_key"])
            revision = int(str(payload["desired_revision"]))
        except (KeyError, ValueError, TypeError) as exc:
            raise ProtocolConflict from exc
        if resource_type not in {"cluster", "execution_target"} or revision < 1:
            raise ProtocolConflict
        await self._lock(session, message.scope.client_id, resource_type, resource_key)
        key = (message.scope.client_id, resource_type, resource_key)
        content_hash = desired_hash(message)
        current = self._resources.get(key)
        if current is None:
            current = ResourceIntent(
                scope=message.scope,
                client_id=message.scope.client_id,
                resource_type=resource_type,
                resource_key=resource_key,
                desired_revision=revision,
                desired_hash=content_hash,
            )
            self._resources[key] = current
        elif current.scope != message.scope:
            raise UnauthorizedPeer
        elif revision == current.desired_revision and content_hash != current.desired_hash:
            raise ProtocolConflict
        elif revision > current.desired_revision:
            current.desired_revision = revision
            current.desired_hash = content_hash
            current.resource_deleted = False
        return current

    async def observe_resource(
        self,
        session: AsyncSession,
        message: MessageEnvelope,
        *,
        resource_id: UUID | None,
        observed_revision: int | None = None,
        deleted: bool | None = None,
    ) -> ResourceIntent:
        """Store physical identity and the revision reached by this handler."""
        current = await self.resource(session, message)
        revision = int(str(message.command.payload["desired_revision"]))
        if revision != current.desired_revision or desired_hash(message) != current.desired_hash:
            raise ProtocolConflict
        if observed_revision is not None:
            if observed_revision < current.observed_revision or observed_revision > revision:
                raise ProtocolConflict
            current.observed_revision = observed_revision
        current.resource_id = resource_id
        if deleted is not None:
            current.resource_deleted = deleted
        return current

    @staticmethod
    async def _lock(session: AsyncSession, client_id: str, resource_type: str, resource_key: str) -> None:
        """Serialize command handlers for one resource inside the domain transaction."""
        digest = hashlib.sha256(
            json.dumps(
                ["ep-direct-resource-v1", client_id, resource_type, resource_key], separators=(",", ":")
            ).encode()
        ).digest()
        await session.execute(select(func.pg_advisory_xact_lock(int.from_bytes(digest[:8], signed=True))))
