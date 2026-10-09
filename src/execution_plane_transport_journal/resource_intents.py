"""Journal-backed resource revision and physical identity adapter."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import select, update

from execution_plane_client.errors import ProtocolConflict, UnauthorizedPeer
from execution_plane_client.resources import ResourceIntent, ResourceIntentStore
from execution_plane_transport_journal.intents import intent_metadata
from execution_plane_transport_journal.records import JournalEntry

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.protocol import MessageEnvelope
    from execution_plane_transport_journal.store import ExchangeJournalStore


class JournalResourceIntentStore(ResourceIntentStore):
    """Read and update resource intent metadata stored on Journal rows."""

    def __init__(self, store: ExchangeJournalStore) -> None:
        """Use the participant's Journal schema and transaction."""
        self._store = store

    async def resource(self, session: AsyncSession, message: MessageEnvelope) -> ResourceIntent:
        """Lock the current exact-scope reservation before participant domain rows."""
        await self._store.intents.lock(session, message)
        metadata = intent_metadata(message)
        if "desired_revision" not in metadata:
            raise ProtocolConflict
        table = self._store.schema.table
        row = (
            (
                await session.execute(
                    select(table)
                    .where(
                        table.c.client_id == message.scope.client_id,
                        table.c.resource_type == metadata["resource_type"],
                        table.c.resource_key == metadata["resource_key"],
                        table.c.resource_head,
                    )
                    .with_for_update(),
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise ProtocolConflict
        entry = JournalEntry.model_validate(dict(row))
        if entry.scope != message.scope:
            raise UnauthorizedPeer
        if entry.resource_type is None or entry.resource_key is None:
            raise ProtocolConflict
        if entry.desired_revision is None or entry.desired_hash is None:
            raise ProtocolConflict
        return ResourceIntent(
            scope=entry.scope,
            client_id=entry.client_id,
            resource_type=entry.resource_type,
            resource_key=entry.resource_key,
            desired_revision=entry.desired_revision,
            desired_hash=entry.desired_hash,
            entry_id=entry.entry_id,
            observed_revision=entry.observed_revision,
            resource_id=entry.resource_id,
            resource_deleted=entry.resource_deleted,
        )

    async def observe_resource(
        self,
        session: AsyncSession,
        message: MessageEnvelope,
        *,
        resource_id: UUID | None,
        observed_revision: int | None = None,
        deleted: bool | None = None,
    ) -> ResourceIntent:
        """Persist the last physical observation on the current reservation."""
        head = await self.resource(session, message)
        metadata = intent_metadata(message)
        if head.desired_revision != metadata["desired_revision"] or head.desired_hash != metadata["desired_hash"]:
            raise ProtocolConflict
        if head.entry_id is None:
            raise ProtocolConflict
        if resource_id is not None:
            await self._store.intents.lock_key(session, ["physical", str(head.resource_type), str(resource_id)])
            owner = await self.owner(session, str(head.resource_type), resource_id)
            if owner is not None and owner.entry_id != head.entry_id:
                raise ProtocolConflict
        values: dict[str, object] = {"resource_id": resource_id, "updated_at": datetime.now(UTC)}
        if observed_revision is not None:
            if observed_revision < head.observed_revision or observed_revision > int(str(metadata["desired_revision"])):
                raise ProtocolConflict
            values["observed_revision"] = observed_revision
        if deleted is not None:
            values["resource_deleted"] = deleted
        table = self._store.schema.table
        changed = (
            (
                await session.execute(
                    update(table).where(table.c.entry_id == head.entry_id).values(**values).returning(table),
                )
            )
            .mappings()
            .one()
        )
        entry = JournalEntry.model_validate(dict(changed))
        if entry.resource_type is None or entry.resource_key is None:
            raise ProtocolConflict
        if entry.desired_revision is None or entry.desired_hash is None:
            raise ProtocolConflict
        return ResourceIntent(
            scope=entry.scope,
            client_id=entry.client_id,
            resource_type=entry.resource_type,
            resource_key=entry.resource_key,
            desired_revision=entry.desired_revision,
            desired_hash=entry.desired_hash,
            entry_id=entry.entry_id,
            observed_revision=entry.observed_revision,
            resource_id=entry.resource_id,
            resource_deleted=entry.resource_deleted,
        )

    async def owner(self, session: AsyncSession, resource_type: str, resource_id: UUID) -> JournalEntry | None:
        """Resolve a physical identity through its current logical resource head."""
        table = self._store.schema.table
        row = (
            (
                await session.execute(
                    select(table).where(
                        table.c.resource_type == resource_type,
                        table.c.resource_id == resource_id,
                        table.c.resource_head,
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        return JournalEntry.model_validate(dict(row)) if row is not None else None
