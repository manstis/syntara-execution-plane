"""Indexed command intent and physical observations on actual journal rows only."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import func, select, update

from execution_plane_client.errors import CommandNotFound, ProtocolConflict, UnauthorizedPeer
from execution_plane_client.journal.records import JournalEntry

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.handlers import HandlingResult
    from execution_plane_client.journal.store import ExchangeJournalStore
    from execution_plane_client.protocol import MessageEnvelope


def desired_hash(message: MessageEnvelope) -> str:
    """Hash domain intent independently of delivery and command identity."""
    return hashlib.sha256(
        json.dumps(
            {"type": message.command.command_type, "payload": message.command.payload},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def intent_metadata(message: MessageEnvelope) -> dict[str, object]:
    """Index only standard resource actions and cancellation targets."""
    command = message.command
    resource_type, _, action = command.command_type.partition(".")
    if resource_type in {"cluster", "execution_target"} and "." not in action:
        payload = command.payload
        return {
            "resource_type": resource_type,
            "resource_key": str(payload["resource_key"]),
            "desired_revision": int(str(payload["desired_revision"])),
            "desired_hash": desired_hash(message),
        }
    if command.command_type == "command.cancel":
        return {"target_command_id": UUID(str(command.payload["target_command_id"]))}
    return {}


class JournalIntents:
    """Use caller sessions for reservations, cancellation and observed results."""

    def __init__(self, store: ExchangeJournalStore) -> None:
        """Use only the injected participant journal."""
        self.store = store

    @staticmethod
    async def _lock_key(session: AsyncSession, identity: list[str]) -> None:
        digest = hashlib.sha256(json.dumps(["journal-intent-v1", *identity], separators=(",", ":")).encode()).digest()
        await session.execute(select(func.pg_advisory_xact_lock(int.from_bytes(digest[:8], signed=True))))

    async def lock(self, session: AsyncSession, message: MessageEnvelope) -> None:
        """Acquire logical intent before insertion or command-family row locks."""
        metadata = intent_metadata(message)
        if "desired_revision" in metadata:
            await self._lock_key(
                session,
                ["resource", message.scope.client_id, str(metadata["resource_type"]), str(metadata["resource_key"])],
            )
        elif message.command.command_type in {"command.cancel", "work.execute"}:
            target = metadata.get("target_command_id") or message.command_id
            await self._lock_key(session, ["command", message.scope.client_id, str(target)])

    async def resource(self, session: AsyncSession, message: MessageEnvelope) -> JournalEntry:
        """Lock the current exact-scope reservation before participant domain rows."""
        await self.lock(session, message)
        metadata = intent_metadata(message)
        if "desired_revision" not in metadata:
            raise ProtocolConflict
        table = self.store.schema.table
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
        return entry

    async def reserve(self, session: AsyncSession, entry: JournalEntry) -> JournalEntry:
        """Transfer the newest inbound resource head without synthetic rows."""
        if entry.direction != "inbound" or entry.desired_revision is None:
            return entry
        table = self.store.schema.table
        row = (
            (
                await session.execute(
                    select(table)
                    .where(
                        table.c.client_id == entry.client_id,
                        table.c.resource_type == entry.resource_type,
                        table.c.resource_key == entry.resource_key,
                        table.c.resource_head,
                    )
                    .with_for_update(),
                )
            )
            .mappings()
            .one_or_none()
        )
        values: dict[str, object] = {"resource_head": True}
        if row is not None:
            head = JournalEntry.model_validate(dict(row))
            if head.scope != entry.scope:
                raise UnauthorizedPeer
            if head.desired_revision == entry.desired_revision and head.desired_hash != entry.desired_hash:
                raise ProtocolConflict
            if head.desired_revision is None:
                raise ProtocolConflict
            if head.desired_revision >= entry.desired_revision:
                return entry
            values.update(
                resource_id=head.resource_id,
                observed_revision=head.observed_revision,
                resource_deleted=head.resource_deleted,
            )
            await session.execute(update(table).where(table.c.entry_id == head.entry_id).values(resource_head=False))
        changed = (
            (
                await session.execute(
                    update(table).where(table.c.entry_id == entry.entry_id).values(**values).returning(table),
                )
            )
            .mappings()
            .one()
        )
        return JournalEntry.model_validate(dict(changed))

    async def observe_resource(
        self,
        session: AsyncSession,
        message: MessageEnvelope,
        *,
        resource_id: UUID | None,
        observed_revision: int | None = None,
        deleted: bool | None = None,
    ) -> JournalEntry:
        """Persist the last physical observation on the current reservation."""
        head = await self.resource(session, message)
        metadata = intent_metadata(message)
        if head.desired_revision != metadata["desired_revision"] or head.desired_hash != metadata["desired_hash"]:
            raise ProtocolConflict
        if resource_id is not None:
            await self._lock_key(session, ["physical", str(head.resource_type), str(resource_id)])
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
        table = self.store.schema.table
        changed = (
            (
                await session.execute(
                    update(table).where(table.c.entry_id == head.entry_id).values(**values).returning(table),
                )
            )
            .mappings()
            .one()
        )
        return JournalEntry.model_validate(dict(changed))

    async def owner(self, session: AsyncSession, resource_type: str, resource_id: UUID) -> JournalEntry | None:
        """Resolve a physical identity through its current logical resource head."""
        table = self.store.schema.table
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

    async def cancelled(self, session: AsyncSession, message: MessageEnvelope) -> bool:
        """Check accepted inbound cancellation intent within the exact original scope."""
        await self.lock(session, message)
        table = self.store.schema.table
        entry_id = await session.scalar(
            select(table.c.entry_id)
            .where(
                table.c.client_id == message.scope.client_id,
                table.c.project_id.is_(None)
                if message.scope.project_id is None
                else table.c.project_id == message.scope.project_id,
                table.c.all_projects == message.scope.all_projects,
                table.c.target_command_id == message.command_id,
                table.c.direction == "inbound",
                table.c.command_type == "command.cancel",
            )
            .limit(1)
        )
        return entry_id is not None

    async def record_result(self, session: AsyncSession, entry_id: UUID, result: HandlingResult) -> None:
        """Persist command status, safe errors and physical association atomically."""
        values: dict[str, object] = {"command_error_code": result.error_code, "updated_at": datetime.now(UTC)}
        if result.status is not None:
            values["command_status"] = result.status.value
        if result.resource is not None:
            values.update(resource_type=result.resource.kind, resource_id=result.resource.resource_id)
            if result.resource.resource_key is not None:
                values["resource_key"] = str(result.resource.resource_key)
            if result.resource.revision is not None:
                values["observed_revision"] = result.resource.revision
        table = self.store.schema.table
        changed = await session.scalar(
            update(table).where(table.c.entry_id == entry_id).values(**values).returning(table.c.entry_id)
        )
        if changed is None:
            raise CommandNotFound
