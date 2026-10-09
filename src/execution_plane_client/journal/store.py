"""Caller-owned PostgreSQL transactions for immutable encrypted exchange records."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol
from uuid import UUID, uuid4

from sqlalchemy import and_, or_, select, text, true, update
from sqlalchemy.dialects.postgresql import insert

from execution_plane_client.crypto import PayloadCipher, canonical_bytes
from execution_plane_client.errors import (
    CommandNotFound,
    InvalidCiphertext,
    ProtocolConflict,
    StaleClaim,
    UnauthorizedPeer,
)
from execution_plane_client.journal.intents import JournalIntents, intent_metadata
from execution_plane_client.journal.notifications import notify_journal
from execution_plane_client.journal.records import Direction, EnsuredOutbound, JournalEntry, LocalContext
from execution_plane_client.protocol import ChildCommandRead, ChildPage, CommandScope, MessageEnvelope, validate_json

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.sql.elements import ColumnElement

    from execution_plane_client.handlers import PreparedEffect
    from execution_plane_client.journal.schema import JournalSchema

_MAX_PAGE = 200
_MAX_CORRELATION_KEY_LENGTH = 255


class PeerIdentity(Protocol):
    """Verified caller identity required to acknowledge an outbound record."""

    @property
    def peer_id(self) -> str:
        """Return the authenticated sending participant."""
        ...

    @property
    def scope(self) -> CommandScope:
        """Return the assertion's verified initiating scope."""
        ...


def _associated_data(row: Mapping[str, object]) -> bytes:
    return json.dumps(
        [
            "exchange-journal-v1",
            str(row["entry_id"]),
            str(row["message_id"]),
            row["client_id"],
            str(row["project_id"]),
            row["all_projects"],
            row["direction"],
            row["envelope_hash"],
        ],
        separators=(",", ":"),
    ).encode()


def _context_associated_data(row: Mapping[str, object]) -> bytes:
    return (
        b"local-context-v1:"
        + _associated_data(row)
        + json.dumps([row["local_context_generation"], row["correlation_key"]], separators=(",", ":")).encode()
    )


class ExchangeJournalStore:
    """Append and read with explicit sessions; never commit or perform HTTP."""

    def __init__(self, schema: JournalSchema, cipher: PayloadCipher) -> None:
        """Use only participant-injected schema and encryption."""
        self.schema = schema
        self.cipher = cipher
        self.intents = JournalIntents(self)

    async def append_outbound(self, session: AsyncSession, message: MessageEnvelope) -> JournalEntry:
        """Freeze outbound intent in the caller's domain transaction."""
        await self.lock_outbound_command_identity(session, message.scope, message.command_id)
        row, _duplicate = await self._append(session, message, "outbound")
        return row

    async def ensure_outbound(
        self,
        session: AsyncSession,
        scope: CommandScope,
        command_id: UUID,
        *,
        command_factory: Callable[[], Awaitable[JournalEntry]],
        context_factory: Callable[[], Mapping[str, object]] | None = None,
        correlation_key: str | None = None,
    ) -> EnsuredOutbound:
        """Reuse a frozen command or create its intent and optional context atomically.

        The factories run only when the command does not exist. This keeps payload
        snapshots lazy and makes context creation part of the caller-owned transaction.
        """
        await self.lock_outbound_command_identity(session, scope, command_id)
        await self.lock_command_family(session, scope, command_id)
        entry = await self.get(session, scope, command_id, "outbound")
        if entry is None:
            created = True
            entry = await command_factory()
            if entry.command_id != command_id or entry.scope != scope or entry.direction != "outbound":
                raise ProtocolConflict
            local_context = None
            if context_factory is not None:
                local_context = await self.set_local_context(
                    session,
                    scope,
                    command_id,
                    data=context_factory(),
                    correlation_key=correlation_key,
                    expected_generation=0,
                )
        else:
            created = False
            local_context = await self.get_local_context(session, scope, command_id, for_update=True)
        return EnsuredOutbound(
            entry=entry,
            message=await self.load_message(session, entry.entry_id),
            local_context=local_context,
            created=created,
        )

    async def lock_outbound_command_identity(
        self, session: AsyncSession, scope: CommandScope, command_id: UUID
    ) -> None:
        """Serialize first creation even when no row exists to lock yet."""
        identity = f"exchange-journal-v1:{scope.client_id}:{command_id}:outbound"
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:identity, 0))"), {"identity": identity}
        )

    async def append_or_confirm_inbound(
        self,
        session: AsyncSession,
        message: MessageEnvelope,
    ) -> tuple[JournalEntry, bool]:
        """Persist receipt or confirm identical previously accepted content."""
        return await self._append(session, message, "inbound")

    async def _append(
        self,
        session: AsyncSession,
        message: MessageEnvelope,
        direction: Direction,
    ) -> tuple[JournalEntry, bool]:
        encoded = canonical_bytes(message)
        # All indexed fields come from the same snapshot as the authenticated bytes.
        snapshot = MessageEnvelope.model_validate_json(encoded)
        # Sender transactions may already own a command family. Admission locks
        # protect inbound reservations and must precede all recipient row locks.
        if direction == "inbound":
            await self.intents.lock(session, snapshot)
        command = snapshot.command
        now = datetime.now(UTC)
        row: dict[str, object] = {
            "entry_id": uuid4(),
            "message_id": snapshot.message_id,
            "command_id": snapshot.command_id,
            "parent_command_id": command.parent_command_id,
            "parent_sequence": command.parent_sequence,
            "message_type": snapshot.message_type,
            "command_type": command.command_type,
            "command_revision": command.command_revision,
            "sequence": snapshot.sequence,
            "source_id": snapshot.source_id,
            "destination_id": snapshot.destination_id,
            "client_id": snapshot.scope.client_id,
            "project_id": snapshot.scope.project_id,
            "all_projects": snapshot.scope.all_projects,
            "direction": direction,
            "target_url": command.target_url,
            "callback_url": command.callback_url,
            "envelope_hash": hashlib.sha256(encoded).hexdigest(),
            "delivery_state": "queued" if direction == "outbound" else "received",
            "handling_state": "idle" if direction == "outbound" else "pending",
            "delivery_due_at": now if direction == "outbound" else None,
            "handling_due_at": now if direction == "inbound" else None,
            "created_at": now,
            "received_at": now if direction == "inbound" else None,
            "updated_at": now,
        }
        row["envelope_ciphertext"] = self.cipher.encrypt(encoded, associated_data=_associated_data(row))
        row.update(intent_metadata(snapshot))
        table = self.schema.table
        created = (
            (
                await session.execute(
                    insert(table).values(**row).on_conflict_do_nothing().returning(table),
                )
            )
            .mappings()
            .one_or_none()
        )
        if created is not None:
            entry = await self.intents.reserve(session, JournalEntry.model_validate(dict(created)))
            await notify_journal(session, self.schema, "delivery" if direction == "outbound" else "handling")
            return entry, False
        # ON CONFLICT leaves unrelated caller state intact, including on duplicates.
        identities = [
            table.c.message_id == snapshot.message_id,
            and_(table.c.client_id == snapshot.scope.client_id, table.c.command_id == snapshot.command_id),
        ]
        if command.parent_sequence is not None:
            identities.append(
                and_(
                    table.c.client_id == snapshot.scope.client_id,
                    table.c.parent_command_id == command.parent_command_id,
                    table.c.parent_sequence == command.parent_sequence,
                )
            )
        found = (
            (
                await session.execute(
                    select(table).where(
                        table.c.direction == direction,
                        or_(*identities),
                    )
                )
            )
            .mappings()
            .all()
        )
        if len(found) != 1 or found[0]["envelope_hash"] != row["envelope_hash"]:
            error = "Immutable command or message identity conflict"
            raise ProtocolConflict(error)
        await notify_journal(session, self.schema, "delivery" if direction == "outbound" else "handling")
        return JournalEntry.model_validate(dict(found[0])), True

    def _scope_filter(self, scope: CommandScope) -> ColumnElement[bool]:
        table = self.schema.table
        # Explicit all-project scope can observe this client's project-scoped work.
        return and_(
            table.c.client_id == scope.client_id,
            true() if scope.all_projects else table.c.project_id == scope.project_id,
        )

    async def get(
        self,
        session: AsyncSession,
        scope: CommandScope,
        command_id: UUID,
        direction: Direction,
    ) -> JournalEntry | None:
        """Read one command without crossing authenticated client/project scope."""
        table = self.schema.table
        row = (
            (
                await session.execute(
                    select(table).where(
                        self._scope_filter(scope),
                        table.c.command_id == command_id,
                        table.c.direction == direction,
                    )
                )
            )
            .mappings()
            .one_or_none()
        )
        return JournalEntry.model_validate(dict(row)) if row is not None else None

    async def lock_command_family(self, session: AsyncSession, scope: CommandScope, command_id: UUID) -> None:
        """Lock the parent before outcome children, matching receipt transaction order."""
        table = self.schema.table
        await session.execute(
            select(table.c.entry_id)
            .where(self._scope_filter(scope), table.c.command_id == command_id)
            .order_by(table.c.entry_id)
            .with_for_update()
        )
        # Acceptance ingress locks its acknowledged command before its control
        # row. Controls have no participant effect and need no family lock.
        await session.execute(
            select(table.c.entry_id)
            .where(
                self._scope_filter(scope),
                table.c.parent_command_id == command_id,
                table.c.command_type != "command.accepted",
            )
            .order_by(table.c.entry_id)
            .with_for_update()
        )

    async def rearm_handling(
        self, session: AsyncSession, scope: CommandScope, command_id: UUID, direction: Direction
    ) -> bool:
        """Retry local handling after a domain effect destination changes, fencing old attempts.

        Call in the same transaction as the correlation update, after locking the
        command family. Only inbound messages and rejected outbound commands have
        local handling obligations; this never resubmits an accepted command.
        """
        table = self.schema.table
        now = datetime.now(UTC)
        entry_id = await session.scalar(
            update(table)
            .where(
                self._scope_filter(scope),
                table.c.command_id == command_id,
                table.c.direction == direction,
                or_(table.c.direction == "inbound", table.c.delivery_state == "rejected"),
            )
            .values(
                handling_state="pending",
                handling_due_at=now,
                handling_owner=None,
                handling_generation=table.c.handling_generation + 1,
                handling_lease_until=None,
                prepared_effect_ciphertext=None,
                handled_at=None,
                reconciliation_required=False,
                reconciliation_due_at=None,
                reconciliation_error_code=None,
                handling_error_code=None,
                observation_pending=False,
                updated_at=now,
            )
            .returning(table.c.entry_id)
        )
        if entry_id is not None:
            await notify_journal(session, self.schema, "handling")
        return entry_id is not None

    async def get_local_context(
        self,
        session: AsyncSession,
        scope: CommandScope,
        command_id: UUID,
        direction: Direction = "outbound",
        *,
        for_update: bool = False,
    ) -> LocalContext | None:
        """Read private correlation after a scoped lookup; never expose it to peers."""
        table = self.schema.table
        statement = select(table).where(
            self._scope_filter(scope),
            table.c.command_id == command_id,
            table.c.direction == direction,
        )
        if for_update:
            statement = statement.with_for_update()
        row = (await session.execute(statement)).mappings().one_or_none()
        if row is None or row["local_context_ciphertext"] is None:
            return None
        data = self.cipher.decrypt(row["local_context_ciphertext"], associated_data=_context_associated_data(dict(row)))
        value = json.loads(data)
        if not isinstance(value, dict):
            raise InvalidCiphertext
        return LocalContext(row["entry_id"], row["local_context_generation"], row["correlation_key"], value)

    async def set_local_context(
        self,
        session: AsyncSession,
        scope: CommandScope,
        command_id: UUID,
        *,
        data: Mapping[str, object],
        correlation_key: str | None,
        expected_generation: int,
        direction: Direction = "outbound",
    ) -> LocalContext:
        """Replace encrypted local correlation atomically with caller state, fencing stale writers."""
        validate_json(dict(data), max_bytes=None)
        if correlation_key is not None and len(correlation_key) > _MAX_CORRELATION_KEY_LENGTH:
            raise ProtocolConflict
        table = self.schema.table
        row = (
            (
                await session.execute(
                    select(table)
                    .where(
                        self._scope_filter(scope),
                        table.c.command_id == command_id,
                        table.c.direction == direction,
                    )
                    .with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise CommandNotFound
        if row["local_context_generation"] != expected_generation:
            raise StaleClaim
        generation = expected_generation + 1
        snapshot = json.dumps(dict(data), allow_nan=False, separators=(",", ":")).encode()
        values = dict(row)
        values.update(local_context_generation=generation, correlation_key=correlation_key)
        ciphertext = self.cipher.encrypt(snapshot, associated_data=_context_associated_data(values))
        await session.execute(
            update(table)
            .where(table.c.entry_id == row["entry_id"])
            .values(
                local_context_ciphertext=ciphertext,
                local_context_generation=generation,
                correlation_key=correlation_key,
                updated_at=datetime.now(UTC),
            )
        )
        return LocalContext(row["entry_id"], generation, correlation_key, json.loads(snapshot))

    async def list_local_context_entries(
        self,
        session: AsyncSession,
        *,
        client_id: str,
        correlation_key: str,
        command_type: str,
        after: UUID | None = None,
        limit: int = 100,
    ) -> list[JournalEntry]:
        """Page a participant's private outbound correlation group without opening payloads."""
        table = self.schema.table
        statement = select(table).where(
            table.c.client_id == client_id,
            table.c.correlation_key == correlation_key,
            table.c.command_type == command_type,
            table.c.direction == "outbound",
            table.c.local_context_ciphertext.is_not(None),
        )
        if after is not None:
            statement = statement.where(table.c.entry_id > after)
        rows = (
            (await session.execute(statement.order_by(table.c.entry_id).limit(max(1, min(limit, _MAX_PAGE)))))
            .mappings()
            .all()
        )
        return [JournalEntry.model_validate(dict(row)) for row in rows]

    async def load_prepared_effect(self, session: AsyncSession, entry_id: UUID) -> PreparedEffect | None:
        """Read private effect input after a scoped lookup or fenced claim."""
        from execution_plane_client.journal.effects import restore_effect  # noqa: PLC0415

        table = self.schema.table
        row = (await session.execute(select(table).where(table.c.entry_id == entry_id))).mappings().one_or_none()
        if row is None:
            raise CommandNotFound
        if row["prepared_effect_ciphertext"] is None:
            return None
        return restore_effect(self.cipher, row["prepared_effect_ciphertext"], entry_id, row["command_id"])

    async def load_message(self, session: AsyncSession, entry_id: UUID) -> MessageEnvelope:
        """Decrypt an internal record after a scoped lookup or fenced claim."""
        table = self.schema.table
        row = (await session.execute(select(table).where(table.c.entry_id == entry_id))).mappings().one_or_none()
        if row is None:
            raise CommandNotFound
        data = self.cipher.decrypt(row["envelope_ciphertext"], associated_data=_associated_data(dict(row)))
        if hashlib.sha256(data).hexdigest() != row["envelope_hash"]:
            raise InvalidCiphertext
        return MessageEnvelope.model_validate_json(data)

    async def list_children(
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
        """Return ordered outcomes, excluding acceptance control from the cursor."""
        if after < 0 or not 1 <= limit <= _MAX_PAGE:
            error = "Invalid child page bounds"
            raise ValueError(error)
        if include_recovery and peer_id is None:
            error = "Recovery requires an authenticated peer"
            raise UnauthorizedPeer(error)
        table = self.schema.table
        query = select(table).where(
            self._scope_filter(scope),
            table.c.parent_command_id == command_id,
            table.c.parent_sequence > after,
        )
        if peer_id is not None:
            query = query.where(or_(table.c.source_id == peer_id, table.c.destination_id == peer_id))
        rows = (
            (
                await session.execute(
                    query.order_by(table.c.parent_sequence).limit(limit + 1),
                )
            )
            .mappings()
            .all()
        )
        children = []
        for row in rows[:limit]:
            entry = JournalEntry.model_validate(dict(row))
            message = await self.load_message(session, entry.entry_id) if include_recovery else None
            children.append(
                ChildCommandRead(
                    command_id=entry.command_id,
                    command_type=entry.command_type,
                    status=entry.command_status,
                    parent_command_id=entry.parent_command_id,
                    parent_sequence=entry.parent_sequence,
                    delivery_state=entry.delivery_state,
                    handling_state=entry.handling_state,
                    created_at=entry.created_at,
                    accepted_at=entry.accepted_at,
                    handled_at=entry.handled_at,
                    reconciliation_required=entry.reconciliation_required,
                    resource=entry.resource,
                    error_code=entry.command_error_code or entry.handling_error_code or entry.delivery_error_code,
                    recovery_message=message,
                )
            )
        return ChildPage(items=children, next_after=children[-1].parent_sequence if len(rows) > limit else None)

    async def acknowledge(
        self,
        session: AsyncSession,
        acceptance: MessageEnvelope,
        identity: PeerIdentity,
    ) -> JournalEntry:
        """Acknowledge only the exact message sent to this authenticated peer."""
        command = acceptance.command
        if command.command_type != "command.accepted" or command.acknowledges_message_id is None:
            raise ProtocolConflict
        table = self.schema.table
        row = (
            (
                await session.execute(
                    select(table)
                    .where(
                        table.c.message_id == command.acknowledges_message_id,
                        table.c.direction == "outbound",
                    )
                    .with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise CommandNotFound
        entry = JournalEntry.model_validate(dict(row))
        if entry.command_type == "command.accepted":
            error = "Acceptance does not accept another acceptance"
            raise ProtocolConflict(error)
        if (
            identity.peer_id != entry.destination_id
            or acceptance.source_id != entry.destination_id
            or acceptance.destination_id != entry.source_id
            or identity.scope != entry.scope
            or acceptance.scope != entry.scope
            or command.parent_command_id != entry.command_id
            or command.target_url != entry.callback_url
            or command.callback_url != entry.target_url
        ):
            raise UnauthorizedPeer
        now = datetime.now(UTC)
        reconciliation: dict[str, object] = {}
        if entry.delivery_state == "rejected":
            # A conflicting late receipt must not silently erase local actor
            # failure responsibility or overwrite an already received completion.
            reconciliation = {
                "reconciliation_required": True,
                "reconciliation_due_at": now,
                "reconciliation_error_code": "acceptance_after_rejection",
                "handling_state": "reconciling",
                "handling_generation": table.c.handling_generation + 1,
                "handling_owner": None,
                "handling_lease_until": None,
            }
        changed = (
            (
                await session.execute(
                    update(table)
                    .where(table.c.entry_id == entry.entry_id)
                    .values(
                        delivery_state="acknowledged",
                        accepted_at=entry.accepted_at or now,
                        delivery_owner=None,
                        delivery_lease_until=None,
                        delivery_due_at=None,
                        acceptance_due_at=None,
                        delivery_error_code=None,
                        updated_at=now,
                        **reconciliation,
                    )
                    .returning(table)
                )
            )
            .mappings()
            .one()
        )
        return JournalEntry.model_validate(dict(changed))
