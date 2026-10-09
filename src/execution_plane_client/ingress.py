"""Atomic durable receipt and finite bilateral acceptance."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol
from uuid import UUID, uuid5

from sqlalchemy import func, select, update

from execution_plane_client.commands.control import Completion
from execution_plane_client.errors import UnauthorizedPeer
from execution_plane_client.handlers import HandlingResult
from execution_plane_client.journal.records import JournalEntry
from execution_plane_client.observers import NullObserver
from execution_plane_client.protocol import Command, CommandStatus, MessageEnvelope

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.handlers import CommandRegistry
    from execution_plane_client.journal.store import ExchangeJournalStore
    from execution_plane_client.observers import ExchangeObserver
    from execution_plane_client.security import AuthenticatedIdentity

# Fixed protocol namespaces; never regenerate between processes or releases.
ACCEPTANCE_COMMAND_NAMESPACE = UUID("8d4b709a-c518-4eec-9e76-d096f0b1c099")
ACCEPTANCE_MESSAGE_NAMESPACE = UUID("6b0c587d-9451-4385-913c-085a7e1a765d")


class ReceiptHook(Protocol):
    """Establish participant-owned responsibility in the inbound transaction."""

    async def __call__(
        self,
        message: MessageEnvelope,
        identity: AuthenticatedIdentity,
        session: AsyncSession,
    ) -> None:
        """Apply domain acceptance without committing or external calls."""
        ...


@dataclass(frozen=True)
class ReceiptResult:
    """A committed caller transaction may safely return this receipt to HTTP."""

    entry: JournalEntry
    duplicate: bool


def acceptance_for(message: MessageEnvelope, received_at: datetime) -> MessageEnvelope:
    """Derive stable acceptance identity, routing, and original-message linkage."""
    command_id = uuid5(ACCEPTANCE_COMMAND_NAMESPACE, str(message.message_id))
    return MessageEnvelope(
        message_id=uuid5(ACCEPTANCE_MESSAGE_NAMESPACE, str(message.message_id)),
        command_id=command_id,
        source_id=message.destination_id,
        destination_id=message.source_id,
        scope=message.scope,
        command=Command(
            command_id=command_id,
            parent_command_id=message.command_id,
            command_type="command.accepted",
            command_revision=1,
            target_url=message.command.callback_url,
            callback_url=message.command.target_url,
            payload={"command_id": str(message.command_id), "message_id": str(message.message_id)},
            occurred_at=received_at,
            acknowledges_message_id=message.message_id,
        ),
    )


class CommandReceiptHandler:
    """Persist inbound responsibility and outbound acceptance before HTTP success."""

    def __init__(
        self,
        store: ExchangeJournalStore,
        catalog: CommandCatalog,
        registry: CommandRegistry,
        *,
        local_id: str,
        local_role: str = "executor",
        receipt_hook: ReceiptHook | None = None,
        observer: ExchangeObserver | None = None,
    ) -> None:
        """Configure participant hooks; identity verification precedes this boundary."""
        self._store = store
        self._catalog = catalog
        self._registry = registry
        self._local_id = local_id
        self._local_role = local_role
        self._hook = receipt_hook
        self._observer = observer or NullObserver()

    async def _completion_parent(self, message: MessageEnvelope, session: AsyncSession) -> JournalEntry:
        table = self._store.schema.table
        row = (
            (
                await session.execute(
                    select(table)
                    .where(
                        table.c.command_id == message.command.parent_command_id,
                        table.c.client_id == message.scope.client_id,
                        table.c.direction == "outbound",
                    )
                    .with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise UnauthorizedPeer
        parent = JournalEntry.model_validate(dict(row))
        if (
            parent.scope != message.scope
            or parent.destination_id != message.source_id
            or parent.source_id != message.destination_id
            or message.command.command_type != f"{parent.command_type}.completed"
            or message.command.target_url != parent.callback_url
            or message.command.callback_url != parent.target_url
        ):
            raise UnauthorizedPeer
        return parent

    async def _observe_completion(
        self,
        message: MessageEnvelope,
        parent: JournalEntry,
        session: AsyncSession,
    ) -> None:
        table = self._store.schema.table
        latest = await session.scalar(
            select(func.max(table.c.parent_sequence)).where(
                table.c.parent_command_id == parent.command_id,
                table.c.client_id == parent.client_id,
                table.c.direction == "inbound",
            )
        )
        if message.command.parent_sequence != latest:
            return
        outcome = Completion.model_validate(message.command.payload)
        await self._store.intents.record_result(
            session,
            parent.entry_id,
            HandlingResult(
                status=CommandStatus(outcome.status),
                resource=outcome.resource,
                error_code=outcome.error_code,
            ),
        )
        values: dict[str, object] = {"command_status": outcome.status, "updated_at": datetime.now(UTC)}
        if outcome.status == "outcome_unknown":
            values.update(reconciliation_required=True, reconciliation_error_code="remote_outcome_unknown")
        elif parent.reconciliation_error_code in {None, "remote_outcome_unknown"}:
            values.update(reconciliation_required=False, reconciliation_error_code=None)
        if parent.delivery_state == "rejected" and parent.handling_state == "pending":
            values.update(
                handling_state="idle", handling_due_at=None, handling_generation=table.c.handling_generation + 1
            )
        await session.execute(update(table).where(table.c.entry_id == parent.entry_id).values(**values))

    async def receive(
        self,
        message: MessageEnvelope,
        identity: AuthenticatedIdentity,
        session: AsyncSession,
    ) -> ReceiptResult:
        """Join the caller's transaction without waiting for any network activity."""
        self._catalog.validate(message)
        command = message.command
        definition = self._catalog.definition(command.command_type, command.command_revision)
        if (
            identity.peer_id != message.source_id
            or identity.scope != message.scope
            or command.command_type not in identity.scopes
            or message.destination_id != self._local_id
            or identity.role not in definition.source_roles
            or self._local_role not in definition.destination_roles
        ):
            self._observer.record("receipt.unauthorized", {"command_id": str(message.command_id)})
            raise UnauthorizedPeer
        if definition.kind != "acceptance":
            self._registry.get(command.command_type, command.command_revision)
        await self._store.intents.lock(session, message)
        parent = await self._completion_parent(message, session) if definition.kind == "completion" else None
        entry, duplicate = await self._store.append_or_confirm_inbound(session, message)
        table = self._store.schema.table
        if definition.kind == "acceptance":
            await self._store.acknowledge(session, message, identity)
            await session.execute(
                update(table)
                .where(table.c.entry_id == entry.entry_id)
                .values(
                    handling_state="handled",
                    handled_at=entry.handled_at or datetime.now(UTC),
                    handling_due_at=None,
                )
            )
        else:
            if parent is not None:
                await self._observe_completion(message, parent, session)
            if not duplicate and self._hook is not None:
                await self._hook(message, identity, session)
            acceptance = acceptance_for(message, entry.created_at)
            outbound = await self._store.append_outbound(session, acceptance)
            if duplicate:
                # Reopen this exact control message, fencing any old HTTP response.
                await session.execute(
                    update(table)
                    .where(table.c.entry_id == outbound.entry_id)
                    .values(
                        delivery_state="queued",
                        delivery_due_at=datetime.now(UTC),
                        delivery_owner=None,
                        delivery_lease_until=None,
                        delivery_generation=table.c.delivery_generation + 1,
                    )
                )
        self._observer.record(
            "receipt.duplicate_staged" if duplicate else "receipt.staged",
            {"command_id": str(message.command_id), "message_id": str(message.message_id)},
        )
        return ReceiptResult(entry, duplicate)
