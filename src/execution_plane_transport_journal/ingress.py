"""Journal receipt adapter around persistence-neutral command validation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import func, select, update

from execution_plane_client.commands.control import Completion
from execution_plane_client.errors import UnauthorizedPeer
from execution_plane_client.handlers import HandlingResult, ReceiptPolicy
from execution_plane_client.ingress import CommandIngress
from execution_plane_client.observers import NullObserver
from execution_plane_client.protocol import CommandStatus, MessageEnvelope
from execution_plane_transport_journal.records import JournalEntry

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.handlers import CommandRegistry
    from execution_plane_client.observers import ExchangeObserver
    from execution_plane_client.security import AuthenticatedIdentity
    from execution_plane_transport_journal.store import ExchangeJournalStore


@dataclass(frozen=True)
class ReceiptResult:
    """A committed caller transaction may safely return this receipt to HTTP."""

    entry: JournalEntry
    duplicate: bool


class CommandReceiptHandler:
    """Persist inbound responsibility and completion state in the caller transaction."""

    def __init__(
        self,
        store: ExchangeJournalStore,
        catalog: CommandCatalog,
        registry: CommandRegistry,
        *,
        local_id: str,
        local_role: str = "executor",
        receipt_hook: ReceiptPolicy | None = None,
        observer: ExchangeObserver | None = None,
    ) -> None:
        """Configure participant hooks after transport identity verification."""
        self._store = store
        self._ingress = CommandIngress(catalog, registry, local_id=local_id, local_role=local_role)
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
        """Join the caller transaction without waiting for any network activity."""
        resolved = self._ingress.resolve(message, identity)
        await self._store.intents.lock(session, message)
        parent = await self._completion_parent(message, session) if resolved.definition.kind == "completion" else None
        entry, duplicate = await self._store.append_or_confirm_inbound(session, message)
        if parent is not None:
            await self._observe_completion(message, parent, session)
        if not duplicate and self._hook is not None:
            await self._hook(message, identity, session)
        self._observer.record(
            "receipt.persisted",
            {"message_id": str(entry.message_id), "command_id": str(entry.command_id), "duplicate": duplicate},
        )
        return ReceiptResult(entry, duplicate)
