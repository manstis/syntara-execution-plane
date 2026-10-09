"""Atomic Journal completion recorder for EP work items."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast
from uuid import UUID

from sqlalchemy import select, update

from execution_plane.models.work_item import WorkItemStatus
from execution_plane_client.completion import completion_result
from execution_plane_client.errors import ProtocolConflict
from execution_plane_client.handlers import CompletionRecorder, HandlingResult
from execution_plane_client.protocol import CommandScope, CommandStatus, ResourceReference
from execution_plane_transport_journal.completion import append_completion

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane.models.work_item import WorkItem
    from execution_plane_transport_journal.store import ExchangeJournalStore


class JournalCompletionRecorder(CompletionRecorder):
    """Commit terminal/unknown state and its original parent's child atomically."""

    def __init__(self, journal: ExchangeJournalStore) -> None:
        """Borrow the participant's encrypted exchange journal."""
        self._journal = journal

    async def __call__(self, session: object, item: object) -> None:
        """Persist completion responsibility; absence of command linkage is an invariant failure."""
        session = cast("AsyncSession", session)
        item = cast("WorkItem", item)
        command_id = UUID(item.request_id)
        scope = CommandScope(client_id=item.client_id, project_id=item.project_id, all_projects=False)
        table = self._journal.schema.table
        entry_id = await session.scalar(
            select(table.c.entry_id)
            .where(
                table.c.client_id == item.client_id,
                table.c.project_id == item.project_id,
                table.c.command_id == command_id,
                table.c.direction == "inbound",
            )
            .with_for_update()
        )
        if entry_id is None:
            error = "Command-backed work completion has no journal parent"
            raise RuntimeError(error)
        message = await self._journal.load_message(session, entry_id)
        record = await self._journal.get(session, scope, command_id, "inbound")
        if record is None or record.resource_id != item.id or record.command_type != "work.execute":
            error = "Work completion does not match the accepted command resource"
            raise RuntimeError(error)
        status = {
            WorkItemStatus.COMPLETED.value: CommandStatus.SUCCEEDED,
            WorkItemStatus.FAILED.value: CommandStatus.FAILED,
            WorkItemStatus.CANCELLED.value: CommandStatus.CANCELLED,
            WorkItemStatus.RECONCILIATION_REQUIRED.value: CommandStatus.OUTCOME_UNKNOWN,
        }[item.status.value]
        result = HandlingResult(
            status=status,
            resource=ResourceReference(kind="work_item", resource_id=item.id),
            result={
                "work_id": str(item.id),
                "work_correlation_id": message.command.payload["work_correlation_id"],
                "result": item.result,
            },
            completion_required=True,
            error_code="work_outcome_unknown" if status == CommandStatus.OUTCOME_UNKNOWN else None,
        )
        child = await append_completion(self._journal, session, message, result, occurred_at=item.completed_at)
        result = completion_result(child)
        if result.status is None:
            raise ProtocolConflict
        status = result.status
        await self._journal.intents.record_result(session, entry_id, result)
        await session.execute(
            update(table)
            .where(table.c.entry_id == entry_id)
            .values(
                command_status=status.value,
                reconciliation_required=status == CommandStatus.OUTCOME_UNKNOWN,
                reconciliation_error_code=result.error_code,
                updated_at=item.completed_at,
            )
        )

    async def after_commit(self, _item: object) -> None:
        """Journal notification is transactional, so no post-commit send is needed."""
