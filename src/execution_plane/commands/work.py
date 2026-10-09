"""WorkStore command adapter and atomic provider-completion journal recorder."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import select, update

from execution_plane.models.work_item import WorkItemStatus
from execution_plane.work_store import WorkStore
from execution_plane_client.commands.work import WorkExecute
from execution_plane_client.completion import append_completion, completion_result
from execution_plane_client.errors import CommandExecutionError, ProtocolConflict
from execution_plane_client.handlers import HandlingResult
from execution_plane_client.protocol import CommandScope, CommandStatus, ResourceReference

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane.models.work_item import WorkItem
    from execution_plane_client.handlers import HandlerContext
    from execution_plane_client.journal.store import ExchangeJournalStore
    from execution_plane_client.protocol import MessageEnvelope


class WorkCommandHandler:
    """Enqueue provider work without performing provider I/O in handling."""

    def __init__(self, journal: ExchangeJournalStore) -> None:
        """Use the same journal for accepted work and eventual terminal children."""
        self._journal = journal

    async def handle(self, message: MessageEnvelope, _context: HandlerContext, session: AsyncSession) -> HandlingResult:
        """Persist stable command/work linkage and honor cancellation arriving first."""
        if message.scope.project_id is None:
            raise ProtocolConflict
        await self._journal.intents.lock(session, message)
        if await self._journal.intents.cancelled(session, message):
            return HandlingResult(status=CommandStatus.CANCELLED, completion_required=True)
        payload = WorkExecute.model_validate(message.command.payload)
        policy = message.command.execution_policy
        if policy is not None and policy.timeout_seconds != payload.invocation.timeout_seconds:
            code = "execution_timeout_mismatch"
            raise CommandExecutionError(code)
        work_payload = payload.model_dump(mode="json", exclude={"work_correlation_id", "workload_type"})
        work_payload.update(
            {"_ep_command_id": str(message.command_id), "_ep_max_attempts": policy.max_attempts if policy else None}
        )
        store = WorkStore.from_transaction(session)
        store.set_completion_recorder(EPCompletionRecorder(self._journal))
        item = await store.dispatch(
            client_id=message.scope.client_id,
            project_id=message.scope.project_id,
            request_id=str(message.command_id),
            work_correlation_id=payload.work_correlation_id,
            payload=work_payload,
        )
        return HandlingResult(
            status=CommandStatus.RUNNING, resource=ResourceReference(kind="work_item", resource_id=item.id)
        )


class EPCompletionRecorder:
    """Commit terminal/unknown state and its original parent's child atomically."""

    def __init__(self, journal: ExchangeJournalStore) -> None:
        """Borrow the participant's encrypted exchange journal."""
        self._journal = journal

    async def __call__(self, session: AsyncSession, item: WorkItem) -> None:
        """Persist completion responsibility; absence of command linkage is an invariant failure."""
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
