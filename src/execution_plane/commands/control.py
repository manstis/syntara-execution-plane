"""Work cancellation acts on the intent and resource recorded in the journal."""

from __future__ import annotations

from typing import TYPE_CHECKING

from execution_plane.commands.work import EPCompletionRecorder
from execution_plane.models.work_item import WorkItemStatus
from execution_plane.work_store import WorkStore
from execution_plane_client.commands.control import CancelCommand
from execution_plane_client.errors import CommandExecutionError
from execution_plane_client.handlers import HandlingResult
from execution_plane_client.protocol import CommandScope, CommandStatus

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.handlers import HandlerContext
    from execution_plane_client.journal.store import ExchangeJournalStore
    from execution_plane_client.protocol import MessageEnvelope


class CancelCommandHandler:
    """Request provider cancellation while the journal owns durable intent."""

    def __init__(self, journal: ExchangeJournalStore) -> None:
        """Use the participant journal for command identity and work outcomes."""
        self._journal = journal

    async def observe(self, message: MessageEnvelope, context: HandlerContext, session: AsyncSession) -> HandlingResult:
        """Observe or repeat the provider's idempotent cancellation request."""
        return await self.handle(message, context, session)

    async def handle(self, message: MessageEnvelope, _context: HandlerContext, session: AsyncSession) -> HandlingResult:
        """Refuse irreversible resource undo and wait for confirmed work termination."""
        payload = CancelCommand.model_validate(message.command.payload)
        await self._journal.intents.lock(session, message)
        target = await self._journal.get(
            session,
            CommandScope(client_id=message.scope.client_id, project_id=None, all_projects=True),
            payload.target_command_id,
            "inbound",
        )
        if target is not None and target.scope != message.scope:
            code = "command_not_found"
            raise CommandExecutionError(code)
        if target is not None and target.command_type != "work.execute":
            code = "cancellation_not_supported"
            raise CommandExecutionError(code)
        if target is None:
            return HandlingResult(status=CommandStatus.SUCCEEDED, completion_required=True)
        if target.command_status in {
            CommandStatus.SUCCEEDED,
            CommandStatus.FAILED,
            CommandStatus.CANCELLED,
            CommandStatus.SUPERSEDED,
        }:
            return HandlingResult(status=CommandStatus.SUCCEEDED, completion_required=True)
        if target.command_status == CommandStatus.OUTCOME_UNKNOWN:
            return HandlingResult(
                status=CommandStatus.OUTCOME_UNKNOWN,
                error_code="work_outcome_unknown",
                completion_required=True,
                observe_after_seconds=1,
            )
        if target.resource_id is None:
            return HandlingResult(status=CommandStatus.RUNNING, observe_after_seconds=1)
        if target.project_id is None:
            code = "invalid_work_scope"
            raise CommandExecutionError(code)
        work_store = WorkStore.from_transaction(session)
        work_store.set_completion_recorder(EPCompletionRecorder(self._journal))
        item = await work_store.request_cancel(
            target.resource_id, client_id=message.scope.client_id, project_id=target.project_id
        )
        if item is not None and item.status == WorkItemStatus.CANCEL_REQUESTED:
            await self._journal.intents.record_result(
                session, target.entry_id, HandlingResult(status=CommandStatus.CANCEL_REQUESTED)
            )
        if item is not None and item.status in {
            WorkItemStatus.COMPLETED,
            WorkItemStatus.FAILED,
            WorkItemStatus.CANCELLED,
        }:
            return HandlingResult(status=CommandStatus.SUCCEEDED, completion_required=True)
        return HandlingResult(status=CommandStatus.RUNNING, observe_after_seconds=1)
