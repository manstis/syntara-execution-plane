"""Direct command cancellation using EP's work state and participant projection."""

from __future__ import annotations

from typing import TYPE_CHECKING

from execution_plane.models.work_item import WorkItemStatus
from execution_plane.work_store import WorkStore
from execution_plane_client.commands.control import CancelCommand
from execution_plane_client.errors import CommandExecutionError, CommandNotFound
from execution_plane_client.handlers import HandlingResult, ObservationHandler, TransactionalHandler
from execution_plane_client.protocol import CommandStatus

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.handlers import HandlerContext
    from execution_plane_client.protocol import MessageEnvelope
    from execution_plane_client.queries import CommandQueryProvider


class CancelCommandHandler(TransactionalHandler, ObservationHandler):
    """Request cancellation and emit the work completion through the selected runtime."""

    def __init__(self, queries: CommandQueryProvider) -> None:
        """Use the selected runtime's command query projection."""
        self._queries = queries

    async def observe(self, message: MessageEnvelope, context: HandlerContext, session: AsyncSession) -> HandlingResult:
        """Recheck provider cancellation until its original work command becomes terminal."""
        return await self.handle(message, context, session)

    async def handle(  # noqa: C901, PLR0911 - Cancellation combines durable query, work and completion states.
        self, message: MessageEnvelope, context: HandlerContext, session: AsyncSession
    ) -> HandlingResult:
        """Cancel an accepted work item without depending on a Journal record."""
        payload = CancelCommand.model_validate(message.command.payload)
        try:
            target = await self._queries.get(message.scope, payload.target_command_id, peer_id=message.source_id)
        except CommandNotFound:
            # The selected transport records this accepted intent before receipt
            # acknowledgement and applies it if work arrives later.
            return HandlingResult(status=CommandStatus.SUCCEEDED, completion_required=True)
        if target.command_type != "work.execute":
            code = "cancellation_not_supported"
            raise CommandExecutionError(code)
        if target.status in {
            CommandStatus.SUCCEEDED,
            CommandStatus.FAILED,
            CommandStatus.CANCELLED,
            CommandStatus.SUPERSEDED,
        }:
            return HandlingResult(status=CommandStatus.SUCCEEDED, completion_required=True)
        if target.status is CommandStatus.OUTCOME_UNKNOWN:
            return HandlingResult(
                status=CommandStatus.OUTCOME_UNKNOWN,
                error_code="work_outcome_unknown",
                completion_required=True,
            )
        if target.resource is None:
            return HandlingResult(status=CommandStatus.RUNNING, observe_after_seconds=1)
        if message.scope.project_id is None:
            code = "invalid_work_scope"
            raise CommandExecutionError(code)

        recorder = context.completion_recorder
        if recorder is None:
            error = "Work cancellation requires a completion recorder"
            raise RuntimeError(error)
        store = WorkStore.from_transaction(session)
        store.set_completion_recorder(recorder)
        item = await store.request_cancel(
            target.resource.resource_id,
            client_id=message.scope.client_id,
            project_id=message.scope.project_id,
        )
        if item.status is WorkItemStatus.CANCELLED:

            async def publish_work_completion() -> None:
                await recorder.after_commit(item)

            return HandlingResult(
                status=CommandStatus.SUCCEEDED,
                completion_required=True,
                after_commit=publish_work_completion,
            )
        if item.status in {WorkItemStatus.COMPLETED, WorkItemStatus.FAILED}:
            return HandlingResult(status=CommandStatus.SUCCEEDED, completion_required=True)
        return HandlingResult(status=CommandStatus.RUNNING, observe_after_seconds=1)
