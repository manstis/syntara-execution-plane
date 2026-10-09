"""WorkStore command adapter usable by direct and Journal runtimes."""

from __future__ import annotations

from typing import TYPE_CHECKING

from execution_plane.work_store import WorkStore
from execution_plane_client.commands.work import WorkExecute
from execution_plane_client.errors import CommandExecutionError, ProtocolConflict
from execution_plane_client.handlers import HandlingResult, TransactionalHandler
from execution_plane_client.protocol import CommandStatus, ResourceReference

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.handlers import HandlerContext
    from execution_plane_client.protocol import MessageEnvelope


class WorkCommandHandler(TransactionalHandler):
    """Enqueue provider work without performing provider I/O in handling."""

    async def handle(self, message: MessageEnvelope, _context: HandlerContext, session: AsyncSession) -> HandlingResult:
        """Persist stable command/work linkage in the participant work table."""
        if message.scope.project_id is None:
            raise ProtocolConflict
        payload = WorkExecute.model_validate(message.command.payload)
        policy = message.command.execution_policy
        if policy is not None and policy.timeout_seconds != payload.invocation.timeout_seconds:
            code = "execution_timeout_mismatch"
            raise CommandExecutionError(code)
        work_payload = payload.model_dump(mode="json", exclude={"work_correlation_id", "workload_type"})
        work_payload.update(
            {
                "_ep_command_id": str(message.command_id),
                "_ep_max_attempts": policy.max_attempts if policy else None,
                "_ep_completion_context": {
                    "message_id": str(message.message_id),
                    "command_id": str(message.command_id),
                    "source_id": message.source_id,
                    "destination_id": message.destination_id,
                    "scope": message.scope.model_dump(mode="json"),
                    "command_type": message.command.command_type,
                    "command_revision": message.command.command_revision,
                    "target_url": message.command.target_url,
                    "callback_url": message.command.callback_url,
                    "occurred_at": message.command.occurred_at.isoformat().replace("+00:00", "Z"),
                    "work_correlation_id": str(payload.work_correlation_id),
                },
            }
        )
        store = WorkStore.from_transaction(session)
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
