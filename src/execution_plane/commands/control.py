"""Cancel existing work without a command queue or cancellation journal."""

from execution_plane.commands.context import EPCommandContext
from execution_plane.work_store import WorkStore
from execution_plane_client.commands.control import CancelCommand
from execution_plane_client.handlers import HandlingResult
from execution_plane_client.protocol import MessageEnvelope, ResourceReference


class CancelCommandHandler:
    """Apply cancellation to authenticated, existing domain work."""

    async def handle(self, message: MessageEnvelope, context: EPCommandContext) -> HandlingResult:
        """Unknown work is not found; cancellation before arrival is not queued."""
        payload = CancelCommand.model_validate(message.command.payload)
        item = await WorkStore.from_engine(context.engine).request_cancel(
            payload.target_command_id,
            client_id=context.client_id,
        )
        return HandlingResult(ResourceReference(kind="work_item", resource_id=item.id))
