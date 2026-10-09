"""Build a direct-transport completion envelope from an EP WorkItem route context."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from execution_plane.models.work_item import WorkItemStatus
from execution_plane_client.completion import completion_message
from execution_plane_client.handlers import HandlingResult
from execution_plane_client.protocol import Command, CommandScope, CommandStatus, MessageEnvelope, ResourceReference

if TYPE_CHECKING:
    from execution_plane.models.work_item import WorkItem


def completion_for_work_item(
    item: WorkItem,
) -> tuple[MessageEnvelope, MessageEnvelope, HandlingResult] | None:
    """Rebuild stable callback identity and result from the encrypted work route."""
    context = item.payload.get("_ep_completion_context")
    if not isinstance(context, dict):
        return None
    status = {
        WorkItemStatus.COMPLETED: CommandStatus.SUCCEEDED,
        WorkItemStatus.FAILED: CommandStatus.FAILED,
        WorkItemStatus.CANCELLED: CommandStatus.CANCELLED,
        WorkItemStatus.RECONCILIATION_REQUIRED: CommandStatus.OUTCOME_UNKNOWN,
    }.get(item.status)
    if status is None:
        return None
    command_id = UUID(str(context["command_id"]))
    original = MessageEnvelope(
        message_id=UUID(str(context["message_id"])),
        command_id=command_id,
        source_id=str(context["source_id"]),
        destination_id=str(context["destination_id"]),
        scope=CommandScope.model_validate(context["scope"]),
        command=Command(
            command_id=command_id,
            command_type=str(context["command_type"]),
            command_revision=int(str(context["command_revision"])),
            target_url=str(context["target_url"]),
            callback_url=str(context["callback_url"]),
            occurred_at=datetime.fromisoformat(str(context["occurred_at"])),
            payload={},
        ),
    )
    result = HandlingResult(
        status=status,
        resource=ResourceReference(kind="work_item", resource_id=item.id),
        result={
            "work_id": str(item.id),
            "work_correlation_id": str(context["work_correlation_id"]),
            "result": item.result,
        },
        completion_required=True,
        error_code="work_outcome_unknown" if status is CommandStatus.OUTCOME_UNKNOWN else None,
    )
    child = completion_message(original, result, sequence=1, occurred_at=item.completed_at)
    return original, child, result
