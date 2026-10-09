"""Stable terminal children shared by generic handlers and participant work stores."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import UUID, uuid5

from execution_plane_client.commands.control import Completion
from execution_plane_client.errors import ProtocolConflict
from execution_plane_client.handlers import HandlingResult
from execution_plane_client.protocol import MAX_PAYLOAD_BYTES, Command, CommandStatus, MessageEnvelope

COMPLETION_COMMAND_NAMESPACE = UUID("d409a176-f783-44d0-b8bd-acd211550fa4")
COMPLETION_MESSAGE_NAMESPACE = UUID("4df96bc1-9b14-4885-b319-089bf67a31b1")


def completion_message(
    original: MessageEnvelope,
    result: HandlingResult,
    *,
    sequence: int,
    occurred_at: datetime | None = None,
) -> MessageEnvelope:
    """Canonicalize a bounded completion before appending or comparing an observation."""
    if result.status is None or original.command.command_type.endswith(".completed"):
        raise ProtocolConflict
    identity = f"{original.message_id}:{sequence}"
    command_id = uuid5(COMPLETION_COMMAND_NAMESPACE, identity)
    message = MessageEnvelope(
        message_id=uuid5(COMPLETION_MESSAGE_NAMESPACE, identity),
        command_id=command_id,
        source_id=original.destination_id,
        destination_id=original.source_id,
        scope=original.scope,
        command=Command(
            command_id=command_id,
            parent_command_id=original.command_id,
            parent_sequence=sequence,
            command_type=f"{original.command.command_type}.completed",
            command_revision=original.command.command_revision,
            target_url=original.command.callback_url,
            callback_url=original.command.target_url,
            occurred_at=occurred_at or datetime.now(UTC),
            payload={},
        ),
    )
    # Reserve the entire completion envelope within the payload budget.
    # The participant retains the full provider result in its own transaction.
    values = message.model_dump(mode="json")
    values["command"]["payload"] = {
        "status": result.status.value,
        "result": result.result,
        "resource": result.resource.model_dump(mode="json") if result.resource else None,
        "error_code": result.error_code,
    }
    encoded = json.dumps(values, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()
    if len(encoded) > MAX_PAYLOAD_BYTES:
        values["command"]["payload"].update(
            status="outcome_unknown" if result.status == CommandStatus.OUTCOME_UNKNOWN else "failed",
            result={},
            error_code="completion_result_too_large",
        )
    message = MessageEnvelope.model_validate(values)
    # Validate terminal status as well as the generic command payload.
    Completion.model_validate(message.command.payload)
    return message


def completion_result(message: MessageEnvelope) -> HandlingResult:
    """Project precisely the bounded outcome actually committed for the peer."""
    payload = Completion.model_validate(message.command.payload)
    return HandlingResult(
        status=CommandStatus(payload.status),
        result=payload.result,
        resource=payload.resource,
        error_code=payload.error_code,
        completion_required=True,
    )
