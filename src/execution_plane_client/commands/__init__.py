"""Typed command builders with explicit stable identity and participant routing."""

from datetime import datetime
from uuid import UUID

from execution_plane_client.protocol import (
    Command,
    CommandScope,
    ExecutionPolicy,
    Identifier,
    MessageEnvelope,
    PositiveInt,
    WireModel,
)


class CommandIdentity(WireModel):
    """Caller-owned identity, reused unchanged for every delivery attempt."""

    command_id: UUID
    message_id: UUID
    occurred_at: datetime


class CommandRoute(WireModel):
    """Explicit participant identity, initiating scope and registered URLs."""

    source_id: Identifier
    destination_id: Identifier
    scope: CommandScope
    target_url: str
    callback_url: str


class ResourceRevision(WireModel):
    """Stable logical resource identity surviving physical resource deletion."""

    resource_key: UUID
    desired_revision: PositiveInt


def build_command(
    command_type: str,
    *,
    identity: CommandIdentity,
    route: CommandRoute,
    payload: WireModel,
    execution_policy: ExecutionPolicy | None = None,
) -> MessageEnvelope:
    """Build one revision-one action without allocating hidden identities."""
    return MessageEnvelope(
        message_id=identity.message_id,
        command_id=identity.command_id,
        source_id=route.source_id,
        destination_id=route.destination_id,
        scope=route.scope,
        command=Command(
            command_id=identity.command_id,
            command_type=command_type,
            command_revision=1,
            target_url=route.target_url,
            callback_url=route.callback_url,
            occurred_at=identity.occurred_at,
            payload=payload.model_dump(mode="json", exclude_unset=command_type.endswith(".update")),
            execution_policy=execution_policy,
        ),
    )
