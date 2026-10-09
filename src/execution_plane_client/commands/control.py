"""Cancellation, receipt and terminal child contracts."""

from typing import Literal
from uuid import UUID

from pydantic import Field, field_validator

from execution_plane_client.commands import CommandIdentity, CommandRoute, build_command
from execution_plane_client.protocol import MessageEnvelope, ResourceReference, WireModel, validate_json


class CancelCommand(WireModel):
    """Request durable cancellation without implying that execution has stopped."""

    target_command_id: UUID


class Completion(WireModel):
    """Terminal or explicitly unknown action outcome."""

    status: Literal["succeeded", "failed", "cancelled", "superseded", "outcome_unknown"]
    result: dict[str, object] = Field(default_factory=dict, repr=False)
    resource: ResourceReference | None = None
    error_code: str | None = None

    @field_validator("result", mode="before")
    @classmethod
    def check_result(cls, value: object) -> object:
        """Keep encrypted outcomes within the shared JSON budget."""
        return validate_json(value)


def cancel_command(*, identity: CommandIdentity, route: CommandRoute, payload: CancelCommand) -> MessageEnvelope:
    """Build a cancellation request using the same durable ingress as work."""
    return build_command("command.cancel", identity=identity, route=route, payload=payload)
