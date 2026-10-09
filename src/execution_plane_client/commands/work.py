"""Versioned work invocation values without node-provider imports."""

from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, StrictStr, field_validator

from execution_plane_client.commands import CommandIdentity, CommandRoute, build_command
from execution_plane_client.protocol import ExecutionPolicy, MessageEnvelope, PositiveInt, WireModel, validate_json


class NodeInvocation(WireModel):
    """Version-one invocation consumed by the existing node protocol."""

    version: Literal[1] = 1
    operation: Literal["execute"] = "execute"
    inputs: dict[str, object]
    credentials: dict[str, object] = Field(repr=False)
    workflow_context: dict[str, object]
    settings: dict[str, object]
    timeout_seconds: PositiveInt
    max_output_bytes: PositiveInt

    @field_validator("inputs", "credentials", "workflow_context", "settings", mode="before")
    @classmethod
    def check_json(cls, value: object) -> object:
        """Keep all nested invocation data portable and bounded."""
        return validate_json(value)


class WorkExecute(WireModel):
    """Script command with stable caller correlation and provider-neutral input."""

    work_correlation_id: UUID
    image: Annotated[StrictStr, Field(min_length=1, max_length=2048)]
    invocation: NodeInvocation = Field(repr=False)
    output_config: dict[str, str] | None = None
    workload_type: Literal["script"] = "script"


def work_execute(
    *,
    identity: CommandIdentity,
    route: CommandRoute,
    payload: WorkExecute,
    execution_policy: ExecutionPolicy | None = None,
) -> MessageEnvelope:
    """Build a stable work command."""
    return build_command(
        "work.execute", identity=identity, route=route, payload=payload, execution_policy=execution_policy
    )
