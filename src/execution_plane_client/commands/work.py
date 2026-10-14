"""Versioned work invocation values without node-provider imports."""

from typing import Annotated, ClassVar, Literal

from pydantic import Field, StrictStr, field_validator

from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.protocol import PositiveInt, WireModel, validate_json


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


class WorkExecute(CommandPayload):
    """Script command with stable caller correlation and provider-neutral input."""

    command_type: ClassVar[str] = "work.execute"

    image: Annotated[StrictStr, Field(min_length=1, max_length=2048)]
    invocation: NodeInvocation = Field(repr=False)
    output_config: dict[str, str] | None = None
    workload_type: Literal["script"] = "script"
