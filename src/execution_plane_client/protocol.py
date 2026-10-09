"""Strict, versioned wire values and redacted command query contracts."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Literal, Self
from urllib.parse import urlsplit
from uuid import UUID  # noqa: TC003 - SQLModel resolves annotations at runtime.

from pydantic import Field, StrictBool, StrictInt, StrictStr, field_validator, model_validator
from sqlmodel import SQLModel

MAX_PAYLOAD_BYTES = 512 * 1024
Identifier = Annotated[StrictStr, Field(min_length=1, max_length=255, pattern=r"^[A-Za-z0-9_.:@/-]+$")]
PositiveInt = Annotated[StrictInt, Field(gt=0)]
Sequence = Annotated[StrictInt, Field(ge=0)]


class WireModel(SQLModel):
    """Forbid silent field loss and freeze top-level wire values."""

    model_config = {"extra": "forbid", "frozen": True, "validate_default": True}


def validate_json(value: object, *, max_bytes: int | None = MAX_PAYLOAD_BYTES) -> object:
    """Reject coercions and non-finite JSON, optionally bounding its encoded size."""
    try:
        encoded = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        message = "Payload is not finite UTF-8 JSON"
        raise ValueError(message) from exc
    if max_bytes is not None and len(encoded) > max_bytes:
        message = f"Payload exceeds {max_bytes // 1024} KiB"
        raise ValueError(message)
    pending = [value]
    while pending:
        item = pending.pop()
        if type(item) is dict:
            if any(not isinstance(key, str) for key in item):
                message = "JSON object keys must be strings"
                raise ValueError(message)
            pending.extend(item.values())
        elif type(item) is list:
            pending.extend(item)
        elif item is not None and type(item) not in (str, bool, int, float):
            message = "Payload must contain JSON values only"
            raise ValueError(message)
    return value


class CommandScope(WireModel):
    """Initiating client and exactly one project or an explicit all-project grant."""

    client_id: Identifier
    project_id: UUID | None
    all_projects: StrictBool = False

    @model_validator(mode="after")
    def check_project(self) -> Self:
        """Disallow ambiguous or implicitly unscoped commands."""
        if (self.project_id is None) != self.all_projects:
            message = "Scope requires one project or explicit all_projects"
            raise ValueError(message)
        return self


class ExecutionPolicy(WireModel):
    """Separate bounded safe execution attempts from transport retries."""

    timeout_seconds: PositiveInt
    max_attempts: PositiveInt | None = None


class Command(WireModel):
    """Immutable logical command content; persistence snapshots nested values."""

    command_id: UUID
    parent_command_id: UUID | None = None
    command_type: Annotated[StrictStr, Field(pattern=r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$")]
    command_revision: PositiveInt
    target_url: StrictStr
    callback_url: StrictStr
    payload: dict[str, object]
    occurred_at: datetime
    parent_sequence: PositiveInt | None = None
    acknowledges_message_id: UUID | None = None
    execution_policy: ExecutionPolicy | None = None

    @field_validator("payload", mode="before")
    @classmethod
    def check_payload(cls, value: object) -> object:
        """Validate without coercing values that affect immutable identity."""
        return validate_json(value)

    @field_validator("occurred_at")
    @classmethod
    def check_time(cls, value: datetime) -> datetime:
        """Require an explicit timezone and serialize instants in UTC."""
        if value.tzinfo is None or value.utcoffset() is None:
            message = "occurred_at requires a timezone"
            raise ValueError(message)
        return value.astimezone(UTC)

    @field_validator("target_url", "callback_url")
    @classmethod
    def check_url(cls, value: str) -> str:
        """Reject non-HTTPS routes and embedded authentication."""
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or any(char.isspace() for char in value)
        ):
            message = "Command route must be an HTTPS URL without userinfo, query or fragment"
            raise ValueError(message)
        # Accessing port also rejects malformed port syntax.
        _ = parsed.port
        return value

    @model_validator(mode="after")
    def check_parent(self) -> Self:
        """Reject self-parenting and ordering without a parent."""
        if self.parent_command_id == self.command_id:
            message = "A command cannot be its own parent"
            raise ValueError(message)
        if self.parent_sequence is not None and self.parent_command_id is None:
            message = "parent_sequence requires parent_command_id"
            raise ValueError(message)
        return self


class MessageEnvelope(WireModel):
    """Signed transport identity around one logical command."""

    message_id: UUID
    message_type: Literal["command.submit"] = "command.submit"
    command_id: UUID
    sequence: Sequence = 0
    source_id: Identifier
    destination_id: Identifier
    scope: CommandScope
    command: Command

    @model_validator(mode="after")
    def check_identity(self) -> Self:
        """Prevent ambiguous duplicate and signature lookup identities."""
        if self.command_id != self.command.command_id:
            message = "Envelope command_id must match command.command_id"
            raise ValueError(message)
        return self


class CommandStatus(StrEnum):
    """Participant-visible action status, independent of transport receipt."""

    ACCEPTED = "accepted"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCEL_REQUESTED = "cancel_requested"
    CANCELLED = "cancelled"
    SUPERSEDED = "superseded"
    OUTCOME_UNKNOWN = "outcome_unknown"


class ResourceReference(WireModel):
    """Safe reference from a command to a participant-owned resource."""

    kind: Identifier
    resource_id: UUID
    resource_key: UUID | None = None
    revision: PositiveInt | None = None


class CommandRead(WireModel):
    """Generic command status; deliberately has no request or private result."""

    command_id: UUID
    command_type: str = ""
    status: CommandStatus
    parent_command_id: UUID | None = None
    resource: ResourceReference | None = None
    latest_child_sequence: Sequence = 0
    delivery_state: str | None = None
    handling_state: str | None = None
    error_code: str | None = None
    reconciliation_required: bool = False
    created_at: datetime | None = None
    accepted_at: datetime | None = None
    handled_at: datetime | None = None


class ChildCommandRead(CommandRead):
    """Ordered child status, optionally carrying authorized outcome recovery."""

    parent_sequence: PositiveInt
    recovery_message: MessageEnvelope | None = None


class ChildPage(WireModel):
    """Bounded child page with a stable per-parent cursor."""

    items: list[ChildCommandRead]
    next_after: Sequence | None = None


class CapabilitiesRead(WireModel):
    """Only installed revisions and handlers are advertised."""

    protocol_revision: Literal[1] = 1
    commands: dict[str, list[int]]
    receipt_handlers: list[str]
    completion_handlers: list[str]
