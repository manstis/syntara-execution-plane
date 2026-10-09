"""Validated journal views without registration in any application's ORM."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import Field
from sqlmodel import SQLModel

from execution_plane_client.protocol import CommandScope, CommandStatus, MessageEnvelope, ResourceReference

Direction = Literal["inbound", "outbound"]


class JournalEntry(SQLModel):
    """Local encrypted record; this is not a public API response model."""

    model_config = {"extra": "forbid", "frozen": True}

    entry_id: UUID
    message_id: UUID
    command_id: UUID
    parent_command_id: UUID | None
    parent_sequence: int | None
    message_type: str
    command_type: str
    command_revision: int
    sequence: int
    source_id: str
    destination_id: str
    client_id: str
    project_id: UUID | None
    all_projects: bool
    direction: Direction
    target_url: str
    callback_url: str
    envelope_hash: str
    envelope_ciphertext: str = Field(repr=False)
    delivery_state: str
    handling_state: str
    command_status: CommandStatus
    resource_type: str | None = None
    resource_key: str | None = None
    resource_id: UUID | None = None
    desired_revision: int | None = None
    desired_hash: str | None = None
    observed_revision: int = 0
    resource_head: bool = False
    resource_deleted: bool = False
    target_command_id: UUID | None = None
    command_error_code: str | None = None
    observation_pending: bool = False
    delivery_attempts: int
    delivery_due_at: datetime | None
    delivery_owner: UUID | None
    delivery_generation: int
    delivery_lease_until: datetime | None
    acceptance_due_at: datetime | None
    handling_attempts: int
    handling_due_at: datetime | None
    handling_owner: UUID | None
    handling_generation: int
    handling_lease_until: datetime | None
    handling_error_code: str | None
    delivery_error_code: str | None
    reconciliation_required: bool
    reconciliation_due_at: datetime | None
    reconciliation_error_code: str | None
    prepared_effect_ciphertext: str | None = Field(repr=False)
    local_context_ciphertext: str | None = Field(default=None, repr=False)
    local_context_generation: int = 0
    correlation_key: str | None = None
    created_at: datetime
    received_at: datetime | None
    sent_at: datetime | None
    accepted_at: datetime | None
    handled_at: datetime | None
    updated_at: datetime

    @property
    def scope(self) -> CommandScope:
        """Return the immutable authenticated initiating scope."""
        return CommandScope(client_id=self.client_id, project_id=self.project_id, all_projects=self.all_projects)

    @property
    def resource(self) -> ResourceReference | None:
        """Project the physical observation without exposing the private intent."""
        if self.resource_type is None or self.resource_id is None:
            return None
        return ResourceReference(
            kind=self.resource_type,
            resource_id=self.resource_id,
            resource_key=UUID(self.resource_key) if self.resource_key is not None else None,
            revision=self.observed_revision or None,
        )


@dataclass(frozen=True)
class LocalContext:
    """Participant-private correlation owned by one journal record and generation."""

    entry_id: UUID
    generation: int
    correlation_key: str | None
    data: dict[str, object] = field(repr=False)


@dataclass(frozen=True)
class EnsuredOutbound:
    """Frozen outbound command and its optional local context after preparation."""

    entry: JournalEntry
    message: MessageEnvelope
    local_context: LocalContext | None
    created: bool
