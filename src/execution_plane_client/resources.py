"""Transport-neutral resource revision contracts for command handlers."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime  # noqa: TC003 - Pydantic resolves model fields at runtime.
from typing import TYPE_CHECKING, Protocol
from uuid import UUID  # noqa: TC003 - Pydantic resolves model fields at runtime.

from pydantic import Field

from execution_plane_client.commands.targets import (
    Placement,  # noqa: TC001 - Pydantic resolves model fields at runtime.
)
from execution_plane_client.protocol import WireModel

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.protocol import CommandScope, MessageEnvelope


class WorkItemRead(WireModel):
    """Authorized work outcome without private invocation or credentials."""

    id: UUID
    project_id: UUID
    request_id: str
    work_correlation_id: UUID
    status: str
    result: dict[str, object] | None = None
    result_omitted: bool = False
    result_size_bytes: int | None = None
    execution_target_id: UUID | None = None
    created_at: datetime
    claimed_at: datetime | None = None
    completed_at: datetime | None = None
    backend_resource_name: str | None = None
    backend_resource_uid: str | None = None
    resource_cleanup_status: str


class ClusterRead(WireModel):
    """Credential-free cluster state and current project grants."""

    id: UUID
    name: str
    endpoint: str
    cluster_type: str
    status: str
    enabled: bool
    labels: dict[str, str] = Field(default_factory=dict)
    project_ids: list[UUID] | None = None
    source_integration_id: UUID | None = None
    source_revision: int = 0
    created_at: datetime
    updated_at: datetime


class ExecutionTargetRead(WireModel):
    """A target's safe placement and eligibility state."""

    id: UUID
    cluster_id: UUID
    name: str
    backend_type: str
    endpoint: str
    placement: Placement
    status: str
    enabled: bool
    is_default: bool
    labels: dict[str, str] = Field(default_factory=dict)
    created_at: datetime
    updated_at: datetime


@dataclass
class ResourceIntent:
    """Desired and observed state for one logical client-owned resource."""

    scope: CommandScope
    client_id: str
    resource_type: str
    resource_key: str
    desired_revision: int
    desired_hash: str
    entry_id: UUID | None = None
    observed_revision: int = 0
    resource_id: UUID | None = None
    resource_deleted: bool = False


class ResourceIntentStore(Protocol):
    """Runtime-provided revision fencing used by cluster and target handlers."""

    async def resource(self, session: AsyncSession, message: MessageEnvelope) -> ResourceIntent:
        """Reserve or load the current revision for a logical resource."""
        ...

    async def observe_resource(
        self,
        session: AsyncSession,
        message: MessageEnvelope,
        *,
        resource_id: UUID | None,
        observed_revision: int | None = None,
        deleted: bool | None = None,
    ) -> ResourceIntent:
        """Record the physical resource and revision observed by the handler."""
        ...


def desired_hash(message: MessageEnvelope) -> str:
    """Hash resource intent independently of command and transport identity."""
    return hashlib.sha256(
        json.dumps(
            {"type": message.command.command_type, "payload": message.command.payload},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
