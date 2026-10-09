"""Safe EP resource reads shared by participants without provider dependencies."""

from datetime import datetime
from uuid import UUID

from pydantic import Field

from execution_plane_client.commands.targets import Placement
from execution_plane_client.protocol import WireModel


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
