"""Domain-side projections and helpers for resource command handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.execution_target import ExecutionTarget, TargetStatus
from execution_plane_client.errors import ProtocolConflict
from execution_plane_client.handlers import HandlingResult
from execution_plane_client.protocol import CommandStatus, ResourceReference
from execution_plane_client.resources import ResourceIntent, ResourceIntentStore, desired_hash

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.protocol import MessageEnvelope


SYSTEM_ACTOR = UUID("fb468148-992b-4422-b851-04f9ed2e6f0b")


def reference(row: ResourceIntent) -> ResourceReference | None:
    """Return only physical identity and observed revision, never credentials."""
    if row.resource_id is None:
        return None
    if row.resource_type is None or row.resource_key is None:
        raise ProtocolConflict
    return ResourceReference(
        kind=row.resource_type,
        resource_id=row.resource_id,
        resource_key=UUID(row.resource_key),
        revision=row.observed_revision or None,
    )


async def current_resource(
    session: AsyncSession, message: MessageEnvelope, intents: ResourceIntentStore
) -> tuple[ResourceIntent, HandlingResult | None]:
    """Fence handlers against the latest received resource intent."""
    row = await intents.resource(session, message)
    revision = int(str(message.command.payload["desired_revision"]))
    if revision < row.desired_revision:
        return row, HandlingResult(status=CommandStatus.SUPERSEDED, resource=reference(row), completion_required=True)
    if desired_hash(message) != row.desired_hash:
        raise ProtocolConflict
    if revision == row.observed_revision:
        return row, HandlingResult(status=CommandStatus.SUCCEEDED, resource=reference(row), completion_required=True)
    return row, None


async def observed(
    session: AsyncSession,
    intents: ResourceIntentStore,
    message: MessageEnvelope,
    row: ResourceIntent,
    resource_id: UUID,
) -> HandlingResult:
    """Record successful physical state through the selected resource-intent adapter."""
    row = await intents.observe_resource(
        session, message, resource_id=resource_id, observed_revision=row.desired_revision, deleted=False
    )
    return HandlingResult(status=CommandStatus.SUCCEEDED, resource=reference(row), completion_required=True)


async def observe_drain(
    session: AsyncSession, message: MessageEnvelope, intents: ResourceIntentStore
) -> HandlingResult:
    """Inspect provider drain progress; the runtime schedules subsequent reads."""
    row, completed = await current_resource(session, message, intents)
    if completed is not None:
        return completed
    physical: Cluster | ExecutionTarget | None = None
    if row.resource_id is not None:
        physical = (
            await session.get(Cluster, row.resource_id)
            if row.resource_type == "cluster"
            else await session.get(ExecutionTarget, row.resource_id)
        )
    if row.resource_deleted and physical is None:
        row = await intents.observe_resource(
            session, message, resource_id=row.resource_id, observed_revision=row.desired_revision, deleted=True
        )
        return HandlingResult(status=CommandStatus.SUCCEEDED, resource=reference(row), completion_required=True)
    if physical is not None and physical.status in {ClusterStatus.ERROR, TargetStatus.FAILED}:
        return HandlingResult(
            status=CommandStatus.FAILED,
            resource=reference(row),
            error_code="resource_drain_failed",
            completion_required=True,
        )
    return HandlingResult(status=CommandStatus.RUNNING, resource=reference(row), observe_after_seconds=1)
