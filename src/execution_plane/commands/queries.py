"""Scoped provider-resource reads authorized through journal associations."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from sqlalchemy import or_, select
from sqlmodel import col

from execution_plane.models.cluster import Cluster
from execution_plane.models.execution_target import ExecutionTarget
from execution_plane.models.work_item import WorkItem
from execution_plane_client.errors import CommandNotFound
from execution_plane_client.protocol import (
    MAX_PAYLOAD_BYTES,
    CommandScope,
)
from execution_plane_client.resources import ClusterRead, ExecutionTargetRead, WorkItemRead

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession
    from sqlalchemy.sql import Select


class EPResourceQueries:
    """Bound every resource lookup by the signed client and project scope."""

    def _work(self, scope: CommandScope) -> Select[tuple[WorkItem]]:
        statement = select(WorkItem).where(col(WorkItem.client_id) == scope.client_id)
        if not scope.all_projects:
            statement = statement.where(col(WorkItem.project_id) == scope.project_id)
        return statement

    async def work_items(self, session: AsyncSession, scope: CommandScope, *, limit: int = 50) -> list[WorkItemRead]:
        """Return bounded authorized work without encrypted submission context."""
        rows = (
            await session.scalars(self._work(scope).order_by(col(WorkItem.created_at).desc()).limit(min(limit, 200)))
        ).all()
        return [
            WorkItemRead.model_validate(row, from_attributes=True).model_copy(
                update={"result": None, "result_omitted": row.result is not None}
            )
            for row in rows
        ]

    async def work_item(self, session: AsyncSession, scope: CommandScope, resource_id: UUID) -> WorkItemRead:
        """Return one scoped work item or the same not-found result as an absent ID."""
        row = await session.scalar(self._work(scope).where(col(WorkItem.id) == resource_id))
        if row is None:
            raise CommandNotFound
        read = WorkItemRead.model_validate(row, from_attributes=True)
        size = len(json.dumps(row.result, ensure_ascii=False, separators=(",", ":")).encode()) if row.result else None
        omitted = size is not None and size > MAX_PAYLOAD_BYTES
        return read.model_copy(
            update={"result": None if omitted else row.result, "result_omitted": omitted, "result_size_bytes": size}
        )

    @staticmethod
    def _clusters(scope: CommandScope) -> Select[tuple[Cluster]]:
        statement = select(Cluster).where(col(Cluster.source_client_id) == scope.client_id)
        if not scope.all_projects:
            statement = statement.where(
                or_(col(Cluster.project_ids).is_(None), col(Cluster.project_ids).contains([str(scope.project_id)]))
            )
        return statement

    async def clusters(self, session: AsyncSession, scope: CommandScope, *, limit: int = 50) -> list[ClusterRead]:
        """List only the client's project-eligible clusters."""
        rows = (
            await session.scalars(self._clusters(scope).order_by(col(Cluster.created_at).desc()).limit(min(limit, 200)))
        ).all()
        return [ClusterRead.model_validate(row, from_attributes=True) for row in rows]

    async def cluster(self, session: AsyncSession, scope: CommandScope, resource_id: UUID) -> ClusterRead:
        """Read safe cluster state without credential or CA material."""
        row = await session.scalar(self._clusters(scope).where(col(Cluster.id) == resource_id))
        if row is None:
            raise CommandNotFound
        return ClusterRead.model_validate(row, from_attributes=True)

    @staticmethod
    def _targets(scope: CommandScope) -> Select[tuple[ExecutionTarget]]:
        statement = select(ExecutionTarget).join(Cluster).where(col(Cluster.source_client_id) == scope.client_id)
        if not scope.all_projects:
            statement = statement.where(
                or_(col(Cluster.project_ids).is_(None), col(Cluster.project_ids).contains([str(scope.project_id)]))
            )
        return statement

    async def targets(
        self, session: AsyncSession, scope: CommandScope, *, limit: int = 50
    ) -> list[ExecutionTargetRead]:
        """List safe placement and state, including registering or draining targets."""
        rows = (
            await session.scalars(
                self._targets(scope).order_by(col(ExecutionTarget.created_at).desc()).limit(min(limit, 200))
            )
        ).all()
        return [ExecutionTargetRead.model_validate(row, from_attributes=True) for row in rows]

    async def target(self, session: AsyncSession, scope: CommandScope, resource_id: UUID) -> ExecutionTargetRead:
        """Read a target only through its client's eligible owning cluster."""
        row = await session.scalar(self._targets(scope).where(col(ExecutionTarget.id) == resource_id))
        if row is None:
            raise CommandNotFound
        return ExecutionTargetRead.model_validate(row, from_attributes=True)
