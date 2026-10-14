"""Client-scoped queries of domain resources, without command history."""

import json
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlmodel import col

from execution_plane.api.schemas import ClusterRead, ExecutionTargetRead, WorkItemRead
from execution_plane.commands.clusters import ResourceNotFoundError
from execution_plane.models.cluster import Cluster
from execution_plane.models.cluster_binding import ClusterBinding
from execution_plane.models.execution_target import ExecutionTarget
from execution_plane.work_store import WorkStore


class EPResourceQueries:
    """Read only resources owned by the authenticated service client."""

    def __init__(self, engine: AsyncEngine, client_id: str) -> None:
        """Borrow the API's engine and verified client identity."""
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)
        self._work = WorkStore.from_engine(engine)
        self._client_id = client_id

    async def work_items(self, limit: int) -> list[WorkItemRead]:
        """List work without embedding potentially large results."""
        items = await self._work.list_for_client(client_id=self._client_id, limit=limit)
        return [
            WorkItemRead.model_validate(item).model_copy(
                update={"result": None, "result_omitted": item.result is not None}
            )
            for item in items
        ]

    async def work_item(self, resource_id: UUID) -> WorkItemRead:
        """Recover the complete stored result without needing command history."""
        item = await self._work.get(resource_id, client_id=self._client_id)
        if item is None:
            raise ResourceNotFoundError
        read = WorkItemRead.model_validate(item)
        if item.result is not None:
            read.result_size_bytes = len(json.dumps(item.result, ensure_ascii=False).encode())
        event = await self._work.get_completion_event(resource_id)
        if event is not None:
            read.completion_event_id, read.state_revision = event.id, event.state_revision
        return read

    async def clusters(self, limit: int) -> list[ClusterRead]:
        """List binding resources, including pending setup and deletion."""
        async with self._sessions() as session:
            rows = await session.scalars(
                select(ClusterBinding)
                .where(col(ClusterBinding.client_id) == self._client_id)
                .order_by(col(ClusterBinding.created_at), col(ClusterBinding.source_integration_id))
                .limit(limit)
            )
            return [ClusterRead.model_validate(row) for row in rows]

    async def cluster(self, resource_id: UUID) -> ClusterRead:
        """Read binding state using its stable source integration UUID."""
        async with self._sessions() as session:
            row = await session.get(ClusterBinding, (self._client_id, resource_id))
            if row is None:
                raise ResourceNotFoundError
            return ClusterRead.model_validate(row)

    async def targets(self, limit: int, resource_id: UUID | None = None) -> list[ExecutionTargetRead]:
        """Authorize targets through ownership of their physical cluster."""
        statement = select(ExecutionTarget).join(Cluster).where(col(Cluster.source_client_id) == self._client_id)
        if resource_id is not None:
            statement = statement.where(col(ExecutionTarget.id) == resource_id)
        async with self._sessions() as session:
            rows = await session.scalars(
                statement.order_by(col(ExecutionTarget.created_at), col(ExecutionTarget.id)).limit(limit)
            )
            return [ExecutionTargetRead.model_validate(row) for row in rows]

    async def target(self, resource_id: UUID) -> ExecutionTargetRead:
        """Use the same not-found response for absent and other-client targets."""
        rows = await self.targets(1, resource_id)
        if not rows:
            raise ResourceNotFoundError
        return rows[0]
