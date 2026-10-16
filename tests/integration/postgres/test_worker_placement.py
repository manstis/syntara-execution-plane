"""PostgreSQL-backed placement integration for the worker."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlmodel import col

from execution_plane import worker
from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.execution_target_reconciler.adapters import build_placement_resolver
from execution_plane.models.cluster import Cluster, ClusterStatus, ClusterType
from execution_plane.models.completion_event import CompletionEvent
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus
from execution_plane.models.execution_target_placement import KubernetesPlacement
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane.work_store import WorkStore

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

    from execution_plane.execution_target_reconciler.placement import PlacementResolver
    from execution_plane.execution_target_reconciler.types import (
        ClusterSnapshot,
        ExecutionTargetSnapshot,
        ReconcileResult,
        WorkRequirements,
    )

pytestmark = pytest.mark.integration


class _StalePlacementResolver:
    def __init__(self, resolver: PlacementResolver, result: ReconcileResult) -> None:
        self._resolver = resolver
        self._result = result

    async def resolve(self, _requirements: WorkRequirements) -> ReconcileResult:
        return self._result

    async def target_is_eligible(
        self,
        cluster: ClusterSnapshot,
        target: ExecutionTargetSnapshot,
        requirements: WorkRequirements,
    ) -> bool:
        return await self._resolver.target_is_eligible(cluster, target, requirements)


@pytest.fixture
async def placement_database(
    migrated_database: str,
) -> AsyncIterator[tuple[AsyncEngine, async_sessionmaker[AsyncSession]]]:
    engine = create_async_engine(migrated_database)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    yield engine, sessions
    await engine.dispose()


async def _seed_work(sessions: async_sessionmaker[AsyncSession], *, matching: bool) -> tuple[UUID, UUID, UUID]:
    cluster_id, target_id, work_id = uuid4(), uuid4(), uuid4()
    selector_key = f"integration-{uuid4()}"
    target_selector_value = "match" if matching else "different"
    now = datetime.now(UTC)
    created_at = datetime(2000, 1, 1, tzinfo=UTC)

    async with sessions.begin() as session:
        session.add(
            Cluster(
                id=cluster_id,
                name=f"worker-placement-{cluster_id}",
                endpoint="https://cluster.test",
                api_key="cluster-secret",
                status=ClusterStatus.ACTIVE,
                enabled=True,
                cluster_type=ClusterType.OPENSHIFT,
                labels={},
                created_by=uuid4(),
                updated_by=uuid4(),
                created_at=now,
                updated_at=now,
            )
        )
        await session.flush()
        session.add(
            ExecutionTarget(
                id=target_id,
                cluster_id=cluster_id,
                name=f"workers-{target_id}",
                backend_type=BackendType.VANILLA_K8S,
                endpoint="https://target.test",
                placement=KubernetesPlacement(namespace="ep"),
                api_key="target-secret",
                status=TargetStatus.ACTIVE,
                enabled=True,
                is_default=False,
                labels={selector_key: target_selector_value},
                created_by=uuid4(),
                updated_by=uuid4(),
                created_at=now,
                updated_at=now,
            )
        )
        await session.flush()
        session.add(
            WorkItem(
                id=work_id,
                client_id="worker-placement-test",
                status=WorkItemStatus.PENDING,
                payload={"selectors": {selector_key: "match"}},
                created_at=created_at,
            )
        )

    return cluster_id, target_id, work_id


async def _cleanup_work(sessions: async_sessionmaker[AsyncSession], ids: tuple[UUID, UUID, UUID]) -> None:
    cluster_id, target_id, work_id = ids
    async with sessions.begin() as session:
        await session.execute(delete(CompletionEvent).where(col(CompletionEvent.work_item_id) == work_id))
        work_item = await session.get(WorkItem, work_id)
        if work_item is not None:
            await session.delete(work_item)
        target = await session.get(ExecutionTarget, target_id)
        if target is not None:
            await session.delete(target)
        cluster = await session.get(Cluster, cluster_id)
        if cluster is not None:
            await session.delete(cluster)


async def test_worker_places_reserved_item_on_the_matching_postgres_target(
    placement_database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    engine, sessions = placement_database
    ids = await _seed_work(sessions, matching=True)

    try:
        async with (
            WorkStore.from_engine(engine) as work_store,
            ClusterStore.from_engine(engine) as cluster_store,
            ExecutionTargetStore.from_engine(engine) as target_store,
        ):
            resolver = build_placement_resolver(cluster_store, target_store)
            reserved = await work_store.reserve_one()
            assert reserved is not None
            assert reserved.id == ids[2]

            assigned = await worker._place_reserved_item(reserved, work_store, resolver)

            assert assigned is not None
            assert assigned.id == ids[2]
            assert assigned.execution_target_id == ids[1]
            assert assigned.status is WorkItemStatus.CLAIMED
            assert assigned.claim_owner_id is not None
            assert assigned.claim_generation == 1

        async with sessions() as session:
            persisted = await session.get(WorkItem, ids[2])
            assert persisted is not None
            assert persisted.execution_target_id == ids[1]
            assert persisted.status is WorkItemStatus.CLAIMED
    finally:
        await _cleanup_work(sessions, ids)


async def test_worker_fails_reserved_item_when_postgres_has_no_matching_target(
    placement_database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    engine, sessions = placement_database
    ids = await _seed_work(sessions, matching=False)

    try:
        async with (
            WorkStore.from_engine(engine) as work_store,
            ClusterStore.from_engine(engine) as cluster_store,
            ExecutionTargetStore.from_engine(engine) as target_store,
        ):
            resolver = build_placement_resolver(cluster_store, target_store)
            reserved = await work_store.reserve_one()
            assert reserved is not None
            assert reserved.id == ids[2]

            assigned = await worker._place_reserved_item(reserved, work_store, resolver)

            assert assigned is None

        async with sessions() as session:
            persisted = await session.get(WorkItem, ids[2])
            assert persisted is not None
            assert persisted.status is WorkItemStatus.FAILED
            assert persisted.result == {
                "error": "No execution target matched placement requirements",
                "error_type": "TargetUnavailable",
            }
            assert persisted.completed_at is not None
            assert persisted.claim_owner_id is None
    finally:
        await _cleanup_work(sessions, ids)


async def test_worker_releases_reservation_when_target_labels_change_during_placement(
    placement_database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    engine, sessions = placement_database
    ids = await _seed_work(sessions, matching=True)

    try:
        async with (
            WorkStore.from_engine(engine) as work_store,
            ClusterStore.from_engine(engine) as cluster_store,
            ExecutionTargetStore.from_engine(engine) as target_store,
        ):
            resolver = build_placement_resolver(cluster_store, target_store)
            reserved = await work_store.reserve_one()
            assert reserved is not None
            assert reserved.id == ids[2]
            assert reserved.status is WorkItemStatus.CLAIMED

            stale_result = await resolver.resolve(worker._requirements_for(reserved))
            assert stale_result.available_targets[0].id == ids[1]

            selector_key = next(iter(reserved.payload["selectors"]))
            async with sessions.begin() as session:
                target = await session.get(ExecutionTarget, ids[1])
                assert target is not None
                target.labels = {selector_key: "changed-after-resolution"}

            assigned = await worker._place_reserved_item(
                reserved,
                work_store,
                cast("PlacementResolver", _StalePlacementResolver(resolver, stale_result)),
            )

            assert assigned is None

        async with sessions() as session:
            persisted = await session.get(WorkItem, ids[2])
            assert persisted is not None
            assert persisted.status is WorkItemStatus.PENDING
            assert persisted.execution_target_id is None
            assert persisted.claim_owner_id is None
            assert persisted.claimed_at is None
            assert persisted.result is None
    finally:
        await _cleanup_work(sessions, ids)


async def test_worker_releases_reservation_when_cluster_is_disabled_during_placement(
    placement_database: tuple[AsyncEngine, async_sessionmaker[AsyncSession]],
) -> None:
    engine, sessions = placement_database
    ids = await _seed_work(sessions, matching=True)

    try:
        async with (
            WorkStore.from_engine(engine) as work_store,
            ClusterStore.from_engine(engine) as cluster_store,
            ExecutionTargetStore.from_engine(engine) as target_store,
        ):
            resolver = build_placement_resolver(cluster_store, target_store)
            reserved = await work_store.reserve_one()
            assert reserved is not None
            assert reserved.id == ids[2]
            assert reserved.status is WorkItemStatus.CLAIMED

            stale_result = await resolver.resolve(worker._requirements_for(reserved))
            assert stale_result.available_targets[0].id == ids[1]

            async with sessions.begin() as session:
                cluster = await session.get(Cluster, ids[0])
                assert cluster is not None
                cluster.enabled = False

            assigned = await worker._place_reserved_item(
                reserved,
                work_store,
                cast("PlacementResolver", _StalePlacementResolver(resolver, stale_result)),
            )

            assert assigned is None

        async with sessions() as session:
            persisted = await session.get(WorkItem, ids[2])
            assert persisted is not None
            assert persisted.status is WorkItemStatus.PENDING
            assert persisted.execution_target_id is None
            assert persisted.claim_owner_id is None
            assert persisted.claimed_at is None
            assert persisted.result is None
    finally:
        await _cleanup_work(sessions, ids)
