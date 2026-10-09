"""Tests for the Execution Plane worker lifecycle."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Self, cast

import pytest

from execution_plane.execution_target_reconciler.types import (
    ClusterSnapshot,
    ExecutionTargetSnapshot,
    ReconcileResult,
    ResolveOutcome,
    WorkRequirements,
)
from execution_plane.models.cluster import Cluster, ClusterStatus, ClusterType
from execution_plane.models.execution_target import BackendType, ExecutionTarget, TargetStatus
from execution_plane.models.execution_target_placement import KubernetesPlacement
from execution_plane.models.work_item import WorkItem, WorkItemStatus

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from execution_plane.execution_target_reconciler.placement import PlacementResolver, WorkerManagerRegistry
    from execution_plane.work_store import WorkStore


def _cluster() -> Cluster:
    now = datetime.now(UTC)
    return Cluster(
        id=uuid.uuid4(),
        name="local-cluster",
        endpoint="https://cluster.example",
        api_key="cluster-secret",
        status=ClusterStatus.ACTIVE,
        enabled=True,
        cluster_type=ClusterType.OPENSHIFT,
        labels={"region": "us-east-1"},
        created_by=uuid.uuid4(),
        created_at=now,
        updated_by=uuid.uuid4(),
        updated_at=now,
    )


def _target(cluster: Cluster, *, name: str = "ep-default") -> ExecutionTarget:
    now = datetime.now(UTC)
    return ExecutionTarget(
        id=uuid.uuid4(),
        cluster_id=cluster.id,
        name=name,
        backend_type=BackendType.VANILLA_K8S,
        endpoint="https://target.example",
        placement=KubernetesPlacement(namespace="ao-execution"),
        api_key="target-secret",
        status=TargetStatus.ACTIVE,
        enabled=True,
        is_default=True,
        labels={"env": "production"},
        created_by=uuid.uuid4(),
        created_at=now,
        updated_by=uuid.uuid4(),
        updated_at=now,
    )


def _reserved_item(payload: dict[str, Any]) -> WorkItem:
    return WorkItem(
        id=uuid.uuid4(),
        client_id="test-client",
        status=WorkItemStatus.CLAIMED,
        payload=payload,
        claim_owner_id=uuid.uuid4(),
        claim_generation=7,
        created_at=datetime.now(UTC),
    )


def _target_snapshot(cluster: Cluster, target: ExecutionTarget) -> ExecutionTargetSnapshot:
    return ExecutionTargetSnapshot(
        id=target.id,
        cluster=ClusterSnapshot(
            id=cluster.id,
            name=cluster.name,
            labels=dict(cluster.labels),
            cluster_type=cluster.cluster_type,
            enabled=True,
        ),
        name=target.name,
        placement=target.placement,
        backend_type=target.backend_type,
        labels=dict(target.labels),
        lifecycle=target.status.value,
        enabled=target.enabled,
        is_default=True,
    )


class _PlacementResolver:
    def __init__(self, result: ReconcileResult, *, eligible: bool) -> None:
        self.result = result
        self.eligible = eligible
        self.resolved_requirements: list[WorkRequirements] = []
        self.eligibility_checks: list[tuple[ClusterSnapshot, ExecutionTargetSnapshot, WorkRequirements]] = []

    async def resolve(self, requirements: WorkRequirements) -> ReconcileResult:
        self.resolved_requirements.append(requirements)
        return self.result

    async def target_is_eligible(
        self,
        cluster: ClusterSnapshot,
        target: ExecutionTargetSnapshot,
        requirements: WorkRequirements,
    ) -> bool:
        self.eligibility_checks.append((cluster, target, requirements))
        return self.eligible


class _WorkStore:
    def __init__(
        self,
        item: WorkItem,
        targets: dict[uuid.UUID, tuple[Cluster, ExecutionTarget]],
        *,
        assigned_target_id: uuid.UUID | None = None,
    ) -> None:
        self.item = item
        self.targets = targets
        self.assigned_target_id = assigned_target_id
        self.assigned_target_ids: list[uuid.UUID] = []
        self.eligibility_results: list[bool] = []
        self.results: list[tuple[dict[str, Any], WorkItemStatus, uuid.UUID, int]] = []
        self.released_claims: list[tuple[uuid.UUID, int]] = []

    async def set_result(
        self,
        _item_id: uuid.UUID,
        result: dict[str, Any],
        status: WorkItemStatus,
        *,
        claim_owner_id: uuid.UUID,
        claim_generation: int,
    ) -> WorkItem:
        self.results.append((result, status, claim_owner_id, claim_generation))
        return self.item

    async def assign_target(
        self,
        _item_id: uuid.UUID,
        target_id: uuid.UUID,
        *,
        claim_owner_id: uuid.UUID,
        claim_generation: int,
        eligibility: Callable[[Cluster, ExecutionTarget], Awaitable[bool]],
    ) -> WorkItem | None:
        assert claim_owner_id == self.item.claim_owner_id
        assert claim_generation == self.item.claim_generation
        self.assigned_target_ids.append(target_id)
        cluster, target = self.targets[target_id]
        self.eligibility_results.append(await eligibility(cluster, target))
        if target_id == self.assigned_target_id and self.eligibility_results[-1]:
            return self.item
        return None

    async def release_claim(
        self,
        _item_id: uuid.UUID,
        *,
        claim_owner_id: uuid.UUID,
        claim_generation: int,
    ) -> bool:
        assert claim_owner_id == self.item.claim_owner_id
        assert claim_generation == self.item.claim_generation
        self.released_claims.append((claim_owner_id, claim_generation))
        return True


class _StoreContext:
    def __init__(self, name: str, events: list[str]) -> None:
        self.name = name
        self.events = events

    async def __aenter__(self) -> Self:
        self.events.append(f"{self.name}:start")
        return self

    async def __aexit__(self, *_: object) -> None:
        self.events.append(f"{self.name}:stop")


class _DrainMonitor:
    def __init__(self, _target_store: object, _cluster_store: object, _work_store: object, events: list[str]) -> None:
        self.events = events

    async def start(self) -> None:
        self.events.append("monitor:start")

    async def stop(self) -> None:
        self.events.append("monitor:stop")


class _CompletionDelivery:
    def __init__(self, _settings: object, events: list[str]) -> None:
        self.events = events

    async def run(self, _store: object) -> None:
        self.events.append("delivery:run")

    async def close(self) -> None:
        self.events.append("delivery:stop")


def test_requirements_for_extracts_payload_constraints() -> None:
    from execution_plane import worker

    item = _reserved_item({"selectors": {"region": "us-east-1"}, "workload_type": "agent"})

    assert worker._requirements_for(item) == WorkRequirements(
        selectors={"region": "us-east-1"},
        workload_type="agent",
    )


def test_requirements_for_ignores_malformed_constraints() -> None:
    from execution_plane import worker

    item = _reserved_item({"selectors": ["region=us-east-1"], "workload_type": 42})

    assert worker._requirements_for(item) == WorkRequirements()


@pytest.mark.asyncio
async def test_place_reserved_item_fails_when_no_target_matches() -> None:
    from execution_plane import worker

    item = _reserved_item({"selectors": {"region": "ap-southeast-1"}})
    store = _WorkStore(item, {})
    resolver = _PlacementResolver(
        ReconcileResult([], [], ResolveOutcome.NO_MATCHING_TARGETS),
        eligible=True,
    )

    assigned = await worker._place_reserved_item(item, cast("WorkStore", store), cast("PlacementResolver", resolver))

    assert assigned is None
    assert resolver.resolved_requirements == [WorkRequirements(selectors={"region": "ap-southeast-1"})]
    assert store.results == [
        (
            {"error": "No execution target matched placement requirements", "error_type": "TargetUnavailable"},
            WorkItemStatus.FAILED,
            item.claim_owner_id,
            item.claim_generation,
        )
    ]
    assert store.assigned_target_ids == []
    assert store.released_claims == []


@pytest.mark.asyncio
async def test_place_reserved_item_revalidates_and_assigns_target() -> None:
    from execution_plane import worker

    cluster = _cluster()
    target = _target(cluster)
    item = _reserved_item({"selectors": {"region": "us-east-1"}, "workload_type": "agent"})
    store = _WorkStore(item, {target.id: (cluster, target)}, assigned_target_id=target.id)
    resolver = _PlacementResolver(
        ReconcileResult([_target_snapshot(cluster, target)], [], ResolveOutcome.MATCHED),
        eligible=True,
    )

    assigned = await worker._place_reserved_item(item, cast("WorkStore", store), cast("PlacementResolver", resolver))

    requirements = WorkRequirements(selectors={"region": "us-east-1"}, workload_type="agent")
    expected_cluster = ClusterSnapshot(
        id=cluster.id,
        name="local-cluster",
        labels={"region": "us-east-1"},
        cluster_type=ClusterType.OPENSHIFT,
        enabled=True,
    )
    expected_target = ExecutionTargetSnapshot(
        id=target.id,
        cluster=expected_cluster,
        name="ep-default",
        placement=KubernetesPlacement(namespace="ao-execution"),
        backend_type=BackendType.VANILLA_K8S,
        labels={"env": "production"},
        lifecycle="active",
        enabled=True,
        is_default=True,
    )
    assert assigned is item
    assert store.assigned_target_ids == [target.id]
    assert store.eligibility_results == [True]
    assert resolver.eligibility_checks == [(expected_cluster, expected_target, requirements)]
    assert store.results == []
    assert store.released_claims == []


@pytest.mark.asyncio
async def test_place_reserved_item_releases_claim_when_targets_become_ineligible() -> None:
    from execution_plane import worker

    cluster = _cluster()
    first = _target(cluster, name="ep-first")
    second = _target(cluster, name="ep-second")
    item = _reserved_item({})
    store = _WorkStore(
        item,
        {first.id: (cluster, first), second.id: (cluster, second)},
    )
    resolver = _PlacementResolver(
        ReconcileResult(
            [_target_snapshot(cluster, first), _target_snapshot(cluster, second)],
            [],
            ResolveOutcome.MATCHED,
        ),
        eligible=False,
    )

    assigned = await worker._place_reserved_item(item, cast("WorkStore", store), cast("PlacementResolver", resolver))

    assert assigned is None
    assert store.assigned_target_ids == [first.id, second.id]
    assert store.eligibility_results == [False, False]
    assert store.released_claims == [(item.claim_owner_id, item.claim_generation)]
    assert store.results == []


@pytest.mark.asyncio
async def test_run_worker_wires_placement_resolver_to_the_poll_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    from execution_plane import worker

    events: list[str] = []
    work_store = _StoreContext("work", events)
    cluster_store = _StoreContext("cluster", events)
    target_store = _StoreContext("target", events)
    workload_manager = object()
    placement_resolver = object()
    built_with: list[tuple[object, object, WorkerManagerRegistry]] = []
    poll_calls: list[tuple[WorkerManagerRegistry, object]] = []

    monkeypatch.setattr(worker, "get_ep_settings", lambda: SimpleNamespace())
    monkeypatch.setattr(
        "execution_plane.worker.WorkStore.from_database_url",
        lambda _url: work_store,
    )
    monkeypatch.setattr(
        "execution_plane.worker.ClusterStore.from_database_url",
        lambda _url: cluster_store,
    )
    monkeypatch.setattr(
        "execution_plane.worker.ExecutionTargetStore.from_database_url",
        lambda _url: target_store,
    )
    monkeypatch.setattr(
        worker,
        "DrainMonitor",
        lambda target, cluster, work: _DrainMonitor(target, cluster, work, events),
    )
    monkeypatch.setattr(worker, "CompletionNotifier", lambda _settings: _CompletionDelivery(SimpleNamespace(), events))
    monkeypatch.setattr(worker, "VanillaK8sWorkerManager", lambda *_args: workload_manager)

    def build_resolver(cluster: object, target: object, managers: WorkerManagerRegistry) -> object:
        built_with.append((cluster, target, managers))
        return placement_resolver

    async def record_poller(
        _store: object,
        _target_store: object,
        managers: WorkerManagerRegistry,
        resolver: object,
        _settings: object,
        _wakeup_event: object,
    ) -> None:
        poll_calls.append((managers, resolver))

    monkeypatch.setattr(worker, "build_placement_resolver", build_resolver)
    monkeypatch.setattr(worker, "_listen_loop", _finished_listener)
    monkeypatch.setattr(worker, "_poll_loop", record_poller)
    monkeypatch.setattr(worker, "run_cluster_binding_reconciler", _finished_reconciler)
    monkeypatch.setattr(worker, "run_orphan_policy_reconciler", _finished_policy_reconciler)

    await worker.run_worker("postgresql+asyncpg://localhost/syntara")

    assert built_with == [(cluster_store, target_store, poll_calls[0][0])]
    assert poll_calls == [(poll_calls[0][0], placement_resolver)]
    assert poll_calls[0][0].get(BackendType.VANILLA_K8S) is workload_manager


@pytest.mark.asyncio
async def test_run_worker_owns_drain_monitor_for_the_worker_lifetime(monkeypatch: pytest.MonkeyPatch) -> None:
    from execution_plane import worker

    events: list[str] = []
    monkeypatch.setattr(worker, "get_ep_settings", lambda: SimpleNamespace())

    monkeypatch.setattr(
        "execution_plane.worker.WorkStore.from_database_url",
        lambda _url: _StoreContext("work", events),
    )
    monkeypatch.setattr(
        "execution_plane.worker.ClusterStore.from_database_url",
        lambda _url: _StoreContext("cluster", events),
    )
    monkeypatch.setattr(
        "execution_plane.worker.ExecutionTargetStore.from_database_url",
        lambda _url: _StoreContext("target", events),
    )
    monkeypatch.setattr(
        worker,
        "DrainMonitor",
        lambda target, cluster, work: _DrainMonitor(target, cluster, work, events),
    )
    monkeypatch.setattr(worker, "CompletionNotifier", lambda settings: _CompletionDelivery(settings, events))
    monkeypatch.setattr(worker, "VanillaK8sWorkerManager", lambda *_args: object())
    monkeypatch.setattr(worker, "build_placement_resolver", lambda *_args: object())
    monkeypatch.setattr(worker, "_listen_loop", _finished_listener)
    monkeypatch.setattr(worker, "_poll_loop", _finished_poller)
    monkeypatch.setattr(worker, "run_cluster_binding_reconciler", _finished_reconciler)
    monkeypatch.setattr(worker, "run_orphan_policy_reconciler", _finished_policy_reconciler)

    await worker.run_worker("postgresql+asyncpg://localhost/syntara")

    assert events.index("monitor:start") < events.index("monitor:stop")
    assert events.index("monitor:stop") < events.index("work:stop")


async def _finished_listener(_database_url: str, _wakeup_event: object) -> None:
    return None


async def _finished_poller(*_args: object) -> None:
    return None


async def _finished_reconciler(_database_url: str) -> None:
    return None


async def _finished_policy_reconciler(*_args: object) -> None:
    return None
