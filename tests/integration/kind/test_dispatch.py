"""End-to-end dispatch integration test: submit → dispatch → COMPLETED.

Requires a real PostgreSQL database (``EP_TEST_DATABASE_URL``) and a live
Kubernetes cluster (``EP_IT_K8S_*`` env vars). Both are provisioned in CI by
the ``test-integration`` GitHub Actions job. Tests are skipped automatically
when either resource is absent.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import time
import uuid
from contextlib import suppress

import pytest

from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.config import EPSettings, get_ep_settings
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.execution_target_reconciler.placement import WorkerManagerRegistry
from execution_plane.models.execution_target import BackendType
from execution_plane.models.execution_target_placement import KubernetesPlacement
from execution_plane.models.work_item import WorkItemStatus
from execution_plane.work_store import WorkStore
from execution_plane.worker import _process_item
from execution_plane.worker_manager.vanilla_k8s.manager import VanillaK8sWorkerManager

_NODE_IMAGE = "quay.io/ahetheri/syntara-node-script:migration-test"
_WORKLOAD_WAIT_SECONDS = 5
_JOB_WAIT_TIMEOUT_SECONDS = 30

_SCRIPT_INVOCATION = {
    "version": 1,
    "operation": "execute",
    "inputs": {"language": "bash", "code": "echo ep-integration-ok"},
    "credentials": {"resolved": {}},
    "workflow_context": {},
    "settings": {},
    "timeout_seconds": 120,
}

_SLEEP_SCRIPT_INVOCATION = {
    "version": 1,
    "operation": "execute",
    "inputs": {"language": "bash", "code": f"sleep {_WORKLOAD_WAIT_SECONDS}"},
    "credentials": {"resolved": {}},
    "workflow_context": {},
    "settings": {},
    "timeout_seconds": 90,
}


def _read_job(job_name: str, namespace: str) -> dict[str, object]:
    result = subprocess.run(
        ["kubectl", "get", "job", job_name, "-n", namespace, "-o", "json"],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def _assert_pod_spec(pod_spec: dict[str, object]) -> None:
    assert pod_spec["nodeSelector"] == {"kubernetes.io/os": "linux"}
    tolerations = pod_spec["tolerations"]
    assert len(tolerations) == 1
    assert tolerations[0]["key"] == "node-role.kubernetes.io/control-plane"
    assert tolerations[0]["operator"] == "Equal"
    assert tolerations[0].get("value", "") == ""
    assert tolerations[0]["effect"] == "NoSchedule"


def _worker_managers(
    target_store: ExecutionTargetStore,
    cluster_store: ClusterStore,
    work_store: WorkStore,
    settings: EPSettings,
) -> WorkerManagerRegistry:
    registry = WorkerManagerRegistry()
    workload_manager = VanillaK8sWorkerManager(target_store, cluster_store, work_store, settings)
    registry.register(BackendType.VANILLA_K8S, workload_manager)
    return registry


@pytest.mark.integration
@pytest.mark.asyncio
async def test_work_item_dispatched_to_kind_cluster_reaches_completed(ep_cluster) -> None:
    """Submit a script work item and assert it completes through a single dispatch cycle.

    The ep_cluster fixture provisions an ACTIVE Cluster+ExecutionTarget backed by
    the kind cluster provisioned in CI. claim_one() assigns that target to the work
    item; _process_item() drives the full cold-start pod lifecycle synchronously.
    """
    database_url = os.environ["EP_TEST_DATABASE_URL"]
    settings = get_ep_settings()
    client_id = "ep-it-dispatch"

    async with (
        WorkStore.from_database_url(database_url) as work_store,
        ClusterStore.from_database_url(database_url) as cluster_store,
        ExecutionTargetStore.from_database_url(database_url) as target_store,
    ):
        item = await work_store.dispatch(
            client_id=client_id,
            item_id=uuid.uuid4(),
            payload={
                "image": _NODE_IMAGE,
                "invocation": _SCRIPT_INVOCATION,
            },
        )

        claimed = await work_store.claim_one()
        assert claimed is not None, "claim_one() returned None — no eligible target is ACTIVE"
        assert claimed.id == item.id

        worker_managers = _worker_managers(target_store, cluster_store, work_store, settings)

        await _process_item(claimed, work_store, target_store, worker_managers, settings)

        final_item = await work_store.get(item.id, client_id=client_id)

    assert final_item is not None
    assert final_item.status is WorkItemStatus.COMPLETED, (
        f"Expected COMPLETED but got {final_item.status}; result={final_item.result}"
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_work_item_dispatched_to_kind_cluster_includes_node_selector_in_pod_spec(ep_cluster) -> None:
    """AAP-97337: configured node selectors must reach the dispatched PodSpec."""
    database_url = os.environ["EP_TEST_DATABASE_URL"]
    settings = get_ep_settings()
    client_id = "ep-it-node-selector"
    namespace = os.environ["EP_IT_K8S_NAMESPACE"]

    async with (
        WorkStore.from_database_url(database_url) as work_store,
        ClusterStore.from_database_url(database_url) as cluster_store,
        ExecutionTargetStore.from_database_url(database_url) as target_store,
    ):
        targets = await target_store.list(cluster_id=ep_cluster.id)
        assert targets, "Expected the kind fixture to provision an ExecutionTarget"
        target = targets[0]
        original_placement = target.placement

        await target_store.update(
            target.id,
            updated_by=uuid.uuid4(),
            placement=KubernetesPlacement(
                namespace=namespace,
                node_selectors=["kubernetes.io/os=linux"],
                tolerations=["node-role.kubernetes.io/control-plane=:NoSchedule"],
            ),
        )

        try:
            item = await work_store.dispatch(
                client_id=client_id,
                item_id=uuid.uuid4(),
                payload={
                    "image": _NODE_IMAGE,
                    "invocation": _SLEEP_SCRIPT_INVOCATION,
                },
            )

            claimed = await work_store.claim_one()
            assert claimed is not None
            assert claimed.id == item.id
            assert claimed.status is WorkItemStatus.CLAIMED

            attempt_id = f"{claimed.id}-{claimed.claim_generation}"
            job_name = "syntara-node-" + hashlib.sha256(attempt_id.encode()).hexdigest()[:32]

            worker_managers = _worker_managers(target_store, cluster_store, work_store, settings)

            processing_task = asyncio.create_task(
                _process_item(claimed, work_store, target_store, worker_managers, settings)
            )

            try:
                deadline = time.monotonic() + _JOB_WAIT_TIMEOUT_SECONDS
                job = None
                while job is None:
                    try:
                        job = await asyncio.to_thread(_read_job, job_name, namespace)
                    except subprocess.CalledProcessError:
                        if time.monotonic() >= deadline:
                            raise
                        await asyncio.sleep(0.5)

                pod_spec = job["spec"]["template"]["spec"]
                _assert_pod_spec(pod_spec)

                dispatched_item = await work_store.get(item.id, client_id=client_id)
                assert dispatched_item is not None
                assert dispatched_item.status is WorkItemStatus.DISPATCHED

                await processing_task
            finally:
                if not processing_task.done():
                    processing_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await processing_task

            final_item = await work_store.get(item.id, client_id=client_id)
        finally:
            await target_store.update(
                target.id,
                updated_by=uuid.uuid4(),
                placement=original_placement,
            )

    assert final_item is not None
    assert final_item.status is WorkItemStatus.COMPLETED, (
        f"Expected COMPLETED but got {final_item.status}; result={final_item.result}"
    )
