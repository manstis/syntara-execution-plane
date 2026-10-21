"""Namespace probe outcomes, skip rules, and secret-free messages."""

from __future__ import annotations

from http import HTTPStatus

import pytest
from kubernetes.client.exceptions import ApiException

from execution_plane.models.cluster import ClusterStatus, ClusterType
from execution_plane.models.execution_target import TargetStatus
from execution_plane.resource_monitor.probes import (
    DispatchingTargetProbe,
    KubernetesNamespaceProbe,
    SkipProbe,
    should_skip_probe,
)
from tests.resource_monitor.conftest import make_cluster, make_target

_TOKEN_BODY = "super-secret-token"  # noqa: S105


@pytest.mark.asyncio
async def test_namespace_probe_success() -> None:
    cluster = make_cluster()
    target = make_target(cluster)
    probe = KubernetesNamespaceProbe(timeout_seconds=1, reader=lambda _cluster, _credential, _namespace: None)

    result = await probe.probe(cluster, "resolved-token", target)

    assert result.healthy is True
    assert result.skipped is False
    assert result.message is None


@pytest.mark.asyncio
async def test_namespace_probe_maps_http_outcomes_without_bodies() -> None:
    cluster = make_cluster()
    target = make_target(cluster, namespace="missing-ns")

    async def probe_status(status: int, reason: str) -> object:
        def reader(_cluster: object, _credential: str, _namespace: str) -> None:
            raise ApiException(status=status, reason=reason)

        return await KubernetesNamespaceProbe(timeout_seconds=1, reader=reader).probe(cluster, "resolved-token", target)

    not_found = await probe_status(HTTPStatus.NOT_FOUND, f"Bearer {_TOKEN_BODY}")
    forbidden = await probe_status(HTTPStatus.FORBIDDEN, f"Bearer {_TOKEN_BODY}")
    unauthorized = await probe_status(HTTPStatus.UNAUTHORIZED, f"Bearer {_TOKEN_BODY}")
    other = await probe_status(HTTPStatus.INTERNAL_SERVER_ERROR, f"Bearer {_TOKEN_BODY}")

    assert not_found.healthy is False
    assert not_found.message == "Namespace missing-ns does not exist"
    assert forbidden.message == "Kubernetes API rejected authorization for the target namespace"
    assert unauthorized.message == "Kubernetes API rejected authorization for the target namespace"
    assert other.message == "Kubernetes API returned HTTP 500"
    for result in (not_found, forbidden, unauthorized, other):
        assert _TOKEN_BODY not in (result.message or "")


@pytest.mark.asyncio
async def test_namespace_probe_timeout_and_unreachable() -> None:
    cluster = make_cluster()
    target = make_target(cluster)

    def hang(_cluster: object, _credential: str, _namespace: str) -> None:
        raise TimeoutError

    def boom(_cluster: object, _credential: str, _namespace: str) -> None:
        leaked = f"connection to https://cluster.example with token {_TOKEN_BODY}"
        raise ConnectionError(leaked)

    timed_out = await KubernetesNamespaceProbe(timeout_seconds=1, reader=hang).probe(cluster, "resolved-token", target)
    unreachable = await KubernetesNamespaceProbe(timeout_seconds=1, reader=boom).probe(
        cluster, "resolved-token", target
    )

    assert timed_out.message == "Kubernetes API timed out"
    assert unreachable.message == "Kubernetes API is unreachable"
    assert _TOKEN_BODY not in (unreachable.message or "")


@pytest.mark.asyncio
async def test_skip_probe_does_not_update() -> None:
    cluster = make_cluster()
    target = make_target(cluster)
    result = await SkipProbe().probe(cluster, "resolved-token", target)
    assert result.skipped is True


@pytest.mark.asyncio
async def test_dispatching_probe_skips_local_and_rhel() -> None:
    calls: list[str] = []

    class RecordingK8s:
        async def probe(self, cluster: object, _credential: str, target: object) -> object:
            del cluster, target
            calls.append("k8s")
            pytest.fail("Kubernetes probe must not run for skipped targets")

    probe = DispatchingTargetProbe(timeout_seconds=1, kubernetes_probe=RecordingK8s())  # type: ignore[arg-type]
    local = make_cluster(endpoint="local://execution-plane")
    rhel = make_cluster(cluster_type=ClusterType.RHEL)
    local_target = make_target(local)
    rhel_target = make_target(rhel)

    assert (await probe.probe(local, "resolved-token", local_target)).skipped is True
    assert (await probe.probe(rhel, "resolved-token", rhel_target)).skipped is True
    assert calls == []


def test_should_skip_probe_covers_lifecycle_and_disabled() -> None:
    cluster = make_cluster()
    assert should_skip_probe(cluster, make_target(cluster, status=TargetStatus.DRAINING)) is True
    assert should_skip_probe(cluster, make_target(cluster, status=TargetStatus.FAILED)) is True
    assert should_skip_probe(cluster, make_target(cluster, status=TargetStatus.REGISTERING)) is True
    assert should_skip_probe(cluster, make_target(cluster, enabled=False)) is True
    assert should_skip_probe(cluster, make_target(cluster, rhel=True)) is True
    assert should_skip_probe(make_cluster(enabled=False), make_target(cluster)) is True
    assert should_skip_probe(make_cluster(status=ClusterStatus.DRAINING), make_target(cluster)) is True
    assert should_skip_probe(cluster, make_target(cluster, status=TargetStatus.ACTIVE)) is False
    assert should_skip_probe(cluster, make_target(cluster, status=TargetStatus.DEGRADED)) is False
