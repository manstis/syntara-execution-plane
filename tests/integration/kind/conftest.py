"""Kubernetes cluster fixture for the dispatch integration tests.

These tests need a live Kubernetes cluster in addition to PostgreSQL. The
cluster is provisioned *outside* the suite (a kind cluster in CI, any reachable
cluster locally) and its coordinates are passed in through the environment:

* ``EP_IT_K8S_ENDPOINT``  — API server URL (e.g. ``https://127.0.0.1:PORT``)
* ``EP_IT_K8S_TOKEN``     — bearer token for the ``syntara-dispatcher`` ServiceAccount
* ``EP_IT_K8S_NAMESPACE`` — target namespace (default: ``execution-plane``)
* ``EP_IT_K8S_CA_CERT``   — PEM CA certificate for the API server (optional; skips
                            TLS verification when absent and ``verify_ssl`` is False)

The ``test-integration-postgres-kind`` CI job provisions a kind cluster and
exports all of these. Local developers can point at any running cluster that
satisfies the RBAC in ``deploy/kubernetes/execution-target/rbac.yaml``.

The shared PostgreSQL fixtures (``migrated_database`` etc.) come from the parent
``tests/integration/conftest.py``.
"""

from __future__ import annotations

import os
import tempfile
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from kubernetes import client as kubernetes_client
from kubernetes.client import BatchV1Api
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlmodel import col

from execution_plane.cluster.cluster_registrar import ClusterRegistrar, NoopDiscoveryMechanism
from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.credential_resolution.resolver import CredentialResolutionError, CredentialResolver
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.models.cluster import Cluster, ClusterType
from execution_plane.models.execution_target import ExecutionTarget
from execution_plane.models.execution_target_placement import KubernetesPlacement
from execution_plane.models.work_item import WorkItem

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

_EP_IT_ENDPOINT = "EP_IT_K8S_ENDPOINT"
_EP_IT_TOKEN = "EP_IT_K8S_TOKEN"  # noqa: S105 — env var name, not a secret value
_EP_IT_NAMESPACE = "EP_IT_K8S_NAMESPACE"
_EP_IT_CA_CERT = "EP_IT_K8S_CA_CERT"
_DEFAULT_NAMESPACE = "execution-plane"
_CLUSTER_NAME = "integration-test"
_CLUSTER_CLIENT_ID = "ep-integration-tests"
_CREDENTIAL_REF = uuid.uuid5(uuid.NAMESPACE_URL, "https://execution-plane.test/integrations/kind")
_CLI_ACTOR_ID = uuid.UUID(int=0)


class StaticCredentialResolver(CredentialResolver):
    """Return the live test-cluster token without requiring an AO callback."""

    def __init__(self, credential: str) -> None:
        """Store the in-memory test credential."""
        self._credential = credential

    async def resolve(self, credential_ref: uuid.UUID, *, client_id: str) -> str:
        """Return the configured test credential for the integration Cluster."""
        if credential_ref != _CREDENTIAL_REF or client_id != _CLUSTER_CLIENT_ID:
            message = "Integration resolver received an unexpected credential reference"
            raise CredentialResolutionError(message)
        return self._credential

    async def close(self) -> None:
        """Keep the test credential in memory only."""


async def _delete_fixture_cluster(database_url: str, cluster_id: uuid.UUID) -> None:
    """Delete fixture-owned rows directly because the domain protects the default Cluster."""
    engine = create_async_engine(database_url)
    try:
        async with async_sessionmaker(engine)() as session:
            target_ids = select(col(ExecutionTarget.id)).where(col(ExecutionTarget.cluster_id) == cluster_id)
            await session.execute(
                update(WorkItem)
                .where(col(WorkItem.execution_target_id).in_(target_ids))
                .values(execution_target_id=None)
            )
            await session.execute(delete(ExecutionTarget).where(col(ExecutionTarget.cluster_id) == cluster_id))
            await session.execute(delete(Cluster).where(col(Cluster.id) == cluster_id))
            await session.commit()
    finally:
        await engine.dispose()


def ep_cluster_configured() -> bool:
    """Return True when kind cluster coordinates are present in the environment."""
    return bool(os.environ.get(_EP_IT_ENDPOINT) and os.environ.get(_EP_IT_TOKEN))


@pytest.fixture(scope="session")
async def ep_cluster(migrated_database: str) -> AsyncGenerator[Cluster, None]:
    """Provision a kind cluster as an ACTIVE Cluster+ExecutionTarget in the test DB.

    Skips when ``EP_IT_K8S_ENDPOINT`` / ``EP_IT_K8S_TOKEN`` are absent so that
    the PostgreSQL-only tests still run on runners without a cluster.

    Yields the provisioned ``Cluster`` and tears it down at the end of the session.
    """
    if not ep_cluster_configured():
        pytest.skip("EP_IT_K8S_ENDPOINT and EP_IT_K8S_TOKEN must be set for cluster dispatch tests")

    endpoint = os.environ[_EP_IT_ENDPOINT]
    namespace = os.environ.get(_EP_IT_NAMESPACE, _DEFAULT_NAMESPACE)
    ca_cert = os.environ.get(_EP_IT_CA_CERT)

    cluster_store = ClusterStore.from_database_url(migrated_database)
    target_store = ExecutionTargetStore.from_database_url(migrated_database)

    cluster: Cluster | None = None
    try:
        cluster_registrar = ClusterRegistrar(cluster_store, target_store, NoopDiscoveryMechanism())

        cluster = await cluster_registrar.provision(
            _CLUSTER_NAME,
            endpoint,
            _CREDENTIAL_REF,
            KubernetesPlacement(namespace=namespace),
            _CLI_ACTOR_ID,
            labels={"provider": "kind", "cluster": _CLUSTER_NAME},
            cluster_type=ClusterType.OPENSHIFT,
            source_client_id=_CLUSTER_CLIENT_ID,
            source_integration_id=uuid.uuid4(),
        )

        if ca_cert:
            await cluster_registrar.sync_update(
                cluster.id,
                updated_by=_CLI_ACTOR_ID,
                ca_certificate=ca_cert,
            )

        yield cluster

    finally:
        try:
            if cluster is not None:
                await _delete_fixture_cluster(migrated_database, cluster.id)
        finally:
            await cluster_store.close()
            await target_store.close()


@pytest.fixture(scope="session")
async def kubernetes_batch_api(ep_cluster: Cluster) -> AsyncGenerator[BatchV1Api, None]:
    """Read Kubernetes resources through the configured test cluster credentials."""
    endpoint = os.environ[_EP_IT_ENDPOINT]
    token = os.environ[_EP_IT_TOKEN]
    ca_cert = os.environ.get(_EP_IT_CA_CERT)

    configuration = kubernetes_client.Configuration()
    configuration.host = endpoint
    configuration.api_key["authorization"] = token
    configuration.api_key["BearerToken"] = token
    configuration.api_key_prefix["authorization"] = "Bearer"
    configuration.api_key_prefix["BearerToken"] = "Bearer"
    configuration.verify_ssl = True

    with tempfile.TemporaryDirectory(prefix="ep-it-k8s-ca-") as ca_directory:
        if ca_cert:
            ca_path = Path(ca_directory) / "ca.pem"
            ca_path.write_text(ca_cert, encoding="utf-8")
            configuration.ssl_ca_cert = str(ca_path)

        with kubernetes_client.ApiClient(configuration) as api_client:
            yield BatchV1Api(api_client)


@pytest.fixture(scope="session")
def integration_credential_resolver(ep_cluster: Cluster) -> CredentialResolver:
    """Provide an AO-free resolver for the live integration Cluster."""
    return StaticCredentialResolver(os.environ[_EP_IT_TOKEN])
