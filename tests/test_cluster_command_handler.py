"""Cluster command label policy without requiring a database."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Self
from unittest.mock import Mock
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from execution_plane.cluster.cluster_store import DefaultClusterError
from execution_plane.commands.clusters import (
    ClusterDeleteCommandHandler,
    ClusterProvisionCommandHandler,
    ClusterRegisterCommandHandler,
    ClusterUpdateCommandHandler,
)
from execution_plane.commands.context import EPCommandContext
from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.cluster_binding import ClusterBinding
from execution_plane_client.commands.clusters import ClusterDelete, ClusterProvision, ClusterRegister, ClusterUpdate
from execution_plane_client.protocol import MessageEnvelope


class _Session:
    def __init__(self, binding: ClusterBinding | None = None, cluster: Cluster | None = None) -> None:
        self.binding = binding
        self.cluster = cluster
        self.added: ClusterBinding | None = None

    def add(self, binding: ClusterBinding) -> None:
        self.added = binding

    async def execute(self, _statement: object) -> None:
        return None

    async def get(self, model: object, _key: object, **_kwargs: object) -> ClusterBinding | Cluster | None:
        if model is Cluster:
            return self.cluster
        return self.binding


class _Transaction:
    def __init__(self, session: _Session) -> None:
        self.session = session

    async def __aenter__(self) -> _Session:
        return self.session

    async def __aexit__(self, *_: object) -> None:
        return None


class _SessionMaker:
    def __init__(self, session: _Session) -> None:
        self.session = session

    def __call__(self, *_: object, **_kwargs: object) -> Self:
        return self

    def begin(self) -> _Transaction:
        return _Transaction(self.session)


def _context(monkeypatch: pytest.MonkeyPatch, session: _Session) -> EPCommandContext:
    monkeypatch.setattr(
        "execution_plane.commands.clusters.async_sessionmaker",
        lambda *_args, **_kwargs: _SessionMaker(session),
    )
    return EPCommandContext(client_id="ao", engine=Mock(spec=AsyncEngine))


@pytest.mark.asyncio
async def test_cluster_register_cleanses_internal_labels(monkeypatch: pytest.MonkeyPatch) -> None:
    session = _Session()
    payload = ClusterRegister(
        resource_key=uuid4(),
        desired_revision=1,
        name="cluster-a",
        endpoint="https://cluster.example",
        namespace="ep",
        credential_ref=uuid4(),
        labels={"team": "platform", "_cluster_id": "spoofed", "_internal": "spoofed"},
    )

    await ClusterRegisterCommandHandler().handle(
        Mock(spec=MessageEnvelope),
        payload,
        _context(monkeypatch, session),
    )

    assert session.added is not None
    assert session.added.labels == {"team": "platform"}


@pytest.mark.parametrize(
    ("payload_class", "handler_class"),
    [
        (ClusterRegister, ClusterRegisterCommandHandler),
        (ClusterProvision, ClusterProvisionCommandHandler),
    ],
)
@pytest.mark.asyncio
async def test_cluster_creation_persists_the_global_default(
    monkeypatch: pytest.MonkeyPatch,
    payload_class: type[ClusterRegister],
    handler_class: type[ClusterRegisterCommandHandler | ClusterProvisionCommandHandler],
) -> None:
    session = _Session()
    payload = payload_class(
        resource_key=uuid4(),
        desired_revision=1,
        name="cluster-a",
        endpoint="https://cluster.example",
        namespace="ep",
        credential_ref=uuid4(),
        is_default=True,
    )

    await handler_class().handle(
        Mock(spec=MessageEnvelope),
        payload,
        _context(monkeypatch, session),
    )

    assert session.added is not None
    assert session.added.is_default is True


@pytest.mark.asyncio
async def test_cluster_update_cleanses_internal_labels(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    key = uuid4()
    binding = ClusterBinding(
        client_id="ao",
        source_integration_id=key,
        revision=1,
        name="cluster-a",
        endpoint="https://cluster.example",
        namespace="ep",
        credential_ref=uuid4(),
        labels={"team": "old"},
        created_at=now,
        updated_at=now,
    )
    session = _Session(binding)
    payload = ClusterUpdate(
        resource_key=key,
        desired_revision=2,
        labels={"team": "new", "_cluster_id": "spoofed", "_internal": "spoofed"},
    )

    await ClusterUpdateCommandHandler().handle(
        Mock(spec=MessageEnvelope),
        payload,
        _context(monkeypatch, session),
    )

    assert binding.labels == {"team": "new"}


@pytest.mark.asyncio
async def test_cluster_update_reassigns_the_global_default(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    key = uuid4()
    binding = ClusterBinding(
        client_id="ao",
        source_integration_id=key,
        revision=1,
        name="cluster-a",
        endpoint="https://cluster.example",
        namespace="ep",
        credential_ref=uuid4(),
        is_default=False,
        created_at=now,
        updated_at=now,
    )
    session = _Session(binding)
    payload = ClusterUpdate(
        resource_key=key,
        desired_revision=2,
        is_default=True,
    )

    await ClusterUpdateCommandHandler().handle(
        Mock(spec=MessageEnvelope),
        payload,
        _context(monkeypatch, session),
    )

    assert binding.is_default is True


@pytest.mark.asyncio
async def test_cluster_update_rejects_demoting_the_current_default(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    key = uuid4()
    cluster = Cluster(
        id=uuid4(),
        name="cluster-a",
        endpoint="https://cluster.example",
        credential_ref=uuid4(),
        status=ClusterStatus.ACTIVE,
        is_default=True,
        created_by=uuid4(),
        created_at=now,
        updated_by=uuid4(),
        updated_at=now,
    )
    binding = ClusterBinding(
        client_id="ao",
        source_integration_id=key,
        revision=1,
        name="cluster-a",
        endpoint="https://cluster.example",
        namespace="ep",
        credential_ref=uuid4(),
        is_default=True,
        cluster_id=cluster.id,
        created_at=now,
        updated_at=now,
    )
    session = _Session(binding, cluster)
    payload = ClusterUpdate(
        resource_key=key,
        desired_revision=2,
        is_default=False,
    )

    with pytest.raises(DefaultClusterError):
        await ClusterUpdateCommandHandler().handle(
            Mock(spec=MessageEnvelope),
            payload,
            _context(monkeypatch, session),
        )

    assert binding.is_default is True


@pytest.mark.asyncio
async def test_cluster_update_rejects_disabling_the_current_default(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    key = uuid4()
    cluster = Cluster(
        id=uuid4(),
        name="cluster-a",
        endpoint="https://cluster.example",
        credential_ref=uuid4(),
        status=ClusterStatus.ACTIVE,
        is_default=True,
        created_by=uuid4(),
        created_at=now,
        updated_by=uuid4(),
        updated_at=now,
    )
    binding = ClusterBinding(
        client_id="ao",
        source_integration_id=key,
        revision=1,
        name="cluster-a",
        endpoint="https://cluster.example",
        namespace="ep",
        credential_ref=uuid4(),
        is_default=True,
        cluster_id=cluster.id,
        created_at=now,
        updated_at=now,
    )
    session = _Session(binding, cluster)
    payload = ClusterUpdate(resource_key=key, desired_revision=2, enabled=False)

    with pytest.raises(DefaultClusterError):
        await ClusterUpdateCommandHandler().handle(
            Mock(spec=MessageEnvelope),
            payload,
            _context(monkeypatch, session),
        )

    assert binding.enabled is True


@pytest.mark.asyncio
async def test_cluster_delete_rejects_the_current_default(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    key = uuid4()
    cluster = Cluster(
        id=uuid4(),
        name="cluster-a",
        endpoint="https://cluster.example",
        credential_ref=uuid4(),
        status=ClusterStatus.ACTIVE,
        is_default=True,
        created_by=uuid4(),
        created_at=now,
        updated_by=uuid4(),
        updated_at=now,
    )
    binding = ClusterBinding(
        client_id="ao",
        source_integration_id=key,
        revision=1,
        name="cluster-a",
        endpoint="https://cluster.example",
        namespace="ep",
        credential_ref=uuid4(),
        is_default=True,
        cluster_id=cluster.id,
        created_at=now,
        updated_at=now,
    )
    session = _Session(binding, cluster)
    payload = ClusterDelete(resource_key=key, desired_revision=2)

    with pytest.raises(DefaultClusterError):
        await ClusterDeleteCommandHandler().handle(
            Mock(spec=MessageEnvelope),
            payload,
            _context(monkeypatch, session),
        )

    assert binding.enabled is True
