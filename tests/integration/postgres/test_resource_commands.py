"""Resource commands converge only the newest client-owned logical revision."""

from uuid import uuid4

import pytest

from execution_plane.models.cluster import ClusterStatus
from execution_plane_client.errors import ProtocolConflict

from .command_helpers import resource_case


async def test_registration_and_provision_are_distinct():
    async with resource_case() as case:
        registered = await case.run(case.message("cluster.register"))
        assert registered.status == "succeeded"
        cluster = (await case.clusters())[0]
        assert cluster.status == ClusterStatus.REGISTERING
        assert await case.targets(cluster.id) == []
        await case.run(
            case.message(
                "execution_target.register",
                cluster_id=str(cluster.id),
                name="default",
                backend_type="vanilla_k8s",
                endpoint=cluster.endpoint,
                placement={"type": "kubernetes", "namespace": "nodes"},
                is_default=True,
            )
        )
        assert (await case.clusters())[0].status == ClusterStatus.ACTIVE
        provisioned = await case.run(case.message())
        assert provisioned.status == "succeeded"
        assert len(await case.targets(provisioned.resource_id)) == 1


async def test_latest_revision_supersedes_old_before_handling_and_conflicts_are_rejected():
    async with resource_case() as case:
        key = uuid4()
        old, new = case.message(key=key), case.message(key=key, revision=2, name="new")
        await case.receive(old)
        await case.receive(new)
        await case.worker.run_once()
        assert (await case.read(old)).status == "superseded"
        assert (await case.read(new)).status == "succeeded"
        assert [c.name for c in await case.clusters()] == ["new"]
        with pytest.raises(ProtocolConflict):
            await case.receive(case.message(key=key, revision=2, name="conflict"))
        identical = await case.run(case.message(key=key, revision=2, name="new"))
        assert identical.resource_id == (await case.read(new)).resource_id
        assert len(await case.clusters()) == 1


async def test_same_name_different_owners_and_cross_owner_target_reference():
    async with resource_case() as case:
        first = await case.run(case.message())
        other = await case.run(case.message(client="other"))
        assert first.resource_id != other.resource_id
        rejected = await case.run(
            case.message(
                "execution_target.register",
                client="other",
                cluster_id=str(first.resource_id),
                name="bad",
                backend_type="vanilla_k8s",
                endpoint="https://cluster.test",
                placement={"type": "kubernetes", "namespace": "nodes"},
            )
        )
        assert rejected.status == "failed"
        assert rejected.error_code == "cluster_not_owned_by_client"


async def test_default_target_protected_and_provision_updates_full_configuration():
    async with resource_case() as case:
        key = uuid4()
        registered = await case.run(case.message(key=key))
        target = (await case.targets(registered.resource_id))[0]
        # Provision owns the default. Direct commands cannot take it over by name/ID.
        result = await case.run(
            case.message(
                "execution_target.register",
                cluster_id=str(registered.resource_id),
                name="default",
                backend_type="vanilla_k8s",
                endpoint="https://cluster.test",
                placement={"type": "kubernetes", "namespace": "nodes"},
                is_default=True,
            )
        )
        assert result.status == "failed"
        updated = await case.run(
            case.message(
                key=key,
                revision=2,
                credential="rotated",
                labels={"new": "label"},
                project_ids=[],
                ca_certificate=None,
                default_target={
                    "name": "renamed",
                    "backend_type": "vanilla_k8s",
                    "credential": "target-secret",
                    "endpoint": "https://new.test",
                    "placement": {"type": "kubernetes", "namespace": "new"},
                },
            )
        )
        assert updated.resource_id == registered.resource_id
        stored = (await case.clusters())[0]
        assert stored.project_ids == []
        assert stored.labels == {"new": "label"}
        assert stored.api_key == "rotated"
        revised = (await case.targets(stored.id))[0]
        assert revised.id == target.id
        assert revised.name == "renamed"
        assert revised.placement.namespace == "new"
        assert revised.api_key == "target-secret"


async def test_delete_waits_for_drain_and_tombstone_blocks_old_upsert():
    from execution_plane.cluster.cluster_store import ClusterStore
    from execution_plane.execution_target.execution_target_store import ExecutionTargetStore

    async with resource_case() as case:
        key = uuid4()
        created = await case.run(case.message(key=key))
        deletion = case.message("cluster.delete", key=key, revision=2)
        result = await case.run(deletion)
        assert result.status == "running"
        await case.observe()
        assert (await case.read(deletion)).status == "running"
        async with case.db.sessions.begin() as session:
            for target in await case.targets(created.resource_id):
                await ExecutionTargetStore.from_transaction(session).finalize_delete(target.id)
            await ClusterStore.from_transaction(session).finalize_delete(created.resource_id)
        await case.observe()
        assert (await case.read(deletion)).status == "succeeded"
        old = await case.run(case.message(key=key))
        assert old.status == "superseded"
        assert await case.clusters() == []
        recreated = await case.run(case.message(key=key, revision=3))
        assert recreated.resource_id != created.resource_id


async def test_direct_update_and_delete_of_registered_default_fail():
    async with resource_case() as case:
        cluster = await case.run(case.message("cluster.register"))
        key = uuid4()
        await case.run(
            case.message(
                "execution_target.register",
                key=key,
                cluster_id=str(cluster.resource_id),
                name="default",
                backend_type="vanilla_k8s",
                endpoint="https://cluster.test",
                placement={"type": "kubernetes", "namespace": "nodes"},
                is_default=True,
            )
        )
        assert (
            await case.run(case.message("execution_target.update", key=key, revision=2, name="bad"))
        ).status == "failed"
        assert (await case.run(case.message("execution_target.delete", key=key, revision=3))).status == "failed"


async def test_concurrent_receipts_and_handlers_converge_highest_revision():
    import asyncio

    async with resource_case() as case:
        key = uuid4()
        messages = [case.message(key=key, revision=revision, name=f"cluster-{revision}") for revision in range(1, 5)]
        await asyncio.gather(*(case.receive(message) for message in reversed(messages)))
        await case.worker.run_once()
        assert [cluster.name for cluster in await case.clusters()] == ["cluster-4"]
        assert (await case.read(messages[-1])).status == "succeeded"
        assert all([(await case.read(message)).status == "superseded" for message in messages[:-1]])


async def test_project_grant_cannot_create_cluster_for_other_projects():
    from execution_plane_client.protocol import CommandScope, MessageEnvelope

    async with resource_case() as case:
        message = case.message()
        values = message.model_dump()
        values["scope"] = CommandScope(client_id="ao", project_id=uuid4(), all_projects=False)
        rejected = await case.run(MessageEnvelope.model_validate(values))
        assert rejected.status == "failed"
        assert rejected.error_code == "project_scope_mismatch"
        assert await case.clusters() == []


async def test_later_enable_revision_reactivates_default_after_delete_started():
    from execution_plane.models.execution_target import TargetStatus

    async with resource_case() as case:
        key = uuid4()
        cluster = await case.run(case.message(key=key))
        await case.run(case.message("cluster.delete", key=key, revision=2))
        update = await case.run(case.message("cluster.update", key=key, revision=3, enabled=True))
        assert update.status == "succeeded"
        stored = (await case.clusters())[0]
        assert stored.status == ClusterStatus.ACTIVE
        assert stored.enabled
        assert (await case.targets(cluster.resource_id))[0].status == TargetStatus.ACTIVE


async def test_provision_recovers_existing_logical_cluster_without_name_adoption():
    from execution_plane.cluster.cluster_store import ClusterStore

    async with resource_case() as case:
        key = uuid4()
        async with case.db.sessions.begin() as session:
            existing = await ClusterStore.from_transaction(session).create(
                "partial", "https://cluster.test", "private", uuid4(), source_client_id="ao", source_integration_id=key
            )
        result = await case.run(case.message(key=key))
        assert result.resource_id == existing.id
        assert len(await case.clusters()) == 1
        assert len(await case.targets(existing.id)) == 1
