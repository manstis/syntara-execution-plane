"""Cluster-only registration and atomic cluster/default-target provisioning."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

from pydantic import TypeAdapter
from sqlalchemy.exc import IntegrityError

from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.commands.resources import (
    SYSTEM_ACTOR,
    current_resource,
    observe_drain,
    observed,
    reference,
)
from execution_plane.commands.targets import create_target
from execution_plane.execution_target.execution_target_registry import ExecutionTargetRegistry
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.models.cluster import ClusterStatus, ClusterType
from execution_plane.models.execution_target_placement import ExecutionTargetPlacement, ExecutionTargetPlacementTypes
from execution_plane_client.commands.clusters import ClusterProvision, ClusterRegister, ClusterUpdate
from execution_plane_client.errors import CommandExecutionError
from execution_plane_client.handlers import HandlingResult, ObservationHandler, TransactionalHandler
from execution_plane_client.protocol import CommandStatus

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane.models.cluster import Cluster
    from execution_plane_client.handlers import HandlerContext
    from execution_plane_client.protocol import MessageEnvelope
    from execution_plane_client.resources import ResourceIntent, ResourceIntentStore


async def converge_default(session: AsyncSession, cluster: Cluster, payload: ClusterProvision) -> None:
    """Compose existing target operations, preserving explicit configuration fields."""
    registry = ExecutionTargetRegistry(ExecutionTargetStore.from_transaction(session))
    targets = await registry.list(cluster_id=cluster.id)
    default = next((target for target in targets if target.is_default), None)
    desired = payload.default_target
    if default is None:
        await create_target(session, cluster.id, desired, is_default=True)
        return
    if default.backend_type.value != desired.backend_type:
        code = "immutable_target_backend"
        raise CommandExecutionError(code)
    placement: ExecutionTargetPlacementTypes = TypeAdapter(ExecutionTargetPlacement).validate_python(
        desired.placement.model_dump()
    )
    await registry.reactivate(
        default.id, updated_by=SYSTEM_ACTOR, endpoint=desired.endpoint, api_key=desired.credential, placement=placement
    )
    await registry.update(default.id, updated_by=SYSTEM_ACTOR, name=desired.name, labels=desired.labels)
    await ClusterStore.from_transaction(session).record_discovery_state(
        cluster.id, ClusterStatus.ACTIVE, None, SYSTEM_ACTOR
    )


class ClusterCommandHandler(TransactionalHandler, ObservationHandler):
    """Converge a logical resource under its revision lock without network calls."""

    def __init__(self, intents: ResourceIntentStore) -> None:
        """Use the selected runtime's resource revision store."""
        self._intents = intents

    async def observe(
        self, message: MessageEnvelope, _context: HandlerContext, session: AsyncSession
    ) -> HandlingResult:
        """Observe a requested drain without repeating its initiating mutation."""
        return await observe_drain(session, message, self._intents)

    async def handle(self, message: MessageEnvelope, _context: HandlerContext, session: AsyncSession) -> HandlingResult:
        """Register, provision, update or begin draining the reserved cluster."""
        if not message.scope.all_projects:
            project_grants = message.command.payload.get("project_ids")
            if (
                message.command.command_type in {"cluster.register", "cluster.provision"}
                or "project_ids" in message.command.payload
            ) and project_grants != [str(message.scope.project_id)]:
                code = "project_scope_mismatch"
                raise CommandExecutionError(code)
        row, completed = await current_resource(session, message, self._intents)
        if completed is not None:
            return completed
        store = ClusterStore.from_transaction(session)
        cluster = (
            await store.get(row.resource_id)
            if row.resource_id is not None
            else await store.get_by_source(row.client_id, UUID(str(row.resource_key)))
        )
        kind = message.command.command_type
        if kind == "cluster.delete":
            row = await self._intents.observe_resource(
                session,
                message,
                resource_id=cluster.id if cluster else row.resource_id,
                observed_revision=row.desired_revision if cluster is None else None,
                deleted=True,
            )
            if cluster is None:
                return HandlingResult(status=CommandStatus.SUCCEEDED, resource=reference(row), completion_required=True)
            await store.request_delete(cluster.id, SYSTEM_ACTOR)
            return HandlingResult(status=CommandStatus.RUNNING, resource=reference(row), observe_after_seconds=1)
        try:
            cluster, payload = await self._configure(session, message, row, store, cluster)
        except IntegrityError as exc:
            code = "cluster_configuration_conflict"
            raise CommandExecutionError(code) from exc
        if payload.enabled is False:
            row = await self._intents.observe_resource(session, message, resource_id=cluster.id, deleted=True)
            await store.request_delete(cluster.id, SYSTEM_ACTOR)
            return HandlingResult(status=CommandStatus.RUNNING, resource=reference(row), observe_after_seconds=1)
        return await observed(session, self._intents, message, row, cluster.id)

    async def _configure(
        self,
        session: AsyncSession,
        message: MessageEnvelope,
        row: ResourceIntent,
        store: ClusterStore,
        cluster: Cluster | None,
    ) -> tuple[Cluster, ClusterRegister | ClusterUpdate]:
        """Apply explicit configuration while preserving missing versus null fields."""
        kind = message.command.command_type
        payload: ClusterRegister | ClusterUpdate
        if kind in {"cluster.register", "cluster.provision"}:
            payload = (ClusterProvision if kind == "cluster.provision" else ClusterRegister).model_validate(
                message.command.payload
            )
            if cluster is None:
                cluster = await store.create(
                    payload.name,
                    payload.endpoint,
                    payload.credential,
                    SYSTEM_ACTOR,
                    payload.labels,
                    cluster_type=ClusterType(payload.cluster_type),
                    source_client_id=row.client_id,
                    source_integration_id=UUID(str(row.resource_key)),
                    source_revision=payload.desired_revision,
                    project_ids=payload.project_ids,
                    ca_certificate=payload.ca_certificate,
                )
            elif cluster.cluster_type.value != payload.cluster_type:
                code = "immutable_cluster_type"
                raise CommandExecutionError(code)
            if cluster.status == ClusterStatus.DRAINING or not cluster.enabled:
                cluster = await store.reactivate(cluster.id, updated_by=SYSTEM_ACTOR, status=ClusterStatus.REGISTERING)
            cluster = await store.update(
                cluster.id,
                updated_by=SYSTEM_ACTOR,
                name=payload.name,
                endpoint=payload.endpoint,
                api_key=payload.credential,
                ca_certificate=payload.ca_certificate,
                clear_ca_certificate=payload.ca_certificate is None,
                labels=payload.labels,
                project_ids=payload.project_ids,
                set_project_ids=True,
                source_revision=payload.desired_revision,
            )
            if isinstance(payload, ClusterProvision):
                await converge_default(session, cluster, payload)
        else:
            if cluster is None:
                code = "resource_not_found"
                raise CommandExecutionError(code)
            payload = ClusterUpdate.model_validate(message.command.payload)
            if any(
                value is None
                for key, value in message.command.payload.items()
                if key in {"name", "endpoint", "credential", "labels", "enabled"}
            ):
                code = "null_required_cluster_field"
                raise CommandExecutionError(code)
            cluster = await store.update(
                cluster.id,
                updated_by=SYSTEM_ACTOR,
                name=payload.name,
                endpoint=payload.endpoint,
                api_key=payload.credential,
                ca_certificate=payload.ca_certificate,
                clear_ca_certificate="ca_certificate" in payload.model_fields_set and payload.ca_certificate is None,
                labels=payload.labels,
                project_ids=payload.project_ids,
                set_project_ids="project_ids" in payload.model_fields_set,
                source_revision=payload.desired_revision,
            )
        if isinstance(payload, ClusterUpdate) and payload.enabled is True and not cluster.enabled:
            cluster = await store.reactivate(cluster.id, updated_by=SYSTEM_ACTOR, status=ClusterStatus.REGISTERING)
            registry = ExecutionTargetRegistry(ExecutionTargetStore.from_transaction(session))
            default = next((target for target in await registry.list(cluster_id=cluster.id) if target.is_default), None)
            if default is not None:
                await registry.reactivate(default.id, updated_by=SYSTEM_ACTOR, placement=default.placement)
                await store.record_discovery_state(cluster.id, ClusterStatus.ACTIVE, None, SYSTEM_ACTOR)
        return cluster, payload
