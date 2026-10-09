"""Execution-target commands preserve cluster ownership and protected defaults."""

from __future__ import annotations

from typing import TYPE_CHECKING

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
from execution_plane.execution_target.execution_target_registry import ExecutionTargetRegistry
from execution_plane.execution_target.execution_target_store import (
    ClusterNotAvailableError,
    DefaultExecutionTargetError,
    ExecutionTargetStore,
)
from execution_plane.models.cluster import Cluster, ClusterStatus
from execution_plane.models.execution_target import BackendType, TargetStatus
from execution_plane.models.execution_target_placement import ExecutionTargetPlacement
from execution_plane_client.commands.targets import TargetDefinition, TargetRegister, TargetUpdate
from execution_plane_client.errors import CommandExecutionError
from execution_plane_client.handlers import HandlingResult, ObservationHandler, TransactionalHandler
from execution_plane_client.protocol import CommandStatus

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane.models.execution_target import ExecutionTarget
    from execution_plane_client.handlers import HandlerContext
    from execution_plane_client.protocol import MessageEnvelope
    from execution_plane_client.resources import ResourceIntentStore


async def create_target(
    session: AsyncSession, cluster_id: UUID, payload: TargetDefinition, *, is_default: bool
) -> ExecutionTarget:
    """Reuse the target registry's local create/activate lifecycle in one transaction."""
    registry = ExecutionTargetRegistry(ExecutionTargetStore.from_transaction(session))
    target = await registry.create(
        cluster_id=cluster_id,
        name=payload.name,
        backend_type=BackendType(payload.backend_type),
        endpoint=payload.endpoint,
        api_key=payload.credential,
        is_default=is_default,
        created_by=SYSTEM_ACTOR,
        placement=TypeAdapter(ExecutionTargetPlacement).validate_python(payload.placement.model_dump()),
        labels=payload.labels,
    )
    await registry.activate(target.id, SYSTEM_ACTOR)
    if is_default:
        await ClusterStore.from_transaction(session).record_discovery_state(
            cluster_id, ClusterStatus.ACTIVE, None, SYSTEM_ACTOR
        )
    return target


class TargetCommandHandler(TransactionalHandler, ObservationHandler):
    """Apply independent target intent only to the newest client-owned revision."""

    def __init__(self, intents: ResourceIntentStore) -> None:
        """Use the selected runtime's resource revision store."""
        self._intents = intents

    async def observe(
        self, message: MessageEnvelope, _context: HandlerContext, session: AsyncSession
    ) -> HandlingResult:
        """Observe a requested drain without repeating its initiating mutation."""
        return await observe_drain(session, message, self._intents)

    async def handle(self, message: MessageEnvelope, context: HandlerContext, session: AsyncSession) -> HandlingResult:
        """Apply local target mutations through participating stores and registries."""
        if message.command.command_type == "execution_target.register":
            payload = TargetRegister.model_validate(message.command.payload)
            cluster = await session.get(Cluster, payload.cluster_id)
            if cluster is None or cluster.source_client_id != context.scope.client_id:
                code = "cluster_not_owned_by_client"
                raise CommandExecutionError(code)
            if (
                not context.scope.all_projects
                and cluster.project_ids is not None
                and context.scope.project_id not in cluster.project_ids
            ):
                code = "project_scope_mismatch"
                raise CommandExecutionError(code)
        row, completed = await current_resource(session, message, self._intents)
        if completed is not None:
            return completed
        store = ExecutionTargetStore.from_transaction(session)
        registry = ExecutionTargetRegistry(store)
        target = await store.get(row.resource_id) if row.resource_id is not None else None
        kind = message.command.command_type
        if target is not None and target.is_default:
            code = "protected_default_target"
            raise CommandExecutionError(code)
        if kind == "execution_target.delete":
            row = await self._intents.observe_resource(
                session,
                message,
                resource_id=row.resource_id,
                observed_revision=row.desired_revision if target is None else None,
                deleted=True,
            )
            if target is None:
                return HandlingResult(status=CommandStatus.SUCCEEDED, resource=reference(row), completion_required=True)
            await registry.request_delete(target.id, SYSTEM_ACTOR)
            return HandlingResult(status=CommandStatus.RUNNING, resource=reference(row), observe_after_seconds=1)
        try:
            target = await self._configure(message, registry, target, session)
        except (DefaultExecutionTargetError, ClusterNotAvailableError, IntegrityError) as exc:
            code = "target_registration_conflict"
            raise CommandExecutionError(code) from exc
        return await observed(session, self._intents, message, row, target.id)

    async def _configure(
        self,
        message: MessageEnvelope,
        registry: ExecutionTargetRegistry,
        target: ExecutionTarget | None,
        session: AsyncSession,
    ) -> ExecutionTarget:
        """Validate identity and apply complete registration or partial update."""
        kind = message.command.command_type
        if kind == "execution_target.register":
            payload = TargetRegister.model_validate(message.command.payload)
            if target is None:
                target = await create_target(session, payload.cluster_id, payload, is_default=payload.is_default)
            else:
                if (
                    target.cluster_id != payload.cluster_id
                    or target.backend_type.value != payload.backend_type
                    or target.is_default != payload.is_default
                ):
                    code = "immutable_target_identity"
                    raise CommandExecutionError(code)
                target = await registry.update(
                    target.id,
                    updated_by=SYSTEM_ACTOR,
                    name=payload.name,
                    endpoint=payload.endpoint,
                    api_key=payload.credential,
                    labels=payload.labels,
                    placement=TypeAdapter(ExecutionTargetPlacement).validate_python(payload.placement.model_dump()),
                )
                if target.status == TargetStatus.DRAINING:
                    await registry.reactivate(target.id, updated_by=SYSTEM_ACTOR, placement=target.placement)
        else:
            if target is None:
                code = "resource_not_found"
                raise CommandExecutionError(code)
            update = TargetUpdate.model_validate(message.command.payload)
            if any(
                value is None
                for key, value in message.command.payload.items()
                if key not in {"resource_key", "desired_revision"}
            ):
                code = "null_required_target_field"
                raise CommandExecutionError(code)
            if update.placement is not None and update.placement.type != target.placement.type:
                code = "immutable_target_backend"
                raise CommandExecutionError(code)
            target = await registry.update(
                target.id,
                updated_by=SYSTEM_ACTOR,
                name=update.name,
                endpoint=update.endpoint,
                api_key=update.credential,
                labels=update.labels,
                placement=None
                if update.placement is None
                else TypeAdapter(ExecutionTargetPlacement).validate_python(update.placement.model_dump()),
            )
        return target
