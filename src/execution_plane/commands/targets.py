"""Execution-target commands use the existing target lifecycle."""

from uuid import UUID

from pydantic import TypeAdapter
from sqlalchemy.ext.asyncio import async_sessionmaker

from execution_plane.commands.clusters import ResourceNotFoundError
from execution_plane.commands.context import EPCommandContext
from execution_plane.execution_target.execution_target_store import DefaultExecutionTargetError, ExecutionTargetStore
from execution_plane.models.cluster import Cluster
from execution_plane.models.execution_target import BackendType, ExecutionTarget
from execution_plane.models.execution_target_placement import ExecutionTargetPlacement
from execution_plane_client.commands.targets import TargetRegister, TargetUpdate
from execution_plane_client.errors import ProtocolConflict
from execution_plane_client.handlers import HandlingResult
from execution_plane_client.protocol import MessageEnvelope, ResourceReference

SYSTEM_ACTOR = UUID(int=0)


class TargetCommandHandler:
    """Authorize ownership before applying target effects in one transaction."""

    async def handle(self, message: MessageEnvelope, context: EPCommandContext) -> HandlingResult:  # noqa: C901
        """Create, patch or drain one caller-identified execution target."""
        resource_id = UUID(str(message.command.payload["resource_id"]))
        sessions = async_sessionmaker(context.engine, expire_on_commit=False)
        async with sessions.begin() as session:
            store = ExecutionTargetStore.from_transaction(session)
            kind = message.command.command_type
            if kind == "execution_target.register":
                payload = TargetRegister.model_validate(message.command.payload)
                cluster = await session.get(Cluster, payload.cluster_id, with_for_update=True)
                if cluster is None or cluster.source_client_id != context.client_id:
                    raise ResourceNotFoundError
                if payload.is_default:
                    raise DefaultExecutionTargetError
                if await session.get(ExecutionTarget, resource_id) is not None:
                    raise ProtocolConflict
                await store.create(
                    cluster_id=payload.cluster_id,
                    name=payload.name,
                    backend_type=BackendType(payload.backend_type),
                    endpoint=payload.endpoint,
                    api_key=payload.credential,
                    is_default=False,
                    created_by=SYSTEM_ACTOR,
                    placement=TypeAdapter(ExecutionTargetPlacement).validate_python(payload.placement.model_dump()),
                    labels=payload.labels,
                    target_id=resource_id,
                )
                await store.activate(resource_id, SYSTEM_ACTOR)
            else:
                target = await session.get(ExecutionTarget, resource_id, with_for_update=True)
                if target is None:
                    raise ResourceNotFoundError
                cluster = await session.get(Cluster, target.cluster_id)
                if cluster is None or cluster.source_client_id != context.client_id:
                    raise ResourceNotFoundError
                if target.is_default:
                    raise DefaultExecutionTargetError
                if kind == "execution_target.delete":
                    await store.request_delete(resource_id, SYSTEM_ACTOR)
                else:
                    update = TargetUpdate.model_validate(message.command.payload)
                    if any(getattr(update, key) is None for key in update.model_fields_set):
                        raise ProtocolConflict
                    if update.placement is not None and update.placement.type != target.placement.type:
                        raise ProtocolConflict
                    await store.update(
                        resource_id,
                        updated_by=SYSTEM_ACTOR,
                        name=update.name,
                        endpoint=update.endpoint,
                        api_key=update.credential,
                        labels=update.labels,
                        placement=None
                        if update.placement is None
                        else TypeAdapter(ExecutionTargetPlacement).validate_python(
                            update.placement.model_dump(exclude_unset=True)
                        ),
                    )
        return HandlingResult(ResourceReference(kind="execution_target", resource_id=resource_id))
