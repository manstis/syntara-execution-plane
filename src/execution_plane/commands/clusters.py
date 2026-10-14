"""Cluster commands write desired state for the existing binding reconciler."""

import hashlib
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from execution_plane.commands.context import EPCommandContext
from execution_plane.models.cluster_binding import ClusterBinding
from execution_plane_client.commands import ResourceRevision
from execution_plane_client.commands.clusters import ClusterRegister, ClusterUpdate
from execution_plane_client.errors import ProtocolConflict
from execution_plane_client.handlers import HandlingResult
from execution_plane_client.protocol import MessageEnvelope, ResourceReference

_CONFIG_FIELDS = ("name", "endpoint", "namespace", "credential", "ca_certificate", "project_ids", "labels", "enabled")


class ResourceNotFoundError(LookupError):
    """A resource is absent or belongs to another client."""


class ClusterCommandHandler:
    """Retain resource revisions, never command identities or envelopes."""

    async def handle(self, message: MessageEnvelope, context: EPCommandContext) -> HandlingResult:
        """Apply the former router's version and identity checks atomically."""
        identity = ResourceRevision.model_validate(
            {key: message.command.payload[key] for key in ("resource_key", "desired_revision")}
        )
        sessions = async_sessionmaker(context.engine, expire_on_commit=False)
        async with sessions.begin() as session:
            # Lock the domain identity even when its binding does not yet exist.
            digest = hashlib.sha256(f"binding:{context.client_id}:{identity.resource_key}".encode()).digest()
            await session.execute(select(func.pg_advisory_xact_lock(int.from_bytes(digest[:8], signed=True))))
            binding = await session.get(
                ClusterBinding, (context.client_id, identity.resource_key), with_for_update=True
            )
            kind = message.command.command_type
            if kind in {"cluster.update", "cluster.delete"}:
                if binding is None:
                    raise ResourceNotFoundError
                values = {field: getattr(binding, field) for field in _CONFIG_FIELDS}
                if kind == "cluster.update":
                    patch = ClusterUpdate.model_validate(message.command.payload)
                    values.update(patch.model_dump(exclude_unset=True, exclude={"resource_key", "desired_revision"}))
                else:
                    values["enabled"] = False
                desired = ClusterRegister(**identity.model_dump(), **values)
            else:
                desired = ClusterRegister.model_validate(message.command.payload)
            if binding is not None and desired.desired_revision < binding.revision:
                raise ProtocolConflict
            if binding is not None and desired.desired_revision == binding.revision:
                if any(getattr(binding, key) != getattr(desired, key) for key in _CONFIG_FIELDS):
                    raise ProtocolConflict
            else:
                now = datetime.now(UTC)
                if binding is None:
                    binding = ClusterBinding(
                        client_id=context.client_id,
                        source_integration_id=identity.resource_key,
                        created_at=now,
                        updated_at=now,
                        revision=desired.desired_revision,
                        **{key: getattr(desired, key) for key in _CONFIG_FIELDS},
                    )
                    session.add(binding)
                else:
                    for key in _CONFIG_FIELDS:
                        setattr(binding, key, getattr(desired, key))
                    binding.revision = desired.desired_revision
                    binding.status = "pending"
                    binding.status_message = None
                    binding.updated_at = now
        return HandlingResult(
            ResourceReference(
                kind="cluster_binding", resource_id=identity.resource_key, revision=identity.desired_revision
            )
        )
