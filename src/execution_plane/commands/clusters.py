"""Cluster commands write desired state for the existing binding reconciler."""

import hashlib
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from execution_plane.commands.context import EPCommandContext
from execution_plane.models.cluster_binding import ClusterBinding
from execution_plane_client.commands.base import CommandPayload
from execution_plane_client.commands.clusters import ClusterDelete, ClusterRegister, ClusterUpdate
from execution_plane_client.errors import ProtocolConflict, UnsupportedCommand
from execution_plane_client.handlers import HandlingResult
from execution_plane_client.protocol import MessageEnvelope, ResourceReference

_CONFIG_FIELDS = ("name", "endpoint", "namespace", "credential", "ca_certificate", "project_ids", "labels", "enabled")


class ResourceNotFoundError(LookupError):
    """A resource is absent or belongs to another client."""


class ClusterCommandHandler:
    """Retain resource revisions, never command identities or envelopes."""

    async def handle(  # noqa: PLR0912 - Validate and apply binding revisions within one transaction.
        self, message: MessageEnvelope, payload: CommandPayload, context: EPCommandContext
    ) -> HandlingResult:
        """Apply the former router's version and identity checks atomically."""
        del message
        if not isinstance(payload, ClusterRegister | ClusterUpdate | ClusterDelete):
            raise UnsupportedCommand
        sessions = async_sessionmaker(context.engine, expire_on_commit=False)
        async with sessions.begin() as session:
            # Lock the domain identity even when its binding does not yet exist.
            digest = hashlib.sha256(f"binding:{context.client_id}:{payload.resource_key}".encode()).digest()
            await session.execute(select(func.pg_advisory_xact_lock(int.from_bytes(digest[:8], signed=True))))
            binding = await session.get(ClusterBinding, (context.client_id, payload.resource_key), with_for_update=True)
            if isinstance(payload, ClusterUpdate | ClusterDelete):
                if binding is None:
                    raise ResourceNotFoundError
                values = {field: getattr(binding, field) for field in _CONFIG_FIELDS}
                if isinstance(payload, ClusterUpdate):
                    values.update(payload.model_dump(exclude_unset=True, exclude={"resource_key", "desired_revision"}))
                else:
                    values["enabled"] = False
                # Validate the complete configuration after merging with persisted state.
                desired = ClusterRegister(
                    resource_key=payload.resource_key, desired_revision=payload.desired_revision, **values
                )
            else:
                desired = payload
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
                        source_integration_id=payload.resource_key,
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
                kind="cluster_binding", resource_id=payload.resource_key, revision=payload.desired_revision
            )
        )
