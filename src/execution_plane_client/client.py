"""Typed enqueue and recovery methods, with explicit transaction ownership."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import TypeAdapter, ValidationError

from execution_plane_client.commands import CommandRoute, clusters, control, targets, work
from execution_plane_client.destinations import DestinationPolicy
from execution_plane_client.errors import ProtocolConflict, RetryableDelivery, UnauthorizedPeer
from execution_plane_client.resources import ClusterRead, ExecutionTargetRead, WorkItemRead

_MAX_CHILD_PAGE = 200

if TYPE_CHECKING:
    from collections.abc import Mapping
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.commands import CommandIdentity, ResourceRevision
    from execution_plane_client.handlers import CommandRegistry
    from execution_plane_client.journal.records import JournalEntry
    from execution_plane_client.journal.store import ExchangeJournalStore
    from execution_plane_client.protocol import ChildPage, CommandRead, CommandScope, ExecutionPolicy, MessageEnvelope
    from execution_plane_client.security import PeerConfig
    from execution_plane_client.transport import HttpCommandTransport


class MediatorClient:
    """Enqueue without committing or HTTP; queries are explicit separate operations."""

    def __init__(
        self,
        store: ExchangeJournalStore,
        catalog: CommandCatalog,
        registry: CommandRegistry,
        transport: HttpCommandTransport,
        *,
        local_id: str,
        local_role: str,
        command_url: str,
        peers: Mapping[str, PeerConfig],
        default_peer_id: str | None = None,
    ) -> None:
        """Inject participant persistence, peer policy and network configuration."""
        self._store = store
        self._catalog = catalog
        self._registry = registry
        self._transport = transport
        self._local_id = local_id
        self._local_role = local_role
        self._command_url = command_url
        self._peers = dict(peers)
        self._policy = DestinationPolicy(peers)
        remote_ids = [peer_id for peer_id in peers if peer_id != local_id]
        self._default_peer_id = default_peer_id or (remote_ids[0] if len(remote_ids) == 1 else None)

    def route(self, scope: CommandScope, *, peer_id: str | None = None) -> CommandRoute:
        """Build explicit routes from the same registered identities used by transport."""
        destination = self._peer(peer_id)
        return CommandRoute(
            source_id=self._local_id,
            destination_id=destination,
            scope=scope,
            target_url=self._peers[destination].command_url,
            callback_url=self._command_url,
        )

    async def submit(self, message: MessageEnvelope, session: AsyncSession) -> JournalEntry:
        """Validate and snapshot intent in the caller-owned domain transaction."""
        self._catalog.validate(message)
        definition = self._catalog.definition(message.command.command_type, message.command.command_revision)
        destination = self._policy.validate(message.command.target_url, message.destination_id)
        if (
            message.source_id != self._local_id
            or message.command.callback_url != self._command_url
            or self._local_role not in definition.source_roles
            or self._peers[destination.peer_id].role not in definition.destination_roles
        ):
            raise UnauthorizedPeer
        if definition.completion_type is not None:
            self._registry.get(definition.completion_type, definition.revision)
        return await self._store.append_outbound(session, message)

    async def cancel(self, message: MessageEnvelope, session: AsyncSession) -> JournalEntry:
        """Enqueue a cancellation through the same journal, never a mutation RPC."""
        if message.command.command_type != "command.cancel":
            raise ProtocolConflict
        return await self.submit(message, session)

    def _peer(self, peer_id: str | None) -> str:
        selected = peer_id or self._default_peer_id
        if selected is None or selected not in self._peers:
            raise UnauthorizedPeer
        return selected

    async def get(self, scope: CommandScope, command_id: UUID, *, peer_id: str | None = None) -> CommandRead:
        """Query peer status outside the caller's domain transaction."""
        return await self._transport.get_command(self._peer(peer_id), scope, command_id)

    async def list_child_commands(
        self,
        scope: CommandScope,
        command_id: UUID,
        after: int = 0,
        limit: int = 50,
        *,
        peer_id: str | None = None,
    ) -> ChildPage:
        """Recover bounded original children; pages may be smaller than the requested limit."""
        if not 1 <= limit <= _MAX_CHILD_PAGE or after < 0:
            error = "Invalid child page bounds"
            raise ValueError(error)
        return await self._transport.get_children(self._peer(peer_id), scope, command_id, after)

    async def get_work_item(
        self, scope: CommandScope, resource_id: UUID, *, peer_id: str | None = None
    ) -> WorkItemRead:
        """Read one authorized work item through the shared transport."""
        data = await self._transport.get_resource(self._peer(peer_id), scope, "work-items", resource_id=resource_id)
        try:
            return WorkItemRead.model_validate(data)
        except ValidationError:
            raise RetryableDelivery from None

    async def list_work_items(
        self, scope: CommandScope, *, limit: int = 50, peer_id: str | None = None
    ) -> list[WorkItemRead]:
        """Read a bounded page of authorized work items."""
        data = await self._transport.get_resource(self._peer(peer_id), scope, "work-items", limit=limit)
        try:
            return TypeAdapter(list[WorkItemRead]).validate_python(data)
        except ValidationError:
            raise RetryableDelivery from None

    async def get_cluster(self, scope: CommandScope, resource_id: UUID, *, peer_id: str | None = None) -> ClusterRead:
        """Read one authorized cluster through the shared transport."""
        data = await self._transport.get_resource(self._peer(peer_id), scope, "clusters", resource_id=resource_id)
        try:
            return ClusterRead.model_validate(data)
        except ValidationError:
            raise RetryableDelivery from None

    async def list_clusters(
        self, scope: CommandScope, *, limit: int = 50, peer_id: str | None = None
    ) -> list[ClusterRead]:
        """Read a bounded page of authorized clusters."""
        data = await self._transport.get_resource(self._peer(peer_id), scope, "clusters", limit=limit)
        try:
            return TypeAdapter(list[ClusterRead]).validate_python(data)
        except ValidationError:
            raise RetryableDelivery from None

    async def get_execution_target(
        self, scope: CommandScope, resource_id: UUID, *, peer_id: str | None = None
    ) -> ExecutionTargetRead:
        """Read one authorized execution target through the shared transport."""
        data = await self._transport.get_resource(
            self._peer(peer_id), scope, "execution-targets", resource_id=resource_id
        )
        try:
            return ExecutionTargetRead.model_validate(data)
        except ValidationError:
            raise RetryableDelivery from None

    async def list_execution_targets(
        self, scope: CommandScope, *, limit: int = 50, peer_id: str | None = None
    ) -> list[ExecutionTargetRead]:
        """Read a bounded page of authorized execution targets."""
        data = await self._transport.get_resource(self._peer(peer_id), scope, "execution-targets", limit=limit)
        try:
            return TypeAdapter(list[ExecutionTargetRead]).validate_python(data)
        except ValidationError:
            raise RetryableDelivery from None

    async def work_execute(
        self,
        session: AsyncSession,
        *,
        identity: CommandIdentity,
        route: CommandRoute,
        payload: work.WorkExecute,
        execution_policy: ExecutionPolicy | None = None,
    ) -> JournalEntry:
        """Enqueue typed script execution."""
        return await self.submit(
            work.work_execute(identity=identity, route=route, payload=payload, execution_policy=execution_policy),
            session,
        )

    async def cluster_register(
        self,
        session: AsyncSession,
        *,
        identity: CommandIdentity,
        route: CommandRoute,
        payload: clusters.ClusterRegister,
    ) -> JournalEntry:
        """Enqueue cluster-only registration."""
        return await self.submit(clusters.cluster_register(identity=identity, route=route, payload=payload), session)

    async def cluster_provision(
        self,
        session: AsyncSession,
        *,
        identity: CommandIdentity,
        route: CommandRoute,
        payload: clusters.ClusterProvision,
    ) -> JournalEntry:
        """Enqueue cluster and default-target convergence."""
        return await self.submit(clusters.cluster_provision(identity=identity, route=route, payload=payload), session)

    async def cluster_update(
        self,
        session: AsyncSession,
        *,
        identity: CommandIdentity,
        route: CommandRoute,
        payload: clusters.ClusterUpdate,
    ) -> JournalEntry:
        """Enqueue a revisioned cluster change."""
        return await self.submit(clusters.cluster_update(identity=identity, route=route, payload=payload), session)

    async def cluster_delete(
        self,
        session: AsyncSession,
        *,
        identity: CommandIdentity,
        route: CommandRoute,
        payload: ResourceRevision,
    ) -> JournalEntry:
        """Enqueue cluster draining/deletion."""
        return await self.submit(clusters.cluster_delete(identity=identity, route=route, payload=payload), session)

    async def target_register(
        self,
        session: AsyncSession,
        *,
        identity: CommandIdentity,
        route: CommandRoute,
        payload: targets.TargetRegister,
    ) -> JournalEntry:
        """Enqueue independent target registration."""
        return await self.submit(targets.target_register(identity=identity, route=route, payload=payload), session)

    async def target_update(
        self,
        session: AsyncSession,
        *,
        identity: CommandIdentity,
        route: CommandRoute,
        payload: targets.TargetUpdate,
    ) -> JournalEntry:
        """Enqueue a revisioned target change."""
        return await self.submit(targets.target_update(identity=identity, route=route, payload=payload), session)

    async def target_delete(
        self,
        session: AsyncSession,
        *,
        identity: CommandIdentity,
        route: CommandRoute,
        payload: ResourceRevision,
    ) -> JournalEntry:
        """Enqueue target draining/deletion."""
        return await self.submit(targets.target_delete(identity=identity, route=route, payload=payload), session)

    async def cancel_command(
        self,
        session: AsyncSession,
        *,
        identity: CommandIdentity,
        route: CommandRoute,
        payload: control.CancelCommand,
    ) -> JournalEntry:
        """Enqueue durable cancellation intent."""
        return await self.submit(control.cancel_command(identity=identity, route=route, payload=payload), session)
