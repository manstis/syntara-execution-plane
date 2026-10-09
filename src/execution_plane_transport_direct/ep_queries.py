"""Compose direct EP command queries from in-memory and provider work state."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast
from uuid import UUID

from sqlalchemy import select
from sqlmodel import col

from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane_client.errors import CommandNotFound, UnauthorizedPeer
from execution_plane_client.protocol import (
    ChildCommandRead,
    ChildPage,
    CommandRead,
    CommandScope,
    CommandStatus,
    ResourceReference,
)
from execution_plane_client.queries import CommandQueryProvider, CommandStateProvider
from execution_plane_transport_direct.queries import InMemoryCommandQueryProvider
from execution_plane_transport_direct.work_completion import completion_for_work_item

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane.models.work_item import WorkItem as WorkItemModel
    from execution_plane_client.handlers import HandlingResult
    from execution_plane_client.protocol import MessageEnvelope


_STATUS = {
    WorkItemStatus.PENDING: CommandStatus.RUNNING,
    WorkItemStatus.CLAIMED: CommandStatus.RUNNING,
    WorkItemStatus.DISPATCHED: CommandStatus.RUNNING,
    WorkItemStatus.CANCEL_REQUESTED: CommandStatus.CANCEL_REQUESTED,
    WorkItemStatus.RECONCILIATION_REQUIRED: CommandStatus.OUTCOME_UNKNOWN,
    WorkItemStatus.COMPLETED: CommandStatus.SUCCEEDED,
    WorkItemStatus.FAILED: CommandStatus.FAILED,
    WorkItemStatus.CANCELLED: CommandStatus.CANCELLED,
}


class EPDirectCommandQueryProvider(CommandStateProvider):
    """Implement direct command state with bounded receipts and authoritative WorkItems."""

    def __init__(
        self,
        sessions: Callable[[], AsyncSession],
        *,
        memory: InMemoryCommandQueryProvider | None = None,
        fallback: CommandQueryProvider | None = None,
    ) -> None:
        """Use bounded protocol memory and the domain database's work-item table."""
        self._sessions = sessions
        self._memory = memory or InMemoryCommandQueryProvider()
        self._fallback = fallback

    def record_received(self, message: MessageEnvelope, *, local_id: str) -> bool:
        """Keep direct ingress deduplication and ownership in bounded process memory."""
        return self._memory.record_received(message, local_id=local_id)

    def has_received(self, message: MessageEnvelope) -> bool:
        """Check the direct receiver's bounded process-local receipt projection."""
        return self._memory.has_received(message)

    def mark_status(
        self,
        scope: CommandScope,
        command_id: UUID,
        status: CommandStatus,
        *,
        resource: ResourceReference | None = None,
        error_code: str | None = None,
    ) -> None:
        """Update non-work command projections in the process-local provider."""
        self._memory.mark_status(scope, command_id, status, resource=resource, error_code=error_code)

    def append_child(self, original: MessageEnvelope, child: MessageEnvelope, result: HandlingResult) -> None:
        """Retain callbacks locally for direct-mode duplicate suppression and reads."""
        self._memory.append_child(original, child, result)

    async def is_cancelled(
        self,
        scope: CommandScope,
        command_id: UUID,
        *,
        session: AsyncSession | None = None,
    ) -> bool:
        """Use the process-local direct transport's accepted cancellation state."""
        return await self._memory.is_cancelled(scope, command_id, session=session)

    async def get(self, scope: CommandScope, command_id: UUID, *, peer_id: str) -> CommandRead:
        """Prefer a current work-item lifecycle status over stale process-local status."""
        item = await self._work_item(scope, command_id)
        if item is not None:
            self._authorize_work_item(item, peer_id)
            return self._read(item)
        try:
            return await self._memory.get(scope, command_id, peer_id=peer_id)
        except CommandNotFound:
            if self._fallback is None:
                raise
            return await self._fallback.get(scope, command_id, peer_id=peer_id)

    async def children(
        self,
        scope: CommandScope,
        command_id: UUID,
        after: int = 0,
        limit: int = 50,
        *,
        peer_id: str,
        include_recovery: bool = False,
    ) -> ChildPage:
        """Recover the terminal work callback from its encrypted route context."""
        try:
            stored = await self._memory.children(
                scope,
                command_id,
                after,
                limit,
                peer_id=peer_id,
                include_recovery=include_recovery,
            )
        except CommandNotFound:
            stored = ChildPage(items=[])
        if stored.items:
            return stored
        item = await self._work_item(scope, command_id)
        if item is None:
            if self._fallback is not None:
                return await self._fallback.children(
                    scope,
                    command_id,
                    after,
                    limit,
                    peer_id=peer_id,
                    include_recovery=include_recovery,
                )
            return stored
        self._authorize_work_item(item, peer_id)
        completion = completion_for_work_item(item)
        if completion is None:
            return stored
        original, child, result = completion
        if include_recovery and original.source_id != peer_id:
            raise UnauthorizedPeer
        child_status = result.status or CommandStatus.SUCCEEDED
        if after >= 1:
            return ChildPage(items=[])
        return ChildPage(
            items=[
                ChildCommandRead(
                    command_id=child.command_id,
                    command_type=child.command.command_type,
                    status=child_status,
                    parent_command_id=command_id,
                    latest_child_sequence=1,
                    delivery_state=None,
                    handling_state="handled",
                    error_code=result.error_code,
                    created_at=child.command.occurred_at,
                    handled_at=item.completed_at,
                    parent_sequence=1,
                    recovery_message=child if include_recovery else None,
                )
            ]
        )

    async def _work_item(self, scope: CommandScope, command_id: UUID) -> WorkItemModel | None:
        """Load a work row only from the authenticated caller/project scope."""
        if scope.project_id is None:
            return None
        async with self._sessions() as session:
            return cast(
                "WorkItemModel | None",
                await session.scalar(
                    select(WorkItem).where(
                        col(WorkItem.client_id) == scope.client_id,
                        col(WorkItem.project_id) == scope.project_id,
                        col(WorkItem.request_id) == str(command_id),
                    ),
                ),
            )

    @staticmethod
    def _authorize_work_item(item: WorkItem, peer_id: str) -> None:
        """Limit WorkItem-backed projections to peers named by the signed command."""
        context = item.payload.get("_ep_completion_context")
        if not isinstance(context, dict) or peer_id not in {context.get("source_id"), context.get("destination_id")}:
            raise UnauthorizedPeer

    @staticmethod
    def _read(item: WorkItem) -> CommandRead:
        status = _STATUS[item.status]
        terminal = item.completed_at is not None
        return CommandRead(
            command_id=UUID(item.request_id),
            command_type="work.execute",
            status=status,
            resource=ResourceReference(kind="work_item", resource_id=item.id),
            latest_child_sequence=1 if terminal else 0,
            handling_state="handled" if terminal else "running",
            error_code="work_outcome_unknown" if status is CommandStatus.OUTCOME_UNKNOWN else None,
            reconciliation_required=status is CommandStatus.OUTCOME_UNKNOWN,
            created_at=item.created_at,
            handled_at=item.completed_at,
        )
