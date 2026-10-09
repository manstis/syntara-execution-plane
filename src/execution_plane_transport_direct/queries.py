"""Bounded in-process query state for direct command runtimes."""

from __future__ import annotations

from collections import OrderedDict, defaultdict
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from execution_plane_client.errors import CommandNotFound, ProtocolConflict, UnauthorizedPeer
from execution_plane_client.protocol import ChildCommandRead, ChildPage, CommandRead, CommandStatus, ResourceReference
from execution_plane_client.queries import CommandStateProvider

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.handlers import HandlingResult
    from execution_plane_client.protocol import CommandScope, MessageEnvelope

_MAX_CHILD_PAGE = 200


class InMemoryCommandQueryProvider(CommandStateProvider):
    """Implement command state with bounded in-process accepted and terminal projections."""

    def __init__(self, *, max_commands: int = 10_000) -> None:
        """Create an in-process store with a finite retention bound."""
        if max_commands < 1:
            error = "max_commands must be positive"
            raise ValueError(error)
        self._max_commands = max_commands
        self._entries: OrderedDict[tuple[str, UUID], tuple[CommandRead, frozenset[str], CommandScope, str]] = (
            OrderedDict()
        )
        self._children: dict[tuple[str, UUID], list[ChildCommandRead]] = defaultdict(list)
        self._seen_messages: OrderedDict[tuple[str, UUID], UUID] = OrderedDict()
        self._cancellations: OrderedDict[tuple[str, UUID], tuple[CommandScope, UUID]] = OrderedDict()

    def _put(self, scope: CommandScope, read: CommandRead, owners: frozenset[str], source_id: str) -> None:
        key = (scope.client_id, read.command_id)
        self._entries[key] = (read, owners, scope, source_id)
        self._entries.move_to_end(key)
        while len(self._entries) > self._max_commands:
            old_key, _ = self._entries.popitem(last=False)
            self._children.pop(old_key, None)
            for child_key in tuple(self._children):
                has_orphaned_child = any(item.command_id == old_key[1] for item in self._children[child_key])
                if child_key[0] == old_key[0] and has_orphaned_child:
                    self._children.pop(child_key, None)
            for message_key, command_id in tuple(self._seen_messages.items()):
                if message_key[0] == old_key[0] and command_id == old_key[1]:
                    self._seen_messages.pop(message_key, None)
            self._cancellations.pop(old_key, None)
            for key, (_, cancellation_id) in tuple(self._cancellations.items()):
                if key[0] == old_key[0] and cancellation_id == old_key[1]:
                    self._cancellations.pop(key, None)

    def record_received(self, message: MessageEnvelope, *, local_id: str) -> bool:
        """Record a received identity once; return false for an exact duplicate."""
        message_key = (message.scope.client_id, message.message_id)
        previous_command = self._seen_messages.get(message_key)
        if previous_command is not None:
            if previous_command != message.command_id:
                raise ProtocolConflict
            return False
        command_key = (message.scope.client_id, message.command_id)
        existing = self._entries.get(command_key)
        if existing is not None:
            raise ProtocolConflict

        command = message.command
        status = CommandStatus.ACCEPTED
        handled_at = None
        if command.command_type.endswith(".completed"):
            status = CommandStatus(str(command.payload["status"]))
            handled_at = command.occurred_at
        read = CommandRead(
            command_id=message.command_id,
            command_type=command.command_type,
            status=status,
            parent_command_id=command.parent_command_id,
            delivery_state="delivered",
            handling_state="pending" if handled_at is None else "handled",
            created_at=command.occurred_at,
            handled_at=handled_at,
        )
        owners = frozenset({message.source_id, local_id})
        self._seen_messages[message_key] = message.command_id
        self._seen_messages.move_to_end(message_key)
        if command.command_type == "command.cancel":
            target_id = UUID(str(command.payload["target_command_id"]))
            cancellation_key = (message.scope.client_id, target_id)
            self._cancellations[cancellation_key] = (message.scope, message.command_id)
            self._cancellations.move_to_end(cancellation_key)
            while len(self._cancellations) > self._max_commands:
                self._cancellations.popitem(last=False)
        elif command.command_type == "work.execute":
            self._cancellations.pop((message.scope.client_id, message.command_id), None)
        self._put(message.scope, read, owners, message.source_id)

        if command.parent_command_id is not None and command.parent_sequence is not None:
            parent_key = (message.scope.client_id, command.parent_command_id)
            parent = self._entries.get(parent_key)
            if parent is None:
                parent_type = command.command_type.removesuffix(".completed")
                parent_read = CommandRead(
                    command_id=command.parent_command_id,
                    command_type=parent_type,
                    status=status,
                    created_at=command.occurred_at,
                    handled_at=handled_at,
                )
                self._put(message.scope, parent_read, owners, message.destination_id)
            else:
                parent_read = parent[0].model_copy(update={"status": status, "handled_at": handled_at})
                self._put(message.scope, parent_read, parent[1], parent[3])
            child = ChildCommandRead(
                **read.model_dump(),
                parent_sequence=command.parent_sequence,
                recovery_message=message,
            )
            self._children[parent_key].append(child)
            parent_read = parent_read.model_copy(update={"latest_child_sequence": command.parent_sequence})
            parent_owners = parent[1] if parent is not None else owners
            parent_source = parent[3] if parent is not None else message.destination_id
            self._put(message.scope, parent_read, parent_owners, parent_source)
        return True

    async def is_cancelled(
        self,
        scope: CommandScope,
        command_id: UUID,
        *,
        session: AsyncSession | None = None,
    ) -> bool:
        """Check whether a prior direct command requested cancellation."""
        _ = session  # The in-memory implementation does not need the caller's database session.
        cancellation = self._cancellations.get((scope.client_id, command_id))
        return cancellation is not None and cancellation[0] == scope

    def has_received(self, message: MessageEnvelope) -> bool:
        """Check exact inbound identity without mutating the accepted projection."""
        return (message.scope.client_id, message.message_id) in self._seen_messages

    def mark_status(
        self,
        scope: CommandScope,
        command_id: UUID,
        status: CommandStatus,
        *,
        resource: ResourceReference | None = None,
        error_code: str | None = None,
    ) -> None:
        """Update one locally accepted command projection."""
        key = (scope.client_id, command_id)
        existing = self._entries.get(key)
        if existing is None or existing[2] != scope:
            raise CommandNotFound
        now = datetime.now(UTC)
        self._put(
            scope,
            existing[0].model_copy(
                update={
                    "status": status,
                    "resource": resource,
                    "error_code": error_code,
                    "handling_state": "running" if status == CommandStatus.RUNNING else "handled",
                    "handled_at": None if status in {CommandStatus.ACCEPTED, CommandStatus.RUNNING} else now,
                }
            ),
            existing[1],
            existing[3],
        )

    def append_child(self, original: MessageEnvelope, child: MessageEnvelope, result: HandlingResult) -> None:
        """Record a completion child and advance its parent to the terminal status."""
        self.record_received(child, local_id=original.destination_id)
        self.mark_status(
            original.scope,
            original.command_id,
            result.status or CommandStatus.SUCCEEDED,
            resource=result.resource,
            error_code=result.error_code,
        )

    async def get(
        self,
        scope: CommandScope,
        command_id: UUID,
        *,
        peer_id: str,
    ) -> CommandRead:
        """Read one projection only for a registered owner peer."""
        entry = self._entries.get((scope.client_id, command_id))
        if entry is None or entry[2] != scope:
            raise CommandNotFound
        if peer_id not in entry[1]:
            raise UnauthorizedPeer
        return entry[0]

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
        """Return ordered children with private envelopes limited to the original sender."""
        if after < 0 or not 1 <= limit <= _MAX_CHILD_PAGE:
            error = "Invalid child page bounds"
            raise ValueError(error)
        parent = self._entries.get((scope.client_id, command_id))
        if parent is None or parent[2] != scope:
            raise CommandNotFound
        if peer_id not in parent[1]:
            raise UnauthorizedPeer
        if include_recovery and peer_id != parent[3]:
            raise UnauthorizedPeer
        items = [item for item in self._children.get((scope.client_id, command_id), []) if item.parent_sequence > after]
        page = items[:limit]
        if not include_recovery:
            page = [item.model_copy(update={"recovery_message": None}) for item in page]
        next_after = page[-1].parent_sequence if len(items) > len(page) and page else None
        return ChildPage(items=page, next_after=next_after)
