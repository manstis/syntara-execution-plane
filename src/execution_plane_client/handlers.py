"""Domain transaction and external-effect handler boundaries."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

from execution_plane_client.errors import ProtocolConflict, UnsupportedCommand

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.catalog import CommandDefinition
    from execution_plane_client.protocol import CommandScope, CommandStatus, MessageEnvelope, ResourceReference
    from execution_plane_client.security import AuthenticatedIdentity
    from execution_plane_client.transport import DispatchReceipt


class CommandDispatcher(Protocol):
    """Explicit outbound command capability supplied by the selected runtime."""

    async def send(self, message: MessageEnvelope) -> DispatchReceipt:
        """Dispatch an immutable command through the selected transport."""
        ...


class CompletionRecorder(Protocol):
    """Bridge a participant's domain completion into its selected transport."""

    async def __call__(self, session: object, item: object) -> None:
        """Stage or otherwise record the completion without committing the session."""
        ...

    async def after_commit(self, item: object) -> None:
        """Deliver completion data after the participant's database transaction commits."""
        ...


class ReceiptPolicy(Protocol):
    """Apply participant-owned acceptance rules in the selected receipt transaction."""

    async def __call__(self, message: MessageEnvelope, identity: AuthenticatedIdentity, session: object) -> None:
        """Validate or persist participant state without committing or external calls."""
        ...


class HandledHook(Protocol):
    """Update participant-specific projections in the selected handling transaction."""

    async def __call__(
        self,
        message: MessageEnvelope,
        context: HandlerContext,
        result: HandlingResult,
        session: object,
    ) -> None:
        """Apply local status changes without committing or making external calls."""
        ...


@dataclass(frozen=True)
class HandlerContext:
    """Explicit authenticated execution context without application globals."""

    scope: CommandScope
    local_id: str
    peer_id: str
    dispatcher: CommandDispatcher
    clock: Callable[[], datetime]
    completion_recorder: CompletionRecorder | None = None
    delivery_rejected: bool = False
    error_code: str | None = None


@dataclass(frozen=True)
class HandlingResult:
    """Local status change and optional final child emitted in the same transaction."""

    status: CommandStatus | None = None
    resource: ResourceReference | None = None
    result: dict[str, object] = field(default_factory=dict, repr=False)
    completion_required: bool = False
    error_code: str | None = None
    observe_after_seconds: float | None = None
    after_commit: Callable[[], Awaitable[None]] | None = field(default=None, compare=False, repr=False)


@dataclass(frozen=True)
class PreparedEffect:
    """Persisted responsibility and fenced domain attempt for an external call."""

    entry_id: UUID
    command_id: UUID
    domain_generation: int
    context: Mapping[str, object] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class EffectResult:
    """External outcome; unknown cannot be treated as safe to retry."""

    outcome: Literal["succeeded", "failed", "unknown"]
    result: dict[str, object] = field(default_factory=dict, repr=False)
    error_code: str | None = None


@runtime_checkable
class TransactionalHandler(Protocol):
    """Handle database-only effects within the shared handling transaction."""

    async def handle(
        self,
        message: MessageEnvelope,
        context: HandlerContext,
        session: AsyncSession,
    ) -> HandlingResult:
        """Apply local effects without committing or making external calls."""
        ...


@runtime_checkable
class ObservationHandler(Protocol):
    """Observe a previously applied domain mutation without repeating it."""

    async def observe(
        self,
        message: MessageEnvelope,
        context: HandlerContext,
        session: AsyncSession,
    ) -> HandlingResult:
        """Return the current outcome and optionally schedule another observation."""
        ...


@runtime_checkable
class ExternalHandler(Protocol):
    """Prepare durably, call externally, then finish with domain/claim fencing."""

    async def prepare(
        self,
        message: MessageEnvelope,
        context: HandlerContext,
        session: AsyncSession,
    ) -> PreparedEffect:
        """Record domain responsibility and return the fenced external input."""
        ...

    async def execute(self, effect: PreparedEffect) -> EffectResult:
        """Perform the external effect after preparation commits."""
        ...

    async def finish(
        self,
        effect: PreparedEffect,
        result: EffectResult,
        context: HandlerContext,
        session: AsyncSession,
    ) -> HandlingResult:
        """Persist outcome only if this remains the prepared domain generation."""
        ...

    async def reconcile(self, effect: PreparedEffect) -> EffectResult | None:
        """Observe uncertain execution without automatically repeating it."""
        ...


class CommandRegistry:
    """Map installed revisions to exactly one participant-owned handler."""

    def __init__(self) -> None:
        """Start with no registered domain handlers."""
        self._items: dict[tuple[str, int], tuple[CommandDefinition, TransactionalHandler | ExternalHandler]] = {}
        self._rejections: dict[tuple[str, int], TransactionalHandler | ExternalHandler] = {}

    def register_rejection(
        self,
        command_type: str,
        revision: int,
        handler: TransactionalHandler | ExternalHandler,
    ) -> None:
        """Register local failure handling using the same external-effect boundary."""
        key = (command_type, revision)
        if key in self._rejections:
            raise ProtocolConflict
        self._rejections[key] = handler

    def rejection_handler(self, command_type: str, revision: int) -> TransactionalHandler | ExternalHandler | None:
        """Return a durable sender-side failure adapter when a local actor needs one."""
        return self._rejections.get((command_type, revision))

    def register(self, definition: CommandDefinition, handler: TransactionalHandler | ExternalHandler) -> None:
        """Reject duplicate registrations."""
        key = (definition.command_type, definition.revision)
        if key in self._items:
            error = "Handler already registered"
            raise ProtocolConflict(error)
        self._items[key] = definition, handler

    def get(self, command_type: str, revision: int) -> TransactionalHandler | ExternalHandler:
        """Require local handling responsibility before accepting a command."""
        try:
            return self._items[command_type, revision][1]
        except KeyError:
            error = "No local command handler"
            raise UnsupportedCommand(error) from None

    def definitions(self) -> list[CommandDefinition]:
        """Return installed definitions for capability advertisement."""
        return [item[0] for item in self._items.values()]
