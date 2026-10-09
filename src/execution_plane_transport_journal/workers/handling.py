"""Atomic local handling and durable preparation around ambiguous external effects."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import select, update

from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.completion import completion_message, completion_result
from execution_plane_client.errors import CommandExecutionError, HandlingDeferred, ProtocolConflict
from execution_plane_client.handlers import (
    CommandDispatcher,
    EffectResult,
    ExternalHandler,
    HandledHook,
    HandlerContext,
    HandlingResult,
    ObservationHandler,
    PreparedEffect,
    TransactionalHandler,
)
from execution_plane_client.protocol import CommandStatus, MessageEnvelope, validate_json
from execution_plane_client.transport import DispatchReceipt, DispatchState
from execution_plane_transport_journal.claims import JournalClaims
from execution_plane_transport_journal.completion import append_completion
from execution_plane_transport_journal.effects import StoredEffect, restore_effect
from execution_plane_transport_journal.records import JournalEntry
from execution_plane_transport_journal.workers.scheduling import run_claimed

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.handlers import CommandRegistry, CompletionRecorder
    from execution_plane_client.observers import ExchangeObserver
    from execution_plane_transport_journal.claims import HandlingClaim
    from execution_plane_transport_journal.store import ExchangeJournalStore


class _SessionJournalDispatcher(CommandDispatcher):
    """Bind completion/follow-up dispatch to the current handling transaction."""

    def __init__(self, store: ExchangeJournalStore, session: AsyncSession) -> None:
        """Retain the active transaction so appends commit with local effects."""
        self._store = store
        self._session = session

    async def send(self, message: MessageEnvelope) -> DispatchReceipt:
        """Append a child command without committing the caller-owned session."""
        entry = await self._store.append_outbound(self._session, message)
        return DispatchReceipt(message_id=entry.message_id, state=DispatchState.QUEUED)


def _now() -> datetime:
    return datetime.now(UTC)


class JournalHandlingWorker:
    """Execute local responsibility independently of receipt acknowledgement."""

    def __init__(
        self,
        store: ExchangeJournalStore,
        sessions: Callable[[], AsyncSession],
        registry: CommandRegistry,
        *,
        local_id: str,
        claims: JournalClaims | None = None,
        clock: Callable[[], datetime] = _now,
        handled_hook: HandledHook | None = None,
        completion_recorder: CompletionRecorder | None = None,
        poll_seconds: float = 1,
        batch_size: int = 10,
        observer: ExchangeObserver | None = None,
    ) -> None:
        """Inject participant handlers and local projection integration."""
        self.wakeup = asyncio.Event()
        self._store = store
        self._sessions = sessions
        self._registry = registry
        self._claims = claims or JournalClaims(store)
        self._local_id = local_id
        self._clock = clock
        self._hook = handled_hook
        self._completion_recorder = completion_recorder
        self._poll_seconds = poll_seconds
        self._batch_size = batch_size
        self._owner = uuid4()
        self._observer = observer

    def _context(self, claim: HandlingClaim, session: AsyncSession) -> HandlerContext:
        message = claim.message
        return HandlerContext(
            scope=message.scope,
            local_id=self._local_id,
            peer_id=message.destination_id if claim.direction == "outbound" else message.source_id,
            dispatcher=_SessionJournalDispatcher(self._store, session),
            clock=self._clock,
            completion_recorder=self._completion_recorder,
            delivery_rejected=claim.direction == "outbound",
            error_code=claim.error_code,
        )

    async def _locked(self, session: AsyncSession, claim: HandlingClaim) -> JournalEntry | None:
        if claim.direction == "inbound":
            await self._store.intents.lock(session, claim.message)
        # Participant context lives on the original command. Lock its family in
        # the same order as token refresh before taking an individual child lock.
        await self._store.lock_command_family(
            session, claim.message.scope, claim.message.command.parent_command_id or claim.message.command_id
        )
        table = self._store.schema.table
        row = (
            (
                await session.execute(
                    select(table)
                    .where(
                        table.c.entry_id == claim.entry_id,
                        table.c.handling_owner == claim.owner_id,
                        table.c.handling_generation == claim.generation,
                    )
                    .with_for_update()
                )
            )
            .mappings()
            .one_or_none()
        )
        return JournalEntry.model_validate(dict(row)) if row is not None else None

    async def _finish(
        self,
        session: AsyncSession,
        claim: HandlingClaim,
        result: HandlingResult,
        *,
        unknown: bool = False,
    ) -> None:
        now = self._clock()
        table = self._store.schema.table
        if unknown:
            result = replace(
                result, status=CommandStatus.OUTCOME_UNKNOWN, error_code=result.error_code or "external_outcome_unknown"
            )
        if result.completion_required and claim.direction == "inbound":
            bounded = await self._identical_unknown(session, claim, result)
            if bounded is None:
                child = await append_completion(self._store, session, claim.message, result, occurred_at=now)
                bounded = completion_result(child)
            result = replace(bounded, observe_after_seconds=result.observe_after_seconds)
        status = CommandStatus.OUTCOME_UNKNOWN if unknown else result.status or CommandStatus.SUCCEEDED
        result = replace(result, status=status)
        observe = result.observe_after_seconds is not None
        if observe and (result.observe_after_seconds is None or result.observe_after_seconds < 0):
            raise ProtocolConflict
        if self._hook is not None:
            await self._hook(claim.message, self._context(claim, session), result, session)
        await self._store.intents.record_result(session, claim.entry_id, result)
        await session.execute(
            update(table)
            .where(table.c.entry_id == claim.entry_id)
            .values(
                handling_state="deferred" if observe else "reconciling" if unknown else "handled",
                handling_owner=None,
                handling_lease_until=None,
                handling_due_at=now + timedelta(seconds=result.observe_after_seconds or 0) if observe else None,
                observation_pending=observe,
                handled_at=None if unknown or observe else now,
                handling_error_code=result.error_code,
                reconciliation_required=unknown or status == CommandStatus.OUTCOME_UNKNOWN,
                reconciliation_due_at=now + timedelta(seconds=self._claims.retry_delay(claim.attempt))
                if unknown and not observe
                else None,
                updated_at=now,
            )
        )

    async def _identical_unknown(
        self,
        session: AsyncSession,
        claim: HandlingClaim,
        result: HandlingResult,
    ) -> HandlingResult | None:
        """Suppress repeated identical unknown observations while retaining new outcomes."""
        if result.status != CommandStatus.OUTCOME_UNKNOWN:
            return None
        table = self._store.schema.table
        latest = await session.scalar(
            select(table.c.entry_id)
            .where(
                table.c.client_id == claim.message.scope.client_id,
                table.c.parent_command_id == claim.message.command_id,
                table.c.direction == "outbound",
                table.c.parent_sequence.is_not(None),
            )
            .order_by(table.c.parent_sequence.desc())
            .limit(1),
        )
        if latest is None:
            return None
        previous_message = await self._store.load_message(session, latest)
        if previous_message.command.parent_sequence is None:
            raise ProtocolConflict
        candidate = completion_message(
            claim.message,
            result,
            sequence=previous_message.command.parent_sequence,
            occurred_at=previous_message.command.occurred_at,
        )
        return (
            completion_result(previous_message)
            if previous_message.command.payload == candidate.command.payload
            else None
        )

    async def _defer(self, claim: HandlingClaim) -> None:
        async with self._sessions() as session, session.begin():
            if await self._locked(session, claim) is None:
                return
            table = self._store.schema.table
            now = self._clock()
            await session.execute(
                update(table)
                .where(table.c.entry_id == claim.entry_id)
                .values(
                    handling_state="deferred",
                    handling_owner=None,
                    handling_lease_until=None,
                    handling_due_at=now + timedelta(seconds=self._claims.retry_delay(claim.attempt)),
                    prepared_effect_ciphertext=table.c.prepared_effect_ciphertext if claim.observe else None,
                    reconciliation_required=table.c.reconciliation_required if claim.observe else False,
                    reconciliation_due_at=table.c.reconciliation_due_at if claim.observe else None,
                    updated_at=now,
                )
            )

    async def _unknown(self, claim: HandlingClaim) -> None:
        async with self._sessions() as session, session.begin():
            entry = await self._locked(session, claim)
            if entry is None:
                return
            definition = CommandCatalog().definition(
                claim.message.command.command_type, claim.message.command.command_revision
            )
            result = HandlingResult(
                status=CommandStatus.OUTCOME_UNKNOWN,
                completion_required=definition.kind == "action"
                and entry.command_status != CommandStatus.OUTCOME_UNKNOWN,
                error_code="external_outcome_unknown",
            )
            await self._finish(session, claim, result, unknown=True)

    async def _transactional(
        self,
        claim: HandlingClaim,
        handler: TransactionalHandler | ExternalHandler | None,
    ) -> None:
        try:
            async with self._sessions() as session, session.begin():
                if await self._locked(session, claim) is None:
                    return
                try:
                    async with session.begin_nested():
                        result = (
                            await self._invoke_transactional(claim, handler, session)
                            if handler is not None
                            else HandlingResult(status=CommandStatus.FAILED, error_code=claim.error_code)
                        )
                except CommandExecutionError as exc:
                    result = HandlingResult(
                        status=CommandStatus.FAILED,
                        error_code=exc.code,
                        completion_required=claim.direction == "inbound",
                    )
                await self._finish(session, claim, result)
        except HandlingDeferred:
            await self._defer(claim)

    async def _invoke_transactional(
        self,
        claim: HandlingClaim,
        handler: TransactionalHandler | ExternalHandler,
        session: AsyncSession,
    ) -> HandlingResult:
        if claim.observe:
            if not isinstance(handler, ObservationHandler):
                raise ProtocolConflict
            return await handler.observe(claim.message, self._context(claim, session), session)
        if (
            claim.direction == "inbound"
            and claim.message.command.command_type == "work.execute"
            and await self._store.intents.is_cancelled(claim.message.scope, claim.message.command_id, session=session)
        ):
            return HandlingResult(status=CommandStatus.CANCELLED, completion_required=True)
        if not isinstance(handler, TransactionalHandler):
            raise ProtocolConflict
        return await handler.handle(claim.message, self._context(claim, session), session)

    async def _prepare(self, claim: HandlingClaim, handler: ExternalHandler) -> PreparedEffect | None:
        async with self._sessions() as session, session.begin():
            if await self._locked(session, claim) is None:
                return None
            effect = await handler.prepare(claim.message, self._context(claim, session), session)
            if effect.entry_id != claim.entry_id or effect.command_id != claim.message.command_id:
                raise ProtocolConflict
            # Private encrypted preparation includes participant token/identity metadata.
            # Wire limits apply at ingress; they must not reject an accepted payload
            # when the handler adds its own durable reconstruction state.
            validate_json(dict(effect.context), max_bytes=None)
            value = StoredEffect(
                entry_id=effect.entry_id,
                command_id=effect.command_id,
                domain_generation=effect.domain_generation,
                context=dict(effect.context),
            )
            ciphertext = self._store.cipher.encrypt(
                value.model_dump_json().encode(), associated_data=b"prepared-effect-v1:" + claim.entry_id.bytes
            )
            table = self._store.schema.table
            await session.execute(
                update(table)
                .where(table.c.entry_id == claim.entry_id)
                .values(
                    handling_state="prepared",
                    prepared_effect_ciphertext=ciphertext,
                    handling_lease_until=self._clock() + timedelta(seconds=self._claims.lease_seconds),
                )
            )
            return effect

    def _restore(self, claim: HandlingClaim) -> PreparedEffect | None:
        if claim.prepared_effect_ciphertext is None:
            return None
        return restore_effect(
            self._store.cipher, claim.prepared_effect_ciphertext, claim.entry_id, claim.message.command_id
        )

    async def _external(self, claim: HandlingClaim, handler: ExternalHandler) -> None:
        try:
            effect = self._restore(claim) if claim.reconcile else await self._prepare(claim, handler)
            if effect is None:
                if claim.reconcile:
                    await self._unknown(claim)
                return
            try:
                outcome = await handler.reconcile(effect) if claim.reconcile else await handler.execute(effect)
            except HandlingDeferred:
                raise
            except Exception:  # noqa: BLE001 - Any uncertain RPC failure preserves responsibility, without replay.
                outcome = EffectResult("unknown", error_code="external_outcome_unknown")
            if outcome is None:
                await self._unknown(claim)
                return
            async with self._sessions() as session, session.begin():
                if await self._locked(session, claim) is None:
                    return
                result = await handler.finish(effect, outcome, self._context(claim, session), session)
                await self._finish(session, claim, result, unknown=outcome.outcome == "unknown")
        except HandlingDeferred:
            await self._defer(claim)

    async def _handle(self, claim: HandlingClaim) -> None:
        command = claim.message.command
        handler = (
            self._registry.rejection_handler(command.command_type, command.command_revision)
            if claim.direction == "outbound"
            else self._registry.get(command.command_type, command.command_revision)
        )
        if claim.observe:
            await self._transactional(claim, handler)
        elif isinstance(handler, ExternalHandler):
            await self._external(claim, handler)
        else:
            await self._transactional(claim, handler)

    async def _claim(self, limit: int) -> list[HandlingClaim]:
        async with self._sessions() as session, session.begin():
            return await self._claims.claim_due_handling(session, self._owner, self._clock(), limit)

    async def _next_due(self) -> datetime | None:
        async with self._sessions() as session:
            return await self._claims.next_handling_at(session)

    async def _process(self, claim: HandlingClaim) -> None:
        await self._handle(claim)
        if self._observer is not None:
            table = self._store.schema.table
            async with self._sessions() as session:
                rows = (
                    (
                        await session.execute(
                            select(
                                table.c.command_id,
                                table.c.handling_state,
                                table.c.created_at,
                                table.c.handled_at,
                                table.c.command_status,
                            ).where(table.c.entry_id.in_([claim.entry_id]))
                        )
                    )
                    .mappings()
                    .all()
                )
            for row in rows:
                self._observer.record(
                    "handling.observed",
                    {
                        "command_id": str(row["command_id"]),
                        "state": row["handling_state"],
                        "status": row["command_status"],
                        "latency_seconds": max(
                            0.0, ((row["handled_at"] or self._clock()) - row["created_at"]).total_seconds()
                        ),
                    },
                )

    async def run_once(self) -> int:
        """Commit independent claims before invoking local or external handlers."""
        claims = await self._claim(self._batch_size)
        await asyncio.gather(*(self._process(claim) for claim in claims))
        return len(claims)

    async def run(self) -> None:
        """Handle on commit hints or durable deadlines without whole-batch waits."""
        await run_claimed(
            self._claim,
            self._process,
            self._next_due,
            self.wakeup,
            clock=self._clock,
            limit=self._batch_size,
            poll_seconds=self._poll_seconds,
        )
