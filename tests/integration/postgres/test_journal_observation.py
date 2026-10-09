"""Observation claims survive restart without replaying initial domain mutations."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.completion import completion_message
from execution_plane_client.errors import HandlingDeferred
from execution_plane_client.handlers import CommandRegistry, HandlingResult
from execution_plane_client.ingress import CommandReceiptHandler
from execution_plane_client.journal.claims import JournalClaims
from execution_plane_client.protocol import CommandStatus, ResourceReference
from execution_plane_client.queries import JournalQueryProvider
from execution_plane_client.security import AuthenticatedIdentity
from execution_plane_client.workers.handling import JournalHandlingWorker

from .exchange_helpers import journal_database


async def test_observation_restart_expired_claim_and_final_resource_are_fenced(work_message):
    async with journal_database() as database:
        store = database.store
        now = datetime.now(UTC) + timedelta(seconds=1)
        registry = CommandRegistry()
        physical = uuid4()

        class Handler:
            handles = 0
            observations = 0

            async def handle(self, message, context, session) -> HandlingResult:
                self.handles += 1
                return HandlingResult(status=CommandStatus.CANCEL_REQUESTED, observe_after_seconds=2)

            async def observe(self, message, context, session) -> HandlingResult:
                self.observations += 1
                return HandlingResult(
                    status=CommandStatus.CANCELLED,
                    resource=ResourceReference(kind="work_item", resource_id=physical),
                    completion_required=True,
                )

        handler = Handler()
        registry.register(CommandCatalog().definition("work.execute", 1), handler)
        async with database.sessions.begin() as session:
            await store.append_or_confirm_inbound(session, work_message)
        worker = JournalHandlingWorker(store, database.sessions, registry, local_id="ep", clock=lambda: now)
        assert await worker.run_once() == 1
        async with database.sessions() as session:
            entry = await store.get(session, work_message.scope, work_message.command_id, "inbound")
            assert entry.observation_pending
            assert entry.handling_due_at == now + timedelta(seconds=2)
        now += timedelta(seconds=3)
        claims = JournalClaims(store, lease_seconds=2)
        async with database.sessions.begin() as session:
            abandoned = (await claims.claim_due_handling(session, uuid4(), now, 1))[0]
            assert abandoned.observe
        now += timedelta(seconds=3)
        restarted = JournalHandlingWorker(
            store, database.sessions, registry, local_id="ep", claims=claims, clock=lambda: now
        )
        assert await restarted.run_once() == 1
        await restarted._handle(abandoned)
        assert (handler.handles, handler.observations) == (1, 1)
        async with database.sessions() as session:
            projected = await JournalQueryProvider(store).get(session, work_message.scope, work_message.command_id)
            assert projected.status == CommandStatus.CANCELLED
            assert projected.resource.resource_id == physical
            entry = await store.get(session, work_message.scope, work_message.command_id, "inbound")
            assert not entry.observation_pending
            assert entry.handling_state == "handled"


@pytest.mark.parametrize("large", [False, True])
async def test_repeated_unknown_observations_do_not_emit_identical_completions(work_message, large):
    async with journal_database() as database:
        store = database.store
        now = datetime.now(UTC) + timedelta(seconds=1)
        registry = CommandRegistry()

        class Handler:
            async def handle(self, message, context, session) -> HandlingResult:
                return self.result()

            async def observe(self, message, context, session) -> HandlingResult:
                return self.result()

            def result(self) -> HandlingResult:
                return HandlingResult(
                    status=CommandStatus.OUTCOME_UNKNOWN,
                    error_code="still_draining",
                    result={"pending": True, "output": "x" * (600_000 if large else 0)},
                    completion_required=True,
                    observe_after_seconds=1,
                )

        registry.register(CommandCatalog().definition("work.execute", 1), Handler())
        async with database.sessions.begin() as session:
            await store.append_or_confirm_inbound(session, work_message)
        worker = JournalHandlingWorker(store, database.sessions, registry, local_id="ep", clock=lambda: now)
        for _ in range(3):
            assert await worker.run_once() == 1
            now += timedelta(seconds=2)
        table = store.schema.table
        async with database.sessions() as session:
            assert (
                await session.scalar(
                    select(func.count()).select_from(table).where(table.c.parent_command_id == work_message.command_id)
                )
                == 1
            )
            projected = await JournalQueryProvider(store).get(session, work_message.scope, work_message.command_id)
            assert projected.error_code == ("completion_result_too_large" if large else "still_draining")
            entry = await store.get(session, work_message.scope, work_message.command_id, "inbound")
            assert entry.observation_pending
            assert entry.handling_state == "deferred"


@pytest.mark.parametrize("initial_status", [CommandStatus.CANCEL_REQUESTED, CommandStatus.OUTCOME_UNKNOWN])
async def test_observation_deferred_rollback_retains_flag_and_previous_outcome(work_message, initial_status):
    async with journal_database() as database:
        store = database.store
        now = datetime.now(UTC) + timedelta(seconds=1)
        registry = CommandRegistry()

        class Handler:
            handles = 0
            observations = 0

            async def handle(self, message, context, session) -> HandlingResult:
                self.handles += 1
                return HandlingResult(status=initial_status, observe_after_seconds=1)

            async def observe(self, message, context, session) -> HandlingResult:
                self.observations += 1
                if self.observations == 1:
                    entry = await store.get(session, message.scope, message.command_id, "inbound")
                    await store.intents.record_result(
                        session,
                        entry.entry_id,
                        HandlingResult(status=CommandStatus.FAILED, error_code="rolled_back"),
                    )
                    raise HandlingDeferred
                return HandlingResult(status=CommandStatus.CANCELLED, completion_required=True)

        handler = Handler()
        registry.register(CommandCatalog().definition("work.execute", 1), handler)
        async with database.sessions.begin() as session:
            await store.append_or_confirm_inbound(session, work_message)
        worker = JournalHandlingWorker(store, database.sessions, registry, local_id="ep", clock=lambda: now)
        assert await worker.run_once() == 1
        now += timedelta(seconds=2)
        assert await worker.run_once() == 1
        async with database.sessions() as session:
            entry = await store.get(session, work_message.scope, work_message.command_id, "inbound")
            assert entry.observation_pending
            assert entry.command_status == initial_status
            assert entry.command_error_code is None
            assert entry.reconciliation_required == (initial_status == CommandStatus.OUTCOME_UNKNOWN)
        now += timedelta(seconds=5)
        assert await worker.run_once() == 1
        assert (handler.handles, handler.observations) == (1, 2)


async def test_received_newest_completion_projects_parent_resource_error_and_rolls_back(work_message):
    async with journal_database() as database:
        store = database.store
        catalog = CommandCatalog()
        registry = CommandRegistry()

        class Handler:
            async def handle(self, message, context, session) -> HandlingResult:
                return HandlingResult()

        registry.register(catalog.definition("work.execute.completed", 1), Handler())
        receipt = CommandReceiptHandler(store, catalog, registry, local_id="ao", local_role="controller")
        identity = AuthenticatedIdentity("ep", work_message.scope, frozenset({"work.execute.completed"}), "executor")
        physical = uuid4()
        outcomes = [
            completion_message(
                work_message,
                HandlingResult(
                    status=status,
                    error_code=error,
                    resource=ResourceReference(kind="work_item", resource_id=resource_id),
                ),
                sequence=sequence,
            )
            for sequence, status, error, resource_id in (
                (1, CommandStatus.OUTCOME_UNKNOWN, "old_unknown", uuid4()),
                (2, CommandStatus.FAILED, "new_failure", physical),
                (3, CommandStatus.SUCCEEDED, None, uuid4()),
            )
        ]
        async with database.sessions.begin() as session:
            await store.append_outbound(session, work_message)
        for outcome in (outcomes[1], outcomes[0], outcomes[1]):
            async with database.sessions.begin() as session:
                await receipt.receive(outcome, identity, session)
        with pytest.raises(RuntimeError, match="rollback"):
            async with database.sessions.begin() as session:
                await receipt.receive(outcomes[2], identity, session)
                error = "rollback"
                raise RuntimeError(error)
        async with database.sessions() as session:
            projected = await JournalQueryProvider(store).get(session, work_message.scope, work_message.command_id)
            assert projected.status == CommandStatus.FAILED
            assert projected.error_code == "new_failure"
            assert projected.resource.resource_id == physical
            assert projected.latest_child_sequence == 2
            assert not projected.reconciliation_required
            assert await store.get(session, work_message.scope, outcomes[2].command_id, "inbound") is None
