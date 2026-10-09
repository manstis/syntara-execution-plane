"""Handling joins local effects atomically and reconciles ambiguous external calls."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, func, select

from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.handlers import CommandRegistry, EffectResult, HandlingResult, PreparedEffect
from execution_plane_client.journal.records import JournalEntry
from execution_plane_client.protocol import CommandStatus

from .exchange_helpers import journal_database
from .test_exchange_handshake import identity_for, receipt_handler


async def test_transactional_effect_and_completion_rollback_together(work_message, monkeypatch):
    from execution_plane_client.workers.handling import JournalHandlingWorker

    async with journal_database() as db:
        effects = Table(
            "domain_effects", MetaData(), Column("id", Integer, primary_key=True), schema=db.store.schema.table.schema
        )
        async with db.sessions() as session:
            connection = await session.connection()
            await connection.run_sync(effects.metadata.create_all)
            await session.commit()

        class Handler:
            async def handle(self, message, context, session) -> HandlingResult:
                await session.execute(effects.insert().values(id=1))
                return HandlingResult(status=CommandStatus.SUCCEEDED, result={"value": 42}, completion_required=True)

        registry = CommandRegistry()
        registry.register(CommandCatalog().definition("work.execute", 1), Handler())
        async with db.sessions.begin() as session:
            await receipt_handler(db.store).receive(work_message, identity_for(work_message), session)
        original_append = db.store.append_outbound

        async def fail_append(session, message) -> JournalEntry:
            if message.command.command_type.endswith(".completed"):
                error = "completion persistence failed"
                raise RuntimeError(error)
            return await original_append(session, message)

        monkeypatch.setattr(db.store, "append_outbound", fail_append)
        worker = JournalHandlingWorker(
            db.store, db.sessions, registry, local_id="ep", clock=lambda: datetime.now(UTC) + timedelta(seconds=1)
        )
        with pytest.raises(RuntimeError, match="completion persistence"):
            await worker.run_once()
        async with db.sessions() as session:
            assert await session.scalar(select(func.count()).select_from(effects)) == 0
        assert len(await db.read_rows()) == 2


async def test_crash_after_external_call_reconciles_without_executing_again(work_message):
    import asyncio

    from execution_plane_client.workers.handling import JournalHandlingWorker

    async with journal_database() as db:
        calls = []
        reconciled = []
        now = datetime.now(UTC) + timedelta(seconds=1)

        class Handler:
            async def prepare(self, message, context, session) -> PreparedEffect:
                entry = await db.store.get(session, message.scope, message.command_id, "inbound")
                return PreparedEffect(entry.entry_id, message.command_id, 1, {"operation": "test-only"})

            async def execute(self, effect) -> EffectResult:
                calls.append(effect.command_id)
                raise asyncio.CancelledError

            async def finish(self, effect, result, context, session) -> HandlingResult:
                return HandlingResult(status=CommandStatus.SUCCEEDED, result=result.result, completion_required=True)

            async def reconcile(self, effect) -> EffectResult:
                reconciled.append(effect.command_id)
                return EffectResult("succeeded", {"value": 42})

        registry = CommandRegistry()
        registry.register(CommandCatalog().definition("work.execute", 1), Handler())
        async with db.sessions.begin() as session:
            await receipt_handler(db.store).receive(work_message, identity_for(work_message), session)
        worker = JournalHandlingWorker(db.store, db.sessions, registry, local_id="ep", clock=lambda: now)
        with pytest.raises(asyncio.CancelledError):
            await worker.run_once()
        now += timedelta(seconds=60)
        restarted = JournalHandlingWorker(db.store, db.sessions, registry, local_id="ep", clock=lambda: now)
        await restarted.run_once()
        assert calls == [work_message.command_id]
        assert reconciled == [work_message.command_id]
        rows = await db.read_rows()
        assert len([row for row in rows if row["command_type"] == "work.execute.completed"]) == 1
        assert next(row for row in rows if row["command_id"] == work_message.command_id)["handling_state"] == "handled"


async def test_recovered_unknown_effect_keeps_explicit_responsibility(work_message):
    import asyncio

    from execution_plane_client.workers.handling import JournalHandlingWorker

    async with journal_database() as db:
        calls = []
        now = datetime.now(UTC) + timedelta(seconds=1)

        class Handler:
            async def prepare(self, message, context, session) -> PreparedEffect:
                entry = await db.store.get(session, message.scope, message.command_id, "inbound")
                return PreparedEffect(entry.entry_id, message.command_id, 1)

            async def execute(self, effect) -> EffectResult:
                calls.append(effect.command_id)
                raise asyncio.CancelledError

            async def finish(self, effect, result, context, session) -> HandlingResult:
                pytest.fail("Unknown observation cannot be finished as success")

            async def reconcile(self, effect) -> None:
                return None

        registry = CommandRegistry()
        registry.register(CommandCatalog().definition("work.execute", 1), Handler())
        async with db.sessions.begin() as session:
            await receipt_handler(db.store).receive(work_message, identity_for(work_message), session)
        worker = JournalHandlingWorker(db.store, db.sessions, registry, local_id="ep", clock=lambda: now)
        with pytest.raises(asyncio.CancelledError):
            await worker.run_once()
        now += timedelta(seconds=60)
        await worker.run_once()
        now += timedelta(seconds=60)
        await worker.run_once()
        assert calls == [work_message.command_id]
        row = next(row for row in await db.read_rows() if row["command_id"] == work_message.command_id)
        assert row["handling_state"] == "reconciling"
        assert row["reconciliation_required"] is True
        assert row["command_status"] == "outcome_unknown"


async def test_unknown_external_result_never_emits_success_or_repeated_outcomes(work_message):
    from execution_plane_client.workers.handling import JournalHandlingWorker

    async with journal_database() as db:
        now = datetime.now(UTC) + timedelta(seconds=1)

        class Handler:
            async def prepare(self, message, context, session) -> PreparedEffect:
                row = await db.store.get(session, message.scope, message.command_id, "inbound")
                return PreparedEffect(row.entry_id, message.command_id, 1)

            async def execute(self, effect) -> EffectResult:
                raise TimeoutError

            async def finish(self, effect, result, context, session) -> HandlingResult:
                # Even an adapter mistake cannot turn an unknown RPC into success.
                return HandlingResult(status=CommandStatus.SUCCEEDED, completion_required=True)

            async def reconcile(self, effect) -> EffectResult:
                return EffectResult("unknown")

        registry = CommandRegistry()
        registry.register(CommandCatalog().definition("work.execute", 1), Handler())
        async with db.sessions.begin() as session:
            await receipt_handler(db.store).receive(work_message, identity_for(work_message), session)
        worker = JournalHandlingWorker(db.store, db.sessions, registry, local_id="ep", clock=lambda: now)
        await worker.run_once()
        now += timedelta(seconds=60)
        await worker.run_once()
        rows = [row for row in await db.read_rows() if row["command_type"] == "work.execute.completed"]
        assert len(rows) == 1
        async with db.sessions() as session:
            completion = await db.store.load_message(session, rows[0]["entry_id"])
        assert completion.command.payload["status"] == "outcome_unknown"
