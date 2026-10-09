"""Regressions for bounded outcomes, accepted cancellation and fair observation."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Never
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from execution_plane.commands.queries import EPResourceQueries
from execution_plane.models.work_item import WorkItem, WorkItemStatus
from execution_plane_client.crypto import canonical_bytes
from execution_plane_client.errors import CommandNotFound
from execution_plane_client.protocol import MAX_PAYLOAD_BYTES, MessageEnvelope
from execution_plane_client.queries import JournalQueryProvider

from .command_helpers import resource_case, work_case
from .exchange_helpers import journal_database
from .test_command_cancellation import cancellation


@pytest.mark.parametrize("size", [600 * 1024, MAX_PAYLOAD_BYTES - 500])
async def test_large_result_preserves_provider_success_and_reports_delivery_failure(work_message, size):
    async with work_case(work_message) as case:
        result = {"output": "x" * size}
        await case.store.set_result(case.work_id, result, WorkItemStatus.COMPLETED)
        stored = await case.reload()
        assert stored.status == WorkItemStatus.COMPLETED
        assert stored.result == result
        async with case.db.sessions() as session:
            table = case.db.journal.schema.table
            child_id = await session.scalar(
                select(table.c.entry_id).where(table.c.command_type == "work.execute.completed")
            )
            child = await case.db.journal.load_message(session, child_id)
            assert len(canonical_bytes(child)) <= MAX_PAYLOAD_BYTES
            assert child.command.payload["status"] == "failed"
            assert child.command.payload["error_code"] == "completion_result_too_large"
            assert child.command.payload["resource"]["resource_id"] == str(case.work_id)
            read = await JournalQueryProvider(case.db.journal).get(
                session, work_message.scope, work_message.command_id, peer_id="ao"
            )
            assert read.status == "failed"
            assert read.error_code == "completion_result_too_large"
        assert await case.store.claim_one() is None


async def test_resource_reads_bound_result_bytes_and_mark_omissions(work_message):
    async with work_case(work_message) as case:
        ids = [case.work_id]
        for _ in range(2):
            values = work_message.model_dump()
            values["message_id"] = uuid4()
            values["command_id"] = values["command"]["command_id"] = uuid4()
            ids.append((await case.run(MessageEnvelope.model_validate(values))).resource_id)
        for work_id in ids:
            await case.store.set_result(work_id, {"output": "x" * (400 * 1024)}, WorkItemStatus.COMPLETED)
        queries = EPResourceQueries(case.db.journal)
        async with case.db.sessions() as session:
            rows = await queries.work_items(session, work_message.scope, peer_id="ao")
            assert len(json.dumps([row.model_dump(mode="json") for row in rows]).encode()) < 1024 * 1024
            assert all(row.result is None and row.result_omitted for row in rows)
            single = await queries.work_item(session, work_message.scope, ids[0], peer_id="ao")
            assert single.result["output"] == "x" * (400 * 1024)
            assert not single.result_omitted
        # Provider results are durable even when too large for an individual response.
        async with case.db.sessions.begin() as session:
            stored = await session.get(WorkItem, ids[0])
            stored.result = {"output": "x" * (2 * 1024 * 1024)}
        async with case.db.sessions() as session:
            single = await queries.work_item(session, work_message.scope, ids[0], peer_id="ao")
            assert single.result is None
            assert single.result_omitted
            assert single.result_size_bytes > 2 * 1024 * 1024


async def test_accepted_cancellation_precedes_concurrent_work_handling(work_message, monkeypatch):
    from execution_plane.commands.control import CancelCommandHandler
    from execution_plane.commands.work import WorkCommandHandler

    async with resource_case() as case:
        work_handled = asyncio.Event()
        case.registry.register(case.catalog.definition("work.execute", 1), WorkCommandHandler(case.db.journal))
        case.registry.register(case.catalog.definition("command.cancel", 1), CancelCommandHandler(case.db.journal))
        cancel = cancellation(work_message.command_id, work_message.scope)
        handle = case.worker._handle

        async def work_before_cancel(claim) -> None:
            # Hold the cancellation before its transaction/identity lock. The
            # original must see accepted intent even while cancel handling waits.
            if claim.message.command_id == cancel.command_id:
                await asyncio.wait_for(work_handled.wait(), 5)
            await handle(claim)
            if claim.message.command_id == work_message.command_id:
                work_handled.set()

        monkeypatch.setattr(case.worker, "_handle", work_before_cancel)
        await case.receive(cancel)
        await case.receive(work_message)
        await case.worker.run_once()
        assert (await case.read(work_message)).status == "cancelled"
        async with case.db.sessions() as session:
            assert await session.scalar(select(func.count()).select_from(WorkItem)) == 0


async def test_peer_reconciliation_observes_beyond_first_unresolved_batch(work_message):
    from execution_plane_client.workers.reconciliation import ReconciliationWorker

    async with journal_database() as db:
        messages = []
        for _ in range(11):
            values = work_message.model_dump()
            values["message_id"] = uuid4()
            values["command_id"] = values["command"]["command_id"] = uuid4()
            message = MessageEnvelope.model_validate(values)
            async with db.sessions.begin() as session:
                await db.store.append_outbound(session, message)
            messages.append(message)
        observed = []

        async def get_command(_peer, _scope, command_id) -> Never:
            observed.append(command_id)
            raise CommandNotFound

        now = datetime.now(UTC) + timedelta(seconds=1)
        worker = ReconciliationWorker(
            db.store, db.sessions, SimpleNamespace(get_command=get_command), None, {}, clock=lambda: now
        )
        await worker.run_once()
        now += timedelta(seconds=6)
        await worker.run_once()
        assert messages[-1].command_id in observed


async def test_resource_reconciliation_observes_beyond_first_unresolved_batch():
    from execution_plane.cluster.cluster_store import ClusterStore
    from execution_plane.execution_target.execution_target_store import ExecutionTargetStore

    async with resource_case() as case:
        for _ in range(101):
            key = uuid4()
            created = await case.run(case.message(key=key))
            deletion = case.message("cluster.delete", key=key, revision=2)
            await case.run(deletion)
        # The newest drain is ready; every older one remains unresolved.
        async with case.db.sessions.begin() as session:
            for target in await case.targets(created.resource_id):
                await ExecutionTargetStore.from_transaction(session).finalize_delete(target.id)
            await ClusterStore.from_transaction(session).finalize_delete(created.resource_id)
        for _ in range(12):
            await case.observe()
        assert (await case.read(deletion)).status == "succeeded"
