"""Provider and command responsibility remain durable across adapter restarts."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from execution_plane.commands.work import EPCompletionRecorder, WorkCommandHandler
from execution_plane.models.work_item import WorkItemStatus
from execution_plane.work_store import WorkStore
from execution_plane_client.journal.claims import JournalClaims
from execution_plane_client.workers.handling import JournalHandlingWorker

from .command_helpers import resource_case


@pytest.mark.parametrize("boundary", ["claimed", "mapped", "provider_unknown", "result_committed"])
async def test_restarted_work_adapter_never_repeats_provider_execution(work_message, boundary):
    async with resource_case() as case:
        case.registry.register(case.catalog.definition("work.execute", 1), WorkCommandHandler(case.db.journal))
        await case.run(case.message())
        await case.receive(work_message)
        if boundary == "claimed":
            async with case.db.sessions.begin() as session:
                claims = await JournalClaims(case.db.journal).claim_due_handling(session, uuid4(), datetime.now(UTC), 1)
                assert len(claims) == 1
        else:
            await case.worker.run_once()
        restarted = JournalHandlingWorker(
            case.db.journal,
            case.db.sessions,
            case.registry,
            local_id="ep",
            clock=lambda: datetime.now(UTC) + timedelta(minutes=1),
        )
        await restarted.run_once()
        record = await case.read(work_message)
        store = WorkStore.from_engine(case.db.sessions.kw["bind"])
        store.set_completion_recorder(EPCompletionRecorder(case.db.journal))
        item = await store.claim_one()
        assert item.id == record.resource_id
        await store.mark_dispatched(item.id, claim_owner_id=item.claim_owner_id, claim_generation=item.claim_generation)
        if boundary == "provider_unknown":
            await store.mark_reconciliation_required(item.id, "test provider lost response")
        else:
            await store.set_result(item.id, {"value": 42}, WorkItemStatus.COMPLETED)
        # Reconstruct both worker and provider store after losing all local state.
        del store, restarted
        store = WorkStore.from_engine(case.db.sessions.kw["bind"])
        store.set_completion_recorder(EPCompletionRecorder(case.db.journal))
        await case.receive(work_message)
        await case.worker.run_once()
        assert (await case.read(work_message)).resource_id == item.id
        assert await store.claim_one() is None
        assert (await case.read(work_message)).status == (
            "outcome_unknown" if boundary == "provider_unknown" else "succeeded"
        )
