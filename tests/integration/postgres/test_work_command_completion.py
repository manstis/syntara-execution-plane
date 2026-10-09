"""Every work result persists its terminal child in the same provider transaction."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from execution_plane.models.work_item import WorkItem, WorkItemStatus

from .command_helpers import work_case
from .test_command_cancellation import cancellation


@pytest.mark.parametrize("producer", ["result", "unknown", "stale", "cancel_pending", "cancel_claimed", "cancel_retry"])
async def test_completion_insert_failure_rolls_back_every_terminal_producer(work_message, monkeypatch, producer):
    async with work_case(work_message) as case:
        item = await case.reload()
        if producer != "cancel_pending":
            item = await case.store.claim_one()
        if producer in {"result", "unknown", "stale", "cancel_retry"}:
            await case.store.mark_dispatched(
                item.id, claim_owner_id=item.claim_owner_id, claim_generation=item.claim_generation
            )
        if producer == "cancel_retry":
            await case.store.request_cancel(item.id, client_id=item.client_id, project_id=item.project_id)
        if producer == "stale":
            async with case.db.sessions.begin() as session:
                stale = await session.get(WorkItem, item.id)
                stale.claimed_at = datetime.now(UTC) - timedelta(minutes=2)
        before = await case.reload()

        async def reject_insert(session, message) -> None:
            error = "journal insert"
            raise RuntimeError(error)

        monkeypatch.setattr(case.db.journal, "append_outbound", reject_insert)
        with pytest.raises(RuntimeError, match="journal insert"):
            if producer == "result":
                await case.store.set_result(item.id, {"output": 42}, WorkItemStatus.COMPLETED)
            elif producer == "unknown":
                await case.store.mark_reconciliation_required(item.id, "ambiguous Execute")
            elif producer == "stale":
                await case.store.claim_one()
            elif producer == "cancel_retry":
                await case.store.requeue_pre_execute_failure(
                    item.id, claim_owner_id=item.claim_owner_id, claim_generation=item.claim_generation
                )
            else:
                await case.store.request_cancel(item.id, client_id=item.client_id, project_id=item.project_id)
        after = await case.reload()
        assert after.status == before.status
        assert after.completed_at == before.completed_at
        assert await case.counts() == 0


async def test_work_mapping_and_completion_identity_survive_repeat(work_message):
    async with work_case(work_message) as case:
        item = await case.reload()
        assert item.request_id == str(work_message.command_id)
        assert item.payload["invocation"] == work_message.command.payload["invocation"]
        assert str(item.work_correlation_id) == work_message.command.payload["work_correlation_id"]
        await case.store.set_result(item.id, {"output": 42}, WorkItemStatus.COMPLETED)
        await case.store.set_result(item.id, {"output": 42}, WorkItemStatus.COMPLETED)
        assert await case.counts() == 1
        async with case.db.sessions() as session:
            table = case.db.journal.schema.table
            entry_id = await session.scalar(
                select(table.c.entry_id).where(table.c.command_type == "work.execute.completed")
            )
            completion = await case.db.journal.load_message(session, entry_id)
            assert completion.command.target_url == work_message.command.callback_url
            assert completion.command.callback_url == work_message.command.target_url
            assert completion.command.parent_command_id == work_message.command_id
            assert completion.command.payload["status"] == "succeeded"
        assert (await case.read(work_message)).status == "succeeded"


async def test_unknown_can_be_observed_without_reexecution_and_then_resolved(work_message):
    async with work_case(work_message) as case:
        await case.store.mark_reconciliation_required(case.work_id, "unknown")
        await case.store.mark_reconciliation_required(case.work_id, "unknown")
        assert await case.counts() == 1
        assert await case.store.claim_one() is None
        await case.store.set_result(case.work_id, {"output": 42}, WorkItemStatus.COMPLETED)
        assert await case.counts() == 2
        assert (await case.read(work_message)).status == "succeeded"


@pytest.mark.parametrize("maximum", [None, 1, 3])
async def test_maximum_attempts_bounds_only_safe_pre_execute_retries(work_message, maximum):
    from execution_plane_client.protocol import MessageEnvelope

    values = work_message.model_dump()
    values["command"]["execution_policy"] = {"timeout_seconds": 30, "max_attempts": maximum}
    message = MessageEnvelope.model_validate(values)
    async with work_case(message) as case:
        for _ in range(maximum or 4):
            item = await case.store.claim_one()
            assert item is not None
            await case.store.mark_dispatched(
                item.id, claim_owner_id=item.claim_owner_id, claim_generation=item.claim_generation
            )
            await case.store.requeue_pre_execute_failure(
                item.id, claim_owner_id=item.claim_owner_id, claim_generation=item.claim_generation
            )
        after = await case.reload()
        assert after.status == (WorkItemStatus.FAILED if maximum else WorkItemStatus.PENDING)
        assert await case.counts() == (1 if maximum else 0)


async def test_unconfigured_runtime_store_cannot_drop_command_completion(work_message):
    from execution_plane.work_store import WorkStore

    async with work_case(work_message) as case:
        store = WorkStore.from_engine(case.db.sessions.kw["bind"])
        with pytest.raises(RuntimeError, match="completion recorder"):
            await store.set_result(case.work_id, {}, WorkItemStatus.COMPLETED)
        assert (await case.reload()).status == WorkItemStatus.PENDING


async def test_cancel_command_terminal_result_and_work_completion_commit_together(work_message):
    async with work_case(work_message) as case:
        assert (await case.run(cancellation(work_message.command_id, work_message.scope))).status == "succeeded"
        assert (await case.reload()).status == WorkItemStatus.CANCELLED
        assert await case.counts() == 1


async def test_cancel_before_submit_creates_no_provider_work(work_message):
    from sqlalchemy import func

    from execution_plane.commands.control import CancelCommandHandler
    from execution_plane.commands.work import WorkCommandHandler

    from .command_helpers import resource_case

    async with resource_case() as case:
        case.registry.register(case.catalog.definition("work.execute", 1), WorkCommandHandler(case.db.journal))
        case.registry.register(case.catalog.definition("command.cancel", 1), CancelCommandHandler(case.db.journal))
        await case.run(cancellation(work_message.command_id, work_message.scope))
        assert (await case.run(work_message)).status == "cancelled"
        async with case.db.sessions() as session:
            assert await session.scalar(select(func.count()).select_from(WorkItem)) == 0


async def test_cancellation_finishes_only_after_dispatched_work_stops(work_message):
    async with work_case(work_message) as case:
        item = await case.store.claim_one()
        await case.store.mark_dispatched(
            item.id, claim_owner_id=item.claim_owner_id, claim_generation=item.claim_generation
        )
        cancel = cancellation(work_message.command_id, work_message.scope)
        assert (await case.run(cancel)).status == "running"
        assert (await case.read(work_message)).status == "cancel_requested"
        await case.observe()
        assert (await case.read(cancel)).status == "running"
        await case.store.set_result(item.id, {"cancelled": True}, WorkItemStatus.CANCELLED)
        await case.observe()
        assert (await case.read(cancel)).status == "succeeded"


async def test_another_clients_cluster_cannot_claim_command_work(work_message):
    from execution_plane.commands.work import WorkCommandHandler
    from execution_plane.work_store import WorkStore

    from .command_helpers import resource_case

    async with resource_case() as case:
        case.registry.register(case.catalog.definition("work.execute", 1), WorkCommandHandler(case.db.journal))
        await case.run(case.message(client="other"))
        await case.run(work_message)
        store = WorkStore.from_engine(case.db.sessions.kw["bind"])
        assert await store.claim_one() is None
