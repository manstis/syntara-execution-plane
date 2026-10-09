"""Bilateral exchange exercises one shared service against two real journals."""

from .exchange_helpers import peer_pair


async def test_lost_response_retries_without_duplicate_external_effect(work_message):
    async with peer_pair() as pair:
        pair.drop_next_response("ep")
        await pair.ao.service.client.submit(work_message)
        assert pair.effects == 0
        await pair.pump(rounds=8)
        assert pair.effects == 1
        assert await pair.ao.delivery_state(work_message.command_id) == "sent"
        assert len(pair.results) == 1


async def test_missing_callback_is_recovered_with_original_identity(work_message):
    async with peer_pair() as pair:
        pair.blocked.add(("ep", "work.execute.completed"))
        await pair.ao.service.client.submit(work_message)
        await pair.pump(rounds=4)
        assert pair.effects == 1
        assert len(pair.results) == 1
        async with pair.ep.sessions() as session:
            children = await pair.ep.service.queries.children(
                session, work_message.scope, work_message.command_id, 0, 50, peer_id="ao"
            )
        assert pair.results == [children.items[0].command_id]
        assert await pair.ep.delivery_state(children.items[0].command_id) == "retrying"
        pair.blocked.clear()
        await pair.pump(rounds=2)
        assert await pair.ep.delivery_state(children.items[0].command_id) == "sent"


async def test_completion_can_arrive_before_original_http_receipt(work_message):
    async with peer_pair() as pair:
        pair.drop_next_response("ep")
        await pair.ao.service.client.submit(work_message)
        await pair.ao.service.delivery.run_once()
        assert await pair.ao.delivery_state(work_message.command_id) == "retrying"
        await pair.ep.service.handling.run_once()
        await pair.ep.service.delivery.run_once()
        await pair.ao.service.handling.run_once()
        assert len(pair.results) == 1
        async with pair.ao.sessions() as session:
            command = await pair.ao.service.queries.get(session, work_message.scope, work_message.command_id)
        assert command.status == "succeeded"
        assert await pair.ao.delivery_state(work_message.command_id) == "retrying"
        await pair.pump(rounds=2)
        assert await pair.ao.delivery_state(work_message.command_id) == "sent"
        assert pair.effects == 1


async def test_http_success_waits_for_receiver_commit(work_message, monkeypatch):
    import pytest
    from sqlalchemy import event
    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.errors import RetryableDelivery

    async with peer_pair() as pair:
        original_sessions = pair.ep.service.sessions

        def fail_commit(session) -> None:
            error = "test-only failed commit"
            raise RuntimeError(error)

        def failing_sessions() -> AsyncSession:
            session = original_sessions()
            event.listen(session.sync_session, "before_commit", fail_commit)
            return session

        monkeypatch.setattr(pair.ep.service, "_sessions", failing_sessions)
        with pytest.raises(RetryableDelivery):
            await pair.ao.service.http_transport.send(work_message)
        monkeypatch.setattr(pair.ep.service, "_sessions", original_sessions)
        async with pair.ep.sessions() as session:
            row = await pair.ep.store.get(session, work_message.scope, work_message.command_id, "inbound")
        assert row is None
