"""Bilateral exchange exercises one shared service against two real journals."""

from .exchange_helpers import peer_pair


async def test_lost_response_retries_without_duplicate_external_effect(work_message):
    async with peer_pair() as pair:
        pair.drop_next_response("ep")
        async with pair.ao.sessions.begin() as session:
            await pair.ao.service.client.submit(work_message, session)
        assert pair.effects == 0
        await pair.pump(rounds=8)
        assert pair.effects == 1
        assert await pair.ao.delivery_state(work_message.command_id) == "acknowledged"
        assert len(pair.results) == 1


async def test_missing_callback_is_recovered_with_original_identity(work_message):
    async with peer_pair() as pair:
        pair.blocked.add(("ep", "work.execute.completed"))
        async with pair.ao.sessions.begin() as session:
            await pair.ao.service.client.submit(work_message, session)
        await pair.pump(rounds=8)
        assert pair.effects == 1
        assert len(pair.results) == 1
        async with pair.ep.sessions() as session:
            children = await pair.ep.service.queries.children(
                session, work_message.scope, work_message.command_id, 0, 50, peer_id="ao"
            )
        assert pair.results == [children.items[0].command_id]
        assert await pair.ep.delivery_state(children.items[0].command_id) == "acknowledged"


async def test_completion_can_arrive_before_original_acceptance(work_message):
    async with peer_pair() as pair:
        pair.blocked.add(("ep", "command.accepted"))
        async with pair.ao.sessions.begin() as session:
            await pair.ao.service.client.submit(work_message, session)
        await pair.pump(rounds=3)
        assert len(pair.results) == 1
        async with pair.ao.sessions() as session:
            command = await pair.ao.service.queries.get(session, work_message.scope, work_message.command_id)
        assert command.status == "succeeded"
        assert command.delivery_state != "acknowledged"
        pair.blocked.clear()
        await pair.pump(rounds=3)
        assert await pair.ao.delivery_state(work_message.command_id) == "acknowledged"
        assert pair.effects == 1


async def test_http_success_waits_for_receiver_commit(work_message):
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

        pair.ep.service.sessions = failing_sessions
        with pytest.raises(RetryableDelivery):
            await pair.ao.service.transport.send(work_message)
        pair.ep.service.sessions = original_sessions
        async with pair.ep.sessions() as session:
            row = await pair.ep.store.get(session, work_message.scope, work_message.command_id, "inbound")
        assert row is None
